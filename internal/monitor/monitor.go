// Package monitor schedules probes, persists their results, opens and
// resolves incidents, and dispatches alerts.
//
// The state machine per service is deliberately hysteretic. A single failed
// probe does not open an incident: FailureThreshold consecutive failures do.
// A single success does not close one: RecoveryThreshold consecutive
// successes do. Without that, one dropped packet pages the on-call.
package monitor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/MahyarNemati/status-page/internal/check"
	"github.com/MahyarNemati/status-page/internal/clock"
	"github.com/MahyarNemati/status-page/internal/config"
	"github.com/MahyarNemati/status-page/internal/notify"
	"github.com/MahyarNemati/status-page/internal/slo"
	"github.com/MahyarNemati/status-page/internal/store"
)

// Monitor runs the probe loop for every configured service.
type Monitor struct {
	cfg    *config.Config
	st     store.Store
	alerts *notify.Router
	clk    clock.Clock
	log    *slog.Logger
	rnd    *rand.Rand

	checkers map[string]check.Checker
	services map[string]config.Service

	mu     sync.Mutex
	states map[string]*serviceState

	// sem bounds how many probes run at once.
	sem chan struct{}
}

// serviceState is the hysteresis and incident bookkeeping for one service.
type serviceState struct {
	// consecutive counts how many probes in a row agreed with last.
	consecutiveFail int
	consecutiveOK   int
	// confirmed is the last status we acted on, as opposed to observed.
	confirmed check.Status
	// incidentID is the open auto incident, or 0.
	incidentID int64
	// transitions records confirmed status flips, for flap detection.
	transitions []time.Time
	flapping    bool
}

// New builds a Monitor. Checkers are constructed up front so a bad target
// fails at startup rather than on the first probe.
func New(cfg *config.Config, st store.Store, alerts *notify.Router, clk clock.Clock, log *slog.Logger) (*Monitor, error) {
	if cfg == nil {
		return nil, errors.New("monitor: nil config")
	}
	if st == nil {
		return nil, errors.New("monitor: nil store")
	}
	if clk == nil {
		clk = clock.Real()
	}
	if log == nil {
		log = slog.Default()
	}

	workers := cfg.Scheduler.Workers
	if workers <= 0 {
		workers = 8
	}

	m := &Monitor{
		cfg:      cfg,
		st:       st,
		alerts:   alerts,
		clk:      clk,
		log:      log,
		rnd:      rand.New(rand.NewPCG(uint64(clk.Now().UnixNano()), 0x9E3779B97F4A7C15)),
		checkers: make(map[string]check.Checker, len(cfg.Services)),
		services: make(map[string]config.Service, len(cfg.Services)),
		states:   make(map[string]*serviceState, len(cfg.Services)),
		sem:      make(chan struct{}, workers),
	}

	for _, svc := range cfg.Services {
		c, err := check.New(svc)
		if err != nil {
			return nil, fmt.Errorf("monitor: service %q: %w", svc.ID, err)
		}
		if alerts != nil {
			for _, name := range svc.Alerts {
				if !alerts.Has(name) {
					return nil, fmt.Errorf("monitor: service %q references unknown alert channel %q", svc.ID, name)
				}
			}
		}
		m.checkers[svc.ID] = c
		m.services[svc.ID] = svc
		m.states[svc.ID] = &serviceState{confirmed: check.StatusUnknown}
	}
	return m, nil
}

// Run probes every service on its interval until ctx is cancelled. It also
// runs the retention prune and the SLO burn-rate evaluation on their own
// schedules. Run blocks and returns nil on clean shutdown.
func (m *Monitor) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	for _, svc := range m.cfg.Services {
		wg.Add(1)
		go func(svc config.Service) {
			defer wg.Done()
			m.loop(ctx, svc)
		}(svc)
	}

	if every := m.cfg.Storage.Retention.Every.D(); every > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.pruneLoop(ctx, every)
		}()
	}
	if every := m.cfg.SLO.EvaluateEvery.D(); every > 0 && len(m.cfg.SLO.BurnRules) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.burnLoop(ctx, every)
		}()
	}

	wg.Wait()
	return nil
}

// loop probes one service forever.
func (m *Monitor) loop(ctx context.Context, svc config.Service) {
	interval := svc.Interval.D()
	if interval <= 0 {
		interval = m.cfg.Defaults.Interval.D()
	}
	if interval <= 0 {
		interval = time.Minute
	}

	// Stagger startup so N services do not all probe on the same tick and
	// hammer a shared dependency.
	if d := m.startupJitter(interval); d > 0 && !m.sleep(ctx, d) {
		return
	}

	for {
		m.CheckOnce(ctx, svc)

		wait := interval
		if j := m.cfg.Scheduler.Jitter; j > 0 {
			// Jitter is a fraction of the interval, applied symmetrically.
			spread := float64(interval) * j
			wait = time.Duration(float64(interval) - spread/2 + m.rnd.Float64()*spread)
			if wait <= 0 {
				wait = interval
			}
		}
		if !m.sleep(ctx, wait) {
			return
		}
	}
}

func (m *Monitor) startupJitter(interval time.Duration) time.Duration {
	if interval <= 0 {
		return 0
	}
	return time.Duration(m.rnd.Int64N(int64(interval)))
}

// sleep waits d or returns false when ctx ends.
func (m *Monitor) sleep(ctx context.Context, d time.Duration) bool {
	t := m.clk.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C():
		return true
	case <-ctx.Done():
		return false
	}
}

// CheckOnce probes a service once, applying retries, and processes the
// outcome. Exported so tests can drive the state machine deterministically.
func (m *Monitor) CheckOnce(ctx context.Context, svc config.Service) {
	select {
	case m.sem <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-m.sem }()

	res := m.probe(ctx, svc)
	if ctx.Err() != nil {
		return
	}

	now := m.clk.Now()
	rec := store.Result{
		ServiceID: svc.ID,
		At:        now,
		Status:    res.Status,
		Latency:   res.Latency,
		Message:   res.Message,
	}
	if err := m.st.SaveResult(ctx, rec); err != nil {
		m.log.Error("save probe result", "service", svc.ID, "err", err)
		// Still run the state machine: losing a row should not also lose the
		// incident transition that row would have triggered.
	}

	m.observe(ctx, svc, res, now)
}

// probe runs the check with retries. A probe is only reported down after
// every attempt failed, so a single packet loss does not count as a failure.
func (m *Monitor) probe(ctx context.Context, svc config.Service) check.Result {
	c := m.checkers[svc.ID]
	if c == nil {
		return check.Result{Status: check.StatusDown, Message: "no checker configured"}
	}

	timeout := svc.Timeout.D()
	if timeout <= 0 {
		timeout = m.cfg.Defaults.Timeout.D()
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	retries := m.cfg.Defaults.Retries
	if svc.Retries != nil {
		retries = *svc.Retries
	}
	if retries < 0 {
		retries = 0
	}
	delay := svc.RetryDelay.D()
	if delay <= 0 {
		delay = m.cfg.Defaults.RetryDelay.D()
	}

	var res check.Result
	for attempt := 0; attempt <= retries; attempt++ {
		if attempt > 0 && delay > 0 && !m.sleep(ctx, delay) {
			return res
		}
		probeCtx, cancel := context.WithTimeout(ctx, timeout)
		res = c.Check(probeCtx)
		cancel()
		if res.Status.Available() {
			return res
		}
	}
	return res
}

// observe advances the hysteresis state machine and reacts to confirmed
// transitions.
func (m *Monitor) observe(ctx context.Context, svc config.Service, res check.Result, now time.Time) {
	failThreshold := svc.FailureThreshold
	if failThreshold <= 0 {
		failThreshold = m.cfg.Defaults.FailureThreshold
	}
	if failThreshold <= 0 {
		failThreshold = 1
	}
	okThreshold := svc.RecoveryThreshold
	if okThreshold <= 0 {
		okThreshold = m.cfg.Defaults.RecoveryThreshold
	}
	if okThreshold <= 0 {
		okThreshold = 1
	}

	m.mu.Lock()
	s := m.states[svc.ID]
	if s == nil {
		s = &serviceState{confirmed: check.StatusUnknown}
		m.states[svc.ID] = s
	}

	available := res.Status.Available()
	if available {
		s.consecutiveOK++
		s.consecutiveFail = 0
	} else {
		s.consecutiveFail++
		s.consecutiveOK = 0
	}

	var (
		openIncident    bool
		resolveIncident bool
		incidentID      int64
		flapping        bool
	)

	switch {
	case !available && s.confirmed != check.StatusDown && s.consecutiveFail >= failThreshold:
		s.confirmed = check.StatusDown
		s.recordTransition(now, m.cfg.Incidents)
		flapping = s.flapping
		// While flapping, keep probing and recording but stop opening a new
		// incident on every flip: the noise buries the signal.
		if !flapping {
			openIncident = true
		}
	case available && s.confirmed == check.StatusDown && s.consecutiveOK >= okThreshold:
		s.confirmed = res.Status
		s.recordTransition(now, m.cfg.Incidents)
		flapping = s.flapping
		if s.incidentID != 0 {
			resolveIncident = true
			incidentID = s.incidentID
			s.incidentID = 0
		}
	case s.confirmed == check.StatusUnknown && available && s.consecutiveOK >= okThreshold:
		// First healthy confirmation after startup: record it without
		// treating it as a recovery (there was no incident).
		s.confirmed = res.Status
	}
	m.mu.Unlock()

	switch {
	case openIncident:
		m.openIncident(ctx, svc, res, now)
	case resolveIncident:
		m.resolveIncident(ctx, svc, incidentID, now)
	}
}

// recordTransition appends a confirmed flip and recomputes the flap flag.
// Caller holds m.mu.
func (s *serviceState) recordTransition(now time.Time, p config.IncidentPolicy) {
	s.transitions = append(s.transitions, now)
	window := p.FlapWindow.D()
	if window <= 0 {
		s.flapping = false
		return
	}
	cutoff := now.Add(-window)
	kept := s.transitions[:0]
	for _, t := range s.transitions {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	s.transitions = kept

	threshold := p.FlapThreshold
	if threshold <= 0 {
		s.flapping = false
		return
	}
	s.flapping = len(s.transitions) >= threshold
}

func (m *Monitor) openIncident(ctx context.Context, svc config.Service, res check.Result, now time.Time) {
	title := fmt.Sprintf("%s is down", displayName(svc))
	in, err := m.st.CreateIncident(ctx, store.Incident{
		ServiceID: svc.ID,
		Title:     title,
		Severity:  string(notify.SeverityCritical),
		State:     store.IncidentOpen,
		Auto:      true,
		StartedAt: now,
	})
	if err != nil {
		m.log.Error("create incident", "service", svc.ID, "err", err)
	} else {
		m.mu.Lock()
		if s := m.states[svc.ID]; s != nil {
			s.incidentID = in.ID
		}
		m.mu.Unlock()
	}

	m.log.Warn("service down", "service", svc.ID, "message", res.Message)
	m.dispatch(ctx, svc, notify.Alert{
		DedupeKey:   "down:" + svc.ID,
		Severity:    notify.SeverityCritical,
		Title:       title,
		Body:        res.Message,
		ServiceID:   svc.ID,
		ServiceName: displayName(svc),
		URL:         m.cfg.Server.PublicURL,
		At:          now,
	})
}

func (m *Monitor) resolveIncident(ctx context.Context, svc config.Service, id int64, now time.Time) {
	var downFor string
	if in, err := m.st.ResolveIncident(ctx, id, now); err != nil {
		m.log.Error("resolve incident", "service", svc.ID, "incident", id, "err", err)
	} else {
		downFor = in.Duration(now).Round(time.Second).String()
	}

	body := "Service is responding normally again."
	if downFor != "" {
		body = fmt.Sprintf("Service is responding normally again after %s.", downFor)
	}

	m.log.Info("service recovered", "service", svc.ID, "down_for", downFor)
	m.dispatch(ctx, svc, notify.Alert{
		DedupeKey:   "down:" + svc.ID,
		Resolved:    true,
		Severity:    notify.SeverityInfo,
		Title:       fmt.Sprintf("%s has recovered", displayName(svc)),
		Body:        body,
		ServiceID:   svc.ID,
		ServiceName: displayName(svc),
		URL:         m.cfg.Server.PublicURL,
		At:          now,
	})
}

func (m *Monitor) dispatch(ctx context.Context, svc config.Service, a notify.Alert) {
	if m.alerts == nil {
		return
	}
	// Alert delivery must not be cancelled by the probe context, and must not
	// block the loop indefinitely either.
	sendCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if err := m.alerts.Dispatch(sendCtx, a, svc.Alerts); err != nil {
		m.log.Error("dispatch alert", "service", svc.ID, "err", err)
	}
}

// pruneLoop enforces retention on a schedule.
func (m *Monitor) pruneLoop(ctx context.Context, every time.Duration) {
	r := store.Retention{
		Raw:    m.cfg.Storage.Retention.Raw.D(),
		Hourly: m.cfg.Storage.Retention.Hourly.D(),
		Daily:  m.cfg.Storage.Retention.Daily.D(),
	}
	for {
		if !m.sleep(ctx, every) {
			return
		}
		stats, err := m.st.Prune(ctx, m.clk.Now(), r)
		if err != nil {
			m.log.Error("prune", "err", err)
			continue
		}
		m.log.Info("pruned", "raw", stats.Raw, "hourly", stats.Hourly, "daily", stats.Daily)
	}
}

// burnLoop evaluates error-budget burn rules on a schedule.
func (m *Monitor) burnLoop(ctx context.Context, every time.Duration) {
	for {
		if !m.sleep(ctx, every) {
			return
		}
		m.EvaluateBurn(ctx)
	}
}

// EvaluateBurn checks burn-rate rules for every service with an objective.
// Exported so tests and the admin API can trigger it.
func (m *Monitor) EvaluateBurn(ctx context.Context) {
	now := m.clk.Now()
	for _, svc := range m.cfg.Services {
		target, ok := m.objective(svc)
		if !ok {
			continue
		}
		fired, err := slo.EvaluateBurn(ctx, m.st, svc.ID, target, m.cfg.SLO.BurnRules, now)
		if err != nil {
			m.log.Error("evaluate burn rate", "service", svc.ID, "err", err)
			continue
		}
		for _, a := range fired {
			sev := notify.Severity(a.Severity)
			m.log.Warn("error budget burning",
				"service", svc.ID, "rule", a.Rule, "long_rate", a.Long.Rate, "short_rate", a.Short.Rate)
			m.dispatch(ctx, svc, notify.Alert{
				DedupeKey:   "burn:" + svc.ID + ":" + a.Rule,
				Severity:    sev,
				Title:       fmt.Sprintf("%s is burning its error budget (%s)", displayName(svc), a.Rule),
				Body:        a.Describe(),
				ServiceID:   svc.ID,
				ServiceName: displayName(svc),
				URL:         m.cfg.Server.PublicURL,
				At:          now,
			})
		}
	}
}

// objective returns the availability target for a service, preferring the
// per-service value over the global one.
func (m *Monitor) objective(svc config.Service) (float64, bool) {
	if svc.SLO != nil && svc.SLO.Target > 0 {
		return svc.SLO.Target, true
	}
	return 0, false
}

// Status is a point-in-time snapshot for one service, used by the web layer.
type Status struct {
	Confirmed check.Status `json:"confirmed"`
	Flapping  bool         `json:"flapping"`
	Incident  int64        `json:"incident_id,omitempty"`
}

// Snapshot returns the confirmed state machine view for every service.
func (m *Monitor) Snapshot() map[string]Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]Status, len(m.states))
	for id, s := range m.states {
		out[id] = Status{Confirmed: s.confirmed, Flapping: s.flapping, Incident: s.incidentID}
	}
	return out
}

func displayName(svc config.Service) string {
	if svc.Name != "" {
		return svc.Name
	}
	return svc.ID
}
