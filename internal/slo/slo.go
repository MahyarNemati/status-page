// Package slo computes availability against per-service objectives, the
// remaining error budget over a rolling window, and multi-window burn-rate
// alerts in the style of the Google SRE workbook.
package slo

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/MahyarNemati/status-page/internal/alert"
	"github.com/MahyarNemati/status-page/internal/clock"
	"github.com/MahyarNemati/status-page/internal/config"
	"github.com/MahyarNemati/status-page/internal/store"
)

// Burn is the evaluation of one burn-rate rule.
type Burn struct {
	Rule      string        `json:"rule"`
	Severity  string        `json:"severity"`
	Factor    float64       `json:"factor"`
	LongWin   time.Duration `json:"long_window_ns"`
	ShortWin  time.Duration `json:"short_window_ns"`
	LongRate  float64       `json:"long_rate"`
	ShortRate float64       `json:"short_rate"`
	Firing    bool          `json:"firing"`
}

// Report is the SLO state of one service.
type Report struct {
	ServiceID   string        `json:"service_id"`
	ServiceName string        `json:"service_name"`
	Target      float64       `json:"target_percent"`
	Window      time.Duration `json:"window_ns"`
	Counts      store.Counts  `json:"counts"`
	// Availability is a ratio in [0,1], or -1 without data.
	Availability float64 `json:"availability"`
	// BudgetTotal is the number of failed checks the window allows.
	BudgetTotal float64 `json:"budget_total"`
	// BudgetRemaining is the unspent fraction of the budget; negative once
	// the objective is breached.
	BudgetRemaining float64   `json:"budget_remaining"`
	Burns           []Burn    `json:"burn_rates"`
	EvaluatedAt     time.Time `json:"evaluated_at"`
}

// Observer receives every fresh report, e.g. to update metrics.
type Observer func(Report)

// Evaluator periodically evaluates every service that declares an SLO.
type Evaluator struct {
	store    store.Store
	clock    clock.Clock
	services []config.Service
	window   time.Duration
	rules    []config.BurnRule
	pub      alert.Publisher
	url      string
	observe  Observer
	log      *slog.Logger

	mu      sync.RWMutex
	reports map[string]Report
	firing  map[string]bool
}

// Options configures an Evaluator.
type Options struct {
	Store     store.Store
	Clock     clock.Clock
	Services  []config.Service
	Policy    config.SLOPolicy
	Publisher alert.Publisher
	PublicURL string
	Observer  Observer
	Logger    *slog.Logger
}

// New returns an Evaluator.
func New(o Options) *Evaluator {
	if o.Clock == nil {
		o.Clock = clock.Real()
	}
	if o.Publisher == nil {
		o.Publisher = alert.PublisherFunc(func(alert.Notification) {})
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	var svcs []config.Service
	for _, s := range o.Services {
		if s.SLO != nil {
			svcs = append(svcs, s)
		}
	}
	return &Evaluator{
		store: o.Store, clock: o.Clock, services: svcs, window: o.Policy.Window.D(), rules: o.Policy.BurnRules,
		pub: o.Publisher, url: o.PublicURL, observe: o.Observer, log: o.Logger,
		reports: map[string]Report{}, firing: map[string]bool{},
	}
}

// Run evaluates immediately and then every interval until ctx is done.
func (e *Evaluator) Run(ctx context.Context, every time.Duration) {
	for {
		if err := e.Evaluate(ctx); err != nil && ctx.Err() == nil {
			e.log.Error("slo evaluation failed", "err", err)
		}
		if !clock.Sleep(e.clock, every, ctx.Done()) {
			return
		}
	}
}

// Evaluate recomputes every report and publishes burn-rate transitions.
func (e *Evaluator) Evaluate(ctx context.Context) error {
	now := e.clock.Now()
	for _, svc := range e.services {
		r, err := e.evaluate(ctx, svc, now)
		if err != nil {
			return fmt.Errorf("service %s: %w", svc.ID, err)
		}
		e.mu.Lock()
		e.reports[svc.ID] = r
		e.mu.Unlock()
		e.transitions(svc, r, now)
		if e.observe != nil {
			e.observe(r)
		}
	}
	return nil
}

func (e *Evaluator) evaluate(ctx context.Context, svc config.Service, now time.Time) (Report, error) {
	r := Report{
		ServiceID: svc.ID, ServiceName: svc.Name, Target: svc.SLO.Target, Window: e.window, EvaluatedAt: now,
	}
	counts, err := e.windowCounts(ctx, svc.ID, now)
	if err != nil {
		return r, err
	}
	allowed := 1 - svc.SLO.Target/100
	r.Counts = counts
	r.Availability = counts.Ratio()
	r.BudgetTotal = allowed * float64(counts.Total)
	r.BudgetRemaining = 1
	if r.BudgetTotal > 0 {
		r.BudgetRemaining = 1 - float64(counts.Down())/r.BudgetTotal
	}

	for _, rule := range e.rules {
		long, err := e.burnRate(ctx, svc.ID, now, rule.Long.D(), allowed)
		if err != nil {
			return r, err
		}
		short, err := e.burnRate(ctx, svc.ID, now, rule.Short.D(), allowed)
		if err != nil {
			return r, err
		}
		r.Burns = append(r.Burns, Burn{
			Rule: rule.Name, Severity: rule.Severity, Factor: rule.Factor,
			LongWin: rule.Long.D(), ShortWin: rule.Short.D(), LongRate: long, ShortRate: short,
			Firing: long >= rule.Factor && short >= rule.Factor,
		})
	}
	return r, nil
}

// windowCounts sums hourly rollups from the hour containing the window
// start and raw results for the current partial hour. The window can
// therefore extend up to one hour further back than configured, which keeps
// it independent of raw retention.
func (e *Evaluator) windowCounts(ctx context.Context, id string, now time.Time) (store.Counts, error) {
	first := store.Hourly.Truncate(now.Add(-e.window))
	current := store.Hourly.Truncate(now)
	buckets, err := e.store.Rollups(ctx, id, store.Hourly, first, current)
	if err != nil {
		return store.Counts{}, err
	}
	var c store.Counts
	for _, b := range buckets {
		c = c.Add(b.Counts)
	}
	tail, err := e.store.RawCounts(ctx, id, current, now.Add(time.Millisecond))
	if err != nil {
		return store.Counts{}, err
	}
	return c.Add(tail), nil
}

// burnRate is the observed error ratio over the window divided by the error
// ratio the objective allows. A rate of 1 spends the budget exactly over
// the SLO window.
func (e *Evaluator) burnRate(ctx context.Context, id string, now time.Time, win time.Duration, allowed float64) (float64, error) {
	// RawCounts is half-open, so shift by the storage resolution to get (now-win, now].
	c, err := e.store.RawCounts(ctx, id, now.Add(-win+time.Millisecond), now.Add(time.Millisecond))
	if err != nil {
		return 0, err
	}
	if c.Total == 0 || allowed <= 0 {
		return 0, nil
	}
	return (float64(c.Down()) / float64(c.Total)) / allowed, nil
}

func (e *Evaluator) transitions(svc config.Service, r Report, now time.Time) {
	for _, b := range r.Burns {
		key := "slo:" + svc.ID + ":" + b.Rule
		e.mu.Lock()
		was := e.firing[key]
		e.firing[key] = b.Firing
		e.mu.Unlock()
		if was == b.Firing {
			continue
		}
		n := alert.Notification{
			Key: key, ServiceID: svc.ID, ServiceName: svc.Name, Severity: b.Severity,
			Title: fmt.Sprintf("%s is burning its error budget (%s)", svc.Name, b.Rule),
			URL:   e.url, At: now, Channels: svc.Alerts,
		}
		if b.Firing {
			n.Kind = alert.BurnRateFiring
			n.Message = fmt.Sprintf("Burn rate %.1fx over %s and %.1fx over %s exceeds %.1fx. %.1f%% of the %s budget remains.",
				b.LongRate, b.LongWin, b.ShortRate, b.ShortWin, b.Factor, r.BudgetRemaining*100, e.window)
			e.log.Warn("burn rate alert firing", "service", svc.ID, "rule", b.Rule, "long", b.LongRate, "short", b.ShortRate)
		} else {
			n.Kind = alert.BurnRateResolved
			n.Message = fmt.Sprintf("Burn rate back below %.1fx.", b.Factor)
			e.log.Info("burn rate alert resolved", "service", svc.ID, "rule", b.Rule)
		}
		e.pub.Publish(n)
	}
}

// Reports returns the latest reports sorted by service id.
func (e *Evaluator) Reports() []Report {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]Report, 0, len(e.reports))
	for _, r := range e.reports {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ServiceID < out[j].ServiceID })
	return out
}

// Report returns the latest report for one service.
func (e *Evaluator) Report(id string) (Report, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	r, ok := e.reports[id]
	return r, ok
}
