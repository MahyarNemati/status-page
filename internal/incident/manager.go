// Package incident turns a stream of check results into incidents. It opens
// an incident after consecutive failures, resolves it after consecutive
// successes, and holds incidents open while a service is flapping.
package incident

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/MahyarNemati/status-page/internal/alert"
	"github.com/MahyarNemati/status-page/internal/check"
	"github.com/MahyarNemati/status-page/internal/config"
	"github.com/MahyarNemati/status-page/internal/store"
)

// Severities used for incidents.
const (
	SeverityMinor    = "minor"
	SeverityMajor    = "major"
	SeverityCritical = "critical"
)

// ValidSeverity reports whether s is a known severity.
func ValidSeverity(s string) bool {
	return s == SeverityMinor || s == SeverityMajor || s == SeverityCritical
}

// SystemAuthor is the author recorded on automatically generated notes.
const SystemAuthor = "monitor"

// Policy configures flap detection: a service that changes between
// available and unavailable FlapThreshold times within FlapWindow is
// considered flapping.
type Policy struct {
	FlapWindow    time.Duration
	FlapThreshold int
}

// Manager tracks per-service state. It is safe for concurrent use.
type Manager struct {
	store     store.Store
	pub       alert.Publisher
	policy    Policy
	publicURL string
	log       *slog.Logger

	mu     sync.Mutex
	states map[string]*state
}

type state struct {
	last    check.Status
	downRun int
	upRun   int
	// downSince is when the current run of failures began; incidents are
	// backdated to it so durations reflect the real outage.
	downSince time.Time
	changes   []time.Time
	flapping  bool
	open      *store.Incident
}

// NewManager returns a Manager. pub may be nil to disable notifications.
func NewManager(st store.Store, pub alert.Publisher, policy Policy, publicURL string, log *slog.Logger) *Manager {
	if pub == nil {
		pub = alert.PublisherFunc(func(alert.Notification) {})
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Manager{store: st, pub: pub, policy: policy, publicURL: publicURL, log: log, states: map[string]*state{}}
}

// Restore reloads open automatic incidents so a restart does not open
// duplicates or lose track of an ongoing outage.
func (m *Manager) Restore(ctx context.Context) error {
	open, err := m.store.ListIncidents(ctx, store.IncidentFilter{State: store.IncidentOpen})
	if err != nil {
		return fmt.Errorf("restore incidents: %w", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range open {
		in := open[i]
		if !in.Auto {
			continue
		}
		st := m.stateLocked(in.ServiceID)
		if st.open == nil {
			st.open = &in
			// Treat the service as down until results prove otherwise.
			st.last = check.StatusDown
		}
	}
	return nil
}

func (m *Manager) stateLocked(id string) *state {
	st, ok := m.states[id]
	if !ok {
		st = &state{}
		m.states[id] = st
	}
	return st
}

// Flapping reports whether the service is currently considered unstable.
func (m *Manager) Flapping(serviceID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.states[serviceID]
	return ok && st.flapping
}

// Observe feeds one result into the state machine. Store writes happen under
// the manager lock so observations for a service are applied in order.
func (m *Manager) Observe(ctx context.Context, svc config.Service, res check.Result, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.stateLocked(svc.ID)

	available := res.Status.Available()
	if st.last != check.StatusUnknown && st.last.Available() != available {
		st.changes = append(st.changes, at)
	}
	st.last = res.Status
	cutoff := at.Add(-m.policy.FlapWindow)
	for len(st.changes) > 0 && !st.changes[0].After(cutoff) {
		st.changes = st.changes[1:]
	}
	wasFlapping := st.flapping
	st.flapping = m.policy.FlapThreshold > 0 && len(st.changes) >= m.policy.FlapThreshold

	if available {
		st.upRun++
		st.downRun = 0
	} else {
		if st.downRun == 0 {
			st.downSince = at
		}
		st.downRun++
		st.upRun = 0
	}

	if st.flapping != wasFlapping {
		m.log.Info("flap state changed", "service", svc.ID, "flapping", st.flapping, "changes", len(st.changes))
		if st.open != nil && st.flapping {
			m.noteLocked(ctx, st.open.ID, at, fmt.Sprintf("Service is flapping: %d state changes in %s.", len(st.changes), m.policy.FlapWindow))
		}
	}

	switch {
	case st.open == nil && (st.downRun >= svc.FailureThreshold || st.flapping):
		title, severity, started := svc.Name+" is down", SeverityMajor, st.downSince
		if st.downRun < svc.FailureThreshold {
			title, severity, started = svc.Name+" is unstable", SeverityMinor, at
		}
		in, err := m.store.CreateIncident(ctx, store.Incident{
			ServiceID: svc.ID, Title: title, Severity: severity, Auto: true, StartedAt: started,
		})
		if err != nil {
			return err
		}
		st.open = &in
		m.noteLocked(ctx, in.ID, at, "Automatically opened: "+res.Message)
		m.log.Warn("incident opened", "service", svc.ID, "incident", in.ID, "severity", severity, "reason", res.Message)
		m.pub.Publish(alert.Notification{
			Kind: alert.IncidentOpened, Key: incidentKey(svc.ID), ServiceID: svc.ID, ServiceName: svc.Name,
			Severity: severity, Title: title, Message: res.Message, URL: m.publicURL, At: at, Channels: svc.Alerts,
		})

	case st.open != nil && !st.flapping && st.upRun >= svc.RecoveryThreshold:
		in, err := m.store.ResolveIncident(ctx, st.open.ID, at)
		if err != nil {
			return err
		}
		st.open = nil
		dur := in.Duration(at).Round(time.Second)
		m.noteLocked(ctx, in.ID, at, fmt.Sprintf("Automatically resolved after %s.", dur))
		m.log.Info("incident resolved", "service", svc.ID, "incident", in.ID, "duration", dur)
		m.pub.Publish(alert.Notification{
			Kind: alert.IncidentResolved, Key: incidentKey(svc.ID), ServiceID: svc.ID, ServiceName: svc.Name,
			Severity: in.Severity, Title: in.Title, Message: fmt.Sprintf("%s recovered after %s.", svc.Name, dur),
			URL: m.publicURL, At: at, Channels: svc.Alerts,
		})
	}
	return nil
}

func (m *Manager) noteLocked(ctx context.Context, id int64, at time.Time, body string) {
	if _, err := m.store.AddNote(ctx, store.Note{IncidentID: id, Author: SystemAuthor, Body: body, CreatedAt: at}); err != nil {
		m.log.Error("add incident note", "incident", id, "err", err)
	}
}

func incidentKey(serviceID string) string { return "incident:" + serviceID }

// ErrInvalid is returned for malformed manual operations.
var ErrInvalid = errors.New("invalid request")

// Declare opens a manual incident, for example planned maintenance or an
// outage the probes cannot see.
func (m *Manager) Declare(ctx context.Context, svc config.Service, title, severity, author, note string, at time.Time) (store.Incident, error) {
	if title == "" || !ValidSeverity(severity) {
		return store.Incident{}, fmt.Errorf("%w: title and a severity of minor, major or critical are required", ErrInvalid)
	}
	in, err := m.store.CreateIncident(ctx, store.Incident{
		ServiceID: svc.ID, Title: title, Severity: severity, StartedAt: at,
	})
	if err != nil {
		return store.Incident{}, err
	}
	if note != "" {
		if _, err := m.store.AddNote(ctx, store.Note{IncidentID: in.ID, Author: author, Body: note, CreatedAt: at}); err != nil {
			return store.Incident{}, err
		}
	}
	m.pub.Publish(alert.Notification{
		Kind: alert.IncidentOpened, Key: fmt.Sprintf("incident:%s:%d", svc.ID, in.ID), ServiceID: svc.ID,
		ServiceName: svc.Name, Severity: severity, Title: title, Message: note, URL: m.publicURL, At: at, Channels: svc.Alerts,
	})
	return m.store.GetIncident(ctx, in.ID)
}

// AddNote appends an update to any incident.
func (m *Manager) AddNote(ctx context.Context, id int64, author, body string, at time.Time) (store.Note, error) {
	if body == "" || author == "" {
		return store.Note{}, fmt.Errorf("%w: author and body are required", ErrInvalid)
	}
	return m.store.AddNote(ctx, store.Note{IncidentID: id, Author: author, Body: body, CreatedAt: at})
}

// Resolve closes an incident by hand. Resolving an automatic incident while
// the service is still failing is allowed; a new one opens on the next
// failing check that crosses the threshold.
func (m *Manager) Resolve(ctx context.Context, svc config.Service, id int64, author, note string, at time.Time) (store.Incident, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	in, err := m.store.GetIncident(ctx, id)
	if err != nil {
		return store.Incident{}, err
	}
	if in.State == store.IncidentResolved {
		return in, nil
	}
	if note != "" {
		if _, err := m.store.AddNote(ctx, store.Note{IncidentID: id, Author: author, Body: note, CreatedAt: at}); err != nil {
			return store.Incident{}, err
		}
	}
	in, err = m.store.ResolveIncident(ctx, id, at)
	if err != nil {
		return store.Incident{}, err
	}
	key := fmt.Sprintf("incident:%s:%d", in.ServiceID, in.ID)
	if st, ok := m.states[in.ServiceID]; ok && st.open != nil && st.open.ID == id {
		st.open = nil
		st.downRun = 0
		key = incidentKey(in.ServiceID)
	}
	m.pub.Publish(alert.Notification{
		Kind: alert.IncidentResolved, Key: key, ServiceID: in.ServiceID, ServiceName: svc.Name,
		Severity: in.Severity, Title: in.Title, Message: fmt.Sprintf("Resolved by %s.", author),
		URL: m.publicURL, At: at, Channels: svc.Alerts,
	})
	return m.store.GetIncident(ctx, id)
}
