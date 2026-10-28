package incident

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/MahyarNemati/status-page/internal/alert"
	"github.com/MahyarNemati/status-page/internal/check"
	"github.com/MahyarNemati/status-page/internal/config"
	"github.com/MahyarNemati/status-page/internal/store"
)

type sink struct {
	mu  sync.Mutex
	got []alert.Notification
}

func (s *sink) Publish(n alert.Notification) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, n)
}

func (s *sink) kinds() []alert.Kind {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []alert.Kind
	for _, n := range s.got {
		out = append(out, n.Kind)
	}
	return out
}

func setup(t *testing.T, policy Policy) (*Manager, store.Store, *sink) {
	t.Helper()
	st, err := store.Open(context.Background(), "sqlite", "file:"+filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	s := &sink{}
	return NewManager(st, s, policy, "https://status.example.com", nil), st, s
}

var svc = config.Service{ID: "api", Name: "API", FailureThreshold: 2, RecoveryThreshold: 2, Alerts: []string{"ops"}}

var t0 = time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

func feed(t *testing.T, m *Manager, statuses string, start time.Time, every time.Duration) time.Time {
	t.Helper()
	at := start
	for _, c := range statuses {
		s := check.StatusUp
		switch c {
		case 'x':
			s = check.StatusDown
		case 'd':
			s = check.StatusDegraded
		}
		if err := m.Observe(context.Background(), svc, check.Result{Status: s, Message: "probe " + s.String()}, at); err != nil {
			t.Fatal(err)
		}
		at = at.Add(every)
	}
	return at
}

func openIncidents(t *testing.T, st store.Store) []store.Incident {
	t.Helper()
	in, err := st.ListIncidents(context.Background(), store.IncidentFilter{State: store.IncidentOpen})
	if err != nil {
		t.Fatal(err)
	}
	return in
}

func TestOpenAndResolveWithThresholds(t *testing.T) {
	m, st, s := setup(t, Policy{FlapWindow: time.Minute, FlapThreshold: 100})

	feed(t, m, "ux.u", t0, time.Minute)
	if len(openIncidents(t, st)) != 0 {
		t.Fatal("single failure must not open an incident")
	}
	at := feed(t, m, "xx", t0.Add(10*time.Minute), time.Minute)
	open := openIncidents(t, st)
	if len(open) != 1 || !open[0].Auto || open[0].Severity != SeverityMajor || open[0].Title != "API is down" {
		t.Fatalf("open incidents = %+v", open)
	}
	if !open[0].StartedAt.Equal(t0.Add(10 * time.Minute)) {
		t.Fatalf("started at %v", open[0].StartedAt)
	}

	at = feed(t, m, "xxdx", at, time.Minute)
	if len(openIncidents(t, st)) != 1 {
		t.Fatal("ongoing failure must not open a second incident")
	}
	feed(t, m, "ud", at, time.Minute)
	if len(openIncidents(t, st)) != 0 {
		t.Fatal("incident should resolve after recovery threshold; degraded counts as available")
	}
	all, _ := st.ListIncidents(context.Background(), store.IncidentFilter{})
	full, _ := st.GetIncident(context.Background(), all[0].ID)
	if full.State != store.IncidentResolved || len(full.Notes) != 2 || full.Notes[0].Author != SystemAuthor {
		t.Fatalf("resolved incident = %+v", full)
	}
	if got := s.kinds(); len(got) != 2 || got[0] != alert.IncidentOpened || got[1] != alert.IncidentResolved {
		t.Fatalf("notifications = %v", got)
	}
	if s.got[0].Channels[0] != "ops" || s.got[0].Key != "incident:api" || s.got[1].Key != "incident:api" {
		t.Fatalf("notification routing = %+v", s.got)
	}
}

func TestFlappingHoldsIncidentOpen(t *testing.T) {
	m, st, s := setup(t, Policy{FlapWindow: 10 * time.Minute, FlapThreshold: 4})

	// Alternating results never reach the failure threshold of 2, but the
	// fourth change within the window marks the service as flapping.
	at := feed(t, m, "uxuxu", t0, time.Minute)
	open := openIncidents(t, st)
	if len(open) != 1 || open[0].Severity != SeverityMinor || open[0].Title != "API is unstable" {
		t.Fatalf("flapping should open a minor incident, got %+v", open)
	}
	if !m.Flapping("api") {
		t.Fatal("expected flapping")
	}

	// Two healthy results would normally resolve, but flapping holds it open.
	at = feed(t, m, "uu", at, time.Minute)
	if len(openIncidents(t, st)) != 1 {
		t.Fatal("incident resolved while still flapping")
	}

	// Once the changes age out of the window the incident resolves.
	feed(t, m, "uu", at.Add(10*time.Minute), time.Minute)
	if len(openIncidents(t, st)) != 0 || m.Flapping("api") {
		t.Fatal("incident should resolve once stable")
	}
	if got := s.kinds(); len(got) != 2 {
		t.Fatalf("flapping produced %d notifications, want open+resolve: %v", len(got), got)
	}
}

func TestRestoreAvoidsDuplicates(t *testing.T) {
	m, st, s := setup(t, Policy{FlapWindow: time.Minute, FlapThreshold: 100})
	feed(t, m, "xx", t0, time.Minute)

	restarted := NewManager(st, s, Policy{FlapWindow: time.Minute, FlapThreshold: 100}, "", nil)
	if err := restarted.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	at := feed(t, restarted, "xxx", t0.Add(time.Hour), time.Minute)
	if len(openIncidents(t, st)) != 1 {
		t.Fatal("restart opened a duplicate incident")
	}
	feed(t, restarted, "uu", at, time.Minute)
	if len(openIncidents(t, st)) != 0 {
		t.Fatal("restored incident not resolved")
	}
}

func TestManualIncidentLifecycle(t *testing.T) {
	m, st, s := setup(t, Policy{FlapWindow: time.Minute, FlapThreshold: 100})
	ctx := context.Background()

	if _, err := m.Declare(ctx, svc, "", "major", "alice", "", t0); err == nil {
		t.Fatal("expected validation error")
	}
	if _, err := m.Declare(ctx, svc, "Maintenance", "urgent", "alice", "", t0); err == nil {
		t.Fatal("expected severity validation error")
	}
	in, err := m.Declare(ctx, svc, "Database maintenance", SeverityMinor, "alice", "Failover in progress.", t0)
	if err != nil {
		t.Fatal(err)
	}
	if in.Auto || len(in.Notes) != 1 || in.Notes[0].Author != "alice" {
		t.Fatalf("declared = %+v", in)
	}
	if _, err := m.AddNote(ctx, in.ID, "bob", "Replica caught up.", t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := m.AddNote(ctx, 9999, "bob", "x", t0); err == nil {
		t.Fatal("note on missing incident should fail")
	}
	res, err := m.Resolve(ctx, svc, in.ID, "alice", "Done.", t0.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if res.State != store.IncidentResolved || len(res.Notes) != 3 {
		t.Fatalf("resolved = %+v", res)
	}
	if again, err := m.Resolve(ctx, svc, in.ID, "alice", "", t0.Add(3*time.Minute)); err != nil || !again.ResolvedAt.Equal(*res.ResolvedAt) {
		t.Fatalf("resolve should be idempotent: %v %v", again.ResolvedAt, err)
	}
	if len(openIncidents(t, st)) != 0 || len(s.kinds()) != 2 {
		t.Fatalf("notifications = %v", s.kinds())
	}
}

func TestManualResolveOfAutoIncident(t *testing.T) {
	m, st, s := setup(t, Policy{FlapWindow: time.Minute, FlapThreshold: 100})
	at := feed(t, m, "xx", t0, time.Minute)
	open := openIncidents(t, st)
	if _, err := m.Resolve(context.Background(), svc, open[0].ID, "alice", "False positive.", at); err != nil {
		t.Fatal(err)
	}
	at = feed(t, m, "x", at, time.Minute)
	if len(openIncidents(t, st)) != 0 {
		t.Fatal("one failure after manual resolve should not reopen")
	}
	feed(t, m, "x", at, time.Minute)
	if len(openIncidents(t, st)) != 1 {
		t.Fatal("threshold failures should reopen")
	}
	if got := s.got[1]; got.Kind != alert.IncidentResolved || got.Key != "incident:api" {
		t.Fatalf("manual resolve notification = %+v", got)
	}
}
