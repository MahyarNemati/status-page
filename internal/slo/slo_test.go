package slo

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/MahyarNemati/status-page/internal/config"
	"github.com/MahyarNemati/status-page/internal/store"
)

var now = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

// fakeStore answers RawCounts from a window-length -> counts table so a test
// can give the long and short windows different error rates.
type fakeStore struct {
	byWindow map[time.Duration]store.Counts
	calls    int
}

func (f *fakeStore) RawCounts(_ context.Context, _ string, from, to time.Time) (store.Counts, error) {
	f.calls++
	c, ok := f.byWindow[to.Sub(from)]
	if !ok {
		return store.Counts{}, nil
	}
	return c, nil
}

// Remaining Store methods are unused by this package.
func (f *fakeStore) SaveResult(context.Context, store.Result) error { return nil }
func (f *fakeStore) LatestResults(context.Context) (map[string]store.Result, error) {
	return nil, nil
}
func (f *fakeStore) RecentResults(context.Context, string, int) ([]store.Result, error) {
	return nil, nil
}
func (f *fakeStore) Rollups(context.Context, string, store.Resolution, time.Time, time.Time) ([]store.Bucket, error) {
	return nil, nil
}
func (f *fakeStore) CreateIncident(_ context.Context, in store.Incident) (store.Incident, error) {
	return in, nil
}
func (f *fakeStore) ResolveIncident(context.Context, int64, time.Time) (store.Incident, error) {
	return store.Incident{}, nil
}
func (f *fakeStore) GetIncident(context.Context, int64) (store.Incident, error) {
	return store.Incident{}, nil
}
func (f *fakeStore) ListIncidents(context.Context, store.IncidentFilter) ([]store.Incident, error) {
	return nil, nil
}
func (f *fakeStore) AddNote(_ context.Context, n store.Note) (store.Note, error) { return n, nil }
func (f *fakeStore) Prune(context.Context, time.Time, store.Retention) (store.PruneStats, error) {
	return store.PruneStats{}, nil
}
func (f *fakeStore) Ping(context.Context) error { return nil }
func (f *fakeStore) Close() error               { return nil }

// counts builds Counts with total probes and down probes.
func counts(total, down int64) store.Counts {
	return store.Counts{Total: total, Up: total - down}
}

func TestErrorBudget(t *testing.T) {
	for _, tc := range []struct {
		target float64
		want   float64
	}{
		{99.9, 0.001},
		{99, 0.01},
		{99.99, 0.0001},
		{100, 0},
	} {
		if got := ErrorBudget(tc.target); math.Abs(got-tc.want) > 1e-12 {
			t.Errorf("ErrorBudget(%v) = %v, want %v", tc.target, got, tc.want)
		}
	}
}

func TestEvaluateMeetingObjective(t *testing.T) {
	// 10000 probes, 5 down = 99.95% against a 99.9% target.
	st := &fakeStore{byWindow: map[time.Duration]store.Counts{30 * 24 * time.Hour: counts(10000, 5)}}
	s, err := Evaluate(context.Background(), st, "api", 99.9, 30*24*time.Hour, now)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if math.Abs(s.Availability-99.95) > 1e-9 {
		t.Errorf("Availability = %v, want 99.95", s.Availability)
	}
	if s.Breaching {
		t.Error("99.95% should not breach a 99.9% objective")
	}
	// 0.05% errors against a 0.1% budget is half the budget.
	if math.Abs(s.BudgetConsumed-0.5) > 1e-9 {
		t.Errorf("BudgetConsumed = %v, want 0.5", s.BudgetConsumed)
	}
	if math.Abs(s.BudgetRemaining-0.5) > 1e-9 {
		t.Errorf("BudgetRemaining = %v, want 0.5", s.BudgetRemaining)
	}
}

func TestEvaluateBreachingObjective(t *testing.T) {
	st := &fakeStore{byWindow: map[time.Duration]store.Counts{time.Hour: counts(1000, 50)}}
	s, err := Evaluate(context.Background(), st, "api", 99.9, time.Hour, now)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if !s.Breaching {
		t.Errorf("95%% availability must breach a 99.9%% objective (got %v)", s.Availability)
	}
	if s.BudgetRemaining != 0 {
		t.Errorf("BudgetRemaining = %v, want 0 once overspent", s.BudgetRemaining)
	}
	if s.BudgetConsumed <= 1 {
		t.Errorf("BudgetConsumed = %v, want > 1", s.BudgetConsumed)
	}
}

func TestDegradedCountsAsAvailable(t *testing.T) {
	// 100 probes: 90 up, 10 degraded, 0 down -> fully available.
	st := &fakeStore{byWindow: map[time.Duration]store.Counts{
		time.Hour: {Total: 100, Up: 90, Degraded: 10},
	}}
	s, err := Evaluate(context.Background(), st, "api", 99.9, time.Hour, now)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if math.Abs(s.Availability-100) > 1e-9 {
		t.Errorf("Availability = %v, want 100 (degraded still serves traffic)", s.Availability)
	}
}

func TestNoDataIsNotABreach(t *testing.T) {
	st := &fakeStore{byWindow: map[time.Duration]store.Counts{}}
	s, err := Evaluate(context.Background(), st, "fresh", 99.9, time.Hour, now)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if s.HasData() {
		t.Error("HasData should be false")
	}
	if s.Breaching {
		t.Error("a service with no probes must not be reported as breaching")
	}
	if s.Availability != -1 {
		t.Errorf("Availability = %v, want -1 to signal no data", s.Availability)
	}
	if s.BudgetRemaining != 1 {
		t.Errorf("BudgetRemaining = %v, want 1", s.BudgetRemaining)
	}
}

func TestEvaluateRejectsBadInput(t *testing.T) {
	st := &fakeStore{byWindow: map[time.Duration]store.Counts{}}
	if _, err := Evaluate(context.Background(), st, "api", 99.9, 0, now); err == nil {
		t.Error("zero window should error")
	}
	if _, err := Evaluate(context.Background(), st, "api", 0, time.Hour, now); err == nil {
		t.Error("zero target should error")
	}
	if _, err := Evaluate(context.Background(), st, "api", 101, time.Hour, now); err == nil {
		t.Error("target above 100 should error")
	}
}

func TestPerfectObjectiveHasNoBudget(t *testing.T) {
	st := &fakeStore{byWindow: map[time.Duration]store.Counts{time.Hour: counts(100, 1)}}
	s, err := Evaluate(context.Background(), st, "api", 100, time.Hour, now)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if !math.IsInf(s.BudgetConsumed, 1) {
		t.Errorf("BudgetConsumed = %v, want +Inf for a 100%% objective with a failure", s.BudgetConsumed)
	}
	if !s.Breaching {
		t.Error("any failure must breach a 100% objective")
	}
}

func fastThenSlow() []config.BurnRule {
	return []config.BurnRule{
		{Name: "fast", Long: config.Duration(time.Hour), Short: config.Duration(5 * time.Minute), Factor: 14.4, Severity: "critical"},
		{Name: "slow", Long: config.Duration(6 * time.Hour), Short: config.Duration(30 * time.Minute), Factor: 6, Severity: "warning"},
	}
}

func TestBurnFiresWhenBothWindowsAreHot(t *testing.T) {
	// 10% errors against a 0.1% budget is a burn rate of 100.
	st := &fakeStore{byWindow: map[time.Duration]store.Counts{
		time.Hour:      counts(1000, 100),
		5 * time.Minute: counts(100, 10),
	}}
	fired, err := EvaluateBurn(context.Background(), st, "api", 99.9, fastThenSlow(), now)
	if err != nil {
		t.Fatalf("EvaluateBurn: %v", err)
	}
	if len(fired) != 1 {
		t.Fatalf("fired %d rules, want exactly 1", len(fired))
	}
	if fired[0].Rule != "fast" || fired[0].Severity != "critical" {
		t.Errorf("fired = %+v, want the fast/critical rule first", fired[0])
	}
	if fired[0].Describe() == "" {
		t.Error("Describe should produce a message")
	}
}

func TestBurnDoesNotFireWhenShortWindowHasCooled(t *testing.T) {
	// The long window still remembers the outage, but the last 5 minutes are
	// clean. This is the case that stops a resolved incident paging forever.
	st := &fakeStore{byWindow: map[time.Duration]store.Counts{
		time.Hour:       counts(1000, 100),
		5 * time.Minute: counts(100, 0),
		6 * time.Hour:   counts(6000, 100),
		30 * time.Minute: counts(600, 0),
	}}
	fired, err := EvaluateBurn(context.Background(), st, "api", 99.9, fastThenSlow(), now)
	if err != nil {
		t.Fatalf("EvaluateBurn: %v", err)
	}
	if len(fired) != 0 {
		t.Fatalf("fired %+v, want nothing once the short window recovered", fired)
	}
}

func TestBurnDoesNotFireOnBriefSpike(t *testing.T) {
	// Short window is hot but the long window is still well inside budget.
	st := &fakeStore{byWindow: map[time.Duration]store.Counts{
		time.Hour:       counts(10000, 2),
		5 * time.Minute: counts(100, 2),
		6 * time.Hour:   counts(60000, 2),
		30 * time.Minute: counts(600, 2),
	}}
	fired, err := EvaluateBurn(context.Background(), st, "api", 99.9, fastThenSlow(), now)
	if err != nil {
		t.Fatalf("EvaluateBurn: %v", err)
	}
	if len(fired) != 0 {
		t.Fatalf("fired %+v, want nothing for a brief spike", fired)
	}
}

func TestBurnReturnsOnlyMostSevereMatch(t *testing.T) {
	// Both rules would match; only the first should be returned so one
	// outage produces one page.
	st := &fakeStore{byWindow: map[time.Duration]store.Counts{
		time.Hour:        counts(1000, 500),
		5 * time.Minute:  counts(100, 50),
		6 * time.Hour:    counts(6000, 3000),
		30 * time.Minute: counts(600, 300),
	}}
	fired, err := EvaluateBurn(context.Background(), st, "api", 99.9, fastThenSlow(), now)
	if err != nil {
		t.Fatalf("EvaluateBurn: %v", err)
	}
	if len(fired) != 1 || fired[0].Rule != "fast" {
		t.Fatalf("fired %+v, want only the fast rule", fired)
	}
}

func TestBurnIgnoresEmptyWindows(t *testing.T) {
	st := &fakeStore{byWindow: map[time.Duration]store.Counts{}}
	fired, err := EvaluateBurn(context.Background(), st, "api", 99.9, fastThenSlow(), now)
	if err != nil {
		t.Fatalf("EvaluateBurn: %v", err)
	}
	if len(fired) != 0 {
		t.Errorf("fired %+v, want nothing with no data", fired)
	}
}

func TestBurnRejectsInvalidRules(t *testing.T) {
	st := &fakeStore{byWindow: map[time.Duration]store.Counts{}}
	bad := [][]config.BurnRule{
		{{Name: "no-factor", Long: config.Duration(time.Hour), Short: config.Duration(time.Minute)}},
		{{Name: "no-long", Short: config.Duration(time.Minute), Factor: 2}},
		{{Name: "inverted", Long: config.Duration(time.Minute), Short: config.Duration(time.Hour), Factor: 2}},
	}
	for _, rules := range bad {
		if _, err := EvaluateBurn(context.Background(), st, "api", 99.9, rules, now); err == nil {
			t.Errorf("rule %q should have been rejected", rules[0].Name)
		}
	}
}

func TestShortCircuitAvoidsNeedlessShortWindowQuery(t *testing.T) {
	// Long window is cold, so the short window must not be queried at all.
	st := &fakeStore{byWindow: map[time.Duration]store.Counts{
		time.Hour:     counts(10000, 0),
		6 * time.Hour: counts(60000, 0),
	}}
	if _, err := EvaluateBurn(context.Background(), st, "api", 99.9, fastThenSlow(), now); err != nil {
		t.Fatalf("EvaluateBurn: %v", err)
	}
	// Two rules, one long-window query each.
	if st.calls != 2 {
		t.Errorf("RawCounts calls = %d, want 2 (short windows skipped)", st.calls)
	}
}
