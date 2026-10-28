package slo

import (
	"context"
	"math"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/MahyarNemati/status-page/internal/alert"
	"github.com/MahyarNemati/status-page/internal/check"
	"github.com/MahyarNemati/status-page/internal/clock"
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

func record(t *testing.T, st store.Store, id string, from time.Time, pattern string) time.Time {
	t.Helper()
	at := from
	for _, c := range pattern {
		s := check.StatusUp
		if c == 'x' {
			s = check.StatusDown
		}
		if err := st.SaveResult(context.Background(), store.Result{ServiceID: id, At: at, Status: s}); err != nil {
			t.Fatal(err)
		}
		at = at.Add(time.Minute)
	}
	return at
}

func repeat(c string, n int) string {
	out := make([]byte, n)
	for i := range out {
		out[i] = c[0]
	}
	return string(out)
}

func TestBudgetAndBurnRate(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, "sqlite", "file:"+filepath.Join(t.TempDir(), "slo.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	start := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	fc := clock.NewFake(start)
	s := &sink{}
	var observed []Report
	e := New(Options{
		Store: st, Clock: fc, Publisher: s,
		Services: []config.Service{
			{ID: "api", Name: "API", SLO: &config.SLO{Target: 99}},
			{ID: "nobudget", Name: "No SLO"},
		},
		Policy: config.SLOPolicy{
			Window: config.Duration(30 * 24 * time.Hour),
			BurnRules: []config.BurnRule{
				{Name: "fast", Long: config.Duration(time.Hour), Short: config.Duration(5 * time.Minute), Factor: 14.4, Severity: "critical"},
			},
		},
		Observer: func(r Report) { observed = append(observed, r) },
	})

	// 120 healthy minutes then 10 failing minutes.
	at := record(t, st, "api", start, repeat("u", 120)+repeat("x", 10))
	fc.Set(at.Add(-time.Minute))
	if err := e.Evaluate(ctx); err != nil {
		t.Fatal(err)
	}
	r, ok := e.Report("api")
	if !ok {
		t.Fatal("no report")
	}
	if _, ok := e.Report("nobudget"); ok || len(e.Reports()) != 1 {
		t.Fatal("services without an SLO must be skipped")
	}
	if r.Counts.Total != 130 || r.Counts.Down() != 10 {
		t.Fatalf("counts = %+v", r.Counts)
	}
	if want := 1 - 10/1.3; math.Abs(r.BudgetRemaining-want) > 1e-9 {
		t.Fatalf("budget remaining = %v, want %v", r.BudgetRemaining, want)
	}
	b := r.Burns[0]
	if math.Abs(b.LongRate-1000.0/60) > 1e-9 || math.Abs(b.ShortRate-100) > 1e-9 || !b.Firing {
		t.Fatalf("burn = %+v", b)
	}
	if len(s.got) != 1 || s.got[0].Kind != alert.BurnRateFiring || s.got[0].Key != "slo:api:fast" || s.got[0].Severity != "critical" {
		t.Fatalf("notifications = %+v", s.got)
	}

	// Re-evaluating without change must not notify again.
	if err := e.Evaluate(ctx); err != nil {
		t.Fatal(err)
	}
	if len(s.got) != 1 {
		t.Fatal("unchanged burn state re-notified")
	}

	// A healthy hour clears the alert.
	at = record(t, st, "api", at, repeat("u", 60))
	fc.Set(at.Add(-time.Minute))
	if err := e.Evaluate(ctx); err != nil {
		t.Fatal(err)
	}
	if len(s.got) != 2 || s.got[1].Kind != alert.BurnRateResolved {
		t.Fatalf("notifications = %+v", s.got)
	}
	if len(observed) != 3 {
		t.Fatalf("observer calls = %d", len(observed))
	}
}

func TestWindowExcludesOldData(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, "sqlite", "file:"+filepath.Join(t.TempDir(), "slo.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	record(t, st, "api", start, repeat("x", 30))
	recent := start.Add(40 * 24 * time.Hour)
	end := record(t, st, "api", recent, repeat("u", 30))

	fc := clock.NewFake(end)
	e := New(Options{
		Store: st, Clock: fc,
		Services: []config.Service{{ID: "api", SLO: &config.SLO{Target: 99.9}}},
		Policy:   config.SLOPolicy{Window: config.Duration(30 * 24 * time.Hour)},
	})
	if err := e.Evaluate(ctx); err != nil {
		t.Fatal(err)
	}
	r, _ := e.Report("api")
	if r.Counts.Total != 30 || r.Counts.Down() != 0 || r.BudgetRemaining != 1 || r.Availability != 1 {
		t.Fatalf("report = %+v", r)
	}
}

func TestNoDataReport(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, "sqlite", "file:"+filepath.Join(t.TempDir(), "slo.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	e := New(Options{
		Store: st, Clock: clock.NewFake(time.Now()),
		Services: []config.Service{{ID: "api", SLO: &config.SLO{Target: 99.9}}},
		Policy:   config.SLOPolicy{Window: config.Duration(30 * 24 * time.Hour)},
	})
	if err := e.Evaluate(ctx); err != nil {
		t.Fatal(err)
	}
	r, _ := e.Report("api")
	if r.Availability != -1 || r.BudgetRemaining != 1 {
		t.Fatalf("report = %+v", r)
	}
}
