package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/MahyarNemati/status-page/internal/check"
)

func openSQLite(t *testing.T) *SQL {
	t.Helper()
	s, err := Open(context.Background(), "sqlite", "file:"+filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestSQLite(t *testing.T) {
	runSuite(t, func(t *testing.T) Store { return openSQLite(t) })
}

// The Postgres suite runs only when a disposable database is provided, e.g.
// STATUSPAGE_TEST_POSTGRES_DSN=postgres://postgres:postgres@localhost:5432/statuspage_test
func TestPostgres(t *testing.T) {
	dsn := os.Getenv("STATUSPAGE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("STATUSPAGE_TEST_POSTGRES_DSN not set")
	}
	runSuite(t, func(t *testing.T) Store {
		s, err := Open(context.Background(), "postgres", dsn)
		if err != nil {
			t.Fatal(err)
		}
		for _, tbl := range []string{"incident_notes", "incidents", "rollups", "check_results"} {
			if _, err := s.db.Exec("DELETE FROM " + tbl); err != nil {
				t.Fatal(err)
			}
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	})
}

func TestReopenKeepsSchema(t *testing.T) {
	dsn := "file:" + filepath.Join(t.TempDir(), "reopen.db")
	ctx := context.Background()
	s, err := Open(ctx, "sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SaveResult(ctx, Result{ServiceID: "a", At: time.Now(), Status: check.StatusUp}); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	s, err = Open(ctx, "sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.LatestResults(ctx)
	if err != nil || len(got) != 1 {
		t.Fatalf("latest after reopen = %v, %v", got, err)
	}
}

func TestRebind(t *testing.T) {
	got := postgresDialect.rebind("a = ? AND b = ?")
	if got != "a = $1 AND b = $2" {
		t.Fatalf("rebind = %q", got)
	}
	if sqliteDialect.rebind("a = ?") != "a = ?" {
		t.Fatal("sqlite must keep ? placeholders")
	}
}

func runSuite(t *testing.T, open func(*testing.T) Store) {
	base := time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)
	ctx := context.Background()

	t.Run("results and rollups", func(t *testing.T) {
		s := open(t)
		seq := []struct {
			at      time.Duration
			status  check.Status
			latency time.Duration
		}{
			{10 * time.Minute, check.StatusUp, 100 * time.Millisecond},
			{20 * time.Minute, check.StatusDown, 300 * time.Millisecond},
			{30 * time.Minute, check.StatusDegraded, 200 * time.Millisecond},
			{90 * time.Minute, check.StatusUp, 50 * time.Millisecond},
			{25 * time.Hour, check.StatusUp, 10 * time.Millisecond},
		}
		for _, r := range seq {
			if err := s.SaveResult(ctx, Result{ServiceID: "api", At: base.Add(r.at), Status: r.status, Latency: r.latency, Message: "m"}); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.SaveResult(ctx, Result{ServiceID: "other", At: base, Status: check.StatusDown}); err != nil {
			t.Fatal(err)
		}

		hours, err := s.Rollups(ctx, "api", Hourly, base, base.Add(48*time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		if len(hours) != 3 {
			t.Fatalf("hour buckets = %d, want 3", len(hours))
		}
		h0 := hours[0]
		if h0.Total != 3 || h0.Up != 1 || h0.Degraded != 1 || h0.Down() != 1 {
			t.Fatalf("first hour = %+v", h0)
		}
		if h0.AvgLatency != 200*time.Millisecond || h0.MaxLatency != 300*time.Millisecond {
			t.Fatalf("latency avg=%v max=%v", h0.AvgLatency, h0.MaxLatency)
		}

		days, err := s.Rollups(ctx, "api", Daily, base, base.Add(48*time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		if len(days) != 2 || days[0].Total != 4 || days[1].Total != 1 || !days[1].Start.Equal(base.Add(24*time.Hour)) {
			t.Fatalf("days = %+v", days)
		}

		c, err := s.RawCounts(ctx, "api", base, base.Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		if c.Total != 3 || c.Available() != 2 || c.Ratio() < 0.66 || c.Ratio() > 0.67 {
			t.Fatalf("raw counts = %+v", c)
		}

		latest, err := s.LatestResults(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(latest) != 2 || !latest["api"].At.Equal(base.Add(25*time.Hour)) || latest["other"].Status != check.StatusDown {
			t.Fatalf("latest = %+v", latest)
		}

		recent, err := s.RecentResults(ctx, "api", 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(recent) != 2 || !recent[0].At.After(recent[1].At) || recent[0].Latency != 10*time.Millisecond {
			t.Fatalf("recent = %+v", recent)
		}
	})

	t.Run("prune by resolution", func(t *testing.T) {
		s := open(t)
		for d := 0; d < 10; d++ {
			if err := s.SaveResult(ctx, Result{ServiceID: "api", At: base.Add(time.Duration(d) * 24 * time.Hour), Status: check.StatusUp}); err != nil {
				t.Fatal(err)
			}
		}
		now := base.Add(10 * 24 * time.Hour)
		st, err := s.Prune(ctx, now, Retention{Raw: 2 * 24 * time.Hour, Hourly: 5 * 24 * time.Hour, Daily: 8 * 24 * time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		if st.Raw != 8 || st.Hourly != 5 || st.Daily != 2 {
			t.Fatalf("prune stats = %+v", st)
		}
		days, _ := s.Rollups(ctx, "api", Daily, base, now)
		if len(days) != 8 {
			t.Fatalf("days left = %d", len(days))
		}
	})

	t.Run("incidents and notes", func(t *testing.T) {
		s := open(t)
		in, err := s.CreateIncident(ctx, Incident{ServiceID: "api", Title: "API down", Severity: "major", Auto: true, StartedAt: base})
		if err != nil {
			t.Fatal(err)
		}
		if in.ID == 0 || in.State != IncidentOpen {
			t.Fatalf("created = %+v", in)
		}
		if _, err := s.CreateIncident(ctx, Incident{ServiceID: "db", Title: "Maintenance", Severity: "minor", StartedAt: base.Add(time.Hour)}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.AddNote(ctx, Note{IncidentID: in.ID, Author: "oncall", Body: "investigating", CreatedAt: base.Add(time.Minute)}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.AddNote(ctx, Note{IncidentID: 9999, Body: "x", CreatedAt: base}); !errors.Is(err, ErrNotFound) {
			t.Fatalf("note on missing incident: %v", err)
		}

		resolved, err := s.ResolveIncident(ctx, in.ID, base.Add(30*time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		if resolved.State != IncidentResolved || resolved.Duration(time.Time{}) != 30*time.Minute || len(resolved.Notes) != 1 {
			t.Fatalf("resolved = %+v", resolved)
		}
		again, err := s.ResolveIncident(ctx, in.ID, base.Add(time.Hour))
		if err != nil || !again.ResolvedAt.Equal(*resolved.ResolvedAt) {
			t.Fatalf("second resolve changed record: %+v %v", again, err)
		}

		open, err := s.ListIncidents(ctx, IncidentFilter{State: IncidentOpen})
		if err != nil || len(open) != 1 || open[0].ServiceID != "db" {
			t.Fatalf("open = %+v, %v", open, err)
		}
		all, err := s.ListIncidents(ctx, IncidentFilter{Limit: 10})
		if err != nil || len(all) != 2 || all[0].ServiceID != "db" {
			t.Fatalf("all = %+v, %v", all, err)
		}
		forAPI, _ := s.ListIncidents(ctx, IncidentFilter{ServiceID: "api"})
		if len(forAPI) != 1 || !forAPI[0].Auto {
			t.Fatalf("api incidents = %+v", forAPI)
		}
		if _, err := s.GetIncident(ctx, 424242); !errors.Is(err, ErrNotFound) {
			t.Fatalf("missing incident err = %v", err)
		}
	})
}
