package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MahyarNemati/status-page/internal/alert"
	"github.com/MahyarNemati/status-page/internal/clock"
	"github.com/MahyarNemati/status-page/internal/config"
	"github.com/MahyarNemati/status-page/internal/monitor"
)

// TestEndToEnd drives the whole service with a fake clock: a real HTTP
// target fails, an incident opens and alerts, the target recovers and the
// recovery notice follows.
func TestEndToEnd(t *testing.T) {
	var healthy atomic.Bool
	healthy.Store(true)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if !healthy.Load() {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		_, _ = io.WriteString(w, "ok")
	}))
	defer target.Close()

	alerts := make(chan alert.Notification, 8)
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var n alert.Notification
		if err := json.NewDecoder(r.Body).Decode(&n); err != nil {
			t.Errorf("decode alert: %v", err)
		}
		alerts <- n
	}))
	defer hook.Close()

	doc := fmt.Sprintf(`
server:
  listen: 127.0.0.1:0
  public_url: https://status.example.com
  page_cache_ttl: 1ns
storage:
  dsn: file:%s
scheduler:
  workers: 2
  jitter: 0
incidents:
  flap_threshold: 10
slo:
  evaluate_every: 1h
alerts:
  channels:
    - name: hook
      type: webhook
      url: %s
services:
  - id: web
    name: Website
    type: http
    target: %s
    interval: 30s
    timeout: 5s
    failure_threshold: 2
    recovery_threshold: 2
    slo: {target: 99.9}
`, filepath.Join(t.TempDir(), "e2e.db"), hook.URL, target.URL)
	cfg, err := config.Parse([]byte(doc), func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}

	fc := clock.NewFake(time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC))
	observed := make(chan monitor.Observation, 8)
	ctx, cancel := context.WithCancel(context.Background())
	a, err := New(ctx, cfg, Options{
		Clock:            fc,
		Logger:           slog.New(slog.DiscardHandler),
		AfterObservation: func(o monitor.Observation) { observed <- o },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	// Scheduler, SLO, prune and gauge loops each hold one timer when idle.
	const idleTimers = 4
	step := func(wantStatus string) {
		t.Helper()
		select {
		case o := <-observed:
			if o.Result.Status.String() != wantStatus {
				t.Fatalf("observation %s (%s), want %s", o.Result.Status, o.Result.Message, wantStatus)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for a check")
		}
		fc.BlockUntil(idleTimers)
	}
	expectAlert := func(kind alert.Kind) alert.Notification {
		t.Helper()
		select {
		case n := <-alerts:
			if n.Kind != kind {
				t.Fatalf("alert kind %s, want %s", n.Kind, kind)
			}
			return n
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for %s", kind)
		}
		return alert.Notification{}
	}

	step("up")
	healthy.Store(false)
	fc.Advance(30 * time.Second)
	step("down")
	select {
	case n := <-alerts:
		t.Fatalf("alert after a single failure: %+v", n)
	default:
	}
	fc.Advance(30 * time.Second)
	step("down")
	opened := expectAlert(alert.IncidentOpened)
	if opened.ServiceID != "web" || opened.URL != "https://status.example.com" || !strings.Contains(opened.Message, "500") {
		t.Fatalf("opened alert = %+v", opened)
	}

	healthy.Store(true)
	fc.Advance(30 * time.Second)
	step("up")
	fc.Advance(30 * time.Second)
	step("up")
	resolved := expectAlert(alert.IncidentResolved)
	if !strings.Contains(resolved.Message, "recovered after 1m30s") {
		t.Fatalf("resolved alert = %+v", resolved)
	}

	srv := httptest.NewServer(a.Handler())
	defer srv.Close()
	page := get(t, srv.URL+"/")
	for _, want := range []string{"All systems operational", "Website is down", "lasted 2m"} {
		if !strings.Contains(page, want) {
			t.Errorf("status page missing %q", want)
		}
	}
	metrics := get(t, srv.URL+"/metrics")
	for _, want := range []string{
		`statuspage_checks_total{service="web",status="down"} 2`,
		`statuspage_checks_total{service="web",status="up"} 3`,
		`statuspage_alerts_total{channel="hook",kind="incident.opened",outcome="sent"} 1`,
	} {
		if !strings.Contains(metrics, want) {
			t.Errorf("metrics missing %s", want)
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func get(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}
