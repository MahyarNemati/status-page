package web

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/MahyarNemati/status-page/internal/check"
	"github.com/MahyarNemati/status-page/internal/clock"
	"github.com/MahyarNemati/status-page/internal/config"
	"github.com/MahyarNemati/status-page/internal/incident"
	"github.com/MahyarNemati/status-page/internal/metrics"
	"github.com/MahyarNemati/status-page/internal/slo"
	"github.com/MahyarNemati/status-page/internal/store"
)

const token = "test-admin-token-0123456789"

type fixture struct {
	srv   *httptest.Server
	store store.Store
	clock *clock.Fake
	mgr   *incident.Manager
	cfg   *config.Config
}

func newFixture(t *testing.T, adminToken string) *fixture {
	t.Helper()
	cfg, err := config.Parse([]byte(`
server:
  title: Acme Status
  page_cache_ttl: 1ns
services:
  - id: api
    name: Public API
    group: Core
    type: http
    target: https://api.example.com/health
    slo: {target: 99.5}
  - id: db
    name: Database
    group: Core
    type: tcp
    target: db.internal:5432
  - id: secret
    type: tcp
    target: 10.0.0.1:22
    hidden: true
`), func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	cfg.Server.AdminToken = adminToken

	st, err := store.Open(context.Background(), "sqlite", "file:"+filepath.Join(t.TempDir(), "web.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	fc := clock.NewFake(time.Date(2026, 9, 30, 15, 0, 0, 0, time.UTC))
	mgr := incident.NewManager(st, nil, incident.Policy{FlapWindow: time.Minute, FlapThreshold: 100}, "", nil)
	ev := slo.New(slo.Options{Store: st, Clock: fc, Services: cfg.Services, Policy: cfg.SLO})

	s, err := New(Deps{Config: cfg, Store: st, Incidents: mgr, SLO: ev, Metrics: metrics.New(), Clock: fc})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	f := &fixture{srv: ts, store: st, clock: fc, mgr: mgr, cfg: cfg}
	f.seed(t, ev)
	return f
}

func (f *fixture) seed(t *testing.T, ev *slo.Evaluator) {
	t.Helper()
	ctx := context.Background()
	now := f.clock.Now()
	save := func(id string, at time.Time, s check.Status) {
		if err := f.store.SaveResult(ctx, store.Result{ServiceID: id, At: at, Status: s, Latency: 42 * time.Millisecond}); err != nil {
			t.Fatal(err)
		}
	}
	// api: healthy for 10 days except a bad day 3 days ago.
	for d := 10; d >= 0; d-- {
		day := now.AddDate(0, 0, -d).Add(-time.Hour)
		for i := range 10 {
			s := check.StatusUp
			if d == 3 && i < 5 {
				s = check.StatusDown
			}
			save("api", day.Add(time.Duration(i)*time.Minute), s)
		}
	}
	save("db", now.Add(-time.Minute), check.StatusDown)
	save("secret", now.Add(-time.Minute), check.StatusUp)
	if err := ev.Evaluate(ctx); err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) do(t *testing.T, method, path, auth, body string) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequest(method, f.srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if auth != "" {
		req.Header.Set("Authorization", "Bearer "+auth)
	}
	resp, err := f.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func TestStatusPageHTML(t *testing.T) {
	f := newFixture(t, "")
	resp, body := f.do(t, "GET", "/", "", "")
	if resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/html") {
		t.Fatalf("status %d %s", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	for _, want := range []string{"Acme Status", "Partial outage", "Public API", "Database", "Core", "Outage", "No incidents in the last 14 days"} {
		if !strings.Contains(body, want) {
			t.Errorf("page missing %q", want)
		}
	}
	if strings.Contains(body, "<script") || strings.Contains(body, "secret") {
		t.Fatal("page must not contain scripts or hidden services")
	}
	if n := strings.Count(body, `class="bar bar-`); n != 2*90 {
		t.Fatalf("rendered %d bars, want 180", n)
	}
	if !strings.Contains(body, "bar bar-major") || !strings.Contains(body, "bar bar-none") {
		t.Fatal("expected a major day and no-data days")
	}
	if csp := resp.Header.Get("Content-Security-Policy"); !strings.Contains(csp, "default-src 'none'") {
		t.Fatalf("csp = %q", csp)
	}

	resp, _ = f.do(t, "GET", "/static/style.css", "", "")
	if resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/css") {
		t.Fatalf("stylesheet: %d %s", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
}

func TestStatusJSON(t *testing.T) {
	f := newFixture(t, "")
	resp, body := f.do(t, "GET", "/api/v1/status", "", "")
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	var page struct {
		Status   string `json:"status"`
		Headline string `json:"headline"`
		Groups   []struct {
			Services []struct {
				ID     string  `json:"id"`
				Status string  `json:"status"`
				Uptime float64 `json:"uptime"`
				Days   []any   `json:"days"`
			} `json:"services"`
		} `json:"groups"`
	}
	if err := json.Unmarshal([]byte(body), &page); err != nil {
		t.Fatal(err)
	}
	svcs := page.Groups[0].Services
	if page.Status != "outage" || len(svcs) != 2 || svcs[0].ID != "api" || svcs[0].Status != "operational" || svcs[1].Status != "outage" {
		t.Fatalf("page = %+v", page)
	}
	if want := 105.0 / 110; svcs[0].Uptime != want || svcs[0].Days != nil {
		t.Fatalf("api uptime = %v (want %v), days=%v", svcs[0].Uptime, want, svcs[0].Days)
	}

	resp, body = f.do(t, "GET", "/api/v1/services/api", "", "")
	if resp.StatusCode != 200 || !strings.Contains(body, `"recent_results"`) || !strings.Contains(body, `"days"`) || !strings.Contains(body, `"slo"`) {
		t.Fatalf("service detail: %d %s", resp.StatusCode, body[:min(len(body), 300)])
	}
	if resp, _ := f.do(t, "GET", "/api/v1/services/secret", "", ""); resp.StatusCode != 404 {
		t.Fatal("hidden service must 404")
	}

	resp, body = f.do(t, "GET", "/api/v1/services/api/uptime?days=30", "", "")
	if resp.StatusCode != 200 || !strings.Contains(body, `"resolution": "day"`) {
		t.Fatalf("uptime: %d %s", resp.StatusCode, body)
	}
	for _, bad := range []string{"?days=0", "?days=x", "?days=200&resolution=hour"} {
		if resp, _ := f.do(t, "GET", "/api/v1/services/api/uptime"+bad, "", ""); resp.StatusCode != 400 {
			t.Fatalf("uptime%s: status %d", bad, resp.StatusCode)
		}
	}

	resp, body = f.do(t, "GET", "/api/v1/slo", "", "")
	if resp.StatusCode != 200 || !strings.Contains(body, `"service_id": "api"`) {
		t.Fatalf("slo: %s", body)
	}
	for path, code := range map[string]int{"/healthz": 200, "/readyz": 200, "/metrics": 200, "/nope": 404} {
		if resp, _ := f.do(t, "GET", path, "", ""); resp.StatusCode != code {
			t.Fatalf("%s: %d", path, resp.StatusCode)
		}
	}
}

func TestAdminAuth(t *testing.T) {
	disabled := newFixture(t, "")
	if resp, _ := disabled.do(t, "GET", "/api/v1/admin/services", token, ""); resp.StatusCode != 404 {
		t.Fatalf("disabled admin: %d", resp.StatusCode)
	}
	f := newFixture(t, token)
	if resp, _ := f.do(t, "GET", "/api/v1/admin/services", "", ""); resp.StatusCode != 401 || resp.Header.Get("WWW-Authenticate") == "" {
		t.Fatalf("missing token: %d", resp.StatusCode)
	}
	if resp, _ := f.do(t, "GET", "/api/v1/admin/services", "wrong", ""); resp.StatusCode != 401 {
		t.Fatalf("wrong token: %d", resp.StatusCode)
	}
	resp, body := f.do(t, "GET", "/api/v1/admin/services", token, "")
	if resp.StatusCode != 200 || !strings.Contains(body, `"secret"`) {
		t.Fatalf("admin services should include hidden ones: %d", resp.StatusCode)
	}
}

func TestAdminIncidentFlow(t *testing.T) {
	f := newFixture(t, token)

	for body, code := range map[string]int{
		`{"service_id":"nope","title":"x","severity":"major"}`: 422,
		`{"service_id":"api","title":"","severity":"major"}`:   422,
		`{"service_id":"api","unknown":1}`:                     400,
		`not json`:                                             400,
	} {
		if resp, got := f.do(t, "POST", "/api/v1/admin/incidents", token, body); resp.StatusCode != code {
			t.Fatalf("%s: %d %s", body, resp.StatusCode, got)
		}
	}

	resp, body := f.do(t, "POST", "/api/v1/admin/incidents", token,
		`{"service_id":"api","title":"Elevated error rates","severity":"major","author":"alice","note":"Investigating."}`)
	if resp.StatusCode != 201 {
		t.Fatalf("declare: %d %s", resp.StatusCode, body)
	}
	var in store.Incident
	if err := json.Unmarshal([]byte(body), &in); err != nil {
		t.Fatal(err)
	}

	f.clock.Advance(10 * time.Minute)
	if resp, body := f.do(t, "POST", "/api/v1/admin/incidents/"+itoa(in.ID)+"/notes", token, `{"body":"Fix deployed."}`); resp.StatusCode != 201 || !strings.Contains(body, `"author": "admin"`) {
		t.Fatalf("note: %d %s", resp.StatusCode, body)
	}
	if resp, _ := f.do(t, "POST", "/api/v1/admin/incidents/999/notes", token, `{"body":"x"}`); resp.StatusCode != 404 {
		t.Fatalf("note on missing incident: %d", resp.StatusCode)
	}

	_, page := f.do(t, "GET", "/", "", "")
	if !strings.Contains(page, "Active incidents") || !strings.Contains(page, "Elevated error rates") || !strings.Contains(page, "Fix deployed.") {
		t.Fatal("active incident not rendered")
	}
	resp, detail := f.do(t, "GET", "/incidents/"+itoa(in.ID), "", "")
	if resp.StatusCode != 200 || !strings.Contains(detail, "Investigating.") || !strings.Contains(detail, "ongoing for 10m") {
		t.Fatalf("incident page: %d", resp.StatusCode)
	}

	f.clock.Advance(20 * time.Minute)
	resp, body = f.do(t, "POST", "/api/v1/admin/incidents/"+itoa(in.ID)+"/resolve", token, "")
	if resp.StatusCode != 200 || !strings.Contains(body, `"state": "resolved"`) {
		t.Fatalf("resolve: %d %s", resp.StatusCode, body)
	}
	if resp, _ := f.do(t, "POST", "/api/v1/admin/incidents/999/resolve", token, ""); resp.StatusCode != 404 {
		t.Fatalf("resolve missing: %d", resp.StatusCode)
	}

	_, page = f.do(t, "GET", "/", "", "")
	if strings.Contains(page, "Active incidents") || !strings.Contains(page, "lasted 30m") {
		t.Fatal("resolved incident should move to past incidents")
	}
	_, list := f.do(t, "GET", "/api/v1/incidents?state=resolved", "", "")
	if !strings.Contains(list, "Elevated error rates") {
		t.Fatalf("incident list: %s", list)
	}
	if resp, _ := f.do(t, "GET", "/api/v1/incidents?state=bogus", "", ""); resp.StatusCode != 400 {
		t.Fatal("invalid state filter accepted")
	}
}

func TestHiddenIncidentsAreNotPublic(t *testing.T) {
	f := newFixture(t, token)
	secret, _ := f.cfg.Service("secret")
	in, err := f.mgr.Declare(context.Background(), secret, "Bastion down", "major", "ops", "", f.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	if resp, _ := f.do(t, "GET", "/api/v1/incidents/"+itoa(in.ID), "", ""); resp.StatusCode != 404 {
		t.Fatal("hidden incident exposed via API")
	}
	if _, body := f.do(t, "GET", "/api/v1/incidents", "", ""); strings.Contains(body, "Bastion") {
		t.Fatal("hidden incident listed")
	}
	if _, body := f.do(t, "GET", "/", "", ""); strings.Contains(body, "Bastion") {
		t.Fatal("hidden incident rendered")
	}
}

func TestHumanize(t *testing.T) {
	for d, want := range map[time.Duration]string{
		30 * time.Second:            "under a minute",
		45 * time.Minute:            "45m",
		2 * time.Hour:               "2h",
		2*time.Hour + 5*time.Minute: "2h 5m",
		50 * time.Hour:              "2d 2h",
		72 * time.Hour:              "3d",
	} {
		if got := humanizeDuration(d); got != want {
			t.Errorf("humanizeDuration(%v) = %q, want %q", d, got, want)
		}
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }
