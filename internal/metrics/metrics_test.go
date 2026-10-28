package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MahyarNemati/status-page/internal/alert"
	"github.com/MahyarNemati/status-page/internal/check"
	"github.com/MahyarNemati/status-page/internal/slo"
)

func TestExposition(t *testing.T) {
	m := New()
	m.ObserveCheck("api", check.Result{Status: check.StatusDegraded, Latency: 120 * time.Millisecond, TLSExpiry: time.Unix(1900000000, 0)})
	m.ObserveCheck("db", check.Result{Status: check.StatusDown})
	m.SetOpenIncidents("db", 1)
	m.ObserveSLO(slo.Report{ServiceID: "api", Availability: 0.999, BudgetRemaining: 0.5,
		Burns: []slo.Burn{{Rule: "fast", LongWin: time.Hour, ShortWin: 5 * time.Minute, LongRate: 2, ShortRate: 3}}})
	m.ObserveAlert("ops", alert.IncidentOpened, alert.Sent)

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	for _, want := range []string{
		`statuspage_service_up{service="api"} 1`,
		`statuspage_service_up{service="db"} 0`,
		`statuspage_checks_total{service="api",status="degraded"} 1`,
		`statuspage_check_duration_seconds_count{service="api"} 1`,
		`statuspage_tls_cert_expiry_timestamp_seconds{service="api"} 1.9e+09`,
		`statuspage_incidents_open{service="db"} 1`,
		`statuspage_slo_error_budget_remaining_ratio{service="api"} 0.5`,
		`statuspage_slo_burn_rate{rule="fast",service="api",window="5m0s"} 3`,
		`statuspage_alerts_total{channel="ops",kind="incident.opened",outcome="sent"} 1`,
		`go_goroutines`,
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("missing %s", want)
		}
	}
}

func TestNilMetricsIsNoop(t *testing.T) {
	var m *Metrics
	m.ObserveCheck("a", check.Result{})
	m.SetOpenIncidents("a", 1)
	m.ObserveSLO(slo.Report{})
	m.ObserveAlert("a", alert.IncidentOpened, alert.Sent)
}
