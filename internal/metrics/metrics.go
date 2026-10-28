// Package metrics exposes Prometheus metrics for checks, incidents, SLOs and
// alert delivery on a dedicated registry.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/MahyarNemati/status-page/internal/alert"
	"github.com/MahyarNemati/status-page/internal/check"
	"github.com/MahyarNemati/status-page/internal/slo"
)

// Metrics holds every collector. A nil *Metrics is valid and records nothing.
type Metrics struct {
	reg *prometheus.Registry

	up           *prometheus.GaugeVec
	latency      *prometheus.HistogramVec
	checks       *prometheus.CounterVec
	tlsExpiry    *prometheus.GaugeVec
	openInc      *prometheus.GaugeVec
	budget       *prometheus.GaugeVec
	availability *prometheus.GaugeVec
	burnRate     *prometheus.GaugeVec
	alerts       *prometheus.CounterVec
}

// New registers all collectors, plus Go runtime and process collectors.
func New() *Metrics {
	m := &Metrics{
		reg: prometheus.NewRegistry(),
		up: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "statuspage_service_up",
			Help: "1 if the latest check counted as available, 0 otherwise.",
		}, []string{"service"}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "statuspage_check_duration_seconds",
			Help:    "Probe latency of the final attempt.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"service"}),
		checks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "statuspage_checks_total",
			Help: "Completed checks by outcome.",
		}, []string{"service", "status"}),
		tlsExpiry: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "statuspage_tls_cert_expiry_timestamp_seconds",
			Help: "NotAfter of the leaf certificate seen by HTTPS checks.",
		}, []string{"service"}),
		openInc: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "statuspage_incidents_open",
			Help: "Open incidents per service.",
		}, []string{"service"}),
		budget: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "statuspage_slo_error_budget_remaining_ratio",
			Help: "Unspent fraction of the error budget over the SLO window.",
		}, []string{"service"}),
		availability: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "statuspage_slo_availability_ratio",
			Help: "Availability over the SLO window.",
		}, []string{"service"}),
		burnRate: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "statuspage_slo_burn_rate",
			Help: "Error budget burn rate per rule and window.",
		}, []string{"service", "rule", "window"}),
		alerts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "statuspage_alerts_total",
			Help: "Alert delivery decisions by channel, kind and outcome.",
		}, []string{"channel", "kind", "outcome"}),
	}
	m.reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.up, m.latency, m.checks, m.tlsExpiry, m.openInc, m.budget, m.availability, m.burnRate, m.alerts,
	)
	return m
}

// Handler serves the registry in the Prometheus exposition format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{Registry: m.reg})
}

// Registry exposes the underlying registry for tests.
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// ObserveCheck records one completed check.
func (m *Metrics) ObserveCheck(service string, r check.Result) {
	if m == nil {
		return
	}
	v := 0.0
	if r.Status.Available() {
		v = 1
	}
	m.up.WithLabelValues(service).Set(v)
	m.latency.WithLabelValues(service).Observe(r.Latency.Seconds())
	m.checks.WithLabelValues(service, r.Status.String()).Inc()
	if !r.TLSExpiry.IsZero() {
		m.tlsExpiry.WithLabelValues(service).Set(float64(r.TLSExpiry.Unix()))
	}
}

// SetOpenIncidents records the number of open incidents for a service.
func (m *Metrics) SetOpenIncidents(service string, n int) {
	if m == nil {
		return
	}
	m.openInc.WithLabelValues(service).Set(float64(n))
}

// ObserveSLO records an SLO report.
func (m *Metrics) ObserveSLO(r slo.Report) {
	if m == nil {
		return
	}
	m.budget.WithLabelValues(r.ServiceID).Set(r.BudgetRemaining)
	if r.Availability >= 0 {
		m.availability.WithLabelValues(r.ServiceID).Set(r.Availability)
	}
	for _, b := range r.Burns {
		m.burnRate.WithLabelValues(r.ServiceID, b.Rule, b.LongWin.String()).Set(b.LongRate)
		m.burnRate.WithLabelValues(r.ServiceID, b.Rule, b.ShortWin.String()).Set(b.ShortRate)
	}
}

// ObserveAlert records an alert delivery decision.
func (m *Metrics) ObserveAlert(channel string, kind alert.Kind, outcome alert.Outcome) {
	if m == nil {
		return
	}
	m.alerts.WithLabelValues(channel, string(kind), string(outcome)).Inc()
}
