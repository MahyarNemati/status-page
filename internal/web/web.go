// Package web serves the public status page, a read-only JSON API and a
// token-guarded admin API for declaring incidents by hand.
package web

import (
	"context"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/MahyarNemati/status-page/internal/check"
	"github.com/MahyarNemati/status-page/internal/clock"
	"github.com/MahyarNemati/status-page/internal/config"
	"github.com/MahyarNemati/status-page/internal/slo"
	"github.com/MahyarNemati/status-page/internal/store"
)

//go:embed templates/*.html
var templateFS embed.FS

// Server serves the status page and APIs.
type Server struct {
	cfg  *config.Config
	st   store.Store
	clk  clock.Clock
	log  *slog.Logger
	tmpl *template.Template

	// cache holds the rendered summary for PageCacheTTL so a link on the
	// front page of a news aggregator does not turn into a self-inflicted
	// outage of the thing reporting the outage.
	mu       sync.RWMutex
	cached   *Summary
	cachedAt time.Time
}

// New builds a Server.
func New(cfg *config.Config, st store.Store, clk clock.Clock, log *slog.Logger) (*Server, error) {
	if cfg == nil || st == nil {
		return nil, errors.New("web: cfg and store are required")
	}
	if clk == nil {
		clk = clock.Real()
	}
	if log == nil {
		log = slog.Default()
	}
	tmpl, err := template.New("").Funcs(funcs()).ParseFS(templateFS, "templates/*.html")
	if err != nil {
		return nil, fmt.Errorf("web: parse templates: %w", err)
	}
	return &Server{cfg: cfg, st: st, clk: clk, log: log, tmpl: tmpl}, nil
}

// Handler returns the router.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /{$}", s.handlePage)
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /readyz", s.handleReady)
	mux.HandleFunc("GET /metrics", s.handleMetrics)

	mux.HandleFunc("GET /api/v1/status", s.handleStatusJSON)
	mux.HandleFunc("GET /api/v1/services/{id}/history", s.handleHistory)
	mux.HandleFunc("GET /api/v1/incidents", s.handleListIncidents)
	mux.HandleFunc("GET /api/v1/incidents/{id}", s.handleGetIncident)

	// Admin routes mutate state and are guarded by a bearer token.
	mux.Handle("POST /api/v1/incidents", s.admin(http.HandlerFunc(s.handleCreateIncident)))
	mux.Handle("POST /api/v1/incidents/{id}/resolve", s.admin(http.HandlerFunc(s.handleResolveIncident)))
	mux.Handle("POST /api/v1/incidents/{id}/notes", s.admin(http.HandlerFunc(s.handleAddNote)))

	return s.recover(s.securityHeaders(mux))
}

// ---------- middleware ----------

// admin requires a bearer token equal to the configured admin token.
func (s *Server) admin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		want := s.cfg.Server.AdminToken
		if want == "" {
			// Refuse rather than allowing: an unset token must not mean
			// "anyone may declare an incident".
			writeError(w, http.StatusForbidden, "admin API is disabled: no admin_token configured")
			return
		}
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		// Constant-time compare so the token cannot be recovered by timing.
		if subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="status-page"`)
			writeError(w, http.StatusUnauthorized, "invalid admin token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("X-Frame-Options", "DENY")
		// The page ships no scripts and no external assets, so the policy can
		// be this tight.
		h.Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; img-src 'self' data:; base-uri 'none'; form-action 'none'")
		next.ServeHTTP(w, r)
	})
}

func (s *Server) recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				s.log.Error("panic serving request", "path", r.URL.Path, "panic", v)
				writeError(w, http.StatusInternalServerError, "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// ---------- view models ----------

// ServiceView is one service on the status page.
type ServiceView struct {
	ID          string        `json:"id"`
	Name        string        `json:"name"`
	Group       string        `json:"group"`
	Description string        `json:"description,omitempty"`
	Status      check.Status  `json:"status"`
	Latency     time.Duration `json:"latency_ns"`
	Message     string        `json:"message,omitempty"`
	LastChecked *time.Time    `json:"last_checked,omitempty"`
	// Uptime is availability over the page window, in percent, or -1.
	Uptime float64 `json:"uptime"`
	// Days is the per-day history strip, oldest first.
	Days []DayView   `json:"days"`
	SLO  *slo.Status `json:"slo,omitempty"`
}

// DayView is one cell in the uptime strip.
type DayView struct {
	Date   time.Time `json:"date"`
	Uptime float64   `json:"uptime"` // percent, or -1 when there is no data
	Total  int64     `json:"total"`
	Down   int64     `json:"down"`
}

// GroupView groups services for display.
type GroupView struct {
	Name     string        `json:"name"`
	Services []ServiceView `json:"services"`
}

// Summary is the whole page.
type Summary struct {
	Title     string           `json:"title"`
	Overall   check.Status     `json:"overall"`
	Headline  string           `json:"headline"`
	UpdatedAt time.Time        `json:"updated_at"`
	Groups    []GroupView      `json:"groups"`
	Incidents []store.Incident `json:"incidents"`
	Window    int              `json:"window_days"`
}

// ---------- handlers ----------

func (s *Server) handlePage(w http.ResponseWriter, r *http.Request) {
	sum, err := s.summary(r.Context())
	if err != nil {
		s.log.Error("build summary", "err", err)
		writeError(w, http.StatusInternalServerError, "could not build status page")
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if ttl := s.cfg.Server.PageCacheTTL.D(); ttl > 0 {
		w.Header().Set("Cache-Control", "public, max-age="+strconv.Itoa(int(ttl.Seconds())))
	}
	if err := s.tmpl.ExecuteTemplate(w, "page.html", sum); err != nil {
		// Headers are already sent at this point, so log rather than
		// pretending we can still return a clean 500.
		s.log.Error("render page", "err", err)
	}
}

func (s *Server) handleStatusJSON(w http.ResponseWriter, r *http.Request) {
	sum, err := s.summary(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not build status")
		return
	}
	writeJSON(w, http.StatusOK, sum)
}

func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	svc, ok := s.service(id)
	if !ok {
		writeError(w, http.StatusNotFound, "unknown service")
		return
	}

	res := store.Resolution(firstNonEmpty(r.URL.Query().Get("resolution"), string(store.Hourly)))
	if res.Size() <= 0 {
		writeError(w, http.StatusBadRequest, "unknown resolution")
		return
	}
	now := s.clk.Now()
	window := 7 * 24 * time.Hour
	if v := r.URL.Query().Get("window"); v != "" {
		d, err := config.ParseDuration(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid window")
			return
		}
		window = d
	}
	// Bound the window so one request cannot ask for a decade of rows.
	if max := 400 * 24 * time.Hour; window > max {
		window = max
	}

	buckets, err := s.st.Rollups(r.Context(), svc.ID, res, now.Add(-window), now)
	if err != nil {
		s.log.Error("rollups", "service", svc.ID, "err", err)
		writeError(w, http.StatusInternalServerError, "could not load history")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"service_id": svc.ID,
		"resolution": res,
		"from":       now.Add(-window),
		"to":         now,
		"buckets":    buckets,
	})
}

func (s *Server) handleListIncidents(w http.ResponseWriter, r *http.Request) {
	f := store.IncidentFilter{Limit: 50}
	q := r.URL.Query()
	if v := q.Get("service_id"); v != "" {
		f.ServiceID = v
	}
	if v := q.Get("state"); v != "" {
		switch store.IncidentState(v) {
		case store.IncidentOpen, store.IncidentResolved:
			f.State = store.IncidentState(v)
		default:
			writeError(w, http.StatusBadRequest, "state must be open or resolved")
			return
		}
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 || n > 500 {
			writeError(w, http.StatusBadRequest, "limit must be between 1 and 500")
			return
		}
		f.Limit = n
	}

	list, err := s.st.ListIncidents(r.Context(), f)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not list incidents")
		return
	}
	if list == nil {
		list = []store.Incident{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"incidents": list})
}

func (s *Server) handleGetIncident(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "incident id must be a number")
		return
	}
	in, err := s.st.GetIncident(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "no such incident")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load incident")
		return
	}
	writeJSON(w, http.StatusOK, in)
}

func (s *Server) handleCreateIncident(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ServiceID string `json:"service_id"`
		Title     string `json:"title"`
		Severity  string `json:"severity"`
	}
	if !decode(w, r, &body) {
		return
	}
	if strings.TrimSpace(body.Title) == "" {
		writeError(w, http.StatusBadRequest, "title is required")
		return
	}
	if body.ServiceID != "" {
		if _, ok := s.service(body.ServiceID); !ok {
			writeError(w, http.StatusBadRequest, "unknown service_id")
			return
		}
	}
	if body.Severity == "" {
		body.Severity = "warning"
	}

	in, err := s.st.CreateIncident(r.Context(), store.Incident{
		ServiceID: body.ServiceID,
		Title:     body.Title,
		Severity:  body.Severity,
		State:     store.IncidentOpen,
		Auto:      false,
		StartedAt: s.clk.Now(),
	})
	if err != nil {
		s.log.Error("create incident", "err", err)
		writeError(w, http.StatusInternalServerError, "could not create incident")
		return
	}
	s.invalidate()
	writeJSON(w, http.StatusCreated, in)
}

func (s *Server) handleResolveIncident(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "incident id must be a number")
		return
	}
	in, err := s.st.ResolveIncident(r.Context(), id, s.clk.Now())
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "no such incident")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not resolve incident")
		return
	}
	s.invalidate()
	writeJSON(w, http.StatusOK, in)
}

func (s *Server) handleAddNote(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "incident id must be a number")
		return
	}
	var body struct {
		Author string `json:"author"`
		Body   string `json:"body"`
	}
	if !decode(w, r, &body) {
		return
	}
	if strings.TrimSpace(body.Body) == "" {
		writeError(w, http.StatusBadRequest, "body is required")
		return
	}

	n, err := s.st.AddNote(r.Context(), store.Note{
		IncidentID: id,
		Author:     firstNonEmpty(body.Author, "status-page"),
		Body:       body.Body,
		CreatedAt:  s.clk.Now(),
	})
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "no such incident")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not add note")
		return
	}
	s.invalidate()
	writeJSON(w, http.StatusCreated, n)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReady reports whether the datastore is reachable, so a load balancer
// can take this instance out of rotation instead of serving a broken page.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if err := s.st.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable", "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// handleMetrics exposes Prometheus text format without pulling in the client
// library: this service has four metrics, not four hundred.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	sum, err := s.summary(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not build metrics")
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	var b strings.Builder
	b.WriteString("# HELP statuspage_service_up Whether the service is currently available (1) or not (0).\n")
	b.WriteString("# TYPE statuspage_service_up gauge\n")
	for _, g := range sum.Groups {
		for _, svc := range g.Services {
			up := 0
			if svc.Status.Available() {
				up = 1
			}
			fmt.Fprintf(&b, "statuspage_service_up{service=%q,group=%q} %d\n", svc.ID, g.Name, up)
		}
	}
	b.WriteString("# HELP statuspage_service_latency_seconds Latency of the most recent probe.\n")
	b.WriteString("# TYPE statuspage_service_latency_seconds gauge\n")
	for _, g := range sum.Groups {
		for _, svc := range g.Services {
			fmt.Fprintf(&b, "statuspage_service_latency_seconds{service=%q} %g\n", svc.ID, svc.Latency.Seconds())
		}
	}
	b.WriteString("# HELP statuspage_service_uptime_ratio Availability over the page window.\n")
	b.WriteString("# TYPE statuspage_service_uptime_ratio gauge\n")
	for _, g := range sum.Groups {
		for _, svc := range g.Services {
			if svc.Uptime < 0 {
				continue
			}
			fmt.Fprintf(&b, "statuspage_service_uptime_ratio{service=%q} %g\n", svc.ID, svc.Uptime/100)
		}
	}
	b.WriteString("# HELP statuspage_error_budget_remaining Fraction of the SLO error budget left.\n")
	b.WriteString("# TYPE statuspage_error_budget_remaining gauge\n")
	for _, g := range sum.Groups {
		for _, svc := range g.Services {
			if svc.SLO == nil {
				continue
			}
			fmt.Fprintf(&b, "statuspage_error_budget_remaining{service=%q} %g\n", svc.ID, svc.SLO.BudgetRemaining)
		}
	}
	b.WriteString("# HELP statuspage_open_incidents Number of currently open incidents.\n")
	b.WriteString("# TYPE statuspage_open_incidents gauge\n")
	open := 0
	for _, in := range sum.Incidents {
		if in.State == store.IncidentOpen {
			open++
		}
	}
	fmt.Fprintf(&b, "statuspage_open_incidents %d\n", open)

	_, _ = w.Write([]byte(b.String()))
}

// ---------- summary building ----------

func (s *Server) invalidate() {
	s.mu.Lock()
	s.cached, s.cachedAt = nil, time.Time{}
	s.mu.Unlock()
}

// summary returns the page model, served from cache when fresh.
func (s *Server) summary(ctx context.Context) (*Summary, error) {
	ttl := s.cfg.Server.PageCacheTTL.D()
	now := s.clk.Now()

	if ttl > 0 {
		s.mu.RLock()
		cached, at := s.cached, s.cachedAt
		s.mu.RUnlock()
		if cached != nil && now.Sub(at) < ttl {
			return cached, nil
		}
	}

	sum, err := s.build(ctx, now)
	if err != nil {
		return nil, err
	}
	if ttl > 0 {
		s.mu.Lock()
		s.cached, s.cachedAt = sum, now
		s.mu.Unlock()
	}
	return sum, nil
}

func (s *Server) build(ctx context.Context, now time.Time) (*Summary, error) {
	latest, err := s.st.LatestResults(ctx)
	if err != nil {
		return nil, fmt.Errorf("latest results: %w", err)
	}

	days := s.cfg.Server.UptimeDays
	if days <= 0 {
		days = 90
	}
	window := time.Duration(days) * 24 * time.Hour

	sum := &Summary{
		Title:     firstNonEmpty(s.cfg.Server.Title, "Status"),
		UpdatedAt: now,
		Window:    days,
		Overall:   check.StatusUp,
	}

	groups := map[string]*GroupView{}
	var order []string

	for _, svc := range s.cfg.Services {
		if svc.Hidden {
			continue
		}
		view := ServiceView{
			ID:          svc.ID,
			Name:        displayName(svc),
			Group:       firstNonEmpty(svc.Group, "Services"),
			Description: svc.Description,
			Status:      check.StatusUnknown,
			Uptime:      -1,
		}
		if r, ok := latest[svc.ID]; ok {
			view.Status, view.Latency, view.Message = r.Status, r.Latency, r.Message
			at := r.At
			view.LastChecked = &at
		}

		counts, err := s.st.RawCounts(ctx, svc.ID, now.Add(-window), now)
		if err != nil {
			return nil, fmt.Errorf("counts for %s: %w", svc.ID, err)
		}
		if ratio := counts.Ratio(); ratio >= 0 {
			view.Uptime = ratio * 100
		}

		view.Days, err = s.dayStrip(ctx, svc.ID, now, days)
		if err != nil {
			return nil, err
		}

		if svc.SLO != nil && svc.SLO.Target > 0 {
			sloWindow := s.cfg.SLO.Window.D()
			if sloWindow <= 0 {
				sloWindow = window
			}
			st, err := slo.Evaluate(ctx, s.st, svc.ID, svc.SLO.Target, sloWindow, now)
			if err != nil {
				return nil, err
			}
			view.SLO = &st
		}

		// Overall status is the worst confirmed service status.
		if view.Status > sum.Overall {
			sum.Overall = view.Status
		}

		g, ok := groups[view.Group]
		if !ok {
			g = &GroupView{Name: view.Group}
			groups[view.Group] = g
			order = append(order, view.Group)
		}
		g.Services = append(g.Services, view)
	}

	for _, name := range order {
		sum.Groups = append(sum.Groups, *groups[name])
	}

	incidents, err := s.st.ListIncidents(ctx, store.IncidentFilter{
		Since: now.Add(-window),
		Limit: 20,
	})
	if err != nil {
		return nil, fmt.Errorf("incidents: %w", err)
	}
	sum.Incidents = incidents
	sort.SliceStable(sum.Incidents, func(i, j int) bool {
		return sum.Incidents[i].StartedAt.After(sum.Incidents[j].StartedAt)
	})

	sum.Headline = headline(sum)
	return sum, nil
}

// dayStrip builds the per-day uptime history, oldest first.
func (s *Server) dayStrip(ctx context.Context, serviceID string, now time.Time, days int) ([]DayView, error) {
	from := store.Daily.Truncate(now.AddDate(0, 0, -(days - 1)))
	buckets, err := s.st.Rollups(ctx, serviceID, store.Daily, from, now)
	if err != nil {
		return nil, fmt.Errorf("day rollups for %s: %w", serviceID, err)
	}
	byDay := make(map[time.Time]store.Bucket, len(buckets))
	for _, b := range buckets {
		byDay[store.Daily.Truncate(b.Start).UTC()] = b
	}

	out := make([]DayView, 0, days)
	for i := 0; i < days; i++ {
		day := store.Daily.Truncate(from.AddDate(0, 0, i)).UTC()
		d := DayView{Date: day, Uptime: -1}
		if b, ok := byDay[day]; ok {
			d.Total, d.Down = b.Total, b.Down()
			if ratio := b.Ratio(); ratio >= 0 {
				d.Uptime = ratio * 100
			}
		}
		out = append(out, d)
	}
	return out, nil
}

func headline(sum *Summary) string {
	var down, degraded int
	for _, g := range sum.Groups {
		for _, svc := range g.Services {
			switch svc.Status {
			case check.StatusDown:
				down++
			case check.StatusDegraded:
				degraded++
			}
		}
	}
	switch {
	case down > 0 && degraded > 0:
		return fmt.Sprintf("%s down, %s degraded", plural(down, "service"), plural(degraded, "service"))
	case down > 0:
		return plural(down, "service") + " down"
	case degraded > 0:
		return plural(degraded, "service") + " degraded"
	default:
		return "All systems operational"
	}
}

func plural(n int, word string) string {
	if n == 1 {
		return fmt.Sprintf("1 %s", word)
	}
	return fmt.Sprintf("%d %ss", n, word)
}

// ---------- helpers ----------

func (s *Server) service(id string) (config.Service, bool) {
	for _, svc := range s.cfg.Services {
		if svc.ID == id {
			return svc, true
		}
	}
	return config.Service{}, false
}

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	// Bound the body so a large POST cannot exhaust memory.
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// Nothing useful left to do: the status line is already written.
		_ = err
	}
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func displayName(svc config.Service) string {
	if svc.Name != "" {
		return svc.Name
	}
	return svc.ID
}

func funcs() template.FuncMap {
	return template.FuncMap{
		"pct": func(v float64) string {
			if v < 0 {
				return "–"
			}
			return strconv.FormatFloat(v, 'f', 2, 64) + "%"
		},
		"ms": func(d time.Duration) string {
			if d <= 0 {
				return "–"
			}
			return strconv.FormatInt(d.Milliseconds(), 10) + " ms"
		},
		"statusClass": func(s check.Status) string {
			switch s {
			case check.StatusUp:
				return "up"
			case check.StatusDegraded:
				return "degraded"
			case check.StatusDown:
				return "down"
			default:
				return "unknown"
			}
		},
		"dayClass": func(d DayView) string {
			switch {
			case d.Uptime < 0:
				return "nodata"
			case d.Uptime >= 99.99:
				return "up"
			case d.Uptime >= 95:
				return "degraded"
			default:
				return "down"
			}
		},
		"dayTitle": func(d DayView) string {
			if d.Uptime < 0 {
				return d.Date.Format("2 Jan 2006") + ": no data"
			}
			return fmt.Sprintf("%s: %.2f%% (%d checks, %d failed)",
				d.Date.Format("2 Jan 2006"), d.Uptime, d.Total, d.Down)
		},
		"mulPct":  func(v float64) float64 { return v * 100 },
		"rfc1123": func(t time.Time) string { return t.UTC().Format(time.RFC1123) },
		"since": func(t time.Time) string {
			return t.UTC().Format("2 Jan 15:04 MST")
		},
	}
}
