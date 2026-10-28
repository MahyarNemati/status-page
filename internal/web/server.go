// Package web serves the public status page, the JSON API, the token
// protected admin API and Prometheus metrics.
package web

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/MahyarNemati/status-page/internal/clock"
	"github.com/MahyarNemati/status-page/internal/config"
	"github.com/MahyarNemati/status-page/internal/incident"
	"github.com/MahyarNemati/status-page/internal/metrics"
	"github.com/MahyarNemati/status-page/internal/slo"
	"github.com/MahyarNemati/status-page/internal/store"
)

//go:embed templates/*.html
var templateFS embed.FS

//go:embed static
var staticFS embed.FS

// Deps are the collaborators the server needs.
type Deps struct {
	Config    *config.Config
	Store     store.Store
	Incidents *incident.Manager
	SLO       *slo.Evaluator
	Metrics   *metrics.Metrics
	Clock     clock.Clock
	Logger    *slog.Logger
}

// Server is the HTTP front end.
type Server struct {
	deps      Deps
	tmpl      *template.Template
	tokenHash [32]byte
	admin     bool

	cacheMu sync.Mutex
	cached  *Page
	cacheAt time.Time
}

// New parses templates and returns a Server.
func New(d Deps) (*Server, error) {
	if d.Clock == nil {
		d.Clock = clock.Real()
	}
	if d.Logger == nil {
		d.Logger = slog.New(slog.DiscardHandler)
	}
	funcs := template.FuncMap{
		"percent": percent,
		"ago":     func(t time.Time) string { return humanizeAgo(d.Clock.Now(), t) },
		"date":    func(t time.Time) string { return t.UTC().Format("Jan 2, 2006 15:04 MST") },
		"iso":     func(t time.Time) string { return t.UTC().Format(time.RFC3339) },
		"dur":     func(dt time.Duration) string { return humanizeDuration(dt) },
		"ms":      func(dt time.Duration) string { return fmt.Sprintf("%d ms", dt.Milliseconds()) },
		"budget":  func(f float64) string { return fmt.Sprintf("%.1f%%", f*100) },
		"lastNote": func(in any) string {
			if i, ok := in.(store.Incident); ok && len(i.Notes) > 0 {
				return i.Notes[len(i.Notes)-1].Body
			}
			return ""
		},
		"incidentDuration": func(in store.Incident) string { return humanizeDuration(in.Duration(d.Clock.Now())) },
	}
	tmpl, err := template.New("").Funcs(funcs).ParseFS(templateFS, "templates/*.html")
	if err != nil {
		return nil, fmt.Errorf("parse templates: %w", err)
	}
	s := &Server{deps: d, tmpl: tmpl}
	if tok := d.Config.Server.AdminToken; tok != "" {
		s.admin = true
		s.tokenHash = sha256.Sum256([]byte(tok))
	}
	return s, nil
}

// Handler returns the root handler with middleware applied.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	static, _ := fs.Sub(staticFS, "static")
	mux.Handle("GET /static/", http.StripPrefix("/static/", cacheFor(24*time.Hour, http.FileServerFS(static))))

	mux.HandleFunc("GET /{$}", s.handleIndex)
	mux.HandleFunc("GET /incidents/{id}", s.handleIncidentPage)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", s.handleReady)
	if s.deps.Metrics != nil {
		mux.Handle("GET /metrics", s.deps.Metrics.Handler())
	}

	mux.HandleFunc("GET /api/v1/status", s.handleStatus)
	mux.HandleFunc("GET /api/v1/services/{id}", s.handleService)
	mux.HandleFunc("GET /api/v1/services/{id}/uptime", s.handleUptime)
	mux.HandleFunc("GET /api/v1/incidents", s.handleIncidents)
	mux.HandleFunc("GET /api/v1/incidents/{id}", s.handleIncident)
	mux.HandleFunc("GET /api/v1/slo", s.handleSLO)

	mux.Handle("GET /api/v1/admin/services", s.requireAdmin(http.HandlerFunc(s.handleAdminServices)))
	mux.Handle("POST /api/v1/admin/incidents", s.requireAdmin(http.HandlerFunc(s.handleDeclare)))
	mux.Handle("POST /api/v1/admin/incidents/{id}/notes", s.requireAdmin(http.HandlerFunc(s.handleNote)))
	mux.Handle("POST /api/v1/admin/incidents/{id}/resolve", s.requireAdmin(http.HandlerFunc(s.handleResolve)))

	return s.recoverer(s.logRequests(securityHeaders(mux)))
}

// page returns the cached page view, rebuilding it after the TTL.
func (s *Server) page(ctx context.Context) (*Page, error) {
	ttl := s.deps.Config.Server.PageCacheTTL.D()
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	now := s.deps.Clock.Now()
	if s.cached != nil && now.Sub(s.cacheAt) < ttl {
		return s.cached, nil
	}
	p, err := s.buildPage(ctx)
	if err != nil {
		return nil, err
	}
	s.cached, s.cacheAt = p, now
	return p, nil
}

// Invalidate drops the cached page so admin changes show immediately.
func (s *Server) Invalidate() {
	s.cacheMu.Lock()
	s.cached = nil
	s.cacheMu.Unlock()
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	p, err := s.page(r.Context())
	if err != nil {
		s.serverError(w, r, err)
		return
	}
	s.render(w, r, "index.html", p)
}

func (s *Server) handleIncidentPage(w http.ResponseWriter, r *http.Request) {
	in, ok := s.visibleIncident(w, r)
	if !ok {
		return
	}
	svc, _ := s.deps.Config.Service(in.ServiceID)
	s.render(w, r, "incident.html", map[string]any{
		"Title":    s.deps.Config.Server.Title,
		"Incident": in,
		"Service":  svc.Name,
	})
}

func (s *Server) render(w http.ResponseWriter, r *http.Request, name string, data any) {
	var buf bytes.Buffer
	if err := s.tmpl.ExecuteTemplate(&buf, name, data); err != nil {
		s.serverError(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=10")
	_, _ = buf.WriteTo(w)
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.deps.Store.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "storage unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	p, err := s.page(r.Context())
	if err != nil {
		s.serverError(w, r, err)
		return
	}
	// The summary omits per-day history; /services/{id} carries it.
	out := *p
	out.Groups = make([]Group, len(p.Groups))
	for i, g := range p.Groups {
		out.Groups[i] = Group{Name: g.Name, Services: make([]ServiceView, len(g.Services))}
		for j, sv := range g.Services {
			sv.Days = nil
			out.Groups[i].Services[j] = sv
		}
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) publicService(w http.ResponseWriter, r *http.Request) (config.Service, bool) {
	svc, ok := s.deps.Config.Service(r.PathValue("id"))
	if !ok || svc.Hidden {
		writeError(w, http.StatusNotFound, "service not found")
		return config.Service{}, false
	}
	return svc, true
}

func (s *Server) handleService(w http.ResponseWriter, r *http.Request) {
	svc, ok := s.publicService(w, r)
	if !ok {
		return
	}
	p, err := s.page(r.Context())
	if err != nil {
		s.serverError(w, r, err)
		return
	}
	recent, err := s.deps.Store.RecentResults(r.Context(), svc.ID, 20)
	if err != nil {
		s.serverError(w, r, err)
		return
	}
	for _, g := range p.Groups {
		for _, sv := range g.Services {
			if sv.ID == svc.ID {
				writeJSON(w, http.StatusOK, map[string]any{"service": sv, "recent_results": nonNil(recent)})
				return
			}
		}
	}
	writeError(w, http.StatusNotFound, "service not found")
}

func (s *Server) handleUptime(w http.ResponseWriter, r *http.Request) {
	svc, ok := s.publicService(w, r)
	if !ok {
		return
	}
	res := store.Daily
	maxSpan := 400 * 24 * time.Hour
	if r.URL.Query().Get("resolution") == "hour" {
		res, maxSpan = store.Hourly, 90*24*time.Hour
	}
	span := time.Duration(s.deps.Config.Server.UptimeDays) * 24 * time.Hour
	if v := r.URL.Query().Get("days"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "days must be a positive integer")
			return
		}
		span = time.Duration(n) * 24 * time.Hour
	}
	if span > maxSpan {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("span exceeds %s for %s resolution", maxSpan, res))
		return
	}
	now := s.deps.Clock.Now()
	buckets, err := s.deps.Store.Rollups(r.Context(), svc.ID, res, res.Truncate(now.Add(-span)), now)
	if err != nil {
		s.serverError(w, r, err)
		return
	}
	var total store.Counts
	for _, b := range buckets {
		total = total.Add(b.Counts)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"service_id": svc.ID, "resolution": res, "uptime": total.Ratio(), "counts": total, "buckets": nonNil(buckets),
	})
}

func (s *Server) handleIncidents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := store.IncidentFilter{ServiceID: q.Get("service"), Limit: 50}
	switch st := store.IncidentState(q.Get("state")); st {
	case "", store.IncidentOpen, store.IncidentResolved:
		f.State = st
	default:
		writeError(w, http.StatusBadRequest, "state must be open or resolved")
		return
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 500 {
			writeError(w, http.StatusBadRequest, "limit must be between 1 and 500")
			return
		}
		f.Limit = n
	}
	list, err := s.deps.Store.ListIncidents(r.Context(), f)
	if err != nil {
		s.serverError(w, r, err)
		return
	}
	out := make([]store.Incident, 0, len(list))
	for _, in := range list {
		if svc, ok := s.deps.Config.Service(in.ServiceID); ok && !svc.Hidden {
			out = append(out, in)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"incidents": out})
}

func (s *Server) visibleIncident(w http.ResponseWriter, r *http.Request) (store.Incident, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusNotFound, "incident not found")
		return store.Incident{}, false
	}
	in, err := s.deps.Store.GetIncident(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "incident not found")
		return store.Incident{}, false
	}
	if err != nil {
		s.serverError(w, r, err)
		return store.Incident{}, false
	}
	if svc, ok := s.deps.Config.Service(in.ServiceID); !ok || svc.Hidden {
		writeError(w, http.StatusNotFound, "incident not found")
		return store.Incident{}, false
	}
	return in, true
}

func (s *Server) handleIncident(w http.ResponseWriter, r *http.Request) {
	if in, ok := s.visibleIncident(w, r); ok {
		writeJSON(w, http.StatusOK, in)
	}
}

func (s *Server) handleSLO(w http.ResponseWriter, _ *http.Request) {
	out := []slo.Report{}
	if s.deps.SLO != nil {
		for _, rep := range s.deps.SLO.Reports() {
			if svc, ok := s.deps.Config.Service(rep.ServiceID); ok && !svc.Hidden {
				out = append(out, rep)
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"slos": out})
}

func (s *Server) handleAdminServices(w http.ResponseWriter, r *http.Request) {
	latest, err := s.deps.Store.LatestResults(r.Context())
	if err != nil {
		s.serverError(w, r, err)
		return
	}
	type row struct {
		config.Service
		Latest   *store.Result `json:"latest,omitempty"`
		Flapping bool          `json:"flapping"`
		SLO      *slo.Report   `json:"slo_report,omitempty"`
	}
	out := make([]row, 0, len(s.deps.Config.Services))
	for _, svc := range s.deps.Config.Services {
		rw := row{Service: svc, Flapping: s.deps.Incidents.Flapping(svc.ID)}
		if l, ok := latest[svc.ID]; ok {
			rw.Latest = &l
		}
		if s.deps.SLO != nil {
			if rep, ok := s.deps.SLO.Report(svc.ID); ok {
				rw.SLO = &rep
			}
		}
		out = append(out, rw)
	}
	writeJSON(w, http.StatusOK, map[string]any{"services": out})
}

type declareRequest struct {
	ServiceID string `json:"service_id"`
	Title     string `json:"title"`
	Severity  string `json:"severity"`
	Author    string `json:"author"`
	Note      string `json:"note"`
}

func (s *Server) handleDeclare(w http.ResponseWriter, r *http.Request) {
	var req declareRequest
	if !decode(w, r, &req) {
		return
	}
	svc, ok := s.deps.Config.Service(req.ServiceID)
	if !ok {
		writeError(w, http.StatusUnprocessableEntity, "unknown service_id")
		return
	}
	in, err := s.deps.Incidents.Declare(r.Context(), svc, req.Title, req.Severity, author(req.Author), req.Note, s.deps.Clock.Now())
	if s.adminResult(w, r, err) {
		s.Invalidate()
		writeJSON(w, http.StatusCreated, in)
	}
}

type noteRequest struct {
	Author string `json:"author"`
	Body   string `json:"body"`
}

func (s *Server) handleNote(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusNotFound, "incident not found")
		return
	}
	var req noteRequest
	if !decode(w, r, &req) {
		return
	}
	n, err := s.deps.Incidents.AddNote(r.Context(), id, author(req.Author), req.Body, s.deps.Clock.Now())
	if s.adminResult(w, r, err) {
		s.Invalidate()
		writeJSON(w, http.StatusCreated, n)
	}
}

func (s *Server) handleResolve(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusNotFound, "incident not found")
		return
	}
	var req noteRequest
	if r.ContentLength != 0 && !decode(w, r, &req) {
		return
	}
	existing, err := s.deps.Store.GetIncident(r.Context(), id)
	if !s.adminResult(w, r, err) {
		return
	}
	svc, _ := s.deps.Config.Service(existing.ServiceID)
	if svc.ID == "" {
		svc = config.Service{ID: existing.ServiceID, Name: existing.ServiceID}
	}
	in, err := s.deps.Incidents.Resolve(r.Context(), svc, id, author(req.Author), req.Body, s.deps.Clock.Now())
	if s.adminResult(w, r, err) {
		s.Invalidate()
		writeJSON(w, http.StatusOK, in)
	}
}

func (s *Server) adminResult(w http.ResponseWriter, r *http.Request, err error) bool {
	switch {
	case err == nil:
		return true
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "incident not found")
	case errors.Is(err, incident.ErrInvalid):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
	default:
		s.serverError(w, r, err)
	}
	return false
}

func author(a string) string {
	if a = strings.TrimSpace(a); a == "" {
		return "admin"
	}
	return a
}

// requireAdmin checks a bearer token. Hashing both sides first keeps the
// comparison constant time regardless of the supplied token's length.
func (s *Server) requireAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.admin {
			writeError(w, http.StatusNotFound, "admin API is disabled; set STATUSPAGE_ADMIN_TOKEN")
			return
		}
		tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		sum := sha256.Sum256([]byte(tok))
		if !ok || subtle.ConstantTimeCompare(sum[:], s.tokenHash[:]) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="status-page"`)
			writeError(w, http.StatusUnauthorized, "invalid or missing bearer token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

const maxBody = 64 << 10

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (s *Server) serverError(w http.ResponseWriter, r *http.Request, err error) {
	s.deps.Logger.Error("request failed", "method", r.Method, "path", r.URL.Path, "err", err)
	writeError(w, http.StatusInternalServerError, "internal error")
}

func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}
