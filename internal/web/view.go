package web

import (
	"context"
	"fmt"
	"time"

	"github.com/MahyarNemati/status-page/internal/check"
	"github.com/MahyarNemati/status-page/internal/config"
	"github.com/MahyarNemati/status-page/internal/incident"
	"github.com/MahyarNemati/status-page/internal/slo"
	"github.com/MahyarNemati/status-page/internal/store"
)

// Level is the public status of a service, ordered by severity.
type Level int

// Status levels.
const (
	LevelUnknown Level = iota
	LevelOperational
	LevelDegraded
	LevelOutage
)

func (l Level) String() string {
	switch l {
	case LevelOperational:
		return "operational"
	case LevelDegraded:
		return "degraded"
	case LevelOutage:
		return "outage"
	default:
		return "unknown"
	}
}

// Label is the human-readable form.
func (l Level) Label() string {
	switch l {
	case LevelOperational:
		return "Operational"
	case LevelDegraded:
		return "Degraded"
	case LevelOutage:
		return "Outage"
	default:
		return "No data"
	}
}

// MarshalText renders the level by name in JSON.
func (l Level) MarshalText() ([]byte, error) { return []byte(l.String()), nil }

// Day is one bar of the uptime history.
type Day struct {
	Date      time.Time    `json:"date"`
	Counts    store.Counts `json:"counts"`
	Uptime    float64      `json:"uptime"`
	Incidents int          `json:"incidents"`
	Class     string       `json:"-"`
	Label     string       `json:"-"`
}

// ServiceView is the public representation of one service.
type ServiceView struct {
	ID          string        `json:"id"`
	Name        string        `json:"name"`
	Group       string        `json:"group,omitempty"`
	Description string        `json:"description,omitempty"`
	Level       Level         `json:"status"`
	LastChecked *time.Time    `json:"last_checked,omitempty"`
	LastStatus  string        `json:"last_result,omitempty"`
	Latency     time.Duration `json:"latency_ns,omitempty"`
	Uptime      float64       `json:"uptime"`
	Days        []Day         `json:"days,omitempty"`
	SLO         *slo.Report   `json:"slo,omitempty"`
}

// UptimeLabel formats the period uptime for display.
func (s ServiceView) UptimeLabel() string { return percent(s.Uptime) }

// Group is a named set of services.
type Group struct {
	Name     string        `json:"name"`
	Services []ServiceView `json:"services"`
}

// Page is everything the public status page renders.
type Page struct {
	Title       string           `json:"title"`
	Level       Level            `json:"status"`
	Headline    string           `json:"headline"`
	Groups      []Group          `json:"groups"`
	Active      []store.Incident `json:"active_incidents"`
	Recent      []store.Incident `json:"recent_incidents"`
	UptimeDays  int              `json:"uptime_days"`
	GeneratedAt time.Time        `json:"generated_at"`
	names       map[string]string
}

// ServiceName resolves a service id for templates.
func (p *Page) ServiceName(id string) string {
	if n, ok := p.names[id]; ok {
		return n
	}
	return id
}

const recentIncidentDays = 14

func (s *Server) buildPage(ctx context.Context) (*Page, error) {
	now := s.deps.Clock.Now().UTC()
	days := s.deps.Config.Server.UptimeDays
	today := now.Truncate(24 * time.Hour)
	from := today.AddDate(0, 0, -(days - 1))

	latest, err := s.deps.Store.LatestResults(ctx)
	if err != nil {
		return nil, err
	}
	incidents, err := s.deps.Store.ListIncidents(ctx, store.IncidentFilter{Since: from})
	if err != nil {
		return nil, err
	}

	page := &Page{
		Title: s.deps.Config.Server.Title, UptimeDays: days, GeneratedAt: now, names: map[string]string{},
	}
	byService := map[string][]store.Incident{}
	for _, in := range incidents {
		byService[in.ServiceID] = append(byService[in.ServiceID], in)
	}

	groupIdx := map[string]int{}
	worst, counted, outages := LevelUnknown, 0, 0
	for _, svc := range s.deps.Config.Services {
		if svc.Hidden {
			continue
		}
		page.names[svc.ID] = svc.Name
		v, err := s.serviceView(ctx, svc, latest, byService[svc.ID], from, today, now)
		if err != nil {
			return nil, err
		}
		if v.Level != LevelUnknown {
			counted++
			worst = max(worst, v.Level)
			if v.Level == LevelOutage {
				outages++
			}
		}
		i, ok := groupIdx[svc.Group]
		if !ok {
			i = len(page.Groups)
			groupIdx[svc.Group] = i
			page.Groups = append(page.Groups, Group{Name: svc.Group})
		}
		page.Groups[i].Services = append(page.Groups[i].Services, v)
	}

	cutoff := now.AddDate(0, 0, -recentIncidentDays)
	for _, in := range incidents {
		if _, visible := page.names[in.ServiceID]; !visible {
			continue
		}
		full, err := s.deps.Store.GetIncident(ctx, in.ID)
		if err != nil {
			return nil, err
		}
		switch {
		case in.State == store.IncidentOpen:
			page.Active = append(page.Active, full)
		case in.ResolvedAt != nil && in.ResolvedAt.After(cutoff):
			page.Recent = append(page.Recent, full)
		}
	}

	page.Level = worst
	switch {
	case counted == 0:
		page.Headline = "Waiting for the first checks"
	case worst == LevelOperational:
		page.Headline = "All systems operational"
	case worst == LevelDegraded:
		page.Headline = "Degraded performance"
	case outages == counted:
		page.Headline = "Major outage"
	default:
		page.Headline = "Partial outage"
	}
	return page, nil
}

func (s *Server) serviceView(ctx context.Context, svc config.Service, latest map[string]store.Result,
	incidents []store.Incident, from, today, now time.Time) (ServiceView, error) {
	v := ServiceView{ID: svc.ID, Name: svc.Name, Group: svc.Group, Description: svc.Description, Uptime: -1}

	if r, ok := latest[svc.ID]; ok {
		at := r.At
		v.LastChecked = &at
		v.LastStatus = r.Status.String()
		v.Latency = r.Latency
		switch r.Status {
		case check.StatusUp:
			v.Level = LevelOperational
		case check.StatusDegraded:
			v.Level = LevelDegraded
		case check.StatusDown:
			v.Level = LevelOutage
		}
	}
	for _, in := range incidents {
		if in.State != store.IncidentOpen {
			continue
		}
		lvl := LevelOutage
		if in.Severity == incident.SeverityMinor {
			lvl = LevelDegraded
		}
		v.Level = max(v.Level, lvl)
	}

	buckets, err := s.deps.Store.Rollups(ctx, svc.ID, store.Daily, from, today.Add(24*time.Hour))
	if err != nil {
		return v, err
	}
	byDay := make(map[int64]store.Bucket, len(buckets))
	var total store.Counts
	for _, b := range buckets {
		byDay[b.Start.Unix()] = b
		total = total.Add(b.Counts)
	}
	v.Uptime = total.Ratio()

	for d := from; !d.After(today); d = d.Add(24 * time.Hour) {
		day := Day{Date: d, Counts: byDay[d.Unix()].Counts}
		day.Uptime = day.Counts.Ratio()
		end := d.Add(24 * time.Hour)
		for _, in := range incidents {
			stop := now
			if in.ResolvedAt != nil {
				stop = *in.ResolvedAt
			}
			if in.StartedAt.Before(end) && !stop.Before(d) {
				day.Incidents++
			}
		}
		day.Class, day.Label = dayClass(day)
		v.Days = append(v.Days, day)
	}

	if s.deps.SLO != nil {
		if r, ok := s.deps.SLO.Report(svc.ID); ok {
			v.SLO = &r
		}
	}
	return v, nil
}

func dayClass(d Day) (class, label string) {
	date := d.Date.Format("Jan 2, 2006")
	if d.Counts.Total == 0 {
		return "none", date + ": no data"
	}
	switch u := d.Uptime; {
	case u >= 0.999:
		class = "ok"
	case u >= 0.99:
		class = "minor"
	case u >= 0.95:
		class = "partial"
	default:
		class = "major"
	}
	label = fmt.Sprintf("%s: %s uptime", date, percent(d.Uptime))
	switch d.Incidents {
	case 0:
	case 1:
		label += ", 1 incident"
	default:
		label += fmt.Sprintf(", %d incidents", d.Incidents)
	}
	return class, label
}

func percent(r float64) string {
	if r < 0 {
		return "n/a"
	}
	if r == 1 {
		return "100%"
	}
	return fmt.Sprintf("%.2f%%", r*100)
}
