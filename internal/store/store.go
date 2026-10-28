// Package store persists check results, rollups and incidents. SQLite is the
// default backend; Postgres is supported through the same SQL implementation.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/MahyarNemati/status-page/internal/check"
)

// ErrNotFound is returned when a requested record does not exist.
var ErrNotFound = errors.New("not found")

// Resolution of a rollup bucket.
type Resolution string

// Rollup resolutions written for every result.
const (
	Hourly Resolution = "hour"
	Daily  Resolution = "day"
)

// Size returns the bucket width.
func (r Resolution) Size() time.Duration {
	if r == Daily {
		return 24 * time.Hour
	}
	return time.Hour
}

// Truncate returns the start of the bucket containing t, in UTC.
func (r Resolution) Truncate(t time.Time) time.Time {
	return t.UTC().Truncate(r.Size())
}

// Result is one persisted probe outcome.
type Result struct {
	ServiceID string        `json:"service_id"`
	At        time.Time     `json:"at"`
	Status    check.Status  `json:"status"`
	Latency   time.Duration `json:"latency_ns"`
	Message   string        `json:"message"`
}

// Counts aggregates results over a window.
type Counts struct {
	Total    int64 `json:"total"`
	Up       int64 `json:"up"`
	Degraded int64 `json:"degraded"`
}

// Available is the number of results that count toward availability.
func (c Counts) Available() int64 { return c.Up + c.Degraded }

// Down is the number of failed results.
func (c Counts) Down() int64 { return c.Total - c.Available() }

// Ratio returns availability in [0,1], or -1 when there is no data.
func (c Counts) Ratio() float64 {
	if c.Total == 0 {
		return -1
	}
	return float64(c.Available()) / float64(c.Total)
}

// Add merges two counts.
func (c Counts) Add(o Counts) Counts {
	return Counts{Total: c.Total + o.Total, Up: c.Up + o.Up, Degraded: c.Degraded + o.Degraded}
}

// Bucket is one downsampled rollup row.
type Bucket struct {
	Start time.Time `json:"start"`
	Counts
	AvgLatency time.Duration `json:"avg_latency_ns"`
	MaxLatency time.Duration `json:"max_latency_ns"`
}

// IncidentState is open or resolved.
type IncidentState string

// Incident lifecycle states.
const (
	IncidentOpen     IncidentState = "open"
	IncidentResolved IncidentState = "resolved"
)

// Incident is an outage, either detected automatically or declared manually.
type Incident struct {
	ID         int64         `json:"id"`
	ServiceID  string        `json:"service_id"`
	Title      string        `json:"title"`
	Severity   string        `json:"severity"`
	State      IncidentState `json:"state"`
	Auto       bool          `json:"auto"`
	StartedAt  time.Time     `json:"started_at"`
	ResolvedAt *time.Time    `json:"resolved_at,omitempty"`
	Notes      []Note        `json:"notes,omitempty"`
}

// Duration returns how long the incident lasted, or has lasted until now.
func (i Incident) Duration(now time.Time) time.Duration {
	if i.ResolvedAt != nil {
		return i.ResolvedAt.Sub(i.StartedAt)
	}
	return now.Sub(i.StartedAt)
}

// Note is a timestamped update attached to an incident.
type Note struct {
	ID         int64     `json:"id"`
	IncidentID int64     `json:"incident_id"`
	Author     string    `json:"author"`
	Body       string    `json:"body"`
	CreatedAt  time.Time `json:"created_at"`
}

// IncidentFilter narrows ListIncidents.
type IncidentFilter struct {
	ServiceID string
	State     IncidentState
	Since     time.Time
	Limit     int
}

// Retention is how long each resolution is kept.
type Retention struct {
	Raw, Hourly, Daily time.Duration
}

// PruneStats reports rows removed by Prune.
type PruneStats struct {
	Raw, Hourly, Daily int64
}

// Store is the persistence boundary used by the rest of the service.
type Store interface {
	SaveResult(ctx context.Context, r Result) error
	LatestResults(ctx context.Context) (map[string]Result, error)
	RecentResults(ctx context.Context, serviceID string, limit int) ([]Result, error)
	RawCounts(ctx context.Context, serviceID string, from, to time.Time) (Counts, error)
	Rollups(ctx context.Context, serviceID string, res Resolution, from, to time.Time) ([]Bucket, error)

	CreateIncident(ctx context.Context, in Incident) (Incident, error)
	ResolveIncident(ctx context.Context, id int64, at time.Time) (Incident, error)
	GetIncident(ctx context.Context, id int64) (Incident, error)
	ListIncidents(ctx context.Context, f IncidentFilter) ([]Incident, error)
	AddNote(ctx context.Context, n Note) (Note, error)

	Prune(ctx context.Context, now time.Time, r Retention) (PruneStats, error)
	Ping(ctx context.Context) error
	Close() error
}
