// Package check implements the network probes: HTTP(S), TCP, DNS and ICMP.
package check

import (
	"context"
	"fmt"
	"time"

	"github.com/MahyarNemati/status-page/internal/config"
)

// Status is the outcome of a single probe.
type Status int

// Probe outcomes. Degraded counts as available for SLO purposes.
const (
	StatusUnknown Status = iota
	StatusUp
	StatusDegraded
	StatusDown
)

func (s Status) String() string {
	switch s {
	case StatusUp:
		return "up"
	case StatusDegraded:
		return "degraded"
	case StatusDown:
		return "down"
	default:
		return "unknown"
	}
}

// Available reports whether the status counts toward availability.
func (s Status) Available() bool { return s == StatusUp || s == StatusDegraded }

// MarshalText renders the status as its name in JSON.
func (s Status) MarshalText() ([]byte, error) { return []byte(s.String()), nil }

// Result is what a probe reports.
type Result struct {
	Status    Status
	Latency   time.Duration
	Message   string
	TLSExpiry time.Time
}

// Checker probes one target.
type Checker interface {
	Check(ctx context.Context) Result
}

// New builds the Checker for a service definition.
func New(svc config.Service) (Checker, error) {
	switch svc.Type {
	case config.CheckHTTP:
		return NewHTTP(svc)
	case config.CheckTCP:
		return &TCP{Address: svc.Target, DegradedLatency: svc.DegradedLatency.D()}, nil
	case config.CheckDNS:
		return NewDNS(svc), nil
	case config.CheckICMP:
		return &ICMP{Host: svc.Target, Privileged: svc.ICMP.Privileged, DegradedLatency: svc.DegradedLatency.D()}, nil
	default:
		return nil, fmt.Errorf("unknown check type %q", svc.Type)
	}
}

func down(start time.Time, format string, a ...any) Result {
	return Result{Status: StatusDown, Latency: time.Since(start), Message: fmt.Sprintf(format, a...)}
}

func gradeLatency(r Result, threshold time.Duration) Result {
	if r.Status == StatusUp && threshold > 0 && r.Latency > threshold {
		r.Status = StatusDegraded
		r.Message = fmt.Sprintf("slow response: %s > %s", r.Latency.Round(time.Millisecond), threshold)
	}
	return r
}
