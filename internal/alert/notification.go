// Package alert delivers incident and SLO notifications to Slack, Discord,
// email and generic webhooks, with per-channel deduplication.
package alert

import (
	"context"
	"fmt"
	"time"
)

// Kind classifies a notification.
type Kind string

// Notification kinds. Every firing kind has a matching recovery kind.
const (
	IncidentOpened   Kind = "incident.opened"
	IncidentResolved Kind = "incident.resolved"
	BurnRateFiring   Kind = "slo.burn_rate"
	BurnRateResolved Kind = "slo.recovered"
)

// Recovery reports whether the kind closes out an earlier firing.
func (k Kind) Recovery() bool { return k == IncidentResolved || k == BurnRateResolved }

// Notification is a channel-agnostic alert.
type Notification struct {
	Kind        Kind      `json:"kind"`
	Key         string    `json:"key"`
	ServiceID   string    `json:"service_id"`
	ServiceName string    `json:"service_name"`
	Severity    string    `json:"severity"`
	Title       string    `json:"title"`
	Message     string    `json:"message"`
	URL         string    `json:"url,omitempty"`
	At          time.Time `json:"at"`
	// Channels limits delivery to the named channels; empty means all.
	Channels []string `json:"-"`
}

// Subject is a one-line summary used for email subjects and chat headlines.
func (n Notification) Subject() string {
	if n.Kind.Recovery() {
		return "[RESOLVED] " + n.Title
	}
	if n.Severity != "" {
		return fmt.Sprintf("[%s] %s", upper(n.Severity), n.Title)
	}
	return n.Title
}

func upper(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'a' && c <= 'z' {
			b[i] = c - 32
		}
	}
	return string(b)
}

// Notifier sends a notification to one destination.
type Notifier interface {
	Name() string
	Send(ctx context.Context, n Notification) error
}

// Publisher accepts notifications for asynchronous delivery.
type Publisher interface {
	Publish(n Notification)
}

// PublisherFunc adapts a function to Publisher.
type PublisherFunc func(Notification)

// Publish calls f(n).
func (f PublisherFunc) Publish(n Notification) { f(n) }
