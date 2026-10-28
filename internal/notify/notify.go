// Package notify delivers alerts to Slack, Discord, generic webhooks and
// email, and suppresses repeats.
//
// Delivery is best-effort by design: a status page that stops probing because
// Slack is down is worse than one that misses a Slack message. Send errors are
// returned for logging but never abort the monitor loop.
package notify

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/smtp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/MahyarNemati/status-page/internal/clock"
	"github.com/MahyarNemati/status-page/internal/config"
)

// Severity orders alerts from informational to urgent.
type Severity string

// Alert severities.
const (
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

// Alert is one notification.
type Alert struct {
	// DedupeKey identifies the logical alert. Two alerts with the same key
	// inside the dedupe window are sent once.
	DedupeKey string `json:"dedupe_key"`
	// Resolved marks a recovery notice. Resolutions are never suppressed:
	// silently dropping one leaves an operator believing a service is still
	// down.
	Resolved    bool      `json:"resolved"`
	Severity    Severity  `json:"severity"`
	Title       string    `json:"title"`
	Body        string    `json:"body"`
	ServiceID   string    `json:"service_id,omitempty"`
	ServiceName string    `json:"service_name,omitempty"`
	URL         string    `json:"url,omitempty"`
	At          time.Time `json:"at"`
}

// Sink delivers alerts to one destination.
type Sink interface {
	// Name is the channel name from the config, used for routing.
	Name() string
	Send(ctx context.Context, a Alert) error
}

// Router fans an alert out to the channels a service subscribes to and
// suppresses duplicates.
type Router struct {
	sinks  map[string]Sink
	order  []string
	window time.Duration
	clk    clock.Clock

	mu   sync.Mutex
	sent map[string]time.Time
}

// NewRouter builds a Router from the alert configuration. Unknown channel
// types are an error: a typo in a channel type should fail at startup, not
// silently discard every alert during an outage.
func NewRouter(cfg config.Alerts, clk clock.Clock) (*Router, error) {
	if clk == nil {
		clk = clock.Real()
	}
	timeout := cfg.Timeout.D()
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	r := &Router{
		sinks:  make(map[string]Sink, len(cfg.Channels)),
		window: cfg.DedupeWindow.D(),
		clk:    clk,
		sent:   make(map[string]time.Time),
	}
	hc := &http.Client{Timeout: timeout}
	for _, ch := range cfg.Channels {
		if ch.Name == "" {
			return nil, errors.New("alert channel is missing a name")
		}
		if _, dup := r.sinks[ch.Name]; dup {
			return nil, fmt.Errorf("duplicate alert channel %q", ch.Name)
		}
		s, err := newSink(ch, hc)
		if err != nil {
			return nil, fmt.Errorf("alert channel %q: %w", ch.Name, err)
		}
		r.sinks[ch.Name] = s
		r.order = append(r.order, ch.Name)
	}
	return r, nil
}

func newSink(ch config.Channel, hc *http.Client) (Sink, error) {
	switch strings.ToLower(ch.Type) {
	case "slack":
		if ch.URL == "" {
			return nil, errors.New("slack channel needs a url")
		}
		return &Slack{channel: ch, hc: hc}, nil
	case "discord":
		if ch.URL == "" {
			return nil, errors.New("discord channel needs a url")
		}
		return &Discord{channel: ch, hc: hc}, nil
	case "webhook":
		if ch.URL == "" {
			return nil, errors.New("webhook channel needs a url")
		}
		return &Webhook{channel: ch, hc: hc}, nil
	case "email", "smtp":
		if ch.SMTP.Host == "" {
			return nil, errors.New("email channel needs smtp.host")
		}
		if len(ch.SMTP.To) == 0 {
			return nil, errors.New("email channel needs at least one smtp.to address")
		}
		return &Email{channel: ch}, nil
	default:
		return nil, fmt.Errorf("unknown channel type %q (want slack, discord, webhook or email)", ch.Type)
	}
}

// Channels lists the configured channel names in config order.
func (r *Router) Channels() []string { return append([]string(nil), r.order...) }

// Has reports whether a channel name is configured.
func (r *Router) Has(name string) bool {
	_, ok := r.sinks[name]
	return ok
}

// Dispatch sends a to the named channels. An empty list means every channel.
// Errors from individual sinks are joined so one broken channel does not stop
// the others.
func (r *Router) Dispatch(ctx context.Context, a Alert, channels []string) error {
	if len(r.sinks) == 0 {
		return nil
	}
	if a.At.IsZero() {
		a.At = r.clk.Now()
	}
	if !r.admit(a) {
		return nil
	}

	targets := channels
	if len(targets) == 0 {
		targets = r.order
	}

	var errs []error
	for _, name := range targets {
		s, ok := r.sinks[name]
		if !ok {
			errs = append(errs, fmt.Errorf("unknown alert channel %q", name))
			continue
		}
		if err := s.Send(ctx, a); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
		}
	}
	return errors.Join(errs...)
}

// admit applies deduplication. A resolution always passes and clears the key
// so the next failure alerts immediately rather than being suppressed by the
// previous outage's entry.
func (r *Router) admit(a Alert) bool {
	if a.DedupeKey == "" || r.window <= 0 {
		return true
	}
	now := r.clk.Now()

	r.mu.Lock()
	defer r.mu.Unlock()
	r.gc(now)

	if a.Resolved {
		delete(r.sent, a.DedupeKey)
		return true
	}
	if last, ok := r.sent[a.DedupeKey]; ok && now.Sub(last) < r.window {
		return false
	}
	r.sent[a.DedupeKey] = now
	return true
}

// gc drops entries older than the dedupe window so the map cannot grow
// without bound in a long-running process. Caller holds r.mu.
func (r *Router) gc(now time.Time) {
	for k, t := range r.sent {
		if now.Sub(t) >= r.window {
			delete(r.sent, k)
		}
	}
}

// ---------- Slack ----------

// Slack posts Block Kit messages to an incoming webhook.
type Slack struct {
	channel config.Channel
	hc      *http.Client
}

// Name returns the channel name.
func (s *Slack) Name() string { return s.channel.Name }

// Send posts the alert to Slack.
func (s *Slack) Send(ctx context.Context, a Alert) error {
	emoji, color := ":white_check_mark:", "#2eb886"
	if !a.Resolved {
		emoji, color = severityEmoji(a.Severity), severityColor(a.Severity)
	}
	fields := []map[string]string{
		{"type": "mrkdwn", "text": "*Severity*\n" + string(a.Severity)},
		{"type": "mrkdwn", "text": "*When*\n" + a.At.UTC().Format(time.RFC1123)},
	}
	if a.ServiceName != "" {
		fields = append(fields, map[string]string{"type": "mrkdwn", "text": "*Service*\n" + a.ServiceName})
	}
	blocks := []map[string]any{
		{"type": "section", "text": map[string]string{"type": "mrkdwn", "text": fmt.Sprintf("%s *%s*", emoji, a.Title)}},
	}
	if a.Body != "" {
		blocks = append(blocks, map[string]any{
			"type": "section",
			"text": map[string]string{"type": "mrkdwn", "text": a.Body},
		})
	}
	blocks = append(blocks, map[string]any{"type": "section", "fields": fields})
	if a.URL != "" {
		blocks = append(blocks, map[string]any{
			"type": "section",
			"text": map[string]string{"type": "mrkdwn", "text": "<" + a.URL + "|Open status page>"},
		})
	}

	payload := map[string]any{
		"text": a.Title, // fallback for notifications and screen readers
		"attachments": []map[string]any{
			{"color": color, "blocks": blocks},
		},
	}
	return postJSON(ctx, s.hc, s.channel.URL, s.channel.Headers, payload, nil)
}

// ---------- Discord ----------

// Discord posts embeds to a Discord webhook.
type Discord struct {
	channel config.Channel
	hc      *http.Client
}

// Name returns the channel name.
func (d *Discord) Name() string { return d.channel.Name }

// Send posts the alert to Discord.
func (d *Discord) Send(ctx context.Context, a Alert) error {
	embed := map[string]any{
		"title":       a.Title,
		"description": a.Body,
		"color":       discordColor(a),
		"timestamp":   a.At.UTC().Format(time.RFC3339),
	}
	if a.URL != "" {
		embed["url"] = a.URL
	}
	if a.ServiceName != "" {
		embed["fields"] = []map[string]any{
			{"name": "Service", "value": a.ServiceName, "inline": true},
			{"name": "Severity", "value": string(a.Severity), "inline": true},
		}
	}
	payload := map[string]any{"embeds": []map[string]any{embed}}
	return postJSON(ctx, d.hc, d.channel.URL, d.channel.Headers, payload, nil)
}

// ---------- Webhook ----------

// Webhook posts the alert as JSON, optionally signed.
type Webhook struct {
	channel config.Channel
	hc      *http.Client
}

// Name returns the channel name.
func (w *Webhook) Name() string { return w.channel.Name }

// Send posts the alert as a JSON body. When the channel has a secret, the
// body is signed with HMAC-SHA256 and sent in X-Signature-256 as
// "sha256=<hex>", so the receiver can verify the payload came from us.
func (w *Webhook) Send(ctx context.Context, a Alert) error {
	sign := func(body []byte, h http.Header) {
		if w.channel.Secret == "" {
			return
		}
		mac := hmac.New(sha256.New, []byte(w.channel.Secret))
		mac.Write(body)
		h.Set("X-Signature-256", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	}
	return postJSON(ctx, w.hc, w.channel.URL, w.channel.Headers, a, sign)
}

// ---------- Email ----------

// Email delivers alerts over SMTP.
type Email struct {
	channel config.Channel
	// sendMail is injectable for tests; nil uses smtp.SendMail.
	sendMail func(addr string, auth smtp.Auth, from string, to []string, msg []byte) error
}

// Name returns the channel name.
func (e *Email) Name() string { return e.channel.Name }

// Send delivers the alert as a plain-text email.
func (e *Email) Send(_ context.Context, a Alert) error {
	s := e.channel.SMTP
	from := s.From
	if from == "" {
		from = s.Username
	}
	if from == "" {
		return errors.New("email channel needs smtp.from or smtp.username")
	}

	subject := a.Title
	if a.Resolved {
		subject = "[RESOLVED] " + subject
	}
	var b bytes.Buffer
	fmt.Fprintf(&b, "From: %s\r\n", from)
	fmt.Fprintf(&b, "To: %s\r\n", strings.Join(s.To, ", "))
	fmt.Fprintf(&b, "Subject: %s\r\n", sanitizeHeader(subject))
	fmt.Fprintf(&b, "Date: %s\r\n", a.At.UTC().Format(time.RFC1123Z))
	b.WriteString("MIME-Version: 1.0\r\n")
	b.WriteString("Content-Type: text/plain; charset=utf-8\r\n\r\n")
	if a.ServiceName != "" {
		fmt.Fprintf(&b, "Service: %s\r\n", a.ServiceName)
	}
	fmt.Fprintf(&b, "Severity: %s\r\n", a.Severity)
	fmt.Fprintf(&b, "When: %s\r\n\r\n", a.At.UTC().Format(time.RFC1123))
	b.WriteString(a.Body)
	if a.URL != "" {
		fmt.Fprintf(&b, "\r\n\r\n%s\r\n", a.URL)
	}

	port := s.Port
	if port == 0 {
		port = 587
	}
	addr := fmt.Sprintf("%s:%d", s.Host, port)

	var auth smtp.Auth
	if s.Username != "" {
		auth = smtp.PlainAuth("", s.Username, s.Password, s.Host)
	}
	send := e.sendMail
	if send == nil {
		send = smtp.SendMail
	}
	return send(addr, auth, from, s.To, b.Bytes())
}

// sanitizeHeader strips CR and LF so an alert title cannot inject extra
// headers into the message.
func sanitizeHeader(s string) string {
	return strings.NewReplacer("\r", " ", "\n", " ").Replace(s)
}

// ---------- shared helpers ----------

func postJSON(ctx context.Context, hc *http.Client, url string, headers map[string]string, payload any, sign func([]byte, http.Header)) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "status-page/1.0")
	// Sort header names so request construction is deterministic in tests.
	names := make([]string, 0, len(headers))
	for k := range headers {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		req.Header.Set(k, headers[k])
	}
	if sign != nil {
		sign(body, req.Header)
	}

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// Drain a bounded amount so the connection can be reused.
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("%s returned %s: %s", url, resp.Status, strings.TrimSpace(string(snippet)))
	}
	return nil
}

func severityEmoji(s Severity) string {
	switch s {
	case SeverityCritical:
		return ":rotating_light:"
	case SeverityWarning:
		return ":warning:"
	default:
		return ":information_source:"
	}
}

func severityColor(s Severity) string {
	switch s {
	case SeverityCritical:
		return "#d00000"
	case SeverityWarning:
		return "#e8a33d"
	default:
		return "#3b82f6"
	}
}

func discordColor(a Alert) int {
	if a.Resolved {
		return 0x2EB886
	}
	switch a.Severity {
	case SeverityCritical:
		return 0xD00000
	case SeverityWarning:
		return 0xE8A33D
	default:
		return 0x3B82F6
	}
}
