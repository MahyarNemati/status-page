package alert

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// postJSON sends body to url and classifies the response. 4xx responses
// other than 408 and 429 are permanent.
func postJSON(ctx context.Context, client *http.Client, url string, body any, headers map[string]string, secret string) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return &PermanentError{Err: fmt.Errorf("encode payload: %w", err)}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return &PermanentError{Err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "status-page/1")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	if secret != "" {
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write(payload)
		req.Header.Set("X-Signature-256", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	err = fmt.Errorf("%s returned %d: %s", url, resp.StatusCode, bytes.TrimSpace(snippet))
	if resp.StatusCode >= 400 && resp.StatusCode < 500 &&
		resp.StatusCode != http.StatusRequestTimeout && resp.StatusCode != http.StatusTooManyRequests {
		return &PermanentError{Err: err}
	}
	return err
}

func color(n Notification) (hex string, rgb int) {
	switch {
	case n.Kind.Recovery():
		return "#2eb67d", 0x2eb67d
	case n.Severity == "critical" || n.Severity == "major":
		return "#e01e5a", 0xe01e5a
	default:
		return "#ecb22e", 0xecb22e
	}
}

// Slack posts to an incoming webhook.
type Slack struct {
	ChannelName string
	URL         string
	Client      *http.Client
}

// Name implements Notifier.
func (s *Slack) Name() string { return s.ChannelName }

// Send implements Notifier.
func (s *Slack) Send(ctx context.Context, n Notification) error {
	c, _ := color(n)
	attachment := map[string]any{
		"color":  c,
		"title":  n.Title,
		"text":   n.Message,
		"footer": n.ServiceName,
		"ts":     n.At.Unix(),
	}
	if n.URL != "" {
		attachment["title_link"] = n.URL
	}
	body := map[string]any{
		"text":        n.Subject(),
		"attachments": []any{attachment},
	}
	return postJSON(ctx, s.Client, s.URL, body, nil, "")
}

// Discord posts to a Discord webhook.
type Discord struct {
	ChannelName string
	URL         string
	Client      *http.Client
}

// Name implements Notifier.
func (d *Discord) Name() string { return d.ChannelName }

// Send implements Notifier.
func (d *Discord) Send(ctx context.Context, n Notification) error {
	_, rgb := color(n)
	embed := map[string]any{
		"title":       n.Subject(),
		"description": n.Message,
		"color":       rgb,
		"timestamp":   n.At.UTC().Format(time.RFC3339),
		"footer":      map[string]string{"text": n.ServiceName},
	}
	if n.URL != "" {
		embed["url"] = n.URL
	}
	body := map[string]any{
		"username":         "status-page",
		"embeds":           []any{embed},
		"allowed_mentions": map[string]any{"parse": []string{}},
	}
	return postJSON(ctx, d.Client, d.URL, body, nil, "")
}

// Webhook posts the notification as JSON. When Secret is set the body is
// signed with HMAC-SHA256 in the X-Signature-256 header.
type Webhook struct {
	ChannelName string
	URL         string
	Headers     map[string]string
	Secret      string
	Client      *http.Client
}

// Name implements Notifier.
func (w *Webhook) Name() string { return w.ChannelName }

// Send implements Notifier.
func (w *Webhook) Send(ctx context.Context, n Notification) error {
	return postJSON(ctx, w.Client, w.URL, n, w.Headers, w.Secret)
}
