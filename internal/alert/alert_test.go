package alert

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MahyarNemati/status-page/internal/clock"
)

type recorder struct {
	name string
	mu   sync.Mutex
	got  []Notification
	fail int32 // number of initial sends to fail
	perm bool
}

func (r *recorder) Name() string { return r.name }

func (r *recorder) Send(_ context.Context, n Notification) error {
	if atomic.AddInt32(&r.fail, -1) >= 0 {
		if r.perm {
			return &PermanentError{Err: errors.New("bad request")}
		}
		return errors.New("temporary")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.got = append(r.got, n)
	return nil
}

func (r *recorder) kinds() []Kind {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []Kind
	for _, n := range r.got {
		out = append(out, n.Kind)
	}
	return out
}

func opened(key string) Notification {
	return Notification{Kind: IncidentOpened, Key: key, Title: "api is down", Severity: "major"}
}

func resolved(key string) Notification {
	return Notification{Kind: IncidentResolved, Key: key, Title: "api recovered"}
}

func TestDispatcherDedupeAndRecovery(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	rec := &recorder{name: "ops"}
	d := NewDispatcher([]Notifier{rec}, DispatcherOptions{DedupeWindow: 30 * time.Minute, Clock: fc})
	ctx := context.Background()

	if out := d.Deliver(ctx, resolved("incident:api")); out["ops"] != Suppressed {
		t.Fatalf("recovery without firing should be suppressed, got %v", out)
	}
	if out := d.Deliver(ctx, opened("incident:api")); out["ops"] != Sent {
		t.Fatalf("first firing: %v", out)
	}
	fc.Advance(5 * time.Minute)
	if out := d.Deliver(ctx, opened("incident:api")); out["ops"] != Suppressed {
		t.Fatalf("duplicate firing inside window: %v", out)
	}
	if out := d.Deliver(ctx, opened("incident:db")); out["ops"] != Sent {
		t.Fatalf("different key must not be deduped: %v", out)
	}
	fc.Advance(30 * time.Minute)
	if out := d.Deliver(ctx, opened("incident:api")); out["ops"] != Sent {
		t.Fatalf("firing after window should act as reminder: %v", out)
	}
	if out := d.Deliver(ctx, resolved("incident:api")); out["ops"] != Sent {
		t.Fatalf("recovery: %v", out)
	}
	if out := d.Deliver(ctx, resolved("incident:api")); out["ops"] != Suppressed {
		t.Fatalf("second recovery: %v", out)
	}
	if out := d.Deliver(ctx, opened("incident:api")); out["ops"] != Sent {
		t.Fatalf("new firing after recovery: %v", out)
	}
	want := []Kind{IncidentOpened, IncidentOpened, IncidentOpened, IncidentResolved, IncidentOpened}
	if got := rec.kinds(); strings.Join(kindStrings(got), ",") != strings.Join(kindStrings(want), ",") {
		t.Fatalf("delivered %v, want %v", got, want)
	}
}

func kindStrings(ks []Kind) []string {
	out := make([]string, len(ks))
	for i, k := range ks {
		out[i] = string(k)
	}
	return out
}

func TestDispatcherChannelRouting(t *testing.T) {
	a, b := &recorder{name: "a"}, &recorder{name: "b"}
	d := NewDispatcher([]Notifier{a, b}, DispatcherOptions{})
	n := opened("k")
	n.Channels = []string{"b"}
	out := d.Deliver(context.Background(), n)
	if len(out) != 1 || out["b"] != Sent || len(a.kinds()) != 0 {
		t.Fatalf("routing: %v a=%v", out, a.kinds())
	}
}

func TestDispatcherRetries(t *testing.T) {
	var (
		mu       sync.Mutex
		observed []Outcome
	)
	transient := &recorder{name: "t", fail: 2}
	perm := &recorder{name: "p", fail: 1, perm: true}
	d := NewDispatcher([]Notifier{transient, perm}, DispatcherOptions{
		Attempts: 3,
		Backoff:  time.Millisecond,
		Observer: func(_ string, _ Kind, o Outcome) {
			mu.Lock()
			defer mu.Unlock()
			observed = append(observed, o)
		},
	})
	out := d.Deliver(context.Background(), opened("k"))
	if out["t"] != Sent {
		t.Fatalf("transient failures should be retried: %v", out)
	}
	if out["p"] != Failed || atomic.LoadInt32(&perm.fail) != 0 {
		t.Fatalf("permanent failure should not be retried: %v", out)
	}
	if len(observed) != 2 {
		t.Fatalf("observer calls = %v", observed)
	}
}

func TestDispatcherRunDrainsQueue(t *testing.T) {
	rec := &recorder{name: "r"}
	d := NewDispatcher([]Notifier{rec}, DispatcherOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { d.Run(ctx); close(done) }()
	d.Publish(opened("a"))
	d.Publish(opened("b"))
	cancel()
	<-done
	if n := len(rec.kinds()); n != 2 {
		t.Fatalf("delivered %d, want 2", n)
	}
}

func capture(t *testing.T, status int) (*httptest.Server, <-chan *http.Request, <-chan []byte) {
	t.Helper()
	reqs := make(chan *http.Request, 4)
	bodies := make(chan []byte, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		reqs <- r
		bodies <- b
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv, reqs, bodies
}

func sample() Notification {
	return Notification{
		Kind: IncidentOpened, Key: "incident:api", ServiceID: "api", ServiceName: "Public API",
		Severity: "major", Title: "Public API is down", Message: "unexpected status 503",
		URL: "https://status.example.com", At: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC),
	}
}

func TestSlackPayload(t *testing.T) {
	srv, _, bodies := capture(t, http.StatusOK)
	s := &Slack{ChannelName: "slack", URL: srv.URL, Client: srv.Client()}
	if err := s.Send(context.Background(), sample()); err != nil {
		t.Fatal(err)
	}
	var got struct {
		Text        string `json:"text"`
		Attachments []struct {
			Color, Title, Text, TitleLink string
		} `json:"attachments"`
	}
	if err := json.Unmarshal(<-bodies, &got); err != nil {
		t.Fatal(err)
	}
	if got.Text != "[MAJOR] Public API is down" || len(got.Attachments) != 1 || got.Attachments[0].Color != "#e01e5a" {
		t.Fatalf("payload = %+v", got)
	}
}

func TestDiscordPayload(t *testing.T) {
	srv, _, bodies := capture(t, http.StatusNoContent)
	d := &Discord{ChannelName: "discord", URL: srv.URL, Client: srv.Client()}
	n := sample()
	n.Kind = IncidentResolved
	if err := d.Send(context.Background(), n); err != nil {
		t.Fatal(err)
	}
	var got struct {
		Embeds []struct {
			Title string `json:"title"`
			Color int    `json:"color"`
		} `json:"embeds"`
	}
	if err := json.Unmarshal(<-bodies, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Embeds) != 1 || got.Embeds[0].Title != "[RESOLVED] Public API is down" || got.Embeds[0].Color != 0x2eb67d {
		t.Fatalf("payload = %+v", got)
	}
}

func TestWebhookSignatureAndHeaders(t *testing.T) {
	srv, reqs, bodies := capture(t, http.StatusAccepted)
	w := &Webhook{ChannelName: "hook", URL: srv.URL, Secret: "s3cret", Headers: map[string]string{"X-Team": "sre"}, Client: srv.Client()}
	if err := w.Send(context.Background(), sample()); err != nil {
		t.Fatal(err)
	}
	r, body := <-reqs, <-bodies
	mac := hmac.New(sha256.New, []byte("s3cret"))
	mac.Write(body)
	if want := "sha256=" + hex.EncodeToString(mac.Sum(nil)); r.Header.Get("X-Signature-256") != want {
		t.Fatalf("signature = %q, want %q", r.Header.Get("X-Signature-256"), want)
	}
	if r.Header.Get("X-Team") != "sre" {
		t.Fatal("custom header missing")
	}
	var n Notification
	if err := json.Unmarshal(body, &n); err != nil || n.Kind != IncidentOpened || n.ServiceID != "api" {
		t.Fatalf("body = %s (%v)", body, err)
	}
}

func TestWebhookErrorClassification(t *testing.T) {
	for _, tc := range []struct {
		status    int
		permanent bool
	}{{400, true}, {404, true}, {429, false}, {500, false}, {503, false}} {
		srv, _, _ := capture(t, tc.status)
		err := (&Webhook{ChannelName: "h", URL: srv.URL, Client: srv.Client()}).Send(context.Background(), sample())
		var perm *PermanentError
		if err == nil || errors.As(err, &perm) != tc.permanent {
			t.Fatalf("status %d: err=%v permanent=%v", tc.status, err, errors.As(err, &perm))
		}
	}
}

// fakeSMTP is a minimal plaintext SMTP server that records one message.
func fakeSMTP(t *testing.T) (host string, port int, msgs <-chan string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	out := make(chan string, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		r := bufio.NewReader(conn)
		reply := func(s string) { _, _ = io.WriteString(conn, s+"\r\n") }
		reply("220 fake ESMTP")
		var data strings.Builder
		var rcpts []string
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			cmd := strings.ToUpper(strings.TrimSpace(line))
			switch {
			case strings.HasPrefix(cmd, "EHLO"):
				reply("250-fake")
				reply("250 8BITMIME")
			case strings.HasPrefix(cmd, "MAIL"):
				reply("250 ok")
			case strings.HasPrefix(cmd, "RCPT"):
				rcpts = append(rcpts, strings.TrimSpace(line[8:]))
				reply("250 ok")
			case cmd == "DATA":
				reply("354 go ahead")
				for {
					l, err := r.ReadString('\n')
					if err != nil {
						return
					}
					if l == ".\r\n" {
						break
					}
					data.WriteString(l)
				}
				reply("250 queued")
			case cmd == "QUIT":
				reply("221 bye")
				out <- strings.Join(rcpts, ",") + "\n" + data.String()
				return
			default:
				reply("502 unsupported")
			}
		}
	}()
	addr := ln.Addr().(*net.TCPAddr)
	return addr.IP.String(), addr.Port, out
}

func TestEmail(t *testing.T) {
	host, port, msgs := fakeSMTP(t)
	e := &Email{ChannelName: "mail", Host: host, Port: port, From: "status@example.com", To: []string{"oncall@example.com", "sre@example.com"}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e.Send(ctx, sample()); err != nil {
		t.Fatal(err)
	}
	msg := <-msgs
	for _, want := range []string{
		"<oncall@example.com>,<sre@example.com>",
		"Subject: [MAJOR] Public API is down",
		"To: oncall@example.com, sre@example.com",
		"unexpected status 503\r\n",
		"Status page: https://status.example.com",
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("message missing %q:\n%s", want, msg)
		}
	}
}
