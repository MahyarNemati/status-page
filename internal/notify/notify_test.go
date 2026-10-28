package notify

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/smtp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MahyarNemati/status-page/internal/clock"
	"github.com/MahyarNemati/status-page/internal/config"
)

func dur(d time.Duration) config.Duration { return config.Duration(d) }

func alert(key string) Alert {
	return Alert{
		DedupeKey:   key,
		Severity:    SeverityCritical,
		Title:       "api is down",
		Body:        "connection refused",
		ServiceID:   "api",
		ServiceName: "API",
		At:          time.Unix(1700000000, 0).UTC(),
	}
}

type capture struct {
	hits  atomic.Int64
	last  atomic.Value // string body
	sig   atomic.Value // string signature
	code  int
	ctype atomic.Value
}

func newCaptureServer(t *testing.T, c *capture) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		c.hits.Add(1)
		c.last.Store(string(b))
		c.sig.Store(r.Header.Get("X-Signature-256"))
		c.ctype.Store(r.Header.Get("Content-Type"))
		if c.code != 0 {
			w.WriteHeader(c.code)
			_, _ = w.Write([]byte("rejected"))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestWebhookPostsSignedJSON(t *testing.T) {
	c := &capture{}
	srv := newCaptureServer(t, c)

	secret := "s3cr3t"
	r, err := NewRouter(config.Alerts{
		Channels: []config.Channel{{Name: "hook", Type: "webhook", URL: srv.URL, Secret: secret}},
	}, clock.Real())
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}

	if err := r.Dispatch(context.Background(), alert("k1"), nil); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if got := c.hits.Load(); got != 1 {
		t.Fatalf("hits = %d, want 1", got)
	}
	if got := c.ctype.Load(); got != "application/json" {
		t.Errorf("Content-Type = %v, want application/json", got)
	}

	body, _ := c.last.Load().(string)
	var decoded Alert
	if err := json.Unmarshal([]byte(body), &decoded); err != nil {
		t.Fatalf("payload is not an Alert: %v", err)
	}
	if decoded.Title != "api is down" || decoded.ServiceID != "api" {
		t.Errorf("payload lost fields: %+v", decoded)
	}

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(body))
	want := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if got, _ := c.sig.Load().(string); got != want {
		t.Errorf("signature = %q, want %q", got, want)
	}
}

func TestWebhookWithoutSecretIsUnsigned(t *testing.T) {
	c := &capture{}
	srv := newCaptureServer(t, c)
	r, _ := NewRouter(config.Alerts{
		Channels: []config.Channel{{Name: "hook", Type: "webhook", URL: srv.URL}},
	}, clock.Real())

	if err := r.Dispatch(context.Background(), alert("k1"), nil); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if got, _ := c.sig.Load().(string); got != "" {
		t.Errorf("signature = %q, want empty", got)
	}
}

func TestNon2xxIsAnError(t *testing.T) {
	c := &capture{code: http.StatusInternalServerError}
	srv := newCaptureServer(t, c)
	r, _ := NewRouter(config.Alerts{
		Channels: []config.Channel{{Name: "hook", Type: "webhook", URL: srv.URL}},
	}, clock.Real())

	err := r.Dispatch(context.Background(), alert("k1"), nil)
	if err == nil {
		t.Fatal("expected an error for a 500 response")
	}
	if !strings.Contains(err.Error(), "hook") {
		t.Errorf("error should name the channel, got %v", err)
	}
}

func TestDedupeSuppressesRepeatsInsideWindow(t *testing.T) {
	c := &capture{}
	srv := newCaptureServer(t, c)
	fake := clock.NewFake(time.Unix(1700000000, 0))
	r, _ := NewRouter(config.Alerts{
		DedupeWindow: dur(5 * time.Minute),
		Channels:     []config.Channel{{Name: "hook", Type: "webhook", URL: srv.URL}},
	}, fake)

	for i := 0; i < 4; i++ {
		if err := r.Dispatch(context.Background(), alert("same"), nil); err != nil {
			t.Fatalf("Dispatch %d: %v", i, err)
		}
		fake.Advance(time.Minute)
	}
	if got := c.hits.Load(); got != 1 {
		t.Fatalf("hits = %d, want 1 (repeats inside the window must be suppressed)", got)
	}

	// Past the window, it alerts again.
	fake.Advance(5 * time.Minute)
	if err := r.Dispatch(context.Background(), alert("same"), nil); err != nil {
		t.Fatalf("Dispatch after window: %v", err)
	}
	if got := c.hits.Load(); got != 2 {
		t.Fatalf("hits = %d, want 2 after the window expired", got)
	}
}

func TestDistinctKeysAreNotSuppressed(t *testing.T) {
	c := &capture{}
	srv := newCaptureServer(t, c)
	fake := clock.NewFake(time.Unix(1700000000, 0))
	r, _ := NewRouter(config.Alerts{
		DedupeWindow: dur(time.Hour),
		Channels:     []config.Channel{{Name: "hook", Type: "webhook", URL: srv.URL}},
	}, fake)

	_ = r.Dispatch(context.Background(), alert("api-down"), nil)
	_ = r.Dispatch(context.Background(), alert("db-down"), nil)
	if got := c.hits.Load(); got != 2 {
		t.Fatalf("hits = %d, want 2 (different services must both alert)", got)
	}
}

func TestResolutionIsNeverSuppressedAndResetsTheKey(t *testing.T) {
	c := &capture{}
	srv := newCaptureServer(t, c)
	fake := clock.NewFake(time.Unix(1700000000, 0))
	r, _ := NewRouter(config.Alerts{
		DedupeWindow: dur(time.Hour),
		Channels:     []config.Channel{{Name: "hook", Type: "webhook", URL: srv.URL}},
	}, fake)

	_ = r.Dispatch(context.Background(), alert("api"), nil) // 1: firing
	resolved := alert("api")
	resolved.Resolved = true
	_ = r.Dispatch(context.Background(), resolved, nil) // 2: resolution passes
	if got := c.hits.Load(); got != 2 {
		t.Fatalf("hits = %d, want 2 (a resolution must never be suppressed)", got)
	}

	// Having resolved, a fresh failure must alert immediately rather than be
	// swallowed by the previous outage's dedupe entry.
	fake.Advance(time.Second)
	_ = r.Dispatch(context.Background(), alert("api"), nil)
	if got := c.hits.Load(); got != 3 {
		t.Fatalf("hits = %d, want 3 (re-failure after recovery must alert)", got)
	}
}

func TestRoutingSendsOnlyToSubscribedChannels(t *testing.T) {
	a, b := &capture{}, &capture{}
	sa, sb := newCaptureServer(t, a), newCaptureServer(t, b)
	r, err := NewRouter(config.Alerts{
		Channels: []config.Channel{
			{Name: "ops", Type: "webhook", URL: sa.URL},
			{Name: "noisy", Type: "webhook", URL: sb.URL},
		},
	}, clock.Real())
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}

	if err := r.Dispatch(context.Background(), alert("k"), []string{"ops"}); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if got := a.hits.Load(); got != 1 {
		t.Errorf("ops hits = %d, want 1", got)
	}
	if got := b.hits.Load(); got != 0 {
		t.Errorf("noisy hits = %d, want 0", got)
	}
}

func TestEmptyChannelListFansOutToAll(t *testing.T) {
	a, b := &capture{}, &capture{}
	sa, sb := newCaptureServer(t, a), newCaptureServer(t, b)
	r, _ := NewRouter(config.Alerts{
		Channels: []config.Channel{
			{Name: "ops", Type: "webhook", URL: sa.URL},
			{Name: "all", Type: "webhook", URL: sb.URL},
		},
	}, clock.Real())

	_ = r.Dispatch(context.Background(), alert("k"), nil)
	if a.hits.Load() != 1 || b.hits.Load() != 1 {
		t.Errorf("hits = %d/%d, want 1/1", a.hits.Load(), b.hits.Load())
	}
}

func TestOneBrokenChannelDoesNotStopTheOthers(t *testing.T) {
	good := &capture{}
	bad := &capture{code: http.StatusBadGateway}
	sgood, sbad := newCaptureServer(t, good), newCaptureServer(t, bad)
	r, _ := NewRouter(config.Alerts{
		Channels: []config.Channel{
			{Name: "bad", Type: "webhook", URL: sbad.URL},
			{Name: "good", Type: "webhook", URL: sgood.URL},
		},
	}, clock.Real())

	err := r.Dispatch(context.Background(), alert("k"), nil)
	if err == nil {
		t.Error("expected the broken channel to report an error")
	}
	if got := good.hits.Load(); got != 1 {
		t.Errorf("good channel hits = %d, want 1 despite the broken one", got)
	}
}

func TestUnknownChannelTypeFailsAtConstruction(t *testing.T) {
	_, err := NewRouter(config.Alerts{
		Channels: []config.Channel{{Name: "x", Type: "carrier-pigeon", URL: "http://x"}},
	}, clock.Real())
	if err == nil {
		t.Fatal("expected an error: a typo in a channel type must fail at startup")
	}
}

func TestDuplicateChannelNameIsRejected(t *testing.T) {
	_, err := NewRouter(config.Alerts{
		Channels: []config.Channel{
			{Name: "ops", Type: "webhook", URL: "http://a"},
			{Name: "ops", Type: "webhook", URL: "http://b"},
		},
	}, clock.Real())
	if err == nil {
		t.Fatal("expected duplicate channel names to be rejected")
	}
}

func TestSlackPayloadHasPlainTextFallback(t *testing.T) {
	c := &capture{}
	srv := newCaptureServer(t, c)
	r, _ := NewRouter(config.Alerts{
		Channels: []config.Channel{{Name: "slack", Type: "slack", URL: srv.URL}},
	}, clock.Real())

	_ = r.Dispatch(context.Background(), alert("k"), nil)
	body, _ := c.last.Load().(string)
	var payload struct {
		Text        string `json:"text"`
		Attachments []struct {
			Color  string           `json:"color"`
			Blocks []map[string]any `json:"blocks"`
		} `json:"attachments"`
	}
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatalf("bad slack payload: %v", err)
	}
	if payload.Text == "" {
		t.Error("slack payload needs a top-level text fallback for notifications")
	}
	if len(payload.Attachments) != 1 || len(payload.Attachments[0].Blocks) == 0 {
		t.Errorf("slack payload missing blocks: %s", body)
	}
}

func TestDiscordUsesGreenWhenResolved(t *testing.T) {
	c := &capture{}
	srv := newCaptureServer(t, c)
	r, _ := NewRouter(config.Alerts{
		Channels: []config.Channel{{Name: "d", Type: "discord", URL: srv.URL}},
	}, clock.Real())

	a := alert("k")
	a.Resolved = true
	_ = r.Dispatch(context.Background(), a, nil)

	body, _ := c.last.Load().(string)
	var payload struct {
		Embeds []struct {
			Color int `json:"color"`
		} `json:"embeds"`
	}
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatalf("bad discord payload: %v", err)
	}
	if len(payload.Embeds) != 1 {
		t.Fatalf("want 1 embed, got %d", len(payload.Embeds))
	}
	if payload.Embeds[0].Color != 0x2EB886 {
		t.Errorf("resolved color = %#x, want 0x2eb886", payload.Embeds[0].Color)
	}
}

func TestEmailBuildsRFC5322MessageAndStripsHeaderInjection(t *testing.T) {
	var gotTo []string
	var gotFrom, gotAddr string
	var msg []byte

	e := &Email{
		channel: config.Channel{
			Name: "mail",
			Type: "email",
			SMTP: config.SMTP{Host: "smtp.example.com", Port: 2525, From: "bot@example.com", To: []string{"a@example.com", "b@example.com"}},
		},
		sendMail: func(addr string, _ smtp.Auth, from string, to []string, m []byte) error {
			gotAddr, gotFrom, gotTo, msg = addr, from, to, m
			return nil
		},
	}

	a := alert("k")
	// A title containing CRLF must not be able to inject extra headers.
	a.Title = "down\r\nBcc: attacker@evil.example"
	if err := e.Send(context.Background(), a); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if gotAddr != "smtp.example.com:2525" {
		t.Errorf("addr = %q", gotAddr)
	}
	if gotFrom != "bot@example.com" || len(gotTo) != 2 {
		t.Errorf("from/to = %q/%v", gotFrom, gotTo)
	}

	text := string(msg)
	headers, _, ok := strings.Cut(text, "\r\n\r\n")
	if !ok {
		t.Fatalf("message has no header/body separator:\n%s", text)
	}
	if strings.Contains(strings.ToLower(headers), "bcc:") {
		t.Errorf("header injection succeeded:\n%s", headers)
	}
	if !strings.Contains(headers, "Subject: down") {
		t.Errorf("subject missing or mangled:\n%s", headers)
	}
}

func TestEmailResolvedSubjectIsPrefixed(t *testing.T) {
	var msg []byte
	e := &Email{
		channel: config.Channel{SMTP: config.SMTP{Host: "h", From: "f@x", To: []string{"t@x"}}},
		sendMail: func(_ string, _ smtp.Auth, _ string, _ []string, m []byte) error {
			msg = m
			return nil
		},
	}
	a := alert("k")
	a.Resolved = true
	if err := e.Send(context.Background(), a); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if !strings.Contains(string(msg), "Subject: [RESOLVED]") {
		t.Errorf("resolved email should be prefixed, got:\n%s", msg)
	}
}

func TestNoChannelsIsNotAnError(t *testing.T) {
	r, err := NewRouter(config.Alerts{}, clock.Real())
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	if err := r.Dispatch(context.Background(), alert("k"), nil); err != nil {
		t.Errorf("Dispatch with no channels should be a no-op, got %v", err)
	}
}
