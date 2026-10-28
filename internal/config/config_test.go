package config

import (
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

const minimal = `
services:
  - id: api
    type: http
    target: https://example.com/health
`

func TestParseAppliesDefaults(t *testing.T) {
	cfg, err := Parse([]byte(minimal), env(nil))
	if err != nil {
		t.Fatal(err)
	}
	s := cfg.Services[0]
	if s.Name != "api" || s.Interval.D() != time.Minute || s.Timeout.D() != 10*time.Second {
		t.Fatalf("defaults not applied: %+v", s)
	}
	if *s.Retries != 0 || s.FailureThreshold != 2 || s.HTTP.Method != "GET" {
		t.Fatalf("defaults not applied: %+v", s)
	}
	if cfg.Storage.Driver != "sqlite" || cfg.Server.UptimeDays != 90 || len(cfg.SLO.BurnRules) != 2 {
		t.Fatalf("global defaults not applied: %+v", cfg)
	}
}

func TestParseEnvExpansionAndOverrides(t *testing.T) {
	doc := `
server:
  admin_token: from-file
alerts:
  channels:
    - name: ops
      type: slack
      url: ${SLACK_URL}
services:
  - id: db
    type: tcp
    target: db.internal:5432
    alerts: [ops]
`
	cfg, err := Parse([]byte(doc), env(map[string]string{
		"SLACK_URL":              "https://hooks.slack.test/abc",
		"STATUSPAGE_ADMIN_TOKEN": "from-env",
		"STATUSPAGE_WORKERS":     "3",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Alerts.Channels[0].URL != "https://hooks.slack.test/abc" {
		t.Fatalf("url = %q", cfg.Alerts.Channels[0].URL)
	}
	if cfg.Server.AdminToken != "from-env" || cfg.Scheduler.Workers != 3 {
		t.Fatalf("env overrides not applied: %+v %+v", cfg.Server, cfg.Scheduler)
	}
}

func TestParseMissingEnv(t *testing.T) {
	_, err := Parse([]byte("server:\n  admin_token: ${NOPE}\n"), env(nil))
	if err == nil || !strings.Contains(err.Error(), "NOPE") {
		t.Fatalf("err = %v", err)
	}
}

func TestParseRejectsUnknownFields(t *testing.T) {
	_, err := Parse([]byte("server:\n  listn: ':80'\n"), env(nil))
	if err == nil {
		t.Fatal("expected error for unknown field")
	}
}

func TestValidateCollectsErrors(t *testing.T) {
	doc := `
services:
  - id: Bad ID
    type: http
    target: ftp://x
  - id: dup
    type: tcp
    target: nohost
    interval: 10s
    timeout: 20s
  - id: dup
    type: dns
    target: example.com
    dns: { record: SRV }
    slo: { target: 100 }
    alerts: [missing]
`
	_, err := Parse([]byte(doc), env(nil))
	if err == nil {
		t.Fatal("expected validation error")
	}
	for _, want := range []string{
		"id must match",
		"http(s) URL",
		"host:port",
		"timeout must be positive",
		"duplicate id",
		"unsupported dns.record",
		"slo.target",
		"unknown alert channel",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("missing %q in:\n%v", want, err)
		}
	}
}

func TestParseDuration(t *testing.T) {
	cases := map[string]time.Duration{"30s": 30 * time.Second, "2d": 48 * time.Hour, "1h30m": 90 * time.Minute}
	for in, want := range cases {
		got, err := ParseDuration(in)
		if err != nil || got != want {
			t.Errorf("ParseDuration(%q) = %v, %v", in, got, err)
		}
	}
	if _, err := ParseDuration("1.5d"); err == nil {
		t.Error("expected error for fractional days")
	}
}
