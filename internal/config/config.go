// Package config loads and validates the YAML configuration, with environment
// overrides for deployment-specific settings and secrets.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// CheckType identifies a probe implementation.
type CheckType string

// Supported probe types.
const (
	CheckHTTP CheckType = "http"
	CheckTCP  CheckType = "tcp"
	CheckDNS  CheckType = "dns"
	CheckICMP CheckType = "icmp"
)

// Config is the root configuration document.
type Config struct {
	Server    Server         `yaml:"server"`
	Storage   Storage        `yaml:"storage"`
	Scheduler Scheduler      `yaml:"scheduler"`
	Defaults  Defaults       `yaml:"defaults"`
	Incidents IncidentPolicy `yaml:"incidents"`
	SLO       SLOPolicy      `yaml:"slo"`
	Alerts    Alerts         `yaml:"alerts"`
	Services  []Service      `yaml:"services"`
}

// Server configures the HTTP listener and status page presentation.
type Server struct {
	Listen       string   `yaml:"listen"`
	Title        string   `yaml:"title"`
	PublicURL    string   `yaml:"public_url"`
	AdminToken   string   `yaml:"admin_token"`
	PageCacheTTL Duration `yaml:"page_cache_ttl"`
	UptimeDays   int      `yaml:"uptime_days"`
}

// Storage selects the persistence backend and retention.
type Storage struct {
	Driver    string    `yaml:"driver"`
	DSN       string    `yaml:"dsn"`
	Retention Retention `yaml:"retention"`
}

// Retention controls how long each resolution of data is kept.
type Retention struct {
	Raw    Duration `yaml:"raw"`
	Hourly Duration `yaml:"hourly"`
	Daily  Duration `yaml:"daily"`
	Every  Duration `yaml:"every"`
}

// Scheduler tunes check execution.
type Scheduler struct {
	Workers int `yaml:"workers"`
	// Jitter is a fraction of the interval; nil means the default of 0.1.
	Jitter *float64 `yaml:"jitter"`
}

// Defaults are applied to services that leave a field unset.
type Defaults struct {
	Interval          Duration `yaml:"interval"`
	Timeout           Duration `yaml:"timeout"`
	Retries           int      `yaml:"retries"`
	RetryDelay        Duration `yaml:"retry_delay"`
	FailureThreshold  int      `yaml:"failure_threshold"`
	RecoveryThreshold int      `yaml:"recovery_threshold"`
}

// IncidentPolicy configures flap detection.
type IncidentPolicy struct {
	FlapWindow    Duration `yaml:"flap_window"`
	FlapThreshold int      `yaml:"flap_threshold"`
}

// SLOPolicy configures error budget evaluation.
type SLOPolicy struct {
	Window        Duration   `yaml:"window"`
	EvaluateEvery Duration   `yaml:"evaluate_every"`
	BurnRules     []BurnRule `yaml:"burn_rules"`
}

// BurnRule is a multi-window burn-rate alert: it fires when the burn rate
// exceeds Factor over both the long and short windows.
type BurnRule struct {
	Name     string   `yaml:"name"`
	Long     Duration `yaml:"long"`
	Short    Duration `yaml:"short"`
	Factor   float64  `yaml:"factor"`
	Severity string   `yaml:"severity"`
}

// Alerts configures notification channels.
type Alerts struct {
	DedupeWindow Duration  `yaml:"dedupe_window"`
	Timeout      Duration  `yaml:"timeout"`
	Channels     []Channel `yaml:"channels"`
}

// Channel is one notification destination.
type Channel struct {
	Name    string            `yaml:"name"`
	Type    string            `yaml:"type"`
	URL     string            `yaml:"url"`
	Headers map[string]string `yaml:"headers"`
	Secret  string            `yaml:"secret"`
	SMTP    SMTP              `yaml:"smtp"`
}

// SMTP holds email delivery settings.
type SMTP struct {
	Host     string   `yaml:"host"`
	Port     int      `yaml:"port"`
	Username string   `yaml:"username"`
	Password string   `yaml:"password"`
	From     string   `yaml:"from"`
	To       []string `yaml:"to"`
}

// Service is one monitored target.
type Service struct {
	ID                string    `yaml:"id"`
	Name              string    `yaml:"name"`
	Group             string    `yaml:"group"`
	Description       string    `yaml:"description"`
	Type              CheckType `yaml:"type"`
	Target            string    `yaml:"target"`
	Interval          Duration  `yaml:"interval"`
	Timeout           Duration  `yaml:"timeout"`
	Retries           *int      `yaml:"retries"`
	RetryDelay        Duration  `yaml:"retry_delay"`
	FailureThreshold  int       `yaml:"failure_threshold"`
	RecoveryThreshold int       `yaml:"recovery_threshold"`
	DegradedLatency   Duration  `yaml:"degraded_latency"`
	Hidden            bool      `yaml:"hidden"`
	Alerts            []string  `yaml:"alerts"`
	HTTP              HTTPCheck `yaml:"http"`
	DNS               DNSCheck  `yaml:"dns"`
	ICMP              ICMPCheck `yaml:"icmp"`
	SLO               *SLO      `yaml:"slo"`
}

// HTTPCheck holds HTTP probe options.
type HTTPCheck struct {
	Method             string            `yaml:"method"`
	Headers            map[string]string `yaml:"headers"`
	Body               string            `yaml:"body"`
	ExpectStatus       []int             `yaml:"expect_status"`
	Keyword            string            `yaml:"keyword"`
	KeywordAbsent      bool              `yaml:"keyword_absent"`
	FollowRedirects    *bool             `yaml:"follow_redirects"`
	InsecureSkipVerify bool              `yaml:"insecure_skip_verify"`
	TLSExpiryWarn      Duration          `yaml:"tls_expiry_warn"`
}

// DNSCheck holds DNS probe options.
type DNSCheck struct {
	Record string   `yaml:"record"`
	Server string   `yaml:"server"`
	Expect []string `yaml:"expect"`
}

// ICMPCheck holds ICMP probe options.
type ICMPCheck struct {
	Privileged bool `yaml:"privileged"`
}

// SLO is a per-service availability objective in percent, e.g. 99.9.
type SLO struct {
	Target float64 `yaml:"target"`
}

// Load reads the file at path, expands ${VAR} references, applies
// environment overrides and defaults, and validates the result.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return Parse(raw, os.LookupEnv)
}

var envRef = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// Parse decodes and validates raw YAML. lookup resolves environment
// variables and is injectable for tests.
func Parse(raw []byte, lookup func(string) (string, bool)) (*Config, error) {
	var missing []string
	expanded := envRef.ReplaceAllFunc(raw, func(m []byte) []byte {
		name := string(m[2 : len(m)-1])
		v, ok := lookup(name)
		if !ok {
			missing = append(missing, name)
		}
		return []byte(v)
	})
	if len(missing) > 0 {
		return nil, fmt.Errorf("config references unset environment variables: %s", strings.Join(missing, ", "))
	}

	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(expanded))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.applyEnv(lookup); err != nil {
		return nil, err
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) applyEnv(lookup func(string) (string, bool)) error {
	str := map[string]*string{
		"STATUSPAGE_LISTEN":      &c.Server.Listen,
		"STATUSPAGE_PUBLIC_URL":  &c.Server.PublicURL,
		"STATUSPAGE_ADMIN_TOKEN": &c.Server.AdminToken,
		"STATUSPAGE_DB_DRIVER":   &c.Storage.Driver,
		"STATUSPAGE_DB_DSN":      &c.Storage.DSN,
	}
	for k, dst := range str {
		if v, ok := lookup(k); ok && v != "" {
			*dst = v
		}
	}
	if v, ok := lookup("STATUSPAGE_WORKERS"); ok && v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("STATUSPAGE_WORKERS: %w", err)
		}
		c.Scheduler.Workers = n
	}
	return nil
}

func (c *Config) applyDefaults() {
	setStr(&c.Server.Listen, ":8080")
	setStr(&c.Server.Title, "Service Status")
	setDur(&c.Server.PageCacheTTL, 5*time.Second)
	if c.Server.UptimeDays == 0 {
		c.Server.UptimeDays = 90
	}

	setStr(&c.Storage.Driver, "sqlite")
	setStr(&c.Storage.DSN, "file:statuspage.db")
	setDur(&c.Storage.Retention.Raw, 7*24*time.Hour)
	setDur(&c.Storage.Retention.Hourly, 90*24*time.Hour)
	setDur(&c.Storage.Retention.Daily, 400*24*time.Hour)
	setDur(&c.Storage.Retention.Every, time.Hour)

	if c.Scheduler.Workers == 0 {
		c.Scheduler.Workers = 8
	}
	if c.Scheduler.Jitter == nil {
		j := 0.1
		c.Scheduler.Jitter = &j
	}

	d := &c.Defaults
	setDur(&d.Interval, time.Minute)
	setDur(&d.Timeout, 10*time.Second)
	setDur(&d.RetryDelay, 2*time.Second)
	if d.FailureThreshold == 0 {
		d.FailureThreshold = 2
	}
	if d.RecoveryThreshold == 0 {
		d.RecoveryThreshold = 2
	}

	setDur(&c.Incidents.FlapWindow, 15*time.Minute)
	if c.Incidents.FlapThreshold == 0 {
		c.Incidents.FlapThreshold = 4
	}

	setDur(&c.SLO.Window, 30*24*time.Hour)
	setDur(&c.SLO.EvaluateEvery, time.Minute)
	if len(c.SLO.BurnRules) == 0 {
		c.SLO.BurnRules = []BurnRule{
			{Name: "fast", Long: Duration(time.Hour), Short: Duration(5 * time.Minute), Factor: 14.4, Severity: "critical"},
			{Name: "slow", Long: Duration(6 * time.Hour), Short: Duration(30 * time.Minute), Factor: 6, Severity: "warning"},
		}
	}

	setDur(&c.Alerts.DedupeWindow, 30*time.Minute)
	setDur(&c.Alerts.Timeout, 10*time.Second)
	for i := range c.Alerts.Channels {
		ch := &c.Alerts.Channels[i]
		if ch.Type == "email" && ch.SMTP.Port == 0 {
			ch.SMTP.Port = 587
		}
	}

	for i := range c.Services {
		s := &c.Services[i]
		setStr(&s.Name, s.ID)
		if s.Interval == 0 {
			s.Interval = d.Interval
		}
		if s.Timeout == 0 {
			s.Timeout = d.Timeout
		}
		if s.Retries == nil {
			r := d.Retries
			s.Retries = &r
		}
		if s.RetryDelay == 0 {
			s.RetryDelay = d.RetryDelay
		}
		if s.FailureThreshold == 0 {
			s.FailureThreshold = d.FailureThreshold
		}
		if s.RecoveryThreshold == 0 {
			s.RecoveryThreshold = d.RecoveryThreshold
		}
		if s.Type == CheckHTTP {
			setStr(&s.HTTP.Method, "GET")
			s.HTTP.Method = strings.ToUpper(s.HTTP.Method)
		}
		if s.Type == CheckDNS {
			setStr(&s.DNS.Record, "A")
			s.DNS.Record = strings.ToUpper(s.DNS.Record)
		}
	}
}

// Validate reports every problem found rather than stopping at the first.
func (c *Config) Validate() error {
	var errs []error
	add := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }

	if c.Storage.Driver != "sqlite" && c.Storage.Driver != "postgres" {
		add("storage.driver must be sqlite or postgres, got %q", c.Storage.Driver)
	}
	if c.Scheduler.Workers < 1 || c.Scheduler.Workers > 1024 {
		add("scheduler.workers must be between 1 and 1024")
	}
	if j := *c.Scheduler.Jitter; j < 0 || j > 0.5 {
		add("scheduler.jitter must be between 0 and 0.5")
	}
	if c.Server.PublicURL != "" {
		if u, err := url.Parse(c.Server.PublicURL); err != nil || u.Scheme == "" || u.Host == "" {
			add("server.public_url must be an absolute URL")
		}
	}
	if t := c.Server.AdminToken; t != "" && len(t) < 16 {
		add("server.admin_token must be at least 16 characters")
	}
	if c.Server.UptimeDays < 1 || c.Server.UptimeDays > 365 {
		add("server.uptime_days must be between 1 and 365")
	}
	if c.Storage.Retention.Raw.D() < c.SLO.longestBurnWindow() {
		add("storage.retention.raw must cover the longest burn-rate window")
	}
	if c.Storage.Retention.Daily.D() < time.Duration(c.Server.UptimeDays)*24*time.Hour {
		add("storage.retention.daily must cover server.uptime_days")
	}
	if c.Incidents.FlapThreshold < 2 {
		add("incidents.flap_threshold must be at least 2")
	}
	for _, r := range c.SLO.BurnRules {
		if r.Name == "" || r.Factor <= 0 || r.Long <= 0 || r.Short <= 0 || r.Short > r.Long {
			add("slo.burn_rules: rule %q needs a name, factor > 0 and 0 < short <= long", r.Name)
		}
	}

	channels := map[string]bool{}
	for i, ch := range c.Alerts.Channels {
		where := fmt.Sprintf("alerts.channels[%d]", i)
		if ch.Name == "" {
			add("%s: name is required", where)
		} else if channels[ch.Name] {
			add("%s: duplicate channel name %q", where, ch.Name)
		}
		channels[ch.Name] = true
		switch ch.Type {
		case "slack", "discord", "webhook":
			if u, err := url.Parse(ch.URL); err != nil || (u.Scheme != "http" && u.Scheme != "https") {
				add("%s: url must be an http(s) URL", where)
			}
		case "email":
			if ch.SMTP.Host == "" || ch.SMTP.From == "" || len(ch.SMTP.To) == 0 {
				add("%s: smtp.host, smtp.from and smtp.to are required", where)
			}
		default:
			add("%s: unknown type %q", where, ch.Type)
		}
	}

	ids := map[string]bool{}
	for i, s := range c.Services {
		where := fmt.Sprintf("services[%d]", i)
		if s.ID != "" {
			where = fmt.Sprintf("service %q", s.ID)
		}
		if !validID.MatchString(s.ID) {
			add("%s: id must match %s", where, validID)
		} else if ids[s.ID] {
			add("%s: duplicate id", where)
		}
		ids[s.ID] = true
		if s.Interval.D() < time.Second {
			add("%s: interval must be at least 1s", where)
		}
		if s.Timeout <= 0 || s.Timeout > s.Interval {
			add("%s: timeout must be positive and no longer than interval", where)
		}
		if *s.Retries < 0 || *s.Retries > 10 {
			add("%s: retries must be between 0 and 10", where)
		}
		if s.FailureThreshold < 1 || s.RecoveryThreshold < 1 {
			add("%s: thresholds must be at least 1", where)
		}
		if s.SLO != nil && (s.SLO.Target <= 0 || s.SLO.Target >= 100) {
			add("%s: slo.target must be between 0 and 100 exclusive", where)
		}
		for _, name := range s.Alerts {
			if !channels[name] {
				add("%s: unknown alert channel %q", where, name)
			}
		}
		errs = append(errs, s.validateTarget(where)...)
	}
	return errors.Join(errs...)
}

var validID = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)

func (s Service) validateTarget(where string) []error {
	var errs []error
	switch s.Type {
	case CheckHTTP:
		u, err := url.Parse(s.Target)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			errs = append(errs, fmt.Errorf("%s: target must be an http(s) URL", where))
		}
		for _, code := range s.HTTP.ExpectStatus {
			if code < 100 || code > 599 {
				errs = append(errs, fmt.Errorf("%s: invalid expect_status %d", where, code))
			}
		}
	case CheckTCP:
		if _, _, err := net.SplitHostPort(s.Target); err != nil {
			errs = append(errs, fmt.Errorf("%s: target must be host:port", where))
		}
	case CheckDNS:
		if s.Target == "" {
			errs = append(errs, fmt.Errorf("%s: target hostname is required", where))
		}
		switch s.DNS.Record {
		case "A", "AAAA", "CNAME", "MX", "TXT", "NS":
		default:
			errs = append(errs, fmt.Errorf("%s: unsupported dns.record %q", where, s.DNS.Record))
		}
		if s.DNS.Server != "" {
			if _, _, err := net.SplitHostPort(s.DNS.Server); err != nil {
				errs = append(errs, fmt.Errorf("%s: dns.server must be host:port", where))
			}
		}
	case CheckICMP:
		if s.Target == "" {
			errs = append(errs, fmt.Errorf("%s: target host is required", where))
		}
	default:
		errs = append(errs, fmt.Errorf("%s: unknown type %q", where, s.Type))
	}
	return errs
}

func (p SLOPolicy) longestBurnWindow() time.Duration {
	var longest time.Duration
	for _, r := range p.BurnRules {
		longest = max(longest, r.Long.D())
	}
	return longest
}

// Service looks up a service by id.
func (c *Config) Service(id string) (Service, bool) {
	for _, s := range c.Services {
		if s.ID == id {
			return s, true
		}
	}
	return Service{}, false
}

func setStr(dst *string, v string) {
	if *dst == "" {
		*dst = v
	}
}

func setDur(dst *Duration, v time.Duration) {
	if *dst == 0 {
		*dst = Duration(v)
	}
}
