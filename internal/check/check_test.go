package check

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"

	"github.com/MahyarNemati/status-page/internal/config"
)

func httpService(target string, mut func(*config.Service)) config.Service {
	s := config.Service{ID: "t", Type: config.CheckHTTP, Target: target, Timeout: config.Duration(2 * time.Second)}
	s.HTTP.Method = "GET"
	if mut != nil {
		mut(&s)
	}
	return s
}

func run(t *testing.T, svc config.Service) Result {
	t.Helper()
	c, err := New(svc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), svc.Timeout.D())
	defer cancel()
	return c.Check(ctx)
}

func TestHTTPCheck(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ok":
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case "/error":
			w.WriteHeader(http.StatusServiceUnavailable)
		case "/slow":
			time.Sleep(50 * time.Millisecond)
		case "/redirect":
			http.Redirect(w, r, "/ok", http.StatusFound)
		case "/hang":
			<-r.Context().Done()
		}
	}))
	defer srv.Close()
	no := false

	cases := []struct {
		name    string
		path    string
		mut     func(*config.Service)
		want    Status
		message string
	}{
		{name: "ok", path: "/ok", want: StatusUp},
		{name: "bad status", path: "/error", want: StatusDown, message: "unexpected status 503"},
		{name: "expected non-2xx", path: "/error", mut: func(s *config.Service) { s.HTTP.ExpectStatus = []int{503} }, want: StatusUp},
		{name: "keyword found", path: "/ok", mut: func(s *config.Service) { s.HTTP.Keyword = `"ok"` }, want: StatusUp},
		{name: "keyword missing", path: "/ok", mut: func(s *config.Service) { s.HTTP.Keyword = "healthy" }, want: StatusDown, message: "not found"},
		{name: "keyword must be absent", path: "/ok", mut: func(s *config.Service) { s.HTTP.Keyword = "ok"; s.HTTP.KeywordAbsent = true }, want: StatusDown, message: "forbidden"},
		{name: "slow is degraded", path: "/slow", mut: func(s *config.Service) { s.DegradedLatency = config.Duration(10 * time.Millisecond) }, want: StatusDegraded, message: "slow"},
		{name: "follows redirects", path: "/redirect", want: StatusUp},
		{name: "redirect not followed", path: "/redirect", mut: func(s *config.Service) { s.HTTP.FollowRedirects = &no; s.HTTP.ExpectStatus = []int{200} }, want: StatusDown, message: "302"},
		{name: "timeout", path: "/hang", mut: func(s *config.Service) { s.Timeout = config.Duration(50 * time.Millisecond) }, want: StatusDown, message: "timeout"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := run(t, httpService(srv.URL+tc.path, tc.mut))
			if res.Status != tc.want {
				t.Fatalf("status = %s (%s), want %s", res.Status, res.Message, tc.want)
			}
			if !strings.Contains(res.Message, tc.message) {
				t.Fatalf("message = %q, want substring %q", res.Message, tc.message)
			}
		})
	}
}

func TestHTTPCheckTLS(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()

	t.Run("untrusted certificate fails", func(t *testing.T) {
		res := run(t, httpService(srv.URL, nil))
		if res.Status != StatusDown || !strings.Contains(res.Message, "tls") {
			t.Fatalf("got %s %q", res.Status, res.Message)
		}
	})
	t.Run("expiry recorded", func(t *testing.T) {
		res := run(t, httpService(srv.URL, func(s *config.Service) { s.HTTP.InsecureSkipVerify = true }))
		if res.Status != StatusUp || res.TLSExpiry.IsZero() {
			t.Fatalf("got %s %q expiry=%v", res.Status, res.Message, res.TLSExpiry)
		}
	})
	t.Run("expiry inside warning window degrades", func(t *testing.T) {
		res := run(t, httpService(srv.URL, func(s *config.Service) {
			s.HTTP.InsecureSkipVerify = true
			s.HTTP.TLSExpiryWarn = config.Duration(200 * 365 * 24 * time.Hour)
		}))
		if res.Status != StatusDegraded || !strings.Contains(res.Message, "certificate expires") {
			t.Fatalf("got %s %q", res.Status, res.Message)
		}
	})
}

func TestTCPCheck(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	addr := ln.Addr().String()
	svc := config.Service{Type: config.CheckTCP, Target: addr, Timeout: config.Duration(time.Second)}
	if res := run(t, svc); res.Status != StatusUp {
		t.Fatalf("open port: %s %q", res.Status, res.Message)
	}
	_ = ln.Close()
	if res := run(t, svc); res.Status != StatusDown {
		t.Fatalf("closed port: %s %q", res.Status, res.Message)
	}
}

// startDNS serves fixed answers over UDP on loopback.
func startDNS(t *testing.T, zone map[string][]string) string {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	go func() {
		buf := make([]byte, 1500)
		for {
			n, addr, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			var p dnsmessage.Parser
			hdr, err := p.Start(buf[:n])
			if err != nil {
				continue
			}
			q, err := p.Question()
			if err != nil {
				continue
			}
			resp := dnsmessage.NewBuilder(nil, dnsmessage.Header{ID: hdr.ID, Response: true, Authoritative: true})
			resp.EnableCompression()
			_ = resp.StartQuestions()
			_ = resp.Question(q)
			_ = resp.StartAnswers()
			name := strings.TrimSuffix(q.Name.String(), ".")
			rh := dnsmessage.ResourceHeader{Name: q.Name, Class: dnsmessage.ClassINET, TTL: 60}
			for _, v := range zone[q.Type.String()+" "+name] {
				switch q.Type {
				case dnsmessage.TypeA:
					var a [4]byte
					copy(a[:], net.ParseIP(v).To4())
					_ = resp.AResource(rh, dnsmessage.AResource{A: a})
				case dnsmessage.TypeMX:
					_ = resp.MXResource(rh, dnsmessage.MXResource{Pref: 10, MX: dnsmessage.MustNewName(v + ".")})
				case dnsmessage.TypeTXT:
					_ = resp.TXTResource(rh, dnsmessage.TXTResource{TXT: []string{v}})
				}
			}
			out, err := resp.Finish()
			if err != nil {
				continue
			}
			_, _ = pc.WriteTo(out, addr)
		}
	}()
	return pc.LocalAddr().String()
}

func TestDNSCheck(t *testing.T) {
	server := startDNS(t, map[string][]string{
		"TypeA example.test":   {"192.0.2.10", "192.0.2.11"},
		"TypeMX example.test":  {"mail.example.test"},
		"TypeTXT example.test": {"v=spf1 -all"},
	})
	svc := func(record string, expect ...string) config.Service {
		s := config.Service{Type: config.CheckDNS, Target: "example.test", Timeout: config.Duration(2 * time.Second)}
		s.DNS = config.DNSCheck{Record: record, Server: server, Expect: expect}
		return s
	}
	cases := []struct {
		name string
		svc  config.Service
		want Status
	}{
		{"A any", svc("A"), StatusUp},
		{"A expected", svc("A", "192.0.2.11"), StatusUp},
		{"A mismatch", svc("A", "192.0.2.99"), StatusDown},
		{"MX expected with trailing dot", svc("MX", "MAIL.example.test."), StatusUp},
		{"TXT expected", svc("TXT", "v=spf1 -all"), StatusUp},
		{"empty answer", func() config.Service { s := svc("A"); s.Target = "missing.test"; return s }(), StatusDown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := run(t, tc.svc)
			if res.Status != tc.want {
				t.Fatalf("status = %s (%s), want %s", res.Status, res.Message, tc.want)
			}
		})
	}
}

func TestStatusAvailability(t *testing.T) {
	if !StatusUp.Available() || !StatusDegraded.Available() || StatusDown.Available() || StatusUnknown.Available() {
		t.Fatal("availability mapping changed")
	}
}
