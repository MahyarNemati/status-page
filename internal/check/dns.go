package check

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strings"
	"time"

	"github.com/MahyarNemati/status-page/internal/config"
)

// DNS resolves a name and optionally asserts that expected values are
// present in the answer.
type DNS struct {
	Host            string
	Record          string
	Expect          []string
	DegradedLatency time.Duration
	resolver        *net.Resolver
}

// NewDNS builds a DNS checker. When svc.DNS.Server is set, queries go
// directly to that server instead of the system resolver.
func NewDNS(svc config.Service) *DNS {
	r := net.DefaultResolver
	if server := svc.DNS.Server; server != "" {
		r = &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, network, server)
			},
		}
	}
	return &DNS{
		Host:            svc.Target,
		Record:          svc.DNS.Record,
		Expect:          svc.DNS.Expect,
		DegradedLatency: svc.DegradedLatency.D(),
		resolver:        r,
	}
}

// Check performs one lookup.
func (d *DNS) Check(ctx context.Context) Result {
	start := time.Now()
	answers, err := d.lookup(ctx)
	if err != nil {
		return down(start, "%s", describeErr(err))
	}
	latency := time.Since(start)
	if len(answers) == 0 {
		return Result{Status: StatusDown, Latency: latency, Message: "empty answer"}
	}
	for _, want := range d.Expect {
		if !slices.Contains(answers, normalize(d.Record, want)) {
			return Result{
				Status:  StatusDown,
				Latency: latency,
				Message: fmt.Sprintf("expected %s %q not in answer %v", d.Record, want, answers),
			}
		}
	}
	res := Result{Status: StatusUp, Latency: latency, Message: strings.Join(answers, ", ")}
	return gradeLatency(res, d.DegradedLatency)
}

func (d *DNS) lookup(ctx context.Context) ([]string, error) {
	var out []string
	switch d.Record {
	case "A", "AAAA":
		network := "ip4"
		if d.Record == "AAAA" {
			network = "ip6"
		}
		ips, err := d.resolver.LookupIP(ctx, network, d.Host)
		if err != nil {
			return nil, err
		}
		for _, ip := range ips {
			out = append(out, ip.String())
		}
	case "CNAME":
		c, err := d.resolver.LookupCNAME(ctx, d.Host)
		if err != nil {
			return nil, err
		}
		out = append(out, normalize("CNAME", c))
	case "MX":
		mx, err := d.resolver.LookupMX(ctx, d.Host)
		if err != nil {
			return nil, err
		}
		for _, m := range mx {
			out = append(out, normalize("MX", m.Host))
		}
	case "NS":
		ns, err := d.resolver.LookupNS(ctx, d.Host)
		if err != nil {
			return nil, err
		}
		for _, n := range ns {
			out = append(out, normalize("NS", n.Host))
		}
	case "TXT":
		txt, err := d.resolver.LookupTXT(ctx, d.Host)
		if err != nil {
			return nil, err
		}
		out = txt
	default:
		return nil, fmt.Errorf("unsupported record type %q", d.Record)
	}
	return out, nil
}

func normalize(record, v string) string {
	switch record {
	case "TXT":
		return v
	case "A", "AAAA":
		if ip := net.ParseIP(v); ip != nil {
			return ip.String()
		}
		return v
	default:
		return strings.ToLower(strings.TrimSuffix(v, "."))
	}
}
