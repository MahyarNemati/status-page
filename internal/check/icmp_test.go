package check

import (
	"context"
	"os"
	"testing"
	"time"
)

// ICMP needs either CAP_NET_RAW or a permissive ping_group_range, which CI
// runners and containers rarely grant, so it is opt-in.
func TestICMPLoopback(t *testing.T) {
	if os.Getenv("STATUSPAGE_TEST_ICMP") == "" {
		t.Skip("set STATUSPAGE_TEST_ICMP=1 to run ICMP tests")
	}
	p := &ICMP{Host: "127.0.0.1", Privileged: os.Getenv("STATUSPAGE_TEST_ICMP") == "privileged"}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if res := p.Check(ctx); res.Status != StatusUp {
		t.Fatalf("ping 127.0.0.1: %s %q", res.Status, res.Message)
	}
}
