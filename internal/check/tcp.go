package check

import (
	"context"
	"net"
	"time"
)

// TCP succeeds when a connection to Address can be established.
type TCP struct {
	Address         string
	DegradedLatency time.Duration
}

// Check dials once.
func (t *TCP) Check(ctx context.Context) Result {
	start := time.Now()
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", t.Address)
	if err != nil {
		return down(start, "%s", describeErr(err))
	}
	latency := time.Since(start)
	_ = conn.Close()
	return gradeLatency(Result{Status: StatusUp, Latency: latency, Message: "connected"}, t.DegradedLatency)
}
