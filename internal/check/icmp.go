package check

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"time"

	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
)

var icmpSeq atomic.Uint32

// ICMP sends a single IPv4 echo request. Unprivileged mode uses datagram
// ICMP sockets, which on Linux requires net.ipv4.ping_group_range to include
// the process group; privileged mode needs CAP_NET_RAW.
type ICMP struct {
	Host            string
	Privileged      bool
	DegradedLatency time.Duration
}

// Check sends one echo and waits for the matching reply.
func (p *ICMP) Check(ctx context.Context) Result {
	start := time.Now()
	ipAddr, err := net.DefaultResolver.LookupIPAddr(ctx, p.Host)
	if err != nil {
		return down(start, "%s", describeErr(err))
	}
	var dst net.IP
	for _, a := range ipAddr {
		if v4 := a.IP.To4(); v4 != nil {
			dst = v4
			break
		}
	}
	if dst == nil {
		return down(start, "no IPv4 address for %s", p.Host)
	}

	network, addr := "udp4", net.Addr(&net.UDPAddr{IP: dst})
	if p.Privileged {
		network, addr = "ip4:icmp", &net.IPAddr{IP: dst}
	}
	conn, err := icmp.ListenPacket(network, "0.0.0.0")
	if err != nil {
		return down(start, "open icmp socket: %v", err)
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}

	id := os.Getpid() & 0xffff
	seq := int(icmpSeq.Add(1) & 0xffff)
	msg := icmp.Message{
		Type: ipv4.ICMPTypeEcho,
		Body: &icmp.Echo{ID: id, Seq: seq, Data: []byte("status-page")},
	}
	wire, err := msg.Marshal(nil)
	if err != nil {
		return down(start, "marshal echo: %v", err)
	}
	sent := time.Now()
	if _, err := conn.WriteTo(wire, addr); err != nil {
		return down(start, "send echo: %v", err)
	}

	buf := make([]byte, 1500)
	for {
		n, _, err := conn.ReadFrom(buf)
		if err != nil {
			return down(start, "%s", describeErr(err))
		}
		reply, err := icmp.ParseMessage(1, buf[:n])
		if err != nil || reply.Type != ipv4.ICMPTypeEchoReply {
			continue
		}
		echo, ok := reply.Body.(*icmp.Echo)
		// Datagram sockets rewrite the identifier, so only match on it in raw mode.
		if !ok || echo.Seq != seq || (p.Privileged && echo.ID != id) {
			continue
		}
		rtt := time.Since(sent)
		return gradeLatency(Result{Status: StatusUp, Latency: rtt, Message: fmt.Sprintf("reply from %s", dst)}, p.DegradedLatency)
	}
}
