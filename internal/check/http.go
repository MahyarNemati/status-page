package check

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/MahyarNemati/status-page/internal/config"
)

const maxBodyBytes = 1 << 20

// HTTP probes an HTTP(S) endpoint for status code, optional keyword and
// certificate expiry.
type HTTP struct {
	client          *http.Client
	url             string
	method          string
	body            string
	headers         map[string]string
	expectStatus    []int
	keyword         string
	keywordAbsent   bool
	tlsExpiryWarn   time.Duration
	degradedLatency time.Duration
}

// NewHTTP builds an HTTP checker with its own transport so connection reuse
// does not mask DNS or TLS failures across services.
func NewHTTP(svc config.Service) (*HTTP, error) {
	transport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		DialContext:         (&net.Dialer{Timeout: svc.Timeout.D()}).DialContext,
		TLSHandshakeTimeout: svc.Timeout.D(),
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: svc.HTTP.InsecureSkipVerify, //nolint:gosec // opt-in per service
			MinVersion:         tls.VersionTLS12,
		},
		DisableKeepAlives: true,
	}
	client := &http.Client{Transport: transport}
	if svc.HTTP.FollowRedirects != nil && !*svc.HTTP.FollowRedirects {
		client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	}
	return &HTTP{
		client:          client,
		url:             svc.Target,
		method:          svc.HTTP.Method,
		body:            svc.HTTP.Body,
		headers:         svc.HTTP.Headers,
		expectStatus:    svc.HTTP.ExpectStatus,
		keyword:         svc.HTTP.Keyword,
		keywordAbsent:   svc.HTTP.KeywordAbsent,
		tlsExpiryWarn:   svc.HTTP.TLSExpiryWarn.D(),
		degradedLatency: svc.DegradedLatency.D(),
	}, nil
}

// Check performs one request.
func (h *HTTP) Check(ctx context.Context) Result {
	start := time.Now()
	var body io.Reader
	if h.body != "" {
		body = strings.NewReader(h.body)
	}
	req, err := http.NewRequestWithContext(ctx, h.method, h.url, body)
	if err != nil {
		return down(start, "build request: %v", err)
	}
	req.Header.Set("User-Agent", "status-page/1 (+https://github.com/MahyarNemati/status-page)")
	for k, v := range h.headers {
		req.Header.Set(k, v)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return down(start, "%s", describeErr(err))
	}
	defer resp.Body.Close()

	var buf []byte
	if h.keyword != "" {
		buf, err = io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
		if err != nil {
			return down(start, "read body: %v", err)
		}
	} else {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxBodyBytes))
	}
	res := Result{Status: StatusUp, Latency: time.Since(start), Message: resp.Status}

	if resp.TLS != nil && len(resp.TLS.PeerCertificates) > 0 {
		res.TLSExpiry = resp.TLS.PeerCertificates[0].NotAfter
	}

	if !h.statusOK(resp.StatusCode) {
		res.Status = StatusDown
		res.Message = fmt.Sprintf("unexpected status %d", resp.StatusCode)
		return res
	}
	if h.keyword != "" {
		found := bytes.Contains(buf, []byte(h.keyword))
		if found == h.keywordAbsent {
			res.Status = StatusDown
			if h.keywordAbsent {
				res.Message = fmt.Sprintf("forbidden keyword %q present", h.keyword)
			} else {
				res.Message = fmt.Sprintf("keyword %q not found", h.keyword)
			}
			return res
		}
	}
	res = gradeLatency(res, h.degradedLatency)
	if res.Status == StatusUp && h.tlsExpiryWarn > 0 && !res.TLSExpiry.IsZero() {
		if left := time.Until(res.TLSExpiry); left < h.tlsExpiryWarn {
			res.Status = StatusDegraded
			res.Message = fmt.Sprintf("certificate expires in %s", left.Round(time.Hour))
		}
	}
	return res
}

func (h *HTTP) statusOK(code int) bool {
	if len(h.expectStatus) == 0 {
		return code >= 200 && code < 400
	}
	return slices.Contains(h.expectStatus, code)
}

func describeErr(err error) string {
	var certErr *tls.CertificateVerificationError
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.As(err, &certErr):
		return "tls: " + certErr.Err.Error()
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return "dns: " + dnsErr.Err
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return opErr.Op + ": " + opErr.Err.Error()
	}
	return err.Error()
}
