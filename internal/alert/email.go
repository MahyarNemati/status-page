package alert

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"mime"
	"net"
	"net/smtp"
	"strconv"
	"strings"
	"time"
)

// Email sends plain-text mail over SMTP. Port 465 uses implicit TLS; other
// ports upgrade with STARTTLS when the server offers it.
type Email struct {
	ChannelName string
	Host        string
	Port        int
	Username    string
	Password    string
	From        string
	To          []string
	// TLSConfig overrides the default verification settings.
	TLSConfig *tls.Config
}

// Name implements Notifier.
func (e *Email) Name() string { return e.ChannelName }

// Send implements Notifier.
func (e *Email) Send(ctx context.Context, n Notification) error {
	addr := net.JoinHostPort(e.Host, strconv.Itoa(e.Port))
	tlsCfg := e.TLSConfig
	if tlsCfg == nil {
		tlsCfg = &tls.Config{ServerName: e.Host, MinVersion: tls.VersionTLS12}
	}

	var d net.Dialer
	var conn net.Conn
	var err error
	if e.Port == 465 {
		conn, err = (&tls.Dialer{NetDialer: &d, Config: tlsCfg}).DialContext(ctx, "tcp", addr)
	} else {
		conn, err = d.DialContext(ctx, "tcp", addr)
	}
	if err != nil {
		return fmt.Errorf("smtp dial: %w", err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	c, err := smtp.NewClient(conn, e.Host)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("smtp handshake: %w", err)
	}
	defer c.Close()

	if ok, _ := c.Extension("STARTTLS"); ok && e.Port != 465 {
		if err := c.StartTLS(tlsCfg); err != nil {
			return fmt.Errorf("smtp starttls: %w", err)
		}
	}
	if e.Username != "" {
		if err := c.Auth(smtp.PlainAuth("", e.Username, e.Password, e.Host)); err != nil {
			return &PermanentError{Err: fmt.Errorf("smtp auth: %w", err)}
		}
	}
	if err := c.Mail(e.From); err != nil {
		return fmt.Errorf("smtp mail from: %w", err)
	}
	for _, rcpt := range e.To {
		if err := c.Rcpt(rcpt); err != nil {
			return fmt.Errorf("smtp rcpt %s: %w", rcpt, err)
		}
	}
	w, err := c.Data()
	if err != nil {
		return fmt.Errorf("smtp data: %w", err)
	}
	if _, err := w.Write(e.message(n)); err != nil {
		return fmt.Errorf("smtp write: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("smtp data close: %w", err)
	}
	return c.Quit()
}

func (e *Email) message(n Notification) []byte {
	var b bytes.Buffer
	hdr := func(k, v string) { fmt.Fprintf(&b, "%s: %s\r\n", k, v) }
	hdr("From", e.From)
	hdr("To", strings.Join(e.To, ", "))
	hdr("Subject", mime.QEncoding.Encode("utf-8", n.Subject()))
	hdr("Date", n.At.Format(time.RFC1123Z))
	hdr("Message-ID", fmt.Sprintf("<%s@%s>", randomID(), e.Host))
	hdr("MIME-Version", "1.0")
	hdr("Content-Type", "text/plain; charset=utf-8")
	hdr("Content-Transfer-Encoding", "8bit")
	b.WriteString("\r\n")

	body := fmt.Sprintf("%s\n\nService: %s\nSeverity: %s\nTime: %s\n",
		n.Message, n.ServiceName, orDash(n.Severity), n.At.UTC().Format(time.RFC1123))
	if n.URL != "" {
		body += "Status page: " + n.URL + "\n"
	}
	// The DATA writer handles dot-stuffing; line endings must be CRLF.
	b.WriteString(strings.ReplaceAll(body, "\n", "\r\n"))
	return b.Bytes()
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func randomID() string {
	var buf [12]byte
	_, _ = rand.Read(buf[:])
	return hex.EncodeToString(buf[:])
}
