package alert

import (
	"fmt"
	"net/http"
	"time"

	"github.com/MahyarNemati/status-page/internal/config"
)

// FromConfig builds notifiers for every configured channel.
func FromConfig(cfg config.Alerts) ([]Notifier, error) {
	client := &http.Client{Timeout: cfg.Timeout.D()}
	if client.Timeout == 0 {
		client.Timeout = 10 * time.Second
	}
	out := make([]Notifier, 0, len(cfg.Channels))
	for _, ch := range cfg.Channels {
		switch ch.Type {
		case "slack":
			out = append(out, &Slack{ChannelName: ch.Name, URL: ch.URL, Client: client})
		case "discord":
			out = append(out, &Discord{ChannelName: ch.Name, URL: ch.URL, Client: client})
		case "webhook":
			out = append(out, &Webhook{ChannelName: ch.Name, URL: ch.URL, Headers: ch.Headers, Secret: ch.Secret, Client: client})
		case "email":
			out = append(out, &Email{
				ChannelName: ch.Name,
				Host:        ch.SMTP.Host,
				Port:        ch.SMTP.Port,
				Username:    ch.SMTP.Username,
				Password:    ch.SMTP.Password,
				From:        ch.SMTP.From,
				To:          ch.SMTP.To,
			})
		default:
			return nil, fmt.Errorf("channel %q: unknown type %q", ch.Name, ch.Type)
		}
	}
	return out, nil
}
