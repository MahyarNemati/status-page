package alert

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/MahyarNemati/status-page/internal/clock"
)

// Outcome of a delivery attempt to one channel.
type Outcome string

// Delivery outcomes, also used as metric labels.
const (
	Sent       Outcome = "sent"
	Suppressed Outcome = "suppressed"
	Failed     Outcome = "failed"
)

// Observer is told about every per-channel delivery decision.
type Observer func(channel string, kind Kind, outcome Outcome)

// DispatcherOptions tunes delivery.
type DispatcherOptions struct {
	DedupeWindow time.Duration
	Timeout      time.Duration
	Attempts     int
	Backoff      time.Duration
	QueueSize    int
	Clock        clock.Clock
	Logger       *slog.Logger
	Observer     Observer
}

// Dispatcher fans notifications out to notifiers. Firing notifications with
// the same key are suppressed per channel within the dedupe window, and a
// recovery is only sent to channels that were told about the firing.
type Dispatcher struct {
	notifiers []Notifier
	opts      DispatcherOptions
	queue     chan Notification

	mu       sync.Mutex
	lastSent map[string]time.Time
	active   map[string]bool
}

// NewDispatcher returns a Dispatcher. Call Run to start background delivery.
func NewDispatcher(notifiers []Notifier, opts DispatcherOptions) *Dispatcher {
	if opts.Attempts < 1 {
		opts.Attempts = 3
	}
	if opts.Backoff <= 0 {
		opts.Backoff = time.Second
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 10 * time.Second
	}
	if opts.QueueSize <= 0 {
		opts.QueueSize = 256
	}
	if opts.Clock == nil {
		opts.Clock = clock.Real()
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	return &Dispatcher{
		notifiers: notifiers,
		opts:      opts,
		queue:     make(chan Notification, opts.QueueSize),
		lastSent:  map[string]time.Time{},
		active:    map[string]bool{},
	}
}

// Publish queues n without blocking. When the queue is full the
// notification is dropped and logged, so a slow channel cannot stall checks.
func (d *Dispatcher) Publish(n Notification) {
	select {
	case d.queue <- n:
	default:
		d.opts.Logger.Error("alert queue full, dropping notification", "kind", n.Kind, "key", n.Key)
		for _, nt := range d.targets(n) {
			d.observe(nt.Name(), n.Kind, Failed)
		}
	}
}

// Run delivers queued notifications until ctx is cancelled, then drains
// whatever is already queued using a short grace period.
func (d *Dispatcher) Run(ctx context.Context) {
	for {
		select {
		case n := <-d.queue:
			d.Deliver(ctx, n)
		case <-ctx.Done():
			grace, cancel := context.WithTimeout(context.WithoutCancel(ctx), d.opts.Timeout)
			defer cancel()
			for {
				select {
				case n := <-d.queue:
					d.Deliver(grace, n)
				default:
					return
				}
			}
		}
	}
}

// Deliver sends n synchronously to every targeted channel and returns the
// per-channel outcome.
func (d *Dispatcher) Deliver(ctx context.Context, n Notification) map[string]Outcome {
	targets := d.targets(n)
	out := make(map[string]Outcome, len(targets))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, nt := range targets {
		if !d.claim(nt.Name(), n) {
			out[nt.Name()] = Suppressed
			d.observe(nt.Name(), n.Kind, Suppressed)
			continue
		}
		wg.Add(1)
		go func(nt Notifier) {
			defer wg.Done()
			err := d.send(ctx, nt, n)
			outcome := Sent
			if err != nil {
				outcome = Failed
				d.opts.Logger.Error("alert delivery failed", "channel", nt.Name(), "kind", n.Kind, "key", n.Key, "err", err)
			} else {
				d.opts.Logger.Info("alert sent", "channel", nt.Name(), "kind", n.Kind, "key", n.Key)
			}
			d.observe(nt.Name(), n.Kind, outcome)
			mu.Lock()
			out[nt.Name()] = outcome
			mu.Unlock()
		}(nt)
	}
	wg.Wait()
	return out
}

func (d *Dispatcher) targets(n Notification) []Notifier {
	if len(n.Channels) == 0 {
		return d.notifiers
	}
	var out []Notifier
	for _, nt := range d.notifiers {
		if slices.Contains(n.Channels, nt.Name()) {
			out = append(out, nt)
		}
	}
	return out
}

// claim decides whether channel should receive n and records the decision.
// A failed send still counts, which avoids hammering a broken endpoint on
// every check; the next firing after the window retries.
func (d *Dispatcher) claim(channel string, n Notification) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	key := channel + "\x00" + n.Key
	now := d.opts.Clock.Now()
	if n.Kind.Recovery() {
		if !d.active[key] {
			return false
		}
		delete(d.active, key)
		return true
	}
	if last, ok := d.lastSent[key]; ok && now.Sub(last) < d.opts.DedupeWindow && d.active[key] {
		return false
	}
	d.lastSent[key] = now
	d.active[key] = true
	return true
}

func (d *Dispatcher) send(ctx context.Context, nt Notifier, n Notification) error {
	var errs []error
	for attempt := 1; attempt <= d.opts.Attempts; attempt++ {
		actx, cancel := context.WithTimeout(ctx, d.opts.Timeout)
		err := nt.Send(actx, n)
		cancel()
		if err == nil {
			return nil
		}
		errs = append(errs, err)
		var perm *PermanentError
		if errors.As(err, &perm) || attempt == d.opts.Attempts {
			break
		}
		if !clock.Sleep(d.opts.Clock, d.opts.Backoff*time.Duration(1<<(attempt-1)), ctx.Done()) {
			break
		}
	}
	return errors.Join(errs...)
}

func (d *Dispatcher) observe(channel string, kind Kind, o Outcome) {
	if d.opts.Observer != nil {
		d.opts.Observer(channel, kind, o)
	}
}

// PermanentError marks a failure that retrying will not fix, such as a 4xx
// response from a webhook.
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }
