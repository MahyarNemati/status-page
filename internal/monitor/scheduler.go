// Package monitor schedules probes. Each service has its own timer loop with
// jittered intervals, and a bounded worker pool executes the checks so a
// burst of slow targets cannot exhaust sockets or goroutines.
package monitor

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/MahyarNemati/status-page/internal/check"
	"github.com/MahyarNemati/status-page/internal/clock"
	"github.com/MahyarNemati/status-page/internal/config"
)

// Target pairs a service definition with its probe.
type Target struct {
	Service config.Service
	Checker check.Checker
}

// Observation is the final outcome of a scheduled run, after retries.
type Observation struct {
	Service  config.Service
	Result   check.Result
	At       time.Time
	Attempts int
}

// Handler consumes observations. It is called from worker goroutines.
type Handler func(ctx context.Context, obs Observation)

// Options configures a Scheduler.
type Options struct {
	Workers int
	// Jitter spreads each interval by up to +/- Jitter (a fraction).
	Jitter  float64
	Clock   clock.Clock
	Handler Handler
	Logger  *slog.Logger
	// Seed makes jitter deterministic in tests; zero uses a random seed.
	Seed uint64
}

// Scheduler runs targets until its context is cancelled.
type Scheduler struct {
	opts Options

	rmu sync.Mutex
	rnd *rand.Rand
}

// New returns a Scheduler.
func New(o Options) *Scheduler {
	if o.Workers < 1 {
		o.Workers = 1
	}
	if o.Clock == nil {
		o.Clock = clock.Real()
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	if o.Handler == nil {
		o.Handler = func(context.Context, Observation) {}
	}
	seed := o.Seed
	if seed == 0 {
		seed = rand.Uint64()
	}
	return &Scheduler{opts: o, rnd: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))}
}

type job struct {
	target Target
	done   chan struct{}
}

// Run blocks until ctx is cancelled and all in-flight checks finish.
func (s *Scheduler) Run(ctx context.Context, targets []Target) {
	jobs := make(chan job)
	var workers sync.WaitGroup
	for range s.opts.Workers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := range jobs {
				obs := s.Execute(ctx, j.target)
				if ctx.Err() == nil {
					s.opts.Handler(ctx, obs)
				}
				close(j.done)
			}
		}()
	}

	var loops sync.WaitGroup
	for _, t := range targets {
		loops.Add(1)
		go func() {
			defer loops.Done()
			s.loop(ctx, t, jobs)
		}()
	}
	loops.Wait()
	close(jobs)
	workers.Wait()
}

// loop schedules one target. It waits for each run to finish before
// arming the next timer, so a service never has overlapping checks, and it
// measures the interval from the start of the previous run to avoid drift.
func (s *Scheduler) loop(ctx context.Context, t Target, jobs chan<- job) {
	interval := t.Service.Interval.D()
	delay := s.initialDelay(interval)
	for {
		if !clock.Sleep(s.opts.Clock, delay, ctx.Done()) {
			return
		}
		started := s.opts.Clock.Now()
		j := job{target: t, done: make(chan struct{})}
		select {
		case jobs <- j:
		case <-ctx.Done():
			return
		}
		select {
		case <-j.done:
		case <-ctx.Done():
			return
		}
		delay = max(0, s.nextInterval(interval)-s.opts.Clock.Now().Sub(started))
	}
}

// Execute runs one check with retries. Retries happen only for failures,
// and each attempt gets the service's full timeout.
func (s *Scheduler) Execute(ctx context.Context, t Target) Observation {
	svc := t.Service
	attempts := 1
	if svc.Retries != nil {
		attempts += *svc.Retries
	}
	at := s.opts.Clock.Now()
	var res check.Result
	for i := 1; ; i++ {
		actx, cancel := context.WithTimeout(ctx, svc.Timeout.D())
		res = t.Checker.Check(actx)
		cancel()
		if res.Status.Available() || i >= attempts || ctx.Err() != nil {
			if i > 1 {
				s.opts.Logger.Debug("check retried", "service", svc.ID, "attempts", i, "status", res.Status)
			}
			return Observation{Service: svc, Result: res, At: at, Attempts: i}
		}
		if !clock.Sleep(s.opts.Clock, svc.RetryDelay.D(), ctx.Done()) {
			return Observation{Service: svc, Result: res, At: at, Attempts: i}
		}
	}
}

// initialDelay staggers first runs across the jitter span so services
// configured with the same interval do not fire in lockstep at startup.
func (s *Scheduler) initialDelay(interval time.Duration) time.Duration {
	if s.opts.Jitter <= 0 {
		return 0
	}
	return time.Duration(s.float() * s.opts.Jitter * float64(interval))
}

func (s *Scheduler) nextInterval(interval time.Duration) time.Duration {
	if s.opts.Jitter <= 0 {
		return interval
	}
	f := 1 + s.opts.Jitter*(2*s.float()-1)
	return time.Duration(f * float64(interval))
}

func (s *Scheduler) float() float64 {
	s.rmu.Lock()
	defer s.rmu.Unlock()
	return s.rnd.Float64()
}
