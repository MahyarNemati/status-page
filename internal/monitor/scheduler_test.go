package monitor

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MahyarNemati/status-page/internal/check"
	"github.com/MahyarNemati/status-page/internal/clock"
	"github.com/MahyarNemati/status-page/internal/config"
)

type checkFunc func(ctx context.Context) check.Result

func (f checkFunc) Check(ctx context.Context) check.Result { return f(ctx) }

func up(context.Context) check.Result { return check.Result{Status: check.StatusUp} }

func service(id string, interval time.Duration, retries int) config.Service {
	return config.Service{
		ID: id, Interval: config.Duration(interval), Timeout: config.Duration(time.Second),
		Retries: &retries, RetryDelay: config.Duration(5 * time.Second),
	}
}

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestSchedulerRunsEachServiceOnItsInterval(t *testing.T) {
	fc := clock.NewFake(epoch)
	obs := make(chan Observation, 16)
	s := New(Options{Workers: 4, Clock: fc, Handler: func(_ context.Context, o Observation) { obs <- o }})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.Run(ctx, []Target{
			{Service: service("fast", 10*time.Second, 0), Checker: checkFunc(up)},
			{Service: service("slow", 30*time.Second, 0), Checker: checkFunc(up)},
		})
		close(done)
	}()

	counts := map[string]int{}
	wait := func(n int) {
		t.Helper()
		for range n {
			select {
			case o := <-obs:
				counts[o.Service.ID]++
				if !o.At.Equal(fc.Now()) {
					t.Fatalf("observation time %v, clock %v", o.At, fc.Now())
				}
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for observation")
			}
		}
		fc.BlockUntil(2)
	}

	wait(2)
	fc.Advance(10 * time.Second)
	wait(1)
	fc.Advance(10 * time.Second)
	wait(1)
	fc.Advance(10 * time.Second)
	wait(2)
	if counts["fast"] != 4 || counts["slow"] != 2 {
		t.Fatalf("counts after 30s = %v", counts)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestExecuteRetriesFailures(t *testing.T) {
	fc := clock.NewFake(epoch)
	s := New(Options{Clock: fc})
	var calls atomic.Int32
	flaky := checkFunc(func(context.Context) check.Result {
		if calls.Add(1) < 3 {
			return check.Result{Status: check.StatusDown, Message: "refused"}
		}
		return check.Result{Status: check.StatusUp}
	})

	out := make(chan Observation, 1)
	go func() {
		out <- s.Execute(context.Background(), Target{Service: service("a", time.Minute, 3), Checker: flaky})
	}()
	for range 2 {
		fc.BlockUntil(1)
		fc.Advance(5 * time.Second)
	}
	o := <-out
	if o.Result.Status != check.StatusUp || o.Attempts != 3 || !o.At.Equal(epoch) {
		t.Fatalf("observation = %+v", o)
	}

	calls.Store(-100)
	go func() {
		out <- s.Execute(context.Background(), Target{Service: service("a", time.Minute, 1), Checker: flaky})
	}()
	fc.BlockUntil(1)
	fc.Advance(5 * time.Second)
	if o := <-out; o.Result.Status != check.StatusDown || o.Attempts != 2 {
		t.Fatalf("exhausted retries = %+v", o)
	}
}

func TestExecuteAppliesTimeout(t *testing.T) {
	s := New(Options{})
	svc := service("hang", time.Minute, 0)
	svc.Timeout = config.Duration(20 * time.Millisecond)
	o := s.Execute(context.Background(), Target{Service: svc, Checker: checkFunc(func(ctx context.Context) check.Result {
		<-ctx.Done()
		return check.Result{Status: check.StatusDown, Message: "timeout"}
	})})
	if o.Result.Status != check.StatusDown {
		t.Fatalf("observation = %+v", o)
	}
}

func TestWorkerPoolBoundsConcurrency(t *testing.T) {
	const workers, targets = 2, 6
	var (
		inFlight, peak atomic.Int32
		seen           sync.WaitGroup
	)
	release := make(chan struct{})
	blocking := checkFunc(func(context.Context) check.Result {
		n := inFlight.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		<-release
		inFlight.Add(-1)
		return check.Result{Status: check.StatusUp}
	})
	var ts []Target
	for i := range targets {
		ts = append(ts, Target{Service: service(string(rune('a'+i)), time.Hour, 0), Checker: blocking})
	}
	seen.Add(targets)
	var once sync.Map
	s := New(Options{Workers: workers, Clock: clock.NewFake(epoch), Handler: func(_ context.Context, o Observation) {
		if _, loaded := once.LoadOrStore(o.Service.ID, true); !loaded {
			seen.Done()
		}
	}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx, ts)

	deadline := time.Now().Add(5 * time.Second)
	for inFlight.Load() < workers && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	if got := inFlight.Load(); got != workers {
		t.Fatalf("in flight = %d, want %d", got, workers)
	}
	close(release)
	seen.Wait()
	if p := peak.Load(); p != workers {
		t.Fatalf("peak concurrency = %d, want %d", p, workers)
	}
}

func TestJitterBounds(t *testing.T) {
	s := New(Options{Jitter: 0.1, Seed: 42})
	interval := 60 * time.Second
	distinct := map[time.Duration]bool{}
	for range 1000 {
		d := s.nextInterval(interval)
		if d < 54*time.Second || d > 66*time.Second {
			t.Fatalf("interval %v outside +/-10%%", d)
		}
		distinct[d] = true
		if first := s.initialDelay(interval); first < 0 || first > 6*time.Second {
			t.Fatalf("initial delay %v outside [0, 6s]", first)
		}
	}
	if len(distinct) < 100 {
		t.Fatalf("jitter produced only %d distinct intervals", len(distinct))
	}
	if New(Options{}).nextInterval(interval) != interval {
		t.Fatal("zero jitter must keep the interval exact")
	}
}
