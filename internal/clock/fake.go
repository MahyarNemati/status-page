package clock

import (
	"sort"
	"sync"
	"time"
)

// Fake is a manually advanced Clock for tests.
type Fake struct {
	mu      sync.Mutex
	cond    *sync.Cond
	now     time.Time
	timers  []*fakeTimer
	nextSeq int
}

// NewFake returns a Fake clock set to start.
func NewFake(start time.Time) *Fake {
	f := &Fake{now: start}
	f.cond = sync.NewCond(&f.mu)
	return f
}

// Now returns the fake current time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// NewTimer registers a timer that fires once the clock is advanced past d.
func (f *Fake) NewTimer(d time.Duration) Timer {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := &fakeTimer{f: f, at: f.now.Add(d), ch: make(chan time.Time, 1), seq: f.nextSeq}
	f.nextSeq++
	if d <= 0 {
		t.ch <- f.now
		t.fired = true
		return t
	}
	f.timers = append(f.timers, t)
	f.cond.Broadcast()
	return t
}

// Advance moves the clock forward, firing due timers in chronological order.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	target := f.now.Add(d)
	for {
		sort.Slice(f.timers, func(i, j int) bool {
			if f.timers[i].at.Equal(f.timers[j].at) {
				return f.timers[i].seq < f.timers[j].seq
			}
			return f.timers[i].at.Before(f.timers[j].at)
		})
		if len(f.timers) == 0 || f.timers[0].at.After(target) {
			break
		}
		t := f.timers[0]
		f.timers = f.timers[1:]
		f.now = t.at
		t.fired = true
		t.ch <- t.at
	}
	f.now = target
	f.mu.Unlock()
}

// Set jumps the clock to t without firing timers that are skipped over in
// order; due timers fire at t.
func (f *Fake) Set(t time.Time) {
	f.Advance(t.Sub(f.Now()))
}

// BlockUntil waits until at least n timers are pending. Tests use it to
// synchronise with goroutines that are about to wait on the clock.
func (f *Fake) BlockUntil(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for len(f.timers) < n {
		f.cond.Wait()
	}
}

// Pending returns the number of armed timers.
func (f *Fake) Pending() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.timers)
}

type fakeTimer struct {
	f     *Fake
	at    time.Time
	ch    chan time.Time
	seq   int
	fired bool
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }

func (t *fakeTimer) Stop() bool {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	if t.fired {
		return false
	}
	for i, other := range t.f.timers {
		if other == t {
			t.f.timers = append(t.f.timers[:i], t.f.timers[i+1:]...)
			t.f.cond.Broadcast()
			return true
		}
	}
	return false
}
