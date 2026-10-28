// Package clock abstracts time so schedulers and state machines can be tested
// deterministically.
package clock

import "time"

// Clock is the subset of the time package the service depends on.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
}

// Timer mirrors *time.Timer behind an interface.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

// Real returns a Clock backed by the system clock.
func Real() Clock { return realClock{} }

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) NewTimer(d time.Duration) Timer { return &realTimer{t: time.NewTimer(d)} }

type realTimer struct{ t *time.Timer }

func (r *realTimer) C() <-chan time.Time { return r.t.C }
func (r *realTimer) Stop() bool          { return r.t.Stop() }

// Sleep blocks for d on c or until done is closed. It reports whether the
// full duration elapsed.
func Sleep(c Clock, d time.Duration, done <-chan struct{}) bool {
	if d <= 0 {
		return true
	}
	t := c.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C():
		return true
	case <-done:
		return false
	}
}
