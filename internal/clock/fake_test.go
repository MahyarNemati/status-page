package clock

import (
	"testing"
	"time"
)

func TestFakeAdvanceFiresInOrder(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f := NewFake(start)
	late := f.NewTimer(2 * time.Second)
	early := f.NewTimer(time.Second)

	f.Advance(500 * time.Millisecond)
	select {
	case <-early.C():
		t.Fatal("timer fired early")
	default:
	}

	f.Advance(2 * time.Second)
	if got := <-early.C(); !got.Equal(start.Add(time.Second)) {
		t.Fatalf("early fired at %v", got)
	}
	if got := <-late.C(); !got.Equal(start.Add(2 * time.Second)) {
		t.Fatalf("late fired at %v", got)
	}
	if !f.Now().Equal(start.Add(2500 * time.Millisecond)) {
		t.Fatalf("now = %v", f.Now())
	}
}

func TestFakeStop(t *testing.T) {
	f := NewFake(time.Unix(0, 0))
	tm := f.NewTimer(time.Second)
	if !tm.Stop() {
		t.Fatal("Stop on armed timer returned false")
	}
	if f.Pending() != 0 {
		t.Fatalf("pending = %d", f.Pending())
	}
	f.Advance(time.Hour)
	select {
	case <-tm.C():
		t.Fatal("stopped timer fired")
	default:
	}
}

func TestSleepCancel(t *testing.T) {
	f := NewFake(time.Unix(0, 0))
	done := make(chan struct{})
	res := make(chan bool)
	go func() { res <- Sleep(f, time.Minute, done) }()
	f.BlockUntil(1)
	close(done)
	if <-res {
		t.Fatal("Sleep reported completion after cancel")
	}
}
