package clock

import (
	"sync"
	"testing"
	"time"
)

func TestManualClockFiresInDeadlineOrder(t *testing.T) {
	start := time.Unix(100, 0)
	c := NewManual(start)
	late := c.NewTimer(2 * time.Second)
	early := c.NewTimer(time.Second)

	c.Advance(time.Second)
	if got := <-early.C(); !got.Equal(start.Add(time.Second)) {
		t.Fatalf("early timer fired at %v", got)
	}
	select {
	case <-late.C():
		t.Fatal("late timer fired too early")
	default:
	}

	c.Advance(time.Second)
	if got := <-late.C(); !got.Equal(start.Add(2 * time.Second)) {
		t.Fatalf("late timer fired at %v", got)
	}
}

func TestManualTimerStopPreventsDelivery(t *testing.T) {
	c := NewManual(time.Unix(100, 0))
	timer := c.NewTimer(time.Second)
	if !timer.Stop() {
		t.Fatal("Stop reported an inactive timer")
	}
	if timer.Stop() {
		t.Fatal("second Stop reported an active timer")
	}
	c.Advance(time.Second)
	select {
	case got := <-timer.C():
		t.Fatalf("stopped timer fired at %v", got)
	default:
	}
}

func TestManualTimerResetReportsPreviousStateAndDiscardsUnreadEvent(t *testing.T) {
	start := time.Unix(100, 0)
	c := NewManual(start)
	timer := c.NewTimer(time.Second)

	if !timer.Reset(2 * time.Second) {
		t.Fatal("Reset reported an inactive timer")
	}
	c.Advance(time.Second)
	select {
	case <-timer.C():
		t.Fatal("reset timer fired at its old deadline")
	default:
	}
	c.Advance(time.Second)
	if got := <-timer.C(); !got.Equal(start.Add(2 * time.Second)) {
		t.Fatalf("reset timer fired at %v", got)
	}
	if timer.Reset(time.Second) {
		t.Fatal("Reset reported an active timer after it fired")
	}
	c.Advance(time.Second)
	if got := <-timer.C(); !got.Equal(start.Add(3 * time.Second)) {
		t.Fatalf("timer did not fire after resetting an expired timer at %v", got)
	}
}

func TestManualClockEqualDeadlinesUseStableCreationOrder(t *testing.T) {
	start := time.Unix(100, 0)
	c := NewManual(start)
	first := c.NewTimer(time.Second)
	second := c.NewTimer(time.Second)
	c.Advance(time.Second)

	if got := <-first.C(); !got.Equal(start.Add(time.Second)) {
		t.Fatalf("first timer fired at %v", got)
	}
	if got := <-second.C(); !got.Equal(start.Add(time.Second)) {
		t.Fatalf("second timer fired at %v", got)
	}
}

func TestManualClockConcurrentUse(t *testing.T) {
	c := NewManual(time.Unix(100, 0))
	const count = 32
	timers := make([]Timer, count)
	for i := range timers {
		timers[i] = c.NewTimer(time.Second)
	}

	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(timer Timer) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				timer.Reset(time.Second)
			}
		}(timers[i])
	}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Advance(time.Second)
		}()
	}
	wg.Wait()
}
