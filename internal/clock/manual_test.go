package clock

import (
	"runtime"
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

func TestManualTimerResetDeliversIfDeadlinePassesDuringReset(t *testing.T) {
	start := time.Unix(100, 0)
	c := NewManual(start)
	timer := c.NewTimer(10 * time.Second)
	manual := timer.(*manualTimer)

	// Hold the delivery lock so Reset has deterministically entered its
	// resetting state but cannot finish draining its old event yet.
	manual.sendMu.Lock()
	resetDone := make(chan bool, 1)
	go func() {
		resetDone <- timer.Reset(time.Second)
	}()
	for attempts := 0; attempts < 10000; attempts++ {
		c.mu.Lock()
		resetting := manual.resetting
		c.mu.Unlock()
		if resetting {
			break
		}
		runtime.Gosched()
		if attempts == 9999 {
			t.Fatal("Reset did not enter its resetting state")
		}
	}

	c.Advance(2 * time.Second)
	manual.sendMu.Unlock()
	if !<-resetDone {
		t.Fatal("Reset reported an inactive timer")
	}

	select {
	case got := <-timer.C():
		if !got.Equal(start.Add(time.Second)) {
			t.Fatalf("reset timer fired at %v, want %v", got, start.Add(time.Second))
		}
	default:
		t.Fatal("reset timer remained overdue without a delivery")
	}
	select {
	case got := <-timer.C():
		t.Fatalf("reset timer fired more than once at %v", got)
	default:
	}
}

func TestManualClockReclaimsFiredAndStoppedTimers(t *testing.T) {
	c := NewManual(time.Unix(200, 0))
	const timerCount = 1_000
	for index := 0; index < timerCount; index++ {
		timer := c.NewTimer(time.Second)
		if index%2 == 0 {
			timer.Stop()
		}
	}

	c.Advance(time.Second)
	c.mu.Lock()
	retained := len(c.timers)
	c.mu.Unlock()
	if retained != 0 {
		t.Fatalf("retained fired/stopped timers = %d, want 0", retained)
	}
}

func TestManualTimerResetReregistersAfterFireAndStop(t *testing.T) {
	start := time.Unix(300, 0)
	c := NewManual(start)
	timer := c.NewTimer(time.Second)
	c.Advance(time.Second)
	if got := <-timer.C(); !got.Equal(start.Add(time.Second)) {
		t.Fatalf("first firing = %v", got)
	}
	assertManualTimerRegistrySize(t, c, 0)

	if timer.Reset(time.Second) {
		t.Fatal("Reset after firing reported active")
	}
	assertManualTimerRegistrySize(t, c, 1)
	c.Advance(time.Second)
	if got := <-timer.C(); !got.Equal(start.Add(2 * time.Second)) {
		t.Fatalf("reset firing = %v", got)
	}
	assertManualTimerRegistrySize(t, c, 0)

	if timer.Reset(time.Second) {
		t.Fatal("Reset after second firing reported active")
	}
	assertManualTimerRegistrySize(t, c, 1)
	if !timer.Stop() {
		t.Fatal("Stop after reset reported inactive")
	}
	assertManualTimerRegistrySize(t, c, 0)

	if timer.Reset(time.Second) {
		t.Fatal("Reset after stop reported active")
	}
	assertManualTimerRegistrySize(t, c, 1)
	c.Advance(time.Second)
	if got := <-timer.C(); !got.Equal(start.Add(3 * time.Second)) {
		t.Fatalf("post-stop reset firing = %v", got)
	}
	assertManualTimerRegistrySize(t, c, 0)
}

func TestManualPendingTimersReportsOnlyRegisteredTimers(t *testing.T) {
	c := NewManual(time.Unix(400, 0))
	first := c.NewTimer(time.Second)
	second := c.NewTimer(2 * time.Second)
	if got := c.PendingTimers(); got != 2 {
		t.Fatalf("pending timers after creation = %d, want 2", got)
	}

	first.Stop()
	if got := c.PendingTimers(); got != 1 {
		t.Fatalf("pending timers after stop = %d, want 1", got)
	}
	c.Advance(2 * time.Second)
	if got := c.PendingTimers(); got != 0 {
		t.Fatalf("pending timers after fire = %d, want 0", got)
	}
	if second.Reset(time.Second) {
		t.Fatal("Reset after fire reported active")
	}
	if got := c.PendingTimers(); got != 1 {
		t.Fatalf("pending timers after reset = %d, want 1", got)
	}
}

func assertManualTimerRegistrySize(t *testing.T, c *Manual, want int) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if got := len(c.timers); got != want {
		t.Fatalf("manual timer registry size = %d, want %d", got, want)
	}
}
