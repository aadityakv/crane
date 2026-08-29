package clock

import (
	"sort"
	"sync"
	"time"
)

// Manual is a mutex-protected clock whose time advances only when Advance is called.
type Manual struct {
	mu     sync.Mutex
	now    time.Time
	serial uint64
	timers map[*manualTimer]struct{}
}

// NewManual returns a deterministic clock initialized at start.
func NewManual(start time.Time) *Manual {
	return &Manual{
		now:    start,
		timers: make(map[*manualTimer]struct{}),
	}
}

// Now returns the manual clock's current time.
func (c *Manual) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// NewTimer returns a timer scheduled relative to the current manual time.
func (c *Manual) NewTimer(duration time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.serial++
	timer := &manualTimer{
		clock:      c,
		deadline:   c.now.Add(duration),
		order:      c.serial,
		generation: 1,
		active:     true,
		channel:    make(chan time.Time, 1),
	}
	c.timers[timer] = struct{}{}
	return timer
}

type dueTimer struct {
	timer      *manualTimer
	deadline   time.Time
	order      uint64
	generation uint64
}

// Advance moves the clock by duration and delivers all newly due timers.
func (c *Manual) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	due := make([]dueTimer, 0)
	for timer := range c.timers {
		if timer.active && !timer.resetting && !timer.deadline.After(c.now) {
			timer.active = false
			due = append(due, dueTimer{
				timer:      timer,
				deadline:   timer.deadline,
				order:      timer.order,
				generation: timer.generation,
			})
		}
	}
	c.mu.Unlock()

	sort.SliceStable(due, func(i, j int) bool {
		if due[i].deadline.Equal(due[j].deadline) {
			return due[i].order < due[j].order
		}
		return due[i].deadline.Before(due[j].deadline)
	})
	for _, event := range due {
		event.timer.deliver(event.deadline, event.generation)
	}
}

type manualTimer struct {
	clock      *Manual
	deadline   time.Time
	order      uint64
	generation uint64
	active     bool
	resetting  bool
	channel    chan time.Time
	sendMu     sync.Mutex
	resetMu    sync.Mutex
}

func (t *manualTimer) C() <-chan time.Time {
	return t.channel
}

// Stop prevents a future delivery and reports whether the timer was active.
func (t *manualTimer) Stop() bool {
	t.clock.mu.Lock()
	wasActive := t.active
	t.active = false
	t.generation++
	t.resetting = false
	t.clock.mu.Unlock()
	return wasActive
}

// Reset schedules the timer relative to the clock's current time. It drains an
// unread prior event, as required for a reusable timer channel.
func (t *manualTimer) Reset(duration time.Duration) bool {
	t.resetMu.Lock()
	defer t.resetMu.Unlock()

	t.clock.mu.Lock()
	wasActive := t.active
	t.active = false
	t.resetting = true
	t.generation++
	generation := t.generation
	deadline := t.clock.now.Add(duration)
	t.clock.mu.Unlock()

	t.sendMu.Lock()
	select {
	case <-t.channel:
	default:
	}
	t.sendMu.Unlock()

	t.clock.mu.Lock()
	if t.generation == generation && t.resetting {
		t.clock.serial++
		t.deadline = deadline
		t.order = t.clock.serial
		t.active = true
		t.resetting = false
	}
	t.clock.mu.Unlock()
	return wasActive
}

func (t *manualTimer) deliver(deadline time.Time, generation uint64) {
	// The send mutex serializes delivery with Reset's drain. The state check is
	// outside the clock lock while the channel send remains non-blocking.
	t.sendMu.Lock()
	t.clock.mu.Lock()
	valid := t.generation == generation && !t.resetting
	t.clock.mu.Unlock()
	if valid {
		select {
		case t.channel <- deadline:
		default:
		}
	}
	t.sendMu.Unlock()
}
