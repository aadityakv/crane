// Package clock provides real and deterministic time sources for protocol code.
package clock

import "time"

// Timer is a clock timer whose channel delivers its deadline once.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(time.Duration) bool
}

// Clock supplies the current time and timers used by a protocol component.
type Clock interface {
	Now() time.Time
	NewTimer(time.Duration) Timer
}

// Real delegates time operations to the standard library.
type Real struct{}

// NewReal returns a clock backed by the system clock.
func NewReal() Real {
	return Real{}
}

// Now returns the current wall-clock time.
func (Real) Now() time.Time {
	return time.Now()
}

// NewTimer returns a timer backed by time.NewTimer.
func (Real) NewTimer(duration time.Duration) Timer {
	return realTimer{timer: time.NewTimer(duration)}
}

type realTimer struct {
	timer *time.Timer
}

// C returns the channel the underlying time.Timer fires on.
func (t realTimer) C() <-chan time.Time {
	return t.timer.C
}

// Stop cancels the underlying time.Timer and reports whether it was still pending.
func (t realTimer) Stop() bool {
	return t.timer.Stop()
}

// Reset re-arms the underlying time.Timer and reports whether it was still pending.
func (t realTimer) Reset(duration time.Duration) bool {
	return t.timer.Reset(duration)
}
