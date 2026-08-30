package wire

import (
	"errors"
	"testing"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
)

func TestReplayGuardRejectsDuplicateAndFutureMessages(t *testing.T) {
	now := time.Unix(1000, 0)
	manualClock := clock.NewManual(now)
	guard := NewReplayGuard(manualClock, 2*time.Minute, 30*time.Second, 100)
	id := RequestID{1}
	if err := guard.Accept(2, id, now); err != nil {
		t.Fatal(err)
	}
	if err := guard.Accept(2, id, now); !errors.Is(err, ErrReplay) {
		t.Fatalf("duplicate error = %v", err)
	}
	if err := guard.Accept(2, RequestID{2}, now.Add(31*time.Second)); !errors.Is(err, ErrTimestamp) {
		t.Fatalf("future timestamp error = %v", err)
	}
}

func TestReplayGuardRejectsExpiredTimestamps(t *testing.T) {
	now := time.Unix(1000, 0)
	guard := NewReplayGuard(clock.NewManual(now), 2*time.Minute, 30*time.Second, 100)

	if err := guard.Accept(2, RequestID{1}, now.Add(-2*time.Minute)); !errors.Is(err, ErrTimestamp) {
		t.Fatalf("timestamp at replay-window boundary error = %v", err)
	}
	if err := guard.Accept(2, RequestID{2}, now.Add(-2*time.Minute-time.Nanosecond)); !errors.Is(err, ErrTimestamp) {
		t.Fatalf("stale timestamp error = %v", err)
	}
}

func TestReplayGuardFailsClosedAtCapacity(t *testing.T) {
	now := time.Unix(1000, 0)
	guard := NewReplayGuard(clock.NewManual(now), 2*time.Minute, 30*time.Second, 2)
	if err := guard.Accept(1, RequestID{1}, now); err != nil {
		t.Fatal(err)
	}
	if err := guard.Accept(1, RequestID{2}, now); err != nil {
		t.Fatal(err)
	}
	if err := guard.Accept(1, RequestID{3}, now); !errors.Is(err, ErrReplayCacheFull) {
		t.Fatalf("capacity error = %v", err)
	}
	if err := guard.Accept(1, RequestID{1}, now); !errors.Is(err, ErrReplay) {
		t.Fatalf("duplicate at capacity error = %v", err)
	}
}

func TestReplayGuardPurgesExpiryBeforeCapacityCheck(t *testing.T) {
	now := time.Unix(1000, 0)
	manualClock := clock.NewManual(now)
	guard := NewReplayGuard(manualClock, 2*time.Minute, 30*time.Second, 1)
	if err := guard.Accept(1, RequestID{1}, now); err != nil {
		t.Fatal(err)
	}

	manualClock.Advance(2*time.Minute + time.Nanosecond)
	if err := guard.Accept(1, RequestID{2}, manualClock.Now()); err != nil {
		t.Fatalf("fresh request after expiry error = %v", err)
	}
	if err := guard.Accept(1, RequestID{1}, manualClock.Now()); !errors.Is(err, ErrReplayCacheFull) {
		t.Fatalf("expired request remained cached or capacity did not fail closed: %v", err)
	}
}

func TestReplayGuardExpiresEntriesFromMessageTimestamp(t *testing.T) {
	now := time.Unix(1000, 0)
	manualClock := clock.NewManual(now)
	guard := NewReplayGuard(manualClock, 2*time.Minute, 30*time.Second, 2)
	if err := guard.Accept(1, RequestID{1}, now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := guard.Accept(1, RequestID{2}, now); err != nil {
		t.Fatal(err)
	}

	manualClock.Advance(time.Minute + time.Nanosecond)
	if err := guard.Accept(1, RequestID{3}, manualClock.Now()); err != nil {
		t.Fatalf("entry did not expire from its message timestamp: %v", err)
	}
	if err := guard.Accept(1, RequestID{2}, now); !errors.Is(err, ErrReplay) {
		t.Fatalf("unexpired entry error = %v", err)
	}
}

func TestReplayGuardWithNilClockFailsClosed(t *testing.T) {
	guard := NewReplayGuard(nil, 2*time.Minute, 30*time.Second, 100)

	if err := guard.Accept(1, RequestID{1}, time.Unix(1000, 0)); !errors.Is(err, ErrReplayConfiguration) {
		t.Fatalf("nil-clock error = %v", err)
	}
}

func TestReplayGuardPreflightAndInvalidCachePreserveValidCapacity(t *testing.T) {
	now := time.Unix(1100, 0)
	guard := NewReplayGuard(clock.NewManual(now), 2*time.Minute, 30*time.Second, 1)
	invalidID := RequestID{1}
	validID := RequestID{2}

	if err := guard.Preflight(7, invalidID, now); err != nil {
		t.Fatalf("invalid candidate preflight error = %v", err)
	}
	guard.RecordInvalid(7, invalidID, now)
	if err := guard.Preflight(7, invalidID, now); !errors.Is(err, ErrReplay) {
		t.Fatalf("repeated invalid preflight error = %v, want ErrReplay", err)
	}
	if err := guard.Preflight(7, validID, now); err != nil {
		t.Fatalf("valid preflight after invalid error = %v", err)
	}
	if err := guard.Commit(7, validID, now); err != nil {
		t.Fatalf("valid commit after invalid error = %v", err)
	}
	if got := len(guard.seen); got != 1 {
		t.Fatalf("accepted replay entries = %d, want 1", got)
	}
	if got := len(guard.invalid); got != 1 {
		t.Fatalf("invalid replay entries = %d, want separately bounded 1", got)
	}
}

func TestReplayGuardInvalidCacheStaysBoundedAndRetainsRecentIDs(t *testing.T) {
	now := time.Unix(1200, 0)
	guard := NewReplayGuard(clock.NewManual(now), 2*time.Minute, 30*time.Second, 2)
	for value := byte(1); value <= 3; value++ {
		requestID := RequestID{value}
		if err := guard.Preflight(8, requestID, now); err != nil {
			t.Fatalf("Preflight(%d) error = %v", value, err)
		}
		guard.RecordInvalid(8, requestID, now)
	}
	if got := len(guard.invalid); got != 2 {
		t.Fatalf("invalid replay entries = %d, want bound 2", got)
	}
	if err := guard.Preflight(8, RequestID{3}, now); !errors.Is(err, ErrReplay) {
		t.Fatalf("recent invalid ID preflight error = %v, want ErrReplay", err)
	}
}

func TestReplayGuardPreflightRejectsTimestampWithoutRecording(t *testing.T) {
	now := time.Unix(1300, 0)
	guard := NewReplayGuard(clock.NewManual(now), 2*time.Minute, 30*time.Second, 1)

	if err := guard.Preflight(9, RequestID{1}, now.Add(-2*time.Minute)); !errors.Is(err, ErrTimestamp) {
		t.Fatalf("stale preflight error = %v, want ErrTimestamp", err)
	}
	if len(guard.seen) != 0 || len(guard.invalid) != 0 {
		t.Fatalf("stale preflight recorded state: seen=%d invalid=%d", len(guard.seen), len(guard.invalid))
	}
}
