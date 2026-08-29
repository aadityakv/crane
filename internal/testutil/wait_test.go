package testutil

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestWaitForChecksImmediatelyAndStopsWhenConditionMatches(t *testing.T) {
	var attempts atomic.Int32
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := WaitFor(ctx, time.Millisecond, func() (bool, error) {
		return attempts.Add(1) == 3, nil
	})
	if err != nil {
		t.Fatalf("WaitFor: %v", err)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("condition attempts = %d, want 3", got)
	}
}

func TestWaitForDeadlineIncludesLastConditionError(t *testing.T) {
	diagnostic := errors.New("snapshot not ready")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := WaitFor(ctx, time.Millisecond, func() (bool, error) {
		return false, diagnostic
	})
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), diagnostic.Error()) {
		t.Fatalf("WaitFor error = %v, want cancellation with last condition error", err)
	}
}

func TestWaitForRejectsInvalidInputs(t *testing.T) {
	//lint:ignore SA1012 This assertion verifies that WaitFor rejects a nil context.
	if err := WaitFor(nil, time.Millisecond, func() (bool, error) { return true, nil }); err == nil {
		t.Fatal("WaitFor accepted nil context")
	}
	if err := WaitFor(context.Background(), 0, func() (bool, error) { return true, nil }); err == nil {
		t.Fatal("WaitFor accepted zero interval")
	}
	if err := WaitFor(context.Background(), time.Millisecond, nil); err == nil {
		t.Fatal("WaitFor accepted nil condition")
	}
}
