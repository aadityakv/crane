package store

import (
	"errors"
	"testing"
	"time"
)

// TestRecoverWorkWithinFailsFastWhileTheStoreIsHeld pins the bounded
// control-path read: while another owner holds the store lock the bounded read
// answers ErrBusy within its wait instead of blocking, and once the lock is
// released it returns the recovered work like RecoverWork.
func TestRecoverWorkWithinFailsFastWhileTheStoreIsHeld(t *testing.T) {
	s, _, _, _ := rebindStoreForTest(t)
	s.mu.Lock()
	started := time.Now()
	_, err := s.RecoverWorkWithin(30 * time.Millisecond)
	elapsed := time.Since(started)
	s.mu.Unlock()
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("held store bounded read err = %v, want ErrBusy", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("bounded read blocked %s past its 30ms wait", elapsed)
	}
	if _, err := s.RecoverWorkWithin(30 * time.Millisecond); err != nil {
		t.Fatalf("released store bounded read err = %v", err)
	}
	if _, err := s.RecoverWorkWithin(0); err != nil {
		t.Fatalf("zero wait must behave like RecoverWork: %v", err)
	}
}
