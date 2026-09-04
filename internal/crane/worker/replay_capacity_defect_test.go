package worker

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/aadityakv/crane/internal/clock"
	"github.com/aadityakv/crane/internal/crane/protocol"
	"github.com/aadityakv/crane/internal/wire"
)

// This file is the minimal deterministic repro of the production defect found
// by the Task 25 four-process integration run (2026-09-03), captured live as:
//
//	T25DBG inventory job=… node=2 err=crane worker rejected command:
//	    message 200 rejected with code 1   (43x, plus 44x on node 1)
//	T25DBG controlError msg=200 raw=wire replay cache full
//
// Defect: the coordinator's +5 dial client opens one TCP connection per
// command, and every authenticated handshake on a fresh connection commits one
// entry into the worker's per-sender replay guard for a full ReplayWindow.
// With the mandated coordinator pacing (DefaultRescanInterval 500ms drives
// fence + status per pass, and an unconverged activateJob pass adds installs,
// per-source checkpoint notices, and inventory queries across every assignment
// node), the per-sender budget (DefaultMaxControlReplayEntriesPerPeer, formerly 512
// entries per ReplayWindow, 2 minutes at production defaults) is exhausted in
// seconds. The rejection then surfaces through controlError's default branch
// as WorkerErrorMalformed with Retryable=false — a transient capacity
// condition masquerading as a deterministic malformed request. The
// coordinator retries only at rescan pace against a cache that stays full
// (each admitted command re-fills it), and the worker-side repair driver
// treats a deterministic typed rejection as terminal for its grant (Task 24
// block-fix I1 ruling), so activation, repair, and sealing livelock. The
// real-process suite failed four consecutive runs in four different
// subscenarios (worker_crash_exactly_after_received,
// worker_crash_exactly_after_processed, store_loss_…,
// leader_loss_after_assignment) with this single root cause.
//
// The two contracts below are what the fix must restore; both are RED on the
// current tree.

// newReplayDefectService returns one ready worker service over a pipe
// listener; only controlError's mapping is exercised, so no client traffic is
// driven through it.
func newReplayDefectService(t *testing.T) *Service {
	t.Helper()
	fixture := newWorkerServiceFixture(t, false)
	service, err := NewService(fixture.options())
	if err != nil {
		t.Fatal(err)
	}
	listener := newServicePipeListener()
	service.listen = func(_, _ string) (net.Listener, error) { return listener, nil }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("Run cancellation = %v", err)
		}
	})
	select {
	case <-service.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("worker service did not become ready")
	}
	return service
}

// TestReplayCapacityRejectionIsTypedRetryable pins the error-matrix contract:
// exhausting the bounded replay cache is a transient capacity condition and
// must answer as a retryable typed WorkerError, never as the default
// non-retryable WorkerErrorMalformed ("request rejected").
func TestReplayCapacityRejectionIsTypedRetryable(t *testing.T) {
	service := newReplayDefectService(t)
	rejection, ok := service.controlError(wire.MessageCraneWorkerHandshake, wire.ErrReplayCacheFull).(protocol.WorkerError)
	if !ok {
		t.Fatalf("replay-capacity rejection rendered as %T, want protocol.WorkerError", rejection)
	}
	if rejection.Code != protocol.WorkerErrorCapacity || !rejection.Retryable {
		t.Fatalf("replay-capacity rejection = code %d retryable %v, want WorkerErrorCapacity retryable", rejection.Code, rejection.Retryable)
	}
}

// TestPerPeerReplayBudgetSurvivesConnectionPerCommandPacing drives the exact
// coordinator pattern — one fresh authenticated session per command from one
// sender — against a ControlOwner configured with the production defaults
// (DefaultMaxControlReplayEntriesPerPeer entries per peer, default ReplayWindow, frozen clock so nothing
// expires). Command 513 within the window must still be refused only through
// a retryable typed rejection; today it is refused as an untyped error that
// controlError renders WorkerErrorMalformed/Retryable=false, which is the
// livelock the integration run captured.
func TestPerPeerReplayBudgetSurvivesConnectionPerCommandPacing(t *testing.T) {
	fixture := newControlFixture(t)
	owner, err := NewControlOwner(ControlOptions{
		Config: fixture.configuration, ClusterID: fixture.cluster, Repository: fixture.repository,
		Engine: fixture.engine, Transfer: fixture.transfer, Gate: fixture.gate, Membership: fixture.members,
		Clock: clock.NewManual(time.Unix(100, 0)),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	handshake := func(index int) error {
		session, sessionErr := owner.NewSession(&net.TCPAddr{IP: net.ParseIP("127.0.0.2"), Port: 40000}, func() error { return nil })
		if sessionErr != nil {
			return sessionErr
		}
		defer session.Close()
		frame := fixture.frameUnchecked(2, byte(1), fixture.handshake(2))
		frame.Header.RequestID = wire.RequestID{byte(index >> 24), byte(index >> 16), byte(index >> 8), byte(index)}
		_, handleErr := session.Handle(context.Background(), frame)
		return handleErr
	}
	limit := DefaultMaxControlReplayEntriesPerPeer
	for index := 1; index <= limit; index++ {
		if err := handshake(index); err != nil {
			t.Fatalf("handshake %d within the per-peer budget failed: %v", index, err)
		}
	}
	err = handshake(limit + 1)
	if err == nil {
		t.Fatal("handshake past the per-peer replay budget was admitted")
	}
	if !errors.Is(err, wire.ErrReplayCacheFull) {
		t.Fatalf("handshake past the budget failed with %v, want wire.ErrReplayCacheFull", err)
	}
	// The livelock: this typed-transient condition is what controlError must
	// classify as retryable capacity, not as deterministic malformed input.
	service := newReplayDefectService(t)
	rejection, ok := service.controlError(wire.MessageCraneWorkerHandshake, err).(protocol.WorkerError)
	if !ok || rejection.Code != protocol.WorkerErrorCapacity || !rejection.Retryable {
		t.Fatalf("replay-capacity handshake rejection = %#v, want retryable WorkerErrorCapacity", rejection)
	}
}

// TestTimestampWindowRejectionCarriesItsOwnDetail pins that a sender whose
// frame timestamp falls outside the replay window is told so: the rejection
// stays retryable capacity (a fresh transmission carries a fresh timestamp)
// but must not be misdescribed as replay-budget exhaustion.
func TestTimestampWindowRejectionCarriesItsOwnDetail(t *testing.T) {
	service := newReplayDefectService(t)
	budget, ok := service.controlError(wire.MessageCraneWorkerHandshake, wire.ErrReplayCacheFull).(protocol.WorkerError)
	if !ok {
		t.Fatalf("replay-budget rejection rendered as %T, want protocol.WorkerError", budget)
	}
	skew, ok := service.controlError(wire.MessageCraneWorkerHandshake, wire.ErrTimestamp).(protocol.WorkerError)
	if !ok {
		t.Fatalf("timestamp rejection rendered as %T, want protocol.WorkerError", skew)
	}
	if skew.Code != protocol.WorkerErrorCapacity || !skew.Retryable {
		t.Fatalf("timestamp rejection = code %d retryable %v, want WorkerErrorCapacity retryable", skew.Code, skew.Retryable)
	}
	if string(skew.Detail) != "request timestamp outside the replay window" || string(skew.Detail) == string(budget.Detail) {
		t.Fatalf("timestamp rejection detail = %q (budget detail %q), want a distinct timestamp-window detail", skew.Detail, budget.Detail)
	}
}
