package admission

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"

	"github.com/aadityakv/crane/internal/crane/model"
)

func TestGateStartsClosedAndRejectsZeroOrClosedEpochs(t *testing.T) {
	gate := NewGate()
	if release, err := gate.Enter(); !errors.Is(err, ErrClosed) || release != nil {
		t.Fatalf("closed Enter returned release=%t, err=%v", release != nil, err)
	}
	if err := gate.Open(model.CoordinatorEpoch{}); err == nil {
		t.Fatal("zero epoch opened gate")
	}
	epoch := gateEpoch(2)
	if err := gate.Open(epoch); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := gate.Open(epoch); err != nil {
		t.Fatalf("idempotent Open: %v", err)
	}
	if err := gate.CloseAndWait(context.Background()); err != nil {
		t.Fatalf("CloseAndWait: %v", err)
	}
	if err := gate.Open(epoch); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("reopen closed epoch = %v", err)
	}
	if err := gate.Open(gateEpoch(1)); err == nil {
		t.Fatal("older epoch reopened gate")
	}
	if err := gate.Open(gateEpoch(3)); err != nil {
		t.Fatalf("newer epoch did not reopen: %v", err)
	}
}

func TestGateCloseAtomicallyBlocksNewEntrantsAndDrainsConcurrentWaiters(t *testing.T) {
	gate := NewGate()
	if err := gate.Open(gateEpoch(1)); err != nil {
		t.Fatal(err)
	}
	release, err := gate.Enter()
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- gate.CloseAndWait(context.Background())
		}()
	}
	closed := false
	for i := 0; i < 1_000_000; i++ {
		another, enterErr := gate.Enter()
		if errors.Is(enterErr, ErrClosed) && another == nil {
			closed = true
			break
		}
		if enterErr == nil {
			another()
		}
		runtime.Gosched()
	}
	if !closed {
		t.Fatal("CloseAndWait did not atomically close admission")
	}
	if err := gate.Open(gateEpoch(2)); !errors.Is(err, ErrClosed) {
		t.Fatalf("new epoch opened before prior entrants drained: %v", err)
	}
	select {
	case err := <-results:
		t.Fatalf("CloseAndWait returned before active entrant left: %v", err)
	default:
	}
	release()
	release() // release is idempotent and cannot underflow the gate.
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("CloseAndWait = %v", err)
		}
	}
	if err := gate.Open(gateEpoch(2)); err != nil {
		t.Fatalf("new epoch did not open after drain: %v", err)
	}
}

func TestGateCanceledCloseWaiterDoesNotLeakOrReopen(t *testing.T) {
	gate := NewGate()
	if err := gate.Open(gateEpoch(3)); err != nil {
		t.Fatal(err)
	}
	release, err := gate.Enter()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := gate.CloseAndWait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("CloseAndWait cancellation = %v", err)
	}
	release()
	if err := gate.CloseAndWait(context.Background()); err != nil {
		t.Fatalf("joining close waiter = %v", err)
	}
	if err := gate.Open(gateEpoch(3)); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("cancellation forgot closed generation: %v", err)
	}
}

func TestGateReportsAdmissionEpoch(t *testing.T) {
	gate := NewGate()
	if epoch, open := gate.AdmissionEpoch(); open || epoch != (model.CoordinatorEpoch{}) {
		t.Fatalf("closed gate reported admission epoch %#v open=%t", epoch, open)
	}
	epoch := gateEpoch(2)
	if err := gate.Open(epoch); err != nil {
		t.Fatal(err)
	}
	if got, open := gate.AdmissionEpoch(); !open || got != epoch {
		t.Fatalf("open gate reported admission epoch %#v open=%t", got, open)
	}
	if err := gate.CloseAndWait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, open := gate.AdmissionEpoch(); open {
		t.Fatal("closed-through gate reported open")
	}
}

func gateEpoch(term uint64) model.CoordinatorEpoch {
	return model.CoordinatorEpoch{Term: term, BeginIndex: term, Coordinator: 1, Nonce: [16]byte{byte(term)}}
}
