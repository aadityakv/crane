// Package admission provides the process-wide synchronous Crane work gate.
package admission

import (
	"context"
	"errors"
	"sync"

	"github.com/aaditya/cs425mp3/internal/crane/model"
)

var (
	// ErrClosed reports that the shared process gate is not admitting new work.
	ErrClosed = errors.New("crane admission gate is closed")
	// ErrStaleEpoch reports an attempt to reopen a leadership generation that
	// has already been closed or that collides at the same Raft order.
	ErrStaleEpoch = errors.New("stale Crane admission epoch")
)

// Gate is a caller-owned, closed-by-default admission barrier. Enter and
// CloseAndWait are synchronous so fencing can drain every admitted mutation
// before durable authority advances.
type Gate struct {
	mu            sync.Mutex
	open          bool
	epoch         model.CoordinatorEpoch
	closedThrough model.CoordinatorEpoch
	active        uint64
	drained       chan struct{}
}

// NewGate returns a closed process gate with no accepted leadership epoch.
func NewGate() *Gate { return &Gate{} }

// Open admits work for epoch when it is strictly newer than every closed
// generation. Repeating the exact epoch is idempotent only while still open.
func (gate *Gate) Open(epoch model.CoordinatorEpoch) error {
	if gate == nil {
		return ErrClosed
	}
	if err := epoch.Validate(); err != nil {
		return err
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if gate.open && epoch == gate.epoch {
		return nil
	}
	if gate.active != 0 {
		return ErrClosed
	}
	if gate.open || gate.closedThrough != (model.CoordinatorEpoch{}) {
		floor := gate.closedThrough
		if gate.open {
			floor = gate.epoch
		}
		comparison := compareEpoch(epoch, floor)
		if comparison <= 0 {
			return ErrStaleEpoch
		}
	}
	gate.open = true
	gate.epoch = epoch
	return nil
}

// Enter reserves one active admission. The returned release function is
// idempotent and must be called after the guarded durable recheck/mutation.
func (gate *Gate) Enter() (func(), error) {
	if gate == nil {
		return nil, ErrClosed
	}
	gate.mu.Lock()
	if !gate.open {
		gate.mu.Unlock()
		return nil, ErrClosed
	}
	if gate.active == 0 {
		gate.drained = make(chan struct{})
	}
	gate.active++
	gate.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			gate.mu.Lock()
			gate.active--
			if gate.active == 0 && gate.drained != nil {
				close(gate.drained)
				gate.drained = nil
			}
			gate.mu.Unlock()
		})
	}, nil
}

// CloseAndWait atomically blocks new entrants, records the current generation
// as closed, and waits for every entrant that crossed the gate to leave.
func (gate *Gate) CloseAndWait(ctx context.Context) error {
	if gate == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("nil admission close context")
	}
	gate.mu.Lock()
	if gate.open {
		gate.closedThrough = gate.epoch
		gate.open = false
	}
	if gate.active == 0 {
		gate.mu.Unlock()
		return nil
	}
	drained := gate.drained
	gate.mu.Unlock()
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func compareEpoch(left, right model.CoordinatorEpoch) int {
	if left.Term < right.Term {
		return -1
	}
	if left.Term > right.Term {
		return 1
	}
	if left.BeginIndex < right.BeginIndex {
		return -1
	}
	if left.BeginIndex > right.BeginIndex {
		return 1
	}
	return 0
}

// AdmissionEpoch reports the coordinator epoch the process gate is currently
// open under. It reports the zero epoch and false exactly while the gate is
// closed, so a durable status exchange can distinguish a fresh process (closed
// gate, no accepted generation) from an admitted one.
func (gate *Gate) AdmissionEpoch() (model.CoordinatorEpoch, bool) {
	if gate == nil {
		return model.CoordinatorEpoch{}, false
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	return gate.epoch, gate.open
}
