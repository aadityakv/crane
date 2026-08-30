package raft

import (
	"errors"
	"fmt"
	"math"
	"sync"
)

// StorageOperation names one fault-injection point shared by deterministic stores.
type StorageOperation uint8

const (
	// StorageOperationRecover fails the next recovery request.
	StorageOperationRecover StorageOperation = iota + 1
	// StorageOperationPersist fails the next persistence request before mutation.
	StorageOperationPersist
	// StorageOperationClose fails the next close request before resources are released.
	StorageOperationClose
	// StorageOperationSnapshotPersist fails the next snapshot compaction before mutation.
	StorageOperationSnapshotPersist
)

// MemoryStore is a deterministic, transaction-safe in-memory StableStore.
type MemoryStore struct {
	mu       sync.Mutex
	identity StorageIdentity
	voters   VoterSet
	state    RecoveredState
	closed   bool
	faults   map[StorageOperation][]error
}

// PersistSnapshot atomically installs an exact local snapshot and compacts its covered prefix.
func (store *MemoryStore) PersistSnapshot(snapshot Snapshot) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrStoreClosed
	}
	if err := store.takeFault(StorageOperationSnapshotPersist); err != nil {
		return fmt.Errorf("persist memory raft snapshot: %w", err)
	}
	prospective, err := compactRecoveredState(store.state, snapshot, store.identity, store.voters)
	if err != nil {
		return err
	}
	store.state = prospective
	return nil
}

// RetainedWALBytes reports deterministic canonical retained entry bytes.
func (store *MemoryStore) RetainedWALBytes() (uint64, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return 0, ErrStoreClosed
	}
	var total uint64
	for _, entry := range store.state.Entries {
		addition := uint64(minimumWALEntryBytes + len(entry.command))
		if total > math.MaxUint64-addition {
			return 0, ErrLogOverflow
		}
		total += addition
	}
	return total, nil
}

// NewMemoryStore returns an empty store bound to identity and voters.
func NewMemoryStore(identity StorageIdentity, voters VoterSet) (*MemoryStore, error) {
	state := RecoveredState{Identity: identity}
	if err := ValidateRecoveredState(state, identity, voters); err != nil {
		return nil, err
	}
	return &MemoryStore{
		identity: identity,
		voters:   voters,
		state:    state,
		faults:   make(map[StorageOperation][]error),
	}, nil
}

// FailNext queues err at one deterministic operation boundary.
func (store *MemoryStore) FailNext(operation StorageOperation, err error) {
	if err == nil {
		return
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.faults[operation] = append(store.faults[operation], err)
}

// Recover returns an independently owned checked state.
func (store *MemoryStore) Recover() (RecoveredState, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return RecoveredState{}, ErrStoreClosed
	}
	if err := store.takeFault(StorageOperationRecover); err != nil {
		return RecoveredState{}, fmt.Errorf("recover memory raft store: %w", err)
	}
	return store.state.Clone(), nil
}

// Persist atomically validates and applies batch without retaining caller aliases.
func (store *MemoryStore) Persist(batch PersistenceBatch) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrStoreClosed
	}
	if err := validatePersistenceBatchBounds(batch); err != nil {
		return err
	}
	if err := store.takeFault(StorageOperationPersist); err != nil {
		return fmt.Errorf("persist memory raft store: %w", err)
	}
	prospective, err := applyValidatedPersistenceBatch(store.state, batch, store.identity, store.voters)
	if err != nil {
		return err
	}
	store.state = prospective
	return nil
}

// Close releases the memory store exactly once.
func (store *MemoryStore) Close() error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrStoreClosed
	}
	if err := store.takeFault(StorageOperationClose); err != nil {
		return fmt.Errorf("close memory raft store: %w", err)
	}
	store.closed = true
	return nil
}

func (store *MemoryStore) takeFault(operation StorageOperation) error {
	queued := store.faults[operation]
	if len(queued) == 0 {
		return nil
	}
	err := queued[0]
	if len(queued) == 1 {
		delete(store.faults, operation)
	} else {
		store.faults[operation] = queued[1:]
	}
	if errors.Is(err, nil) {
		return nil
	}
	return err
}
