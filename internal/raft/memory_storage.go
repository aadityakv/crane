package raft

import (
	"bytes"
	"crypto/sha256"
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
	// StorageOperationSnapshotStageWrite fails before newly staged bytes are owned.
	StorageOperationSnapshotStageWrite
	// StorageOperationSnapshotStageSync fails before a staged offset becomes acknowledged.
	StorageOperationSnapshotStageSync
	// StorageOperationSnapshotInstall fails before the completed snapshot mutates durable state.
	StorageOperationSnapshotInstall
)

// MemoryStore is a deterministic, transaction-safe in-memory StableStore.
type MemoryStore struct {
	mu            sync.Mutex
	identity      StorageIdentity
	voters        VoterSet
	snapshotLimit uint64
	state         RecoveredState
	closed        bool
	faults        map[StorageOperation][]error
	stage         *memorySnapshotStage
}

type memorySnapshotStage struct {
	metadata snapshotTransferMetadata
	bytes    []byte
}

// StageSnapshotChunk durably advances one exact in-memory transfer chunk.
func (store *MemoryStore) StageSnapshotChunk(request InstallSnapshotRequest) (SnapshotStageResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return SnapshotStageResult{}, ErrStoreClosed
	}
	limits := DefaultCodecLimits()
	limits.MaxSnapshotBytes = store.snapshotLimit
	if err := validateSnapshotRequest(request, limits); err != nil {
		store.stage = nil
		return SnapshotStageResult{}, fmt.Errorf("%w: %v", ErrSnapshotRejected, err)
	}
	metadata := snapshotTransferMetadataFor(request)
	if store.stage == nil {
		if request.Offset != 0 {
			return SnapshotStageResult{}, fmt.Errorf("%w: transfer must begin at offset zero", ErrSnapshotRejected)
		}
		store.stage = &memorySnapshotStage{metadata: metadata}
	} else if store.stage.metadata != metadata {
		store.stage = nil
		return SnapshotStageResult{}, fmt.Errorf("%w: transfer metadata changed", ErrSnapshotRejected)
	}

	nextOffset := uint64(len(store.stage.bytes))
	end := request.Offset + uint64(len(request.Chunk))
	switch {
	case request.Offset == nextOffset:
		if err := store.takeFault(StorageOperationSnapshotStageWrite); err != nil {
			store.stage = nil
			return SnapshotStageResult{}, fmt.Errorf("stage memory raft snapshot bytes: %w", err)
		}
		candidate := append(cloneBytes(store.stage.bytes), cloneBytes(request.Chunk)...)
		if err := store.takeFault(StorageOperationSnapshotStageSync); err != nil {
			store.stage = nil
			return SnapshotStageResult{}, fmt.Errorf("sync memory raft snapshot bytes: %w", err)
		}
		store.stage.bytes = candidate
		nextOffset = end
	case end <= nextOffset:
		if !bytes.Equal(store.stage.bytes[request.Offset:end], request.Chunk) {
			store.stage = nil
			return SnapshotStageResult{}, fmt.Errorf("%w: duplicate bytes changed", ErrSnapshotRejected)
		}
	case request.Offset < nextOffset && end > nextOffset:
		store.stage = nil
		return SnapshotStageResult{}, fmt.Errorf("%w: chunk partially overlaps durable bytes", ErrSnapshotRejected)
	default:
		store.stage = nil
		return SnapshotStageResult{}, fmt.Errorf("%w: chunk leaves an offset gap", ErrSnapshotRejected)
	}

	if !request.Done {
		return SnapshotStageResult{NextOffset: nextOffset}, nil
	}
	stateBytes := cloneBytes(store.stage.bytes)
	checksum := sha256.Sum256(stateBytes)
	if SnapshotChecksum(checksum) != request.Checksum {
		store.stage = nil
		return SnapshotStageResult{}, fmt.Errorf("%w: complete state checksum mismatch", ErrSnapshotRejected)
	}
	snapshot, err := NewSnapshot(store.identity, metadata.metadata, stateBytes, store.snapshotLimit)
	if err != nil || snapshot.ID != request.SnapshotID {
		store.stage = nil
		return SnapshotStageResult{}, fmt.Errorf("%w: complete snapshot identity mismatch: %v", ErrSnapshotRejected, err)
	}
	prospective, err := installRecoveredSnapshot(store.state, snapshot, store.identity, store.voters)
	if err != nil {
		store.stage = nil
		return SnapshotStageResult{}, err
	}
	if err := store.takeFault(StorageOperationSnapshotInstall); err != nil {
		store.stage = nil
		return SnapshotStageResult{}, fmt.Errorf("install memory raft snapshot: %w", err)
	}
	store.state = prospective
	store.stage = nil
	return SnapshotStageResult{NextOffset: nextOffset, Done: true, State: prospective.Clone()}, nil
}

// AbortSnapshotStage discards one incomplete in-memory transfer.
func (store *MemoryStore) AbortSnapshotStage() error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrStoreClosed
	}
	store.stage = nil
	return nil
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
	prospective, err := compactRecoveredState(store.state, snapshot, store.identity, store.voters, store.snapshotLimit)
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
	return NewMemoryStoreWithOptions(identity, voters, StoreOptions{})
}

// NewMemoryStoreWithOptions returns an empty store with one immutable snapshot limit.
func NewMemoryStoreWithOptions(identity StorageIdentity, voters VoterSet, options StoreOptions) (*MemoryStore, error) {
	limit, err := options.snapshotLimit()
	if err != nil {
		return nil, err
	}
	state := RecoveredState{Identity: identity}
	if err := ValidateRecoveredState(state, identity, voters); err != nil {
		return nil, err
	}
	return &MemoryStore{
		identity:      identity,
		voters:        voters,
		snapshotLimit: limit,
		state:         state,
		faults:        make(map[StorageOperation][]error),
	}, nil
}

// SnapshotLimit returns the immutable maximum snapshot state bytes.
func (store *MemoryStore) SnapshotLimit() uint64 { return store.snapshotLimit }

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
	if err := validateRecoveredStateWithSnapshotLimit(store.state, store.identity, store.voters, store.snapshotLimit); err != nil {
		return RecoveredState{}, err
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
	if batch.Snapshot != nil && uint64(len(batch.Snapshot.state)) > store.snapshotLimit {
		return fmt.Errorf("%w: snapshot state length %d exceeds configured limit %d", ErrInvalidStorageState, len(batch.Snapshot.state), store.snapshotLimit)
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
	store.stage = nil
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
