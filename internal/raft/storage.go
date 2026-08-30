package raft

import (
	"fmt"
	"math"
)

// StableStore durably owns all safety-critical Raft state for one voter.
type StableStore interface {
	// Recover returns an independently owned, fully validated state.
	Recover() (RecoveredState, error)
	// Persist atomically makes every effect in one owned batch durable.
	Persist(PersistenceBatch) error
	// Close releases the store exactly once.
	Close() error
}

// RecoveredState is one independently owned durable voter state.
type RecoveredState struct {
	// Identity binds this state to a format, cluster, local voter, and fixed voter set.
	Identity StorageIdentity
	// HardState is the latest durable term, vote, and commit position.
	HardState HardState
	// SnapshotBase is the compacted log position, or zero before the first snapshot.
	SnapshotBase SnapshotMetadata
	// AppliedIndex is the durable recovery seam at or after SnapshotBase.
	AppliedIndex uint64
	// Entries is the retained contiguous suffix immediately after SnapshotBase.
	Entries []Entry
}

// Clone returns an independently owned copy of the complete recovered state.
func (state RecoveredState) Clone() RecoveredState {
	state.Entries = cloneEntries(state.Entries)
	return state
}

// PersistenceBatch is one atomic durable transition. ReplaceFrom explicitly
// truncates the retained suffix beginning at that index before Entries are
// installed; zero means the log is unchanged.
type PersistenceBatch struct {
	// HardState replaces the durable hard state when non-nil.
	HardState *HardState
	// ReplaceFrom is the first retained index replaced or removed by this batch.
	ReplaceFrom uint64
	// Entries is an owned contiguous replacement suffix beginning at ReplaceFrom.
	Entries []Entry
	// SnapshotBase replaces the compacted base when non-nil (reserved for Task 9).
	SnapshotBase *SnapshotMetadata
	// AppliedIndex replaces the durable recovery seam when non-nil.
	AppliedIndex *uint64
}

// Clone returns a batch whose pointers, slice, entries, and command bytes are independent.
func (batch PersistenceBatch) Clone() PersistenceBatch {
	owned := batch
	if batch.HardState != nil {
		state := *batch.HardState
		owned.HardState = &state
	}
	if batch.SnapshotBase != nil {
		base := *batch.SnapshotBase
		owned.SnapshotBase = &base
	}
	if batch.AppliedIndex != nil {
		index := *batch.AppliedIndex
		owned.AppliedIndex = &index
	}
	owned.Entries = cloneEntries(batch.Entries)
	return owned
}

// ValidateRecoveredState verifies identity and every durable Raft state invariant.
func ValidateRecoveredState(state RecoveredState, expected StorageIdentity, voters VoterSet) error {
	if expected.FormatVersion != StorageFormatVersion1 || state.Identity != expected {
		return fmt.Errorf("%w: got format=%d cluster=%x local=%d voters=%x", ErrInvalidStorageState, state.Identity.FormatVersion, state.Identity.ClusterID, state.Identity.LocalVoterID, state.Identity.VoterFingerprint)
	}
	if state.Identity.ClusterID == ([16]byte{}) || state.Identity.VoterFingerprint != voters.Fingerprint() {
		return fmt.Errorf("%w: identity does not match configured voters", ErrInvalidStorageState)
	}
	if err := voters.ValidateLocalID(state.Identity.LocalVoterID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidStorageState, err)
	}
	if state.HardState.VotedFor != 0 {
		if state.HardState.Term == 0 || !voters.Contains(state.HardState.VotedFor) {
			return fmt.Errorf("%w: vote %d is invalid in term %d", ErrInvalidStorageState, state.HardState.VotedFor, state.HardState.Term)
		}
	}
	base := state.SnapshotBase
	if (base.LastIncludedIndex == 0) != (base.LastIncludedTerm == 0) {
		return fmt.Errorf("%w: snapshot index and term must both be zero or nonzero", ErrInvalidStorageState)
	}
	if base.LastIncludedTerm > state.HardState.Term {
		return fmt.Errorf("%w: snapshot term %d exceeds hard-state term %d", ErrInvalidStorageState, base.LastIncludedTerm, state.HardState.Term)
	}
	if base.LastIncludedIndex > state.AppliedIndex || state.AppliedIndex > state.HardState.CommitIndex {
		return fmt.Errorf("%w: snapshot=%d applied=%d commit=%d", ErrInvalidStorageState, base.LastIncludedIndex, state.AppliedIndex, state.HardState.CommitIndex)
	}

	lastIndex := base.LastIncludedIndex
	previousTerm := base.LastIncludedTerm
	for index, entry := range state.Entries {
		if lastIndex == math.MaxUint64 {
			return fmt.Errorf("%w: retained index overflows after %d", ErrInvalidStorageState, lastIndex)
		}
		lastIndex++
		if entry.Index != lastIndex {
			return fmt.Errorf("%w: entry %d index=%d want=%d", ErrInvalidStorageState, index, entry.Index, lastIndex)
		}
		if err := validateLogEntry(entry); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidStorageState, err)
		}
		if entry.Term < previousTerm || entry.Term > state.HardState.Term {
			return fmt.Errorf("%w: entry %d term=%d after=%d hard=%d", ErrInvalidStorageState, entry.Index, entry.Term, previousTerm, state.HardState.Term)
		}
		previousTerm = entry.Term
	}
	if state.HardState.CommitIndex > lastIndex {
		return fmt.Errorf("%w: commit=%d last=%d", ErrInvalidStorageState, state.HardState.CommitIndex, lastIndex)
	}
	return nil
}

func applyPersistenceBatch(current RecoveredState, batch PersistenceBatch, expected StorageIdentity, voters VoterSet) (RecoveredState, error) {
	batch = batch.Clone()
	if batch.HardState == nil && batch.ReplaceFrom == 0 && len(batch.Entries) == 0 && batch.SnapshotBase == nil && batch.AppliedIndex == nil {
		return RecoveredState{}, fmt.Errorf("%w: empty persistence batch", ErrInvalidStorageState)
	}
	if batch.ReplaceFrom == 0 && len(batch.Entries) != 0 {
		return RecoveredState{}, fmt.Errorf("%w: replacement entries require ReplaceFrom", ErrInvalidStorageState)
	}
	prospective := current.Clone()
	if batch.SnapshotBase != nil {
		base := *batch.SnapshotBase
		if base.LastIncludedIndex < prospective.SnapshotBase.LastIncludedIndex {
			return RecoveredState{}, fmt.Errorf("%w: snapshot index regressed", ErrInvalidStorageState)
		}
		if base.LastIncludedIndex == prospective.SnapshotBase.LastIncludedIndex && base != prospective.SnapshotBase {
			return RecoveredState{}, fmt.Errorf("%w: snapshot metadata changed at the same index", ErrInvalidStorageState)
		}
		if base.LastIncludedIndex > prospective.HardState.CommitIndex {
			return RecoveredState{}, fmt.Errorf("%w: snapshot is not committed", ErrInvalidStorageState)
		}
		if base.LastIncludedIndex > prospective.SnapshotBase.LastIncludedIndex {
			offset := base.LastIncludedIndex - prospective.SnapshotBase.LastIncludedIndex
			if offset > uint64(len(prospective.Entries)) {
				return RecoveredState{}, fmt.Errorf("%w: snapshot index unavailable", ErrInvalidStorageState)
			}
			if offset != 0 && prospective.Entries[offset-1].Term != base.LastIncludedTerm {
				return RecoveredState{}, fmt.Errorf("%w: snapshot term does not match retained log", ErrInvalidStorageState)
			}
			prospective.Entries = cloneEntries(prospective.Entries[offset:])
		}
		prospective.SnapshotBase = base
	}
	if batch.ReplaceFrom != 0 {
		baseIndex := prospective.SnapshotBase.LastIncludedIndex
		if batch.ReplaceFrom <= baseIndex || batch.ReplaceFrom <= prospective.HardState.CommitIndex {
			return RecoveredState{}, fmt.Errorf("%w: replacement begins at protected index %d", ErrInvalidStorageState, batch.ReplaceFrom)
		}
		lastIndex := baseIndex + uint64(len(prospective.Entries))
		if lastIndex < baseIndex || batch.ReplaceFrom > lastIndex+1 || (lastIndex == math.MaxUint64 && batch.ReplaceFrom > lastIndex) {
			return RecoveredState{}, fmt.Errorf("%w: replacement index %d is not contiguous after %d", ErrInvalidStorageState, batch.ReplaceFrom, lastIndex)
		}
		offset64 := batch.ReplaceFrom - baseIndex - 1
		if offset64 > uint64(len(prospective.Entries)) || offset64 > uint64(math.MaxInt) {
			return RecoveredState{}, fmt.Errorf("%w: replacement offset is outside local integer domain", ErrInvalidStorageState)
		}
		if len(batch.Entries) != 0 && batch.Entries[0].Index != batch.ReplaceFrom {
			return RecoveredState{}, fmt.Errorf("%w: first replacement index=%d want=%d", ErrInvalidStorageState, batch.Entries[0].Index, batch.ReplaceFrom)
		}
		entries := cloneEntries(prospective.Entries[:int(offset64)])
		entries = append(entries, cloneEntries(batch.Entries)...)
		prospective.Entries = entries
	}
	if batch.HardState != nil {
		next := *batch.HardState
		if next.Term < prospective.HardState.Term || next.CommitIndex < prospective.HardState.CommitIndex {
			return RecoveredState{}, fmt.Errorf("%w: hard-state term or commit regressed", ErrInvalidStorageState)
		}
		if next.Term == prospective.HardState.Term && prospective.HardState.VotedFor != 0 && next.VotedFor != prospective.HardState.VotedFor {
			return RecoveredState{}, fmt.Errorf("%w: vote changed within term %d", ErrInvalidStorageState, next.Term)
		}
		prospective.HardState = next
	}
	if batch.AppliedIndex != nil {
		if *batch.AppliedIndex < prospective.AppliedIndex {
			return RecoveredState{}, fmt.Errorf("%w: applied index regressed", ErrInvalidStorageState)
		}
		prospective.AppliedIndex = *batch.AppliedIndex
	}
	if err := ValidateRecoveredState(prospective, expected, voters); err != nil {
		return RecoveredState{}, err
	}
	return prospective.Clone(), nil
}
