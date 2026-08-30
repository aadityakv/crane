package raft

import (
	"bytes"
	"fmt"
)

func (node *Node) validateReady(ready Ready) error {
	_, _, err := node.validateReadyAndDerive(ready)
	return err
}

func (node *Node) validateReadyAndDerive(ready Ready) (PersistenceBatch, RecoveredState, error) {
	if err := node.validateReadyStructure(ready); err != nil {
		return PersistenceBatch{}, RecoveredState{}, err
	}
	batch := persistenceBatchForReady(ready)
	prospective := node.durableState.Clone()
	if persistenceBatchHasEffects(batch) {
		var err error
		prospective, err = applyPersistenceBatch(node.durableState, batch, node.options.Identity, node.options.Voters)
		if err != nil {
			return PersistenceBatch{}, RecoveredState{}, fmt.Errorf("%w: prospective Ready persistence: %v", ErrInvalidCoreState, err)
		}
	}
	if err := node.validateProspectiveCoreState(prospective); err != nil {
		return PersistenceBatch{}, RecoveredState{}, err
	}
	for _, message := range ready.Messages {
		if err := node.validateMessageDurability(message, node.durableState, prospective); err != nil {
			return PersistenceBatch{}, RecoveredState{}, err
		}
	}
	return batch, prospective, nil
}

func persistenceBatchHasEffects(batch PersistenceBatch) bool {
	return batch.HardState != nil || batch.ReplaceFrom != 0 || batch.SnapshotBase != nil || batch.AppliedIndex != nil
}

func (node *Node) validateProspectiveCoreState(prospective RecoveredState) error {
	if prospective.HardState != node.core.HardState() {
		return fmt.Errorf("%w: Ready persistence does not match Core HardState", ErrInvalidCoreState)
	}
	state := node.core.log.State()
	if prospective.SnapshotBase.LastIncludedIndex != state.SnapshotIndex ||
		prospective.SnapshotBase.LastIncludedTerm != state.SnapshotTerm {
		return fmt.Errorf("%w: Ready persistence does not match Core log base", ErrInvalidCoreState)
	}
	if len(prospective.Entries) != len(state.Entries) {
		return fmt.Errorf("%w: Ready persistence retained %d entries, Core has %d", ErrInvalidCoreState, len(prospective.Entries), len(state.Entries))
	}
	for index := range state.Entries {
		if !sameEntry(prospective.Entries[index], state.Entries[index]) {
			return fmt.Errorf("%w: Ready persistence differs from Core entry %d", ErrInvalidCoreState, state.Entries[index].Index)
		}
	}
	return nil
}

func (node *Node) validateMessageDurability(message PeerMessage, current, prospective RecoveredState) error {
	requiredHardState := false
	requiredEntriesThrough := uint64(0)

	requireTerm := func(term uint64) error {
		if prospective.HardState.Term < term {
			return fmt.Errorf("%w: outbound RPC term %d is not durable", ErrInvalidCoreState, term)
		}
		if current.HardState.Term < term {
			requiredHardState = true
		}
		return nil
	}
	requireCommit := func(commit uint64) error {
		if prospective.HardState.CommitIndex < commit {
			return fmt.Errorf("%w: outbound RPC commit %d is not durable", ErrInvalidCoreState, commit)
		}
		if current.HardState.CommitIndex < commit {
			requiredHardState = true
		}
		return nil
	}
	requireVote := func(term uint64, voter uint16) error {
		if prospective.HardState.Term != term || prospective.HardState.VotedFor != voter {
			return fmt.Errorf("%w: outbound vote for %d in term %d is not durable", ErrInvalidCoreState, voter, term)
		}
		if current.HardState.Term != term || current.HardState.VotedFor != voter {
			requiredHardState = true
		}
		return nil
	}
	requireCoreEntry := func(index uint64) error {
		if index == 0 {
			return nil
		}
		coreEntry, err := node.core.log.Entry(index)
		if err != nil {
			return fmt.Errorf("%w: outbound RPC references unavailable Core entry %d", ErrInvalidCoreState, index)
		}
		prospectiveEntry, exists := recoveredEntryAt(prospective, index)
		if !exists || !sameEntry(prospectiveEntry, coreEntry) {
			return fmt.Errorf("%w: outbound RPC entry %d is not durably identical", ErrInvalidCoreState, index)
		}
		currentEntry, exists := recoveredEntryAt(current, index)
		if !exists || !sameEntry(currentEntry, coreEntry) {
			if index > requiredEntriesThrough {
				requiredEntriesThrough = index
			}
		}
		return nil
	}
	requireRPCEntry := func(entry Entry) error {
		prospectiveEntry, exists := recoveredEntryAt(prospective, entry.Index)
		if !exists || !sameEntry(prospectiveEntry, entry) {
			return fmt.Errorf("%w: outbound RPC carries entry %d without identical durability", ErrInvalidCoreState, entry.Index)
		}
		currentEntry, exists := recoveredEntryAt(current, entry.Index)
		if !exists || !sameEntry(currentEntry, entry) {
			if entry.Index > requiredEntriesThrough {
				requiredEntriesThrough = entry.Index
			}
		}
		return nil
	}

	switch rpc := message.RPC.(type) {
	case PreVoteRequest:
		if err := requireTerm(rpc.CurrentTerm); err != nil {
			return err
		}
	case PreVoteResponse:
		if err := requireTerm(rpc.Term); err != nil {
			return err
		}
	case RequestVoteRequest:
		if err := requireVote(rpc.Term, rpc.CandidateID); err != nil {
			return err
		}
	case RequestVoteResponse:
		if err := requireTerm(rpc.Term); err != nil {
			return err
		}
		if rpc.Granted {
			if err := requireVote(rpc.Term, rpc.CandidateID); err != nil {
				return err
			}
		}
	case AppendEntriesRequest:
		if err := requireTerm(rpc.Term); err != nil {
			return err
		}
		if err := requireCommit(rpc.LeaderCommit); err != nil {
			return err
		}
		if rpc.PrevLogIndex != 0 {
			term, err := recoveredTermAt(prospective, rpc.PrevLogIndex)
			if err != nil || term != rpc.PrevLogTerm {
				return fmt.Errorf("%w: outbound AppendEntries previous position is not durable", ErrInvalidCoreState)
			}
			if rpc.PrevLogIndex > prospective.SnapshotBase.LastIncludedIndex {
				if err := requireCoreEntry(rpc.PrevLogIndex); err != nil {
					return err
				}
			}
		}
		for _, entry := range rpc.Entries {
			if err := requireRPCEntry(entry); err != nil {
				return err
			}
		}
	case AppendEntriesResponse:
		if err := requireTerm(rpc.Term); err != nil {
			return err
		}
		if rpc.Success {
			if err := requireCommit(node.core.HardState().CommitIndex); err != nil {
				return err
			}
			if err := requireCoreEntry(rpc.MatchIndex); err != nil {
				return err
			}
		}
	case InstallSnapshotRequest:
		if err := requireTerm(rpc.Term); err != nil {
			return err
		}
		metadata := SnapshotMetadata{LastIncludedIndex: rpc.LastIncludedIndex, LastIncludedTerm: rpc.LastIncludedTerm, StateMachineSchemaVersion: rpc.StateMachineSchemaVersion}
		if prospective.Snapshot == nil || prospective.Snapshot.ID != rpc.SnapshotID ||
			prospective.Snapshot.Metadata != metadata || prospective.Snapshot.StateChecksum != rpc.Checksum {
			return fmt.Errorf("%w: outbound InstallSnapshot does not match durable snapshot", ErrInvalidCoreState)
		}
		state := prospective.Snapshot.StateBytes()
		end := rpc.Offset + uint64(len(rpc.Chunk))
		if rpc.TotalLength != uint64(len(state)) || end > uint64(len(state)) ||
			!bytes.Equal(rpc.Chunk, state[rpc.Offset:end]) {
			return fmt.Errorf("%w: outbound InstallSnapshot chunk is not durably identical", ErrInvalidCoreState)
		}
	case InstallSnapshotResponse:
		if err := requireTerm(rpc.Term); err != nil {
			return err
		}
	}

	if message.Requires.HardState != requiredHardState {
		return fmt.Errorf("%w: outbound RPC HardState declaration=%t want=%t", ErrInvalidCoreState, message.Requires.HardState, requiredHardState)
	}
	if message.Requires.EntriesThrough != requiredEntriesThrough {
		return fmt.Errorf("%w: outbound RPC entries declaration=%d want=%d", ErrInvalidCoreState, message.Requires.EntriesThrough, requiredEntriesThrough)
	}
	return nil
}

func recoveredEntryAt(state RecoveredState, index uint64) (Entry, bool) {
	if index <= state.SnapshotBase.LastIncludedIndex {
		return Entry{}, false
	}
	offset := index - state.SnapshotBase.LastIncludedIndex - 1
	if offset >= uint64(len(state.Entries)) {
		return Entry{}, false
	}
	return state.Entries[offset].Clone(), true
}

func recoveredTermAt(state RecoveredState, index uint64) (uint64, error) {
	if index == 0 {
		return 0, nil
	}
	if index == state.SnapshotBase.LastIncludedIndex {
		return state.SnapshotBase.LastIncludedTerm, nil
	}
	entry, exists := recoveredEntryAt(state, index)
	if !exists {
		return 0, ErrLogUnavailable
	}
	return entry.Term, nil
}
