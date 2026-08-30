package raft

import (
	"errors"
	"fmt"
)

// ConflictHint identifies the local term range a replicating peer should skip.
type ConflictHint struct {
	// Term is the conflicting local term, or zero when the previous index is unavailable.
	Term uint64
	// Index is the first available index in Term, or the next index after the local log.
	Index uint64
}

// LogState is an owned point-in-time copy of the checked log state.
type LogState struct {
	// SnapshotIndex is the compacted logical base.
	SnapshotIndex uint64
	// SnapshotTerm is the term at SnapshotIndex.
	SnapshotTerm uint64
	// CommitIndex is the greatest committed log index.
	CommitIndex uint64
	// AppliedIndex is the greatest applied log index.
	AppliedIndex uint64
	// Entries contains the retained contiguous suffix after SnapshotIndex.
	Entries []Entry
}

// Log owns one compacted base and a contiguous retained Raft log suffix.
type Log struct {
	snapshotIndex uint64
	snapshotTerm  uint64
	commitIndex   uint64
	appliedIndex  uint64
	lastIndex     uint64
	entries       []Entry
}

// NewLog validates recovered indices and takes ownership through defensive copies.
func NewLog(snapshotIndex, snapshotTerm, commitIndex, appliedIndex uint64, entries []Entry) (*Log, error) {
	if (snapshotIndex == 0) != (snapshotTerm == 0) {
		return nil, fmt.Errorf("%w: snapshot index and term must both be zero or nonzero", ErrLogInvariant)
	}
	if appliedIndex < snapshotIndex || commitIndex < appliedIndex {
		return nil, fmt.Errorf("%w: snapshot=%d applied=%d commit=%d", ErrLogInvariant, snapshotIndex, appliedIndex, commitIndex)
	}

	owned := make([]Entry, len(entries))
	next := snapshotIndex
	for i, entry := range entries {
		var ok bool
		next, ok = checkedNextIndex(next)
		if !ok {
			return nil, fmt.Errorf("%w: retained entry after index %d", ErrLogOverflow, snapshotIndex)
		}
		if entry.Index != next {
			return nil, fmt.Errorf("%w: retained entry index=%d want=%d", ErrLogGap, entry.Index, next)
		}
		if err := validateLogEntry(entry); err != nil {
			return nil, err
		}
		owned[i] = entry.Clone()
	}
	if commitIndex > next {
		return nil, fmt.Errorf("%w: commit=%d last=%d", ErrLogInvariant, commitIndex, next)
	}

	return &Log{
		snapshotIndex: snapshotIndex,
		snapshotTerm:  snapshotTerm,
		commitIndex:   commitIndex,
		appliedIndex:  appliedIndex,
		lastIndex:     next,
		entries:       owned,
	}, nil
}

// State returns an independently owned copy of all log state.
func (l *Log) State() LogState {
	return LogState{
		SnapshotIndex: l.snapshotIndex,
		SnapshotTerm:  l.snapshotTerm,
		CommitIndex:   l.commitIndex,
		AppliedIndex:  l.appliedIndex,
		Entries:       cloneLogEntries(l.entries),
	}
}

// SnapshotIndex returns the compacted logical base index.
func (l *Log) SnapshotIndex() uint64 { return l.snapshotIndex }

// SnapshotTerm returns the term at the compacted logical base.
func (l *Log) SnapshotTerm() uint64 { return l.snapshotTerm }

// CommitIndex returns the greatest committed index.
func (l *Log) CommitIndex() uint64 { return l.commitIndex }

// AppliedIndex returns the greatest applied index.
func (l *Log) AppliedIndex() uint64 { return l.appliedIndex }

// LastIndex returns the greatest available snapshot or retained index.
func (l *Log) LastIndex() uint64 { return l.lastIndex }

// LastTerm returns the term at LastIndex.
func (l *Log) LastTerm() uint64 {
	if len(l.entries) == 0 {
		return l.snapshotTerm
	}
	return l.entries[len(l.entries)-1].Term
}

// Term returns the exact term at an available index, including the snapshot base.
func (l *Log) Term(index uint64) (uint64, error) {
	if index < l.snapshotIndex {
		return 0, fmt.Errorf("%w: index=%d base=%d", ErrLogCompacted, index, l.snapshotIndex)
	}
	if index == l.snapshotIndex {
		return l.snapshotTerm, nil
	}
	offset, err := l.retainedOffset(index)
	if err != nil {
		return 0, err
	}
	return l.entries[offset].Term, nil
}

// Entry returns an owned copy of one exact retained entry.
func (l *Log) Entry(index uint64) (Entry, error) {
	if index <= l.snapshotIndex {
		return Entry{}, fmt.Errorf("%w: index=%d base=%d", ErrLogCompacted, index, l.snapshotIndex)
	}
	offset, err := l.retainedOffset(index)
	if err != nil {
		return Entry{}, err
	}
	return l.entries[offset].Clone(), nil
}

// Append proves a previous position before repairing the retained suffix.
func (l *Log) Append(prevIndex, prevTerm uint64, entries []Entry) (ConflictHint, error) {
	localTerm, err := l.Term(prevIndex)
	if err != nil {
		if errors.Is(err, ErrLogUnavailable) {
			next, ok := checkedNextIndex(l.lastIndex)
			if !ok {
				return ConflictHint{}, fmt.Errorf("%w: unavailable index after %d", ErrLogOverflow, l.lastIndex)
			}
			return ConflictHint{Index: next}, err
		}
		if errors.Is(err, ErrLogCompacted) {
			return ConflictHint{Term: l.snapshotTerm, Index: l.snapshotIndex}, err
		}
		return ConflictHint{}, err
	}
	if localTerm != prevTerm {
		first, err := l.firstIndexOfTerm(prevIndex, localTerm)
		if err != nil {
			return ConflictHint{}, err
		}
		return ConflictHint{Term: localTerm, Index: first}, fmt.Errorf(
			"%w: previous index=%d local term=%d peer term=%d",
			ErrLogMismatch,
			prevIndex,
			localTerm,
			prevTerm,
		)
	}
	if len(entries) == 0 {
		return ConflictHint{}, nil
	}

	expected := prevIndex
	for _, entry := range entries {
		previous := expected
		var ok bool
		expected, ok = checkedNextIndex(expected)
		if !ok {
			return ConflictHint{}, fmt.Errorf("%w: entry after index %d", ErrLogOverflow, previous)
		}
		if entry.Index != expected {
			return ConflictHint{}, fmt.Errorf("%w: entry index=%d want=%d", ErrLogGap, entry.Index, expected)
		}
		if err := validateLogEntry(entry); err != nil {
			return ConflictHint{}, err
		}
	}
	owned := cloneLogEntries(entries)

	for i, entry := range owned {
		if entry.Index > l.lastIndex {
			l.entries = append(cloneLogEntries(l.entries), owned[i:]...)
			l.lastIndex = owned[len(owned)-1].Index
			return ConflictHint{}, nil
		}
		offset, err := l.retainedOffset(entry.Index)
		if err != nil {
			return ConflictHint{}, err
		}
		if l.entries[offset].Term == entry.Term {
			continue
		}
		if entry.Index <= l.commitIndex {
			return ConflictHint{}, fmt.Errorf(
				"%w: conflict index=%d commit=%d applied=%d",
				ErrCommittedConflict,
				entry.Index,
				l.commitIndex,
				l.appliedIndex,
			)
		}
		repaired := cloneLogEntries(l.entries[:offset])
		repaired = append(repaired, owned[i:]...)
		l.entries = repaired
		l.lastIndex = owned[len(owned)-1].Index
		return ConflictHint{}, nil
	}
	return ConflictHint{}, nil
}

// AdvanceCommit monotonically moves the commit index through available entries.
func (l *Log) AdvanceCommit(index uint64) error {
	if index < l.commitIndex {
		return fmt.Errorf("%w: commit=%d proposed=%d", ErrLogRegression, l.commitIndex, index)
	}
	if index > l.lastIndex {
		return fmt.Errorf("%w: commit=%d last=%d", ErrLogUnavailable, index, l.lastIndex)
	}
	l.commitIndex = index
	return nil
}

// AdvanceApplied monotonically moves the applied index no farther than commit.
func (l *Log) AdvanceApplied(index uint64) error {
	if index < l.appliedIndex {
		return fmt.Errorf("%w: applied=%d proposed=%d", ErrLogRegression, l.appliedIndex, index)
	}
	if index > l.commitIndex {
		return fmt.Errorf("%w: applied=%d commit=%d", ErrLogInvariant, index, l.commitIndex)
	}
	l.appliedIndex = index
	return nil
}

// Compact establishes an exact applied entry as the newer snapshot base.
func (l *Log) Compact(index, term uint64) error {
	if index < l.snapshotIndex {
		return fmt.Errorf("%w: compact=%d base=%d", ErrLogCompacted, index, l.snapshotIndex)
	}
	if index == l.snapshotIndex {
		if term != l.snapshotTerm {
			return fmt.Errorf("%w: base index=%d local term=%d proposed term=%d", ErrLogMismatch, index, l.snapshotTerm, term)
		}
		return nil
	}
	if index > l.lastIndex {
		return fmt.Errorf("%w: compact=%d last=%d", ErrLogUnavailable, index, l.lastIndex)
	}
	if index > l.appliedIndex {
		return fmt.Errorf("%w: compact=%d applied=%d", ErrLogInvariant, index, l.appliedIndex)
	}
	localTerm, err := l.Term(index)
	if err != nil {
		return err
	}
	if localTerm != term {
		return fmt.Errorf("%w: compact index=%d local term=%d proposed term=%d", ErrLogMismatch, index, localTerm, term)
	}
	offset, err := l.retainedOffset(index)
	if err != nil {
		return err
	}
	l.entries = cloneLogEntries(l.entries[offset+1:])
	l.snapshotIndex = index
	l.snapshotTerm = term
	return nil
}

// RestoreSnapshot installs a newer logical base while preserving protected local state.
func (l *Log) RestoreSnapshot(index, term uint64) error {
	if index < l.snapshotIndex {
		return nil
	}
	if index == l.snapshotIndex {
		if term != l.snapshotTerm {
			return fmt.Errorf("%w: base index=%d local term=%d snapshot term=%d", ErrLogMismatch, index, l.snapshotTerm, term)
		}
		return nil
	}
	if term == 0 {
		return fmt.Errorf("%w: nonzero snapshot index %d has zero term", ErrLogInvariant, index)
	}

	matchingSuffix := false
	retained := []Entry(nil)
	newLast := index
	if index <= l.lastIndex {
		localTerm, err := l.Term(index)
		if err != nil {
			return err
		}
		if localTerm == term {
			offset, err := l.retainedOffset(index)
			if err != nil {
				return err
			}
			matchingSuffix = true
			retained = cloneLogEntries(l.entries[offset+1:])
			newLast = l.lastIndex
		} else if index <= l.commitIndex {
			return fmt.Errorf(
				"%w: snapshot index=%d local term=%d snapshot term=%d commit=%d",
				ErrCommittedConflict,
				index,
				localTerm,
				term,
				l.commitIndex,
			)
		}
	}

	newCommit := maxLogIndex(l.commitIndex, index)
	newApplied := maxLogIndex(l.appliedIndex, index)
	if index > newApplied || newApplied > newCommit || newCommit > newLast {
		classification := ErrLogInvariant
		if !matchingSuffix && l.commitIndex > index {
			classification = ErrCommittedConflict
		}
		return fmt.Errorf(
			"%w: snapshot=%d applied=%d commit=%d last=%d",
			classification,
			index,
			newApplied,
			newCommit,
			newLast,
		)
	}

	l.snapshotIndex = index
	l.snapshotTerm = term
	l.appliedIndex = newApplied
	l.commitIndex = newCommit
	l.lastIndex = newLast
	l.entries = retained
	return nil
}

func (l *Log) retainedOffset(index uint64) (int, error) {
	if index <= l.snapshotIndex {
		return 0, fmt.Errorf("%w: index=%d base=%d", ErrLogCompacted, index, l.snapshotIndex)
	}
	if index > l.lastIndex {
		return 0, fmt.Errorf("%w: index=%d last=%d", ErrLogUnavailable, index, l.lastIndex)
	}
	offset := index - l.snapshotIndex - 1
	if offset >= uint64(len(l.entries)) {
		return 0, fmt.Errorf("%w: index=%d retained=%d", ErrLogInvariant, index, len(l.entries))
	}
	return int(offset), nil
}

func checkedNextIndex(index uint64) (uint64, bool) {
	if index == ^uint64(0) {
		return 0, false
	}
	return index + 1, true
}

func maxLogIndex(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func (l *Log) firstIndexOfTerm(index, term uint64) (uint64, error) {
	if index == l.snapshotIndex {
		return l.snapshotIndex, nil
	}
	offset, err := l.retainedOffset(index)
	if err != nil {
		return 0, err
	}
	for offset > 0 && l.entries[offset-1].Term == term {
		offset--
	}
	if offset == 0 && l.snapshotTerm == term {
		return l.snapshotIndex, nil
	}
	return l.entries[offset].Index, nil
}

func cloneLogEntries(entries []Entry) []Entry {
	if entries == nil {
		return nil
	}
	owned := make([]Entry, len(entries))
	for i := range entries {
		owned[i] = entries[i].Clone()
	}
	return owned
}

func validateLogEntry(entry Entry) error {
	if entry.Index == 0 || entry.Term == 0 {
		return fmt.Errorf("%w: index and term must be nonzero", ErrInvalidEntry)
	}
	if entry.Kind != EntryCommand && entry.Kind != EntryNoOp {
		return fmt.Errorf("%w: unknown kind %d", ErrInvalidEntry, entry.Kind)
	}
	if entry.Kind == EntryNoOp && len(entry.command) != 0 {
		return fmt.Errorf("%w: no-op entry carries command bytes", ErrInvalidEntry)
	}
	return nil
}
