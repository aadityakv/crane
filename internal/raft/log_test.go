package raft

import (
	"errors"
	"math"
	"testing"
)

func TestLogEmptyBaseAndLastTerm(t *testing.T) {
	log, err := NewLog(0, 0, 0, 0, nil)
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	if got := log.SnapshotIndex(); got != 0 {
		t.Fatalf("SnapshotIndex = %d, want 0", got)
	}
	if got := log.SnapshotTerm(); got != 0 {
		t.Fatalf("SnapshotTerm = %d, want 0", got)
	}
	if got := log.LastIndex(); got != 0 {
		t.Fatalf("LastIndex = %d, want 0", got)
	}
	if got := log.LastTerm(); got != 0 {
		t.Fatalf("LastTerm = %d, want 0", got)
	}
	if got, err := log.Term(0); err != nil || got != 0 {
		t.Fatalf("Term(0) = (%d, %v), want (0, nil)", got, err)
	}
	assertLogInvariant(t, log)
}

func TestLogExactLookupOwnsEntries(t *testing.T) {
	retained := []Entry{
		mustLogEntry(t, 6, 2, EntryCommand, []byte("six")),
		mustLogEntry(t, 7, 3, EntryCommand, []byte("seven")),
	}
	log, err := NewLog(5, 2, 7, 6, retained)
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}

	retained[0].command[0] = 'X'
	entry, err := log.Entry(6)
	if err != nil {
		t.Fatalf("Entry(6): %v", err)
	}
	if got := string(entry.CommandBytes()); got != "six" {
		t.Fatalf("Entry(6) command = %q, want %q", got, "six")
	}
	entry.command[0] = 'Y'
	entryAgain, err := log.Entry(6)
	if err != nil {
		t.Fatalf("Entry(6) again: %v", err)
	}
	if got := string(entryAgain.CommandBytes()); got != "six" {
		t.Fatalf("Entry(6) after caller mutation = %q, want %q", got, "six")
	}

	state := log.State()
	state.Entries[1].command[0] = 'Z'
	stateAgain := log.State()
	if got := string(stateAgain.Entries[1].CommandBytes()); got != "seven" {
		t.Fatalf("State entry after caller mutation = %q, want %q", got, "seven")
	}
	if got := log.LastIndex(); got != 7 {
		t.Fatalf("LastIndex = %d, want 7", got)
	}
	if got := log.LastTerm(); got != 3 {
		t.Fatalf("LastTerm = %d, want 3", got)
	}
	if got, err := log.Term(7); err != nil || got != 3 {
		t.Fatalf("Term(7) = (%d, %v), want (3, nil)", got, err)
	}
	assertLogInvariant(t, log)
}

func TestLogCheckedMaximumIndex(t *testing.T) {
	log, err := NewLog(
		math.MaxUint64-1,
		9,
		math.MaxUint64,
		math.MaxUint64-1,
		[]Entry{mustLogEntry(t, math.MaxUint64, 10, EntryNoOp, nil)},
	)
	if err != nil {
		t.Fatalf("NewLog near maximum: %v", err)
	}
	if got, err := log.Term(math.MaxUint64); err != nil || got != 10 {
		t.Fatalf("Term(MaxUint64) = (%d, %v), want (10, nil)", got, err)
	}
	if _, err := log.Entry(math.MaxUint64 - 2); !errors.Is(err, ErrLogCompacted) {
		t.Fatalf("Entry(compacted) error = %v, want ErrLogCompacted", err)
	}
	before := log.State()
	_, err = log.Append(math.MaxUint64, 10, []Entry{
		mustLogEntry(t, 1, 11, EntryNoOp, nil),
	})
	if !errors.Is(err, ErrLogOverflow) {
		t.Fatalf("Append past MaxUint64 error = %v, want ErrLogOverflow", err)
	}
	assertLogStateEqual(t, log.State(), before)
	assertLogInvariant(t, log)
}

func TestLogAppendMatchingEntriesOwnsInput(t *testing.T) {
	log := mustNewLog(t, 0, 0, 0, 0, nil)
	incoming := []Entry{
		mustLogEntry(t, 1, 1, EntryCommand, []byte("one")),
		mustLogEntry(t, 2, 1, EntryCommand, []byte("two")),
	}
	if _, err := log.Append(0, 0, incoming); err != nil {
		t.Fatalf("Append: %v", err)
	}
	incoming[0].command[0] = 'X'
	entry, err := log.Entry(1)
	if err != nil {
		t.Fatalf("Entry(1): %v", err)
	}
	if got := string(entry.CommandBytes()); got != "one" {
		t.Fatalf("appended command after input mutation = %q, want %q", got, "one")
	}
	if got := log.LastIndex(); got != 2 {
		t.Fatalf("LastIndex = %d, want 2", got)
	}
	assertLogInvariant(t, log)
}

func TestLogAppendMatchingRetransmissionIsIdempotent(t *testing.T) {
	entries := []Entry{
		mustLogEntry(t, 1, 1, EntryNoOp, nil),
		mustLogEntry(t, 2, 2, EntryCommand, []byte("two")),
	}
	log := mustNewLog(t, 0, 0, 1, 1, entries)
	before := log.State()
	if _, err := log.Append(0, 0, entries); err != nil {
		t.Fatalf("Append retransmission: %v", err)
	}
	assertLogStateEqual(t, log.State(), before)
	assertLogInvariant(t, log)
}

func TestLogAppendRemovesOnlyDivergentUncommittedSuffix(t *testing.T) {
	log := mustNewLog(t, 0, 0, 2, 2, []Entry{
		mustLogEntry(t, 1, 1, EntryNoOp, nil),
		mustLogEntry(t, 2, 1, EntryCommand, []byte("two")),
		mustLogEntry(t, 3, 2, EntryCommand, []byte("old-three")),
		mustLogEntry(t, 4, 2, EntryCommand, []byte("old-four")),
	})
	incoming := []Entry{
		mustLogEntry(t, 3, 3, EntryCommand, []byte("new-three")),
		mustLogEntry(t, 4, 3, EntryCommand, []byte("new-four")),
		mustLogEntry(t, 5, 3, EntryNoOp, nil),
	}
	if _, err := log.Append(2, 1, incoming); err != nil {
		t.Fatalf("Append divergent suffix: %v", err)
	}
	if got := log.LastIndex(); got != 5 {
		t.Fatalf("LastIndex = %d, want 5", got)
	}
	for _, test := range []struct {
		index uint64
		term  uint64
		text  string
	}{
		{index: 2, term: 1, text: "two"},
		{index: 3, term: 3, text: "new-three"},
		{index: 4, term: 3, text: "new-four"},
	} {
		entry, err := log.Entry(test.index)
		if err != nil {
			t.Fatalf("Entry(%d): %v", test.index, err)
		}
		if entry.Term != test.term || string(entry.CommandBytes()) != test.text {
			t.Fatalf("Entry(%d) = term %d command %q, want term %d command %q", test.index, entry.Term, entry.CommandBytes(), test.term, test.text)
		}
	}
	assertLogInvariant(t, log)
}

func TestLogAppendRejectsGapWithoutMutation(t *testing.T) {
	log := mustNewLog(t, 0, 0, 0, 0, []Entry{
		mustLogEntry(t, 1, 1, EntryNoOp, nil),
	})
	before := log.State()
	_, err := log.Append(1, 1, []Entry{
		mustLogEntry(t, 3, 2, EntryNoOp, nil),
	})
	if !errors.Is(err, ErrLogGap) {
		t.Fatalf("Append gap error = %v, want ErrLogGap", err)
	}
	assertLogStateEqual(t, log.State(), before)
	assertLogInvariant(t, log)
}

func TestLogLookupClassifiesCompactedAndUnavailable(t *testing.T) {
	log := mustNewLog(t, 5, 2, 6, 5, []Entry{
		mustLogEntry(t, 6, 3, EntryNoOp, nil),
	})
	for _, index := range []uint64{0, 4} {
		if _, err := log.Term(index); !errors.Is(err, ErrLogCompacted) {
			t.Fatalf("Term(%d) error = %v, want ErrLogCompacted", index, err)
		}
	}
	if _, err := log.Entry(5); !errors.Is(err, ErrLogCompacted) {
		t.Fatalf("Entry(base) error = %v, want ErrLogCompacted", err)
	}
	if _, err := log.Term(7); !errors.Is(err, ErrLogUnavailable) {
		t.Fatalf("Term(unavailable) error = %v, want ErrLogUnavailable", err)
	}
	if _, err := log.Entry(math.MaxUint64); !errors.Is(err, ErrLogUnavailable) {
		t.Fatalf("Entry(MaxUint64) error = %v, want ErrLogUnavailable", err)
	}
}

func TestLogAppendReturnsFirstIndexOfConflictingTerm(t *testing.T) {
	log := mustNewLog(t, 5, 2, 5, 5, []Entry{
		mustLogEntry(t, 6, 3, EntryNoOp, nil),
		mustLogEntry(t, 7, 3, EntryNoOp, nil),
		mustLogEntry(t, 8, 4, EntryNoOp, nil),
		mustLogEntry(t, 9, 4, EntryNoOp, nil),
	})
	before := log.State()
	hint, err := log.Append(9, 99, nil)
	if !errors.Is(err, ErrLogMismatch) {
		t.Fatalf("Append mismatched previous term error = %v, want ErrLogMismatch", err)
	}
	if hint != (ConflictHint{Term: 4, Index: 8}) {
		t.Fatalf("conflict hint = %+v, want term=4 index=8", hint)
	}
	assertLogStateEqual(t, log.State(), before)
}

func TestLogAppendReturnsNextIndexWhenPreviousEntryUnavailable(t *testing.T) {
	log := mustNewLog(t, 0, 0, 0, 0, []Entry{
		mustLogEntry(t, 1, 1, EntryNoOp, nil),
		mustLogEntry(t, 2, 1, EntryNoOp, nil),
	})
	before := log.State()
	hint, err := log.Append(8, 1, nil)
	if !errors.Is(err, ErrLogUnavailable) {
		t.Fatalf("Append unavailable previous index error = %v, want ErrLogUnavailable", err)
	}
	if hint != (ConflictHint{Index: 3}) {
		t.Fatalf("conflict hint = %+v, want term=0 index=3", hint)
	}
	assertLogStateEqual(t, log.State(), before)
}

func TestLogAppendRejectsAppliedConflict(t *testing.T) {
	log := mustNewLog(t, 0, 0, 2, 2, []Entry{
		mustLogEntry(t, 1, 1, EntryNoOp, nil),
		mustLogEntry(t, 2, 1, EntryCommand, []byte("committed")),
	})
	before := log.State()
	_, err := log.Append(0, 0, []Entry{
		mustLogEntry(t, 1, 2, EntryNoOp, nil),
	})
	if !errors.Is(err, ErrCommittedConflict) {
		t.Fatalf("Append applied conflict error = %v, want ErrCommittedConflict", err)
	}
	assertLogStateEqual(t, log.State(), before)
	assertLogInvariant(t, log)
}

func TestLogAppendRejectsCommittedNotAppliedConflict(t *testing.T) {
	log := mustNewLog(t, 0, 0, 3, 1, []Entry{
		mustLogEntry(t, 1, 1, EntryNoOp, nil),
		mustLogEntry(t, 2, 1, EntryCommand, []byte("committed-pending")),
		mustLogEntry(t, 3, 2, EntryNoOp, nil),
	})
	before := log.State()
	_, err := log.Append(1, 1, []Entry{
		mustLogEntry(t, 2, 3, EntryCommand, []byte("replacement")),
	})
	if !errors.Is(err, ErrCommittedConflict) {
		t.Fatalf("Append committed-not-applied conflict error = %v, want ErrCommittedConflict", err)
	}
	assertLogStateEqual(t, log.State(), before)
	assertLogInvariant(t, log)
}

func TestLogAdvanceCommitBoundaries(t *testing.T) {
	log := mustNewLog(t, 0, 0, 1, 0, []Entry{
		mustLogEntry(t, 1, 1, EntryNoOp, nil),
		mustLogEntry(t, 2, 1, EntryNoOp, nil),
		mustLogEntry(t, 3, 2, EntryNoOp, nil),
	})

	if err := log.AdvanceCommit(1); err != nil {
		t.Fatalf("AdvanceCommit(equal): %v", err)
	}
	assertLogInvariant(t, log)
	if err := log.AdvanceCommit(3); err != nil {
		t.Fatalf("AdvanceCommit(last): %v", err)
	}
	if got := log.CommitIndex(); got != 3 {
		t.Fatalf("CommitIndex = %d, want 3", got)
	}
	assertLogInvariant(t, log)

	for _, test := range []struct {
		name  string
		index uint64
		want  error
	}{
		{name: "regression", index: 2, want: ErrLogRegression},
		{name: "unavailable", index: 4, want: ErrLogUnavailable},
		{name: "hostile maximum", index: math.MaxUint64, want: ErrLogUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := log.State()
			if err := log.AdvanceCommit(test.index); !errors.Is(err, test.want) {
				t.Fatalf("AdvanceCommit(%d) error = %v, want %v", test.index, err, test.want)
			}
			assertLogStateEqual(t, log.State(), before)
			assertLogInvariant(t, log)
		})
	}
}

func TestLogAdvanceAppliedBoundaries(t *testing.T) {
	log := mustNewLog(t, 0, 0, 3, 1, []Entry{
		mustLogEntry(t, 1, 1, EntryNoOp, nil),
		mustLogEntry(t, 2, 1, EntryNoOp, nil),
		mustLogEntry(t, 3, 2, EntryNoOp, nil),
	})

	if err := log.AdvanceApplied(1); err != nil {
		t.Fatalf("AdvanceApplied(equal): %v", err)
	}
	if err := log.AdvanceApplied(3); err != nil {
		t.Fatalf("AdvanceApplied(commit): %v", err)
	}
	if got := log.AppliedIndex(); got != 3 {
		t.Fatalf("AppliedIndex = %d, want 3", got)
	}
	assertLogInvariant(t, log)

	for _, test := range []struct {
		name  string
		index uint64
		want  error
	}{
		{name: "regression", index: 2, want: ErrLogRegression},
		{name: "past commit", index: 4, want: ErrLogInvariant},
		{name: "hostile maximum", index: math.MaxUint64, want: ErrLogInvariant},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := log.State()
			if err := log.AdvanceApplied(test.index); !errors.Is(err, test.want) {
				t.Fatalf("AdvanceApplied(%d) error = %v, want %v", test.index, err, test.want)
			}
			assertLogStateEqual(t, log.State(), before)
			assertLogInvariant(t, log)
		})
	}
}

func TestLogCompactEstablishesOwnedAppliedBase(t *testing.T) {
	log := mustNewLog(t, 0, 0, 4, 3, []Entry{
		mustLogEntry(t, 1, 1, EntryCommand, []byte("one")),
		mustLogEntry(t, 2, 1, EntryCommand, []byte("two")),
		mustLogEntry(t, 3, 2, EntryCommand, []byte("three")),
		mustLogEntry(t, 4, 2, EntryCommand, []byte("four")),
	})
	oldRetainedByte := &log.entries[3].command[0]
	if err := log.Compact(3, 2); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if log.SnapshotIndex() != 3 || log.SnapshotTerm() != 2 {
		t.Fatalf("snapshot base = (%d, %d), want (3, 2)", log.SnapshotIndex(), log.SnapshotTerm())
	}
	if len(log.entries) != 1 || log.entries[0].Index != 4 {
		t.Fatalf("retained entries = %+v, want only index 4", log.entries)
	}
	if &log.entries[0].command[0] == oldRetainedByte {
		t.Fatal("compaction retained the old backing storage")
	}
	assertLogInvariant(t, log)
}

func TestLogCompactRejectsInvalidBaseWithoutMutation(t *testing.T) {
	log := mustNewLog(t, 2, 1, 4, 3, []Entry{
		mustLogEntry(t, 3, 2, EntryNoOp, nil),
		mustLogEntry(t, 4, 2, EntryNoOp, nil),
	})
	for _, test := range []struct {
		name  string
		index uint64
		term  uint64
		want  error
	}{
		{name: "stale", index: 1, term: 1, want: ErrLogCompacted},
		{name: "base term mismatch", index: 2, term: 9, want: ErrLogMismatch},
		{name: "retained term mismatch", index: 3, term: 9, want: ErrLogMismatch},
		{name: "past applied", index: 4, term: 2, want: ErrLogInvariant},
		{name: "unavailable", index: 5, term: 2, want: ErrLogUnavailable},
		{name: "hostile maximum", index: math.MaxUint64, term: 2, want: ErrLogUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := log.State()
			if err := log.Compact(test.index, test.term); !errors.Is(err, test.want) {
				t.Fatalf("Compact(%d, %d) error = %v, want %v", test.index, test.term, err, test.want)
			}
			assertLogStateEqual(t, log.State(), before)
			assertLogInvariant(t, log)
		})
	}
	before := log.State()
	if err := log.Compact(2, 1); err != nil {
		t.Fatalf("Compact(equal base): %v", err)
	}
	assertLogStateEqual(t, log.State(), before)
}

func TestLogNewLogRejectsInvalidRecoveredState(t *testing.T) {
	valid := []Entry{mustLogEntry(t, 6, 3, EntryNoOp, nil)}
	for _, test := range []struct {
		name     string
		base     uint64
		baseTerm uint64
		commit   uint64
		applied  uint64
		entries  []Entry
		want     error
	}{
		{name: "zero index nonzero term", base: 0, baseTerm: 1, want: ErrLogInvariant},
		{name: "nonzero index zero term", base: 5, baseTerm: 0, commit: 5, applied: 5, want: ErrLogInvariant},
		{name: "applied below snapshot", base: 5, baseTerm: 2, commit: 5, applied: 4, entries: valid, want: ErrLogInvariant},
		{name: "commit below applied", base: 5, baseTerm: 2, commit: 5, applied: 6, entries: valid, want: ErrLogInvariant},
		{name: "commit unavailable", base: 5, baseTerm: 2, commit: 7, applied: 5, entries: valid, want: ErrLogInvariant},
		{name: "retained gap", base: 5, baseTerm: 2, commit: 5, applied: 5, entries: []Entry{mustLogEntry(t, 7, 3, EntryNoOp, nil)}, want: ErrLogGap},
		{name: "retained overflow", base: math.MaxUint64, baseTerm: 2, commit: math.MaxUint64, applied: math.MaxUint64, entries: []Entry{mustLogEntry(t, 1, 3, EntryNoOp, nil)}, want: ErrLogOverflow},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewLog(test.base, test.baseTerm, test.commit, test.applied, test.entries); !errors.Is(err, test.want) {
				t.Fatalf("NewLog error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestLogRestoreSnapshotStaleAndEqualDoNotDecreaseState(t *testing.T) {
	log := mustNewLog(t, 5, 2, 7, 6, []Entry{
		mustLogEntry(t, 6, 3, EntryNoOp, nil),
		mustLogEntry(t, 7, 3, EntryNoOp, nil),
		mustLogEntry(t, 8, 4, EntryNoOp, nil),
	})
	before := log.State()
	if err := log.RestoreSnapshot(4, 99); err != nil {
		t.Fatalf("RestoreSnapshot(stale): %v", err)
	}
	assertLogStateEqual(t, log.State(), before)
	if err := log.RestoreSnapshot(5, 2); err != nil {
		t.Fatalf("RestoreSnapshot(equal): %v", err)
	}
	assertLogStateEqual(t, log.State(), before)
	assertLogInvariant(t, log)
}

func TestLogRestoreSnapshotRejectsEqualTermMismatch(t *testing.T) {
	log := mustNewLog(t, 5, 2, 7, 6, []Entry{
		mustLogEntry(t, 6, 3, EntryNoOp, nil),
		mustLogEntry(t, 7, 3, EntryNoOp, nil),
	})
	before := log.State()
	if err := log.RestoreSnapshot(5, 9); !errors.Is(err, ErrLogMismatch) {
		t.Fatalf("RestoreSnapshot(equal mismatched term) error = %v, want ErrLogMismatch", err)
	}
	assertLogStateEqual(t, log.State(), before)
	assertLogInvariant(t, log)
}

func TestLogRestoreSnapshotRetainsSuffixOnExactMatch(t *testing.T) {
	log := mustNewLog(t, 2, 1, 5, 3, []Entry{
		mustLogEntry(t, 3, 2, EntryCommand, []byte("three")),
		mustLogEntry(t, 4, 2, EntryCommand, []byte("four")),
		mustLogEntry(t, 5, 3, EntryCommand, []byte("five")),
		mustLogEntry(t, 6, 3, EntryCommand, []byte("six")),
	})
	oldSuffixByte := &log.entries[2].command[0]
	if err := log.RestoreSnapshot(4, 2); err != nil {
		t.Fatalf("RestoreSnapshot(matching): %v", err)
	}
	state := log.State()
	if state.SnapshotIndex != 4 || state.SnapshotTerm != 2 {
		t.Fatalf("snapshot base = (%d, %d), want (4, 2)", state.SnapshotIndex, state.SnapshotTerm)
	}
	if state.AppliedIndex != 4 || state.CommitIndex != 5 {
		t.Fatalf("protected indices = applied %d commit %d, want applied 4 commit 5", state.AppliedIndex, state.CommitIndex)
	}
	if len(state.Entries) != 2 || state.Entries[0].Index != 5 || state.Entries[1].Index != 6 {
		t.Fatalf("retained suffix = %+v, want indices 5 and 6", state.Entries)
	}
	if &log.entries[0].command[0] == oldSuffixByte {
		t.Fatal("snapshot restore retained the old backing storage")
	}
	assertLogInvariant(t, log)
}

func TestLogRestoreSnapshotDropsUnmatchedUncommittedSuffix(t *testing.T) {
	log := mustNewLog(t, 2, 1, 4, 3, []Entry{
		mustLogEntry(t, 3, 2, EntryNoOp, nil),
		mustLogEntry(t, 4, 2, EntryNoOp, nil),
		mustLogEntry(t, 5, 3, EntryNoOp, nil),
		mustLogEntry(t, 6, 3, EntryNoOp, nil),
	})
	if err := log.RestoreSnapshot(5, 9); err != nil {
		t.Fatalf("RestoreSnapshot(unmatched newer): %v", err)
	}
	state := log.State()
	if state.SnapshotIndex != 5 || state.SnapshotTerm != 9 {
		t.Fatalf("snapshot base = (%d, %d), want (5, 9)", state.SnapshotIndex, state.SnapshotTerm)
	}
	if state.AppliedIndex != 5 || state.CommitIndex != 5 || log.LastIndex() != 5 {
		t.Fatalf("indices = applied %d commit %d last %d, want all 5", state.AppliedIndex, state.CommitIndex, log.LastIndex())
	}
	if len(state.Entries) != 0 {
		t.Fatalf("retained entries = %+v, want none", state.Entries)
	}
	assertLogInvariant(t, log)
}

func TestLogRestoreSnapshotRejectsCommittedConflictWithoutMutation(t *testing.T) {
	log := mustNewLog(t, 2, 1, 6, 3, []Entry{
		mustLogEntry(t, 3, 2, EntryNoOp, nil),
		mustLogEntry(t, 4, 2, EntryNoOp, nil),
		mustLogEntry(t, 5, 3, EntryNoOp, nil),
		mustLogEntry(t, 6, 3, EntryNoOp, nil),
	})
	before := log.State()
	if err := log.RestoreSnapshot(5, 9); !errors.Is(err, ErrCommittedConflict) {
		t.Fatalf("RestoreSnapshot(committed conflict) error = %v, want ErrCommittedConflict", err)
	}
	assertLogStateEqual(t, log.State(), before)
	assertLogInvariant(t, log)
}

func TestLogRestoreSnapshotBeyondLastIndexAdvancesProtectedBase(t *testing.T) {
	log := mustNewLog(t, 0, 0, 2, 1, []Entry{
		mustLogEntry(t, 1, 1, EntryNoOp, nil),
		mustLogEntry(t, 2, 1, EntryNoOp, nil),
		mustLogEntry(t, 3, 2, EntryNoOp, nil),
	})
	if err := log.RestoreSnapshot(10, 7); err != nil {
		t.Fatalf("RestoreSnapshot(beyond last): %v", err)
	}
	state := log.State()
	if state.SnapshotIndex != 10 || state.AppliedIndex != 10 || state.CommitIndex != 10 || log.LastIndex() != 10 {
		t.Fatalf("indices = snapshot %d applied %d commit %d last %d, want all 10", state.SnapshotIndex, state.AppliedIndex, state.CommitIndex, log.LastIndex())
	}
	assertLogInvariant(t, log)
}

func TestLogRestoreSnapshotAtMaximumIndexDoesNotOverflow(t *testing.T) {
	log := mustNewLog(t, math.MaxUint64-1, 8, math.MaxUint64-1, math.MaxUint64-1, nil)
	if err := log.RestoreSnapshot(math.MaxUint64, 9); err != nil {
		t.Fatalf("RestoreSnapshot(MaxUint64): %v", err)
	}
	state := log.State()
	if state.SnapshotIndex != math.MaxUint64 || state.AppliedIndex != math.MaxUint64 || state.CommitIndex != math.MaxUint64 || log.LastIndex() != math.MaxUint64 {
		t.Fatalf("maximum indices = snapshot %d applied %d commit %d last %d", state.SnapshotIndex, state.AppliedIndex, state.CommitIndex, log.LastIndex())
	}
	assertLogInvariant(t, log)
	before := log.State()
	if err := log.RestoreSnapshot(math.MaxUint64, 10); !errors.Is(err, ErrLogMismatch) {
		t.Fatalf("RestoreSnapshot(maximum equal mismatch) error = %v, want ErrLogMismatch", err)
	}
	assertLogStateEqual(t, log.State(), before)
}

func TestLogRestoreSnapshotRejectsInvalidNewBaseWithoutMutation(t *testing.T) {
	log := mustNewLog(t, 0, 0, 0, 0, nil)
	before := log.State()
	if err := log.RestoreSnapshot(1, 0); !errors.Is(err, ErrLogInvariant) {
		t.Fatalf("RestoreSnapshot(nonzero index, zero term) error = %v, want ErrLogInvariant", err)
	}
	assertLogStateEqual(t, log.State(), before)
}

func TestLogDeterministicModelSequence(t *testing.T) {
	runLogModelSequence(t, []byte{
		0, 0, 0, 1, 1, 0, 3, 0, 4, 0, 2, 1, 5, 0, 6, 1,
		0, 2, 3, 2, 4, 2, 6, 3, 1, 1, 2, 0, 5, 3, 6, 4,
	})
}

func FuzzLogModelSequence(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 0, 3, 1, 4, 1, 5, 1, 6, 1})
	f.Add([]byte{0, 0, 0, 1, 2, 0, 3, 0, 4, 0, 5, 0, 6, 0})
	f.Add([]byte{0xff, 0xff, 6, 0xff, 3, 0xff, 4, 0xff, 5, 0xff})
	f.Fuzz(func(t *testing.T, operations []byte) {
		runLogModelSequence(t, operations)
	})
}

func runLogModelSequence(t *testing.T, operations []byte) {
	t.Helper()
	log := mustNewLog(t, 0, 0, 0, 0, nil)
	steps := 0
	for cursor := 0; cursor+1 < len(operations) && steps < 64; cursor, steps = cursor+2, steps+1 {
		op := operations[cursor] % 7
		argument := operations[cursor+1]
		before := log.State()
		beforeLast := log.LastIndex()
		var err error
		switch op {
		case 0:
			err = modelAppend(log, argument, steps)
		case 1:
			err = modelRetransmit(log, argument)
		case 2:
			err = modelConflict(log, argument, steps)
		case 3:
			err = log.AdvanceCommit(modelProtectedCandidate(log.CommitIndex(), log.LastIndex(), argument))
		case 4:
			err = log.AdvanceApplied(modelProtectedCandidate(log.AppliedIndex(), log.CommitIndex(), argument))
		case 5:
			err = modelCompact(log, argument)
		case 6:
			err = modelRestore(log, argument)
		}

		if err != nil {
			assertLogStateEqual(t, log.State(), before)
			if log.LastIndex() != beforeLast {
				t.Fatalf("rejected operation %d changed last index from %d to %d", op, beforeLast, log.LastIndex())
			}
			continue
		}
		after := log.State()
		if after.SnapshotIndex < before.SnapshotIndex ||
			after.AppliedIndex < before.AppliedIndex ||
			after.CommitIndex < before.CommitIndex {
			t.Fatalf("operation %d decreased protected indices: before=%+v after=%+v", op, before, after)
		}
		assertLogInvariant(t, log)
		assertLogContiguous(t, log)
		assertLogStateOwnership(t, log)
	}
}

func modelAppend(log *Log, argument byte, step int) error {
	prevIndex := log.LastIndex()
	switch argument % 4 {
	case 1:
		prevIndex = log.SnapshotIndex()
	case 2:
		if log.LastIndex() < math.MaxUint64-1 {
			prevIndex = log.LastIndex() + 2
		}
	case 3:
		prevIndex = math.MaxUint64
	}
	prevTerm, termErr := log.Term(prevIndex)
	if termErr != nil {
		prevTerm = uint64(argument%7) + 1
	}
	index := uint64(0)
	if prevIndex != math.MaxUint64 {
		index = prevIndex + 1
	}
	incoming := []Entry{{
		Index:   index,
		Term:    uint64(argument%7) + 1,
		Kind:    EntryCommand,
		command: []byte{byte(step), argument},
	}}
	_, err := log.Append(prevIndex, prevTerm, incoming)
	incoming[0].command[0] ^= 0xff
	return err
}

func modelRetransmit(log *Log, argument byte) error {
	state := log.State()
	if len(state.Entries) == 0 {
		return modelAppend(log, 0, int(argument))
	}
	start := int(argument) % len(state.Entries)
	prevIndex := state.SnapshotIndex
	prevTerm := state.SnapshotTerm
	if start > 0 {
		prevIndex = state.Entries[start-1].Index
		prevTerm = state.Entries[start-1].Term
	}
	incoming := state.Entries[start:]
	_, err := log.Append(prevIndex, prevTerm, incoming)
	for i := range incoming {
		if len(incoming[i].command) != 0 {
			incoming[i].command[0] ^= 0xff
		}
	}
	return err
}

func modelConflict(log *Log, argument byte, step int) error {
	state := log.State()
	if len(state.Entries) == 0 {
		return modelAppend(log, argument, step)
	}
	position := int(argument) % len(state.Entries)
	local := state.Entries[position]
	prevIndex := state.SnapshotIndex
	prevTerm := state.SnapshotTerm
	if position > 0 {
		prevIndex = state.Entries[position-1].Index
		prevTerm = state.Entries[position-1].Term
	}
	term := local.Term + 1
	if term == 0 {
		term = 1
	}
	incoming := []Entry{{
		Index:   local.Index,
		Term:    term,
		Kind:    EntryCommand,
		command: []byte{argument, byte(step)},
	}}
	_, err := log.Append(prevIndex, prevTerm, incoming)
	incoming[0].command[0] ^= 0xff
	return err
}

func modelProtectedCandidate(current, limit uint64, argument byte) uint64 {
	switch argument % 5 {
	case 0:
		return current
	case 1:
		return limit
	case 2:
		if current > 0 {
			return current - 1
		}
	case 3:
		if limit < math.MaxUint64 {
			return limit + 1
		}
	case 4:
		return math.MaxUint64
	}
	return current
}

func modelCompact(log *Log, argument byte) error {
	index := log.SnapshotIndex()
	switch argument % 5 {
	case 1:
		index = log.AppliedIndex()
	case 2:
		index = log.LastIndex()
	case 3:
		if index > 0 {
			index--
		}
	case 4:
		index = math.MaxUint64
	}
	term, err := log.Term(index)
	if err != nil {
		term = uint64(argument%7) + 1
	} else if argument&0x20 != 0 {
		term++
		if term == 0 {
			term = 1
		}
	}
	return log.Compact(index, term)
}

func modelRestore(log *Log, argument byte) error {
	index := log.SnapshotIndex()
	term := log.SnapshotTerm()
	switch argument % 6 {
	case 0:
		if index > 0 {
			index--
		}
		term = uint64(argument%7) + 1
	case 1:
	case 2:
		index = log.LastIndex()
		if localTerm, err := log.Term(index); err == nil {
			term = localTerm
		}
	case 3:
		if log.LastIndex() < math.MaxUint64 {
			index = log.LastIndex() + 1
			term = uint64(argument%7) + 1
		}
	case 4:
		index = math.MaxUint64
		term = uint64(argument%7) + 1
	case 5:
		index = log.CommitIndex()
		if localTerm, err := log.Term(index); err == nil {
			term = localTerm + 1
			if term == 0 {
				term = 1
			}
		}
	}
	return log.RestoreSnapshot(index, term)
}

func assertLogContiguous(t *testing.T, log *Log) {
	t.Helper()
	state := log.State()
	expected := state.SnapshotIndex
	for i, entry := range state.Entries {
		if expected == math.MaxUint64 {
			t.Fatalf("entry[%d] follows MaxUint64 snapshot/index", i)
		}
		expected++
		if entry.Index != expected {
			t.Fatalf("entry[%d].Index = %d, want %d", i, entry.Index, expected)
		}
	}
	if expected != log.LastIndex() {
		t.Fatalf("contiguous end = %d, LastIndex = %d", expected, log.LastIndex())
	}
}

func assertLogStateOwnership(t *testing.T, log *Log) {
	t.Helper()
	state := log.State()
	for i := range state.Entries {
		if len(state.Entries[i].command) == 0 {
			continue
		}
		before := state.Entries[i].command[0]
		state.Entries[i].command[0] ^= 0xff
		fresh := log.State()
		if fresh.Entries[i].command[0] != before {
			t.Fatalf("State entry %d aliases the owned log", i)
		}
		return
	}
}

func mustNewLog(t *testing.T, snapshotIndex, snapshotTerm, commitIndex, appliedIndex uint64, entries []Entry) *Log {
	t.Helper()
	log, err := NewLog(snapshotIndex, snapshotTerm, commitIndex, appliedIndex, entries)
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	return log
}

func mustLogEntry(t *testing.T, index, term uint64, kind EntryKind, command []byte) Entry {
	t.Helper()
	entry, err := NewEntry(index, term, kind, command)
	if err != nil {
		t.Fatalf("NewEntry(%d, %d): %v", index, term, err)
	}
	return entry
}

func assertLogInvariant(t *testing.T, log *Log) {
	t.Helper()
	state := log.State()
	if state.SnapshotIndex > state.AppliedIndex ||
		state.AppliedIndex > state.CommitIndex ||
		state.CommitIndex > log.LastIndex() {
		t.Fatalf(
			"log invariant violated: snapshot=%d applied=%d commit=%d last=%d",
			state.SnapshotIndex,
			state.AppliedIndex,
			state.CommitIndex,
			log.LastIndex(),
		)
	}
	expected := state.SnapshotIndex
	for i, entry := range state.Entries {
		if expected == math.MaxUint64 {
			t.Fatalf("entry[%d] follows MaxUint64 snapshot/index", i)
		}
		expected++
		want := expected
		if entry.Index != want {
			t.Fatalf("entry[%d].Index = %d, want %d", i, entry.Index, want)
		}
	}
}

func assertLogStateEqual(t *testing.T, got, want LogState) {
	t.Helper()
	if got.SnapshotIndex != want.SnapshotIndex ||
		got.SnapshotTerm != want.SnapshotTerm ||
		got.CommitIndex != want.CommitIndex ||
		got.AppliedIndex != want.AppliedIndex ||
		len(got.Entries) != len(want.Entries) {
		t.Fatalf("log state = %+v, want %+v", got, want)
	}
	for i := range got.Entries {
		if got.Entries[i].Index != want.Entries[i].Index ||
			got.Entries[i].Term != want.Entries[i].Term ||
			got.Entries[i].Kind != want.Entries[i].Kind ||
			string(got.Entries[i].CommandBytes()) != string(want.Entries[i].CommandBytes()) {
			t.Fatalf("log state entry[%d] = %+v, want %+v", i, got.Entries[i], want.Entries[i])
		}
	}
}
