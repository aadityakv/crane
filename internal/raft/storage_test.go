package raft

import (
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestStorageRecoveredStateAndBatchOwnEntryBytes(t *testing.T) {
	identity, voters := testStorageIdentity(t, 1)
	command := []byte("one")
	entry, err := NewEntry(1, 1, EntryCommand, command)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewMemoryStore(identity, voters)
	if err != nil {
		t.Fatalf("NewMemoryStore: %v", err)
	}
	hardState := HardState{Term: 1, CommitIndex: 1}
	batch := PersistenceBatch{HardState: &hardState, ReplaceFrom: 1, Entries: []Entry{entry}}
	if err := store.Persist(batch); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	command[0] = 'X'
	entry.command[0] = 'Y'
	batch.Entries[0].command[0] = 'Z'
	first, err := store.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	first.Entries[0].command[0] = 'Q'
	first.HardState.Term = 99
	second, err := store.Recover()
	if err != nil {
		t.Fatalf("Recover again: %v", err)
	}
	if got := string(second.Entries[0].CommandBytes()); got != "one" {
		t.Fatalf("recovered command = %q, want one", got)
	}
	if second.HardState.Term != 1 {
		t.Fatalf("recovered term = %d, want 1", second.HardState.Term)
	}
}

func TestMemoryStoreSuffixReplacementRemovesOldTailExactly(t *testing.T) {
	identity, voters := testStorageIdentity(t, 1)
	store, err := NewMemoryStore(identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	initial := PersistenceBatch{
		HardState:   hardStatePointer(HardState{Term: 2, CommitIndex: 1}),
		ReplaceFrom: 1,
		Entries: []Entry{
			mustStorageEntry(t, 1, 1, "one"),
			mustStorageEntry(t, 2, 2, "old-two"),
			mustStorageEntry(t, 3, 2, "old-three"),
		},
	}
	if err := store.Persist(initial); err != nil {
		t.Fatal(err)
	}
	replacement := PersistenceBatch{
		HardState:   hardStatePointer(HardState{Term: 3, CommitIndex: 1}),
		ReplaceFrom: 2,
		Entries: []Entry{
			mustStorageEntry(t, 2, 3, "new-two"),
		},
	}
	if err := store.Persist(replacement); err != nil {
		t.Fatalf("Persist replacement: %v", err)
	}
	state, err := store.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Entries) != 2 || state.Entries[1].Index != 2 || string(state.Entries[1].CommandBytes()) != "new-two" {
		t.Fatalf("replacement retained wrong suffix: %+v", state.Entries)
	}
}

func TestMemoryStoreRejectsInvalidProspectiveStateWithoutMutation(t *testing.T) {
	identity, voters := testStorageIdentity(t, 1)
	store, err := NewMemoryStore(identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(PersistenceBatch{
		HardState:   hardStatePointer(HardState{Term: 2, VotedFor: 1, CommitIndex: 1}),
		ReplaceFrom: 1,
		Entries:     []Entry{mustStorageEntry(t, 1, 2, "one")},
	}); err != nil {
		t.Fatal(err)
	}
	before, _ := store.Recover()

	tests := []struct {
		name  string
		batch PersistenceBatch
	}{
		{name: "term regression", batch: PersistenceBatch{HardState: hardStatePointer(HardState{Term: 1, VotedFor: 1, CommitIndex: 1})}},
		{name: "commit regression", batch: PersistenceBatch{HardState: hardStatePointer(HardState{Term: 2, VotedFor: 1})}},
		{name: "vote change same term", batch: PersistenceBatch{HardState: hardStatePointer(HardState{Term: 2, VotedFor: 2, CommitIndex: 1})}},
		{name: "non voter", batch: PersistenceBatch{HardState: hardStatePointer(HardState{Term: 3, VotedFor: 9, CommitIndex: 1})}},
		{name: "overwrite committed", batch: PersistenceBatch{ReplaceFrom: 1, Entries: []Entry{mustStorageEntry(t, 1, 3, "changed")}}},
		{name: "ambiguous append", batch: PersistenceBatch{Entries: []Entry{mustStorageEntry(t, 2, 2, "two")}}},
		{name: "gap", batch: PersistenceBatch{ReplaceFrom: 3, Entries: []Entry{mustStorageEntry(t, 3, 2, "three")}}},
		{name: "entry above hard term", batch: PersistenceBatch{HardState: hardStatePointer(HardState{Term: 3, CommitIndex: 1}), ReplaceFrom: 2, Entries: []Entry{mustStorageEntry(t, 2, 4, "two")}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := store.Persist(test.batch); !errors.Is(err, ErrInvalidStorageState) {
				t.Fatalf("Persist error = %v, want ErrInvalidStorageState", err)
			}
			after, recoverErr := store.Recover()
			if recoverErr != nil {
				t.Fatal(recoverErr)
			}
			assertRecoveredStateEqual(t, after, before)
		})
	}
}

func TestStorageValidationCoversRecoveryBoundsAndTermOrdering(t *testing.T) {
	identity, voters := testStorageIdentity(t, 1)
	valid := RecoveredState{
		Identity:     identity,
		HardState:    HardState{Term: 4, VotedFor: 2, CommitIndex: 3},
		SnapshotBase: SnapshotMetadata{LastIncludedIndex: 1, LastIncludedTerm: 2},
		AppliedIndex: 2,
		Entries: []Entry{
			mustStorageEntry(t, 2, 3, "two"),
			mustStorageEntry(t, 3, 4, "three"),
		},
	}
	if err := ValidateRecoveredState(valid, identity, voters); err != nil {
		t.Fatalf("valid state rejected: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*RecoveredState)
	}{
		{name: "format", mutate: func(s *RecoveredState) { s.Identity.FormatVersion++ }},
		{name: "cluster", mutate: func(s *RecoveredState) { s.Identity.ClusterID[0]++ }},
		{name: "local", mutate: func(s *RecoveredState) { s.Identity.LocalVoterID++ }},
		{name: "fingerprint", mutate: func(s *RecoveredState) { s.Identity.VoterFingerprint[0]++ }},
		{name: "vote not member", mutate: func(s *RecoveredState) { s.HardState.VotedFor = 9 }},
		{name: "vote without term", mutate: func(s *RecoveredState) { s.HardState.Term = 0 }},
		{name: "snapshot pair", mutate: func(s *RecoveredState) { s.SnapshotBase.LastIncludedTerm = 0 }},
		{name: "applied below snapshot", mutate: func(s *RecoveredState) { s.AppliedIndex = 0 }},
		{name: "applied above commit", mutate: func(s *RecoveredState) { s.AppliedIndex = 4 }},
		{name: "commit above last", mutate: func(s *RecoveredState) { s.HardState.CommitIndex = 4 }},
		{name: "entry gap", mutate: func(s *RecoveredState) { s.Entries[0].Index = 3 }},
		{name: "term decreases", mutate: func(s *RecoveredState) { s.Entries[1].Term = 2 }},
		{name: "term above hard", mutate: func(s *RecoveredState) { s.Entries[1].Term = 5 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := valid.Clone()
			test.mutate(&candidate)
			if err := ValidateRecoveredState(candidate, identity, voters); !errors.Is(err, ErrInvalidStorageState) {
				t.Fatalf("ValidateRecoveredState error = %v, want ErrInvalidStorageState", err)
			}
		})
	}
}

func TestMemoryStoreInjectedPersistFaultIsAtomicAndConsumed(t *testing.T) {
	identity, voters := testStorageIdentity(t, 1)
	store, err := NewMemoryStore(identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected persist failure")
	store.FailNext(StorageOperationPersist, injected)
	batch := PersistenceBatch{HardState: hardStatePointer(HardState{Term: 1})}
	if err := store.Persist(batch); !errors.Is(err, injected) {
		t.Fatalf("Persist error = %v, want injected failure", err)
	}
	state, _ := store.Recover()
	if state.HardState != (HardState{}) {
		t.Fatalf("failed Persist mutated state: %+v", state.HardState)
	}
	if err := store.Persist(batch); err != nil {
		t.Fatalf("Persist retry: %v", err)
	}
}

func TestMemoryStoreCanAcquireOneVoteInCurrentTerm(t *testing.T) {
	identity, voters := testStorageIdentity(t, 1)
	store, err := NewMemoryStore(identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(PersistenceBatch{HardState: hardStatePointer(HardState{Term: 4})}); err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(PersistenceBatch{HardState: hardStatePointer(HardState{Term: 4, VotedFor: 2})}); err != nil {
		t.Fatalf("acquire vote in current term: %v", err)
	}
	state, err := store.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if state.HardState.VotedFor != 2 {
		t.Fatalf("VotedFor = %d, want 2", state.HardState.VotedFor)
	}
}

func TestMemoryStoreCloseExactlyOnce(t *testing.T) {
	identity, voters := testStorageIdentity(t, 1)
	store, err := NewMemoryStore(identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := store.Close(); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("second Close error = %v, want ErrStoreClosed", err)
	}
	if _, err := store.Recover(); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("Recover after close error = %v, want ErrStoreClosed", err)
	}
}

func TestStorageSuffixReplacementAtMaximumIndexUsesCheckedArithmetic(t *testing.T) {
	identity, voters := testStorageIdentity(t, 1)
	current := RecoveredState{
		Identity:     identity,
		HardState:    HardState{Term: 2, CommitIndex: math.MaxUint64 - 1},
		SnapshotBase: SnapshotMetadata{LastIncludedIndex: math.MaxUint64 - 1, LastIncludedTerm: 1},
		AppliedIndex: math.MaxUint64 - 1,
		Entries:      []Entry{mustStorageEntry(t, math.MaxUint64, 2, "old")},
	}
	got, err := applyPersistenceBatch(current, PersistenceBatch{
		ReplaceFrom: math.MaxUint64,
		Entries:     []Entry{mustStorageEntry(t, math.MaxUint64, 2, "new")},
	}, identity, voters)
	if err != nil {
		t.Fatalf("replace maximum index: %v", err)
	}
	if len(got.Entries) != 1 || string(got.Entries[0].CommandBytes()) != "new" {
		t.Fatalf("maximum replacement = %+v, want new entry", got.Entries)
	}
}

func testStorageIdentity(t *testing.T, localID uint16) (StorageIdentity, VoterSet) {
	t.Helper()
	voters := mustVoterSet(t, 3)
	identity, err := NewStorageIdentity(StorageFormatVersion1, [16]byte{1, 2, 3, 4}, localID, voters)
	if err != nil {
		t.Fatal(err)
	}
	return identity, voters
}

func mustStorageEntry(t *testing.T, index, term uint64, command string) Entry {
	t.Helper()
	entry, err := NewEntry(index, term, EntryCommand, []byte(command))
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

func hardStatePointer(state HardState) *HardState { return &state }

func assertRecoveredStateEqual(t *testing.T, got, want RecoveredState) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("recovered state = %+v, want %+v", got, want)
	}
}
