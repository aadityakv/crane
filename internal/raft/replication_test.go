package raft

import (
	"errors"
	"reflect"
	"testing"
)

func TestAppendHeartbeatMatchesAndCapsCommitAtVerifiedPrevIndex(t *testing.T) {
	entries := testEntries(t,
		testEntrySpec{term: 1, command: "one"},
		testEntrySpec{term: 1, command: "two"},
	)
	core := newReplicationCore(t, 3, 1, HardState{Term: 2}, 0, entries, []uint64{4, 6})
	if err := core.Tick(2); err != nil {
		t.Fatal(err)
	}

	request := AppendEntriesRequest{
		LeaderID: 2, Term: 2, Generation: 7,
		PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 2,
	}
	if err := core.Step(2, request); err != nil {
		t.Fatal(err)
	}
	ready := requireReady(t, core)
	response := appendResponseFromReady(t, ready)
	if !response.Success || response.MatchIndex != 1 {
		t.Fatalf("heartbeat response = %#v, want success through exactly prev index 1", response)
	}
	if got := core.Status().CommitIndex; got != 1 {
		t.Fatalf("heartbeat commit = %d, want verified cap 1", got)
	}
	if ready.HardState == nil || *ready.HardState != (HardState{Term: 2, CommitIndex: 1}) {
		t.Fatalf("heartbeat hard state = %#v, want durable commit 1", ready.HardState)
	}
	if len(ready.Entries) != 0 {
		t.Fatalf("empty heartbeat emitted unstable entries: %#v", ready.Entries)
	}
	if len(ready.CommittedEntries) != 1 || ready.CommittedEntries[0].Index != 1 || string(ready.CommittedEntries[0].CommandBytes()) != "one" {
		t.Fatalf("heartbeat committed output = %#v, want only verified entry 1", ready.CommittedEntries)
	}
	if got, want := ready.Messages[0].Requires, (DurabilityPrerequisite{HardState: true}); got != want {
		t.Fatalf("heartbeat success prerequisite = %#v, want %#v", got, want)
	}
	if got, want := core.ElectionDeadline(), uint64(8); got != want {
		t.Fatalf("matching contact deadline = %d, want %d", got, want)
	}
	advanceReadyToken(t, core, ready)
	if got := core.Status().AppliedIndex; got != 1 {
		t.Fatalf("heartbeat applied index after Advance = %d, want 1", got)
	}
}

func TestAppendMissingAndMismatchedPrevReturnExactLogHints(t *testing.T) {
	entries := testEntries(t,
		testEntrySpec{term: 1, command: "one"},
		testEntrySpec{term: 2, command: "two"},
		testEntrySpec{term: 2, command: "three"},
	)
	tests := []struct {
		name          string
		prevIndex     uint64
		prevTerm      uint64
		conflictTerm  uint64
		conflictIndex uint64
	}{
		{name: "missing previous entry", prevIndex: 5, prevTerm: 2, conflictIndex: 4},
		{name: "mismatched previous term", prevIndex: 3, prevTerm: 1, conflictTerm: 2, conflictIndex: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			core := newReplicationCore(t, 3, 1, HardState{Term: 3}, 0, entries, []uint64{4, 4})
			request := AppendEntriesRequest{
				LeaderID: 2, Term: 3, Generation: 9,
				PrevLogIndex: test.prevIndex, PrevLogTerm: test.prevTerm,
			}
			if err := core.Step(2, request); err != nil {
				t.Fatal(err)
			}
			ready := requireReady(t, core)
			response := appendResponseFromReady(t, ready)
			if response.Success || response.MatchIndex != 0 || response.ConflictTerm != test.conflictTerm || response.ConflictIndex != test.conflictIndex {
				t.Fatalf("rejection = %#v, want term/index %d/%d", response, test.conflictTerm, test.conflictIndex)
			}
			if ready.HardState != nil || len(ready.Entries) != 0 || ready.Messages[0].Requires != (DurabilityPrerequisite{}) {
				t.Fatalf("log mismatch produced durability work: %#v", ready)
			}
		})
	}
}

func TestAppendRepairsOnlyUncommittedSuffixAndMatchingRetryIsIdempotent(t *testing.T) {
	local := testEntries(t,
		testEntrySpec{term: 1, command: "kept"},
		testEntrySpec{term: 2, command: "old-two"},
		testEntrySpec{term: 2, command: "old-three"},
	)
	replacement := testEntriesFrom(t, 2,
		testEntrySpec{term: 3, command: "new-two"},
		testEntrySpec{term: 3, command: "new-three"},
	)
	core := newReplicationCore(t, 3, 1, HardState{Term: 3}, 0, local, []uint64{4, 4, 4})
	request := AppendEntriesRequest{
		LeaderID: 2, Term: 3, Generation: 11,
		PrevLogIndex: 1, PrevLogTerm: 1, Entries: replacement,
	}

	if err := core.Step(2, request); err != nil {
		t.Fatal(err)
	}
	ready := requireReady(t, core)
	response := appendResponseFromReady(t, ready)
	if !response.Success || response.MatchIndex != 3 {
		t.Fatalf("repair response = %#v, want success through 3", response)
	}
	if !reflect.DeepEqual(ready.Entries, replacement) {
		t.Fatalf("repair unstable entries = %#v, want exact replacement %#v", ready.Entries, replacement)
	}
	if got, want := ready.Messages[0].Requires, (DurabilityPrerequisite{EntriesThrough: 3}); got != want {
		t.Fatalf("repair success prerequisite = %#v, want %#v", got, want)
	}
	if err := core.Advance(ready.Token); err != nil {
		t.Fatal(err)
	}

	request.Generation++
	if err := core.Step(2, request); err != nil {
		t.Fatal(err)
	}
	retry := requireReady(t, core)
	retryResponse := appendResponseFromReady(t, retry)
	if !retryResponse.Success || retryResponse.MatchIndex != 3 {
		t.Fatalf("retry response = %#v, want idempotent success through 3", retryResponse)
	}
	if len(retry.Entries) != 0 || retry.Messages[0].Requires != (DurabilityPrerequisite{}) {
		t.Fatalf("matching retry repeated persistence work: %#v", retry)
	}
	state := core.LogState()
	if !reflect.DeepEqual(state.Entries, append(testEntries(t, testEntrySpec{term: 1, command: "kept"}), replacement...)) {
		t.Fatalf("repaired log = %#v, want kept prefix plus replacement", state.Entries)
	}
}

func TestAppendCommittedButUnappliedConflictIsFatalWithoutPeerRejection(t *testing.T) {
	local := testEntries(t,
		testEntrySpec{term: 1, command: "applied"},
		testEntrySpec{term: 1, command: "committed-not-applied"},
	)
	core := newReplicationCore(t, 3, 1, HardState{Term: 3, CommitIndex: 2}, 1, local, []uint64{4, 4})
	replacement := testEntriesFrom(t, 2, testEntrySpec{term: 3, command: "illegal"})
	beforeLog := core.LogState()
	request := AppendEntriesRequest{
		LeaderID: 2, Term: 3, Generation: 13,
		PrevLogIndex: 1, PrevLogTerm: 1, Entries: replacement,
	}

	if err := core.Step(2, request); !errors.Is(err, ErrCommittedConflict) {
		t.Fatalf("protected append error = %v, want ErrCommittedConflict", err)
	}
	if got := core.LogState(); !reflect.DeepEqual(got, beforeLog) {
		t.Fatalf("fatal conflict changed protected log:\n got=%#v\nwant=%#v", got, beforeLog)
	}
	if _, ok := core.Ready(); ok {
		t.Fatal("fatal committed conflict emitted an ordinary peer rejection")
	}
}

func TestAppendHigherTermSuccessRequiresTermAndEntriesDurable(t *testing.T) {
	core := newReplicationCore(t, 3, 1, HardState{Term: 1, VotedFor: 1}, 0, nil, []uint64{4, 4})
	entry := testEntries(t, testEntrySpec{term: 2, command: "new"})[0]
	request := AppendEntriesRequest{
		LeaderID: 2, Term: 2, Generation: 15,
		Entries: []Entry{entry}, LeaderCommit: 1,
	}

	if err := core.Step(2, request); err != nil {
		t.Fatal(err)
	}
	ready := requireReady(t, core)
	response := appendResponseFromReady(t, ready)
	if !response.Success || response.MatchIndex != 1 || response.Term != 2 || response.RequestTerm != 2 {
		t.Fatalf("higher-term append response = %#v, want correlated success", response)
	}
	if ready.HardState == nil || *ready.HardState != (HardState{Term: 2, CommitIndex: 1}) {
		t.Fatalf("higher-term append hard state = %#v, want term 2 commit 1", ready.HardState)
	}
	if !reflect.DeepEqual(ready.Entries, []Entry{entry}) {
		t.Fatalf("higher-term unstable entries = %#v, want appended entry", ready.Entries)
	}
	if got, want := ready.Messages[0].Requires, (DurabilityPrerequisite{HardState: true, EntriesThrough: 1}); got != want {
		t.Fatalf("higher-term success prerequisite = %#v, want %#v", got, want)
	}
}

func TestAppendStaleTermRejectsWithoutLeaderContactOrMutation(t *testing.T) {
	entries := testEntries(t, testEntrySpec{term: 2, command: "local"})
	core := newReplicationCore(t, 3, 1, HardState{Term: 3}, 0, entries, []uint64{4})
	if err := core.Tick(2); err != nil {
		t.Fatal(err)
	}
	beforeStatus := core.Status()
	beforeDeadline := core.ElectionDeadline()
	beforeLog := core.LogState()
	request := AppendEntriesRequest{LeaderID: 2, Term: 2, Generation: 17, PrevLogIndex: 1, PrevLogTerm: 2}

	if err := core.Step(2, request); err != nil {
		t.Fatal(err)
	}
	ready := requireReady(t, core)
	response := appendResponseFromReady(t, ready)
	if response.Success || response.Term != 3 || response.RequestTerm != 2 || response.Generation != 17 || response.ConflictIndex != 2 {
		t.Fatalf("stale append response = %#v, want safe current-term rejection", response)
	}
	if got := core.Status(); got != beforeStatus {
		t.Fatalf("stale append changed status from %#v to %#v", beforeStatus, got)
	}
	if got := core.ElectionDeadline(); got != beforeDeadline {
		t.Fatalf("stale append reset deadline from %d to %d", beforeDeadline, got)
	}
	if got := core.LogState(); !reflect.DeepEqual(got, beforeLog) {
		t.Fatalf("stale append changed log:\n got=%#v\nwant=%#v", got, beforeLog)
	}
	if ready.HardState != nil || len(ready.Entries) != 0 || ready.Messages[0].Requires != (DurabilityPrerequisite{}) {
		t.Fatalf("stale append produced durability work: %#v", ready)
	}
}

func TestProgressInitializesLeaderAndFollowersAtExactLogBounds(t *testing.T) {
	core, request := leaderWithNoOpRequest(t, 2)
	self, ok := core.Progress(1)
	if !ok {
		t.Fatal("leader progress missing local voter")
	}
	if self.MatchIndex != 1 || self.NextIndex != 2 || self.Generation != 0 || self.ActiveGeneration != 0 || self.SnapshotNeeded {
		t.Fatalf("leader self progress = %#v, want complete local log through 1", self)
	}
	follower, ok := core.Progress(2)
	if !ok {
		t.Fatal("leader progress missing follower 2")
	}
	if follower.MatchIndex != 0 || follower.NextIndex != 1 || follower.Generation != 1 || follower.ActiveGeneration != 1 || follower.SnapshotNeeded {
		t.Fatalf("follower progress = %#v, want match 0 next 1 active generation 1", follower)
	}
	if request.Generation != follower.ActiveGeneration || request.PrevLogIndex != 0 || len(request.Entries) != 1 || request.Entries[0].Index != 1 {
		t.Fatalf("initial request = %#v, want generation-bound no-op from next index 1", request)
	}
}

func TestReplicationConsumesExactPeerGenerationOnceAndOldFailureCannotUndoNewerSuccess(t *testing.T) {
	core, first := leaderWithNoOpRequest(t, 2)
	beforeFuture, _ := core.Progress(2)
	forged := appendSuccess(first, 2)
	forged.Generation++
	if err := core.Step(2, forged); err != nil {
		t.Fatal(err)
	}
	if got, _ := core.Progress(2); got != beforeFuture {
		t.Fatalf("future generation changed progress from %#v to %#v", beforeFuture, got)
	}

	if err := core.Tick(6); err != nil {
		t.Fatal(err)
	}
	heartbeatReady := requireReady(t, core)
	second := appendRequestTo(t, heartbeatReady, 2)
	if second.Generation != first.Generation+1 {
		t.Fatalf("second generation = %d, want %d", second.Generation, first.Generation+1)
	}
	advanceReadyToken(t, core, heartbeatReady)
	if err := core.Step(2, appendSuccess(second, 2)); err != nil {
		t.Fatal(err)
	}
	commitReady := requireReady(t, core)
	advanceReadyToken(t, core, commitReady)
	afterSuccess, _ := core.Progress(2)
	if afterSuccess.MatchIndex != 1 || afterSuccess.NextIndex != 2 || afterSuccess.ActiveGeneration != 0 {
		t.Fatalf("newer success progress = %#v, want match 1 next 2 consumed", afterSuccess)
	}

	oldFailure := AppendEntriesResponse{
		ResponderID: 2, LeaderID: 1, Term: 1, RequestTerm: 1,
		Generation: first.Generation, Success: false, ConflictIndex: 1,
	}
	if err := core.Step(2, oldFailure); err != nil {
		t.Fatal(err)
	}
	if got, _ := core.Progress(2); got != afterSuccess {
		t.Fatalf("old failure changed progress from %#v to %#v", afterSuccess, got)
	}
	if _, ok := core.Ready(); ok {
		t.Fatal("old consumed failure emitted output")
	}
}

func TestReplicationFailureFastBacktracksAfterLastLocalConflictTerm(t *testing.T) {
	entries := testEntries(t,
		testEntrySpec{term: 1, command: "one"},
		testEntrySpec{term: 2, command: "two"},
		testEntrySpec{term: 2, command: "three"},
		testEntrySpec{term: 3, command: "four"},
	)
	core, initial := electReplicationLeader(t, 0, 0, HardState{Term: 3}, entries)
	rejection := AppendEntriesResponse{
		ResponderID: 2, LeaderID: 1, Term: 4, RequestTerm: 4,
		Generation: initial.Generation, Success: false,
		ConflictTerm: 2, ConflictIndex: 2,
	}
	if err := core.Step(2, rejection); err != nil {
		t.Fatal(err)
	}
	ready := requireReady(t, core)
	retry := appendRequestTo(t, ready, 2)
	if retry.Generation != initial.Generation+1 || retry.PrevLogIndex != 3 || retry.PrevLogTerm != 2 {
		t.Fatalf("fast retry metadata = %#v, want generation 2 after local term-2 tail at 3", retry)
	}
	if len(retry.Entries) != 2 || retry.Entries[0].Index != 4 || retry.Entries[1].Index != 5 {
		t.Fatalf("fast retry entries = %#v, want contiguous indices 4 and 5", retry.Entries)
	}
	progress, _ := core.Progress(2)
	if progress.MatchIndex != 0 || progress.NextIndex != 4 || progress.ActiveGeneration != retry.Generation {
		t.Fatalf("fast retry progress = %#v, want monotonic match 0 next 4", progress)
	}
}

func TestReplicationGenerationIsPeerLocalAndWrongPeerResponseIsAtomic(t *testing.T) {
	core, first := leaderWithNoOpRequest(t, 2)
	rejectInitialAppend(t, core, first, 1)
	retryReady := requireReady(t, core)
	retry := appendRequestTo(t, retryReady, 2)
	advanceReadyToken(t, core, retryReady)
	beforePeer3, _ := core.Progress(3)
	beforeStatus := core.Status()
	forged := appendSuccess(retry, 3)
	if err := core.Step(3, forged); err != nil {
		t.Fatal(err)
	}
	if got, _ := core.Progress(3); got != beforePeer3 {
		t.Fatalf("peer-2 generation changed peer-3 progress from %#v to %#v", beforePeer3, got)
	}
	if got := core.Status(); got != beforeStatus {
		t.Fatalf("wrong-peer response changed status from %#v to %#v", beforeStatus, got)
	}
	if _, ok := core.Ready(); ok {
		t.Fatal("wrong-peer generation emitted output")
	}
}

func TestProgressMarksSnapshotNeededWhenHintReachesCompactedBase(t *testing.T) {
	entries := testEntriesFrom(t, 4, testEntrySpec{term: 3, command: "four"})
	core, initial := electReplicationLeader(t, 3, 2, HardState{Term: 3, CommitIndex: 3}, entries)
	rejection := AppendEntriesResponse{
		ResponderID: 2, LeaderID: 1, Term: 4, RequestTerm: 4,
		Generation: initial.Generation, Success: false, ConflictIndex: 2,
	}
	if err := core.Step(2, rejection); err != nil {
		t.Fatal(err)
	}
	progress, _ := core.Progress(2)
	if !progress.SnapshotNeeded || progress.MatchIndex != 0 || progress.NextIndex != 2 || progress.ActiveGeneration != 0 {
		t.Fatalf("compacted rejection progress = %#v, want snapshot-needed at next 2", progress)
	}
	if _, ok := core.Ready(); ok {
		t.Fatal("snapshot-needed progress emitted Task 9 transfer work")
	}
}

func TestCheckQuorumCountsExactMismatchResponseThenRequiresFreshWindowEvidence(t *testing.T) {
	core, initial := leaderWithNoOpRequest(t, 2)
	rejection := AppendEntriesResponse{
		ResponderID: 2, LeaderID: 1, Term: 1, RequestTerm: 1,
		Generation: initial.Generation, Success: false, ConflictIndex: 1,
	}
	if err := core.Step(2, rejection); err != nil {
		t.Fatal(err)
	}
	advanceReady(t, core)
	if err := core.Tick(10); err != nil {
		t.Fatal(err)
	}
	if got := core.Status().Role; got != RoleLeader {
		t.Fatalf("role with self plus exact rejection = %v, want leader", got)
	}
	if ready, ok := core.Ready(); ok {
		advanceReadyToken(t, core, ready)
	}
	if err := core.Tick(15); err != nil {
		t.Fatal(err)
	}
	if got := core.Status().Role; got != RoleFollower {
		t.Fatalf("role without fresh response evidence = %v, want follower", got)
	}
}

func TestCheckQuorumDoesNotCountIssuedWritesWithoutResponses(t *testing.T) {
	core, _ := leaderWithNoOpRequest(t, 2)
	if err := core.Tick(10); err != nil {
		t.Fatal(err)
	}
	if got := core.Status().Role; got != RoleFollower {
		t.Fatalf("role after writes without responses = %v, want follower", got)
	}
}

func TestReplicationBatchUsesEntryCountAndCanonicalEncodedByteCaps(t *testing.T) {
	entries := testEntries(t,
		testEntrySpec{term: 1, command: "aaaaaaaaaa"},
		testEntrySpec{term: 1, command: "bbbbbbbbbbbbbbbbbbbb"},
		testEntrySpec{term: 1, command: "cccccccccccccccccccccccccccccc"},
	)

	t.Run("entry count", func(t *testing.T) {
		core, initial := electReplicationLeaderWithOptions(t, 0, 0, HardState{Term: 1}, entries, 2, 0)
		rejectInitialAppend(t, core, initial, 1)
		request := appendRequestTo(t, requireReady(t, core), 2)
		if len(request.Entries) != 2 || request.Entries[0].Index != 1 || request.Entries[1].Index != 2 {
			t.Fatalf("count-capped entries = %#v, want largest two-entry prefix", request.Entries)
		}
	})

	t.Run("actual encoded bytes", func(t *testing.T) {
		oracle := AppendEntriesRequest{
			LeaderID: 1, Term: 2, Generation: 2,
			Entries: cloneEntries(entries[:2]),
		}
		_, twoPayload, err := EncodeRPC(oracle, DefaultCodecLimits())
		if err != nil {
			t.Fatal(err)
		}
		oneOver := oracle
		oneOver.Entries = cloneEntries(entries[:3])
		_, threePayload, err := EncodeRPC(oneOver, DefaultCodecLimits())
		if err != nil {
			t.Fatal(err)
		}
		if len(threePayload) <= len(twoPayload) {
			t.Fatalf("oracle payload sizes two=%d three=%d, want strict growth", len(twoPayload), len(threePayload))
		}

		core, initial := electReplicationLeaderWithOptions(t, 0, 0, HardState{Term: 1}, entries, 4, uint64(len(twoPayload)))
		rejectInitialAppend(t, core, initial, 1)
		request := appendRequestTo(t, requireReady(t, core), 2)
		if len(request.Entries) != 2 {
			t.Fatalf("byte-capped entries = %d, want exact two-entry prefix", len(request.Entries))
		}
		_, payload, err := EncodeRPC(request, CodecLimits{MaxAppendEntries: 4, MaxEncodedBytes: uint64(len(twoPayload))})
		if err != nil {
			t.Fatalf("selected request does not fit canonical cap: %v", err)
		}
		if got := len(payload); got != len(twoPayload) {
			t.Fatalf("selected encoded bytes = %d, want boundary %d", got, len(twoPayload))
		}
	})
}

func TestReplicationEmitsHeartbeatWhenNoEntryFitsEncodedCap(t *testing.T) {
	entries := testEntries(t, testEntrySpec{term: 1, command: "entry-does-not-fit"})
	empty := AppendEntriesRequest{LeaderID: 1, Term: 2, Generation: 2}
	_, payload, err := EncodeRPC(empty, DefaultCodecLimits())
	if err != nil {
		t.Fatal(err)
	}
	core, initial := electReplicationLeaderWithOptions(t, 0, 0, HardState{Term: 1}, entries, 2, uint64(len(payload)))
	rejectInitialAppend(t, core, initial, 1)
	request := appendRequestTo(t, requireReady(t, core), 2)
	if len(request.Entries) != 0 || request.PrevLogIndex != 0 || request.PrevLogTerm != 0 {
		t.Fatalf("no-fit request = %#v, want empty heartbeat at log base", request)
	}
	if _, encoded, err := EncodeRPC(request, CodecLimits{MaxAppendEntries: 2, MaxEncodedBytes: uint64(len(payload))}); err != nil || len(encoded) != len(payload) {
		t.Fatalf("heartbeat encoding bytes/error = %d/%v, want exact cap %d", len(encoded), err, len(payload))
	}
}

func TestNewCoreRejectsHeartbeatIntervalAtElectionMinimum(t *testing.T) {
	log, err := NewLog(0, 0, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewCore(CoreOptions{
		LocalID: 1, Voters: testCoreVoters(t, 3), Log: log,
		ElectionTimeoutMin: 5, ElectionTimeoutMax: 10, HeartbeatInterval: 5,
		Random: &coreScriptedRandom{values: []uint64{10}},
	})
	if !errors.Is(err, ErrInvalidCoreState) {
		t.Fatalf("NewCore(heartbeat at election minimum) error = %v, want ErrInvalidCoreState", err)
	}
}

func TestAppendRejectsConfiguredEntryCountAtomically(t *testing.T) {
	log, err := NewLog(0, 0, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	core, err := NewCore(CoreOptions{
		LocalID: 1, Voters: testCoreVoters(t, 3), HardState: HardState{Term: 1},
		Log: log, ElectionTimeoutMin: 5, ElectionTimeoutMax: 10,
		MaxAppendEntries: 1,
		Random:           &coreScriptedRandom{values: []uint64{10, 10}},
	})
	if err != nil {
		t.Fatal(err)
	}
	before := snapshotCore(t, core)
	request := AppendEntriesRequest{
		LeaderID: 2, Term: 1, Generation: 1,
		Entries: testEntries(t,
			testEntrySpec{term: 1, command: "one"},
			testEntrySpec{term: 1, command: "two"},
		),
	}
	if err := core.Step(2, request); !errors.Is(err, ErrRPCTooLarge) {
		t.Fatalf("configured oversize append error = %v, want ErrRPCTooLarge", err)
	}
	assertCoreSnapshot(t, core, before)
}

type testEntrySpec struct {
	term    uint64
	kind    EntryKind
	command string
}

func testEntries(t *testing.T, specs ...testEntrySpec) []Entry {
	t.Helper()
	return testEntriesFrom(t, 1, specs...)
}

func testEntriesFrom(t *testing.T, firstIndex uint64, specs ...testEntrySpec) []Entry {
	t.Helper()
	entries := make([]Entry, len(specs))
	for offset, spec := range specs {
		kind := spec.kind
		if kind == 0 {
			kind = EntryCommand
		}
		command := []byte(spec.command)
		if kind == EntryNoOp {
			command = nil
		}
		entry, err := NewEntry(firstIndex+uint64(offset), spec.term, kind, command)
		if err != nil {
			t.Fatal(err)
		}
		entries[offset] = entry
	}
	return entries
}

func newReplicationCore(t *testing.T, voters int, localID uint16, hardState HardState, applied uint64, entries []Entry, random []uint64) *Core {
	t.Helper()
	log, err := NewLog(0, 0, hardState.CommitIndex, applied, entries)
	if err != nil {
		t.Fatal(err)
	}
	core, err := NewCore(CoreOptions{
		LocalID: localID, Voters: testCoreVoters(t, voters),
		HardState: hardState, Log: log, AppliedIndex: applied,
		ElectionTimeoutMin: 5, ElectionTimeoutMax: 10,
		Random: &coreScriptedRandom{values: random},
	})
	if err != nil {
		t.Fatal(err)
	}
	return core
}

func electReplicationLeader(t *testing.T, snapshotIndex, snapshotTerm uint64, hardState HardState, entries []Entry) (*Core, AppendEntriesRequest) {
	t.Helper()
	return electReplicationLeaderWithOptions(t, snapshotIndex, snapshotTerm, hardState, entries, 0, 0)
}

func electReplicationLeaderWithOptions(t *testing.T, snapshotIndex, snapshotTerm uint64, hardState HardState, entries []Entry, maxEntries uint16, maxBytes uint64) (*Core, AppendEntriesRequest) {
	t.Helper()
	log, err := NewLog(snapshotIndex, snapshotTerm, hardState.CommitIndex, hardState.CommitIndex, entries)
	if err != nil {
		t.Fatal(err)
	}
	core, err := NewCore(CoreOptions{
		LocalID: 1, Voters: testCoreVoters(t, 3), HardState: hardState,
		Log: log, AppliedIndex: hardState.CommitIndex,
		ElectionTimeoutMin: 5, ElectionTimeoutMax: 10,
		MaxAppendEntries: maxEntries, MaxAppendBytes: maxBytes,
		Random: &coreScriptedRandom{values: []uint64{10, 10, 10, 10, 10, 10}},
	})
	if err != nil {
		t.Fatal(err)
	}
	startPreVote(t, core, 5)
	preVote := PreVoteResponse{
		ResponderID: 2, CandidateID: 1, Term: hardState.Term,
		RequestCurrentTerm: hardState.Term, ProspectiveTerm: hardState.Term + 1, Granted: true,
	}
	if err := core.Step(2, preVote); err != nil {
		t.Fatal(err)
	}
	advanceReady(t, core)
	vote := RequestVoteResponse{
		ResponderID: 2, CandidateID: 1, Term: hardState.Term + 1,
		RequestTerm: hardState.Term + 1, Granted: true,
	}
	if err := core.Step(2, vote); err != nil {
		t.Fatal(err)
	}
	ready := advanceReady(t, core)
	return core, appendRequestTo(t, ready, 2)
}

func rejectInitialAppend(t *testing.T, core *Core, initial AppendEntriesRequest, conflictIndex uint64) {
	t.Helper()
	rejection := AppendEntriesResponse{
		ResponderID: 2, LeaderID: initial.LeaderID,
		Term: initial.Term, RequestTerm: initial.Term, Generation: initial.Generation,
		Success: false, ConflictIndex: conflictIndex,
	}
	if err := core.Step(2, rejection); err != nil {
		t.Fatal(err)
	}
}

func appendRequestTo(t *testing.T, ready Ready, peerID uint16) AppendEntriesRequest {
	t.Helper()
	for _, message := range ready.Messages {
		if message.To != peerID {
			continue
		}
		request, ok := message.RPC.(AppendEntriesRequest)
		if !ok {
			t.Fatalf("outbound to %d = %T, want AppendEntriesRequest", peerID, message.RPC)
		}
		return request
	}
	t.Fatalf("Ready has no append request to peer %d: %#v", peerID, ready.Messages)
	return AppendEntriesRequest{}
}

func appendSuccess(request AppendEntriesRequest, responderID uint16) AppendEntriesResponse {
	matchIndex := request.PrevLogIndex
	if len(request.Entries) != 0 {
		matchIndex = request.Entries[len(request.Entries)-1].Index
	}
	return AppendEntriesResponse{
		ResponderID: responderID, LeaderID: request.LeaderID,
		Term: request.Term, RequestTerm: request.Term, Generation: request.Generation,
		Success: true, MatchIndex: matchIndex,
	}
}

func advanceReadyToken(t *testing.T, core *Core, ready Ready) {
	t.Helper()
	if err := core.Advance(ready.Token); err != nil {
		t.Fatal(err)
	}
}

func appendResponseFromReady(t *testing.T, ready Ready) AppendEntriesResponse {
	t.Helper()
	if len(ready.Messages) != 1 {
		t.Fatalf("Ready messages = %d, want one append response", len(ready.Messages))
	}
	response, ok := ready.Messages[0].RPC.(AppendEntriesResponse)
	if !ok {
		t.Fatalf("Ready RPC = %T, want AppendEntriesResponse", ready.Messages[0].RPC)
	}
	return response
}
