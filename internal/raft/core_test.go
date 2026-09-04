package raft

import (
	"errors"
	"math"
	"reflect"
	"testing"

	"crane/internal/config"
)

type coreScriptedRandom struct {
	values []uint64
	index  int
}

func (s *coreScriptedRandom) Uint64() uint64 {
	if s.index >= len(s.values) {
		panic("core test exhausted scripted randomness")
	}
	value := s.values[s.index]
	s.index++
	return value
}

func TestReadyCloneOwnsNestedEntriesCommandsMessagesAndHardState(t *testing.T) {
	entry, err := NewEntry(1, 1, EntryCommand, []byte("entry-command"))
	if err != nil {
		t.Fatal(err)
	}
	messageEntry, err := NewEntry(2, 1, EntryCommand, []byte("message-command"))
	if err != nil {
		t.Fatal(err)
	}
	hardState := HardState{Term: 1, VotedFor: 1}
	original := Ready{
		Token:     ReadyToken(7),
		HardState: &hardState,
		Entries:   []Entry{entry},
		Messages: []PeerMessage{{
			To: 2,
			RPC: AppendEntriesRequest{
				LeaderID:     1,
				Term:         1,
				Generation:   1,
				PrevLogIndex: 1,
				PrevLogTerm:  1,
				Entries:      []Entry{messageEntry},
			},
			Requires: DurabilityPrerequisite{HardState: true, EntriesThrough: 2},
		}},
	}

	cloned := original.Clone()
	hardState.Term = 9
	original.Entries[0].command[0] = 'X'
	request := original.Messages[0].RPC.(AppendEntriesRequest)
	request.Entries[0].command[0] = 'Y'
	original.Messages[0].RPC = request
	original.Messages[0].Requires.EntriesThrough = 99

	if got, want := *cloned.HardState, (HardState{Term: 1, VotedFor: 1}); got != want {
		t.Fatalf("cloned hard state = %#v, want %#v", got, want)
	}
	if got, want := string(cloned.Entries[0].CommandBytes()), "entry-command"; got != want {
		t.Fatalf("cloned unstable command = %q, want %q", got, want)
	}
	clonedRequest := cloned.Messages[0].RPC.(AppendEntriesRequest)
	if got, want := string(clonedRequest.Entries[0].CommandBytes()), "message-command"; got != want {
		t.Fatalf("cloned message command = %q, want %q", got, want)
	}
	if got, want := cloned.Messages[0].Requires, (DurabilityPrerequisite{HardState: true, EntriesThrough: 2}); got != want {
		t.Fatalf("cloned prerequisite = %#v, want %#v", got, want)
	}
}

func TestReadyReturnsOneStableIndependentlyOwnedBatch(t *testing.T) {
	core := newElectionCore(t, 3, 1, HardState{}, nil, 5, 15, []uint64{10, 10, 10})
	if err := core.Tick(5); err != nil {
		t.Fatal(err)
	}

	first, ok := core.Ready()
	if !ok {
		t.Fatal("Ready() = false, want pre-vote batch")
	}
	if len(first.Messages) != 2 {
		t.Fatalf("first Ready messages = %d, want 2", len(first.Messages))
	}
	first.Messages[0].To = 99

	second, ok := core.Ready()
	if !ok {
		t.Fatal("second Ready() = false, want same outstanding batch")
	}
	if second.Token != first.Token {
		t.Fatalf("second token = %d, want stable %d", second.Token, first.Token)
	}
	if second.Messages[0].To == 99 {
		t.Fatal("mutating first Ready changed independently owned re-read")
	}
	if err := core.Tick(6); !errors.Is(err, ErrReadyOutstanding) {
		t.Fatalf("Tick with Ready outstanding error = %v, want ErrReadyOutstanding", err)
	}
	third, ok := core.Ready()
	if !ok || !reflect.DeepEqual(second, third) {
		t.Fatalf("Ready changed while outstanding:\nsecond=%#v\nthird=%#v", second, third)
	}
}

func TestReadyAdvanceRequiresExactLiveTokenAtomically(t *testing.T) {
	core := newElectionCore(t, 3, 1, HardState{}, nil, 5, 15, []uint64{10, 10})
	if err := core.Tick(5); err != nil {
		t.Fatal(err)
	}
	ready, ok := core.Ready()
	if !ok {
		t.Fatal("Ready() = false, want batch")
	}
	before := ready.Clone()

	if err := core.Advance(ready.Token + 1); !errors.Is(err, ErrAdvanceToken) {
		t.Fatalf("Advance(mismatch) error = %v, want ErrAdvanceToken", err)
	}
	afterMismatch, ok := core.Ready()
	if !ok || !reflect.DeepEqual(before, afterMismatch) {
		t.Fatalf("mismatched Advance changed Ready:\nbefore=%#v\nafter=%#v", before, afterMismatch)
	}
	if err := core.Advance(ready.Token); err != nil {
		t.Fatalf("Advance(exact) error = %v", err)
	}
	if _, ok := core.Ready(); ok {
		t.Fatal("Ready remained available after exact Advance")
	}
	request := PreVoteRequest{CandidateID: 2, CurrentTerm: 0, ProspectiveTerm: 1}
	if err := core.Step(2, request); err != nil {
		t.Fatal(err)
	}
	next := requireReady(t, core)
	if next.Token == ready.Token {
		t.Fatalf("next Ready reused stale token %d", ready.Token)
	}
	if err := core.Advance(ready.Token); !errors.Is(err, ErrAdvanceToken) {
		t.Fatalf("Advance(stale with live batch) error = %v, want ErrAdvanceToken", err)
	}
	afterStale := requireReady(t, core)
	if !reflect.DeepEqual(next, afterStale) {
		t.Fatalf("stale Advance changed live Ready:\nbefore=%#v\nafter=%#v", next, afterStale)
	}
	if err := core.Advance(next.Token); err != nil {
		t.Fatalf("Advance(next exact) error = %v", err)
	}
	if err := core.Advance(ReadyToken(0)); !errors.Is(err, ErrAdvanceToken) {
		t.Fatalf("Advance(missing) error = %v, want ErrAdvanceToken", err)
	}
}

func TestReadyTokenExhaustionFailsClosedWithoutReusingAncientToken(t *testing.T) {
	core := newElectionCore(t, 3, 1, HardState{}, nil, 5, 15, []uint64{10, 10})
	core.nextToken = ReadyToken(math.MaxUint64)
	if err := core.Tick(5); err != nil {
		t.Fatal(err)
	}
	if ready, ok := core.Ready(); ok {
		t.Fatalf("Ready at exhausted token = %#v, want no reusable token", ready)
	}
	if !errors.Is(core.Err(), ErrReadyTokenExhausted) {
		t.Fatalf("Core.Err() = %v, want ErrReadyTokenExhausted", core.Err())
	}
	if err := core.Advance(ReadyToken(1)); !errors.Is(err, ErrReadyTokenExhausted) {
		t.Fatalf("Advance(ancient token 1) error = %v, want exhaustion failure", err)
	}
	if err := core.Tick(6); !errors.Is(err, ErrReadyTokenExhausted) {
		t.Fatalf("Tick after exhaustion error = %v, want stable exhaustion failure", err)
	}
	if err := core.Step(2, PreVoteRequest{CandidateID: 2, CurrentTerm: 0, ProspectiveTerm: 1}); !errors.Is(err, ErrReadyTokenExhausted) {
		t.Fatalf("Step after exhaustion error = %v, want stable exhaustion failure", err)
	}
	if _, err := core.ProposeEntry([]byte("after-exhaustion")); !errors.Is(err, ErrReadyTokenExhausted) {
		t.Fatalf("ProposeEntry after exhaustion error = %v, want stable exhaustion failure", err)
	}
}

func TestReadyGrantedVoteRequiresHardStateDurability(t *testing.T) {
	core := newElectionCore(t, 3, 1, HardState{}, nil, 5, 15, []uint64{10})
	request := RequestVoteRequest{CandidateID: 2, Term: 1}
	if err := core.Step(2, request); err != nil {
		t.Fatal(err)
	}
	ready := requireReady(t, core)
	if ready.HardState == nil || *ready.HardState != (HardState{Term: 1, VotedFor: 2}) {
		t.Fatalf("vote Ready hard state = %#v, want term 1 vote 2", ready.HardState)
	}
	if len(ready.Messages) != 1 {
		t.Fatalf("vote Ready messages = %d, want 1", len(ready.Messages))
	}
	response, ok := ready.Messages[0].RPC.(RequestVoteResponse)
	if !ok || !response.Granted || response.ResponderID != 1 || response.CandidateID != 2 || response.Term != 1 || response.RequestTerm != 1 {
		t.Fatalf("vote response = %#v, want canonical correlated grant", ready.Messages[0].RPC)
	}
	if got, want := ready.Messages[0].Requires, (DurabilityPrerequisite{HardState: true}); got != want {
		t.Fatalf("vote grant prerequisite = %#v, want %#v", got, want)
	}
}

func TestReadyElectionRequestsRequireHardStateDurability(t *testing.T) {
	core := newElectionCore(t, 3, 1, HardState{}, nil, 5, 15, []uint64{10, 10, 10})
	startPreVote(t, core, 5)
	response := PreVoteResponse{
		ResponderID:        2,
		CandidateID:        1,
		Term:               0,
		RequestCurrentTerm: 0,
		ProspectiveTerm:    1,
		Granted:            true,
	}
	if err := core.Step(2, response); err != nil {
		t.Fatal(err)
	}
	ready := requireReady(t, core)
	if ready.HardState == nil || *ready.HardState != (HardState{Term: 1, VotedFor: 1}) {
		t.Fatalf("election Ready hard state = %#v, want durable self vote", ready.HardState)
	}
	if len(ready.Messages) != 2 {
		t.Fatalf("election Ready messages = %d, want 2", len(ready.Messages))
	}
	for _, outbound := range ready.Messages {
		request, ok := outbound.RPC.(RequestVoteRequest)
		if !ok {
			t.Fatalf("election outbound RPC = %T, want RequestVoteRequest", outbound.RPC)
		}
		if request.CandidateID != 1 || request.Term != 1 {
			t.Fatalf("election request = %#v, want candidate 1 term 1", request)
		}
		if got, want := outbound.Requires, (DurabilityPrerequisite{HardState: true}); got != want {
			t.Fatalf("election prerequisite = %#v, want %#v", got, want)
		}
	}
}

func TestReadyNoOpReplicationRequiresEntryDurability(t *testing.T) {
	core := electedCandidate(t, 3, 1)
	if err := core.Step(2, RequestVoteResponse{ResponderID: 2, CandidateID: 1, Term: 1, RequestTerm: 1, Granted: true}); err != nil {
		t.Fatal(err)
	}
	ready := requireReady(t, core)
	if ready.HardState != nil {
		t.Fatalf("leader Ready hard state = %#v, want already-durable election state", ready.HardState)
	}
	if len(ready.Entries) != 1 || ready.Entries[0].Index != 1 || ready.Entries[0].Term != 1 || ready.Entries[0].Kind != EntryNoOp {
		t.Fatalf("leader unstable entries = %#v, want one term-1 no-op at index 1", ready.Entries)
	}
	if len(ready.Messages) != 2 {
		t.Fatalf("leader replication messages = %d, want 2", len(ready.Messages))
	}
	for _, outbound := range ready.Messages {
		request, ok := outbound.RPC.(AppendEntriesRequest)
		if !ok || request.LeaderID != 1 || request.Term != 1 || len(request.Entries) != 1 || request.Entries[0].Kind != EntryNoOp {
			t.Fatalf("leader replication RPC = %#v, want canonical no-op append", outbound.RPC)
		}
		if got, want := outbound.Requires, (DurabilityPrerequisite{EntriesThrough: 1}); got != want {
			t.Fatalf("no-op replication prerequisite = %#v, want %#v", got, want)
		}
	}
}

func TestNewCoreRejectsInconsistentRecoveredState(t *testing.T) {
	voters := testCoreVoters(t, 3)
	baseLog, err := NewLog(0, 0, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*CoreOptions)
	}{
		{name: "non-voter local identity", mutate: func(options *CoreOptions) { options.LocalID = 9 }},
		{name: "nil log", mutate: func(options *CoreOptions) { options.Log = nil }},
		{name: "nil random source", mutate: func(options *CoreOptions) { options.Random = nil }},
		{name: "zero election minimum", mutate: func(options *CoreOptions) { options.ElectionTimeoutMin = 0 }},
		{name: "empty election interval", mutate: func(options *CoreOptions) { options.ElectionTimeoutMax = options.ElectionTimeoutMin }},
		{name: "applied mismatch", mutate: func(options *CoreOptions) { options.AppliedIndex = 1 }},
		{name: "commit mismatch", mutate: func(options *CoreOptions) { options.HardState.CommitIndex = 1 }},
		{name: "vote outside voters", mutate: func(options *CoreOptions) { options.HardState = HardState{Term: 1, VotedFor: 9} }},
		{name: "vote without term", mutate: func(options *CoreOptions) { options.HardState.VotedFor = 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := CoreOptions{
				LocalID:            1,
				Voters:             voters,
				HardState:          HardState{},
				Log:                baseLog,
				AppliedIndex:       0,
				ElectionTimeoutMin: 5,
				ElectionTimeoutMax: 15,
				Random:             &coreScriptedRandom{values: []uint64{10}},
			}
			test.mutate(&options)
			if _, err := NewCore(options); !errors.Is(err, ErrInvalidCoreState) {
				t.Fatalf("NewCore() error = %v, want ErrInvalidCoreState", err)
			}
		})
	}
}

func TestNewCoreRejectsRecoveredLogTermAboveHardState(t *testing.T) {
	entry, err := NewEntry(1, 2, EntryCommand, []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	log, err := NewLog(0, 0, 0, 0, []Entry{entry})
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewCore(CoreOptions{
		LocalID:            1,
		Voters:             testCoreVoters(t, 3),
		HardState:          HardState{Term: 1},
		Log:                log,
		AppliedIndex:       0,
		ElectionTimeoutMin: 5,
		ElectionTimeoutMax: 15,
		Random:             &coreScriptedRandom{values: []uint64{10}},
	})
	if !errors.Is(err, ErrInvalidCoreState) {
		t.Fatalf("NewCore(log term above current) error = %v, want ErrInvalidCoreState", err)
	}
}

func TestNewCoreOwnsRecoveredLog(t *testing.T) {
	recovered, err := NewLog(0, 0, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	core, err := NewCore(CoreOptions{
		LocalID:            1,
		Voters:             testCoreVoters(t, 3),
		HardState:          HardState{Term: 1},
		Log:                recovered,
		ElectionTimeoutMin: 5,
		ElectionTimeoutMax: 15,
		Random:             &coreScriptedRandom{values: []uint64{10}},
	})
	if err != nil {
		t.Fatal(err)
	}
	entry, err := NewEntry(1, 1, EntryCommand, []byte("external"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recovered.Append(0, 0, []Entry{entry}); err != nil {
		t.Fatal(err)
	}
	if got := core.Status().LastIndex; got != 0 {
		t.Fatalf("core last index after caller mutated recovered log = %d, want owned 0", got)
	}
}

func TestElectionDeadlineArithmeticOverflowFailsWithoutStateChange(t *testing.T) {
	core := newElectionCore(t, 3, 1, HardState{}, nil, 5, 15, []uint64{10, 9})
	beforeStatus := core.Status()
	beforeHardState := core.HardState()
	beforeDeadline := core.ElectionDeadline()

	if err := core.Tick(math.MaxUint64 - 2); !errors.Is(err, ErrDeadlineOverflow) {
		t.Fatalf("Tick(overflowing deadline reset) error = %v, want ErrDeadlineOverflow", err)
	}
	if got := core.Status(); got != beforeStatus {
		t.Fatalf("overflow changed status from %#v to %#v", beforeStatus, got)
	}
	if got := core.HardState(); got != beforeHardState {
		t.Fatalf("overflow changed hard state from %#v to %#v", beforeHardState, got)
	}
	if got := core.ElectionDeadline(); got != beforeDeadline {
		t.Fatalf("overflow changed deadline from %d to %d", beforeDeadline, got)
	}
	if _, ok := core.Ready(); ok {
		t.Fatal("overflow produced a Ready batch")
	}
}

func testCoreVoters(t *testing.T, count int) VoterSet {
	t.Helper()
	configured := make([]config.RaftVoter, count)
	for index := range configured {
		configured[index] = config.RaftVoter{
			NodeID:   uint16(index + 1),
			Endpoint: "127.0.0.1:" + string(rune('0'+index+1)) + "008",
		}
	}
	voters, err := NewVoterSet(configured)
	if err != nil {
		t.Fatal(err)
	}
	return voters
}

func newElectionCore(t *testing.T, voterCount int, localID uint16, hardState HardState, entries []Entry, minimum, maximum uint64, randomValues []uint64) *Core {
	t.Helper()
	log, err := NewLog(0, 0, hardState.CommitIndex, hardState.CommitIndex, entries)
	if err != nil {
		t.Fatal(err)
	}
	core, err := NewCore(CoreOptions{
		LocalID:            localID,
		Voters:             testCoreVoters(t, voterCount),
		HardState:          hardState,
		Log:                log,
		AppliedIndex:       hardState.CommitIndex,
		ElectionTimeoutMin: minimum,
		ElectionTimeoutMax: maximum,
		Random:             &coreScriptedRandom{values: randomValues},
	})
	if err != nil {
		t.Fatal(err)
	}
	return core
}

func requireReady(t *testing.T, core *Core) Ready {
	t.Helper()
	ready, ok := core.Ready()
	if !ok {
		t.Fatal("Ready() = false, want batch")
	}
	return ready
}

func advanceReady(t *testing.T, core *Core) Ready {
	t.Helper()
	ready := requireReady(t, core)
	if err := core.Advance(ready.Token); err != nil {
		t.Fatal(err)
	}
	return ready
}

func startPreVote(t *testing.T, core *Core, tick uint64) Ready {
	t.Helper()
	if err := core.Tick(tick); err != nil {
		t.Fatal(err)
	}
	return advanceReady(t, core)
}

func electedCandidate(t *testing.T, voterCount int, localID uint16) *Core {
	t.Helper()
	core := newElectionCore(t, voterCount, localID, HardState{}, nil, 5, 15, []uint64{10, 10, 10, 10, 10, 10, 10, 10})
	startPreVote(t, core, 5)
	responder := uint16(1)
	if responder == localID {
		responder = 2
	}
	response := PreVoteResponse{
		ResponderID:        responder,
		CandidateID:        localID,
		RequestCurrentTerm: 0,
		ProspectiveTerm:    1,
		Granted:            true,
	}
	if voterCount == 5 {
		if err := core.Step(responder, response); err != nil {
			t.Fatal(err)
		}
		if _, ok := core.Ready(); ok {
			t.Fatal("one peer pre-vote formed a five-voter majority")
		}
		responder++
		if responder == localID {
			responder++
		}
		response.ResponderID = responder
	}
	if err := core.Step(responder, response); err != nil {
		t.Fatal(err)
	}
	advanceReady(t, core)
	if got := core.Status().Role; got != RoleCandidate {
		t.Fatalf("role = %v, want candidate", got)
	}
	return core
}
