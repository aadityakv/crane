package raft

import (
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestElectionDeadlineSamplingIncludesLowerAndExcludesUpperBounds(t *testing.T) {
	core := newElectionCore(t, 3, 1, HardState{}, nil, 5, 15, []uint64{10, 9})
	if got, want := core.ElectionDeadline(), uint64(5); got != want {
		t.Fatalf("initial deadline = %d, want inclusive lower bound %d", got, want)
	}
	if err := core.Tick(5); err != nil {
		t.Fatal(err)
	}
	if got, want := core.ElectionDeadline(), uint64(19); got != want {
		t.Fatalf("resampled deadline = %d, want current 5 plus maximum timeout below 15", got)
	}
	if core.ElectionDeadline() == 20 {
		t.Fatal("resampled deadline used excluded upper timeout bound")
	}
}

func TestElectionDeadlineSamplingRejectsModuloBiasedValues(t *testing.T) {
	source := &coreScriptedRandom{values: []uint64{5, 16}}
	log, err := NewLog(0, 0, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	core, err := NewCore(CoreOptions{
		LocalID:            1,
		Voters:             testCoreVoters(t, 3),
		Log:                log,
		ElectionTimeoutMin: 5,
		ElectionTimeoutMax: 15,
		Random:             source,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := core.ElectionDeadline(), uint64(11); got != want {
		t.Fatalf("deadline = %d, want 5 + unbiased bounded sample 6", got)
	}
	if source.index != 2 {
		t.Fatalf("sampler consumed %d values, want rejected value then retry", source.index)
	}
}

func TestElectionTickRejectsLogicalTimeRegressionWithoutMutation(t *testing.T) {
	core := newElectionCore(t, 3, 1, HardState{}, nil, 5, 15, []uint64{10})
	if err := core.Tick(4); err != nil {
		t.Fatal(err)
	}
	before := snapshotCore(t, core)
	if err := core.Tick(3); !errors.Is(err, ErrTickRegression) {
		t.Fatalf("Tick(regression) error = %v, want ErrTickRegression", err)
	}
	assertCoreSnapshot(t, core, before)
}

func TestPreVoteRecentLeaderContactIsRejectedAndResetsDeadline(t *testing.T) {
	core := newElectionCore(t, 3, 1, HardState{Term: 2}, nil, 5, 15, []uint64{10, 9})
	if err := core.Tick(2); err != nil {
		t.Fatal(err)
	}
	contact := AppendEntriesRequest{LeaderID: 2, Term: 2, Generation: 1}
	if err := core.Step(2, contact); err != nil {
		t.Fatal(err)
	}
	if got, want := core.ElectionDeadline(), uint64(16); got != want {
		t.Fatalf("leader contact deadline = %d, want current 2 plus sampled 14", got)
	}
	if got := core.Status().LeaderID; got != 2 {
		t.Fatalf("leader after contact = %d, want 2", got)
	}
	advanceReady(t, core)

	request := PreVoteRequest{CandidateID: 3, CurrentTerm: 2, ProspectiveTerm: 3}
	if err := core.Step(3, request); err != nil {
		t.Fatal(err)
	}
	ready := requireReady(t, core)
	response, ok := ready.Messages[0].RPC.(PreVoteResponse)
	if !ok || response.Granted {
		t.Fatalf("pre-vote response after recent leader contact = %#v, want denial", ready.Messages[0].RPC)
	}
	if ready.HardState != nil || ready.Messages[0].Requires != (DurabilityPrerequisite{}) {
		t.Fatalf("pre-vote denial unexpectedly requires persistence: %#v", ready)
	}
}

func TestPreVoteProspectiveTermNeverChangesDurableCurrentTerm(t *testing.T) {
	core := newElectionCore(t, 3, 1, HardState{Term: 5, VotedFor: 2}, nil, 5, 15, []uint64{10})
	request := PreVoteRequest{CandidateID: 3, CurrentTerm: 7, ProspectiveTerm: 8}
	if err := core.Step(3, request); err != nil {
		t.Fatal(err)
	}
	ready := requireReady(t, core)
	response := ready.Messages[0].RPC.(PreVoteResponse)
	if !response.Granted || response.Term != 5 || response.RequestCurrentTerm != 7 || response.ProspectiveTerm != 8 {
		t.Fatalf("pre-vote response = %#v, want term-separated grant", response)
	}
	if ready.HardState != nil {
		t.Fatalf("pre-vote emitted hard state %#v", *ready.HardState)
	}
	if got, want := core.HardState(), (HardState{Term: 5, VotedFor: 2}); got != want {
		t.Fatalf("pre-vote changed durable state to %#v, want %#v", got, want)
	}
}

func TestPreVoteLogFreshnessUsesLastTermThenIndex(t *testing.T) {
	tests := []struct {
		name      string
		current   uint64
		lastIndex uint64
		lastTerm  uint64
		granted   bool
	}{
		{name: "older term loses despite longer index", current: 3, lastIndex: 99, lastTerm: 2, granted: false},
		{name: "same term shorter loses", current: 3, lastIndex: 1, lastTerm: 3, granted: false},
		{name: "same position ties", current: 3, lastIndex: 2, lastTerm: 3, granted: true},
		{name: "same term longer wins", current: 3, lastIndex: 3, lastTerm: 3, granted: true},
		{name: "newer term wins despite shorter index", current: 4, lastIndex: 1, lastTerm: 4, granted: true},
	}
	entries := localFreshnessEntries(t)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			core := newElectionCore(t, 3, 1, HardState{Term: 3}, entries, 5, 15, []uint64{10})
			request := PreVoteRequest{
				CandidateID:     2,
				CurrentTerm:     test.current,
				ProspectiveTerm: test.current + 1,
				LastLogIndex:    test.lastIndex,
				LastLogTerm:     test.lastTerm,
			}
			if err := core.Step(2, request); err != nil {
				t.Fatal(err)
			}
			response := requireReady(t, core).Messages[0].RPC.(PreVoteResponse)
			if response.Granted != test.granted {
				t.Fatalf("pre-vote granted = %t, want %t", response.Granted, test.granted)
			}
		})
	}
}

func TestVoteLogFreshnessUsesLastTermThenIndex(t *testing.T) {
	tests := []struct {
		name      string
		lastIndex uint64
		lastTerm  uint64
		granted   bool
	}{
		{name: "older term loses despite longer index", lastIndex: 99, lastTerm: 2, granted: false},
		{name: "same term shorter loses", lastIndex: 1, lastTerm: 3, granted: false},
		{name: "same position ties", lastIndex: 2, lastTerm: 3, granted: true},
		{name: "same term longer wins", lastIndex: 3, lastTerm: 3, granted: true},
	}
	entries := localFreshnessEntries(t)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			core := newElectionCore(t, 3, 1, HardState{Term: 3}, entries, 5, 15, []uint64{10})
			request := RequestVoteRequest{CandidateID: 2, Term: 3, LastLogIndex: test.lastIndex, LastLogTerm: test.lastTerm}
			if err := core.Step(2, request); err != nil {
				t.Fatal(err)
			}
			response := requireReady(t, core).Messages[0].RPC.(RequestVoteResponse)
			if response.Granted != test.granted {
				t.Fatalf("vote granted = %t, want %t", response.Granted, test.granted)
			}
		})
	}
}

func TestElectionPersistsSelfVoteBeforeRequestingPeers(t *testing.T) {
	core := newElectionCore(t, 3, 1, HardState{Term: 4}, nil, 5, 15, []uint64{10, 10, 10})
	startPreVote(t, core, 5)
	grant := PreVoteResponse{ResponderID: 2, CandidateID: 1, Term: 4, RequestCurrentTerm: 4, ProspectiveTerm: 5, Granted: true}
	if err := core.Step(2, grant); err != nil {
		t.Fatal(err)
	}
	ready := requireReady(t, core)
	if got, want := core.Status().Role, RoleCandidate; got != want {
		t.Fatalf("role = %v, want %v", got, want)
	}
	if got, want := core.HardState(), (HardState{Term: 5, VotedFor: 1}); got != want {
		t.Fatalf("candidate hard state = %#v, want %#v", got, want)
	}
	if ready.HardState == nil || *ready.HardState != core.HardState() {
		t.Fatalf("Ready hard state = %#v, want current self vote %#v", ready.HardState, core.HardState())
	}
}

func TestVoteGrantsAtMostOncePerTermAndRetriesIdempotently(t *testing.T) {
	core := newElectionCore(t, 3, 1, HardState{Term: 1}, nil, 5, 15, []uint64{10})
	request := RequestVoteRequest{CandidateID: 2, Term: 2}
	if err := core.Step(2, request); err != nil {
		t.Fatal(err)
	}
	first := advanceReady(t, core)
	if !first.Messages[0].RPC.(RequestVoteResponse).Granted {
		t.Fatal("first vote was denied")
	}
	if first.HardState == nil || *first.HardState != (HardState{Term: 2, VotedFor: 2}) {
		t.Fatalf("first vote hard state = %#v", first.HardState)
	}

	if err := core.Step(2, request); err != nil {
		t.Fatal(err)
	}
	retry := advanceReady(t, core)
	if !retry.Messages[0].RPC.(RequestVoteResponse).Granted {
		t.Fatal("same-candidate retry was denied")
	}
	if retry.HardState != nil || retry.Messages[0].Requires != (DurabilityPrerequisite{}) {
		t.Fatalf("idempotent retry requested new durability: %#v", retry)
	}

	other := RequestVoteRequest{CandidateID: 3, Term: 2}
	if err := core.Step(3, other); err != nil {
		t.Fatal(err)
	}
	denial := requireReady(t, core)
	if denial.Messages[0].RPC.(RequestVoteResponse).Granted {
		t.Fatal("second candidate received a vote in the same term")
	}
	if denial.HardState != nil || core.HardState() != (HardState{Term: 2, VotedFor: 2}) {
		t.Fatalf("same-term denial changed durable vote: ready=%#v state=%#v", denial, core.HardState())
	}
}

func TestElectionSplitVoteRetriesOnlyAtNewSampledDeadline(t *testing.T) {
	core := newElectionCore(t, 3, 1, HardState{}, nil, 5, 15, []uint64{10, 10, 10, 9})
	startPreVote(t, core, 5)
	grant := PreVoteResponse{ResponderID: 2, CandidateID: 1, RequestCurrentTerm: 0, ProspectiveTerm: 1, Granted: true}
	if err := core.Step(2, grant); err != nil {
		t.Fatal(err)
	}
	advanceReady(t, core)
	if got, want := core.ElectionDeadline(), uint64(10); got != want {
		t.Fatalf("candidate retry deadline = %d, want %d", got, want)
	}
	if err := core.Tick(9); err != nil {
		t.Fatal(err)
	}
	if _, ok := core.Ready(); ok {
		t.Fatal("candidate retried before its sampled deadline")
	}
	if err := core.Tick(10); err != nil {
		t.Fatal(err)
	}
	ready := requireReady(t, core)
	if got, want := core.Status().Role, RolePreCandidate; got != want {
		t.Fatalf("role at retry deadline = %v, want %v", got, want)
	}
	for _, outbound := range ready.Messages {
		request := outbound.RPC.(PreVoteRequest)
		if request.CurrentTerm != 1 || request.ProspectiveTerm != 2 {
			t.Fatalf("retry pre-vote correlation = %#v, want current 1 prospective 2", request)
		}
	}
	if got, want := core.ElectionDeadline(), uint64(24); got != want {
		t.Fatalf("retry resampled deadline = %d, want %d", got, want)
	}
}

func TestPreVoteLowerTermGrantRoundTripsWithoutResponderDurableMutation(t *testing.T) {
	candidate := newElectionCore(t, 3, 1, HardState{Term: 7, VotedFor: 1}, nil, 5, 15, []uint64{10, 10, 10})
	preVoteReady := startPreVote(t, candidate, 5)
	request := outboundPreVoteRequestTo(t, preVoteReady, 2)
	if got, want := candidate.HardState(), (HardState{Term: 7, VotedFor: 1}); got != want {
		t.Fatalf("starting pre-vote changed candidate hard state to %#v, want %#v", got, want)
	}

	voter := newElectionCore(t, 3, 2, HardState{Term: 5, VotedFor: 3}, nil, 5, 15, []uint64{10})
	voterBefore := voter.HardState()
	if err := voter.Step(1, request); err != nil {
		t.Fatal(err)
	}
	voterReady := requireReady(t, voter)
	if voterReady.HardState != nil {
		t.Fatalf("pre-vote responder emitted hard state %#v", *voterReady.HardState)
	}
	response, ok := voterReady.Messages[0].RPC.(PreVoteResponse)
	if !ok || !response.Granted || response.Term != 5 || response.RequestCurrentTerm != 7 || response.ProspectiveTerm != 8 {
		t.Fatalf("lower-term pre-vote response = %#v, want correlated term-5 grant", voterReady.Messages[0].RPC)
	}
	if got := voter.HardState(); got != voterBefore {
		t.Fatalf("pre-vote request changed responder hard state from %#v to %#v", voterBefore, got)
	}

	if err := candidate.Step(2, response); err != nil {
		t.Fatal(err)
	}
	electionReady := requireReady(t, candidate)
	if got, want := candidate.Status().Role, RoleCandidate; got != want {
		t.Fatalf("role after lower-term grant = %v, want %v", got, want)
	}
	if electionReady.HardState == nil || *electionReady.HardState != (HardState{Term: 8, VotedFor: 1}) {
		t.Fatalf("real election hard state = %#v, want term 8 self vote", electionReady.HardState)
	}
}

func TestPreVoteProspectiveTermResponderDeniesAndForgedGrantCannotCount(t *testing.T) {
	candidate := newElectionCore(t, 3, 1, HardState{Term: 7}, nil, 5, 15, []uint64{10, 10, 10})
	preVoteReady := startPreVote(t, candidate, 5)
	request := outboundPreVoteRequestTo(t, preVoteReady, 2)

	prospectiveVoter := newElectionCore(t, 3, 2, HardState{Term: 8, VotedFor: 2}, nil, 5, 15, []uint64{10})
	voterBefore := prospectiveVoter.HardState()
	if err := prospectiveVoter.Step(1, request); err != nil {
		t.Fatal(err)
	}
	denialReady := requireReady(t, prospectiveVoter)
	denial := denialReady.Messages[0].RPC.(PreVoteResponse)
	if denial.Granted || denial.Term != 8 {
		t.Fatalf("prospective-term responder = %#v, want term-8 denial", denial)
	}
	if denialReady.HardState != nil || prospectiveVoter.HardState() != voterBefore {
		t.Fatalf("prospective-term pre-vote changed durable state: ready=%#v state=%#v", denialReady, prospectiveVoter.HardState())
	}

	before := snapshotCore(t, candidate)
	forged := denial
	forged.Granted = true
	if err := candidate.Step(2, forged); err != nil {
		t.Fatal(err)
	}
	assertCoreSnapshot(t, candidate, before)
}

func TestElectionDuplicateAndMisaddressedVotesDoNotFormQuorum(t *testing.T) {
	core := electedCandidateAtTerm(t, 5, 1, 1)
	before := snapshotCore(t, core)
	stale := RequestVoteResponse{ResponderID: 2, CandidateID: 1, Term: 1, RequestTerm: 1, Granted: true}
	if err := core.Step(2, stale); err != nil {
		t.Fatal(err)
	}
	assertCoreSnapshot(t, core, before)

	wrongCandidate := RequestVoteResponse{ResponderID: 2, CandidateID: 4, Term: 2, RequestTerm: 2, Granted: true}
	if err := core.Step(2, wrongCandidate); err != nil {
		t.Fatal(err)
	}
	assertCoreSnapshot(t, core, before)

	nonVoter := RequestVoteResponse{ResponderID: 9, CandidateID: 1, Term: 2, RequestTerm: 2, Granted: true}
	if err := core.Step(9, nonVoter); !errors.Is(err, ErrNotVoter) {
		t.Fatalf("non-voter response error = %v, want ErrNotVoter", err)
	}
	assertCoreSnapshot(t, core, before)

	grant := RequestVoteResponse{ResponderID: 2, CandidateID: 1, Term: 2, RequestTerm: 2, Granted: true}
	if err := core.Step(2, grant); err != nil {
		t.Fatal(err)
	}
	if got := core.Status().Role; got != RoleCandidate {
		t.Fatalf("one peer grant role = %v, want candidate", got)
	}
	if err := core.Step(2, grant); err != nil {
		t.Fatal(err)
	}
	if got := core.Status().Role; got != RoleCandidate {
		t.Fatalf("duplicate peer grant role = %v, want candidate", got)
	}
	if _, ok := core.Ready(); ok {
		t.Fatal("duplicate grant produced output or formed quorum")
	}

	grant.ResponderID = 3
	if err := core.Step(3, grant); err != nil {
		t.Fatal(err)
	}
	if got := core.Status().Role; got != RoleLeader {
		t.Fatalf("distinct majority role = %v, want leader", got)
	}
}

func TestVoteHigherTermResponseRequiresCurrentElectionCorrelation(t *testing.T) {
	core := electedCandidateAtTerm(t, 3, 1, 1)
	before := snapshotCore(t, core)
	futureElection := RequestVoteResponse{
		ResponderID: 2,
		CandidateID: 1,
		Term:        3,
		RequestTerm: 3,
		Granted:     true,
	}
	if err := core.Step(2, futureElection); err != nil {
		t.Fatal(err)
	}
	assertCoreSnapshot(t, core, before)
}

func TestHigherTermReplicationResponseRequiresActiveLeaderCorrelation(t *testing.T) {
	core := electedCandidate(t, 3, 1)
	grant := RequestVoteResponse{ResponderID: 2, CandidateID: 1, Term: 1, RequestTerm: 1, Granted: true}
	if err := core.Step(2, grant); err != nil {
		t.Fatal(err)
	}
	advanceReady(t, core)
	before := snapshotCore(t, core)
	futureRequest := AppendEntriesResponse{
		ResponderID:   2,
		LeaderID:      1,
		Term:          3,
		RequestTerm:   2,
		Generation:    1,
		Success:       false,
		ConflictIndex: 1,
	}
	if err := core.Step(2, futureRequest); err != nil {
		t.Fatal(err)
	}
	assertCoreSnapshot(t, core, before)
}

func TestHigherTermAppendResponseWithUnissuedGenerationCannotStepDown(t *testing.T) {
	core, issued := leaderWithNoOpRequest(t, 2)
	before := snapshotCore(t, core)
	forged := higherTermAppendRejection(issued, 2, 2)
	forged.Generation++
	if err := core.Step(2, forged); err != nil {
		t.Fatal(err)
	}
	assertCoreSnapshot(t, core, before)
}

func TestHigherTermIssuedAppendResponseStepsDown(t *testing.T) {
	core, issued := leaderWithNoOpRequest(t, 2)
	response := higherTermAppendRejection(issued, 2, 2)
	if err := core.Step(2, response); err != nil {
		t.Fatal(err)
	}
	ready := requireReady(t, core)
	if got := core.Status(); got.Role != RoleFollower || got.Term != 2 || got.LeaderID != 0 {
		t.Fatalf("status after issued higher response = %#v, want follower term 2", got)
	}
	if ready.HardState == nil || *ready.HardState != (HardState{Term: 2}) {
		t.Fatalf("issued higher response hard state = %#v, want term 2 cleared vote", ready.HardState)
	}
}

func TestDuplicateAppendGenerationCannotStepDown(t *testing.T) {
	core, issued := leaderWithNoOpRequest(t, 2)
	accepted := AppendEntriesResponse{
		ResponderID: 2,
		LeaderID:    issued.LeaderID,
		Term:        issued.Term,
		RequestTerm: issued.Term,
		Generation:  issued.Generation,
		Success:     true,
		MatchIndex:  issued.Entries[0].Index,
	}
	if err := core.Step(2, accepted); err != nil {
		t.Fatal(err)
	}
	commitReady := requireReady(t, core)
	if commitReady.HardState == nil || commitReady.HardState.CommitIndex != 1 {
		t.Fatalf("append acknowledgement commit Ready = %#v, want durable no-op commit", commitReady)
	}
	advanceReadyToken(t, core, commitReady)
	beforeDuplicate := snapshotCore(t, core)
	duplicate := higherTermAppendRejection(issued, 2, 2)
	if err := core.Step(2, duplicate); err != nil {
		t.Fatal(err)
	}
	assertCoreSnapshot(t, core, beforeDuplicate)
}

func TestHigherTermUnissuedSnapshotResponseCannotStepDown(t *testing.T) {
	core, _ := leaderWithNoOpRequest(t, 2)
	before := snapshotCore(t, core)
	response := InstallSnapshotResponse{
		ResponderID: 2,
		LeaderID:    1,
		Term:        2,
		RequestTerm: 1,
		TransferID:  TransferID{1},
		SnapshotID:  SnapshotID{1},
		Success:     false,
	}
	if err := core.Step(2, response); err != nil {
		t.Fatal(err)
	}
	assertCoreSnapshot(t, core, before)
}

func TestHigherTermRequestVoteStepsDownEveryRole(t *testing.T) {
	tests := []struct {
		name string
		core func(*testing.T) *Core
	}{
		{name: "follower", core: func(t *testing.T) *Core {
			return newElectionCore(t, 3, 1, HardState{Term: 1}, nil, 5, 15, []uint64{10})
		}},
		{name: "pre-candidate", core: func(t *testing.T) *Core {
			core := newElectionCore(t, 3, 1, HardState{Term: 1}, nil, 5, 15, []uint64{10, 10})
			startPreVote(t, core, 5)
			return core
		}},
		{name: "candidate", core: func(t *testing.T) *Core { return electedCandidate(t, 3, 1) }},
		{name: "leader", core: func(t *testing.T) *Core {
			core := electedCandidate(t, 3, 1)
			grant := RequestVoteResponse{ResponderID: 2, CandidateID: 1, Term: 1, RequestTerm: 1, Granted: true}
			if err := core.Step(2, grant); err != nil {
				t.Fatal(err)
			}
			advanceReady(t, core)
			return core
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			core := test.core(t)
			current := core.HardState().Term
			request := RequestVoteRequest{
				CandidateID:  2,
				Term:         current + 1,
				LastLogIndex: core.Status().LastIndex,
				LastLogTerm:  lastTermOfState(core.LogState()),
			}
			if request.LastLogIndex == 0 {
				request.LastLogTerm = 0
			}
			if err := core.Step(2, request); err != nil {
				t.Fatal(err)
			}
			ready := requireReady(t, core)
			if got := core.Status(); got.Role != RoleFollower || got.Term != current+1 || got.LeaderID != 0 {
				t.Fatalf("status after higher term = %#v, want follower term %d without leader", got, current+1)
			}
			if ready.HardState == nil || ready.HardState.Term != current+1 {
				t.Fatalf("higher-term Ready hard state = %#v, want term %d", ready.HardState, current+1)
			}
		})
	}
}

func TestHigherTermLeaderRequestsPersistTermAndResetContact(t *testing.T) {
	transferID := TransferID{1}
	snapshotID := SnapshotID{1}
	checksum := SnapshotChecksum{1}
	tests := []struct {
		name string
		rpc  RPC
	}{
		{name: "append entries", rpc: AppendEntriesRequest{LeaderID: 2, Term: 2, Generation: 1}},
		{name: "install snapshot", rpc: InstallSnapshotRequest{
			LeaderID:                  2,
			Term:                      2,
			TransferID:                transferID,
			SnapshotID:                snapshotID,
			LastIncludedIndex:         1,
			LastIncludedTerm:          1,
			StateMachineSchemaVersion: 1,
			Checksum:                  checksum,
			Done:                      true,
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			core := newElectionCore(t, 3, 1, HardState{Term: 1, VotedFor: 1}, nil, 5, 15, []uint64{10, 9})
			if err := core.Tick(2); err != nil {
				t.Fatal(err)
			}
			if err := core.Step(2, test.rpc); err != nil {
				t.Fatal(err)
			}
			ready := requireReady(t, core)
			if got, want := core.Status(), (Status{Role: RoleFollower, Term: 2, LeaderID: 2}); got != want {
				t.Fatalf("status = %#v, want %#v", got, want)
			}
			if ready.HardState == nil || *ready.HardState != (HardState{Term: 2}) {
				t.Fatalf("higher leader request hard state = %#v, want cleared vote in term 2", ready.HardState)
			}
			if got, want := core.ElectionDeadline(), uint64(16); got != want {
				t.Fatalf("leader contact deadline = %d, want %d", got, want)
			}
		})
	}
}

func TestHigherTermActiveResponsesPersistTermAndStepDown(t *testing.T) {
	tests := []struct {
		name      string
		candidate bool
		rpc       RPC
	}{
		{name: "request vote", candidate: true, rpc: RequestVoteResponse{
			ResponderID: 2,
			CandidateID: 1,
			Term:        2,
			RequestTerm: 1,
			Granted:     false,
		}},
		{name: "append entries", rpc: AppendEntriesResponse{
			ResponderID:   2,
			LeaderID:      1,
			Term:          2,
			RequestTerm:   1,
			Generation:    1,
			Success:       false,
			ConflictIndex: 1,
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			core := electedCandidate(t, 3, 1)
			if !test.candidate {
				grant := RequestVoteResponse{ResponderID: 2, CandidateID: 1, Term: 1, RequestTerm: 1, Granted: true}
				if err := core.Step(2, grant); err != nil {
					t.Fatal(err)
				}
				advanceReady(t, core)
			}
			if err := core.Step(2, test.rpc); err != nil {
				t.Fatal(err)
			}
			ready := requireReady(t, core)
			if got := core.Status(); got.Role != RoleFollower || got.Term != 2 || got.LeaderID != 0 {
				t.Fatalf("status after higher response = %#v, want follower term 2", got)
			}
			if ready.HardState == nil || *ready.HardState != (HardState{Term: 2}) {
				t.Fatalf("higher response hard state = %#v, want term 2 cleared vote", ready.HardState)
			}
		})
	}
}

func TestHigherTermLeaderContactAtLogicalMaximumAdoptsBeforeDeadlineExhaustion(t *testing.T) {
	tests := []struct {
		name string
		rpc  RPC
	}{
		{name: "append entries", rpc: AppendEntriesRequest{LeaderID: 2, Term: 2, Generation: 1}},
		{name: "install snapshot", rpc: InstallSnapshotRequest{
			LeaderID:                  2,
			Term:                      2,
			TransferID:                TransferID{1},
			SnapshotID:                SnapshotID{1},
			LastIncludedIndex:         1,
			LastIncludedTerm:          1,
			StateMachineSchemaVersion: 1,
			Checksum:                  SnapshotChecksum{1},
			Done:                      true,
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			core, _ := leaderWithNoOpRequest(t, 2)
			core.now = math.MaxUint64
			if err := core.Step(2, test.rpc); err != nil {
				t.Fatalf("higher-term contact at logical maximum error = %v", err)
			}
			ready := requireReady(t, core)
			if got := core.Status(); got.Role != RoleFollower || got.Term != 2 || got.LeaderID != 2 {
				t.Fatalf("status after exhausted contact = %#v, want follower term 2 leader 2", got)
			}
			if ready.HardState == nil || *ready.HardState != (HardState{Term: 2}) {
				t.Fatalf("exhausted contact hard state = %#v, want durable term 2 cleared vote", ready.HardState)
			}
			if got := core.ElectionDeadline(); got != math.MaxUint64 {
				t.Fatalf("exhausted election deadline = %d, want MaxUint64", got)
			}
			if len(ready.SnapshotActions) != 0 {
				if _, err := core.CompleteSnapshotAction(ready.Token, SnapshotActionResult{Rejected: true}); err != nil {
					t.Fatal(err)
				}
			}
			if err := core.Advance(ready.Token); err != nil {
				t.Fatal(err)
			}
			if err := core.Tick(math.MaxUint64); err != nil {
				t.Fatalf("exhausted follower Tick error = %v", err)
			}
			if _, ok := core.Ready(); ok {
				t.Fatal("deadline-exhausted follower campaigned")
			}
		})
	}
}

func TestHigherTermRequestVoteAtLogicalMaximumPersistsVoteAndExhaustsDeadline(t *testing.T) {
	core, _ := leaderWithNoOpRequest(t, 2)
	if err := core.Tick(math.MaxUint64); err != nil {
		t.Fatal(err)
	}
	request := RequestVoteRequest{CandidateID: 2, Term: 2, LastLogIndex: 1, LastLogTerm: 1}
	if err := core.Step(2, request); err != nil {
		t.Fatal(err)
	}
	ready := requireReady(t, core)
	if got := core.Status(); got.Role != RoleFollower || got.Term != 2 || got.LeaderID != 0 {
		t.Fatalf("status after exhausted vote request = %#v, want follower term 2", got)
	}
	if ready.HardState == nil || *ready.HardState != (HardState{Term: 2, VotedFor: 2}) {
		t.Fatalf("exhausted vote hard state = %#v, want durable term 2 vote 2", ready.HardState)
	}
	response := ready.Messages[0].RPC.(RequestVoteResponse)
	if !response.Granted || ready.Messages[0].Requires != (DurabilityPrerequisite{HardState: true}) {
		t.Fatalf("exhausted vote response/dependency = %#v / %#v", response, ready.Messages[0].Requires)
	}
	if got := core.ElectionDeadline(); got != math.MaxUint64 {
		t.Fatalf("exhausted vote deadline = %d, want MaxUint64", got)
	}
}

func TestHigherTermIssuedResponseAtLogicalMaximumAdoptsAndExhaustsDeadline(t *testing.T) {
	core, issued := leaderWithNoOpRequest(t, 2)
	core.now = math.MaxUint64
	response := higherTermAppendRejection(issued, 2, 2)
	if err := core.Step(2, response); err != nil {
		t.Fatal(err)
	}
	ready := requireReady(t, core)
	if got := core.Status(); got.Role != RoleFollower || got.Term != 2 || got.LeaderID != 0 {
		t.Fatalf("status after exhausted issued response = %#v, want follower term 2", got)
	}
	if ready.HardState == nil || *ready.HardState != (HardState{Term: 2}) {
		t.Fatalf("exhausted response hard state = %#v, want durable term 2", ready.HardState)
	}
	if got := core.ElectionDeadline(); got != math.MaxUint64 {
		t.Fatalf("exhausted response deadline = %d, want MaxUint64", got)
	}
	if err := core.Advance(ready.Token); err != nil {
		t.Fatal(err)
	}
	if err := core.Tick(math.MaxUint64); err != nil {
		t.Fatalf("exhausted response follower Tick error = %v", err)
	}
	if _, ok := core.Ready(); ok {
		t.Fatal("deadline-exhausted response follower campaigned")
	}
}

func TestHigherTermValidResponseStepsLeaderDownBeforeFurtherOutput(t *testing.T) {
	core := electedCandidate(t, 3, 1)
	grant := RequestVoteResponse{ResponderID: 2, CandidateID: 1, Term: 1, RequestTerm: 1, Granted: true}
	if err := core.Step(2, grant); err != nil {
		t.Fatal(err)
	}
	advanceReady(t, core)
	response := AppendEntriesResponse{
		ResponderID:   2,
		LeaderID:      1,
		Term:          2,
		RequestTerm:   1,
		Generation:    1,
		Success:       false,
		ConflictIndex: 1,
	}
	if err := core.Step(2, response); err != nil {
		t.Fatal(err)
	}
	ready := requireReady(t, core)
	if got := core.Status(); got.Role != RoleFollower || got.Term != 2 || got.LeaderID != 0 {
		t.Fatalf("status after higher response = %#v, want follower term 2", got)
	}
	if ready.HardState == nil || *ready.HardState != (HardState{Term: 2, CommitIndex: 0}) {
		t.Fatalf("higher response Ready hard state = %#v, want term 2 cleared vote", ready.HardState)
	}
}

func TestHigherPreVoteTermDoesNotStepLeaderDown(t *testing.T) {
	core := electedCandidate(t, 3, 1)
	grant := RequestVoteResponse{ResponderID: 2, CandidateID: 1, Term: 1, RequestTerm: 1, Granted: true}
	if err := core.Step(2, grant); err != nil {
		t.Fatal(err)
	}
	advanceReady(t, core)
	before := snapshotCore(t, core)
	request := PreVoteRequest{CandidateID: 2, CurrentTerm: 8, ProspectiveTerm: 9, LastLogIndex: 1, LastLogTerm: 1}
	if err := core.Step(2, request); err != nil {
		t.Fatal(err)
	}
	ready := requireReady(t, core)
	response := ready.Messages[0].RPC.(PreVoteResponse)
	if response.Granted {
		t.Fatal("leader granted higher prospective pre-vote")
	}
	if got := core.Status(); got != before.status {
		t.Fatalf("higher pre-vote changed leader status from %#v to %#v", before.status, got)
	}
	if got := core.HardState(); got != before.hardState {
		t.Fatalf("higher pre-vote changed hard state from %#v to %#v", before.hardState, got)
	}
}

func TestElectionTermOverflowFailsClosed(t *testing.T) {
	core := newElectionCore(t, 3, 1, HardState{Term: math.MaxUint64}, nil, 5, 15, []uint64{10})
	before := snapshotCore(t, core)
	if err := core.Tick(5); !errors.Is(err, ErrTermOverflow) {
		t.Fatalf("Tick(max term) error = %v, want ErrTermOverflow", err)
	}
	assertCoreSnapshot(t, core, before)
}

func TestLeadershipRequiresExactMajorityForThreeAndFiveVoters(t *testing.T) {
	for _, voterCount := range []int{3, 5} {
		t.Run(string(rune('0'+voterCount))+" voters", func(t *testing.T) {
			core := electedCandidate(t, voterCount, 1)
			neededPeerVotes := voterCount / 2
			for peerVote := 1; peerVote <= neededPeerVotes; peerVote++ {
				peerID := uint16(peerVote + 1)
				response := RequestVoteResponse{ResponderID: peerID, CandidateID: 1, Term: 1, RequestTerm: 1, Granted: true}
				if err := core.Step(peerID, response); err != nil {
					t.Fatal(err)
				}
				want := RoleCandidate
				if peerVote == neededPeerVotes {
					want = RoleLeader
				}
				if got := core.Status().Role; got != want {
					t.Fatalf("role after %d peer votes = %v, want %v", peerVote, got, want)
				}
			}
		})
	}
}

func TestLeadershipAppendsExactlyOneCurrentTermNoOpOnDuplicateVotes(t *testing.T) {
	core := electedCandidate(t, 3, 1)
	grant := RequestVoteResponse{ResponderID: 2, CandidateID: 1, Term: 1, RequestTerm: 1, Granted: true}
	if err := core.Step(2, grant); err != nil {
		t.Fatal(err)
	}
	ready := advanceReady(t, core)
	if len(ready.Entries) != 1 || ready.Entries[0].Kind != EntryNoOp || ready.Entries[0].Term != 1 || len(ready.Entries[0].CommandBytes()) != 0 {
		t.Fatalf("leadership entries = %#v, want exactly one command-free current-term no-op", ready.Entries)
	}
	if err := core.Step(2, grant); err != nil {
		t.Fatal(err)
	}
	if _, ok := core.Ready(); ok {
		t.Fatal("duplicate vote after leadership emitted another Ready")
	}
	state := core.LogState()
	if state.SnapshotIndex != 0 || len(state.Entries) != 1 || state.Entries[0].Kind != EntryNoOp {
		t.Fatalf("log after duplicate vote = %#v, want one no-op", state)
	}
}

func TestLeadershipProposalRemainsFencedBeforeCurrentTermCommit(t *testing.T) {
	core := electedCandidate(t, 3, 1)
	grant := RequestVoteResponse{ResponderID: 2, CandidateID: 1, Term: 1, RequestTerm: 1, Granted: true}
	if err := core.Step(2, grant); err != nil {
		t.Fatal(err)
	}
	advanceReady(t, core)
	before := snapshotCore(t, core)
	if _, err := core.ProposeEntry([]byte("must-not-append")); !errors.Is(err, ErrLeadershipNotAuthorized) {
		t.Fatalf("ProposeEntry before current-term commit error = %v, want ErrLeadershipNotAuthorized", err)
	}
	assertCoreSnapshot(t, core, before)
}

func TestElectionGeneratedRequestsUseCanonicalSchemasAndCorrelation(t *testing.T) {
	core := newElectionCore(t, 3, 1, HardState{}, nil, 5, 15, []uint64{10, 10, 10})
	preVote := startPreVote(t, core, 5)
	assertCanonicalOutbound(t, core, preVote, 1)
	for _, outbound := range preVote.Messages {
		if outbound.Requires != (DurabilityPrerequisite{}) {
			t.Fatalf("non-durable pre-vote prerequisite = %#v, want none", outbound.Requires)
		}
	}
	grantPreVote := PreVoteResponse{ResponderID: 2, CandidateID: 1, RequestCurrentTerm: 0, ProspectiveTerm: 1, Granted: true}
	if err := core.Step(2, grantPreVote); err != nil {
		t.Fatal(err)
	}
	vote := advanceReady(t, core)
	assertCanonicalOutbound(t, core, vote, 1)
	grantVote := RequestVoteResponse{ResponderID: 2, CandidateID: 1, Term: 1, RequestTerm: 1, Granted: true}
	if err := core.Step(2, grantVote); err != nil {
		t.Fatal(err)
	}
	appendReady := requireReady(t, core)
	assertCanonicalOutbound(t, core, appendReady, 1)
}

type coreSnapshot struct {
	status    Status
	hardState HardState
	deadline  uint64
	log       LogState
	ready     Ready
	hasReady  bool
}

func snapshotCore(t *testing.T, core *Core) coreSnapshot {
	t.Helper()
	ready, ok := core.Ready()
	return coreSnapshot{
		status:    core.Status(),
		hardState: core.HardState(),
		deadline:  core.ElectionDeadline(),
		log:       core.LogState(),
		ready:     ready,
		hasReady:  ok,
	}
}

func assertCoreSnapshot(t *testing.T, core *Core, want coreSnapshot) {
	t.Helper()
	got := snapshotCore(t, core)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("core state changed:\n got=%#v\nwant=%#v", got, want)
	}
}

func localFreshnessEntries(t *testing.T) []Entry {
	t.Helper()
	first, err := NewEntry(1, 2, EntryCommand, []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewEntry(2, 3, EntryCommand, []byte("two"))
	if err != nil {
		t.Fatal(err)
	}
	return []Entry{first, second}
}

func electedCandidateAtTerm(t *testing.T, voterCount int, localID uint16, currentTerm uint64) *Core {
	t.Helper()
	core := newElectionCore(t, voterCount, localID, HardState{Term: currentTerm}, nil, 5, 15, []uint64{10, 10, 10, 10})
	startPreVote(t, core, 5)
	needed := voterCount / 2
	for peer := 2; peer <= needed; peer++ {
		response := PreVoteResponse{
			ResponderID:        uint16(peer),
			CandidateID:        localID,
			Term:               currentTerm,
			RequestCurrentTerm: currentTerm,
			ProspectiveTerm:    currentTerm + 1,
			Granted:            true,
		}
		if err := core.Step(response.ResponderID, response); err != nil {
			t.Fatal(err)
		}
	}
	response := PreVoteResponse{
		ResponderID:        uint16(needed + 1),
		CandidateID:        localID,
		Term:               currentTerm,
		RequestCurrentTerm: currentTerm,
		ProspectiveTerm:    currentTerm + 1,
		Granted:            true,
	}
	if err := core.Step(response.ResponderID, response); err != nil {
		t.Fatal(err)
	}
	advanceReady(t, core)
	return core
}

func lastTermOfState(state LogState) uint64 {
	if len(state.Entries) == 0 {
		return state.SnapshotTerm
	}
	return state.Entries[len(state.Entries)-1].Term
}

func assertCanonicalOutbound(t *testing.T, core *Core, ready Ready, senderID uint16) {
	t.Helper()
	voters := core.Voters()
	for _, outbound := range ready.Messages {
		if outbound.To == senderID || !voters.Contains(outbound.To) {
			t.Fatalf("outbound recipient = %d, want configured peer", outbound.To)
		}
		if err := ValidateRPCSender(outbound.RPC, senderID, voters); err != nil {
			t.Fatalf("outbound %T sender correlation: %v", outbound.RPC, err)
		}
		if _, _, err := EncodeRPC(outbound.RPC, DefaultCodecLimits()); err != nil {
			t.Fatalf("outbound %T canonical encoding: %v", outbound.RPC, err)
		}
	}
}

func outboundPreVoteRequestTo(t *testing.T, ready Ready, peerID uint16) PreVoteRequest {
	t.Helper()
	for _, outbound := range ready.Messages {
		if outbound.To == peerID {
			request, ok := outbound.RPC.(PreVoteRequest)
			if !ok {
				t.Fatalf("outbound to %d = %T, want PreVoteRequest", peerID, outbound.RPC)
			}
			return request
		}
	}
	t.Fatalf("no pre-vote request to peer %d in %#v", peerID, ready.Messages)
	return PreVoteRequest{}
}

func leaderWithNoOpRequest(t *testing.T, peerID uint16) (*Core, AppendEntriesRequest) {
	t.Helper()
	core := electedCandidate(t, 3, 1)
	grant := RequestVoteResponse{ResponderID: 2, CandidateID: 1, Term: 1, RequestTerm: 1, Granted: true}
	if err := core.Step(2, grant); err != nil {
		t.Fatal(err)
	}
	ready := advanceReady(t, core)
	for _, outbound := range ready.Messages {
		if outbound.To == peerID {
			request, ok := outbound.RPC.(AppendEntriesRequest)
			if !ok {
				t.Fatalf("outbound to %d = %T, want AppendEntriesRequest", peerID, outbound.RPC)
			}
			return core, request
		}
	}
	t.Fatalf("no append request to peer %d in %#v", peerID, ready.Messages)
	return nil, AppendEntriesRequest{}
}

func higherTermAppendRejection(request AppendEntriesRequest, responderID uint16, term uint64) AppendEntriesResponse {
	return AppendEntriesResponse{
		ResponderID:   responderID,
		LeaderID:      request.LeaderID,
		Term:          term,
		RequestTerm:   request.Term,
		Generation:    request.Generation,
		Success:       false,
		ConflictIndex: 1,
	}
}
