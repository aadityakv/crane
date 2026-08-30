package raft

import (
	"errors"
	"reflect"
	"testing"
)

func TestCommitRequiresExactMajorityForThreeAndFiveVoters(t *testing.T) {
	for _, voterCount := range []int{3, 5} {
		t.Run(voterCountName(voterCount), func(t *testing.T) {
			core, requests := electEmptyLeader(t, voterCount)
			neededPeers := voterCount / 2
			for peer := 2; peer <= neededPeers+1; peer++ {
				request := requests[uint16(peer)]
				if err := core.Step(uint16(peer), appendSuccess(request, uint16(peer))); err != nil {
					t.Fatal(err)
				}
				if peer <= neededPeers {
					if got := core.Status().CommitIndex; got != 0 {
						t.Fatalf("%d voters committed with self plus %d peers: %d", voterCount, peer-1, got)
					}
					if _, ok := core.Ready(); ok {
						t.Fatal("minority acknowledgement emitted commit work")
					}
					continue
				}
				ready := requireReady(t, core)
				if got := core.Status().CommitIndex; got != 1 {
					t.Fatalf("%d-voter majority commit = %d, want 1", voterCount, got)
				}
				if ready.HardState == nil || ready.HardState.CommitIndex != 1 {
					t.Fatalf("majority Ready hard state = %#v, want durable commit 1", ready.HardState)
				}
			}
		})
	}
}

func TestCommitDoesNotCountOlderTermEntryDirectlyButCommitsItThroughCurrentTermEntry(t *testing.T) {
	entries := testEntries(t, testEntrySpec{term: 1, command: "older"})
	core, initial := electReplicationLeaderWithOptions(t, 0, 0, HardState{Term: 1}, entries, 1, 0)
	rejectInitialAppend(t, core, initial, 1)
	olderRequest := appendRequestTo(t, requireReady(t, core), 2)
	advanceReady(t, core)
	if len(olderRequest.Entries) != 1 || olderRequest.Entries[0].Index != 1 || olderRequest.Entries[0].Term != 1 {
		t.Fatalf("older retry = %#v, want exact term-1 entry at index 1", olderRequest)
	}

	if err := core.Step(2, appendSuccess(olderRequest, 2)); err != nil {
		t.Fatal(err)
	}
	currentRequestReady := requireReady(t, core)
	if currentRequestReady.HardState != nil || core.Status().CommitIndex != 0 {
		t.Fatalf("older-term majority committed directly: status=%#v ready=%#v", core.Status(), currentRequestReady)
	}
	currentRequest := appendRequestTo(t, currentRequestReady, 2)
	if len(currentRequest.Entries) != 1 || currentRequest.Entries[0].Index != 2 || currentRequest.Entries[0].Term != 2 || currentRequest.Entries[0].Kind != EntryNoOp {
		t.Fatalf("current-term retry = %#v, want term-2 no-op at index 2", currentRequest)
	}
	advanceReadyToken(t, core, currentRequestReady)

	if err := core.Step(2, appendSuccess(currentRequest, 2)); err != nil {
		t.Fatal(err)
	}
	ready := requireReady(t, core)
	if got := core.Status().CommitIndex; got != 2 {
		t.Fatalf("current-term majority commit = %d, want indirect prefix through 2", got)
	}
	if ready.HardState == nil || ready.HardState.CommitIndex != 2 {
		t.Fatalf("indirect commit hard state = %#v, want commit 2", ready.HardState)
	}
}

func TestCommitReadyOwnsOrderedEntriesAndAdvanceEmitsThemOnce(t *testing.T) {
	entries := testEntries(t, testEntrySpec{term: 1, command: "older"})
	core, initial := electReplicationLeader(t, 0, 0, HardState{Term: 1}, entries)
	if err := core.Step(2, appendSuccess(initial, 2)); err != nil {
		t.Fatal(err)
	}
	ready := requireReady(t, core)
	if len(ready.CommittedEntries) != 2 {
		t.Fatalf("committed entries = %#v, want older command and current no-op", ready.CommittedEntries)
	}
	if ready.CommittedEntries[0].Index != 1 || ready.CommittedEntries[0].Kind != EntryCommand || string(ready.CommittedEntries[0].CommandBytes()) != "older" {
		t.Fatalf("first committed entry = %#v, want owned older command", ready.CommittedEntries[0])
	}
	if ready.CommittedEntries[1].Index != 2 || ready.CommittedEntries[1].Kind != EntryNoOp || len(ready.CommittedEntries[1].CommandBytes()) != 0 {
		t.Fatalf("second committed entry = %#v, want command-free no-op", ready.CommittedEntries[1])
	}
	ready.CommittedEntries[0].command[0] = 'X'
	reread := requireReady(t, core)
	if string(reread.CommittedEntries[0].CommandBytes()) != "older" {
		t.Fatal("mutating committed output changed live Ready ownership")
	}
	advanceReadyToken(t, core, reread)
	if got := core.Status().AppliedIndex; got != 2 {
		t.Fatalf("applied index after owner Advance = %d, want 2", got)
	}
	if _, ok := core.Ready(); ok {
		t.Fatal("committed entries repeated after Advance")
	}
	before := core.LogState()
	if err := core.Step(2, appendSuccess(initial, 2)); err != nil {
		t.Fatal(err)
	}
	if got := core.LogState(); !reflect.DeepEqual(got, before) {
		t.Fatalf("duplicate consumed success changed log state:\n got=%#v\nwant=%#v", got, before)
	}
	if _, ok := core.Ready(); ok {
		t.Fatal("duplicate consumed success re-emitted committed entries")
	}
}

func TestProposalDuplicateCommandsReceiveDistinctExactCommittedIdentities(t *testing.T) {
	core, authorityReady := authorizedLeader(t)
	if len(authorityReady.CommittedProposals) != 0 || len(authorityReady.CommittedEntries) != 1 || authorityReady.CommittedEntries[0].Kind != EntryNoOp {
		t.Fatalf("leadership no-op outputs = %#v / %#v, want no proposal handoff", authorityReady.CommittedEntries, authorityReady.CommittedProposals)
	}

	var identities []ProposalID
	for wantIndex := uint64(2); wantIndex <= 3; wantIndex++ {
		entry, err := core.ProposeEntry([]byte("duplicate-command"))
		if err != nil {
			t.Fatal(err)
		}
		if entry.Index != wantIndex || entry.Term != 1 || entry.Kind != EntryCommand || string(entry.CommandBytes()) != "duplicate-command" {
			t.Fatalf("proposed entry = %#v, want exact command at index %d", entry, wantIndex)
		}
		persistReady := requireReady(t, core)
		if len(persistReady.Entries) != 1 || !sameEntry(persistReady.Entries[0], entry) {
			t.Fatalf("proposal unstable entries = %#v, want exact appended entry", persistReady.Entries)
		}
		request := appendRequestTo(t, persistReady, 2)
		if got, want := messageTo(t, persistReady, 2).Requires, (DurabilityPrerequisite{EntriesThrough: wantIndex}); got != want {
			t.Fatalf("proposal replication prerequisite = %#v, want %#v", got, want)
		}
		advanceReadyToken(t, core, persistReady)
		if err := core.Step(2, appendSuccess(request, 2)); err != nil {
			t.Fatal(err)
		}
		commitReady := requireReady(t, core)
		if len(commitReady.CommittedProposals) != 1 {
			t.Fatalf("committed proposal handoffs = %#v, want one exact record", commitReady.CommittedProposals)
		}
		handoff := commitReady.CommittedProposals[0]
		if handoff.ID == 0 || !sameEntry(handoff.Entry, entry) {
			t.Fatalf("committed proposal = %#v, want nonzero identity bound to %#v", handoff, entry)
		}
		if len(commitReady.CommittedEntries) != 1 || !sameEntry(commitReady.CommittedEntries[0], entry) {
			t.Fatalf("committed entries = %#v, want exact proposal entry", commitReady.CommittedEntries)
		}
		commitReady.CommittedProposals[0].Entry.command[0] = 'X'
		if reread := requireReady(t, core); !sameEntry(reread.CommittedProposals[0].Entry, entry) {
			t.Fatal("mutating committed proposal handoff changed live Ready ownership")
		}
		identities = append(identities, handoff.ID)
		advanceReadyToken(t, core, commitReady)
	}
	if identities[0] == identities[1] {
		t.Fatalf("duplicate commands reused proposal identity %d", identities[0])
	}
}

func TestBarrierProposalTracksAnExactNoOpIdentity(t *testing.T) {
	core, _ := authorizedLeader(t)
	proposalID, entry, err := core.proposeTracked(EntryNoOp, nil)
	if err != nil {
		t.Fatal(err)
	}
	if proposalID == 0 || entry.Index != 2 || entry.Term != 1 || entry.Kind != EntryNoOp || len(entry.CommandBytes()) != 0 {
		t.Fatalf("tracked barrier = id %d entry %#v, want exact term-1 no-op at index 2", proposalID, entry)
	}
	persistReady := advanceReady(t, core)
	request := appendRequestTo(t, persistReady, 2)
	if err := core.Step(2, appendSuccess(request, 2)); err != nil {
		t.Fatal(err)
	}
	commitReady := requireReady(t, core)
	if len(commitReady.CommittedProposals) != 1 || commitReady.CommittedProposals[0].ID != proposalID || !sameEntry(commitReady.CommittedProposals[0].Entry, entry) {
		t.Fatalf("committed barriers = %#v, want exact identity %d", commitReady.CommittedProposals, proposalID)
	}
	if len(commitReady.CommittedEntries) != 1 || commitReady.CommittedEntries[0].Kind != EntryNoOp {
		t.Fatalf("committed entries = %#v, want barrier no-op", commitReady.CommittedEntries)
	}
}

func TestProposalOverwriteFailsExactIdentityAndCannotCrossComplete(t *testing.T) {
	core, _ := authorizedLeader(t)
	proposed, err := core.ProposeEntry([]byte("original"))
	if err != nil {
		t.Fatal(err)
	}
	advanceReady(t, core)
	replacement := testEntriesFrom(t, proposed.Index, testEntrySpec{term: 2, command: "replacement"})
	request := AppendEntriesRequest{
		LeaderID: 2, Term: 2, Generation: 41,
		PrevLogIndex: 1, PrevLogTerm: 1,
		LeaderCommit: 2, Entries: replacement,
	}
	if err := core.Step(2, request); err != nil {
		t.Fatal(err)
	}
	ready := requireReady(t, core)
	if len(ready.FailedProposals) != 1 {
		t.Fatalf("failed proposals = %#v, want exact overwritten record", ready.FailedProposals)
	}
	failure := ready.FailedProposals[0]
	if failure.ID == 0 || !sameEntry(failure.Entry, proposed) || !errors.Is(failure.Err, ErrProposalFailed) {
		t.Fatalf("proposal failure = %#v, want original entry and classified failure", failure)
	}
	if len(ready.CommittedProposals) != 0 {
		t.Fatalf("replacement cross-completed original proposal: %#v", ready.CommittedProposals)
	}
	if len(ready.CommittedEntries) != 1 || !sameEntry(ready.CommittedEntries[0], replacement[0]) {
		t.Fatalf("replacement committed entries = %#v, want only later occupant", ready.CommittedEntries)
	}
	ready.FailedProposals[0].Entry.command[0] = 'X'
	if reread := requireReady(t, core); !sameEntry(reread.FailedProposals[0].Entry, proposed) {
		t.Fatal("mutating failed proposal record changed live Ready ownership")
	}
	response := appendResponseFromReady(t, Ready{Messages: []PeerMessage{messageTo(t, ready, 2)}})
	if !response.Success || response.MatchIndex != 2 {
		t.Fatalf("replacement response = %#v, want success through replacement", response)
	}
}

func TestProposalHigherTermResponseFailsPendingWithoutCompletingIt(t *testing.T) {
	core, _ := authorizedLeader(t)
	proposed, err := core.ProposeEntry([]byte("pending"))
	if err != nil {
		t.Fatal(err)
	}
	persistReady := advanceReady(t, core)
	issued := appendRequestTo(t, persistReady, 2)
	higher := AppendEntriesResponse{
		ResponderID: 2, LeaderID: 1, Term: 2, RequestTerm: 1,
		Generation: issued.Generation, Success: false, ConflictIndex: 1,
	}
	if err := core.Step(2, higher); err != nil {
		t.Fatal(err)
	}
	ready := requireReady(t, core)
	if ready.HardState == nil || ready.HardState.Term != 2 || len(ready.FailedProposals) != 1 || !sameEntry(ready.FailedProposals[0].Entry, proposed) {
		t.Fatalf("higher-term proposal failure Ready = %#v, want durable term and exact failure", ready)
	}
	if len(ready.CommittedProposals) != 0 {
		t.Fatalf("higher term completed pending proposal: %#v", ready.CommittedProposals)
	}
}

func electEmptyLeader(t *testing.T, voterCount int) (*Core, map[uint16]AppendEntriesRequest) {
	t.Helper()
	core := electedCandidate(t, voterCount, 1)
	for peer := 2; peer <= voterCount/2+1; peer++ {
		response := RequestVoteResponse{ResponderID: uint16(peer), CandidateID: 1, Term: 1, RequestTerm: 1, Granted: true}
		if err := core.Step(uint16(peer), response); err != nil {
			t.Fatal(err)
		}
	}
	ready := advanceReady(t, core)
	requests := make(map[uint16]AppendEntriesRequest, voterCount-1)
	for _, message := range ready.Messages {
		request, ok := message.RPC.(AppendEntriesRequest)
		if !ok {
			t.Fatalf("leader outbound = %T, want AppendEntriesRequest", message.RPC)
		}
		requests[message.To] = request
	}
	return core, requests
}

func authorizedLeader(t *testing.T) (*Core, Ready) {
	t.Helper()
	core, requests := electEmptyLeader(t, 3)
	if err := core.Step(2, appendSuccess(requests[2], 2)); err != nil {
		t.Fatal(err)
	}
	ready := requireReady(t, core)
	advanceReadyToken(t, core, ready)
	if core.Status().CommitIndex != 1 || core.Status().AppliedIndex != 1 {
		t.Fatalf("authorized leader status = %#v, want committed/applied no-op", core.Status())
	}
	return core, ready
}

func messageTo(t *testing.T, ready Ready, peerID uint16) PeerMessage {
	t.Helper()
	for _, message := range ready.Messages {
		if message.To == peerID {
			return message
		}
	}
	t.Fatalf("Ready has no message to peer %d: %#v", peerID, ready.Messages)
	return PeerMessage{}
}

func voterCountName(count int) string {
	if count == 3 {
		return "three voters"
	}
	return "five voters"
}
