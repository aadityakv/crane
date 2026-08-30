package raft

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
)

func TestInstallSnapshotStoreFsyncsExactChunksAcceptsIdenticalDuplicateAndInstalls(t *testing.T) {
	identity, voters := testStorageIdentity(t, 1)
	factories := []struct {
		name string
		open func(*testing.T) StableStore
	}{
		{name: "memory", open: func(t *testing.T) StableStore {
			store, err := NewMemoryStore(identity, voters)
			if err != nil {
				t.Fatal(err)
			}
			return store
		}},
		{name: "file", open: func(t *testing.T) StableStore {
			store, err := OpenFileStore(t.TempDir(), identity, voters)
			if err != nil {
				t.Fatal(err)
			}
			return store
		}},
	}
	for _, factory := range factories {
		t.Run(factory.name, func(t *testing.T) {
			store := factory.open(t)
			defer store.Close()
			seedInstallStore(t, store)
			stager := store.(interface {
				StageSnapshotChunk(InstallSnapshotRequest) (SnapshotStageResult, error)
			})
			first, final := installRequests(t, identity)
			progress, err := stager.StageSnapshotChunk(first)
			if err != nil || progress.NextOffset != 3 || progress.Done {
				t.Fatalf("first chunk = %#v, %v", progress, err)
			}
			duplicate, err := stager.StageSnapshotChunk(first)
			if err != nil || duplicate.NextOffset != 3 || duplicate.Done {
				t.Fatalf("duplicate chunk = %#v, %v", duplicate, err)
			}
			completed, err := stager.StageSnapshotChunk(final)
			if err != nil || !completed.Done || completed.NextOffset != 5 {
				t.Fatalf("final chunk = %#v, %v", completed, err)
			}
			if completed.State.Snapshot == nil || string(completed.State.Snapshot.StateBytes()) != "abcde" || completed.State.SnapshotBase.LastIncludedIndex != 2 {
				t.Fatalf("installed state = %+v", completed.State)
			}
			if completed.State.HardState.CommitIndex != 2 || completed.State.AppliedIndex != 2 || len(completed.State.Entries) != 1 || completed.State.Entries[0].Index != 3 {
				t.Fatalf("installed suffix/indices = %+v", completed.State)
			}
		})
	}
}

func TestInstallSnapshotMismatchingIncludedTermDiscardsLocalSuffix(t *testing.T) {
	identity, voters := testStorageIdentity(t, 1)
	store, err := NewMemoryStore(identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	seedInstallStore(t, store)
	snapshot, err := NewSnapshot(identity, SnapshotMetadata{LastIncludedIndex: 2, LastIncludedTerm: 3, StateMachineSchemaVersion: 6}, []byte("new"), 64)
	if err != nil {
		t.Fatal(err)
	}
	request := InstallSnapshotRequest{LeaderID: 2, Term: 3, TransferID: TransferID{3}, SnapshotID: snapshot.ID,
		LastIncludedIndex: 2, LastIncludedTerm: 3, StateMachineSchemaVersion: 6,
		TotalLength: 3, Checksum: snapshot.StateChecksum, Chunk: []byte("new"), Done: true}
	installed, err := store.StageSnapshotChunk(request)
	if err != nil {
		t.Fatal(err)
	}
	if !installed.Done || installed.State.SnapshotBase != snapshot.Metadata || len(installed.State.Entries) != 0 ||
		installed.State.HardState.CommitIndex != 2 || installed.State.AppliedIndex != 2 {
		t.Fatalf("mismatching suffix install = %+v", installed.State)
	}
}

func TestInstallSnapshotStoreRejectsAndCleansGapOverlapChangedDuplicateAndChecksum(t *testing.T) {
	identity, voters := testStorageIdentity(t, 1)
	tests := []struct {
		name   string
		mutate func(first, final InstallSnapshotRequest) []InstallSnapshotRequest
	}{
		{name: "gap", mutate: func(first, _ InstallSnapshotRequest) []InstallSnapshotRequest {
			first.Offset = 1
			return []InstallSnapshotRequest{first}
		}},
		{name: "partial overlap", mutate: func(first, final InstallSnapshotRequest) []InstallSnapshotRequest {
			final.Offset = 2
			final.Chunk = []byte("cde")
			return []InstallSnapshotRequest{first, final}
		}},
		{name: "changed duplicate", mutate: func(first, _ InstallSnapshotRequest) []InstallSnapshotRequest {
			changed := first
			changed.Chunk = []byte("abX")
			return []InstallSnapshotRequest{first, changed}
		}},
		{name: "metadata change", mutate: func(first, final InstallSnapshotRequest) []InstallSnapshotRequest {
			final.LastIncludedTerm++
			return []InstallSnapshotRequest{first, final}
		}},
		{name: "checksum", mutate: func(first, final InstallSnapshotRequest) []InstallSnapshotRequest {
			first.Checksum[0]++
			final.Checksum = first.Checksum
			return []InstallSnapshotRequest{first, final}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, err := NewMemoryStore(identity, voters)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			seedInstallStore(t, store)
			first, final := installRequests(t, identity)
			requests := test.mutate(first, final)
			for index, request := range requests {
				_, err = store.StageSnapshotChunk(request)
				if index != len(requests)-1 && err != nil {
					t.Fatalf("setup chunk %d: %v", index, err)
				}
			}
			if !errors.Is(err, ErrSnapshotRejected) {
				t.Fatalf("last chunk error = %v, want ErrSnapshotRejected", err)
			}
			continuation := final
			continuation.Offset = 3
			if _, err := store.StageSnapshotChunk(continuation); !errors.Is(err, ErrSnapshotRejected) {
				t.Fatalf("continuation after cleanup error = %v, want ErrSnapshotRejected", err)
			}
			state, err := store.Recover()
			if err != nil {
				t.Fatal(err)
			}
			if state.Snapshot != nil || state.SnapshotBase.LastIncludedIndex != 0 || state.HardState.CommitIndex != 1 {
				t.Fatalf("rejected transfer mutated durable state: %+v", state)
			}
		})
	}
}

func TestInstallSnapshotMemoryFaultsNeverAcknowledgeUndurableOffsetOrInstall(t *testing.T) {
	identity, voters := testStorageIdentity(t, 1)
	for _, operation := range []StorageOperation{StorageOperationSnapshotStageWrite, StorageOperationSnapshotStageSync} {
		t.Run(fmt.Sprint(operation), func(t *testing.T) {
			store, err := NewMemoryStore(identity, voters)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			seedInstallStore(t, store)
			first, final := installRequests(t, identity)
			store.FailNext(operation, errors.New("injected stage boundary"))
			if progress, err := store.StageSnapshotChunk(first); err == nil || progress.NextOffset != 0 {
				t.Fatalf("faulted stage acknowledged progress %#v, %v", progress, err)
			}
			if _, err := store.StageSnapshotChunk(final); !errors.Is(err, ErrSnapshotRejected) {
				t.Fatalf("faulted stage retained continuation: %v", err)
			}
		})
	}

	store, err := NewMemoryStore(identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	seedInstallStore(t, store)
	first, final := installRequests(t, identity)
	if _, err := store.StageSnapshotChunk(first); err != nil {
		t.Fatal(err)
	}
	store.FailNext(StorageOperationSnapshotInstall, errors.New("injected install boundary"))
	if result, err := store.StageSnapshotChunk(final); err == nil || result.Done {
		t.Fatalf("faulted install acknowledged completion %#v, %v", result, err)
	}
	state, err := store.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if state.Snapshot != nil || state.HardState.CommitIndex != 1 {
		t.Fatalf("faulted install mutated state: %+v", state)
	}
}

func TestInstallSnapshotCrashBetweenSnapshotAndWALRecoversExactOldPairOnSuffixMismatch(t *testing.T) {
	directory := t.TempDir()
	identity, voters := testStorageIdentity(t, 1)
	store, err := OpenFileStore(directory, identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	seedInstallStore(t, store)
	oldSnapshot, err := NewSnapshot(identity, SnapshotMetadata{LastIncludedIndex: 1, LastIncludedTerm: 1, StateMachineSchemaVersion: 5}, []byte("old"), 64)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PersistSnapshot(oldSnapshot); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	operations := defaultFileStoreOperations()
	originalOpen := operations.rootOpenFile
	armed := false
	operations.rootOpenFile = func(root *os.Root, name string, flags int, mode os.FileMode) (*os.File, error) {
		if armed && strings.HasPrefix(name, ".wal.tmp-") {
			return nil, errors.New("injected replacement WAL create")
		}
		return originalOpen(root, name, flags, mode)
	}
	injected, err := openFileStore(directory, identity, voters, operations)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := NewSnapshot(identity, SnapshotMetadata{LastIncludedIndex: 2, LastIncludedTerm: 3, StateMachineSchemaVersion: 6}, []byte("mismatch"), 64)
	if err != nil {
		t.Fatal(err)
	}
	request := InstallSnapshotRequest{LeaderID: 2, Term: 3, TransferID: TransferID{9}, SnapshotID: snapshot.ID,
		LastIncludedIndex: 2, LastIncludedTerm: 3, StateMachineSchemaVersion: 6,
		TotalLength: 8, Checksum: snapshot.StateChecksum, Chunk: []byte("mismatch"), Done: true}
	armed = true
	if _, err := injected.StageSnapshotChunk(request); err == nil {
		t.Fatal("injected install unexpectedly succeeded")
	}
	armed = false
	_ = injected.Close()

	reopened, err := OpenFileStore(directory, identity, voters)
	if err != nil {
		t.Fatalf("reopen old pair: %v", err)
	}
	defer reopened.Close()
	state, err := reopened.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if state.Snapshot == nil || state.Snapshot.ID != oldSnapshot.ID || state.SnapshotBase != oldSnapshot.Metadata || state.HardState.CommitIndex != 1 || len(state.Entries) != 2 {
		t.Fatalf("recovered mixed pair: %+v", state)
	}
}

func TestInstallSnapshotFileStoreDiscardsUntrustedStageOnRestart(t *testing.T) {
	directory := t.TempDir()
	identity, voters := testStorageIdentity(t, 1)
	store, err := OpenFileStore(directory, identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	seedInstallStore(t, store)
	first, final := installRequests(t, identity)
	if _, err := store.StageSnapshotChunk(first); err != nil {
		t.Fatal(err)
	}
	stageFile := store.stage.file
	store.stage = nil
	if err := store.ops.close(stageFile); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenFileStore(directory, identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.StageSnapshotChunk(final); !errors.Is(err, ErrSnapshotRejected) {
		t.Fatalf("continued untrusted restart stage: %v", err)
	}
	if progress, err := reopened.StageSnapshotChunk(first); err != nil || progress.NextOffset != 3 {
		t.Fatalf("fresh restart stage = %#v, %v", progress, err)
	}
}

func TestInstallSnapshotCoreAcknowledgesStaleWithoutStagingAndEmitsNewerOwnedAction(t *testing.T) {
	identity, _ := testStorageIdentity(t, 1)
	staleCore := installFollowerCore(t, 2)
	first, _ := installRequests(t, identity)
	first.LastIncludedIndex = 2
	first.LastIncludedTerm = 2
	if err := staleCore.Step(2, first); err != nil {
		t.Fatal(err)
	}
	ready, ok := staleCore.Ready()
	if !ok || len(ready.SnapshotActions) != 0 || len(ready.Messages) != 1 {
		t.Fatalf("stale Ready = %#v", ready)
	}
	response, ok := ready.Messages[0].RPC.(InstallSnapshotResponse)
	if !ok || !response.Success || !response.Done || response.NextOffset != first.TotalLength {
		t.Fatalf("stale response = %#v", ready.Messages[0].RPC)
	}
	if staleCore.LogState().SnapshotIndex != 0 || staleCore.HardState().CommitIndex != 2 {
		t.Fatalf("stale snapshot mutated Core: log=%+v hard=%+v", staleCore.LogState(), staleCore.HardState())
	}

	newerCore := installFollowerCore(t, 1)
	if err := newerCore.Step(2, first); err != nil {
		t.Fatal(err)
	}
	ready, ok = newerCore.Ready()
	if !ok || len(ready.Messages) != 0 || len(ready.SnapshotActions) != 1 {
		t.Fatalf("newer Ready = %#v", ready)
	}
	action := ready.SnapshotActions[0]
	if action.Kind != SnapshotActionStage || action.Request.Offset != 0 || string(action.Request.Chunk) != "abc" {
		t.Fatalf("newer action = %#v", action)
	}
	action.Request.Chunk[0] = 'X'
	repeated, _ := newerCore.Ready()
	if string(repeated.SnapshotActions[0].Request.Chunk) != "abc" {
		t.Fatal("Ready snapshot action aliases caller mutation")
	}
}

func TestInstallSnapshotNodeStaleRequestDoesNotRestoreOrStage(t *testing.T) {
	identity, voters := testStorageIdentity(t, 1)
	store, err := NewMemoryStore(identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Persist(PersistenceBatch{HardState: hardStatePointer(HardState{Term: 3, CommitIndex: 2}), ReplaceFrom: 1, Entries: []Entry{
		mustStorageEntry(t, 1, 1, "one"), mustStorageEntry(t, 2, 2, "local-two"), mustStorageEntry(t, 3, 3, "local-three"),
	}}); err != nil {
		t.Fatal(err)
	}
	durable, _ := store.Recover()
	machine := &task8StateMachine{}
	transport := &task8Transport{result: TransportAccepted, notify: make(chan PeerMessage, 1)}
	node := &Node{options: NodeOptions{LocalID: 1, Voters: voters, Identity: identity, Store: store, StateMachine: machine, Transport: transport,
		SnapshotEntryThreshold: math.MaxUint64, SnapshotByteThreshold: math.MaxUint64, MaxSnapshotBytes: 64},
		core: installFollowerCore(t, 2), durableState: durable, pendingLocal: make(map[ProposalID]pendingLocalRequest)}
	first, _ := installRequests(t, identity)
	if err := node.core.Step(2, first); err != nil {
		t.Fatal(err)
	}
	if err := node.drainReady(context.Background()); err != nil {
		t.Fatal(err)
	}
	response := (<-transport.notify).RPC.(InstallSnapshotResponse)
	if !response.Success || !response.Done || machine.restoreCalls != 0 || store.stage != nil {
		t.Fatalf("stale handling response=%#v restore=%d staged=%#v", response, machine.restoreCalls, store.stage)
	}
}

func TestInstallSnapshotCoreCompletionCorrelatesDurableOffsetAndInstalledState(t *testing.T) {
	identity, voters := testStorageIdentity(t, 1)
	store, err := NewMemoryStore(identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	seedInstallStore(t, store)
	core := installFollowerCore(t, 1)
	first, final := installRequests(t, identity)
	if err := core.Step(2, first); err != nil {
		t.Fatal(err)
	}
	ready, _ := core.Ready()
	staged, err := store.StageSnapshotChunk(first)
	if err != nil {
		t.Fatal(err)
	}
	message, err := core.CompleteSnapshotAction(ready.Token, SnapshotActionResult{NextOffset: staged.NextOffset})
	if err != nil {
		t.Fatal(err)
	}
	response := message.RPC.(InstallSnapshotResponse)
	if !response.Success || response.Done || response.NextOffset != 3 {
		t.Fatalf("durable offset response = %#v", response)
	}
	if err := core.Advance(ready.Token); err != nil {
		t.Fatal(err)
	}

	if err := core.Step(2, final); err != nil {
		t.Fatal(err)
	}
	ready, _ = core.Ready()
	installed, err := store.StageSnapshotChunk(final)
	if err != nil {
		t.Fatal(err)
	}
	message, err = core.CompleteSnapshotAction(ready.Token, SnapshotActionResult{NextOffset: installed.NextOffset, Done: true, State: installed.State})
	if err != nil {
		t.Fatal(err)
	}
	response = message.RPC.(InstallSnapshotResponse)
	if !response.Success || !response.Done || response.NextOffset != 5 {
		t.Fatalf("installed response = %#v", response)
	}
	if got := core.LogState(); got.SnapshotIndex != 2 || got.CommitIndex != 2 || got.AppliedIndex != 2 || len(got.Entries) != 1 || got.Entries[0].Index != 3 {
		t.Fatalf("installed Core log = %+v", got)
	}
	if err := core.Advance(ready.Token); err != nil {
		t.Fatal(err)
	}
}

func TestInstallSnapshotNodeStagesBeforeOffsetAndInstallsRestoresBeforeDone(t *testing.T) {
	identity, voters := testStorageIdentity(t, 1)
	memory, err := NewMemoryStore(identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	defer memory.Close()
	seedInstallStore(t, memory)
	events := &task8EventLog{}
	store := &installOrderingStore{MemoryStore: memory, events: events}
	machine := &task8StateMachine{events: events}
	transport := &task8Transport{result: TransportAccepted, events: events, notify: make(chan PeerMessage, 2)}
	durable, err := store.Recover()
	if err != nil {
		t.Fatal(err)
	}
	node := &Node{
		options: NodeOptions{LocalID: 1, Voters: voters, Identity: identity, Store: store, StateMachine: machine, Transport: transport,
			SnapshotEntryThreshold: math.MaxUint64, SnapshotByteThreshold: math.MaxUint64, MaxSnapshotBytes: 64},
		core: installFollowerCore(t, 1), durableState: durable, pendingLocal: make(map[ProposalID]pendingLocalRequest),
	}
	first, final := installRequests(t, identity)
	if err := node.core.Step(2, first); err != nil {
		t.Fatal(err)
	}
	if err := node.drainReady(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := events.snapshot(); !equalStrings(got, []string{"stage:0", "handoff"}) {
		t.Fatalf("offset ordering = %v", got)
	}
	firstResponse := (<-transport.notify).RPC.(InstallSnapshotResponse)
	if !firstResponse.Success || firstResponse.Done || firstResponse.NextOffset != 3 {
		t.Fatalf("offset response = %#v", firstResponse)
	}

	events.clear()
	if err := node.core.Step(2, final); err != nil {
		t.Fatal(err)
	}
	if err := node.drainReady(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := events.snapshot(); !equalStrings(got, []string{"stage:3", "install", "restore", "handoff"}) {
		t.Fatalf("completion ordering = %v", got)
	}
	done := (<-transport.notify).RPC.(InstallSnapshotResponse)
	if !done.Success || !done.Done || done.NextOffset != 5 {
		t.Fatalf("completion response = %#v", done)
	}
	if machine.restoreCalls != 1 || string(machine.restoreBytes[0]) != "abcde" {
		t.Fatalf("restore calls=%d bytes=%q", machine.restoreCalls, machine.restoreBytes)
	}
}

func TestInstallSnapshotTermChangeAbortsStageBeforeProtocolResponses(t *testing.T) {
	identity, voters := testStorageIdentity(t, 1)
	memory, err := NewMemoryStore(identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	defer memory.Close()
	seedInstallStore(t, memory)
	events := &task8EventLog{}
	store := &installOrderingStore{MemoryStore: memory, events: events}
	transport := &task8Transport{result: TransportAccepted, events: events}
	durable, _ := store.Recover()
	node := &Node{options: NodeOptions{LocalID: 1, Voters: voters, Identity: identity, Store: store,
		StateMachine: &task8StateMachine{}, Transport: transport, SnapshotEntryThreshold: math.MaxUint64, SnapshotByteThreshold: math.MaxUint64, MaxSnapshotBytes: 64},
		core: installFollowerCore(t, 1), durableState: durable, pendingLocal: make(map[ProposalID]pendingLocalRequest)}
	first, _ := installRequests(t, identity)
	if err := node.core.Step(2, first); err != nil {
		t.Fatal(err)
	}
	if err := node.drainReady(context.Background()); err != nil {
		t.Fatal(err)
	}
	events.clear()
	request := AppendEntriesRequest{LeaderID: 2, Term: 4, Generation: 1, PrevLogIndex: 3, PrevLogTerm: 3, LeaderCommit: 1}
	if err := node.core.Step(2, request); err != nil {
		t.Fatal(err)
	}
	if err := node.drainReady(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := events.snapshot()
	if len(got) < 3 || got[0] != "persist" || got[1] != "abort" || got[2] != "handoff" {
		t.Fatalf("term-change cleanup ordering = %v", got)
	}
	if memory.stage != nil {
		t.Fatal("term change left staged snapshot bytes")
	}
}

func TestInstallSnapshotLeaderRetriesIdenticalChunkCorrelatesAndResumesAppend(t *testing.T) {
	identity, _ := testStorageIdentity(t, 1)
	entries := testEntriesFrom(t, 4, testEntrySpec{term: 3, command: "four"})
	core, initial := electReplicationLeader(t, 3, 2, HardState{Term: 3, CommitIndex: 3}, entries)
	rejectInitialAppend(t, core, initial, 2)
	snapshot, err := NewSnapshot(identity, SnapshotMetadata{LastIncludedIndex: 3, LastIncludedTerm: 2, StateMachineSchemaVersion: 6}, []byte("abcde"), 64)
	if err != nil {
		t.Fatal(err)
	}
	transferID := TransferID{7}
	if err := core.StartSnapshotTransfer(2, snapshot, transferID, 3); err != nil {
		t.Fatal(err)
	}
	ready, ok := core.Ready()
	if !ok {
		t.Fatal("missing first snapshot chunk")
	}
	first := installRequestTo(t, ready, 2)
	if first.Offset != 0 || string(first.Chunk) != "abc" || first.Done || first.TransferID != transferID || first.SnapshotID != snapshot.ID {
		t.Fatalf("first chunk = %#v", first)
	}
	first.Chunk[0] = 'X'
	repeated, _ := core.Ready()
	if got := installRequestTo(t, repeated, 2); string(got.Chunk) != "abc" {
		t.Fatal("outgoing snapshot chunk aliases Ready caller")
	}
	advanceReadyToken(t, core, ready)

	if err := core.Tick(6); err != nil {
		t.Fatal(err)
	}
	retryReady, _ := core.Ready()
	retry := installRequestTo(t, retryReady, 2)
	if retry.Offset != 0 || string(retry.Chunk) != "abc" || retry.Done {
		t.Fatalf("retry chunk = %#v", retry)
	}
	advanceReadyToken(t, core, retryReady)

	wrong := InstallSnapshotResponse{ResponderID: 2, LeaderID: 1, Term: 5, RequestTerm: 4,
		TransferID: TransferID{8}, SnapshotID: snapshot.ID, NextOffset: 3}
	if err := core.Step(2, wrong); err != nil {
		t.Fatal(err)
	}
	if core.Status().Role != RoleLeader || core.HardState().Term != 4 {
		t.Fatal("uncorrelated higher term changed leader")
	}

	accepted := InstallSnapshotResponse{ResponderID: 2, LeaderID: 1, Term: 4, RequestTerm: 4,
		TransferID: transferID, SnapshotID: snapshot.ID, NextOffset: 3, Success: true}
	if err := core.Step(2, accepted); err != nil {
		t.Fatal(err)
	}
	nextReady, _ := core.Ready()
	final := installRequestTo(t, nextReady, 2)
	if final.Offset != 3 || string(final.Chunk) != "de" || !final.Done {
		t.Fatalf("final chunk = %#v", final)
	}
	advanceReadyToken(t, core, nextReady)
	before, _ := core.Progress(2)
	for _, ignored := range []InstallSnapshotResponse{
		accepted,
		{ResponderID: 2, LeaderID: 1, Term: 4, RequestTerm: 4, TransferID: transferID, SnapshotID: snapshot.ID, NextOffset: 6, Success: true},
	} {
		if err := core.Step(2, ignored); err != nil {
			t.Fatal(err)
		}
		if _, ok := core.Ready(); ok {
			t.Fatal("stale/future transfer response emitted work")
		}
		if after, _ := core.Progress(2); after != before {
			t.Fatalf("stale/future response changed progress: before=%#v after=%#v", before, after)
		}
	}

	done := accepted
	done.NextOffset = 5
	done.Done = true
	if err := core.Step(2, done); err != nil {
		t.Fatal(err)
	}
	resumeReady, _ := core.Ready()
	resume := appendRequestTo(t, resumeReady, 2)
	if resume.PrevLogIndex != 3 || len(resume.Entries) == 0 || resume.Entries[0].Index != 4 {
		t.Fatalf("resumed append = %#v", resume)
	}
	progress, _ := core.Progress(2)
	if progress.SnapshotNeeded || progress.MatchIndex != 3 || progress.NextIndex != 4 || !progress.ActiveTransferID.IsZero() {
		t.Fatalf("completed transfer progress = %#v", progress)
	}
}

func TestInstallSnapshotLeaderStepsDownOnlyForExactlyCorrelatedHigherTerm(t *testing.T) {
	identity, _ := testStorageIdentity(t, 1)
	entries := testEntriesFrom(t, 4, testEntrySpec{term: 3, command: "four"})
	core, initial := electReplicationLeader(t, 3, 2, HardState{Term: 3, CommitIndex: 3}, entries)
	rejectInitialAppend(t, core, initial, 2)
	snapshot, err := NewSnapshot(identity, SnapshotMetadata{LastIncludedIndex: 3, LastIncludedTerm: 2, StateMachineSchemaVersion: 6}, []byte("abc"), 64)
	if err != nil {
		t.Fatal(err)
	}
	transferID := TransferID{11}
	if err := core.StartSnapshotTransfer(2, snapshot, transferID, 3); err != nil {
		t.Fatal(err)
	}
	ready, _ := core.Ready()
	advanceReadyToken(t, core, ready)
	response := InstallSnapshotResponse{ResponderID: 2, LeaderID: 1, Term: 5, RequestTerm: 4,
		TransferID: transferID, SnapshotID: snapshot.ID}
	uncorrelated := response
	uncorrelated.NextOffset = 1
	if err := core.Step(2, uncorrelated); err != nil {
		t.Fatal(err)
	}
	if core.Status().Role != RoleLeader || core.HardState().Term != 4 {
		t.Fatal("future-offset higher term changed leader")
	}
	if err := core.Step(2, response); err != nil {
		t.Fatal(err)
	}
	if core.Status().Role != RoleFollower || core.HardState().Term != 5 {
		t.Fatalf("correlated higher term did not step down: %v %+v", core.Status().Role, core.HardState())
	}
}

func TestInstallSnapshotLeaderCompletesEmptyStateSnapshot(t *testing.T) {
	identity, _ := testStorageIdentity(t, 1)
	entries := testEntriesFrom(t, 4, testEntrySpec{term: 3, command: "four"})
	core, initial := electReplicationLeader(t, 3, 2, HardState{Term: 3, CommitIndex: 3}, entries)
	rejectInitialAppend(t, core, initial, 2)
	snapshot, err := NewSnapshot(identity, SnapshotMetadata{LastIncludedIndex: 3, LastIncludedTerm: 2, StateMachineSchemaVersion: 6}, nil, 64)
	if err != nil {
		t.Fatal(err)
	}
	transferID := TransferID{12}
	if err := core.StartSnapshotTransfer(2, snapshot, transferID, 3); err != nil {
		t.Fatal(err)
	}
	ready, _ := core.Ready()
	request := installRequestTo(t, ready, 2)
	if !request.Done || request.TotalLength != 0 || len(request.Chunk) != 0 {
		t.Fatalf("empty request = %#v", request)
	}
	advanceReadyToken(t, core, ready)
	response := InstallSnapshotResponse{ResponderID: 2, LeaderID: 1, Term: 4, RequestTerm: 4,
		TransferID: transferID, SnapshotID: snapshot.ID, Success: true, Done: true}
	if err := core.Step(2, response); err != nil {
		t.Fatal(err)
	}
	resume, ok := core.Ready()
	if !ok {
		t.Fatal("empty snapshot completion did not resume append")
	}
	_ = appendRequestTo(t, resume, 2)
}

func TestInstallSnapshotNodeStartsNeededTransferFromDurableSnapshotAndInjectedID(t *testing.T) {
	identity, voters := testStorageIdentity(t, 1)
	entries := testEntriesFrom(t, 4, testEntrySpec{term: 3, command: "four"})
	core, initial := electReplicationLeader(t, 3, 2, HardState{Term: 3, CommitIndex: 3}, entries)
	rejectInitialAppend(t, core, initial, 2)
	snapshot, err := NewSnapshot(identity, SnapshotMetadata{LastIncludedIndex: 3, LastIncludedTerm: 2, StateMachineSchemaVersion: 6}, []byte("abcde"), 64)
	if err != nil {
		t.Fatal(err)
	}
	source := &fixedTransferIDSource{id: TransferID{19}}
	node := &Node{core: core, durableState: RecoveredState{Identity: identity, HardState: core.HardState(), SnapshotBase: snapshot.Metadata, Snapshot: &snapshot, AppliedIndex: 3, Entries: cloneEntries(entries)},
		options: NodeOptions{LocalID: 1, Voters: voters, Identity: identity, TransferIDs: source, MaxSnapshotChunkBytes: 3}}
	started, err := node.maybeStartSnapshotTransfer()
	if err != nil || !started {
		t.Fatalf("start needed transfer = %t, %v", started, err)
	}
	ready, ok := core.Ready()
	if !ok {
		t.Fatal("needed transfer emitted no Ready")
	}
	request := installRequestTo(t, ready, 2)
	if request.TransferID != source.id || request.SnapshotID != snapshot.ID || string(request.Chunk) != "abc" {
		t.Fatalf("started request = %#v", request)
	}
}

func TestInstallSnapshotNodeFailsClosedOnTransferIDExhaustion(t *testing.T) {
	identity, voters := testStorageIdentity(t, 1)
	entries := testEntriesFrom(t, 4, testEntrySpec{term: 3, command: "four"})
	core, initial := electReplicationLeader(t, 3, 2, HardState{Term: 3, CommitIndex: 3}, entries)
	rejectInitialAppend(t, core, initial, 2)
	snapshot, err := NewSnapshot(identity, SnapshotMetadata{LastIncludedIndex: 3, LastIncludedTerm: 2, StateMachineSchemaVersion: 6}, []byte("abc"), 64)
	if err != nil {
		t.Fatal(err)
	}
	node := &Node{core: core, durableState: RecoveredState{Identity: identity, HardState: core.HardState(), SnapshotBase: snapshot.Metadata, Snapshot: &snapshot, AppliedIndex: 3, Entries: cloneEntries(entries)},
		options: NodeOptions{LocalID: 1, Voters: voters, Identity: identity, TransferIDs: &fixedTransferIDSource{}, MaxSnapshotChunkBytes: 3}}
	if _, err := node.maybeStartSnapshotTransfer(); !errors.Is(err, ErrTransferIDExhausted) {
		t.Fatalf("zero transfer identity error = %v", err)
	}
	if _, ok := core.Ready(); ok {
		t.Fatal("transfer-ID exhaustion emitted protocol work")
	}
}

type fixedTransferIDSource struct {
	id  TransferID
	err error
}

func (source *fixedTransferIDSource) NextTransferID(uint16) (TransferID, error) {
	return source.id, source.err
}

func installRequestTo(t *testing.T, ready Ready, peerID uint16) InstallSnapshotRequest {
	t.Helper()
	for _, message := range ready.Messages {
		if message.To != peerID {
			continue
		}
		if request, ok := message.RPC.(InstallSnapshotRequest); ok {
			return request
		}
	}
	t.Fatalf("Ready has no InstallSnapshot request to %d: %#v", peerID, ready.Messages)
	return InstallSnapshotRequest{}
}

type installOrderingStore struct {
	*MemoryStore
	events *task8EventLog
}

func (store *installOrderingStore) Persist(batch PersistenceBatch) error {
	store.events.add("persist")
	return store.MemoryStore.Persist(batch)
}

func (store *installOrderingStore) StageSnapshotChunk(request InstallSnapshotRequest) (SnapshotStageResult, error) {
	store.events.add("stage:" + fmt.Sprint(request.Offset))
	result, err := store.MemoryStore.StageSnapshotChunk(request)
	if result.Done {
		store.events.add("install")
	}
	return result, err
}

func (store *installOrderingStore) AbortSnapshotStage() error {
	store.events.add("abort")
	return store.MemoryStore.AbortSnapshotStage()
}

func installFollowerCore(t *testing.T, commit uint64) *Core {
	t.Helper()
	_, voters := testStorageIdentity(t, 1)
	entries := []Entry{mustStorageEntry(t, 1, 1, "one"), mustStorageEntry(t, 2, 2, "local-two"), mustStorageEntry(t, 3, 3, "local-three")}
	log, err := NewLog(0, 0, commit, commit, entries)
	if err != nil {
		t.Fatal(err)
	}
	core, err := NewCore(CoreOptions{LocalID: 1, Voters: voters, HardState: HardState{Term: 3, CommitIndex: commit}, Log: log, AppliedIndex: commit,
		ElectionTimeoutMin: 5, ElectionTimeoutMax: 10, HeartbeatInterval: 1, Random: task8ZeroOffsetRandom{}})
	if err != nil {
		t.Fatal(err)
	}
	return core
}

func seedInstallStore(t *testing.T, store StableStore) {
	t.Helper()
	if err := store.Persist(PersistenceBatch{HardState: hardStatePointer(HardState{Term: 3, CommitIndex: 1}), ReplaceFrom: 1, Entries: []Entry{
		mustStorageEntry(t, 1, 1, "one"), mustStorageEntry(t, 2, 2, "local-two"), mustStorageEntry(t, 3, 3, "local-three"),
	}}); err != nil {
		t.Fatal(err)
	}
}

func installRequests(t *testing.T, identity StorageIdentity) (InstallSnapshotRequest, InstallSnapshotRequest) {
	t.Helper()
	metadata := SnapshotMetadata{LastIncludedIndex: 2, LastIncludedTerm: 2, StateMachineSchemaVersion: 6}
	snapshot, err := NewSnapshot(identity, metadata, []byte("abcde"), 64)
	if err != nil {
		t.Fatal(err)
	}
	base := InstallSnapshotRequest{LeaderID: 2, Term: 3, TransferID: TransferID{1}, SnapshotID: snapshot.ID,
		LastIncludedIndex: metadata.LastIncludedIndex, LastIncludedTerm: metadata.LastIncludedTerm,
		StateMachineSchemaVersion: metadata.StateMachineSchemaVersion, TotalLength: 5, Checksum: snapshot.StateChecksum,
	}
	first := base
	first.Chunk = []byte("abc")
	final := base
	final.Offset = 3
	final.Chunk = []byte("de")
	final.Done = true
	return first, final
}
