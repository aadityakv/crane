package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
)

type task8EventLog struct {
	mu     sync.Mutex
	events []string
}

func (log *task8EventLog) add(event string) {
	log.mu.Lock()
	defer log.mu.Unlock()
	log.events = append(log.events, event)
}

func (log *task8EventLog) snapshot() []string {
	log.mu.Lock()
	defer log.mu.Unlock()
	return append([]string(nil), log.events...)
}

func (log *task8EventLog) clear() {
	log.mu.Lock()
	defer log.mu.Unlock()
	log.events = nil
}

type task8Store struct {
	mu             sync.Mutex
	state          RecoveredState
	recoverErr     error
	persistErr     error
	closeErr       error
	persistStarted chan struct{}
	persistRelease <-chan struct{}
	persistOnce    sync.Once
	recovers       int
	persists       []PersistenceBatch
	closes         int
	events         *task8EventLog
}

func (store *task8Store) Recover() (RecoveredState, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.recovers++
	if store.events != nil {
		store.events.add("recover")
	}
	return store.state.Clone(), store.recoverErr
}

func (store *task8Store) Persist(batch PersistenceBatch) error {
	store.mu.Lock()
	if store.events != nil {
		store.events.add("persist")
	}
	if store.persistErr != nil {
		store.mu.Unlock()
		return store.persistErr
	}
	started, release := store.persistStarted, store.persistRelease
	store.mu.Unlock()
	if started != nil {
		store.persistOnce.Do(func() { close(started) })
	}
	if release != nil {
		<-release
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.persists = append(store.persists, batch.Clone())
	return nil
}

func (store *task8Store) Close() error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.closes++
	if store.events != nil {
		store.events.add("close")
	}
	return store.closeErr
}

type task8ApplyCall struct {
	index   uint64
	term    uint64
	command []byte
}

type task8StateMachine struct {
	mu           sync.Mutex
	restoreErr   error
	applyErrAt   uint64
	restoreCalls int
	restoreBytes [][]byte
	applyCalls   []task8ApplyCall
	lastResult   []byte
	events       *task8EventLog
}

func (machine *task8StateMachine) Apply(index, term uint64, command []byte) ([]byte, error) {
	machine.mu.Lock()
	defer machine.mu.Unlock()
	if machine.events != nil {
		machine.events.add(fmt.Sprintf("apply:%d", index))
	}
	machine.applyCalls = append(machine.applyCalls, task8ApplyCall{index: index, term: term, command: cloneBytes(command)})
	if len(command) != 0 {
		command[0] ^= 0xff
	}
	if index == machine.applyErrAt {
		return nil, errors.New("injected application failure")
	}
	machine.lastResult = []byte(fmt.Sprintf("result:%d", index))
	return machine.lastResult, nil
}

func (*task8StateMachine) Capture(uint64, uint64) (SnapshotCapture, error) { return nil, nil }

func (machine *task8StateMachine) Restore(_ uint32, snapshot []byte) error {
	machine.mu.Lock()
	defer machine.mu.Unlock()
	machine.restoreCalls++
	machine.restoreBytes = append(machine.restoreBytes, cloneBytes(snapshot))
	if machine.events != nil {
		machine.events.add("restore")
	}
	return machine.restoreErr
}

type task8Transport struct {
	mu     sync.Mutex
	result TransportHandoff
	err    error
	mutate bool
	events *task8EventLog
	notify chan PeerMessage
}

func (transport *task8Transport) Handoff(message PeerMessage) (TransportHandoff, error) {
	transport.mu.Lock()
	if transport.mutate {
		if request, ok := message.RPC.(AppendEntriesRequest); ok && len(request.Entries) != 0 && len(request.Entries[len(request.Entries)-1].command) != 0 {
			request.Entries[len(request.Entries)-1].command[0] ^= 0xff
			message.RPC = request
		}
	}
	if transport.events != nil {
		transport.events.add("handoff")
	}
	result, err := transport.result, transport.err
	transport.mu.Unlock()
	if transport.notify != nil {
		transport.notify <- message
	}
	return result, err
}

type task8ZeroOffsetRandom struct{}

func (task8ZeroOffsetRandom) Uint64() uint64 { return 10_000_000_000 }

func TestNodeConstructorHasNoStorageApplicationTimerOrGoroutineEffect(t *testing.T) {
	options, store, machine, manual := task8NodeOptions(t, RecoveredState{})
	node, err := NewNode(options)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if store.recovers != 0 || store.closes != 0 || len(store.persists) != 0 {
		t.Fatalf("constructor storage effects = recover %d persist %d close %d", store.recovers, len(store.persists), store.closes)
	}
	if machine.restoreCalls != 0 || len(machine.applyCalls) != 0 {
		t.Fatalf("constructor application effects = restore %d apply %d", machine.restoreCalls, len(machine.applyCalls))
	}
	if got := manual.PendingTimers(); got != 0 {
		t.Fatalf("constructor timers = %d, want 0", got)
	}
	if _, err := node.Propose(context.Background(), []byte("before-run")); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("Propose before Run error = %v, want ErrNotRunning", err)
	}
	if _, err := node.Barrier(context.Background()); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("Barrier before Run error = %v, want ErrNotRunning", err)
	}
	if _, err := node.SubscribeLeadership(context.Background(), 1); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("Subscribe before Run error = %v, want ErrNotRunning", err)
	}
	if err := node.SubmitRPC(context.Background(), 2, PreVoteRequest{}); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("SubmitRPC before Run error = %v, want ErrNotRunning", err)
	}
}

func TestNodeConstructorRejectsInvalidAppendBoundsBeforeEffects(t *testing.T) {
	options, store, machine, manual := task8NodeOptions(t, RecoveredState{})
	options.MaxAppendBytes = 1
	if _, err := NewNode(options); !errors.Is(err, ErrInvalidCoreState) {
		t.Fatalf("NewNode invalid append bounds error = %v, want ErrInvalidCoreState", err)
	}
	if store.recovers != 0 || store.closes != 0 || len(store.persists) != 0 || machine.restoreCalls != 0 || manual.PendingTimers() != 0 {
		t.Fatal("invalid constructor performed storage, application, or timer effects")
	}
}

func TestNodeConstructorRejectsElectionMinimumBelowThreeHeartbeats(t *testing.T) {
	options, store, machine, manual := task8NodeOptions(t, RecoveredState{})
	options.ElectionTimeoutMin = 2 * options.HeartbeatInterval
	if _, err := NewNode(options); !errors.Is(err, ErrInvalidCoreState) {
		t.Fatalf("NewNode invalid timing error = %v, want ErrInvalidCoreState", err)
	}
	if store.recovers != 0 || store.closes != 0 || len(store.persists) != 0 || machine.restoreCalls != 0 || manual.PendingTimers() != 0 {
		t.Fatal("invalid constructor performed storage, application, or timer effects")
	}
}

func TestRecoveryReplaysEveryCommittedEntryFromEmptySnapshotAndSkipsNoOp(t *testing.T) {
	entries := []Entry{
		mustStorageEntry(t, 1, 1, "one"),
		mustTask8NoOp(t, 2, 1),
		mustStorageEntry(t, 3, 2, "three"),
		mustStorageEntry(t, 4, 2, "uncommitted"),
	}
	state := RecoveredState{HardState: HardState{Term: 2, CommitIndex: 3}, AppliedIndex: 3, Entries: entries}
	options, store, machine, manual := task8NodeOptions(t, state)
	node, err := NewNode(options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- node.Run(ctx) }()
	<-node.Ready()

	machine.mu.Lock()
	if machine.restoreCalls != 1 || len(machine.restoreBytes) != 1 || machine.restoreBytes[0] != nil {
		t.Fatalf("Restore calls = %d bytes=%#v, want one empty-base restore", machine.restoreCalls, machine.restoreBytes)
	}
	if len(machine.applyCalls) != 2 || machine.applyCalls[0].index != 1 || machine.applyCalls[1].index != 3 {
		t.Fatalf("Apply calls = %#v, want committed commands 1 and 3 only", machine.applyCalls)
	}
	if got := string(machine.applyCalls[0].command); got != "one" {
		t.Fatalf("first applied command = %q, want owned original", got)
	}
	machine.mu.Unlock()
	if got := string(state.Entries[0].CommandBytes()); got != "one" {
		t.Fatalf("application mutation aliased recovered command = %q", got)
	}
	if got, want := node.Status(), (Status{Role: RoleFollower, Term: 2, CommitIndex: 3, AppliedIndex: 3, LastIndex: 4}); got != want {
		t.Fatalf("Status after recovery = %#v, want %#v", got, want)
	}
	if len(store.persists) != 0 {
		t.Fatalf("ordinary recovery replay persisted applied progress: %#v", store.persists)
	}

	cancel()
	if err := <-runResult; err != nil {
		t.Fatalf("Run shutdown error = %v", err)
	}
	if got := manual.PendingTimers(); got != 0 {
		t.Fatalf("timers after shutdown = %d, want 0", got)
	}
	if store.closes != 1 {
		t.Fatalf("store closes = %d, want 1", store.closes)
	}
	if _, err := node.Propose(context.Background(), []byte("after-stop")); !errors.Is(err, ErrStopped) {
		t.Fatalf("Propose after stop error = %v, want ErrStopped", err)
	}
}

func TestRecoveryFailsClosedWithoutTask9SnapshotBytes(t *testing.T) {
	state := RecoveredState{
		HardState:    HardState{Term: 1, CommitIndex: 1},
		SnapshotBase: SnapshotMetadata{LastIncludedIndex: 1, LastIncludedTerm: 1, StateMachineSchemaVersion: 7},
		AppliedIndex: 1,
	}
	options, store, machine, _ := task8NodeOptions(t, state)
	node, err := NewNode(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Run(context.Background()); !errors.Is(err, ErrSnapshotUnavailable) {
		t.Fatalf("Run error = %v, want ErrSnapshotUnavailable", err)
	}
	if machine.restoreCalls != 0 || len(machine.applyCalls) != 0 {
		t.Fatalf("unsupported snapshot reached application: restore=%d apply=%d", machine.restoreCalls, len(machine.applyCalls))
	}
	if store.closes != 1 {
		t.Fatalf("store closes = %d, want 1", store.closes)
	}
	select {
	case <-node.Ready():
		t.Fatal("Ready closed after recovery failure")
	default:
	}
}

func TestRecoveryReturnsRestoreAndApplyFailures(t *testing.T) {
	t.Run("restore", func(t *testing.T) {
		failure := errors.New("restore failed")
		options, store, machine, _ := task8NodeOptions(t, RecoveredState{})
		machine.restoreErr = failure
		node, err := NewNode(options)
		if err != nil {
			t.Fatal(err)
		}
		if err := node.Run(context.Background()); !errors.Is(err, failure) {
			t.Fatalf("Run error = %v, want restore failure", err)
		}
		if store.closes != 1 {
			t.Fatalf("store closes = %d, want 1", store.closes)
		}
	})

	t.Run("apply", func(t *testing.T) {
		state := RecoveredState{HardState: HardState{Term: 1, CommitIndex: 1}, Entries: []Entry{mustStorageEntry(t, 1, 1, "one")}}
		options, store, machine, _ := task8NodeOptions(t, state)
		machine.applyErrAt = 1
		node, err := NewNode(options)
		if err != nil {
			t.Fatal(err)
		}
		if err := node.Run(context.Background()); err == nil || err.Error() != "apply recovered entry 1: injected application failure" {
			t.Fatalf("Run error = %v, want exact apply failure", err)
		}
		if store.closes != 1 {
			t.Fatalf("store closes = %d, want 1", store.closes)
		}
	})
}

func TestExecutionOrderPersistsBeforeHandoffAndAdvancesWhenPeerUnavailable(t *testing.T) {
	events := &task8EventLog{}
	options, store, machine, _ := task8NodeOptions(t, RecoveredState{})
	store.events = events
	machine.events = events
	transport := options.Transport.(*task8Transport)
	transport.events = events
	transport.result = TransportUnavailable
	node, err := NewNode(options)
	if err != nil {
		t.Fatal(err)
	}
	node.afterAdvance = func(ReadyToken) { events.add("advance") }
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- node.Run(ctx) }()
	<-node.Ready()
	if err := node.SubmitRPC(context.Background(), 2, RequestVoteRequest{CandidateID: 2, Term: 1}); err != nil {
		t.Fatalf("SubmitRPC: %v", err)
	}
	if got, want := events.snapshot(), []string{"recover", "restore", "persist", "handoff", "advance"}; !equalStrings(got, want) {
		t.Fatalf("execution events = %v, want %v", got, want)
	}
	cancel()
	if err := <-runResult; err != nil {
		t.Fatal(err)
	}
}

func TestExecutionOrderStopsWithoutLaterEffectsAfterPersistenceFailure(t *testing.T) {
	events := &task8EventLog{}
	failure := errors.New("disk sync failed")
	options, store, machine, _ := task8NodeOptions(t, RecoveredState{})
	store.events = events
	store.persistErr = failure
	machine.events = events
	transport := options.Transport.(*task8Transport)
	transport.events = events
	node, err := NewNode(options)
	if err != nil {
		t.Fatal(err)
	}
	node.afterAdvance = func(ReadyToken) { events.add("advance") }
	runResult := make(chan error, 1)
	go func() { runResult <- node.Run(context.Background()) }()
	<-node.Ready()
	rpcResult := make(chan error, 1)
	go func() {
		rpcResult <- node.SubmitRPC(context.Background(), 2, RequestVoteRequest{CandidateID: 2, Term: 1})
	}()
	if err := <-runResult; !errors.Is(err, failure) {
		t.Fatalf("Run error = %v, want persistence failure", err)
	}
	if err := <-rpcResult; !errors.Is(err, ErrStopped) {
		t.Fatalf("SubmitRPC error = %v, want ErrStopped after terminal failure", err)
	}
	if got, want := events.snapshot(), []string{"recover", "restore", "persist", "close"}; !equalStrings(got, want) {
		t.Fatalf("failure events = %v, want no later effects %v", got, want)
	}
}

func TestExecutionOrderHandsOffBeforeApplyingSameReady(t *testing.T) {
	events := &task8EventLog{}
	options, store, machine, _ := task8NodeOptions(t, RecoveredState{})
	store.events = events
	machine.events = events
	transport := options.Transport.(*task8Transport)
	transport.events = events
	node, cancel, runResult := startTask8Follower(t, options)
	defer stopTask8Node(t, cancel, runResult)
	node.afterAdvance = func(ReadyToken) { events.add("advance") }
	events.clear()

	entry := mustStorageEntry(t, 1, 1, "one")
	request := AppendEntriesRequest{
		LeaderID: 2, Term: 1, Generation: 1,
		LeaderCommit: 1, Entries: []Entry{entry},
	}
	if err := node.SubmitRPC(context.Background(), 2, request); err != nil {
		t.Fatalf("SubmitRPC: %v", err)
	}
	if got, want := events.snapshot(), []string{"persist", "handoff", "apply:1", "advance"}; !equalStrings(got, want) {
		t.Fatalf("same-Ready execution events = %v, want %v", got, want)
	}
}

func TestExecutionOrderValidatesProposalDependencyGraphBeforeEffects(t *testing.T) {
	options, _, _, _ := task8NodeOptions(t, RecoveredState{})
	node, cancel, runResult := startTask8Follower(t, options)
	defer stopTask8Node(t, cancel, runResult)
	entry := mustStorageEntry(t, 1, 1, "unknown-waiter")
	ready := Ready{
		Token:              1,
		CommittedEntries:   []Entry{entry},
		CommittedProposals: []CommittedProposal{{ID: 99, Entry: entry}},
	}
	if err := node.validateReady(ready); !errors.Is(err, ErrInvalidCoreState) {
		t.Fatalf("validateReady error = %v, want unknown waiter rejected before effects", err)
	}
}

func TestNodeCallerCancellationDoesNotBlockOrAbandonStartedPersistence(t *testing.T) {
	events := &task8EventLog{}
	persistStarted := make(chan struct{})
	persistRelease := make(chan struct{})
	options, store, _, _ := task8NodeOptions(t, RecoveredState{})
	store.events = events
	store.persistStarted = persistStarted
	store.persistRelease = persistRelease
	transport := options.Transport.(*task8Transport)
	transport.events = events
	node, cancelNode, runResult := startTask8Follower(t, options)
	advanced := make(chan struct{})
	var advanceOnce sync.Once
	node.afterAdvance = func(ReadyToken) {
		events.add("advance")
		advanceOnce.Do(func() { close(advanced) })
	}

	callerCtx, cancelCaller := context.WithCancel(context.Background())
	callerResult := make(chan error, 1)
	go func() {
		callerResult <- node.SubmitRPC(callerCtx, 2, RequestVoteRequest{CandidateID: 2, Term: 1})
	}()
	<-persistStarted
	cancelCaller()
	if err := <-callerResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled caller error = %v, want context.Canceled", err)
	}
	if got, want := events.snapshot(), []string{"recover", "persist"}; !equalStrings(got, want) {
		t.Fatalf("effects before persistence release = %v, want %v", got, want)
	}
	close(persistRelease)
	<-advanced
	if got, want := events.snapshot(), []string{"recover", "persist", "handoff", "advance"}; !equalStrings(got, want) {
		t.Fatalf("effects after persistence release = %v, want %v", got, want)
	}
	stopTask8Node(t, cancelNode, runResult)
}

func TestProposalAndBarrierCompleteOnlyAfterExactApplyAndAdvanceOrdering(t *testing.T) {
	events := &task8EventLog{}
	options, store, machine, manual := task8NodeOptions(t, RecoveredState{})
	store.events = events
	machine.events = events
	transport := options.Transport.(*task8Transport)
	transport.events = events
	transport.notify = make(chan PeerMessage, 64)
	transport.mutate = true
	node, cancel, runResult := startAuthorizedTask8Node(t, options, manual, transport)
	defer stopTask8Node(t, cancel, runResult)
	node.afterAdvance = func(ReadyToken) { events.add("advance") }
	node.afterResult = func(ProposalID) { events.add("result") }
	events.clear()

	command := []byte("set-x")
	proposalResult := make(chan struct {
		result ProposalResult
		err    error
	}, 1)
	go func() {
		result, err := node.Propose(context.Background(), command)
		proposalResult <- struct {
			result ProposalResult
			err    error
		}{result: result, err: err}
	}()
	request := task8AppendRequestTo(t, transport.notify, 2, EntryCommand)
	_ = task8AppendRequestTo(t, transport.notify, 3, EntryCommand)
	if err := node.SubmitRPC(context.Background(), 2, appendSuccess(request, 2)); err != nil {
		t.Fatal(err)
	}
	completed := <-proposalResult
	if completed.err != nil {
		t.Fatalf("Propose error = %v", completed.err)
	}
	if got, want := completed.result, (ProposalResult{Index: 2, Term: 1, Result: []byte("result:2")}); got.Index != want.Index || got.Term != want.Term || string(got.Result) != string(want.Result) {
		t.Fatalf("ProposalResult = %#v, want %#v", got, want)
	}
	if got := string(command); got != "set-x" {
		t.Fatalf("application mutation aliased caller command = %q", got)
	}
	completed.result.Result[0] = 'X'
	machine.mu.Lock()
	if got := string(machine.lastResult); got != "result:2" {
		t.Fatalf("caller mutation aliased application result = %q", got)
	}
	if got := string(machine.applyCalls[0].command); got != "set-x" {
		t.Fatalf("transport mutation aliased committed application command = %q", got)
	}
	applyCount := len(machine.applyCalls)
	machine.mu.Unlock()
	if got, want := events.snapshot(), []string{"persist", "handoff", "handoff", "advance", "persist", "apply:2", "result", "advance"}; !equalStrings(got, want) {
		t.Fatalf("proposal execution events = %v, want %v", got, want)
	}

	events.clear()
	barrierResult := make(chan struct {
		index uint64
		err   error
	}, 1)
	go func() {
		index, err := node.Barrier(context.Background())
		barrierResult <- struct {
			index uint64
			err   error
		}{index: index, err: err}
	}()
	barrierRequest := task8AppendRequestTo(t, transport.notify, 2, EntryNoOp)
	_ = task8AppendRequestTo(t, transport.notify, 3, EntryNoOp)
	if err := node.SubmitRPC(context.Background(), 2, appendSuccess(barrierRequest, 2)); err != nil {
		t.Fatal(err)
	}
	barrier := <-barrierResult
	if barrier.err != nil || barrier.index != 3 {
		t.Fatalf("Barrier = (%d,%v), want (3,nil)", barrier.index, barrier.err)
	}
	machine.mu.Lock()
	if len(machine.applyCalls) != applyCount {
		t.Fatalf("barrier invoked StateMachine.Apply: calls=%#v", machine.applyCalls)
	}
	machine.mu.Unlock()
	if got, want := events.snapshot(), []string{"persist", "handoff", "handoff", "advance", "persist", "result", "advance"}; !equalStrings(got, want) {
		t.Fatalf("barrier execution events = %v, want %v", got, want)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for index, batch := range store.persists {
		if batch.AppliedIndex != nil {
			t.Fatalf("ordinary Ready batch %d persisted volatile applied index %d", index, *batch.AppliedIndex)
		}
	}
}

func TestProposalCancellationBeforeAndAfterAppendIsNonblockingAndSafe(t *testing.T) {
	options, _, machine, manual := task8NodeOptions(t, RecoveredState{})
	transport := options.Transport.(*task8Transport)
	transport.notify = make(chan PeerMessage, 64)
	node, cancelNode, runResult := startAuthorizedTask8Node(t, options, manual, transport)
	defer stopTask8Node(t, cancelNode, runResult)

	before, cancelBefore := context.WithCancel(context.Background())
	cancelBefore()
	if _, err := node.Propose(before, []byte("never-appended")); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled Propose error = %v, want context.Canceled", err)
	}
	if got := node.Status().LastIndex; got != 1 {
		t.Fatalf("pre-canceled proposal changed last index to %d", got)
	}

	after, cancelAfter := context.WithCancel(context.Background())
	proposalResult := make(chan error, 1)
	go func() {
		_, err := node.Propose(after, []byte("commit-after-cancel"))
		proposalResult <- err
	}()
	request := task8AppendRequestTo(t, transport.notify, 2, EntryCommand)
	_ = task8AppendRequestTo(t, transport.notify, 3, EntryCommand)
	cancelAfter()
	if err := <-proposalResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("post-append canceled Propose error = %v, want context.Canceled", err)
	}
	if err := node.SubmitRPC(context.Background(), 2, appendSuccess(request, 2)); err != nil {
		t.Fatal(err)
	}
	if got := node.Status().AppliedIndex; got != 2 {
		t.Fatalf("orphaned committed proposal applied index = %d, want 2", got)
	}
	machine.mu.Lock()
	defer machine.mu.Unlock()
	if len(machine.applyCalls) != 1 || string(machine.applyCalls[0].command) != "commit-after-cancel" {
		t.Fatalf("orphaned proposal apply calls = %#v", machine.applyCalls)
	}
}

func TestProposalNonLeaderErrorCarriesBestEffortLeaderHint(t *testing.T) {
	options, _, _, _ := task8NodeOptions(t, RecoveredState{})
	node, err := NewNode(options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- node.Run(ctx) }()
	<-node.Ready()
	request := AppendEntriesRequest{LeaderID: 2, Term: 1, Generation: 1}
	if err := node.SubmitRPC(context.Background(), 2, request); err != nil {
		t.Fatal(err)
	}
	_, proposalErr := node.Propose(context.Background(), []byte("follower"))
	var notLeader *NotLeaderError
	if !errors.Is(proposalErr, ErrNotLeader) || !errors.As(proposalErr, &notLeader) || notLeader.LeaderID != 2 {
		t.Fatalf("Propose error = %#v, want typed leader hint 2", proposalErr)
	}
	cancel()
	if err := <-runResult; err != nil {
		t.Fatal(err)
	}
}

func TestProposalLeadershipLossCannotCrossCompleteReplacement(t *testing.T) {
	options, _, machine, manual := task8NodeOptions(t, RecoveredState{})
	transport := options.Transport.(*task8Transport)
	transport.notify = make(chan PeerMessage, 64)
	node, cancelNode, runResult := startAuthorizedTask8Node(t, options, manual, transport)
	defer stopTask8Node(t, cancelNode, runResult)

	proposalResult := make(chan error, 1)
	go func() {
		_, err := node.Propose(context.Background(), []byte("original"))
		proposalResult <- err
	}()
	_ = task8AppendRequestTo(t, transport.notify, 2, EntryCommand)
	_ = task8AppendRequestTo(t, transport.notify, 3, EntryCommand)
	replacement, err := NewEntry(2, 2, EntryCommand, []byte("replacement"))
	if err != nil {
		t.Fatal(err)
	}
	request := AppendEntriesRequest{
		LeaderID: 2, Term: 2, Generation: 40,
		PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 2, Entries: []Entry{replacement},
	}
	if err := node.SubmitRPC(context.Background(), 2, request); err != nil {
		t.Fatal(err)
	}
	if err := <-proposalResult; !errors.Is(err, ErrProposalFailed) {
		t.Fatalf("overwritten Propose error = %v, want ErrProposalFailed", err)
	}
	machine.mu.Lock()
	defer machine.mu.Unlock()
	if len(machine.applyCalls) != 1 || string(machine.applyCalls[0].command) != "replacement" {
		t.Fatalf("replacement apply calls = %#v, want only replacement", machine.applyCalls)
	}
}

func TestProposalDuplicateCommandsRemainIndependentAtNodeBoundary(t *testing.T) {
	options, _, _, manual := task8NodeOptions(t, RecoveredState{})
	transport := options.Transport.(*task8Transport)
	transport.notify = make(chan PeerMessage, 64)
	node, cancelNode, runResult := startAuthorizedTask8Node(t, options, manual, transport)
	defer stopTask8Node(t, cancelNode, runResult)

	for wantIndex := uint64(2); wantIndex <= 3; wantIndex++ {
		resultChannel := make(chan localRequestResponse, 1)
		go func() {
			result, err := node.Propose(context.Background(), []byte("duplicate"))
			resultChannel <- localRequestResponse{result: result, err: err}
		}()
		request := task8AppendRequestTo(t, transport.notify, 2, EntryCommand)
		_ = task8AppendRequestTo(t, transport.notify, 3, EntryCommand)
		if err := node.SubmitRPC(context.Background(), 2, appendSuccess(request, 2)); err != nil {
			t.Fatal(err)
		}
		completed := <-resultChannel
		if completed.err != nil || completed.result.Index != wantIndex || completed.result.Term != 1 {
			t.Fatalf("duplicate proposal result %d = %#v error=%v", wantIndex, completed.result, completed.err)
		}
	}
}

func TestNodePendingProposalBoundaryRejectsExactlyCapacityPlusOne(t *testing.T) {
	options, _, _, manual := task8NodeOptions(t, RecoveredState{})
	transport := options.Transport.(*task8Transport)
	transport.notify = make(chan PeerMessage, 8)
	node, cancelNode, runResult := startAuthorizedTask8Node(t, options, manual, transport)

	for index := 0; index < MaxPendingLocalRequests; index++ {
		proposalCtx, cancelProposal := context.WithCancel(context.Background())
		proposalResult := make(chan error, 1)
		go func() {
			_, err := node.Propose(proposalCtx, []byte("pending"))
			proposalResult <- err
		}()
		_ = task8AppendRequestTo(t, transport.notify, 2, EntryCommand)
		_ = task8AppendRequestTo(t, transport.notify, 3, EntryCommand)
		cancelProposal()
		if err := <-proposalResult; !errors.Is(err, context.Canceled) {
			t.Fatalf("pending proposal %d cancellation error = %v", index, err)
		}
	}
	if got := node.pendingReservations.Load(); got != MaxPendingLocalRequests {
		t.Fatalf("pending reservations = %d, want %d", got, MaxPendingLocalRequests)
	}
	if _, err := node.Propose(context.Background(), []byte("capacity-plus-one")); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("capacity+1 Propose error = %v, want ErrOverloaded", err)
	}
	stopTask8Node(t, cancelNode, runResult)
	if got := node.pendingReservations.Load(); got != 0 {
		t.Fatalf("pending reservations after shutdown = %d, want 0", got)
	}
}

func TestNodeManualClockReusesOneTimerForElectionsHeartbeatsAndIdleShutdown(t *testing.T) {
	options, _, _, manual := task8NodeOptions(t, RecoveredState{})
	transport := options.Transport.(*task8Transport)
	transport.notify = make(chan PeerMessage, 64)
	node, cancelNode, runResult := startAuthorizedTask8Node(t, options, manual, transport)
	if got := manual.PendingTimers(); got != 1 {
		t.Fatalf("pending timers after election = %d, want one reusable timer", got)
	}
	for heartbeat := 0; heartbeat < 3; heartbeat++ {
		manual.Advance(time.Second)
		for _, peerID := range []uint16{2, 3} {
			message := task8RPCTo(t, transport.notify, peerID, func(rpc RPC) bool {
				request, ok := rpc.(AppendEntriesRequest)
				return ok && len(request.Entries) == 0
			})
			if request := message.RPC.(AppendEntriesRequest); request.Term != 1 || request.LeaderID != 1 {
				t.Fatalf("heartbeat = %#v", request)
			}
		}
		// An owner-processed rejected RPC is a synchronization barrier after the
		// heartbeat Ready has advanced and its reusable timer has reset.
		if err := node.SubmitRPC(context.Background(), 99, PreVoteRequest{}); !errors.Is(err, ErrNotVoter) {
			t.Fatalf("owner synchronization error = %v, want ErrNotVoter", err)
		}
		if got := manual.PendingTimers(); got != 1 {
			t.Fatalf("pending timers after heartbeat %d = %d, want 1", heartbeat, got)
		}
	}
	stopTask8Node(t, cancelNode, runResult)
	if got := manual.PendingTimers(); got != 0 {
		t.Fatalf("pending timers after idle shutdown = %d, want 0", got)
	}
}

func TestNodeLogicalClockRegressionIsFatal(t *testing.T) {
	options, _, _, manual := task8NodeOptions(t, RecoveredState{})
	transport := options.Transport.(*task8Transport)
	transport.notify = make(chan PeerMessage, 8)
	node, cancel, runResult := startTask8Follower(t, options)
	defer cancel()
	manual.Advance(5 * time.Second)
	_ = task8RPCTo(t, transport.notify, 2, func(rpc RPC) bool { _, ok := rpc.(PreVoteRequest); return ok })
	_ = task8RPCTo(t, transport.notify, 3, func(rpc RPC) bool { _, ok := rpc.(PreVoteRequest); return ok })
	manual.Advance(-time.Second)
	rpcResult := make(chan error, 1)
	go func() {
		rpcResult <- node.SubmitRPC(context.Background(), 2, PreVoteRequest{CandidateID: 2, CurrentTerm: 0, ProspectiveTerm: 1})
	}()
	if err := <-runResult; !errors.Is(err, ErrTickRegression) {
		t.Fatalf("Run error = %v, want ErrTickRegression", err)
	}
	if err := <-rpcResult; !errors.Is(err, ErrStopped) {
		t.Fatalf("SubmitRPC error = %v, want ErrStopped after clock regression", err)
	}
}

func TestNodeStatusIsRaceSafeDuringSerializedTransitions(t *testing.T) {
	options, _, _, _ := task8NodeOptions(t, RecoveredState{})
	node, cancelNode, runResult := startTask8Follower(t, options)
	stopReaders := make(chan struct{})
	var readers sync.WaitGroup
	for reader := 0; reader < 8; reader++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stopReaders:
					return
				default:
					_ = node.Status()
				}
			}
		}()
	}
	for term := uint64(1); term <= 40; term++ {
		leaderID := uint16(2 + term%2)
		if err := node.SubmitRPC(context.Background(), leaderID, AppendEntriesRequest{LeaderID: leaderID, Term: term, Generation: 1}); err != nil {
			t.Fatal(err)
		}
	}
	close(stopReaders)
	readers.Wait()
	if status := node.Status(); status.Term != 40 || status.Role != RoleFollower {
		t.Fatalf("final Status = %#v, want follower term 40", status)
	}
	stopTask8Node(t, cancelNode, runResult)
}

func TestNodeLifecycleIsSingleUseAndCloseFailureDoesNotMaskEarlierFatalError(t *testing.T) {
	t.Run("stopped APIs and second Run", func(t *testing.T) {
		options, _, _, _ := task8NodeOptions(t, RecoveredState{})
		node, cancel, runResult := startTask8Follower(t, options)
		stopTask8Node(t, cancel, runResult)
		if err := node.Run(context.Background()); !errors.Is(err, ErrStopped) {
			t.Fatalf("second Run error = %v, want ErrStopped", err)
		}
		if _, err := node.Barrier(context.Background()); !errors.Is(err, ErrStopped) {
			t.Fatalf("Barrier after stop error = %v", err)
		}
		if _, err := node.SubscribeLeadership(context.Background(), 1); !errors.Is(err, ErrStopped) {
			t.Fatalf("Subscribe after stop error = %v", err)
		}
		if err := node.SubmitRPC(context.Background(), 2, PreVoteRequest{}); !errors.Is(err, ErrStopped) {
			t.Fatalf("SubmitRPC after stop error = %v", err)
		}
	})

	t.Run("close error after cancellation", func(t *testing.T) {
		closeFailure := errors.New("close failed")
		options, store, _, _ := task8NodeOptions(t, RecoveredState{})
		store.closeErr = closeFailure
		node, cancel, runResult := startTask8Follower(t, options)
		_ = node
		cancel()
		if err := <-runResult; !errors.Is(err, closeFailure) {
			t.Fatalf("Run error = %v, want close failure", err)
		}
	})

	t.Run("persistence error wins over close", func(t *testing.T) {
		persistFailure := errors.New("persist failed")
		closeFailure := errors.New("close failed")
		options, store, _, _ := task8NodeOptions(t, RecoveredState{})
		store.persistErr = persistFailure
		store.closeErr = closeFailure
		node, cancel, runResult := startTask8Follower(t, options)
		defer cancel()
		go func() { _ = node.SubmitRPC(context.Background(), 2, RequestVoteRequest{CandidateID: 2, Term: 1}) }()
		if err := <-runResult; !errors.Is(err, persistFailure) || errors.Is(err, closeFailure) {
			t.Fatalf("Run error = %v, want persistence failure without close masking", err)
		}
	})
}

func startAuthorizedTask8Node(t *testing.T, options NodeOptions, manual *clock.Manual, transport *task8Transport) (*Node, context.CancelFunc, <-chan error) {
	t.Helper()
	node, err := NewNode(options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- node.Run(ctx) }()
	<-node.Ready()
	manual.Advance(5 * time.Second)
	_ = task8RPCTo(t, transport.notify, 2, func(rpc RPC) bool { _, ok := rpc.(PreVoteRequest); return ok })
	_ = task8RPCTo(t, transport.notify, 3, func(rpc RPC) bool { _, ok := rpc.(PreVoteRequest); return ok })
	if err := node.SubmitRPC(context.Background(), 2, PreVoteResponse{
		ResponderID: 2, CandidateID: 1, RequestCurrentTerm: 0, ProspectiveTerm: 1, Granted: true,
	}); err != nil {
		t.Fatal(err)
	}
	_ = task8RPCTo(t, transport.notify, 2, func(rpc RPC) bool { _, ok := rpc.(RequestVoteRequest); return ok })
	_ = task8RPCTo(t, transport.notify, 3, func(rpc RPC) bool { _, ok := rpc.(RequestVoteRequest); return ok })
	if err := node.SubmitRPC(context.Background(), 2, RequestVoteResponse{
		ResponderID: 2, CandidateID: 1, Term: 1, RequestTerm: 1, Granted: true,
	}); err != nil {
		t.Fatal(err)
	}
	request := task8AppendRequestTo(t, transport.notify, 2, EntryNoOp)
	thirdRequest := task8AppendRequestTo(t, transport.notify, 3, EntryNoOp)
	if err := node.SubmitRPC(context.Background(), 2, appendSuccess(request, 2)); err != nil {
		t.Fatal(err)
	}
	if err := node.SubmitRPC(context.Background(), 3, appendSuccess(thirdRequest, 3)); err != nil {
		t.Fatal(err)
	}
	if status := node.Status(); status.Role != RoleLeader || status.Term != 1 || status.AppliedIndex != 1 {
		t.Fatalf("authorized node status = %#v", status)
	}
	return node, cancel, runResult
}

func task8AppendRequestTo(t *testing.T, messages <-chan PeerMessage, peerID uint16, kind EntryKind) AppendEntriesRequest {
	t.Helper()
	message := task8RPCTo(t, messages, peerID, func(rpc RPC) bool {
		request, ok := rpc.(AppendEntriesRequest)
		return ok && len(request.Entries) != 0 && request.Entries[len(request.Entries)-1].Kind == kind
	})
	return message.RPC.(AppendEntriesRequest)
}

func task8RPCTo(t *testing.T, messages <-chan PeerMessage, peerID uint16, matches func(RPC) bool) PeerMessage {
	t.Helper()
	for {
		message := <-messages
		if message.To == peerID && matches(message.RPC) {
			return message
		}
	}
}

func stopTask8Node(t *testing.T, cancel context.CancelFunc, runResult <-chan error) {
	t.Helper()
	cancel()
	if err := <-runResult; err != nil {
		t.Fatalf("Run shutdown error = %v", err)
	}
}

func task8NodeOptions(t *testing.T, state RecoveredState) (NodeOptions, *task8Store, *task8StateMachine, *clock.Manual) {
	t.Helper()
	identity, voters := testStorageIdentity(t, 1)
	state.Identity = identity
	store := &task8Store{state: state}
	machine := &task8StateMachine{}
	manual := clock.NewManual(time.Unix(100, 0))
	return NodeOptions{
		LocalID: 1, Voters: voters, Identity: identity,
		Store: store, StateMachine: machine,
		Transport: &task8Transport{result: TransportAccepted},
		Clock:     manual, Random: task8ZeroOffsetRandom{},
		ElectionTimeoutMin: 5 * time.Second,
		ElectionTimeoutMax: 10 * time.Second,
		HeartbeatInterval:  1 * time.Second,
	}, store, machine, manual
}

func mustTask8NoOp(t *testing.T, index, term uint64) Entry {
	t.Helper()
	entry, err := NewEntry(index, term, EntryNoOp, nil)
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
