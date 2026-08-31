package raft

import (
	"context"
	"errors"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/config"
	"github.com/aaditya/cs425mp3/internal/wire"
)

func TestTransportQueueOwnsMessagesAndPinsBoundary(t *testing.T) {
	queue := newPeerIntentQueue(PeerQueueCapacity)
	command := []byte("owned")
	entry, err := NewEntry(1, 1, EntryCommand, command)
	if err != nil {
		t.Fatal(err)
	}
	message := PeerMessage{To: 2, RPC: RequestVoteRequest{CandidateID: 1, Term: 1}}
	for index := 0; index < PeerQueueCapacity; index++ {
		if got := queue.offer(message); got != TransportAccepted {
			t.Fatalf("offer %d = %d, want accepted", index, got)
		}
	}
	if got := queue.offer(message); got != TransportUnavailable {
		t.Fatalf("boundary+1 offer = %d, want unavailable", got)
	}

	ownedQueue := newPeerIntentQueue(1)
	appendMessage := PeerMessage{To: 2, RPC: AppendEntriesRequest{
		LeaderID: 1, Term: 1, Generation: 1, Entries: []Entry{entry},
	}}
	if got := ownedQueue.offer(appendMessage); got != TransportAccepted {
		t.Fatalf("owned offer = %d, want accepted", got)
	}
	command[0] = 'X'
	appendMessage.RPC.(AppendEntriesRequest).Entries[0].command[0] = 'Y'
	got, ok := ownedQueue.takeNow()
	if !ok {
		t.Fatal("owned queue is empty")
	}
	if command := got.RPC.(AppendEntriesRequest).Entries[0].CommandBytes(); string(command) != "owned" {
		t.Fatalf("owned command = %q, want owned", command)
	}
}

func TestTransportQueueCoalescesOnlyTailAppendIntent(t *testing.T) {
	queue := newPeerIntentQueue(3)
	first := PeerMessage{To: 2, RPC: AppendEntriesRequest{LeaderID: 1, Term: 3, Generation: 1, LeaderCommit: 1}}
	newest := PeerMessage{To: 2, RPC: AppendEntriesRequest{LeaderID: 1, Term: 3, Generation: 2, LeaderCommit: 2}}
	if queue.offer(first) != TransportAccepted || queue.offer(newest) != TransportAccepted {
		t.Fatal("append offers were unavailable")
	}
	if got := queue.length(); got != 1 {
		t.Fatalf("coalesced length = %d, want 1", got)
	}
	got, ok := queue.takeNow()
	if !ok || got.RPC.(AppendEntriesRequest).Generation != 2 {
		t.Fatalf("coalesced message = %#v, want generation 2", got)
	}

	var transfer TransferID
	transfer[0] = 1
	var snapshot SnapshotID
	snapshot[0] = 2
	queue = newPeerIntentQueue(3)
	chunks := []InstallSnapshotRequest{
		{LeaderID: 1, Term: 3, TransferID: transfer, SnapshotID: snapshot, TotalLength: 2, Offset: 0, Chunk: []byte{1}},
		{LeaderID: 1, Term: 3, TransferID: transfer, SnapshotID: snapshot, TotalLength: 2, Offset: 1, Chunk: []byte{2}, Done: true},
	}
	for _, chunk := range chunks {
		if queue.offer(PeerMessage{To: 2, RPC: chunk}) != TransportAccepted {
			t.Fatal("snapshot offer was unavailable")
		}
	}
	for index, want := range []uint64{0, 1} {
		got, ok := queue.takeNow()
		if !ok || got.RPC.(InstallSnapshotRequest).Offset != want {
			t.Fatalf("snapshot %d = %#v, want offset %d", index, got, want)
		}
	}
}

func TestTransportQueueCoalescesOnlyProvableAppendSupersession(t *testing.T) {
	entry, err := NewEntry(5, 3, EntryCommand, []byte("same"))
	if err != nil {
		t.Fatal(err)
	}
	different, err := NewEntry(5, 3, EntryCommand, []byte("different"))
	if err != nil {
		t.Fatal(err)
	}
	base := PeerMessage{To: 2, RPC: AppendEntriesRequest{
		LeaderID: 1, Term: 3, Generation: 7, PrevLogIndex: 4, PrevLogTerm: 2,
		LeaderCommit: 4, Entries: []Entry{entry},
	}, Requires: DurabilityPrerequisite{HardState: true, EntriesThrough: 5}}
	for _, test := range []struct {
		name   string
		mutate func(*PeerMessage)
	}{
		{name: "hard state requirement weakens", mutate: func(message *PeerMessage) { message.Requires.HardState = false }},
		{name: "entries requirement decreases", mutate: func(message *PeerMessage) { message.Requires.EntriesThrough = 4 }},
		{name: "previous index changes", mutate: func(message *PeerMessage) {
			rpc := message.RPC.(AppendEntriesRequest)
			rpc.PrevLogIndex++
			message.RPC = rpc
		}},
		{name: "previous term changes", mutate: func(message *PeerMessage) {
			rpc := message.RPC.(AppendEntriesRequest)
			rpc.PrevLogTerm++
			message.RPC = rpc
		}},
		{name: "entry content changes", mutate: func(message *PeerMessage) {
			rpc := message.RPC.(AppendEntriesRequest)
			rpc.Entries = []Entry{different}
			message.RPC = rpc
		}},
		{name: "equal generation changes commit", mutate: func(message *PeerMessage) {
			rpc := message.RPC.(AppendEntriesRequest)
			rpc.Generation = 7
			rpc.LeaderCommit++
			message.RPC = rpc
		}},
		{name: "leader commit decreases", mutate: func(message *PeerMessage) {
			rpc := message.RPC.(AppendEntriesRequest)
			rpc.LeaderCommit--
			message.RPC = rpc
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			queue := newPeerIntentQueue(2)
			replacement := PeerMessage{To: base.To, RPC: CloneRPC(base.RPC), Requires: base.Requires}
			rpc := replacement.RPC.(AppendEntriesRequest)
			rpc.Generation++
			replacement.RPC = rpc
			test.mutate(&replacement)
			if queue.offer(base) != TransportAccepted || queue.offer(replacement) != TransportAccepted {
				t.Fatal("append offers were unavailable")
			}
			if got := queue.length(); got != 2 {
				t.Fatalf("queue length = %d, want semantically distinct intents retained", got)
			}
		})
	}
}

func TestTransportQueueSafeAppendCoalescingPreservesOwnedReplacement(t *testing.T) {
	command := []byte("owned replacement")
	entry, err := NewEntry(5, 3, EntryCommand, command)
	if err != nil {
		t.Fatal(err)
	}
	first := PeerMessage{To: 2, RPC: AppendEntriesRequest{
		LeaderID: 1, Term: 3, Generation: 7, PrevLogIndex: 4, PrevLogTerm: 2,
		LeaderCommit: 4, Entries: []Entry{entry},
	}, Requires: DurabilityPrerequisite{HardState: true, EntriesThrough: 5}}
	replacement := PeerMessage{To: first.To, RPC: CloneRPC(first.RPC), Requires: first.Requires}
	rpc := replacement.RPC.(AppendEntriesRequest)
	rpc.Generation = 8
	rpc.LeaderCommit = 5
	replacement.RPC = rpc
	queue := newPeerIntentQueue(2)
	if queue.offer(first) != TransportAccepted || queue.offer(replacement) != TransportAccepted {
		t.Fatal("append offers were unavailable")
	}
	if got := queue.length(); got != 1 {
		t.Fatalf("safe supersession length = %d, want one", got)
	}
	command[0] = 'X'
	replacement.RPC.(AppendEntriesRequest).Entries[0].command[0] = 'Y'
	got, ok := queue.takeNow()
	if !ok {
		t.Fatal("coalesced queue is empty")
	}
	appendRPC := got.RPC.(AppendEntriesRequest)
	if appendRPC.Generation != 8 || appendRPC.LeaderCommit != 5 || string(appendRPC.Entries[0].CommandBytes()) != "owned replacement" {
		t.Fatalf("owned replacement = %#v command=%q", appendRPC, appendRPC.Entries[0].CommandBytes())
	}
}

func TestTransportQueueDoesNotCoalesceStrongerPersistenceRequirement(t *testing.T) {
	entry, err := NewEntry(5, 3, EntryCommand, []byte("same"))
	if err != nil {
		t.Fatal(err)
	}
	base := PeerMessage{To: 2, RPC: AppendEntriesRequest{
		LeaderID: 1, Term: 3, Generation: 7, PrevLogIndex: 4, PrevLogTerm: 2,
		LeaderCommit: 4, Entries: []Entry{entry},
	}}
	for _, test := range []struct {
		name     string
		requires DurabilityPrerequisite
	}{
		{name: "stronger HardState", requires: DurabilityPrerequisite{HardState: true}},
		{name: "stronger EntriesThrough", requires: DurabilityPrerequisite{EntriesThrough: 5}},
		{name: "both stronger", requires: DurabilityPrerequisite{HardState: true, EntriesThrough: 5}},
	} {
		t.Run(test.name, func(t *testing.T) {
			queue := newPeerIntentQueue(2)
			replacement := PeerMessage{To: base.To, RPC: CloneRPC(base.RPC), Requires: test.requires}
			rpc := replacement.RPC.(AppendEntriesRequest)
			rpc.Generation++
			replacement.RPC = rpc
			if queue.offer(base) != TransportAccepted || queue.offer(replacement) != TransportAccepted {
				t.Fatal("append offers were unavailable")
			}
			if got := queue.length(); got != 2 {
				t.Fatalf("queue length = %d, want stronger persistence intent retained", got)
			}
		})
	}
}

func TestTransportQueueDoesNotCoalesceAwayEntryBearingAppend(t *testing.T) {
	queue := newPeerIntentQueue(2)
	entry, err := NewEntry(1, 3, EntryCommand, []byte("keep"))
	if err != nil {
		t.Fatal(err)
	}
	entryIntent := PeerMessage{To: 2, RPC: AppendEntriesRequest{
		LeaderID: 1, Term: 3, Generation: 1, Entries: []Entry{entry},
	}}
	heartbeat := PeerMessage{To: 2, RPC: AppendEntriesRequest{
		LeaderID: 1, Term: 3, Generation: 2, PrevLogIndex: 1, PrevLogTerm: 3, LeaderCommit: 1,
	}}
	if queue.offer(entryIntent) != TransportAccepted || queue.offer(heartbeat) != TransportAccepted {
		t.Fatal("append offers were unavailable")
	}
	if got := queue.length(); got != 2 {
		t.Fatalf("queue length = %d, want both entry intent and heartbeat", got)
	}
	first, _ := queue.takeNow()
	if got := first.RPC.(AppendEntriesRequest).Entries; len(got) != 1 || string(got[0].CommandBytes()) != "keep" {
		t.Fatalf("first append entries = %#v, want retained entry", got)
	}
}

func TestTransportHandoffIsUnavailableOutsideRun(t *testing.T) {
	transport := newTask10Transport(t, task10TransportOptions{})
	message := PeerMessage{To: 2, RPC: PreVoteRequest{CandidateID: 1, CurrentTerm: 1, ProspectiveTerm: 2}}
	if got, err := transport.Handoff(message); err != nil || got != TransportUnavailable {
		t.Fatalf("before Run handoff = (%d, %v), want unavailable", got, err)
	}
}

func TestTransportHandoffUnavailableUntilEveryOwnerAcknowledgesStartup(t *testing.T) {
	transport := newTask10Transport(t, task10TransportOptions{})
	ownersEntered := make(chan struct{}, 3)
	releaseOwners := make(chan struct{})
	transport.beforeOwnerStart = func(context.Context, string, uint16) error {
		ownersEntered <- struct{}{}
		<-releaseOwners
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- transport.Run(ctx, newBlockingListener(), task10Ingress{}) }()
	for owner := 0; owner < 3; owner++ {
		select {
		case <-ownersEntered:
		case <-time.After(time.Second):
			t.Fatal("owner did not enter startup gate")
		}
	}
	if got, err := transport.Handoff(PeerMessage{To: 2, RPC: RequestVoteRequest{CandidateID: 1, Term: 1}}); err != nil || got != TransportUnavailable {
		t.Fatalf("pre-ownership Handoff = (%d, %v), want unavailable", got, err)
	}
	select {
	case <-transport.Ready():
		t.Fatal("Ready closed before owner acknowledgements")
	default:
	}
	close(releaseOwners)
	awaitClosed(t, transport.Ready())
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestTransportImmediateOwnerStartupFailurePrecedesReady(t *testing.T) {
	for _, test := range []struct {
		name   string
		owner  string
		peerID uint16
	}{
		{name: "accept owner", owner: "accept"},
		{name: "peer worker", owner: "peer", peerID: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			failure := errors.New("injected owner startup failure")
			transport := newTask10Transport(t, task10TransportOptions{})
			transport.beforeOwnerStart = func(_ context.Context, owner string, peerID uint16) error {
				if owner == test.owner && peerID == test.peerID {
					return failure
				}
				return nil
			}
			err := transport.Run(context.Background(), newBlockingListener(), task10Ingress{})
			if !errors.Is(err, failure) {
				t.Fatalf("Run error = %v, want owner failure", err)
			}
			select {
			case <-transport.Ready():
				t.Fatal("startup failure closed Ready")
			default:
			}
			awaitClosed(t, transport.Done())
		})
	}
}

func TestTransportReportedFatalIsNotMaskedByConcurrentCancellation(t *testing.T) {
	failure := errors.New("reported fatal")
	for attempt := 0; attempt < 100; attempt++ {
		transport := newTask10Transport(t, task10TransportOptions{})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		fatal := make(chan error, 1)
		transport.reportFatal(ctx, fatal, failure)
		select {
		case err := <-fatal:
			if !errors.Is(err, failure) {
				t.Fatalf("attempt %d fatal = %v", attempt, err)
			}
		default:
			t.Fatalf("attempt %d cancellation masked an already-reported fatal", attempt)
		}
	}
}

func TestTransportRunReturnsRecordedFatalOverSimultaneousCancellation(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previousProcs)
	for _, phase := range []string{"startup", "runtime"} {
		t.Run(phase, func(t *testing.T) {
			for attempt := 0; attempt < 25; attempt++ {
				listener := newTask10QueuedListener()
				if phase == "startup" {
					_ = listener.Close()
				}
				transport := newTask10Transport(t, task10TransportOptions{})
				recorded := make(chan struct{})
				releaseFatal := make(chan struct{})
				var recordedOnce sync.Once
				transport.afterFatalRecorded = func() {
					recordedOnce.Do(func() { close(recorded) })
					<-releaseFatal
				}
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan error, 1)
				go func() { done <- transport.Run(ctx, listener, task10Ingress{}) }()
				if phase == "runtime" {
					awaitClosed(t, transport.Ready())
					_ = listener.Close()
				}
				awaitClosed(t, recorded)
				cancel()
				close(releaseFatal)
				if err := <-done; !errors.Is(err, net.ErrClosed) {
					t.Fatalf("attempt %d Run error = %v, want recorded accept fatal", attempt, err)
				}
			}
		})
	}
}

func TestTransportIntentionalCancellationDoesNotManufactureFatal(t *testing.T) {
	transport := newTask10Transport(t, task10TransportOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- transport.Run(ctx, newBlockingListener(), task10Ingress{}) }()
	awaitClosed(t, transport.Ready())
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("intentional cancellation error = %v, want nil", err)
	}
}

func TestTransportCancellationDrainsTrulyFullQueueAndJoins(t *testing.T) {
	dialStarted := make(chan struct{})
	var dialOnce sync.Once
	transport := newTask10Transport(t, task10TransportOptions{dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
		dialOnce.Do(func() { close(dialStarted) })
		<-ctx.Done()
		return nil, ctx.Err()
	}})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- transport.Run(ctx, newBlockingListener(), task10Ingress{}) }()
	awaitClosed(t, transport.Ready())
	if got, err := transport.Handoff(PeerMessage{To: 2, RPC: RequestVoteRequest{CandidateID: 1, Term: 1}}); err != nil || got != TransportAccepted {
		t.Fatalf("in-flight Handoff = (%d, %v)", got, err)
	}
	awaitClosed(t, dialStarted)
	for index := 0; index < PeerQueueCapacity; index++ {
		message := PeerMessage{To: 2, RPC: RequestVoteRequest{CandidateID: 1, Term: uint64(index + 2)}}
		if got, err := transport.Handoff(message); err != nil || got != TransportAccepted {
			t.Fatalf("fill Handoff %d = (%d, %v)", index, got, err)
		}
	}
	if got := transport.queues[2].length(); got != PeerQueueCapacity {
		t.Fatalf("queue length = %d, want truly full %d", got, PeerQueueCapacity)
	}
	if got, err := transport.Handoff(PeerMessage{To: 2, RPC: RequestVoteRequest{CandidateID: 1, Term: 100}}); err != nil || got != TransportUnavailable {
		t.Fatalf("full+1 Handoff = (%d, %v), want unavailable", got, err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not join full-queue worker")
	}
	if got := transport.queues[2].length(); got != 0 {
		t.Fatalf("drained queue length = %d, want zero", got)
	}
}

func TestTransportCapsInboundHandlersAndJoinsThemOnCancellation(t *testing.T) {
	listener := newTask10QueuedListener()
	transport := newTask10Transport(t, task10TransportOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- transport.Run(ctx, listener, task10Ingress{}) }()
	awaitClosed(t, transport.Ready())
	clients := make([]net.Conn, 0, TransportMaxInboundConnections+1)
	defer func() {
		for _, client := range clients {
			_ = client.Close()
		}
	}()
	for index := 0; index < TransportMaxInboundConnections; index++ {
		client, server := net.Pipe()
		clients = append(clients, client)
		listener.offer(server)
		awaitTask10ConnectionCount(t, transport, index+1)
	}
	overflowClient, overflowServer := net.Pipe()
	clients = append(clients, overflowClient)
	listener.offer(overflowServer)
	if err := overflowClient.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	if _, err := overflowClient.Read(one[:]); err == nil {
		t.Fatal("handler capacity+1 connection remained open")
	}
	awaitTask10ConnectionCount(t, transport, TransportMaxInboundConnections)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("transport did not join maximum inbound handlers")
	}
	awaitTask10ConnectionCount(t, transport, 0)
}

func TestTransportRunKeepsSlowPeerFromDelayingAnotherAndJoins(t *testing.T) {
	listener := newBlockingListener()
	var dialMu sync.Mutex
	dialed := make(map[string]int)
	transport := newTask10Transport(t, task10TransportOptions{dial: func(ctx context.Context, _, address string) (net.Conn, error) {
		dialMu.Lock()
		dialed[address]++
		dialMu.Unlock()
		<-ctx.Done()
		return nil, ctx.Err()
	}})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- transport.Run(ctx, listener, task10Ingress{}) }()
	awaitClosed(t, transport.Ready())

	for _, peerID := range []uint16{2, 3} {
		message := PeerMessage{To: peerID, RPC: RequestVoteRequest{CandidateID: 1, Term: 1}}
		if got, err := transport.Handoff(message); err != nil || got != TransportAccepted {
			t.Fatalf("peer %d handoff = (%d, %v), want accepted", peerID, got, err)
		}
	}
	deadline := time.Now().Add(time.Second)
	for {
		dialMu.Lock()
		count := len(dialed)
		dialMu.Unlock()
		if count == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dialed endpoints = %#v, want both remote voters", dialed)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run after cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not join peer workers and accept loop")
	}
	if got, err := transport.Handoff(PeerMessage{To: 2, RPC: RequestVoteRequest{CandidateID: 1, Term: 1}}); err != nil || got != TransportUnavailable {
		t.Fatalf("after Run handoff = (%d, %v), want unavailable", got, err)
	}
}

func TestTransportRunTreatsRuntimeListenerCloseAsFatal(t *testing.T) {
	listener := newBlockingListener()
	transport := newTask10Transport(t, task10TransportOptions{})
	done := make(chan error, 1)
	go func() { done <- transport.Run(context.Background(), listener, task10Ingress{}) }()
	awaitClosed(t, transport.Ready())
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("runtime listener close returned nil, want fatal accept error")
		}
	case <-time.After(time.Second):
		t.Fatal("runtime listener close did not terminate transport")
	}
}

type task10TransportOptions struct {
	dial       TCPDialContext
	requestIDs RequestIDSource
	backoff    BackoffFunc
	clock      clock.Clock
	rpcTimeout time.Duration
}

func newTask10Transport(t *testing.T, seams task10TransportOptions) *TCPTransport {
	t.Helper()
	voters := task10Voters(t)
	clusterID := [16]byte{1}
	options := TCPTransportOptions{
		LocalID:                1,
		Voters:                 voters,
		ClusterID:              clusterID,
		ApplicationFingerprint: task5ApplicationFingerprint,
		Authenticator:          wire.NewHMACAuthenticator([]byte("01234567890123456789012345678901")),
		Clock:                  seams.clock,
		ReplayWindow:           2 * time.Minute,
		RPCTimeout:             seams.rpcTimeout,
		RequestIDs:             seams.requestIDs,
		DialContext:            seams.dial,
		Backoff:                seams.backoff,
	}
	if options.RPCTimeout == 0 {
		options.RPCTimeout = 100 * time.Millisecond
	}
	if options.Clock == nil {
		options.Clock = clock.NewManual(time.Unix(1000, 0))
	}
	if options.RequestIDs == nil {
		options.RequestIDs = &task10RequestIDs{}
	}
	if options.DialContext == nil {
		options.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}
	}
	if options.Backoff == nil {
		options.Backoff = func(ctx context.Context, _ time.Duration) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				return nil
			}
		}
	}
	transport, err := NewTCPTransport(options)
	if err != nil {
		t.Fatal(err)
	}
	return transport
}

func task10Voters(t *testing.T) VoterSet {
	t.Helper()
	voters, err := NewVoterSet([]config.RaftVoter{
		{NodeID: 1, Endpoint: "127.0.0.1:31001"},
		{NodeID: 2, Endpoint: "127.0.0.1:31002"},
		{NodeID: 3, Endpoint: "127.0.0.1:31003"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return voters
}

type task10RequestIDs struct {
	mu    sync.Mutex
	next  uint64
	calls []wire.RequestID
}

func (source *task10RequestIDs) NextRequestID() (wire.RequestID, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.next++
	id := wire.RequestID{byte(source.next), byte(source.next >> 8), byte(source.next >> 16), byte(source.next >> 24)}
	source.calls = append(source.calls, id)
	return id, nil
}

type task10Ingress struct {
	called chan RPC
}

func (ingress task10Ingress) SubmitRPC(_ context.Context, _ uint16, rpc RPC) error {
	if ingress.called != nil {
		ingress.called <- CloneRPC(rpc)
	}
	return nil
}

type blockingListener struct {
	closed chan struct{}
	once   sync.Once
}

type task10QueuedListener struct {
	connections chan net.Conn
	closed      chan struct{}
	once        sync.Once
}

func newTask10QueuedListener() *task10QueuedListener {
	return &task10QueuedListener{connections: make(chan net.Conn), closed: make(chan struct{})}
}

func (listener *task10QueuedListener) Accept() (net.Conn, error) {
	select {
	case connection := <-listener.connections:
		return connection, nil
	case <-listener.closed:
		return nil, net.ErrClosed
	}
}

func (listener *task10QueuedListener) offer(connection net.Conn) {
	select {
	case listener.connections <- connection:
	case <-listener.closed:
		_ = connection.Close()
	}
}

func (listener *task10QueuedListener) Close() error {
	listener.once.Do(func() { close(listener.closed) })
	return nil
}

func (*task10QueuedListener) Addr() net.Addr { return task10Addr("127.0.0.1:0") }

func awaitTask10ConnectionCount(t *testing.T, transport *TCPTransport, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		got := 0
		transport.connections.Range(func(_, _ any) bool {
			got++
			return true
		})
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("tracked connections = %d, want %d", got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func newBlockingListener() *blockingListener { return &blockingListener{closed: make(chan struct{})} }

func (listener *blockingListener) Accept() (net.Conn, error) {
	<-listener.closed
	return nil, net.ErrClosed
}

func (listener *blockingListener) Close() error {
	listener.once.Do(func() { close(listener.closed) })
	return nil
}

func (*blockingListener) Addr() net.Addr { return task10Addr("127.0.0.1:0") }

type task10Addr string

func (address task10Addr) Network() string { return "tcp" }
func (address task10Addr) String() string  { return string(address) }

func awaitClosed(t *testing.T, channel <-chan struct{}) {
	t.Helper()
	select {
	case <-channel:
	case <-time.After(time.Second):
		t.Fatal("channel did not close")
	}
}

func readFrameOrError(stream *wire.TCPFrameStream) (wire.Frame, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return stream.ReadFrame(ctx)
}

func writeFrameForTest(stream *wire.TCPFrameStream, frame wire.Frame) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return stream.WriteFrame(ctx, frame)
}

func expectClosedStream(t *testing.T, stream *wire.TCPFrameStream) {
	t.Helper()
	if _, err := readFrameOrError(stream); err == nil {
		t.Fatal("stream remained open after protocol rejection")
	}
}
