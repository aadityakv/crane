package raft

import (
	"context"
	"net"
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
}

func newTask10Transport(t *testing.T, seams task10TransportOptions) *TCPTransport {
	t.Helper()
	voters := task10Voters(t)
	clusterID := [16]byte{1}
	options := TCPTransportOptions{
		LocalID:       1,
		Voters:        voters,
		ClusterID:     clusterID,
		Authenticator: wire.NewHMACAuthenticator([]byte("01234567890123456789012345678901")),
		Clock:         clock.NewManual(time.Unix(1000, 0)),
		ReplayWindow:  2 * time.Minute,
		RPCTimeout:    100 * time.Millisecond,
		RequestIDs:    seams.requestIDs,
		DialContext:   seams.dial,
		Backoff:       seams.backoff,
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
	next  byte
	calls []wire.RequestID
}

func (source *task10RequestIDs) NextRequestID() (wire.RequestID, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.next++
	id := wire.RequestID{source.next}
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
