package control

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aadityakv/crane/internal/crane/clientstate"
	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
	"github.com/aadityakv/crane/internal/crane/state"
	"github.com/aadityakv/crane/internal/wire"
)

// clientTestTopology returns one small finite valid topology.
func clientTestTopology(t *testing.T, name string) model.TopologySpec {
	t.Helper()
	topology := model.TopologySpec{
		SchemaVersion: 1,
		Name:          name,
		Stages: []model.StageSpec{
			{StageID: 1, Name: "numbers", Role: model.Source, Parallelism: 1, Operator: model.OperatorSpec{Name: "range", Version: 1, Settings: []model.Setting{{Key: "end_exclusive", Value: "4"}, {Key: "start", Value: "1"}}}},
			{StageID: 2, Name: "scaled", Role: model.Transform, Parallelism: 1, Operator: model.OperatorSpec{Name: "multiply", Version: 1, Settings: []model.Setting{{Key: "factor", Value: "3"}}}},
			{StageID: 3, Name: "collected", Role: model.Sink, Parallelism: 1, Operator: model.OperatorSpec{Name: "collect", Version: 1}},
		},
		Edges: []model.EdgeSpec{
			{EdgeID: 1, SourceStageID: 1, DestinationStageID: 2, Routing: model.Shuffle},
			{EdgeID: 2, SourceStageID: 2, DestinationStageID: 3, Routing: model.Shuffle},
		},
		RegistryFingerprint: model.RegistryFingerprint(),
	}
	if _, err := model.ValidateTopology(topology); err != nil {
		t.Fatalf("client test topology is invalid: %v", err)
	}
	return topology
}

// clientHarness pairs one real +6 service fixture with a durable client store
// and a recording dial seam that maps every derived endpoint to the fixture.
type clientHarness struct {
	t         *testing.T
	fixture   *serviceFixture
	statePath string
	store     *clientstate.ClientStore

	mu       sync.Mutex
	dialed   []string
	dialErr  func(address string, attempt int) error
	dialHook func(address string, attempt int)
	connWrap func(conn net.Conn, attempt int) net.Conn
}

func newClientHarness(t *testing.T, openGate bool) *clientHarness {
	t.Helper()
	fixture := newServiceFixture(t, state.NewMachine())
	if openGate {
		fixture.seedEpochAndOpenGate()
	}
	fixture.start()
	stateDir := filepath.Join(t.TempDir(), "crane-client")
	if err := os.Mkdir(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	harness := &clientHarness{t: t, fixture: fixture, statePath: filepath.Join(stateDir, "state.crane")}
	harness.store = harness.openStore()
	return harness
}

func (h *clientHarness) openStore() *clientstate.ClientStore {
	h.t.Helper()
	store, err := clientstate.OpenClientState(h.statePath, h.fixture.service.clusterID)
	if err != nil {
		h.t.Fatalf("open client state: %v", err)
	}
	return store
}

func (h *clientHarness) dial(ctx context.Context, address string) (net.Conn, error) {
	h.mu.Lock()
	h.dialed = append(h.dialed, address)
	attempt := len(h.dialed)
	dialErr, dialHook, connWrap := h.dialErr, h.dialHook, h.connWrap
	h.mu.Unlock()
	if dialHook != nil {
		dialHook(address, attempt)
	}
	if dialErr != nil {
		if err := dialErr(address, attempt); err != nil {
			return nil, err
		}
	}
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", h.fixture.address())
	if err != nil {
		return nil, err
	}
	if connWrap != nil {
		conn = connWrap(conn, attempt)
	}
	return conn, nil
}

func (h *clientHarness) dialedAddresses() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.dialed...)
}

func (h *clientHarness) options() ClientOptions {
	return ClientOptions{
		Config:         controlTestConfig(h.t, 2),
		Authenticator:  h.fixture.authenticator,
		Clock:          h.fixture.clock,
		Store:          h.store,
		MaxAttempts:    5,
		MaxRedirects:   4,
		RetryBackoff:   0,
		RequestTimeout: 2 * time.Second,
		Dial:           h.dial,
	}
}

func (h *clientHarness) client() *Client {
	h.t.Helper()
	return h.clientFrom(h.options())
}

func (h *clientHarness) clientFrom(options ClientOptions) *Client {
	h.t.Helper()
	client, err := NewClient(options)
	if err != nil {
		h.t.Fatalf("construct client: %v", err)
	}
	return client
}

// ambiguousDropConn waits for proof that the server answered, then surfaces a
// dropped connection before delivering any response byte.
type ambiguousDropConn struct{ net.Conn }

func (c ambiguousDropConn) Read(buffer []byte) (int, error) {
	single := make([]byte, 1)
	if _, err := c.Conn.Read(single); err != nil {
		return 0, err
	}
	return 0, io.ErrUnexpectedEOF
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// scriptedControlServer serves scripted single-frame responses over real +6
// framing without any service state.
func scriptedControlServer(t *testing.T, auth wire.Authenticator, clusterID [16]byte, respond func(wire.Frame) *wire.Frame) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			go func(connection net.Conn) {
				defer connection.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				limits := controlClientLimits(clusterID)
				frame, err := wire.ReadTCPFrame(ctx, connection, auth, limits, 2*time.Second)
				if err != nil {
					return
				}
				response := respond(frame)
				if response == nil {
					return
				}
				// The scripted response may deliberately name another cluster,
				// so write without the expected-cluster restriction.
				open := wire.DefaultLimits()
				open.MaxFrameSize = limits.MaxFrameSize
				_ = wire.WriteTCPFrame(ctx, connection, *response, auth, open, 2*time.Second)
			}(connection)
		}
	}()
	return listener.Addr().String()
}

func scriptedResponseFrame(t *testing.T, request wire.Frame, clusterID [16]byte, message protocol.ControlMessage) *wire.Frame {
	t.Helper()
	payload, err := protocol.MarshalControlMessage(message)
	if err != nil {
		t.Errorf("marshal scripted response: %v", err)
		return nil
	}
	return &wire.Frame{Header: wire.Header{
		Version: wire.Version1, Message: message.MessageType(), ClusterID: clusterID, SenderID: 1,
		RequestID: request.Header.RequestID, TimestampMillis: request.Header.TimestampMillis, Codec: wire.CodecBinary,
	}, Payload: payload}
}

func expectedSubmitJobID(t *testing.T, store *clientstate.ClientStore, sequence uint64, topology model.TopologySpec) model.JobID {
	t.Helper()
	validated, err := model.ValidateTopology(topology)
	if err != nil {
		t.Fatal(err)
	}
	return model.DeriveJobID(model.ClientRequestID{ClientID: store.State().ClientID, Sequence: sequence}, validated.Digest())
}

func TestClientSubmitReservesPendingBeforeSendAndResolvesDurably(t *testing.T) {
	harness := newClientHarness(t, true)
	topology := clientTestTopology(t, "reserve-before-send")

	var observedPending []byte
	var observedSequence uint64
	harness.dialHook = func(string, int) {
		// The durable reservation must be complete before the first send.
		inspection, err := clientstate.OpenClientState(harness.statePath, harness.fixture.service.clusterID)
		if err != nil {
			t.Errorf("inspect state during dial: %v", err)
			return
		}
		state := inspection.State()
		observedPending = state.Pending
		observedSequence = state.NextSequence
	}

	client := harness.client()
	job, revision, err := client.Submit(testContext(t), topology)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if revision != 1 {
		t.Fatalf("submit revision = %d, want the validated first durable revision 1", revision)
	}
	if len(observedPending) == 0 || observedSequence != 1 {
		t.Fatalf("first send observed pending=%d bytes sequence=%d, want durable reservation", len(observedPending), observedSequence)
	}
	if want := expectedSubmitJobID(t, harness.store, 1, topology); job != want {
		t.Fatalf("submit job = %x, want %x", job, want)
	}

	finalState := harness.store.State()
	if finalState.NextSequence != 2 || len(finalState.Pending) != 0 {
		t.Fatalf("post-submit state = %#v", finalState)
	}
	resolved, err := protocol.UnmarshalControlMessage(wire.MessageCraneSubmitResponse, finalState.Resolved)
	if err != nil {
		t.Fatalf("resolution bytes are not the durable submit response: %v", err)
	}
	response := resolved.(protocol.SubmitResponse)
	if response.JobID != job || response.JobControlRevision != 1 {
		t.Fatalf("durable resolution = %#v", response)
	}
	if _, ok := viewJob(harness.fixture.machine.View(), job); !ok {
		t.Fatal("submitted job is not retained in the replicated view")
	}
}

func TestClientSubmitRetryReplaysExactBytesAfterAmbiguousDrop(t *testing.T) {
	harness := newClientHarness(t, true)
	topology := clientTestTopology(t, "ambiguous-drop-retry")
	harness.connWrap = func(conn net.Conn, attempt int) net.Conn {
		if attempt == 1 {
			return ambiguousDropConn{Conn: conn}
		}
		return conn
	}

	client := harness.client()
	job, _, err := client.Submit(testContext(t), topology)
	if err != nil {
		t.Fatalf("submit after ambiguous drop: %v", err)
	}
	if want := expectedSubmitJobID(t, harness.store, 1, topology); job != want {
		t.Fatalf("submit job = %x, want %x", job, want)
	}
	if dialed := harness.dialedAddresses(); len(dialed) != 2 {
		t.Fatalf("dialed %d times, want exactly one retry: %v", len(dialed), dialed)
	}
	proposals := harness.fixture.raft.capturedProposals()
	if len(proposals) != 2 || !bytes.Equal(proposals[0], proposals[1]) {
		t.Fatalf("retry proposed %d commands, want two byte-identical replays", len(proposals))
	}
	view := harness.fixture.machine.View()
	record, ok := viewJob(view, job)
	if !ok || record.JobControlRevision != 1 {
		t.Fatal("replayed submit must resolve to exactly one durable job")
	}
}

func TestClientSubmitRetryStopsAtBoundedAttempts(t *testing.T) {
	harness := newClientHarness(t, true)
	topology := clientTestTopology(t, "bounded-attempts")
	harness.connWrap = func(conn net.Conn, _ int) net.Conn { return ambiguousDropConn{Conn: conn} }

	client := harness.client()
	_, _, err := client.Submit(testContext(t), topology)
	if !errors.Is(err, ErrClientAttemptsExhausted) {
		t.Fatalf("submit with every response dropped = %v, want ErrClientAttemptsExhausted", err)
	}
	if dialed := harness.dialedAddresses(); len(dialed) != 5 {
		t.Fatalf("dialed %d times, want the exact bounded budget 5", len(dialed))
	}
	pending := harness.store.State()
	if len(pending.Pending) == 0 || pending.NextSequence != 1 {
		t.Fatalf("exhausted submit lost its durable reservation: %#v", pending)
	}

	// Recovery resolves the exact same reservation without a new sequence.
	harness.connWrap = nil
	job, _, err := harness.client().Submit(testContext(t), topology)
	if err != nil {
		t.Fatalf("resume submit: %v", err)
	}
	if want := expectedSubmitJobID(t, harness.store, 1, topology); job != want {
		t.Fatalf("resumed job = %x, want %x", job, want)
	}
	if state := harness.store.State(); state.NextSequence != 2 || len(state.Pending) != 0 {
		t.Fatalf("resumed state = %#v", state)
	}
}

func TestClientSubmitCrashResumeAcrossRestartRetriesSameIdentity(t *testing.T) {
	harness := newClientHarness(t, true)
	topology := clientTestTopology(t, "crash-resume")
	harness.dialErr = func(string, int) error { return errors.New("injected dial failure") }

	// Crash after the pending persist: the reservation exists, no server saw it.
	_, _, err := harness.client().Submit(testContext(t), topology)
	if !errors.Is(err, ErrClientAttemptsExhausted) {
		t.Fatalf("unsendable submit = %v, want ErrClientAttemptsExhausted", err)
	}
	if proposals := harness.fixture.raft.capturedProposals(); len(proposals) != 0 {
		t.Fatalf("server observed %d proposals before any send", len(proposals))
	}

	// Restart: a reopened store and fresh client resume the exact identity.
	harness.dialErr = nil
	harness.store = harness.openStore()
	job, _, err := harness.client().Submit(testContext(t), topology)
	if err != nil {
		t.Fatalf("post-restart submit: %v", err)
	}
	if want := expectedSubmitJobID(t, harness.store, 1, topology); job != want {
		t.Fatalf("post-restart job = %x, want sequence-1 identity %x", job, want)
	}
	if state := harness.store.State(); state.NextSequence != 2 {
		t.Fatalf("post-restart sequence = %d, want 2", state.NextSequence)
	}

	// A repeated submit after resolution is a new command with a new identity.
	second, _, err := harness.client().Submit(testContext(t), topology)
	if err != nil {
		t.Fatalf("second submit: %v", err)
	}
	if want := expectedSubmitJobID(t, harness.store, 2, topology); second != want || second == job {
		t.Fatalf("second job = %x, want distinct sequence-2 identity %x", second, want)
	}
}

func TestClientResumePendingResolvesReservationOnce(t *testing.T) {
	harness := newClientHarness(t, true)
	topology := clientTestTopology(t, "resume-pending-api")
	harness.dialErr = func(string, int) error { return errors.New("injected dial failure") }
	if _, _, err := harness.client().Submit(testContext(t), topology); !errors.Is(err, ErrClientAttemptsExhausted) {
		t.Fatalf("unsendable submit = %v, want ErrClientAttemptsExhausted", err)
	}

	harness.dialErr = nil
	client := harness.client()
	resumed, err := client.ResumePending(testContext(t))
	if err != nil || !resumed {
		t.Fatalf("resume pending = (%v, %v), want (true, nil)", resumed, err)
	}
	if state := harness.store.State(); state.NextSequence != 2 || len(state.Pending) != 0 {
		t.Fatalf("resumed state = %#v", state)
	}
	if resumed, err := client.ResumePending(testContext(t)); err != nil || resumed {
		t.Fatalf("second resume = (%v, %v), want (false, nil)", resumed, err)
	}
}

func TestClientCancelRetryResolvesConsumedRejectionsExactly(t *testing.T) {
	harness := newClientHarness(t, true)
	topology := clientTestTopology(t, "cancel-matrix")
	client := harness.client()
	ctx := testContext(t)
	job, _, err := client.Submit(ctx, topology)
	if err != nil {
		t.Fatal(err)
	}

	// A stale expected revision is deterministically rejected and durably
	// consumes the client sequence, so the client must advance.
	_, err = client.Cancel(ctx, job, 7)
	var rejection *RequestRejectedError
	if !errors.As(err, &rejection) || rejection.Code != protocol.ControlErrorRevisionMismatch {
		t.Fatalf("stale cancel = %v, want RevisionMismatch rejection", err)
	}
	if state := harness.store.State(); state.NextSequence != 3 || len(state.Pending) != 0 {
		t.Fatalf("consumed rejection state = %#v, want advanced resolved sequence", state)
	}

	// An unknown retained job is NotFound and also consumes the sequence.
	_, err = client.Cancel(ctx, model.JobID{0xde, 0xad, 1}, 1)
	if !errors.As(err, &rejection) || rejection.Code != protocol.ControlErrorNotFound {
		t.Fatalf("unknown-job cancel = %v, want NotFound rejection", err)
	}
	if state := harness.store.State(); state.NextSequence != 4 {
		t.Fatalf("NotFound sequence = %d, want 4", state.NextSequence)
	}

	// The exact-revision cancel succeeds with the validated +1 revision.
	revision, err := client.Cancel(ctx, job, 1)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if revision != 2 {
		t.Fatalf("cancel revision = %d, want 2", revision)
	}
	if state := harness.store.State(); state.NextSequence != 5 {
		t.Fatalf("post-cancel sequence = %d, want 5", state.NextSequence)
	}
}

func TestClientRedirectFollowsCheckedLeaderHint(t *testing.T) {
	harness := newClientHarness(t, true)
	topology := clientTestTopology(t, "redirect-follow")
	harness.fixture.raft.setLeader(false, 2)
	harness.dialHook = func(address string, _ int) {
		if address == "127.0.0.2:19206" {
			harness.fixture.raft.setLeader(true, 0)
		}
	}

	job, _, err := harness.client().Submit(testContext(t), topology)
	if err != nil {
		t.Fatalf("submit through redirect: %v", err)
	}
	if want := expectedSubmitJobID(t, harness.store, 1, topology); job != want {
		t.Fatalf("redirected job = %x, want %x", job, want)
	}
	dialed := harness.dialedAddresses()
	if len(dialed) != 2 || dialed[0] != "127.0.0.1:19106" || dialed[1] != "127.0.0.2:19206" {
		t.Fatalf("dialed = %v, want the hinted leader's derived +6 endpoint", dialed)
	}
}

// TestClientRedirectToUnreachableLeaderFallsBackToVoters pins the Task 24
// leader-loss transient: a follower's checked hint names a leader that died
// before its followers learned of a successor, so the hinted +6 endpoint
// refuses connections. The client must not spend every remaining attempt on
// that endpoint; it falls back to the remaining static voters, which answer
// once leadership settles.
func TestClientRedirectToUnreachableLeaderFallsBackToVoters(t *testing.T) {
	harness := newClientHarness(t, true)
	topology := clientTestTopology(t, "redirect-dead-leader")
	harness.fixture.raft.setLeader(false, 2)
	harness.dialErr = func(address string, _ int) error {
		if address == "127.0.0.2:19206" {
			return &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
		}
		return nil
	}
	harness.dialHook = func(address string, attempt int) {
		if attempt >= 3 {
			harness.fixture.raft.setLeader(true, 0)
		}
	}

	job, _, err := harness.client().Submit(testContext(t), topology)
	if err != nil {
		t.Fatalf("submit past a dead hinted leader: %v", err)
	}
	if want := expectedSubmitJobID(t, harness.store, 1, topology); job != want {
		t.Fatalf("job = %x, want %x", job, want)
	}
	dialed := harness.dialedAddresses()
	if len(dialed) != 3 || dialed[0] != "127.0.0.1:19106" || dialed[1] != "127.0.0.2:19206" || dialed[2] == "127.0.0.2:19206" {
		t.Fatalf("dialed = %v, want the hint tried once and then a remaining static voter", dialed)
	}
}

// TestClientAttemptsExhaustedReportsReachedEndpointError pins the diagnostic
// half of the Task 24 leader-loss transient: when reached voters drop the
// exchange (an unauthorized sender gets no response) and a later attempt
// cannot dial a dead voter at all, the exhausted error names what the
// reached endpoint did, not only the last dial failure.
func TestClientAttemptsExhaustedReportsReachedEndpointError(t *testing.T) {
	harness := newClientHarness(t, true)
	harness.dialErr = func(address string, attempt int) error {
		if attempt%2 == 1 {
			return &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
		}
		return nil
	}
	harness.connWrap = func(conn net.Conn, _ int) net.Conn {
		_ = conn.Close()
		return conn
	}
	_, err := harness.client().Status(testContext(t), model.JobID{1})
	if !errors.Is(err, ErrClientAttemptsExhausted) || !strings.Contains(err.Error(), "last reached-endpoint error") || !strings.Contains(err.Error(), "last dial error") {
		t.Fatalf("exhausted error = %v, want both the reached-endpoint and dial causes", err)
	}
}

func TestClientRedirectRejectsLoopsAndPreservesReservation(t *testing.T) {
	harness := newClientHarness(t, true)
	topology := clientTestTopology(t, "redirect-loop")
	harness.fixture.raft.setLeader(false, 2)

	_, _, err := harness.client().Submit(testContext(t), topology)
	if !errors.Is(err, ErrClientRedirectLoop) {
		t.Fatalf("looping redirects = %v, want ErrClientRedirectLoop", err)
	}
	if state := harness.store.State(); len(state.Pending) == 0 || state.NextSequence != 1 {
		t.Fatalf("redirect loop lost the reservation: %#v", state)
	}

	// Leadership recovery resumes the same reservation to success.
	harness.fixture.raft.setLeader(true, 0)
	job, _, err := harness.client().Submit(testContext(t), topology)
	if err != nil {
		t.Fatalf("post-loop resume: %v", err)
	}
	if want := expectedSubmitJobID(t, harness.store, 1, topology); job != want {
		t.Fatalf("post-loop job = %x, want %x", job, want)
	}
}

func TestClientRedirectRejectsUntrustedEndpoints(t *testing.T) {
	harness := newClientHarness(t, true)
	topology := clientTestTopology(t, "redirect-untrusted")
	clusterID := harness.fixture.service.clusterID
	address := scriptedControlServer(t, harness.fixture.authenticator, clusterID, func(frame wire.Frame) *wire.Frame {
		return scriptedResponseFrame(t, frame, clusterID, protocol.LeaderRedirect{Endpoints: []string{"127.0.0.9:19906"}})
	})
	options := harness.options()
	options.Dial = func(ctx context.Context, _ string) (net.Conn, error) {
		dialer := net.Dialer{}
		return dialer.DialContext(ctx, "tcp", address)
	}

	_, _, err := harness.clientFrom(options).Submit(testContext(t), topology)
	if !errors.Is(err, ErrClientRedirectUntrusted) {
		t.Fatalf("foreign redirect = %v, want ErrClientRedirectUntrusted", err)
	}
	if state := harness.store.State(); len(state.Pending) == 0 || state.NextSequence != 1 {
		t.Fatalf("untrusted redirect lost the reservation: %#v", state)
	}
}

func TestClientRetryRejectsWrongClusterResponses(t *testing.T) {
	harness := newClientHarness(t, true)
	topology := clientTestTopology(t, "wrong-cluster")
	clusterID := harness.fixture.service.clusterID
	foreign := [16]byte{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9}
	address := scriptedControlServer(t, harness.fixture.authenticator, clusterID, func(frame wire.Frame) *wire.Frame {
		response := scriptedResponseFrame(t, frame, clusterID, protocol.LeaderRedirect{Endpoints: []string{"127.0.0.1:19106"}})
		if response != nil {
			response.Header.ClusterID = foreign
		}
		return response
	})
	options := harness.options()
	options.MaxAttempts = 3
	options.Dial = func(ctx context.Context, _ string) (net.Conn, error) {
		dialer := net.Dialer{}
		return dialer.DialContext(ctx, "tcp", address)
	}

	_, _, err := harness.clientFrom(options).Submit(testContext(t), topology)
	if !errors.Is(err, ErrClientAttemptsExhausted) {
		t.Fatalf("wrong-cluster responses = %v, want bounded ErrClientAttemptsExhausted", err)
	}
	if state := harness.store.State(); len(state.Pending) == 0 || state.NextSequence != 1 {
		t.Fatalf("wrong-cluster exchange mutated the reservation: %#v", state)
	}
}

func TestClientReportsForfeitedDedupIdentityAfterStateRollback(t *testing.T) {
	harness := newClientHarness(t, true)
	ctx := testContext(t)
	if _, _, err := harness.client().Submit(ctx, clientTestTopology(t, "rollback-one")); err != nil {
		t.Fatal(err)
	}
	backup := readStateFile(t, harness.statePath)
	if _, _, err := harness.client().Submit(ctx, clientTestTopology(t, "rollback-two")); err != nil {
		t.Fatal(err)
	}

	// Roll the identity store back to before the second submit, forging a
	// lost-state restart that would reuse sequence 2 with different bytes.
	writeStateFile(t, harness.statePath, backup)
	harness.store = harness.openStore()
	_, _, err := harness.client().Submit(ctx, clientTestTopology(t, "rollback-three"))
	if !errors.Is(err, ErrClientIdentityForfeited) {
		t.Fatalf("rolled-back identity submit = %v, want ErrClientIdentityForfeited", err)
	}
	if state := harness.store.State(); state.NextSequence != 2 || len(state.Pending) == 0 {
		t.Fatalf("forfeited identity mutated local state: %#v", state)
	}
}

func TestClientRetryWaitsThroughRetryableStarting(t *testing.T) {
	harness := newClientHarness(t, false)
	topology := clientTestTopology(t, "retryable-starting")
	harness.dialHook = func(_ string, attempt int) {
		if attempt == 3 {
			harness.fixture.seedEpochAndOpenGate()
		}
	}

	job, _, err := harness.client().Submit(testContext(t), topology)
	if err != nil {
		t.Fatalf("submit through Starting retries: %v", err)
	}
	if want := expectedSubmitJobID(t, harness.store, 1, topology); job != want {
		t.Fatalf("job after retries = %x, want %x", job, want)
	}
	if dialed := harness.dialedAddresses(); len(dialed) < 3 {
		t.Fatalf("dialed %d times, want at least three attempts", len(dialed))
	}
}

func TestClientStatusReadsWithoutDurableStore(t *testing.T) {
	harness := newClientHarness(t, true)
	ctx := testContext(t)
	topology := clientTestTopology(t, "status-read")
	job, _, err := harness.client().Submit(ctx, topology)
	if err != nil {
		t.Fatal(err)
	}

	options := harness.options()
	options.Store = nil
	reader := harness.clientFrom(options)
	status, err := reader.Status(ctx, job)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.JobID != job || status.State != protocol.JobPending || status.JobControlRevision != 1 {
		t.Fatalf("status = %#v", status)
	}

	var rejection *RequestRejectedError
	if _, err := reader.Status(ctx, model.JobID{0x77, 1}); !errors.As(err, &rejection) || rejection.Code != protocol.ControlErrorNotFound {
		t.Fatalf("unknown status = %v, want NotFound rejection", err)
	}

	if _, _, err := reader.Submit(ctx, topology); !errors.Is(err, ErrClientStoreRequired) {
		t.Fatalf("submit without store = %v, want ErrClientStoreRequired", err)
	}
	if _, err := reader.Cancel(ctx, job, 1); !errors.Is(err, ErrClientStoreRequired) {
		t.Fatalf("cancel without store = %v, want ErrClientStoreRequired", err)
	}
}

func TestClientResultPageValidatesExactCorrelation(t *testing.T) {
	harness := newClientHarness(t, true)
	clusterID := harness.fixture.service.clusterID
	request := protocol.ResultPageRequest{JobID: model.JobID{0x31, 2}, ManifestDigest: [32]byte{5, 6}, PageBytes: 4096}

	buildPage := func(bind protocol.ResultPageRequest) protocol.ResultPageResponse {
		return protocol.ResultPageResponse{
			JobID: bind.JobID, ManifestDigest: bind.ManifestDigest, RequestHasLastTuple: bind.HasLastTuple,
			RequestLast: bind.Last, PageBytes: bind.PageBytes, NextHasLastTuple: bind.HasLastTuple, NextLast: bind.Last, End: true,
		}
	}

	t.Run("exactly bound empty page", func(t *testing.T) {
		address := scriptedControlServer(t, harness.fixture.authenticator, clusterID, func(frame wire.Frame) *wire.Frame {
			return scriptedResponseFrame(t, frame, clusterID, buildPage(request))
		})
		options := harness.options()
		options.Store = nil
		options.Dial = func(ctx context.Context, _ string) (net.Conn, error) {
			dialer := net.Dialer{}
			return dialer.DialContext(ctx, "tcp", address)
		}
		page, err := harness.clientFrom(options).ResultPage(testContext(t), request)
		if err != nil {
			t.Fatalf("result page: %v", err)
		}
		if !page.End || len(page.Records) != 0 {
			t.Fatalf("page = %#v", page)
		}
	})

	t.Run("mis-bound page is rejected", func(t *testing.T) {
		address := scriptedControlServer(t, harness.fixture.authenticator, clusterID, func(frame wire.Frame) *wire.Frame {
			skewed := request
			skewed.PageBytes = request.PageBytes + 1
			return scriptedResponseFrame(t, frame, clusterID, buildPage(skewed))
		})
		options := harness.options()
		options.Store = nil
		options.MaxAttempts = 2
		options.Dial = func(ctx context.Context, _ string) (net.Conn, error) {
			dialer := net.Dialer{}
			return dialer.DialContext(ctx, "tcp", address)
		}
		if _, err := harness.clientFrom(options).ResultPage(testContext(t), request); !errors.Is(err, ErrClientAttemptsExhausted) {
			t.Fatalf("mis-bound page = %v, want bounded rejection", err)
		}
	})
}

func readStateFile(t *testing.T, path string) []byte {
	t.Helper()
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func writeStateFile(t *testing.T, path string, encoded []byte) {
	t.Helper()
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestClientRedirectRetriesTransientlyUnreachableLeader pins the Task 27
// full-restart finding: a hinted leader whose first connection fails is not a
// redirect, so when the remaining voters keep hinting it the client follows
// the hint again instead of reporting a redirect loop.
func TestClientRedirectRetriesTransientlyUnreachableLeader(t *testing.T) {
	harness := newClientHarness(t, true)
	topology := clientTestTopology(t, "redirect-transient-leader")
	harness.fixture.raft.setLeader(false, 2)
	failedOnce := false
	harness.dialErr = func(address string, _ int) error {
		if address == "127.0.0.2:19206" && !failedOnce {
			failedOnce = true
			return &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
		}
		return nil
	}
	harness.dialHook = func(address string, _ int) {
		if address == "127.0.0.2:19206" && failedOnce {
			harness.fixture.raft.setLeader(true, 0)
		}
	}

	job, _, err := harness.client().Submit(testContext(t), topology)
	if err != nil {
		t.Fatalf("submit past a transiently unreachable hinted leader: %v", err)
	}
	if want := expectedSubmitJobID(t, harness.store, 1, topology); job != want {
		t.Fatalf("job = %x, want %x", job, want)
	}
	dialed := harness.dialedAddresses()
	if len(dialed) < 3 || dialed[len(dialed)-1] != "127.0.0.2:19206" {
		t.Fatalf("dialed = %v, want the hinted leader retried after its transient failure", dialed)
	}
}

// TestClientTreatsResultTooLargeAsConsumedRejection pins that a request-bound
// ResultTooLarge completes the submit and cancel exchanges (so the reservation
// is durably resolved and surfaces as a typed rejection) while a retryable
// CapacityExhausted still retries the exact bytes.
func TestClientTreatsResultTooLargeAsConsumedRejection(t *testing.T) {
	harness := newClientHarness(t, true)
	client := harness.client()

	submit := submitRequestFor(t, 0x72, 1, queryTopology(1))
	tooLarge := requestBoundError(submit, protocol.ControlErrorResultTooLarge, false, "durable command result exceeds the replicated cache bound")
	if err := client.submitAccept(submit)(tooLarge); err != nil {
		t.Fatalf("submit ResultTooLarge classified as %v, want a completed exchange", err)
	}
	capacity := requestBoundError(submit, protocol.ControlErrorCapacityExhausted, true, "replicated capacity exhausted")
	if err := client.submitAccept(submit)(capacity); !errors.Is(err, errClientRetryExchange) {
		t.Fatalf("submit CapacityExhausted classified as %v, want retry", err)
	}

	cancel := cancelRequestFor(t, 0x72, 2, model.JobID{0x72}, 1)
	tooLarge = requestBoundError(cancel, protocol.ControlErrorResultTooLarge, false, "durable command result exceeds the replicated cache bound")
	if err := client.cancelAccept(cancel)(tooLarge); err != nil {
		t.Fatalf("cancel ResultTooLarge classified as %v, want a completed exchange", err)
	}
}

func TestClientListJobsReadsEveryRetainedJob(t *testing.T) {
	harness := newClientHarness(t, true)
	ctx := testContext(t)
	job, _, err := harness.client().Submit(ctx, clientTestTopology(t, "list-jobs"))
	if err != nil {
		t.Fatal(err)
	}
	options := harness.options()
	options.Store = nil
	reader := harness.clientFrom(options)
	listing, err := reader.ListJobs(ctx)
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if listing.LeaderNodeID == 0 || len(listing.Jobs) != 1 || listing.Jobs[0].JobID != job {
		t.Fatalf("listing = %#v", listing)
	}
	if listing.Jobs[0].State != protocol.JobPending || listing.Jobs[0].JobControlRevision != 1 {
		t.Fatalf("listing summary = %#v", listing.Jobs[0])
	}
}

func TestClientListJobsRetriesThroughStarting(t *testing.T) {
	harness := newClientHarness(t, false)
	harness.dialHook = func(_ string, attempt int) {
		if attempt == 2 {
			harness.fixture.seedEpochAndOpenGate()
		}
	}
	listing, err := harness.client().ListJobs(testContext(t))
	if err != nil {
		t.Fatalf("list jobs through Starting: %v", err)
	}
	if len(listing.Jobs) != 0 {
		t.Fatalf("listing = %#v, want no jobs", listing)
	}
}
