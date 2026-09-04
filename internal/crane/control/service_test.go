package control

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aadityakv/crane/internal/clock"
	"github.com/aadityakv/crane/internal/config"
	"github.com/aadityakv/crane/internal/crane/admission"
	"github.com/aadityakv/crane/internal/crane/membership"
	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
	"github.com/aadityakv/crane/internal/crane/state"
	"github.com/aadityakv/crane/internal/raft"
	"github.com/aadityakv/crane/internal/swim"
	"github.com/aadityakv/crane/internal/wire"
)

// controlTestConfig returns one fully validated voter node configuration.
func controlTestConfig(t *testing.T, nodeID uint16) config.NodeConfig {
	t.Helper()
	secret := t.TempDir() + "/cluster.secret"
	if err := os.WriteFile(secret, bytes.Repeat([]byte{1}, 32), 0o600); err != nil {
		t.Fatal(err)
	}
	host := "127.0.0.1"
	if nodeID == 2 {
		host = "127.0.0.2"
	} else if nodeID == 3 {
		host = "127.0.0.3"
	} else if nodeID > 3 {
		host = "127.0.0.9"
	}
	basePort := uint16(19100)
	if nodeID >= 2 && nodeID <= 3 {
		basePort = 19100 + (nodeID-1)*100
	}
	configuration := config.NodeConfig{
		NodeID: nodeID, ClusterID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", BindHost: host, AdvertiseHost: host,
		BasePort: basePort, Introducer: "127.0.0.1:19102", StorageDir: t.TempDir(), ClusterSecretFile: secret,
		RaftVoters: []config.RaftVoter{{NodeID: 1, Endpoint: "127.0.0.1:19106"}, {NodeID: 2, Endpoint: "127.0.0.2:19206"}, {NodeID: 3, Endpoint: "127.0.0.3:19306"}},
		Raft:       config.DefaultRaftConfig(), Crane: config.DefaultCraneConfig(), Timing: config.DefaultTimingConfig(),
	}
	if err := configuration.Validate(); err != nil {
		t.Fatal(err)
	}
	return configuration
}

// fixtureRaft is a leader-scriptable RaftAPI applying proposals to one machine.
type fixtureRaft struct {
	mu          sync.Mutex
	ready       chan struct{}
	machine     *state.Machine
	index       uint64
	term        uint64
	leader      bool
	leaderHint  uint16
	barriers    int
	proposals   [][]byte
	proposeHook func([]byte) error
}

func newFixtureRaft(machine *state.Machine) *fixtureRaft {
	ready := make(chan struct{})
	close(ready)
	return &fixtureRaft{ready: ready, machine: machine, index: 100, term: 2, leader: true}
}

func (r *fixtureRaft) Ready() <-chan struct{} { return r.ready }

func (r *fixtureRaft) Barrier(context.Context) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.leader {
		return 0, &raft.NotLeaderError{LeaderID: r.leaderHint}
	}
	r.barriers++
	return r.index, nil
}

func (r *fixtureRaft) Propose(_ context.Context, command []byte) (raft.ProposalResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.leader {
		return raft.ProposalResult{}, &raft.NotLeaderError{LeaderID: r.leaderHint}
	}
	if r.proposeHook != nil {
		if err := r.proposeHook(command); err != nil {
			return raft.ProposalResult{}, err
		}
	}
	r.index++
	result, err := r.machine.Apply(r.index, r.term, command)
	if err != nil {
		return raft.ProposalResult{}, err
	}
	r.proposals = append(r.proposals, append([]byte(nil), command...))
	return raft.ProposalResult{Index: r.index, Term: r.term, Result: result}, nil
}

func (r *fixtureRaft) setLeader(leader bool, hint uint16) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.leader, r.leaderHint = leader, hint
}

func (r *fixtureRaft) capturedProposals() [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	captured := make([][]byte, len(r.proposals))
	for index, proposal := range r.proposals {
		captured[index] = append([]byte(nil), proposal...)
	}
	return captured
}

func (r *fixtureRaft) barrierCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.barriers
}

// fixtureMembership is a scriptable authorizer view for request admission.
type fixtureMembership struct {
	mu           sync.Mutex
	ready        chan struct{}
	members      []swim.Member
	authorizeErr error
}

func newFixtureMembership(members ...swim.Member) *fixtureMembership {
	ready := make(chan struct{})
	close(ready)
	return &fixtureMembership{ready: ready, members: members}
}

func (m *fixtureMembership) Ready() <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ready
}

func (m *fixtureMembership) View() membership.View {
	m.mu.Lock()
	defer m.mu.Unlock()
	return membership.View{Members: append([]swim.Member(nil), m.members...)}
}

func (m *fixtureMembership) AuthorizeTCP(uint16, net.Addr) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.authorizeErr
}

func (m *fixtureMembership) setAuthorizeError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.authorizeErr = err
}

// serviceFixture composes one service over loopback TCP with scripted seams.
type serviceFixture struct {
	t             *testing.T
	configuration config.NodeConfig
	machine       *state.Machine
	raft          *fixtureRaft
	gate          *admission.Gate
	members       *fixtureMembership
	clock         *clock.Manual
	authenticator wire.Authenticator
	fetcher       ResultFetcher
	service       *Service
	wakes         atomic.Int64

	mu        sync.Mutex
	requested []string
	listener  net.Listener
	runErr    chan error
	cancelRun context.CancelFunc
}

func newServiceFixture(t *testing.T, machine *state.Machine) *serviceFixture {
	t.Helper()
	fixture := &serviceFixture{
		t: t, configuration: controlTestConfig(t, 1), machine: machine,
		raft: newFixtureRaft(machine), gate: admission.NewGate(),
		members: newFixtureMembership(
			swim.Member{NodeID: 1, Host: "127.0.0.1", BasePort: 19100, Incarnation: 1, Status: swim.Alive},
			swim.Member{NodeID: 2, Host: "127.0.0.1", BasePort: 19200, Incarnation: 1, Status: swim.Alive},
		),
		clock:         clock.NewManual(time.Unix(100, 0)),
		authenticator: wire.NewHMACAuthenticator(bytes.Repeat([]byte{7}, 32)),
		runErr:        make(chan error, 1),
	}
	fixture.buildService()
	return fixture
}

func (f *serviceFixture) options() ServiceOptions {
	return ServiceOptions{
		Config: f.configuration, Authenticator: f.authenticator, Clock: f.clock,
		Membership: &membership.Authorizer{}, Raft: f.raft, Machine: f.machine, Gate: f.gate,
		Results:         &QueryEngine{Machine: f.machine, Fetcher: f.fetcher},
		WakeCoordinator: func() { f.wakes.Add(1) },
	}
}

func (f *serviceFixture) buildService() {
	f.t.Helper()
	f.buildServiceFrom(f.options())
}

func (f *serviceFixture) buildServiceFrom(options ServiceOptions) {
	f.t.Helper()
	service, err := NewService(options)
	if err != nil {
		f.t.Fatalf("construct control service: %v", err)
	}
	service.membership = f.members
	service.listen = func(network, address string) (net.Listener, error) {
		f.mu.Lock()
		f.requested = append(f.requested, network+"/"+address)
		f.mu.Unlock()
		listener, listenErr := net.Listen("tcp", "127.0.0.1:0")
		if listenErr != nil {
			return nil, listenErr
		}
		f.mu.Lock()
		f.listener = listener
		f.mu.Unlock()
		return listener, nil
	}
	f.service = service
}

func (f *serviceFixture) start() {
	f.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	f.cancelRun = cancel
	go func() { f.runErr <- f.service.Run(ctx) }()
	select {
	case <-f.service.Ready():
	case err := <-f.runErr:
		f.t.Fatalf("service exited before ready: %v", err)
	case <-time.After(2 * time.Second):
		f.t.Fatal("service never became ready")
	}
	f.t.Cleanup(func() {
		cancel()
		select {
		case <-f.runErr:
		case <-time.After(2 * time.Second):
			f.t.Fatal("service did not stop")
		}
	})
}

func (f *serviceFixture) address() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listener.Addr().String()
}

func (f *serviceFixture) seedEpochAndOpenGate() model.CoordinatorEpoch {
	f.t.Helper()
	view := f.machine.View()
	epoch := view.CoordinatorEpoch
	if epoch == (model.CoordinatorEpoch{}) {
		nonce := [16]byte{0x55, byte(view.CoordinatorRevision + 1)}
		begin, err := state.NewBeginCoordinatorEpoch(queryCommandID("service-begin", nonce[:]), view.CoordinatorRevision, 1, nonce)
		if err != nil {
			f.t.Fatal(err)
		}
		seedMachine(f.t, f.machine, 1, begin)
		epoch = f.machine.View().CoordinatorEpoch
	}
	if err := f.gate.Open(epoch); err != nil {
		f.t.Fatalf("open admission gate: %v", err)
	}
	return epoch
}

func testRequestID(t *testing.T) wire.RequestID {
	t.Helper()
	var id wire.RequestID
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatal(err)
	}
	return id
}

func controlClientLimits(clusterID [16]byte) wire.Limits {
	limits := wire.DefaultLimits()
	limits.MaxFrameSize = int(model.PublicControlMaxFrameBytesV1)
	limits.ExpectedClusterID = &clusterID
	return limits
}

// exchangeFrame performs one full connection round: dial, one request frame,
// one response frame, and returns the decoded typed response.
func (f *serviceFixture) exchangeFrame(sender uint16, requestID wire.RequestID, message protocol.ControlMessage) (protocol.ControlMessage, error) {
	f.t.Helper()
	payload, err := protocol.MarshalControlMessage(message)
	if err != nil {
		f.t.Fatalf("marshal request: %v", err)
	}
	return f.exchangeRaw(sender, requestID, message.MessageType(), payload)
}

func (f *serviceFixture) exchangeRaw(sender uint16, requestID wire.RequestID, messageType wire.MessageType, payload []byte) (protocol.ControlMessage, error) {
	f.t.Helper()
	connection, err := net.Dial("tcp", f.address())
	if err != nil {
		f.t.Fatalf("dial control service: %v", err)
	}
	defer connection.Close()
	stream := wire.NewTCPFrameStream(connection, f.authenticator, controlClientLimits(f.service.clusterID), 2*time.Second)
	frame := wire.Frame{Header: wire.Header{
		Version: wire.Version1, Message: messageType, ClusterID: f.service.clusterID, SenderID: sender,
		RequestID: requestID, TimestampMillis: f.clock.Now().UnixMilli(), Codec: wire.CodecBinary,
	}, Payload: payload}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := stream.WriteFrame(ctx, frame); err != nil {
		return nil, err
	}
	response, err := stream.ReadFrame(ctx)
	if err != nil {
		return nil, err
	}
	if response.Header.RequestID != requestID || response.Header.SenderID != f.configuration.NodeID {
		f.t.Fatalf("response correlation header = %#v", response.Header)
	}
	return protocol.UnmarshalControlMessage(response.Header.Message, response.Payload)
}

// exchange requires a decoded response for one uniquely identified request.
func (f *serviceFixture) exchange(message protocol.ControlMessage) protocol.ControlMessage {
	f.t.Helper()
	response, err := f.exchangeFrame(2, testRequestID(f.t), message)
	if err != nil {
		f.t.Fatalf("exchange %T: %v", message, err)
	}
	return response
}

func statusRequest(job model.JobID) protocol.StatusRequest { return protocol.StatusRequest{JobID: job} }

func requireControlError(t *testing.T, response protocol.ControlMessage, code protocol.ControlErrorCode) protocol.ControlError {
	t.Helper()
	controlError, ok := response.(protocol.ControlError)
	if !ok {
		t.Fatalf("response = %#v, want ControlError code %d", response, code)
	}
	if controlError.Code != code {
		t.Fatalf("control error code = %d (detail %q), want %d", controlError.Code, controlError.Detail, code)
	}
	return controlError
}

func TestControlServiceConstructorValidatesWithoutSideEffects(t *testing.T) {
	machine := state.NewMachine()
	fixture := newServiceFixture(t, machine)
	if got := fixture.service.Name(); got != "crane-control" {
		t.Fatalf("Name() = %q", got)
	}
	select {
	case <-fixture.service.Ready():
		t.Fatal("Ready closed before Run")
	default:
	}
	fixture.mu.Lock()
	requested := len(fixture.requested)
	fixture.mu.Unlock()
	if requested != 0 {
		t.Fatalf("constructor opened %d listeners", requested)
	}

	valid := fixture.options()
	mutations := []struct {
		name   string
		mutate func(*ServiceOptions)
	}{
		{"nil authenticator", func(o *ServiceOptions) { o.Authenticator = nil }},
		{"nil clock", func(o *ServiceOptions) { o.Clock = nil }},
		{"nil membership", func(o *ServiceOptions) { o.Membership = nil }},
		{"nil machine", func(o *ServiceOptions) { o.Machine = nil }},
		{"nil gate", func(o *ServiceOptions) { o.Gate = nil }},
		{"nil results", func(o *ServiceOptions) { o.Results = nil }},
		{"nil wake", func(o *ServiceOptions) { o.WakeCoordinator = nil }},
		{"voter requires raft", func(o *ServiceOptions) { o.Raft = nil }},
		{"foreign results machine", func(o *ServiceOptions) { o.Results = &QueryEngine{Machine: state.NewMachine()} }},
		{"invalid config", func(o *ServiceOptions) { o.Config.NodeID = 0 }},
	}
	for _, mutation := range mutations {
		options := valid
		mutation.mutate(&options)
		if _, err := NewService(options); err == nil {
			t.Fatalf("NewService accepted %s", mutation.name)
		}
	}
}

func TestControlServiceRunBindsExactPublicEndpointOnce(t *testing.T) {
	fixture := newServiceFixture(t, state.NewMachine())
	fixture.start()
	fixture.mu.Lock()
	requested := append([]string(nil), fixture.requested...)
	fixture.mu.Unlock()
	if len(requested) != 1 || requested[0] != "tcp/127.0.0.1:19104" {
		t.Fatalf("listen requests = %v, want exactly tcp/127.0.0.1:19104", requested)
	}
	if err := fixture.service.Run(context.Background()); err == nil {
		t.Fatal("second Run call succeeded")
	}
}

func TestControlServiceServesOneBoundedRequestPerConnection(t *testing.T) {
	fixture := newServiceFixture(t, state.NewMachine())
	fixture.seedEpochAndOpenGate()
	fixture.start()

	connection, err := net.Dial("tcp", fixture.address())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	stream := wire.NewTCPFrameStream(connection, fixture.authenticator, controlClientLimits(fixture.service.clusterID), 2*time.Second)
	request := statusRequest(model.JobID{0x31})
	payload, err := protocol.MarshalControlMessage(request)
	if err != nil {
		t.Fatal(err)
	}
	requestID := testRequestID(t)
	frame := wire.Frame{Header: wire.Header{Version: wire.Version1, Message: request.MessageType(), ClusterID: fixture.service.clusterID, SenderID: 2, RequestID: requestID, TimestampMillis: fixture.clock.Now().UnixMilli(), Codec: wire.CodecBinary}, Payload: payload}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := stream.WriteFrame(ctx, frame); err != nil {
		t.Fatal(err)
	}
	response, err := stream.ReadFrame(ctx)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := protocol.UnmarshalControlMessage(response.Header.Message, response.Payload)
	if err != nil {
		t.Fatal(err)
	}
	requireControlError(t, decoded, protocol.ControlErrorNotFound)
	if _, err := stream.ReadFrame(ctx); err == nil {
		t.Fatal("connection served a second response")
	}
}

func TestControlServiceRejectsUnauthenticatedFrames(t *testing.T) {
	fixture := newServiceFixture(t, state.NewMachine())
	fixture.seedEpochAndOpenGate()
	fixture.start()
	request := statusRequest(model.JobID{0x32})
	payload, err := protocol.MarshalControlMessage(request)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("TamperedMACClosesWithoutResponse", func(t *testing.T) {
		connection, err := net.Dial("tcp", fixture.address())
		if err != nil {
			t.Fatal(err)
		}
		defer connection.Close()
		limits := controlClientLimits(fixture.service.clusterID)
		limits.ExpectedClusterID = nil
		encoded, err := wire.Encode(wire.Header{Version: wire.Version1, Message: request.MessageType(), ClusterID: fixture.service.clusterID, SenderID: 2, RequestID: testRequestID(t), TimestampMillis: fixture.clock.Now().UnixMilli(), Codec: wire.CodecBinary}, payload, wire.NewHMACAuthenticator(bytes.Repeat([]byte{9}, 32)), limits)
		if err != nil {
			t.Fatal(err)
		}
		prefix := []byte{byte(len(encoded) >> 24), byte(len(encoded) >> 16), byte(len(encoded) >> 8), byte(len(encoded))}
		if _, err := connection.Write(append(prefix, encoded...)); err != nil {
			t.Fatal(err)
		}
		assertConnectionClosedWithoutResponse(t, connection)
	})

	t.Run("ForeignClusterClosesWithoutResponse", func(t *testing.T) {
		connection, err := net.Dial("tcp", fixture.address())
		if err != nil {
			t.Fatal(err)
		}
		defer connection.Close()
		limits := controlClientLimits(fixture.service.clusterID)
		limits.ExpectedClusterID = nil
		encoded, err := wire.Encode(wire.Header{Version: wire.Version1, Message: request.MessageType(), ClusterID: [16]byte{0xFE}, SenderID: 2, RequestID: testRequestID(t), TimestampMillis: fixture.clock.Now().UnixMilli(), Codec: wire.CodecBinary}, payload, fixture.authenticator, limits)
		if err != nil {
			t.Fatal(err)
		}
		prefix := []byte{byte(len(encoded) >> 24), byte(len(encoded) >> 16), byte(len(encoded) >> 8), byte(len(encoded))}
		if _, err := connection.Write(append(prefix, encoded...)); err != nil {
			t.Fatal(err)
		}
		assertConnectionClosedWithoutResponse(t, connection)
	})

	t.Run("NonMemberSenderClosesWithoutResponse", func(t *testing.T) {
		if _, err := fixture.exchangeFrame(99, testRequestID(t), request); err == nil {
			t.Fatal("non-member sender received a response")
		}
	})

	t.Run("DeadMemberSenderClosesWithoutResponse", func(t *testing.T) {
		fixture.members.mu.Lock()
		fixture.members.members = append(fixture.members.members, swim.Member{NodeID: 5, Host: "127.0.0.1", BasePort: 19500, Incarnation: 1, Status: swim.Dead})
		fixture.members.mu.Unlock()
		if _, err := fixture.exchangeFrame(5, testRequestID(t), request); err == nil {
			t.Fatal("dead member sender received a response")
		}
	})

	t.Run("SourceIPDenialClosesWithoutResponse", func(t *testing.T) {
		fixture.members.setAuthorizeError(errors.New("ip mismatch"))
		defer fixture.members.setAuthorizeError(nil)
		if _, err := fixture.exchangeFrame(2, testRequestID(t), request); err == nil {
			t.Fatal("denied source IP received a response")
		}
	})

	t.Run("ResponseTypedMessageClosesWithoutResponse", func(t *testing.T) {
		if _, err := fixture.exchangeRaw(2, testRequestID(t), wire.MessageCraneSubmitResponse, payload); err == nil {
			t.Fatal("response-typed message received a response")
		}
	})

	t.Run("ReplayedRequestIDClosesWithoutResponse", func(t *testing.T) {
		requestID := testRequestID(t)
		if _, err := fixture.exchangeFrame(2, requestID, request); err != nil {
			t.Fatalf("first use of request ID failed: %v", err)
		}
		if _, err := fixture.exchangeFrame(2, requestID, request); err == nil {
			t.Fatal("replayed request ID received a response")
		}
	})

	t.Run("MalformedPayloadGetsTypedErrorThenInvalidReplayIsRejected", func(t *testing.T) {
		requestID := testRequestID(t)
		truncated := append([]byte(nil), payload[:len(payload)-1]...)
		response, err := fixture.exchangeRaw(2, requestID, wire.MessageCraneStatusRequest, truncated)
		if err != nil {
			t.Fatalf("malformed payload got no typed response: %v", err)
		}
		controlError := requireControlError(t, response, protocol.ControlErrorMalformed)
		if controlError.RelatedMessage != wire.MessageCraneStatusRequest || controlError.Retryable {
			t.Fatalf("malformed error binding = %#v", controlError)
		}
		if _, err := fixture.exchangeRaw(2, requestID, wire.MessageCraneStatusRequest, truncated); err == nil {
			t.Fatal("replay of invalid request ID received a response")
		}
	})

	t.Run("UnsupportedSchemaGetsTypedError", func(t *testing.T) {
		response, err := fixture.exchangeRaw(2, testRequestID(t), wire.MessageCraneStatusRequest, []byte{0xFF, 0xFF, 0xFF})
		if err != nil {
			t.Fatalf("unsupported schema got no typed response: %v", err)
		}
		requireControlError(t, response, protocol.ControlErrorUnsupportedSchema)
	})
}

func assertConnectionClosedWithoutResponse(t *testing.T, connection net.Conn) {
	t.Helper()
	_ = connection.SetReadDeadline(time.Now().Add(2 * time.Second))
	buffer := make([]byte, 1)
	if _, err := connection.Read(buffer); err == nil {
		t.Fatal("connection produced response bytes")
	}
}

func TestControlServiceAnswersStartingWhileDependenciesOrGateClosed(t *testing.T) {
	t.Run("GateClosed", func(t *testing.T) {
		fixture := newServiceFixture(t, state.NewMachine())
		fixture.seedEpochAndOpenGate()
		if err := fixture.gate.CloseAndWait(context.Background()); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		fixture.start()
		// gate decoupling: reads now succeed through the closed gate (pinned by
		// TestDispatchServesReadsWithClosedGate), so the Starting answer is
		// asserted on a mutation, which still enters the gate.
		response := fixture.exchange(submitRequestFor(t, 0x68, 1, queryTopology(1)))
		controlError := requireControlError(t, response, protocol.ControlErrorStarting)
		if !controlError.Retryable {
			t.Fatal("Starting must be retryable")
		}
	})
	t.Run("RaftNotReady", func(t *testing.T) {
		fixture := newServiceFixture(t, state.NewMachine())
		fixture.raft.mu.Lock()
		fixture.raft.ready = make(chan struct{})
		fixture.raft.mu.Unlock()
		fixture.seedEpochAndOpenGate()
		fixture.start()
		requireControlError(t, fixture.exchange(statusRequest(model.JobID{0x34})), protocol.ControlErrorStarting)
	})
	t.Run("MembershipNotReady", func(t *testing.T) {
		fixture := newServiceFixture(t, state.NewMachine())
		fixture.members.mu.Lock()
		fixture.members.ready = make(chan struct{})
		fixture.members.mu.Unlock()
		fixture.seedEpochAndOpenGate()
		fixture.start()
		requireControlError(t, fixture.exchange(statusRequest(model.JobID{0x35})), protocol.ControlErrorStarting)
	})
}

func TestControlServiceBoundsConcurrentConnections(t *testing.T) {
	fixture := newServiceFixture(t, state.NewMachine())
	fixture.seedEpochAndOpenGate()
	fixture.service.maxConnections = 1
	listener := fixture.usePeerListener()
	fixture.start()

	// The held connection occupies the sole global slot without sending a
	// frame; the budget is shared across peers, so a different peer's
	// connection is also refused.
	held := dialControlPeer(listener, "10.0.0.2")
	defer held.Close()
	overflow := dialControlPeer(listener, "10.0.0.3")
	defer overflow.Close()
	requireConnectionClosedBeforeRead(t, overflow)

	_ = held.Close()
	deadline := time.Now().Add(2 * time.Second)
	for {
		response, err := fixture.exchangeOverConn(dialControlPeer(listener, "10.0.0.2"), 2, testRequestID(t), statusRequest(model.JobID{0x36}))
		if err == nil {
			requireControlError(t, response, protocol.ControlErrorNotFound)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("released connection slot never served a request: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// peerConn binds one end of an in-memory pipe to a scripted source address so
// per-peer admission is testable without any network.
type peerConn struct {
	net.Conn
	remote net.Addr
}

func (connection *peerConn) RemoteAddr() net.Addr { return connection.remote }

// SetReadDeadline and SetWriteDeadline emulate TCP semantics: unlike a raw
// net.Pipe, a deadline on a TCP connection never fails merely because the
// remote end closed, so the frame stream's deadline bookkeeping cannot turn a
// completed exchange into an error.
func (connection *peerConn) SetReadDeadline(value time.Time) error {
	if err := connection.Conn.SetReadDeadline(value); errors.Is(err, io.ErrClosedPipe) {
		return nil
	} else if err != nil {
		return err
	}
	return nil
}

func (connection *peerConn) SetWriteDeadline(value time.Time) error {
	if err := connection.Conn.SetWriteDeadline(value); errors.Is(err, io.ErrClosedPipe) {
		return nil
	} else if err != nil {
		return err
	}
	return nil
}

// peerListener is one deterministic net.Listener seam handing scripted peer
// connections to the +4 accept loop.
type peerListener struct {
	conns  chan net.Conn
	closed chan struct{}
	once   sync.Once
}

func newPeerListener() *peerListener {
	return &peerListener{conns: make(chan net.Conn), closed: make(chan struct{})}
}

func (listener *peerListener) Accept() (net.Conn, error) {
	select {
	case connection := <-listener.conns:
		return connection, nil
	case <-listener.closed:
		return nil, errors.New("peer listener closed")
	}
}

func (listener *peerListener) Close() error {
	listener.once.Do(func() { close(listener.closed) })
	return nil
}

func (*peerListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 19106}
}

// usePeerListener installs the deterministic in-memory listener seam before
// the service starts.
func (f *serviceFixture) usePeerListener() *peerListener {
	f.t.Helper()
	listener := newPeerListener()
	f.service.listen = func(string, string) (net.Listener, error) { return listener, nil }
	return listener
}

// dialControlPeer offers the listener one connection from the given source
// host. The dial completes only once the accept loop has taken it, so every
// later dial observes all earlier admission decisions.
func dialControlPeer(listener *peerListener, host string) net.Conn {
	client, server := net.Pipe()
	listener.conns <- &peerConn{Conn: server, remote: &net.TCPAddr{IP: net.ParseIP(host), Port: 40000}}
	return &peerConn{Conn: client, remote: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 19106}}
}

// requireConnectionClosedBeforeRead asserts the service closed the connection
// before reading any frame: the client observes the server-side close itself,
// not a local read deadline on a connection left waiting for its frame.
func requireConnectionClosedBeforeRead(t *testing.T, connection net.Conn) {
	t.Helper()
	// Setting the deadline may itself report the peer's close; only the read
	// outcome distinguishes a fail-closed connection from one left waiting.
	_ = connection.SetReadDeadline(time.Now().Add(750 * time.Millisecond))
	buffer := make([]byte, 1)
	_, err := connection.Read(buffer)
	switch {
	case err == nil:
		t.Fatal("connection produced bytes before its admission decision")
	case errors.Is(err, os.ErrDeadlineExceeded):
		t.Fatal("connection stayed open waiting for a frame instead of being closed fail-closed")
	}
}

// exchangeOverConn performs one full request/response round over one
// already-established client connection.
func (f *serviceFixture) exchangeOverConn(connection net.Conn, sender uint16, requestID wire.RequestID, message protocol.ControlMessage) (protocol.ControlMessage, error) {
	f.t.Helper()
	payload, err := protocol.MarshalControlMessage(message)
	if err != nil {
		f.t.Fatalf("marshal request: %v", err)
	}
	stream := wire.NewTCPFrameStream(connection, f.authenticator, controlClientLimits(f.service.clusterID), 2*time.Second)
	frame := wire.Frame{Header: wire.Header{
		Version: wire.Version1, Message: message.MessageType(), ClusterID: f.service.clusterID, SenderID: sender,
		RequestID: requestID, TimestampMillis: f.clock.Now().UnixMilli(), Codec: wire.CodecBinary,
	}, Payload: payload}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := stream.WriteFrame(ctx, frame); err != nil {
		return nil, err
	}
	response, err := stream.ReadFrame(ctx)
	if err != nil {
		return nil, err
	}
	if response.Header.RequestID != requestID || response.Header.SenderID != f.configuration.NodeID {
		f.t.Fatalf("response correlation header = %#v", response.Header)
	}
	return protocol.UnmarshalControlMessage(response.Header.Message, response.Payload)
}

// requirePeerSlotsDrained waits until every per-peer connection slot has been
// released.
func requirePeerSlotsDrained(t *testing.T, service *Service) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		service.peerMu.Lock()
		remaining := len(service.peerConnections)
		service.peerMu.Unlock()
		if remaining == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("per-peer accounting retained %d peers after connection close", remaining)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestControlServiceBoundsConnectionsPerPeer(t *testing.T) {
	fixture := newServiceFixture(t, state.NewMachine())
	fixture.seedEpochAndOpenGate()
	listener := fixture.usePeerListener()
	fixture.start()

	held := make([]net.Conn, 0, DefaultMaxControlConnectionsPerPeer)
	defer func() {
		for _, connection := range held {
			_ = connection.Close()
		}
	}()
	for index := 0; index < DefaultMaxControlConnectionsPerPeer; index++ {
		held = append(held, dialControlPeer(listener, "10.0.0.2"))
	}

	// The peer already holds its full per-peer bound: its next connection is
	// closed fail-closed before any frame is read, with no mutation, no
	// replay eviction, and no response.
	overflow := dialControlPeer(listener, "10.0.0.2")
	defer overflow.Close()
	requireConnectionClosedBeforeRead(t, overflow)

	// A different peer stays admissible within the same global budget.
	response, err := fixture.exchangeOverConn(dialControlPeer(listener, "10.0.0.3"), 2, testRequestID(t), statusRequest(model.JobID{0x37}))
	if err != nil {
		t.Fatalf("different peer exchange failed: %v", err)
	}
	requireControlError(t, response, protocol.ControlErrorNotFound)
}

func TestControlServiceReleasesPerPeerSlotsOnConnectionClose(t *testing.T) {
	t.Run("ClientClose", func(t *testing.T) {
		fixture := newServiceFixture(t, state.NewMachine())
		fixture.seedEpochAndOpenGate()
		listener := fixture.usePeerListener()
		fixture.start()
		held := make([]net.Conn, 0, DefaultMaxControlConnectionsPerPeer)
		for index := 0; index < DefaultMaxControlConnectionsPerPeer; index++ {
			held = append(held, dialControlPeer(listener, "10.0.0.2"))
		}
		for _, connection := range held {
			_ = connection.Close()
		}
		requirePeerSlotsDrained(t, fixture.service)
		response, err := fixture.exchangeOverConn(dialControlPeer(listener, "10.0.0.2"), 2, testRequestID(t), statusRequest(model.JobID{0x38}))
		if err != nil {
			t.Fatalf("client-closed peer connections never released their per-peer slots: %v", err)
		}
		requireControlError(t, response, protocol.ControlErrorNotFound)
	})

	t.Run("HandlerCompletion", func(t *testing.T) {
		fixture := newServiceFixture(t, state.NewMachine())
		fixture.seedEpochAndOpenGate()
		listener := fixture.usePeerListener()
		fixture.start()
		// One more sequential full exchange than the per-peer bound proves
		// each completed handler returns its peer's slot.
		for index := 0; index <= DefaultMaxControlConnectionsPerPeer; index++ {
			response, err := fixture.exchangeOverConn(dialControlPeer(listener, "10.0.0.2"), 2, testRequestID(t), statusRequest(model.JobID{0x39}))
			if err != nil {
				t.Fatalf("sequential exchange %d failed: %v", index, err)
			}
			requireControlError(t, response, protocol.ControlErrorNotFound)
			requirePeerSlotsDrained(t, fixture.service)
		}
	})

	t.Run("Cancellation", func(t *testing.T) {
		fixture := newServiceFixture(t, state.NewMachine())
		fixture.seedEpochAndOpenGate()
		listener := fixture.usePeerListener()
		fixture.start()
		for index := 0; index < DefaultMaxControlConnectionsPerPeer; index++ {
			connection := dialControlPeer(listener, "10.0.0.2")
			defer connection.Close()
		}
		fixture.cancelRun()
		select {
		case err := <-fixture.runErr:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("Run returned %v", err)
			}
			fixture.runErr <- err
		case <-time.After(2 * time.Second):
			t.Fatal("Run did not join after cancellation with open peer connections")
		}
		fixture.service.peerMu.Lock()
		remaining := len(fixture.service.peerConnections)
		fixture.service.peerMu.Unlock()
		if remaining != 0 {
			t.Fatalf("per-peer accounting retained %d peers after cancellation", remaining)
		}
	})
}

func TestControlServiceCancellationClosesSlowClientsAndJoins(t *testing.T) {
	fixture := newServiceFixture(t, state.NewMachine())
	fixture.seedEpochAndOpenGate()
	fixture.start()

	slow, err := net.Dial("tcp", fixture.address())
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Close()
	fixture.cancelRun()
	select {
	case err := <-fixture.runErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v", err)
		}
		fixture.runErr <- err
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not join after cancellation with an open slow client")
	}
	assertConnectionClosedWithoutResponse(t, slow)
	if connection, err := net.Dial("tcp", fixture.address()); err == nil {
		assertConnectionClosedWithoutResponse(t, connection)
		_ = connection.Close()
	}
}

// TestControlServicePerPeerReplayBudgetAdmitsConfiguredCountThenDropsWithoutResponse
// pins the +4 half of the replay-budget change: one peer is admitted exactly
// its per-peer budget of request identities inside the replay window, the
// next frame from that peer is dropped without any response (the client's
// ambiguous-drop retry path handles it), other peers keep their own budget,
// and the budget frees once the window has elapsed.
func TestControlServicePerPeerReplayBudgetAdmitsConfiguredCountThenDropsWithoutResponse(t *testing.T) {
	fixture := newServiceFixture(t, state.NewMachine())
	production := fixture.service.replay
	const perPeer = 2
	fixture.service.replay = newServiceReplay(fixture.clock, production.window, production.future, DefaultMaxControlReplayEntries, perPeer)
	fixture.start()
	request := statusRequest(model.JobID{0x44})

	for index := 0; index < perPeer; index++ {
		if _, err := fixture.exchangeFrame(2, testRequestID(t), request); err != nil {
			t.Fatalf("request %d within the per-peer budget: %v", index+1, err)
		}
	}
	if _, err := fixture.exchangeFrame(2, testRequestID(t), request); err == nil {
		t.Fatal("frame beyond the per-peer replay budget received a response, want a silent drop")
	}
	if _, err := fixture.exchangeFrame(1, testRequestID(t), request); err != nil {
		t.Fatalf("another peer lost admission to the first peer's exhausted budget: %v", err)
	}

	fixture.clock.Advance(production.window + time.Second)
	if _, err := fixture.exchangeFrame(2, testRequestID(t), request); err != nil {
		t.Fatalf("peer budget did not free after the replay window elapsed: %v", err)
	}
}

// TestDispatchServesReadsWithClosedGate pins that reads are served from the
// post-barrier applied view even while the admission gate is closed, while
// mutations still receive the retryable starting error.
func TestDispatchServesReadsWithClosedGate(t *testing.T) {
	seeded := seedQueryFixture(t, querySeed{sinkPartitions: 2, sealPartitions: 2, succeed: true})
	fixture := newServiceFixture(t, seeded.machine)
	fixture.start()

	statusResponse := fixture.exchange(statusRequest(seeded.job))
	status, ok := statusResponse.(protocol.StatusResponse)
	if !ok {
		t.Fatalf("status read through the closed gate = %#v, want StatusResponse", statusResponse)
	}
	if status.JobID != seeded.job || status.State != protocol.JobSucceeded {
		t.Fatalf("closed-gate status = %#v", status)
	}
	if fixture.raft.barrierCount() == 0 {
		t.Fatal("status read skipped the leader barrier")
	}

	listingResponse := fixture.exchange(protocol.JobListRequest{})
	listing, ok := listingResponse.(protocol.JobListResponse)
	if !ok {
		t.Fatalf("job list read through the closed gate = %#v, want JobListResponse", listingResponse)
	}
	view := seeded.machine.View()
	if listing.LeaderNodeID != fixture.configuration.NodeID || listing.AppliedIndex != view.AppliedIndex || len(listing.Jobs) != 1 || listing.Jobs[0].JobID != seeded.job {
		t.Fatalf("closed-gate job list = %#v", listing)
	}

	submit := submitRequestFor(t, 0x67, 1, queryTopology(1))
	controlError := requireControlError(t, fixture.exchange(submit), protocol.ControlErrorStarting)
	if !controlError.Retryable || string(controlError.Detail) != "admission gate is closed" {
		t.Fatalf("closed-gate mutation = %#v, want retryable gate-closed Starting", controlError)
	}
}
