package control

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/config"
	"github.com/aaditya/cs425mp3/internal/crane/admission"
	"github.com/aaditya/cs425mp3/internal/crane/membership"
	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
	"github.com/aaditya/cs425mp3/internal/crane/state"
	"github.com/aaditya/cs425mp3/internal/raft"
	"github.com/aaditya/cs425mp3/internal/swim"
	"github.com/aaditya/cs425mp3/internal/wire"
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
		RaftVoters: []config.RaftVoter{{NodeID: 1, Endpoint: "127.0.0.1:19108"}, {NodeID: 2, Endpoint: "127.0.0.2:19208"}, {NodeID: 3, Endpoint: "127.0.0.3:19308"}},
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
	if len(requested) != 1 || requested[0] != "tcp/127.0.0.1:19106" {
		t.Fatalf("listen requests = %v, want exactly tcp/127.0.0.1:19106", requested)
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
		fixture.start()
		response := fixture.exchange(statusRequest(model.JobID{0x33}))
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
	fixture.start()

	held, err := net.Dial("tcp", fixture.address())
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	// The held connection occupies the sole slot without sending a frame.
	time.Sleep(50 * time.Millisecond)
	overflow, err := net.Dial("tcp", fixture.address())
	if err != nil {
		t.Fatal(err)
	}
	defer overflow.Close()
	assertConnectionClosedWithoutResponse(t, overflow)

	_ = held.Close()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if response, err := fixture.exchangeFrame(2, testRequestID(t), statusRequest(model.JobID{0x36})); err == nil {
			requireControlError(t, response, protocol.ControlErrorNotFound)
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("released connection slot never served a request")
		}
		time.Sleep(10 * time.Millisecond)
	}
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
