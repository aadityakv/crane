package worker

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/config"
	"github.com/aaditya/cs425mp3/internal/crane/admission"
	"github.com/aaditya/cs425mp3/internal/crane/membership"
	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
	"github.com/aaditya/cs425mp3/internal/crane/store"
	"github.com/aaditya/cs425mp3/internal/transport"
	"github.com/aaditya/cs425mp3/internal/wire"
)

func TestWorkerServiceConstructorIsSideEffectFreeAndRequiresExactDependencies(t *testing.T) {
	fixture := newWorkerServiceFixture(t, false)
	service, err := NewService(fixture.options())
	if err != nil {
		t.Fatal(err)
	}
	if service.Name() != "crane-worker" || service.gate != fixture.gate {
		t.Fatalf("service name/gate = %q/%p want crane-worker/%p", service.Name(), service.gate, fixture.gate)
	}
	if fixture.openCalls != 0 || fixture.datagram.operationCount() != 0 || service.store != nil || service.engine != nil || service.endpoint != nil || service.tuple != nil || service.control != nil {
		t.Fatalf("constructor performed runtime work: opens=%d datagram=%d store=%p engine=%p endpoint=%p tuple=%p control=%p", fixture.openCalls, fixture.datagram.operationCount(), service.store, service.engine, service.endpoint, service.tuple, service.control)
	}
	select {
	case <-service.Ready():
		t.Fatal("constructor closed Ready")
	default:
	}
	if _, err := service.LocalStatus(context.Background()); !errors.Is(err, ErrServiceNotReady) {
		t.Fatalf("pre-Run LocalStatus error = %v", err)
	}

	valid := fixture.options()
	tests := []struct {
		name   string
		mutate func(*ServiceOptions)
	}{
		{name: "authenticator", mutate: func(o *ServiceOptions) { o.Authenticator = nil }},
		{name: "clock", mutate: func(o *ServiceOptions) { o.Clock = nil }},
		{name: "membership", mutate: func(o *ServiceOptions) { o.Membership = nil }},
		{name: "gate", mutate: func(o *ServiceOptions) { o.Gate = nil }},
		{name: "open store", mutate: func(o *ServiceOptions) { o.OpenStore = nil }},
		{name: "datagram", mutate: func(o *ServiceOptions) { o.Datagram = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := valid
			test.mutate(&options)
			if _, err := NewService(options); err == nil {
				t.Fatal("invalid service options accepted")
			}
		})
	}
}

func TestWorkerServiceReadyOrdersStoreRecoveryAndComposesExactGateEndpointAndSocket(t *testing.T) {
	fixture := newWorkerServiceFixture(t, true)
	service, err := NewService(fixture.options())
	if err != nil {
		t.Fatal(err)
	}
	service.listen = fixture.listen
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	<-fixture.openStarted
	select {
	case <-service.Ready():
		t.Fatal("Ready closed before store recovery")
	default:
	}
	if fixture.listenerCalls != 0 || fixture.datagram.operationCount() != 0 {
		t.Fatalf("network ownership began before store recovery: listener=%d datagram=%d", fixture.listenerCalls, fixture.datagram.operationCount())
	}
	close(fixture.openRelease)
	select {
	case <-service.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("worker service did not become ready")
	}
	if service.store == nil || service.engine == nil || service.endpoint == nil || service.tuple == nil || service.control == nil {
		t.Fatal("Ready closed before full worker composition")
	}
	if service.gate != fixture.gate || service.engine.gate != fixture.gate || service.endpoint != service.tuple.endpoint || service.endpoint != service.engine.sender {
		t.Fatalf("pointer composition gate=%p/%p endpoint=%p tuple=%p sender=%p", service.gate, service.engine.gate, service.endpoint, service.tuple.endpoint, service.engine.sender)
	}
	if service.engine.acceptedRetry != time.Duration(fixture.configuration.Crane.TupleRetryInterval) || service.engine.completedRetry != time.Duration(fixture.configuration.Crane.TupleCompletionRetryInterval) {
		t.Fatalf("engine retries = %v/%v", service.engine.acceptedRetry, service.engine.completedRetry)
	}
	if service.endpoint.injected != fixture.datagram || service.endpoint.datagram != fixture.datagram {
		t.Fatal("+7 send/receive owner did not retain and activate the exact injected socket")
	}
	if fixture.openCalls != 1 || fixture.listenerCalls != 1 || fixture.openPath != filepath.Join(fixture.configuration.StorageDir, WorkerStoreDirectory) || fixture.openIdentity.NodeID != fixture.configuration.NodeID || fixture.openIdentity.ClusterID != service.clusterID || fixture.openOptions.MaxBytes != fixture.configuration.Crane.MaxWorkerStoreBytes {
		t.Fatalf("Run dependencies: opens=%d listeners=%d path=%q identity=%#v options=%#v", fixture.openCalls, fixture.listenerCalls, fixture.openPath, fixture.openIdentity, fixture.openOptions)
	}

	if release, err := fixture.gate.Enter(); !errors.Is(err, admission.ErrClosed) || release != nil {
		t.Fatalf("recovered Running assignment reopened gate before reinstall: release=%v err=%v", release != nil, err)
	}
	status, err := service.LocalStatus(context.Background())
	if err != nil || status.NodeID != fixture.configuration.NodeID || status.WorkerEpoch != fixture.worker.localEpoch || status.CoordinatorEpoch != fixture.worker.epoch || len(status.Assignments) != 1 {
		t.Fatalf("LocalStatus = %#v,%v", status, err)
	}
	status.Assignments[0].AssignmentDigest[0]++
	again, err := service.LocalStatus(context.Background())
	if err != nil || again.Assignments[0].AssignmentDigest == status.Assignments[0].AssignmentDigest {
		t.Fatal("LocalStatus did not return deeply owned data")
	}

	install := protocol.AssignmentSetInstall{Assignment: fixture.worker.assignment.Assignment, Specification: fixture.worker.assignment.Topology.Spec(), SpecificationDigest: fixture.worker.assignment.Topology.Digest(), JobControlRevision: fixture.worker.assignment.JobControlRevision, SchedulingState: model.Running, CoordinatorEpoch: fixture.worker.epoch}
	if _, err := service.control.handleAssignment(context.Background(), controlPeer{node: fixture.worker.epoch.Coordinator, epoch: model.WorkerEpoch{2}}, install); err != nil {
		t.Fatalf("exact-current Running reinstall: %v", err)
	}
	if release, err := fixture.gate.Enter(); err != nil {
		t.Fatalf("current reinstall did not open exact shared gate: %v", err)
	} else {
		release()
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run cancellation = %v", err)
	}
	select {
	case <-fixture.listener.closed:
	default:
		t.Fatal("+5 listener remained open after Run")
	}
	select {
	case <-fixture.datagram.closed:
	default:
		t.Fatal("+7 socket remained open after Run")
	}
	if _, err := service.store.RecoverWork(); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("store after Run = %v, want closed", err)
	}
	if release, err := fixture.gate.Enter(); !errors.Is(err, admission.ErrClosed) || release != nil {
		t.Fatalf("gate after Run release=%v err=%v", release != nil, err)
	}
}

func TestWorkerServiceListenerFailureBeforeReadyClosesRecoveredStoreAndSocket(t *testing.T) {
	fixture := newWorkerServiceFixture(t, false)
	service, err := NewService(fixture.options())
	if err != nil {
		t.Fatal(err)
	}
	want := errors.New("listen failed")
	service.listen = func(string, string) (net.Listener, error) { return nil, want }
	if err := service.Run(context.Background()); !errors.Is(err, want) {
		t.Fatalf("Run listener error = %v", err)
	}
	select {
	case <-service.Ready():
		t.Fatal("listener failure closed Ready")
	default:
	}
	if service.store == nil {
		t.Fatal("test did not reach recovered store")
	}
	if _, err := service.store.RecoverWork(); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("store after listener failure = %v", err)
	}
	if fixture.datagram.operationCount() != 0 {
		t.Fatalf("listener failure activated +7 socket %d times", fixture.datagram.operationCount())
	}
}

func TestWorkerServiceControlTransportRequiresHandshakeAndKeepsServing(t *testing.T) {
	fixture := newWorkerServiceFixture(t, false)
	service, err := NewService(fixture.options())
	if err != nil {
		t.Fatal(err)
	}
	listener := newServicePipeListener()
	service.listen = func(_, _ string) (net.Listener, error) { return listener, nil }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	select {
	case <-service.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("worker service did not become ready")
	}
	connection, serverConnection := net.Pipe()
	listener.connections <- serverConnection
	limits := wire.DefaultLimits()
	limits.MaxFrameSize = int(model.WorkerControlMaxFrameBytesV1)
	limits.ExpectedClusterID = &service.clusterID
	stream := wire.NewTCPFrameStream(connection, fixture.authenticator, limits, time.Second)
	payload, err := protocol.MarshalWorkerMessage(protocol.FenceRequest{CoordinatorEpoch: fixture.worker.epoch})
	if err != nil {
		t.Fatal(err)
	}
	requestID := wire.RequestID{1}
	request := wire.Frame{Header: wire.Header{Version: wire.Version1, Message: wire.MessageCraneWorkerFenceRequest, ClusterID: service.clusterID, SenderID: fixture.worker.epoch.Coordinator, RequestID: requestID, TimestampMillis: fixture.clock.Now().UnixMilli(), Codec: wire.CodecBinary}, Payload: payload}
	if err := stream.WriteFrame(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	response, err := stream.ReadFrame(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	message, err := protocol.UnmarshalWorkerMessage(response.Header.Message, response.Payload)
	rejection, ok := message.(protocol.WorkerError)
	if err != nil || !ok || response.Header.RequestID != requestID || response.Header.SenderID != fixture.configuration.NodeID || rejection.Code != protocol.WorkerErrorUnauthorized || rejection.RelatedMessage != wire.MessageCraneWorkerFenceRequest || rejection.CoordinatorEpoch != fixture.worker.epoch {
		t.Fatalf("pre-handshake response = %#v %#v, %v", response.Header, message, err)
	}
	if _, err := service.LocalStatus(context.Background()); err != nil {
		t.Fatalf("typed client rejection stopped service: %v", err)
	}
	_ = stream.Close()
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run cancellation = %v", err)
	}
}

type workerServiceFixture struct {
	configuration config.NodeConfig
	worker        workerTestFixture
	gate          *admission.Gate
	datagram      *tupleTestDatagram
	membership    *membership.Authorizer
	clock         clock.Clock
	authenticator wire.Authenticator
	listener      *serviceTestListener
	openStarted   chan struct{}
	openRelease   chan struct{}
	openCalls     int
	listenerCalls int
	openPath      string
	openIdentity  store.Identity
	openOptions   store.Options
	mu            sync.Mutex
}

func newWorkerServiceFixture(t *testing.T, blockOpen bool) *workerServiceFixture {
	t.Helper()
	configuration := tupleTestConfig(t)
	worker := workerFixture(t)
	fixture := &workerServiceFixture{configuration: configuration, worker: worker, gate: admission.NewGate(), datagram: newTupleTestDatagram(), membership: &membership.Authorizer{}, clock: clock.NewManual(time.Unix(100, 0)), authenticator: wire.NewHMACAuthenticator([]byte("01234567890123456789012345678901")), listener: newServiceTestListener(), openStarted: make(chan struct{}), openRelease: make(chan struct{})}
	if !blockOpen {
		close(fixture.openRelease)
	}
	return fixture
}

func (fixture *workerServiceFixture) options() ServiceOptions {
	return ServiceOptions{Config: fixture.configuration, Authenticator: fixture.authenticator, Clock: fixture.clock, Membership: fixture.membership, Gate: fixture.gate, OpenStore: fixture.openStore, Datagram: fixture.datagram}
}

func (fixture *workerServiceFixture) openStore(path string, identity store.Identity, options store.Options) (*store.Store, error) {
	fixture.mu.Lock()
	fixture.openCalls++
	fixture.openPath, fixture.openIdentity, fixture.openOptions = path, identity, options
	fixture.mu.Unlock()
	select {
	case <-fixture.openStarted:
	default:
		close(fixture.openStarted)
	}
	<-fixture.openRelease
	options.NewWorkerEpoch = func() (model.WorkerEpoch, error) { return fixture.worker.localEpoch, nil }
	workerStore, err := store.Open(path, identity, options)
	if err != nil {
		return nil, err
	}
	if err := workerStore.Fence(fixture.worker.epoch); err != nil {
		_ = workerStore.Close()
		return nil, err
	}
	if err := workerStore.InstallAssignment(fixture.worker.assignment.Assignment, fixture.worker.assignment.Topology.Spec(), fixture.worker.assignment.JobControlRevision, fixture.worker.assignment.SchedulingState, fixture.worker.epoch); err != nil {
		_ = workerStore.Close()
		return nil, err
	}
	return workerStore, nil
}

func (fixture *workerServiceFixture) listen(network, address string) (net.Listener, error) {
	fixture.mu.Lock()
	fixture.listenerCalls++
	fixture.mu.Unlock()
	return fixture.listener, nil
}

type serviceTestListener struct {
	closed    chan struct{}
	closeOnce sync.Once
}

func newServiceTestListener() *serviceTestListener {
	return &serviceTestListener{closed: make(chan struct{})}
}
func (listener *serviceTestListener) Accept() (net.Conn, error) {
	<-listener.closed
	return nil, net.ErrClosed
}
func (listener *serviceTestListener) Close() error {
	listener.closeOnce.Do(func() { close(listener.closed) })
	return nil
}
func (*serviceTestListener) Addr() net.Addr { return serviceTestAddr("127.0.0.1:0") }

type servicePipeListener struct {
	connections chan net.Conn
	closed      chan struct{}
	closeOnce   sync.Once
}

func newServicePipeListener() *servicePipeListener {
	return &servicePipeListener{connections: make(chan net.Conn, 1), closed: make(chan struct{})}
}
func (listener *servicePipeListener) Accept() (net.Conn, error) {
	select {
	case connection := <-listener.connections:
		return connection, nil
	case <-listener.closed:
		return nil, net.ErrClosed
	}
}
func (listener *servicePipeListener) Close() error {
	listener.closeOnce.Do(func() { close(listener.closed) })
	return nil
}
func (*servicePipeListener) Addr() net.Addr { return serviceTestAddr("127.0.0.1:0") }

type serviceTestAddr string

func (address serviceTestAddr) Network() string { return "tcp" }
func (address serviceTestAddr) String() string  { return string(address) }

var _ transport.SourceDatagram = (*tupleTestDatagram)(nil)
