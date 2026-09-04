package worker

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/aadityakv/crane/internal/clock"
	"github.com/aadityakv/crane/internal/config"
	"github.com/aadityakv/crane/internal/crane/admission"
	"github.com/aadityakv/crane/internal/crane/membership"
	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
	"github.com/aadityakv/crane/internal/crane/store"
	"github.com/aadityakv/crane/internal/swim"
	"github.com/aadityakv/crane/internal/transport"
	"github.com/aadityakv/crane/internal/wire"
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
	controlMembers := &controlTestMembership{view: membership.View{Members: []swim.Member{{NodeID: fixture.configuration.NodeID, Host: "127.0.0.1", BasePort: fixture.configuration.BasePort, Incarnation: 1, Status: swim.Alive}, {NodeID: fixture.worker.epoch.Coordinator, Host: "127.0.0.2", BasePort: fixture.configuration.BasePort, Incarnation: 1, Status: swim.Alive}}}}
	service.control.membership = controlMembers
	controlSession, err := service.control.NewSession(&net.TCPAddr{IP: net.ParseIP("127.0.0.2"), Port: 40000}, func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := controlSession.authenticate(fixture.worker.epoch.Coordinator, protocol.WorkerHandshake{NodeID: fixture.worker.epoch.Coordinator, WorkerEpoch: model.WorkerEpoch{2}, ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint()}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.control.handleAssignment(context.Background(), controlSession, controlPeer{node: fixture.worker.epoch.Coordinator, epoch: model.WorkerEpoch{2}}, install); err != nil {
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

func TestWorkerServiceDeepClonesConfigAndMapsUnavailableTransferSeams(t *testing.T) {
	fixture := newWorkerServiceFixture(t, false)
	options := fixture.options()
	service, err := NewService(options)
	if err != nil {
		t.Fatal(err)
	}
	for index := range options.Config.RaftVoters {
		if options.Config.RaftVoters[index].NodeID == fixture.worker.epoch.Coordinator {
			options.Config.RaftVoters[index].NodeID = 99
		}
	}
	service.listen = fixture.listen
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	select {
	case <-service.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("worker service did not become ready")
	}
	if err := service.control.authorizeCoordinator(controlPeer{node: fixture.worker.epoch.Coordinator, epoch: model.WorkerEpoch{2}}, fixture.worker.epoch, false); err != nil {
		t.Fatalf("caller config mutation changed service voter authorization: %v", err)
	}
	for _, test := range []struct {
		message wire.MessageType
		err     error
	}{
		{message: wire.MessageCraneResultArtifactChunk, err: ErrResultArtifactUnavailable},
		{message: wire.MessageCraneResultFetchRequest, err: ErrResultFetchUnavailable},
	} {
		response, ok := service.controlError(test.message, test.err).(protocol.WorkerError)
		if !ok || response.Code != protocol.WorkerErrorUnavailable || !response.Retryable {
			t.Fatalf("unavailable seam %d response = %#v", test.message, response)
		}
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run cancellation = %v", err)
	}
}

func TestWorkerControlWireTypesArtifactAndFetchUnavailableAndRejectsResponses(t *testing.T) {
	fixture := newControlFixture(t)
	fixture.repository.work.Fence = fixture.epoch
	path := filepath.Join(t.TempDir(), "worker")
	durable, err := store.Open(path, store.Identity{ClusterID: fixture.cluster, NodeID: fixture.repository.localNode}, store.Options{MaxBytes: 8 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return fixture.repository.localEpoch, nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	if err := durable.Fence(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	authenticator := wire.NewHMACAuthenticator([]byte("01234567890123456789012345678901"))
	repository := &serviceRepository{Store: durable, node: fixture.repository.localNode, fatal: make(chan error, 1)}
	service := &Service{configuration: fixture.configuration, authenticator: authenticator, clock: clock.NewManual(time.Unix(100, 0)), clusterID: fixture.cluster, store: durable, repository: repository}
	client, server := net.Pipe()
	session, err := fixture.owner.NewSession(&net.TCPAddr{IP: net.ParseIP("127.0.0.2"), Port: 40000}, server.Close)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.handleControlConnection(ctx, server, session) }()
	limits := wire.DefaultLimits()
	limits.MaxFrameSize = int(model.WorkerControlMaxFrameBytesV1)
	limits.ExpectedClusterID = &fixture.cluster
	stream := wire.NewTCPFrameStream(client, authenticator, limits, time.Second)
	requestID := byte(1)
	roundTrip := func(message protocol.WorkerMessage) protocol.WorkerMessage {
		t.Helper()
		frame := fixture.frame(t, 2, requestID, message)
		requestID++
		if err := stream.WriteFrame(context.Background(), frame); err != nil {
			t.Fatal(err)
		}
		response, err := stream.ReadFrame(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := protocol.UnmarshalWorkerMessage(response.Header.Message, response.Payload)
		if err != nil {
			t.Fatal(err)
		}
		return decoded
	}
	if _, ok := roundTrip(fixture.handshake(2)).(protocol.WorkerHandshakeAck); !ok {
		t.Fatal("handshake did not succeed")
	}
	var sink model.TaskID
	for _, replica := range fixture.assignment.Assignment.ResultReplicas {
		sink = replica.SinkTask
		break
	}
	data := []byte{1, 2}
	checksum := sha256.Sum256(data)
	artifact := protocol.ResultArtifact{JobID: fixture.assignment.Assignment.JobID, SinkTask: sink, SpecificationHash: fixture.assignment.Topology.Digest(), RecordCount: 1, TotalLength: uint64(len(data)), Checksum: checksum}
	transfer := protocol.TransferChunk{TransferID: protocol.TransferID{1}, JobID: artifact.JobID, TotalLength: uint64(len(data)), Checksum: checksum, Data: data, Final: true}
	chunk := protocol.ResultArtifactChunk{Transfer: transfer, Artifact: artifact, DestinationNodeID: fixture.repository.localNode, DestinationWorkerEpoch: fixture.repository.localEpoch, CoordinatorEpoch: fixture.epoch}
	fetch := protocol.ResultFetchRequest{Artifact: artifact, ReplicaNodeID: fixture.repository.localNode, ReplicaWorkerEpoch: fixture.repository.localEpoch, CoordinatorEpoch: fixture.epoch}
	for _, message := range []protocol.WorkerMessage{chunk, fetch} {
		response, ok := roundTrip(message).(protocol.WorkerError)
		if !ok || response.Code != protocol.WorkerErrorUnavailable || !response.Retryable || response.RelatedMessage != message.MessageType() {
			t.Fatalf("unavailable wire response = %#v", response)
		}
	}
	ack := protocol.ResultArtifactAck{TransferID: transfer.TransferID, NodeID: fixture.repository.localNode, WorkerEpoch: fixture.repository.localEpoch, Artifact: artifact, NextOffset: uint64(len(data)), Complete: true, CoordinatorEpoch: fixture.epoch}
	fetchChunk := protocol.ResultFetchChunk{Transfer: transfer, Artifact: artifact, SourceNodeID: fixture.repository.localNode, SourceWorkerEpoch: fixture.repository.localEpoch, CoordinatorEpoch: fixture.epoch}
	for _, message := range []protocol.WorkerMessage{ack, fetchChunk} {
		response, ok := roundTrip(message).(protocol.WorkerError)
		if !ok || response.Code == protocol.WorkerErrorUnavailable {
			t.Fatalf("unsolicited response accepted/unavailable = %#v", response)
		}
	}
	if fixture.transfer.artifactCalls != 1 || fixture.transfer.fetchCalls != 1 {
		t.Fatalf("unexpected storage effects: artifact=%d fetch=%d", fixture.transfer.artifactCalls, fixture.transfer.fetchCalls)
	}
	cancel()
	_ = stream.Close()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("connection shutdown = %v", err)
	}
}

func TestWorkerControlWireResultRecordClosedSuccessCancellationAndCorrelation(t *testing.T) {
	fixture := newControlFixture(t)
	chunk := validControlResultChunk(t, fixture)
	authenticator := wire.NewHMACAuthenticator([]byte("01234567890123456789012345678901"))
	durable, err := store.Open(filepath.Join(t.TempDir(), "worker"), store.Identity{ClusterID: fixture.cluster, NodeID: fixture.repository.localNode}, store.Options{MaxBytes: 8 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return fixture.repository.localEpoch, nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	if err := durable.Fence(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	repository := &serviceRepository{Store: durable, node: fixture.repository.localNode, fatal: make(chan error, 1)}
	service := &Service{configuration: fixture.configuration, authenticator: authenticator, clock: clock.NewManual(time.Unix(100, 0)), clusterID: fixture.cluster, store: durable, repository: repository}
	client, server := net.Pipe()
	session, err := fixture.owner.NewSession(&net.TCPAddr{IP: net.ParseIP("127.0.0.2"), Port: 40000}, server.Close)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.handleControlConnection(ctx, server, session) }()
	limits := wire.DefaultLimits()
	limits.MaxFrameSize = int(model.WorkerControlMaxFrameBytesV1)
	limits.ExpectedClusterID = &fixture.cluster
	stream := wire.NewTCPFrameStream(client, authenticator, limits, time.Second)
	write := func(id byte, message protocol.WorkerMessage) {
		t.Helper()
		frame := fixture.frame(t, 2, id, message)
		if err := stream.WriteFrame(context.Background(), frame); err != nil {
			t.Fatal(err)
		}
	}
	read := func(id byte) protocol.WorkerMessage {
		t.Helper()
		frame, err := stream.ReadFrame(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if frame.Header.RequestID != (wire.RequestID{id}) || frame.Header.SenderID != fixture.configuration.NodeID {
			t.Fatalf("response correlation header=%#v", frame.Header)
		}
		message, err := protocol.UnmarshalWorkerMessage(frame.Header.Message, frame.Payload)
		if err != nil {
			t.Fatal(err)
		}
		return message
	}
	write(1, fixture.handshake(2))
	if _, ok := read(1).(protocol.WorkerHandshakeAck); !ok {
		t.Fatal("handshake did not succeed")
	}
	// A closed process admission gate does not withhold a replicated record
	// (Task 24 defect #9 ruling): the transfer owner answers it.
	fixture.transfer.ack = protocol.ResultRecordAck{TransferID: chunk.Transfer.TransferID, NodeID: chunk.DestinationNodeID, WorkerEpoch: chunk.DestinationWorkerEpoch, NextOffset: chunk.Transfer.TotalLength, TotalLength: chunk.Transfer.TotalLength, Checksum: chunk.Transfer.Checksum, Complete: true, CoordinatorEpoch: chunk.Provenance.CoordinatorEpoch}
	write(2, chunk)
	if ack, ok := read(2).(protocol.ResultRecordAck); !ok || ack != fixture.transfer.ack {
		t.Fatalf("closed-gate response=%#v", ack)
	}
	if fixture.transfer.calls != 1 {
		t.Fatalf("closed gate reached transfer owner %d times, want 1", fixture.transfer.calls)
	}
	if err := fixture.gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	write(3, chunk)
	if ack, ok := read(3).(protocol.ResultRecordAck); !ok || ack != fixture.transfer.ack {
		t.Fatalf("successful 212/213 response=%#v", ack)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	fixture.owner.beforeMutation = func(kind string) {
		if kind == "result-record" {
			close(entered)
			<-release
		}
	}
	write(4, chunk)
	<-entered
	cancel()
	close(release)
	readCtx, readCancel := context.WithTimeout(context.Background(), time.Second)
	defer readCancel()
	if _, err := stream.ReadFrame(readCtx); err == nil {
		t.Fatal("canceled in-flight 212 produced a response")
	}
	_ = stream.Close()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("canceled connection error=%v", err)
	}
	if fixture.transfer.calls != 2 {
		t.Fatalf("canceled 212 reached transfer owner: calls=%d", fixture.transfer.calls)
	}
}

func TestWorkerControlWireRejectsBadMACAndWrongClusterBeforeMutation(t *testing.T) {
	for _, test := range []struct {
		name        string
		auth        wire.Authenticator
		mutateFrame func(*wire.Frame)
	}{
		{name: "bad MAC", auth: wire.NewHMACAuthenticator([]byte("abcdef0123456789abcdef0123456789")), mutateFrame: func(*wire.Frame) {}},
		{name: "wrong cluster", auth: wire.NewHMACAuthenticator([]byte("01234567890123456789012345678901")), mutateFrame: func(frame *wire.Frame) { frame.Header.ClusterID[0]++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newControlFixture(t)
			authenticator := wire.NewHMACAuthenticator([]byte("01234567890123456789012345678901"))
			durable, err := store.Open(filepath.Join(t.TempDir(), "worker"), store.Identity{ClusterID: fixture.cluster, NodeID: fixture.repository.localNode}, store.Options{MaxBytes: 8 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return fixture.repository.localEpoch, nil }})
			if err != nil {
				t.Fatal(err)
			}
			defer durable.Close()
			repository := &serviceRepository{Store: durable, node: fixture.repository.localNode, fatal: make(chan error, 1)}
			service := &Service{configuration: fixture.configuration, authenticator: authenticator, clock: clock.NewManual(time.Unix(100, 0)), clusterID: fixture.cluster, store: durable, repository: repository}
			client, server := net.Pipe()
			session, err := fixture.owner.NewSession(&net.TCPAddr{IP: net.ParseIP("127.0.0.2"), Port: 40000}, server.Close)
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- service.handleControlConnection(context.Background(), server, session) }()
			stream := wire.NewTCPFrameStream(client, test.auth, wire.DefaultLimits(), time.Second)
			frame := fixture.frame(t, 2, 1, fixture.handshake(2))
			test.mutateFrame(&frame)
			_ = stream.WriteFrame(context.Background(), frame)
			if err := <-done; err == nil {
				t.Fatal("invalid frame kept control connection alive")
			}
			_ = stream.Close()
			if fixture.repository.fenceCalls != 0 || fixture.repository.installCalls != 0 || fixture.repository.observeCalls != 0 || fixture.transfer.calls != 0 {
				t.Fatal("invalid authenticated framing reached control mutation")
			}
		})
	}
}

func TestWorkerServiceStoreAuthorityFailureIsFatal(t *testing.T) {
	fixture := newWorkerServiceFixture(t, false)
	service, err := NewService(fixture.options())
	if err != nil {
		t.Fatal(err)
	}
	service.listen = fixture.listen
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	select {
	case <-service.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("worker service did not become ready")
	}
	if err := service.store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := service.LocalStatus(context.Background()); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("LocalStatus after store close = %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, store.ErrClosed) {
			t.Fatalf("fatal store authority error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("store authority failure did not stop worker service")
	}
}

func TestWorkerServiceCancellationWaitsForExactGateDrain(t *testing.T) {
	fixture := newWorkerServiceFixture(t, false)
	fixture.configuration.Crane.WorkerControlTimeout = config.Duration(100 * time.Millisecond)
	fixture.configuration.Crane.FailureGracePeriod = config.Duration(200 * time.Millisecond)
	service, err := NewService(fixture.options())
	if err != nil {
		t.Fatal(err)
	}
	service.listen = fixture.listen
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	select {
	case <-service.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("worker service did not become ready")
	}
	if err := fixture.gate.Open(fixture.worker.epoch); err != nil {
		t.Fatal(err)
	}
	release, err := fixture.gate.Enter()
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("Run returned before admitted work drained: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	release()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run cancellation = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not join after gate drain")
	}
}

func TestWorkerControlListenerDoesNotAcceptBeforeEngineAndTupleReady(t *testing.T) {
	fixture := newControlFixture(t)
	listener := newServicePipeListener()
	authenticator := wire.NewHMACAuthenticator([]byte("01234567890123456789012345678901"))
	service := &Service{configuration: fixture.configuration, authenticator: authenticator, clock: clock.NewManual(time.Unix(100, 0)), clusterID: fixture.cluster}
	ownershipReady := make(chan struct{})
	dispatchReady := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.runControl(ctx, listener, fixture.owner, ownershipReady, dispatchReady) }()
	<-ownershipReady
	client, server := net.Pipe()
	listener.connections <- server
	limits := wire.DefaultLimits()
	limits.MaxFrameSize = int(model.WorkerControlMaxFrameBytesV1)
	limits.ExpectedClusterID = &fixture.cluster
	stream := wire.NewTCPFrameStream(client, authenticator, limits, time.Second)
	frame := fixture.frame(t, 2, 1, fixture.handshake(2))
	written := make(chan error, 1)
	go func() { written <- stream.WriteFrame(context.Background(), frame) }()
	select {
	case err := <-written:
		t.Fatalf("pre-ready listener accepted frame: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if fixture.repository.fenceCalls != 0 {
		t.Fatal("pre-ready control mutated durable state")
	}
	close(dispatchReady)
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	response, err := stream.ReadFrame(context.Background())
	if err != nil || response.Header.Message != wire.MessageCraneWorkerHandshakeAck {
		t.Fatalf("post-ready handshake = %#v, %v", response.Header, err)
	}
	_ = stream.Close()
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("control shutdown = %v", err)
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
