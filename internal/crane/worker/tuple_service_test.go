package worker

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/config"
	"github.com/aaditya/cs425mp3/internal/crane/membership"
	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
	"github.com/aaditya/cs425mp3/internal/crane/store"
	"github.com/aaditya/cs425mp3/internal/swim"
	"github.com/aaditya/cs425mp3/internal/transport"
	"github.com/aaditya/cs425mp3/internal/wire"
)

func TestTupleServicePersistsCustodyThenRepliesFromSameSocketWithFreshACK(t *testing.T) {
	seams := newTupleServiceTestSeams(t)
	defer seams.stop(t)

	incomingID := wire.RequestID{0x31}
	seams.inject(t, seams.fixture.message(t, 1), incomingID)
	sent := seams.nextSend(t)
	if sent.source != seams.local || sent.destination != seams.remote {
		t.Fatalf("ACK route = %s -> %s, want %s -> %s", sent.source, sent.destination, seams.local, seams.remote)
	}
	frame, err := wire.Decode(sent.payload, seams.authenticator, wire.Limits{ExpectedClusterID: &seams.endpoint.clusterID})
	if err != nil {
		t.Fatal(err)
	}
	if frame.Header.Message != wire.MessageCraneTupleDeliveryAck || frame.Header.RequestID == incomingID || frame.Header.RequestID == (wire.RequestID{}) {
		t.Fatalf("ACK header = %#v, want ACK with fresh nonzero request ID", frame.Header)
	}
	ack, err := protocol.UnmarshalTupleACK(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	message := seams.fixture.message(t, 1)
	if ack.DeliveryID != message.DeliveryID || ack.Destination != message.Destination || ack.Assignment != message.Assignment || ack.Coordinator != message.Coordinator || ack.Status != protocol.TupleAccepted {
		t.Fatalf("ACK = %+v, want exact Accepted envelope", ack)
	}
	seams.repository.mu.Lock()
	receiveCalls := seams.repository.receiveCalls
	seams.repository.mu.Unlock()
	if receiveCalls != 1 {
		t.Fatalf("durable Receive calls = %d, want 1 before ACK", receiveCalls)
	}

	// A logical duplicate uses a new transmission RequestID and receives a new
	// ACK without taking custody twice.
	seams.inject(t, message, wire.RequestID{0x32})
	second := seams.nextSend(t)
	secondFrame, err := wire.Decode(second.payload, seams.authenticator, wire.Limits{ExpectedClusterID: &seams.endpoint.clusterID})
	if err != nil || secondFrame.Header.RequestID == frame.Header.RequestID {
		t.Fatalf("duplicate ACK frame = %#v,%v", secondFrame.Header, err)
	}
	seams.repository.mu.Lock()
	receiveCalls = seams.repository.receiveCalls
	seams.repository.mu.Unlock()
	if receiveCalls != 1 {
		t.Fatalf("duplicate durable Receive calls = %d, want 1", receiveCalls)
	}
}

func TestTupleServiceRejectsTruncationAuthenticationSourceAndUnknownTaskBeforeMutation(t *testing.T) {
	tests := []struct {
		name      string
		packet    func(*tupleServiceTestSeams) transport.Packet
		wantNACK  bool
		requestID wire.RequestID
	}{
		{name: "truncated", requestID: wire.RequestID{1}, packet: func(s *tupleServiceTestSeams) transport.Packet {
			packet := s.deliveryPacket(t, s.fixture.message(t, 2), wire.RequestID{1})
			packet.Truncated = true
			return packet
		}},
		{name: "wrong cluster", requestID: wire.RequestID{2}, packet: func(s *tupleServiceTestSeams) transport.Packet {
			return s.deliveryPacketWithCluster(t, s.fixture.message(t, 2), wire.RequestID{2}, [16]byte{9})
		}},
		{name: "wrong mac", requestID: wire.RequestID{3}, packet: func(s *tupleServiceTestSeams) transport.Packet {
			packet := s.deliveryPacket(t, s.fixture.message(t, 2), wire.RequestID{3})
			packet.Data[len(packet.Data)-1] ^= 1
			return packet
		}},
		{name: "wrong source port", requestID: wire.RequestID{4}, packet: func(s *tupleServiceTestSeams) transport.Packet {
			packet := s.deliveryPacket(t, s.fixture.message(t, 2), wire.RequestID{4})
			packet.From.Port++
			return packet
		}},
		{name: "sender does not own producer", requestID: wire.RequestID{5}, packet: func(s *tupleServiceTestSeams) transport.Packet {
			packet := s.deliveryPacket(t, s.fixture.message(t, 2), wire.RequestID{5})
			frame, err := wire.Decode(packet.Data, s.authenticator, wire.Limits{ExpectedClusterID: &s.endpoint.clusterID})
			if err != nil {
				t.Fatal(err)
			}
			frame.Header.SenderID = 2
			packet.Data, err = wire.Encode(frame.Header, frame.Payload, s.authenticator, wire.DefaultLimits())
			if err != nil {
				t.Fatal(err)
			}
			return packet
		}},
		{name: "unknown task", requestID: wire.RequestID{6}, wantNACK: true, packet: func(s *tupleServiceTestSeams) transport.Packet {
			message := s.fixture.message(t, 2)
			message.DeliveryID.DestinationTask.Partition++
			message.Destination.Task = message.DeliveryID.DestinationTask
			return s.deliveryPacket(t, message, wire.RequestID{6})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			seams := newTupleServiceTestSeams(t)
			defer seams.stop(t)
			seams.datagram.packets <- test.packet(seams)
			if test.wantNACK {
				sent := seams.nextSend(t)
				frame, err := wire.Decode(sent.payload, seams.authenticator, wire.Limits{ExpectedClusterID: &seams.endpoint.clusterID})
				if err != nil || frame.Header.Message != wire.MessageCraneTupleDeliveryNack || frame.Header.RequestID == test.requestID {
					t.Fatalf("NACK frame = %#v,%v", frame.Header, err)
				}
			} else {
				seams.expectNoSend(t)
			}
			seams.repository.mu.Lock()
			receiveCalls := seams.repository.receiveCalls
			seams.repository.mu.Unlock()
			if receiveCalls != 0 {
				t.Fatalf("rejected packet performed %d durable Receive calls", receiveCalls)
			}
		})
	}
}

// TestTupleServiceReplayCapacityProcessesUnrecordedWithoutEviction pins the
// Task 24 defect #8 ruling: a bounded replay cache exhausted by an
// authenticated peer never suppresses idempotent +7 handling. A fresh request
// at capacity is processed and answered without being recorded, the retained
// identities are never evicted (their replays stay silent), and a replay of
// the unrecorded identity is simply re-answered idempotently.
func TestTupleServiceReplayCapacityProcessesUnrecordedWithoutEviction(t *testing.T) {
	seams := newTupleServiceTestSeams(t)
	defer seams.stop(t)
	seams.service.replay = newTupleReplay(seams.clock, time.Minute, time.Second, 1, 1)

	seams.inject(t, seams.fixture.message(t, 1), wire.RequestID{0x41})
	_ = seams.nextSend(t)
	seams.inject(t, seams.fixture.message(t, 2), wire.RequestID{0x42})
	unrecorded := seams.nextSend(t)
	frame, err := wire.Decode(unrecorded.payload, seams.authenticator, wire.Limits{ExpectedClusterID: &seams.endpoint.clusterID})
	if err != nil || frame.Header.Message != wire.MessageCraneTupleDeliveryAck {
		t.Fatalf("fresh delivery at replay capacity = %d,%v, want ACK", frame.Header.Message, err)
	}
	// Full capacity retains the first request rather than evicting it and
	// reopening its replay window.
	seams.datagram.packets <- seams.deliveryPacket(t, seams.fixture.message(t, 1), wire.RequestID{0x41})
	seams.expectNoSend(t)
	// The unrecorded identity replays into idempotent custody: answered again,
	// no further durable mutation.
	seams.inject(t, seams.fixture.message(t, 2), wire.RequestID{0x42})
	replayed := seams.nextSend(t)
	frame, err = wire.Decode(replayed.payload, seams.authenticator, wire.Limits{ExpectedClusterID: &seams.endpoint.clusterID})
	if err != nil || frame.Header.Message != wire.MessageCraneTupleDeliveryAck {
		t.Fatalf("replayed unrecorded delivery = %d,%v, want ACK", frame.Header.Message, err)
	}
	seams.repository.mu.Lock()
	receiveCalls := seams.repository.receiveCalls
	seams.repository.mu.Unlock()
	if receiveCalls != 2 {
		t.Fatalf("replay pressure performed %d durable Receive calls, want exactly one per distinct delivery", receiveCalls)
	}
}

func TestTupleServiceSemanticInvalidReplayDoesNotConsumeAcceptedCapacity(t *testing.T) {
	seams := newTupleServiceTestSeams(t)
	defer seams.stop(t)
	seams.service.replay = newTupleReplay(seams.clock, time.Minute, time.Second, 1, 1)

	stale := seams.fixture.message(t, 1)
	stale.Coordinator.Term++
	seams.inject(t, stale, wire.RequestID{0x61})
	first := seams.nextSend(t)
	firstFrame, err := wire.Decode(first.payload, seams.authenticator, wire.Limits{ExpectedClusterID: &seams.endpoint.clusterID})
	if err != nil || firstFrame.Header.Message != wire.MessageCraneTupleDeliveryNack {
		t.Fatalf("stale delivery response = %d,%v, want NACK", firstFrame.Header.Message, err)
	}

	seams.inject(t, seams.fixture.message(t, 1), wire.RequestID{0x62})
	valid := seams.nextSend(t)
	validFrame, err := wire.Decode(valid.payload, seams.authenticator, wire.Limits{ExpectedClusterID: &seams.endpoint.clusterID})
	if err != nil || validFrame.Header.Message != wire.MessageCraneTupleDeliveryAck {
		t.Fatalf("valid delivery after semantic invalid = %d,%v, want ACK", validFrame.Header.Message, err)
	}

	// At capacity a fresh invalid request is still answered (processed without
	// being recorded, defect #8 ruling) while the retained live invalid ID is
	// never evicted: its replay stays silent.
	seams.inject(t, stale, wire.RequestID{0x63})
	unrecorded := seams.nextSend(t)
	unrecordedFrame, err := wire.Decode(unrecorded.payload, seams.authenticator, wire.Limits{ExpectedClusterID: &seams.endpoint.clusterID})
	if err != nil || unrecordedFrame.Header.Message != wire.MessageCraneTupleDeliveryNack {
		t.Fatalf("fresh invalid delivery at capacity = %d,%v, want NACK", unrecordedFrame.Header.Message, err)
	}
	seams.inject(t, stale, wire.RequestID{0x61})
	seams.expectNoSend(t)
	seams.clock.Advance(time.Minute)
	seams.inject(t, stale, wire.RequestID{0x63})
	expired := seams.nextSend(t)
	expiredFrame, err := wire.Decode(expired.payload, seams.authenticator, wire.Limits{ExpectedClusterID: &seams.endpoint.clusterID})
	if err != nil || expiredFrame.Header.Message != wire.MessageCraneTupleDeliveryNack {
		t.Fatalf("invalid delivery after replay expiry = %d,%v, want NACK", expiredFrame.Header.Message, err)
	}
}

func TestTupleServiceFiveHundredTwelveFreshStaleDeliveriesCannotConsumeAcceptedCapacity(t *testing.T) {
	seams := newTupleServiceTestSeams(t)
	defer seams.stop(t)
	stale := seams.fixture.message(t, 1)
	stale.Coordinator.Term++
	for sequence := uint64(1); sequence <= uint64(tupleReplayEntriesPerSender+1); sequence++ {
		seams.inject(t, stale, tupleRequestID(sequence))
	}
	// Replaying the oldest strict-invalid identity must not evict it.
	seams.inject(t, stale, tupleRequestID(1))
	seams.inject(t, seams.fixture.message(t, 1), tupleRequestID(uint64(tupleReplayEntriesPerSender+2)))
	waitFor(t, func() bool { return len(seams.datagram.sentPackets()) >= tupleReplayEntriesPerSender+1 })
	sends := seams.datagram.sentPackets()
	if got := len(sends); got != tupleReplayEntriesPerSender+1 {
		t.Fatalf("responses after invalid pressure = %d, want 512 NACKs plus one valid ACK", got)
	}
	last, err := wire.Decode(sends[len(sends)-1].payload, seams.authenticator, wire.Limits{ExpectedClusterID: &seams.endpoint.clusterID})
	if err != nil || last.Header.Message != wire.MessageCraneTupleDeliveryAck {
		t.Fatalf("valid delivery after 513 stale deliveries = %d,%v, want ACK", last.Header.Message, err)
	}
	seams.repository.mu.Lock()
	receiveCalls := seams.repository.receiveCalls
	seams.repository.mu.Unlock()
	if receiveCalls != 1 {
		t.Fatalf("stale delivery pressure performed %d durable Receive calls, want only the valid delivery", receiveCalls)
	}
}

func TestTupleServiceAuthenticatedWrongSourceInvalidCacheRetainsOldAndReclaimsExpiry(t *testing.T) {
	seams := newTupleServiceTestSeams(t)
	defer seams.stop(t)
	seams.service.replay = newTupleReplay(seams.clock, time.Minute, time.Second, 2, 2)
	message := seams.fixture.message(t, 1)
	for _, id := range []wire.RequestID{{0x71}, {0x72}, {0x73}, {0x71}} {
		packet := seams.deliveryPacket(t, message, id)
		packet.From.Port++
		seams.datagram.packets <- packet
	}
	seams.inject(t, message, wire.RequestID{0x74})
	_ = seams.nextSend(t) // The valid sentinel proves every prior packet ran.
	if err := seams.service.replay.preflight(1, wire.RequestID{0x71}, seams.clock.Now()); !errors.Is(err, wire.ErrReplay) {
		t.Fatalf("old live wrong-source ID = %v, want ErrReplay", err)
	}
	if err := seams.service.replay.preflight(1, wire.RequestID{0x73}, seams.clock.Now()); err != nil {
		t.Fatalf("new wrong-source ID was recorded after strict capacity rejection: %v", err)
	}

	seams.clock.Advance(time.Minute)
	packet := seams.deliveryPacket(t, message, wire.RequestID{0x73})
	packet.From.Port++
	seams.datagram.packets <- packet
	seams.inject(t, message, wire.RequestID{0x75})
	_ = seams.nextSend(t)
	if err := seams.service.replay.preflight(1, wire.RequestID{0x73}, seams.clock.Now()); !errors.Is(err, wire.ErrReplay) {
		t.Fatalf("wrong-source ID after expiry admission = %v, want ErrReplay", err)
	}
}

func tupleRequestID(sequence uint64) wire.RequestID {
	var id wire.RequestID
	binary.BigEndian.PutUint64(id[8:], sequence)
	return id
}

func TestTupleServiceCancellationClosesSocketAndJoinsBlockingReceive(t *testing.T) {
	configuration := tupleTestConfig(t)
	datagram := newCloseOnlyTupleDatagram()
	endpoint, err := NewTupleEndpoint(TupleEndpointOptions{Config: configuration, Authenticator: wire.NewHMACAuthenticator(bytes.Repeat([]byte{0xa5}, 32)), Clock: clock.NewManual(time.Unix(1_700_000_000, 0)), Membership: &membership.Authorizer{}, Datagram: datagram})
	if err != nil {
		t.Fatal(err)
	}
	fixture := workerFixture(t)
	engine, err := NewEngine(testEngineOptions(newFakeRepository(fixture), admissionGateForTupleTest(t, fixture), endpoint))
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewTupleService(TupleServiceOptions{Endpoint: endpoint, Engine: engine})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	<-service.Ready()
	select {
	case <-datagram.receiveStarted:
	case <-time.After(time.Second):
		t.Fatal("receive loop did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run cancellation = %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Run cancellation did not close the socket and join blocked Receive")
	}
	select {
	case <-datagram.receiveExited:
	default:
		t.Fatal("Run returned before its receive loop joined")
	}
}

func TestTupleServiceFatalDeliveryStorePoisonClosesAndJoinsSocket(t *testing.T) {
	seams := newTupleServiceTestSeams(t)
	seams.repository.mu.Lock()
	seams.repository.receiveErr = store.ErrUnavailable
	seams.repository.mu.Unlock()
	seams.inject(t, seams.fixture.message(t, 1), wire.RequestID{0x81})
	select {
	case err := <-seams.serviceDone:
		if !errors.Is(err, store.ErrUnavailable) {
			t.Fatalf("delivery poison Run error = %v, want store.ErrUnavailable", err)
		}
	case <-time.After(time.Second):
		t.Fatal("TupleService swallowed fatal delivery store poison")
	}
	select {
	case <-seams.datagram.closed:
	default:
		t.Fatal("TupleService returned poison before closing and joining its socket")
	}
	seams.cancelEngine()
	if err := <-seams.engineDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Engine stop = %v", err)
	}
}

func TestTupleServicePostCustodyReplayCommitFailureWithholdsACKAndFreshRetryIsIdempotent(t *testing.T) {
	seams := newTupleServiceTestSeams(t)
	defer seams.stop(t)
	seams.repository.receiveStarted = make(chan struct{})
	seams.repository.receiveRelease = make(chan struct{})
	message := seams.fixture.message(t, 1)
	seams.inject(t, message, wire.RequestID{0x83})
	select {
	case <-seams.repository.receiveStarted:
	case <-time.After(time.Second):
		t.Fatal("delivery did not reach durable custody")
	}
	seams.clock.Advance(time.Duration(seams.configuration.Timing.ReplayWindow))
	close(seams.repository.receiveRelease)
	seams.expectNoSend(t)

	seams.inject(t, message, wire.RequestID{0x84})
	retry := seams.nextSend(t)
	frame, err := wire.Decode(retry.payload, seams.authenticator, wire.Limits{ExpectedClusterID: &seams.endpoint.clusterID})
	if err != nil || frame.Header.Message != wire.MessageCraneTupleDeliveryAck {
		t.Fatalf("fresh logical retry after post-custody replay failure = %d,%v, want ACK", frame.Header.Message, err)
	}
	seams.repository.mu.Lock()
	receiveCalls := seams.repository.receiveCalls
	seams.repository.mu.Unlock()
	if receiveCalls != 1 {
		t.Fatalf("fresh logical retry repeated durable custody %d times, want 1", receiveCalls)
	}
}

func TestTupleServiceFatalACKStorePoisonClosesAndJoinsSocket(t *testing.T) {
	fixture := workerFixture(t)
	baseRepository := newFakeRepository(fixture)
	delivery := fixture.delivery(t, 1)
	outbox := store.OutboxRecord{ID: delivery.ID, Tuple: delivery.Tuple, Producer: delivery.Producer, Destination: delivery.Destination, AssignmentRevision: delivery.AssignmentRevision, AssignmentDigest: delivery.AssignmentDigest, CoordinatorEpoch: delivery.CoordinatorEpoch}
	baseRepository.work.Outboxes = []store.OutboxRecord{outbox}
	baseRepository.outboxes[outbox.ID] = outbox
	repository := &unavailableOutboxRepository{fakeRepository: baseRepository}

	configuration := tupleTestConfig(t)
	authenticator := wire.NewHMACAuthenticator(bytes.Repeat([]byte{0xa5}, 32))
	manual := clock.NewManual(time.Unix(100, 0))
	datagram := newTupleTestDatagram()
	endpoint, err := NewTupleEndpoint(TupleEndpointOptions{Config: configuration, Authenticator: authenticator, Clock: manual, Membership: &membership.Authorizer{}, Datagram: datagram})
	if err != nil {
		t.Fatal(err)
	}
	local, _ := configuration.BindEndpoint(config.ServiceCraneTupleACK)
	endpoint.peers = tupleTestMembership{members: []swim.Member{{NodeID: 1, Host: configuration.AdvertiseHost, BasePort: configuration.BasePort, Incarnation: 1, Status: swim.Alive}}, auth: func(nodeID uint16, source config.Endpoint, service config.Service) error {
		if nodeID == 1 && source == local && service == config.ServiceCraneTupleACK {
			return nil
		}
		return membership.ErrUnauthorized
	}}
	options := testEngineOptions(repository, admissionGateForTupleTest(t, fixture), endpoint)
	options.Clock = manual
	engine, err := NewEngine(options)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewTupleService(TupleServiceOptions{Endpoint: endpoint, Engine: engine})
	if err != nil {
		t.Fatal(err)
	}
	serviceContext, cancelService := context.WithCancel(context.Background())
	defer cancelService()
	serviceDone := make(chan error, 1)
	go func() { serviceDone <- service.Run(serviceContext) }()
	<-service.Ready()
	engineContext, cancelEngine := context.WithCancel(context.Background())
	engineDone := runEngine(t, engineContext, engine)
	<-engine.Ready()

	message := deliveryMessageForOutbox(outbox)
	ack := protocol.TupleACK{DeliveryID: message.DeliveryID, Destination: message.Destination, Assignment: message.Assignment, Coordinator: message.Coordinator, Status: protocol.TupleCompleted}
	injectTupleACK(t, datagram, endpoint, authenticator, manual, local, ack, wire.RequestID{0x82})
	select {
	case err := <-serviceDone:
		if !errors.Is(err, store.ErrUnavailable) {
			t.Fatalf("ACK poison Run error = %v, want store.ErrUnavailable", err)
		}
	case <-time.After(time.Second):
		t.Fatal("TupleService swallowed fatal ACK store poison")
	}
	select {
	case <-datagram.closed:
	default:
		t.Fatal("TupleService returned ACK poison before closing and joining its socket")
	}
	cancelEngine()
	if err := <-engineDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Engine stop = %v", err)
	}
}

type unavailableOutboxRepository struct{ *fakeRepository }

func (repository *unavailableOutboxRepository) MarkOutboxCompleted(model.DeliveryID) error {
	return store.ErrUnavailable
}

func TestTupleRetrySurvivesLostDeliveryAndReorderedCompletedAcceptedACKs(t *testing.T) {
	fixture := workerFixture(t)
	repository := newFakeRepository(fixture)
	delivery := fixture.delivery(t, 1)
	outbox := store.OutboxRecord{ID: delivery.ID, Tuple: delivery.Tuple, Producer: delivery.Producer, Destination: delivery.Destination, AssignmentRevision: delivery.AssignmentRevision, AssignmentDigest: delivery.AssignmentDigest, CoordinatorEpoch: delivery.CoordinatorEpoch}
	repository.work.Outboxes = []store.OutboxRecord{outbox}
	repository.outboxes[outbox.ID] = outbox

	configuration := tupleTestConfig(t)
	authenticator := wire.NewHMACAuthenticator(bytes.Repeat([]byte{0xa5}, 32))
	manual := clock.NewManual(time.Unix(100, 0))
	datagram := newTupleTestDatagram()
	endpoint, err := NewTupleEndpoint(TupleEndpointOptions{Config: configuration, Authenticator: authenticator, Clock: manual, Membership: &membership.Authorizer{}, Datagram: datagram})
	if err != nil {
		t.Fatal(err)
	}
	local, _ := configuration.BindEndpoint(config.ServiceCraneTupleACK)
	endpoint.peers = tupleTestMembership{members: []swim.Member{{NodeID: 1, Host: configuration.AdvertiseHost, BasePort: configuration.BasePort, Incarnation: 1, Status: swim.Alive}}, auth: func(nodeID uint16, source config.Endpoint, service config.Service) error {
		if nodeID == 1 && source == local && service == config.ServiceCraneTupleACK {
			return nil
		}
		return membership.ErrUnauthorized
	}}
	options := testEngineOptions(repository, admissionGateForTupleTest(t, fixture), endpoint)
	options.Clock = manual
	engine, err := NewEngine(options)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewTupleService(TupleServiceOptions{Endpoint: endpoint, Engine: engine})
	if err != nil {
		t.Fatal(err)
	}
	serviceContext, cancelService := context.WithCancel(context.Background())
	serviceDone := make(chan error, 1)
	go func() { serviceDone <- service.Run(serviceContext) }()
	<-service.Ready()
	engineContext, cancelEngine := context.WithCancel(context.Background())
	engineDone := runEngine(t, engineContext, engine)
	<-engine.Ready()

	first := nextTupleTestSend(t, datagram)
	firstFrame, err := wire.Decode(first.payload, authenticator, wire.Limits{ExpectedClusterID: &endpoint.clusterID})
	if err != nil {
		t.Fatal(err)
	}
	manual.Advance(options.AcceptedRetryInterval)
	second := nextTupleTestSend(t, datagram)
	secondFrame, err := wire.Decode(second.payload, authenticator, wire.Limits{ExpectedClusterID: &endpoint.clusterID})
	if err != nil {
		t.Fatal(err)
	}
	if firstFrame.Header.RequestID == secondFrame.Header.RequestID {
		t.Fatal("durable retry reused the lost transmission RequestID")
	}

	message := deliveryMessageForOutbox(outbox)
	completed := protocol.TupleACK{DeliveryID: message.DeliveryID, Destination: message.Destination, Assignment: message.Assignment, Coordinator: message.Coordinator, Status: protocol.TupleCompleted}
	accepted := completed
	accepted.Status = protocol.TupleAccepted
	injectTupleACK(t, datagram, endpoint, authenticator, manual, local, completed, wire.RequestID{0x51})
	waitFor(t, func() bool {
		repository.mu.Lock()
		defer repository.mu.Unlock()
		return repository.outboxes[outbox.ID].Completed
	})
	injectTupleACK(t, datagram, endpoint, authenticator, manual, local, accepted, wire.RequestID{0x52})
	waitFor(t, func() bool {
		return errors.Is(service.replay.preflight(accepted.Destination.WorkerID, wire.RequestID{0x52}, manual.Now()), wire.ErrReplay)
	})
	repository.mu.Lock()
	stored := repository.outboxes[outbox.ID]
	repository.mu.Unlock()
	if !stored.Completed {
		t.Fatal("late Accepted ACK reopened a durably Completed outbox")
	}

	cancelService()
	if err := <-serviceDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("TupleService stop = %v", err)
	}
	cancelEngine()
	if err := <-engineDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Engine stop = %v", err)
	}
}

func nextTupleTestSend(t *testing.T, datagram *tupleTestDatagram) tupleTestSend {
	t.Helper()
	select {
	case <-datagram.sent:
		packets := datagram.sentPackets()
		return packets[len(packets)-1]
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for tuple transmission")
		return tupleTestSend{}
	}
}

func injectTupleACK(t *testing.T, datagram *tupleTestDatagram, endpoint *TupleEndpoint, authenticator wire.Authenticator, sourceClock clock.Clock, source config.Endpoint, ack protocol.TupleACK, requestID wire.RequestID) {
	t.Helper()
	payload, err := protocol.MarshalTupleACK(ack)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := wire.Encode(wire.Header{Version: wire.Version1, Message: wire.MessageCraneTupleDeliveryAck, ClusterID: endpoint.clusterID, SenderID: ack.Destination.WorkerID, RequestID: requestID, TimestampMillis: sourceClock.Now().UnixMilli(), Codec: wire.CodecBinary}, payload, authenticator, wire.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	datagram.packets <- transport.Packet{From: source, Data: frame}
}

type closeOnlyTupleDatagram struct {
	closed         chan struct{}
	receiveStarted chan struct{}
	receiveExited  chan struct{}
	closeOnce      sync.Once
	receiveOnce    sync.Once
}

func newCloseOnlyTupleDatagram() *closeOnlyTupleDatagram {
	return &closeOnlyTupleDatagram{closed: make(chan struct{}), receiveStarted: make(chan struct{}), receiveExited: make(chan struct{})}
}

func (d *closeOnlyTupleDatagram) Send(context.Context, config.Endpoint, []byte) error { return nil }
func (d *closeOnlyTupleDatagram) SendFrom(context.Context, config.Endpoint, config.Endpoint, []byte) error {
	return nil
}
func (d *closeOnlyTupleDatagram) Receive(context.Context) (transport.Packet, error) {
	d.receiveOnce.Do(func() { close(d.receiveStarted) })
	<-d.closed
	select {
	case <-d.receiveExited:
	default:
		close(d.receiveExited)
	}
	return transport.Packet{}, transport.ErrDatagramClosed
}
func (d *closeOnlyTupleDatagram) Close() error {
	d.closeOnce.Do(func() { close(d.closed) })
	return nil
}

type tupleServiceTestSeams struct {
	fixture       workerTestFixture
	repository    *fakeRepository
	configuration config.NodeConfig
	authenticator wire.Authenticator
	clock         *clock.Manual
	datagram      *tupleTestDatagram
	endpoint      *TupleEndpoint
	service       *TupleService
	local         config.Endpoint
	remote        config.Endpoint
	cancelEngine  context.CancelFunc
	cancelService context.CancelFunc
	engineDone    <-chan error
	serviceDone   <-chan error
}

func newTupleServiceTestSeams(t *testing.T) *tupleServiceTestSeams {
	t.Helper()
	fixture := workerFixture(t)
	repository := newFakeRepository(fixture)
	configuration := tupleTestConfig(t)
	authenticator := wire.NewHMACAuthenticator(bytes.Repeat([]byte{0xa5}, 32))
	manual := clock.NewManual(time.Unix(1_700_000_000, 0))
	datagram := newTupleTestDatagram()
	endpoint, err := NewTupleEndpoint(TupleEndpointOptions{Config: configuration, Authenticator: authenticator, Clock: manual, Membership: &membership.Authorizer{}, Datagram: datagram})
	if err != nil {
		t.Fatal(err)
	}
	local, _ := configuration.BindEndpoint(config.ServiceCraneTupleACK)
	remote := local
	endpoint.peers = tupleTestMembership{
		members: []swim.Member{{NodeID: 1, Host: configuration.AdvertiseHost, BasePort: configuration.BasePort, Incarnation: 1, Status: swim.Alive}},
		auth: func(nodeID uint16, source config.Endpoint, service config.Service) error {
			if nodeID != 1 || source != remote || service != config.ServiceCraneTupleACK {
				return membership.ErrUnauthorized
			}
			return nil
		},
	}
	options := testEngineOptions(repository, admissionGateForTupleTest(t, fixture), endpoint)
	options.Execute = func(ctx context.Context, _ model.OperatorSpec, _ model.Tuple) ([]model.Tuple, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	engine, err := NewEngine(options)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewTupleService(TupleServiceOptions{Endpoint: endpoint, Engine: engine})
	if err != nil {
		t.Fatal(err)
	}
	engineContext, cancelEngine := context.WithCancel(context.Background())
	engineDone := runEngine(t, engineContext, engine)
	select {
	case <-engine.Ready():
	case <-time.After(time.Second):
		t.Fatal("Engine did not become ready")
	}
	serviceContext, cancelService := context.WithCancel(context.Background())
	serviceDone := make(chan error, 1)
	go func() { serviceDone <- service.Run(serviceContext) }()
	select {
	case <-service.Ready():
	case <-time.After(time.Second):
		t.Fatal("TupleService did not become ready")
	}
	return &tupleServiceTestSeams{fixture: fixture, repository: repository, configuration: configuration, authenticator: authenticator, clock: manual, datagram: datagram, endpoint: endpoint, service: service, local: local, remote: remote, cancelEngine: cancelEngine, cancelService: cancelService, engineDone: engineDone, serviceDone: serviceDone}
}

func (s *tupleServiceTestSeams) deliveryPacket(t *testing.T, message protocol.TupleDelivery, id wire.RequestID) transport.Packet {
	t.Helper()
	return s.deliveryPacketWithCluster(t, message, id, s.endpoint.clusterID)
}

func (s *tupleServiceTestSeams) deliveryPacketWithCluster(t *testing.T, message protocol.TupleDelivery, id wire.RequestID, cluster [16]byte) transport.Packet {
	t.Helper()
	payload, err := protocol.MarshalTupleDelivery(message)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := wire.Encode(wire.Header{Version: wire.Version1, Message: wire.MessageCraneTupleDelivery, ClusterID: cluster, SenderID: message.Producer.WorkerID, RequestID: id, TimestampMillis: s.clock.Now().UnixMilli(), Codec: wire.CodecBinary}, payload, s.authenticator, wire.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	return transport.Packet{From: s.remote, Data: frame}
}

func (s *tupleServiceTestSeams) inject(t *testing.T, message protocol.TupleDelivery, id wire.RequestID) {
	t.Helper()
	s.datagram.packets <- s.deliveryPacket(t, message, id)
}

func (s *tupleServiceTestSeams) nextSend(t *testing.T) tupleTestSend {
	t.Helper()
	select {
	case <-s.datagram.sent:
		packets := s.datagram.sentPackets()
		return packets[len(packets)-1]
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for tuple response")
		return tupleTestSend{}
	}
}

func (s *tupleServiceTestSeams) expectNoSend(t *testing.T) {
	t.Helper()
	select {
	case <-s.datagram.sent:
		t.Fatalf("unexpected tuple response: %+v", s.datagram.sentPackets())
	case <-time.After(30 * time.Millisecond):
	}
}

func (s *tupleServiceTestSeams) stop(t *testing.T) {
	t.Helper()
	s.cancelService()
	if err := <-s.serviceDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("TupleService stop = %v", err)
	}
	s.cancelEngine()
	if err := <-s.engineDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Engine stop = %v", err)
	}
}
