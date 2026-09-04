package worker

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/aadityakv/crane/internal/clock"
	"github.com/aadityakv/crane/internal/config"
	"github.com/aadityakv/crane/internal/crane/integrationhook"
	"github.com/aadityakv/crane/internal/crane/membership"
	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
	"github.com/aadityakv/crane/internal/crane/store"
	"github.com/aadityakv/crane/internal/swim"
	"github.com/aadityakv/crane/internal/wire"
)

// scriptedHook answers DatagramAction from a per-(direction,message) queue
// and records every consultation and boundary in order.
type scriptedHook struct {
	mu      sync.Mutex
	queues  map[integrationhook.Direction]map[wire.MessageType][]integrationhook.Action
	entries []string
}

func newScriptedHook() *scriptedHook {
	return &scriptedHook{queues: map[integrationhook.Direction]map[wire.MessageType][]integrationhook.Action{}}
}

func (hook *scriptedHook) script(direction integrationhook.Direction, message wire.MessageType, actions ...integrationhook.Action) {
	hook.mu.Lock()
	defer hook.mu.Unlock()
	if hook.queues[direction] == nil {
		hook.queues[direction] = map[wire.MessageType][]integrationhook.Action{}
	}
	hook.queues[direction][message] = append(hook.queues[direction][message], actions...)
}

func (hook *scriptedHook) DurableBoundary(name string) {
	hook.mu.Lock()
	defer hook.mu.Unlock()
	hook.entries = append(hook.entries, "boundary:"+name)
}

func (hook *scriptedHook) DatagramAction(direction integrationhook.Direction, message wire.MessageType) integrationhook.Action {
	hook.mu.Lock()
	defer hook.mu.Unlock()
	action := integrationhook.Pass
	if queue := hook.queues[direction][message]; len(queue) > 0 {
		action = queue[0]
		hook.queues[direction][message] = queue[1:]
	}
	hook.entries = append(hook.entries, direction.String()+":"+integrationhook.MessageName(message)+":"+action.String())
	return action
}

func (hook *scriptedHook) log() []string {
	hook.mu.Lock()
	defer hook.mu.Unlock()
	return append([]string(nil), hook.entries...)
}

func TestTupleEndpointConsultsHookOnRealSendPath(t *testing.T) {
	configuration := tupleTestConfig(t)
	authenticator := wire.NewHMACAuthenticator(bytes.Repeat([]byte{0xa5}, 32))
	datagram := newTupleTestDatagram()
	hook := newScriptedHook()
	endpoint, err := NewTupleEndpoint(TupleEndpointOptions{Config: configuration, Authenticator: authenticator, Clock: clock.NewManual(time.Unix(1_700_000_000, 0)), Membership: &membership.Authorizer{}, Datagram: datagram, Hook: hook})
	if err != nil {
		t.Fatal(err)
	}
	endpoint.peers = tupleTestMembership{members: []swim.Member{{NodeID: 1, Host: configuration.AdvertiseHost, BasePort: configuration.BasePort, Incarnation: 1, Status: swim.Alive}}}
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
	select {
	case <-service.Ready():
	case <-time.After(time.Second):
		t.Fatal("TupleService did not become ready")
	}
	defer func() {
		cancel()
		<-done
	}()

	hook.script(integrationhook.Send, wire.MessageCraneTupleDelivery, integrationhook.Drop, integrationhook.Duplicate)
	message := fixture.message(t, 1)
	for i := 0; i < 3; i++ {
		if err := endpoint.Send(context.Background(), message); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	sends := datagram.sentPackets()
	// Drop → 0, Duplicate → 2, Pass → 1.
	if len(sends) != 3 {
		t.Fatalf("sent packets = %d, want 3 (drop, duplicate×2, pass)", len(sends))
	}
	source, _ := configuration.BindEndpoint(config.ServiceCraneTupleACK)
	for index, sent := range sends {
		if sent.source != source {
			t.Fatalf("send %d left source %s, want the bound +7 endpoint %s", index, sent.source, source)
		}
	}
	if !bytes.Equal(sends[0].payload, sends[1].payload) {
		t.Fatal("duplicate transmitted different bytes from the original")
	}
	if bytes.Equal(sends[1].payload, sends[2].payload) {
		t.Fatal("pass transmission reused the duplicated frame")
	}
	want := []string{"send:delivery:drop", "send:delivery:duplicate", "send:delivery:pass"}
	if got := hook.log(); len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("hook consultations = %v, want %v", got, want)
	}
}

// holdingHook holds every delivery addressed to `to` until released.
type holdingHook struct {
	scriptedHook
	to   uint16
	held []func()
}

func (hook *holdingHook) HoldDatagram(message wire.MessageType, destination uint16, resend func()) bool {
	if message != wire.MessageCraneTupleDelivery || destination != hook.to {
		return false
	}
	hook.held = append(hook.held, resend)
	return true
}

func TestTupleEndpointHeldDeliveryLeavesSameSocketOnlyWhenReleased(t *testing.T) {
	configuration := tupleTestConfig(t)
	authenticator := wire.NewHMACAuthenticator(bytes.Repeat([]byte{0xa5}, 32))
	datagram := newTupleTestDatagram()
	hook := &holdingHook{scriptedHook: *newScriptedHook(), to: 1}
	endpoint, err := NewTupleEndpoint(TupleEndpointOptions{Config: configuration, Authenticator: authenticator, Clock: clock.NewManual(time.Unix(1_700_000_000, 0)), Membership: &membership.Authorizer{}, Datagram: datagram, Hook: hook})
	if err != nil {
		t.Fatal(err)
	}
	endpoint.peers = tupleTestMembership{members: []swim.Member{{NodeID: 1, Host: configuration.AdvertiseHost, BasePort: configuration.BasePort, Incarnation: 1, Status: swim.Alive}}}
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
	defer func() {
		cancel()
		<-done
	}()
	if err := endpoint.Send(context.Background(), fixture.message(t, 1)); err != nil {
		t.Fatal(err)
	}
	if got := len(datagram.sentPackets()); got != 0 {
		t.Fatalf("held delivery reached the socket: %d sends", got)
	}
	if len(hook.held) != 1 {
		t.Fatalf("held closures = %d, want 1", len(hook.held))
	}
	hook.held[0]()
	sends := datagram.sentPackets()
	source, _ := configuration.BindEndpoint(config.ServiceCraneTupleACK)
	if len(sends) != 1 || sends[0].source != source {
		t.Fatalf("released send = %+v, want exactly one from %s", sends, source)
	}
	frame, err := wire.Decode(sends[0].payload, authenticator, wire.Limits{ExpectedClusterID: &endpoint.clusterID})
	if err != nil || frame.Header.Message != wire.MessageCraneTupleDelivery {
		t.Fatalf("released frame = %#v,%v", frame.Header, err)
	}
}

func TestTupleServiceInboundDropSkipsCustodyAndACKAndDuplicateACKLeavesSocketTwice(t *testing.T) {
	seams := newTupleServiceTestSeams(t)
	defer seams.stop(t)
	hook := newScriptedHook()
	seams.endpoint.hook = hook
	hook.script(integrationhook.Receive, wire.MessageCraneTupleDelivery, integrationhook.Drop)
	hook.script(integrationhook.Send, wire.MessageCraneTupleDeliveryAck, integrationhook.Duplicate)

	seams.inject(t, seams.fixture.message(t, 1), wire.RequestID{0x41})
	seams.expectNoSend(t)
	seams.repository.mu.Lock()
	receiveCalls := seams.repository.receiveCalls
	seams.repository.mu.Unlock()
	if receiveCalls != 0 {
		t.Fatalf("dropped inbound delivery took custody: receiveCalls=%d", receiveCalls)
	}

	seams.inject(t, seams.fixture.message(t, 1), wire.RequestID{0x42})
	first := seams.nextSend(t)
	second := seams.nextSend(t)
	if !bytes.Equal(first.payload, second.payload) || first.source != seams.local || second.source != seams.local {
		t.Fatalf("duplicate ACK = %v/%v bytes-equal=%v", first.source, second.source, bytes.Equal(first.payload, second.payload))
	}
	frame, err := wire.Decode(first.payload, seams.authenticator, wire.Limits{ExpectedClusterID: &seams.endpoint.clusterID})
	if err != nil || frame.Header.Message != wire.MessageCraneTupleDeliveryAck {
		t.Fatalf("duplicated frame = %#v,%v", frame.Header, err)
	}
	seams.repository.mu.Lock()
	receiveCalls = seams.repository.receiveCalls
	seams.repository.mu.Unlock()
	if receiveCalls != 1 {
		t.Fatalf("passed delivery Receive calls = %d, want 1", receiveCalls)
	}
}

// TestTupleServiceDurableBoundaryPrecedesACKOnRealStore composes the real
// worker store behind the tuple service and proves the Received boundary is
// published after the store's fsync and strictly before the ACK is written.
func TestTupleServiceDurableBoundaryPrecedesACKOnRealStore(t *testing.T) {
	fixture := workerFixture(t)
	configuration := tupleTestConfig(t)
	authenticator := wire.NewHMACAuthenticator(bytes.Repeat([]byte{0xa5}, 32))
	manual := clock.NewManual(time.Unix(1_700_000_000, 0))
	datagram := newTupleTestDatagram()
	hook := newScriptedHook()
	endpoint, err := NewTupleEndpoint(TupleEndpointOptions{Config: configuration, Authenticator: authenticator, Clock: manual, Membership: &membership.Authorizer{}, Datagram: datagram, Hook: hook})
	if err != nil {
		t.Fatal(err)
	}
	local, _ := configuration.BindEndpoint(config.ServiceCraneTupleACK)
	endpoint.peers = tupleTestMembership{
		members: []swim.Member{{NodeID: 1, Host: configuration.AdvertiseHost, BasePort: configuration.BasePort, Incarnation: 1, Status: swim.Alive}},
		auth: func(nodeID uint16, source config.Endpoint, service config.Service) error {
			if nodeID != 1 || source != local || service != config.ServiceCraneTupleACK {
				return membership.ErrUnauthorized
			}
			return nil
		},
	}
	durable, err := store.Open(t.TempDir()+"/worker", store.Identity{ClusterID: endpoint.clusterID, NodeID: fixture.localNode}, store.Options{MaxBytes: 8 << 20, Hook: hook, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return fixture.localEpoch, nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	if err := durable.Fence(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	if err := durable.InstallAssignment(fixture.assignment.Assignment, fixture.topology.Spec(), 1, model.Running, fixture.epoch); err != nil {
		t.Fatal(err)
	}
	repository := &serviceRepository{Store: durable, node: fixture.localNode, fatal: make(chan error, 1)}
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
	case <-time.After(2 * time.Second):
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
	seams := &tupleServiceTestSeams{fixture: fixture, configuration: configuration, authenticator: authenticator, clock: manual, datagram: datagram, endpoint: endpoint, service: service, local: local, remote: local, cancelEngine: cancelEngine, cancelService: cancelService, engineDone: engineDone, serviceDone: serviceDone}
	defer seams.stop(t)

	// Discard the boundaries published while seeding the store. The local
	// source task starts emitting its own deliveries once the engine is
	// Ready, so only the injected delivery's own chain is asserted below.
	seeded := len(hook.log())
	seams.inject(t, fixture.message(t, 1), wire.RequestID{0x51})
	awaitACK := func() {
		t.Helper()
		deadline := time.After(5 * time.Second)
		for {
			for _, sent := range datagram.sentPackets() {
				frame, err := wire.Decode(sent.payload, authenticator, wire.Limits{ExpectedClusterID: &endpoint.clusterID})
				if err == nil && frame.Header.Message == wire.MessageCraneTupleDeliveryAck {
					return
				}
				if err == nil && frame.Header.Message == wire.MessageCraneTupleDeliveryNack {
					nack, _ := protocol.UnmarshalTupleNACK(frame.Payload)
					t.Fatalf("delivery answered with NACK code %d", nack.Code)
				}
			}
			select {
			case <-datagram.sent:
			case <-deadline:
				t.Fatalf("no ACK left the socket; hook log %v", hook.log()[seeded:])
			}
		}
	}
	awaitACK()
	entries := hook.log()[seeded:]
	position := func(entry string) int {
		for index, candidate := range entries {
			if candidate == entry {
				return index
			}
		}
		return -1
	}
	received, boundary, acked := position("recv:delivery:pass"), position("boundary:"+store.BoundaryDeliveryReceived), position("send:ack:pass")
	if received < 0 || boundary < 0 || acked < 0 || !(received < boundary && boundary < acked) {
		t.Fatalf("ordered hook log = %v, want recv:delivery < boundary:%s < send:ack", entries, store.BoundaryDeliveryReceived)
	}
	count := 0
	for _, entry := range entries {
		if entry == "boundary:"+store.BoundaryDeliveryReceived {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("delivery-received published %d times, want exactly once: %v", count, entries)
	}

	// The durable duplicate is answered from custody: a second ACK leaves the
	// socket with no second Received boundary.
	ackedBefore := len(datagram.sentPackets())
	seams.inject(t, fixture.message(t, 1), wire.RequestID{0x52})
	deadline := time.After(5 * time.Second)
	for {
		acks := 0
		for _, sent := range datagram.sentPackets()[ackedBefore:] {
			frame, err := wire.Decode(sent.payload, authenticator, wire.Limits{ExpectedClusterID: &endpoint.clusterID})
			if err == nil && frame.Header.Message == wire.MessageCraneTupleDeliveryAck {
				acks++
			}
		}
		if acks >= 1 {
			break
		}
		select {
		case <-datagram.sent:
		case <-deadline:
			t.Fatal("duplicate delivery was not acknowledged")
		}
	}
	count = 0
	for _, entry := range hook.log()[seeded:] {
		if entry == "boundary:"+store.BoundaryDeliveryReceived {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("duplicate custody republished delivery-received: %d occurrences", count)
	}
}

func TestWorkerServiceThreadsHookIntoStoreAndEndpoint(t *testing.T) {
	fixture := newWorkerServiceFixture(t, false)
	hook := newScriptedHook()
	options := fixture.options()
	options.Hook = hook
	service, err := NewService(options)
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
	fixture.mu.Lock()
	storeHook := fixture.openOptions.Hook
	fixture.mu.Unlock()
	if storeHook != integrationhook.Hook(hook) {
		t.Fatalf("store hook = %T, want the injected hook", storeHook)
	}
	if service.endpoint.hook != integrationhook.Hook(hook) {
		t.Fatalf("endpoint hook = %T, want the injected hook", service.endpoint.hook)
	}
	cancel()
	<-done

	// A service without an injected hook composes the production no-op.
	plain := newWorkerServiceFixture(t, false)
	plainService, err := NewService(plain.options())
	if err != nil {
		t.Fatal(err)
	}
	plainService.listen = plain.listen
	plainContext, cancelPlain := context.WithCancel(context.Background())
	plainDone := make(chan error, 1)
	go func() { plainDone <- plainService.Run(plainContext) }()
	select {
	case <-plainService.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("plain worker service did not become ready")
	}
	if _, ok := plainService.endpoint.hook.(integrationhook.Noop); !ok {
		t.Fatalf("default endpoint hook = %T, want Noop", plainService.endpoint.hook)
	}
	cancelPlain()
	<-plainDone
}
