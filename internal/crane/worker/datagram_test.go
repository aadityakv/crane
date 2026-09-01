package worker

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/config"
	"github.com/aaditya/cs425mp3/internal/crane/admission"
	"github.com/aaditya/cs425mp3/internal/crane/membership"
	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
	"github.com/aaditya/cs425mp3/internal/swim"
	"github.com/aaditya/cs425mp3/internal/transport"
	"github.com/aaditya/cs425mp3/internal/wire"
)

func TestCraneDatagramEndpointConstructionDoesNotOpenOrUseSocket(t *testing.T) {
	datagram := newTupleTestDatagram()
	endpoint, err := NewTupleEndpoint(TupleEndpointOptions{
		Config:        tupleTestConfig(t),
		Authenticator: wire.NewHMACAuthenticator(bytes.Repeat([]byte{0xa5}, 32)),
		Clock:         clock.NewManual(time.Unix(1_700_000_000, 0)),
		Membership:    &membership.Authorizer{},
		Datagram:      datagram,
	})
	if err != nil {
		t.Fatal(err)
	}
	if endpoint == nil || datagram.operationCount() != 0 {
		t.Fatalf("construction endpoint=%v operations=%d, want endpoint and no socket operation", endpoint, datagram.operationCount())
	}
}

func TestCraneDatagramEndpointConstructionDoesNotReopenValidatedSecretFile(t *testing.T) {
	configuration := tupleTestConfig(t)
	if err := os.Remove(configuration.ClusterSecretFile); err != nil {
		t.Fatal(err)
	}
	if _, err := NewTupleEndpoint(TupleEndpointOptions{
		Config:        configuration,
		Authenticator: wire.NewHMACAuthenticator(bytes.Repeat([]byte{0xa5}, 32)),
		Clock:         clock.NewManual(time.Unix(1_700_000_000, 0)),
		Membership:    &membership.Authorizer{},
		Datagram:      newTupleTestDatagram(),
	}); err != nil {
		t.Fatalf("side-effect-free construction reopened validated secret path: %v", err)
	}
}

func TestCraneDatagramSendBeforeActivationReturnsTypedNotReady(t *testing.T) {
	datagram := newTupleTestDatagram()
	endpoint, err := NewTupleEndpoint(TupleEndpointOptions{
		Config:        tupleTestConfig(t),
		Authenticator: wire.NewHMACAuthenticator(bytes.Repeat([]byte{0xa5}, 32)),
		Clock:         clock.NewManual(time.Unix(1_700_000_000, 0)),
		Membership:    &membership.Authorizer{},
		Datagram:      datagram,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := endpoint.Send(context.Background(), protocol.TupleDelivery{}); !errors.Is(err, ErrTupleEndpointNotReady) {
		t.Fatalf("Send before activation error = %v, want ErrTupleEndpointNotReady", err)
	}
	if datagram.operationCount() != 0 {
		t.Fatalf("not-ready send performed %d datagram operations", datagram.operationCount())
	}
}

func TestCraneDatagramReplayBoundsPerSenderAndSenderGuardMapWithoutEviction(t *testing.T) {
	manual := clock.NewManual(time.Unix(1_700_000_000, 0))
	replay := newTupleReplay(manual, time.Minute, time.Second, 1, 1)
	first := wire.RequestID{1}
	if err := replay.preflight(1, first, manual.Now()); err != nil {
		t.Fatal(err)
	}
	if err := replay.commit(1, first, manual.Now()); err != nil {
		t.Fatal(err)
	}
	if err := replay.preflight(1, wire.RequestID{2}, manual.Now()); !errors.Is(err, wire.ErrReplayCacheFull) {
		t.Fatalf("per-sender capacity error = %v, want ErrReplayCacheFull", err)
	}
	if err := replay.preflight(1, first, manual.Now()); !errors.Is(err, wire.ErrReplay) {
		t.Fatalf("retained first request error = %v, want ErrReplay", err)
	}
	manual.Advance(time.Minute)
	if err := replay.preflight(2, wire.RequestID{3}, manual.Now()); !errors.Is(err, wire.ErrReplayCacheFull) {
		t.Fatalf("sender guard map capacity error = %v, want ErrReplayCacheFull", err)
	}
}

func TestCraneDatagramStrictInvalidPreflightsPerSenderBeforeGlobalCommit(t *testing.T) {
	manual := clock.NewManual(time.Unix(1_700_000_000, 0))
	replay := newTupleReplay(manual, time.Minute, time.Second, 2, 1)
	first := wire.RequestID{1}
	if err := replay.preflightInvalid(1, first, manual.Now()); err != nil {
		t.Fatal(err)
	}
	if err := replay.commitInvalid(1, first, manual.Now()); err != nil {
		t.Fatal(err)
	}
	if err := replay.preflightInvalid(1, wire.RequestID{2}, manual.Now()); !errors.Is(err, wire.ErrReplayCacheFull) {
		t.Fatalf("per-sender invalid capacity = %v, want ErrReplayCacheFull", err)
	}
	// The rejected second ID must not partially consume global invalid capacity;
	// another sender can still use the remaining global slot.
	if err := replay.preflightInvalid(2, wire.RequestID{3}, manual.Now()); err != nil {
		t.Fatalf("independent sender invalid preflight = %v", err)
	}
	if err := replay.commitInvalid(2, wire.RequestID{3}, manual.Now()); err != nil {
		t.Fatalf("independent sender invalid commit = %v", err)
	}
	if err := replay.preflight(1, wire.RequestID{4}, manual.Now()); err != nil {
		t.Fatalf("invalid entries consumed accepted capacity: %v", err)
	}
}

func TestCraneDatagramUsesExactPlusSevenSourceFreshRequestIDsAndRejectsOversizeBeforeSend(t *testing.T) {
	configuration := tupleTestConfig(t)
	authenticator := wire.NewHMACAuthenticator(bytes.Repeat([]byte{0xa5}, 32))
	datagram := newTupleTestDatagram()
	endpoint, err := NewTupleEndpoint(TupleEndpointOptions{Config: configuration, Authenticator: authenticator, Clock: clock.NewManual(time.Unix(1_700_000_000, 0)), Membership: &membership.Authorizer{}, Datagram: datagram})
	if err != nil {
		t.Fatal(err)
	}
	endpoint.peers = tupleTestMembership{members: []swim.Member{{NodeID: 1, Host: configuration.AdvertiseHost, BasePort: configuration.BasePort, Incarnation: 1, Status: swim.Alive}}}

	fixture := workerFixture(t)
	repository := newFakeRepository(fixture)
	engine, err := NewEngine(testEngineOptions(repository, admissionGateForTupleTest(t, fixture), endpoint))
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewTupleService(TupleServiceOptions{Endpoint: endpoint, Engine: engine})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewTupleService(TupleServiceOptions{Endpoint: endpoint, Engine: engine}); !errors.Is(err, ErrTupleEndpointInUse) {
		t.Fatalf("second service construction error = %v, want ErrTupleEndpointInUse", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	select {
	case <-service.Ready():
	case <-time.After(time.Second):
		t.Fatal("TupleService did not become ready")
	}

	message := fixture.message(t, 1)
	if err := endpoint.Send(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if err := endpoint.Send(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	sends := datagram.sentPackets()
	if len(sends) != 2 {
		t.Fatalf("sent packets = %d, want 2", len(sends))
	}
	wantSource, _ := configuration.BindEndpoint(config.ServiceCraneTupleACK)
	wantDestination, _ := configuration.AdvertiseEndpoint(config.ServiceCraneTupleACK)
	var firstID wire.RequestID
	for index, sent := range sends {
		if sent.source != wantSource || sent.destination != wantDestination {
			t.Fatalf("send %d route = %s -> %s, want %s -> %s", index, sent.source, sent.destination, wantSource, wantDestination)
		}
		frame, decodeErr := wire.Decode(sent.payload, authenticator, wire.Limits{ExpectedClusterID: &endpoint.clusterID})
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		if frame.Header.Message != wire.MessageCraneTupleDelivery || frame.Header.SenderID != configuration.NodeID || frame.Header.RequestID == (wire.RequestID{}) {
			t.Fatalf("send %d header = %#v", index, frame.Header)
		}
		decoded, decodeErr := protocol.UnmarshalTupleDelivery(frame.Payload)
		if decodeErr != nil || decoded.DeliveryID != message.DeliveryID {
			t.Fatalf("send %d delivery = %+v,%v", index, decoded, decodeErr)
		}
		if index == 0 {
			firstID = frame.Header.RequestID
		} else if frame.Header.RequestID == firstID {
			t.Fatal("successive transmissions reused RequestID")
		}
	}

	oversized := message
	oversized.Tuple = model.Tuple{Fields: []model.Field{{Name: "a", Value: model.Value{Type: model.ValueBytes, Bytes: make([]byte, 513)}}}}
	if err := endpoint.Send(context.Background(), oversized); err == nil {
		t.Fatal("oversized tuple send succeeded")
	}
	if got := len(datagram.sentPackets()); got != 2 {
		t.Fatalf("oversized tuple reached network: sends=%d", got)
	}
	forgedProducer := message
	forgedProducer.Producer.WorkerID = 2
	if err := endpoint.Send(context.Background(), forgedProducer); err == nil {
		t.Fatal("endpoint sent a delivery whose producer token names another worker")
	}
	if got := len(datagram.sentPackets()); got != 2 {
		t.Fatalf("foreign producer token reached network: sends=%d", got)
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("TupleService cancellation = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("TupleService did not join receive loop")
	}
	if err := endpoint.Send(context.Background(), message); !errors.Is(err, ErrTupleEndpointNotReady) {
		t.Fatalf("send after shutdown error = %v, want ErrTupleEndpointNotReady", err)
	}
}

type tupleTestMembership struct {
	members []swim.Member
	auth    func(uint16, config.Endpoint, config.Service) error
}

func (m tupleTestMembership) View() membership.View {
	return membership.View{Revision: 1, Members: append([]swim.Member(nil), m.members...)}
}

func (m tupleTestMembership) AuthorizeUDP(nodeID uint16, remote net.Addr, service config.Service) error {
	address, ok := remote.(*net.UDPAddr)
	if !ok || address.Port < 1 || address.Port > 65535 {
		return membership.ErrUnauthorized
	}
	if m.auth != nil {
		return m.auth(nodeID, config.Endpoint{Host: address.IP.String(), Port: uint16(address.Port)}, service)
	}
	return nil
}

type tupleTestSend struct {
	source      config.Endpoint
	destination config.Endpoint
	payload     []byte
}

type tupleTestDatagram struct {
	mu         sync.Mutex
	operations int
	sends      []tupleTestSend
	packets    chan transport.Packet
	sent       chan struct{}
	closed     chan struct{}
	closeOnce  sync.Once
}

func newTupleTestDatagram() *tupleTestDatagram {
	return &tupleTestDatagram{packets: make(chan transport.Packet, 32), sent: make(chan struct{}, 32), closed: make(chan struct{})}
}

func (d *tupleTestDatagram) Send(ctx context.Context, destination config.Endpoint, payload []byte) error {
	return d.SendFrom(ctx, config.Endpoint{}, destination, payload)
}

func (d *tupleTestDatagram) SendFrom(ctx context.Context, source, destination config.Endpoint, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d.mu.Lock()
	d.operations++
	d.sends = append(d.sends, tupleTestSend{source: source, destination: destination, payload: append([]byte(nil), payload...)})
	d.mu.Unlock()
	select {
	case d.sent <- struct{}{}:
	default:
	}
	return nil
}

func (d *tupleTestDatagram) sentPackets() []tupleTestSend {
	d.mu.Lock()
	defer d.mu.Unlock()
	result := make([]tupleTestSend, len(d.sends))
	for index, sent := range d.sends {
		result[index] = sent
		result[index].payload = append([]byte(nil), sent.payload...)
	}
	return result
}

func (d *tupleTestDatagram) Receive(ctx context.Context) (transport.Packet, error) {
	d.mu.Lock()
	d.operations++
	d.mu.Unlock()
	select {
	case packet := <-d.packets:
		return packet, nil
	case <-d.closed:
		return transport.Packet{}, transport.ErrDatagramClosed
	case <-ctx.Done():
		return transport.Packet{}, ctx.Err()
	}
}

func (d *tupleTestDatagram) Close() error {
	d.mu.Lock()
	d.operations++
	d.mu.Unlock()
	d.closeOnce.Do(func() { close(d.closed) })
	return nil
}

func (d *tupleTestDatagram) operationCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.operations
}

func tupleTestConfig(t *testing.T) config.NodeConfig {
	t.Helper()
	secret := t.TempDir() + "/cluster.secret"
	if err := os.WriteFile(secret, bytes.Repeat([]byte{1}, 32), 0o600); err != nil {
		t.Fatal(err)
	}
	configuration := config.NodeConfig{
		NodeID: 1, ClusterID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", BindHost: "127.0.0.1", AdvertiseHost: "127.0.0.1",
		BasePort: 19100, Introducer: "127.0.0.1:19102", StorageDir: t.TempDir(), ClusterSecretFile: secret,
		RaftVoters: []config.RaftVoter{{NodeID: 1, Endpoint: "127.0.0.1:19108"}, {NodeID: 2, Endpoint: "127.0.0.2:19208"}, {NodeID: 3, Endpoint: "127.0.0.3:19308"}},
		Raft:       config.DefaultRaftConfig(), Crane: config.DefaultCraneConfig(), Timing: config.DefaultTimingConfig(),
	}
	if err := configuration.Validate(); err != nil {
		t.Fatal(err)
	}
	return configuration
}

func admissionGateForTupleTest(t *testing.T, fixture workerTestFixture) *admission.Gate {
	t.Helper()
	gate := admission.NewGate()
	if err := gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	return gate
}
