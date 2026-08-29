package swim

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/config"
	"github.com/aaditya/cs425mp3/internal/random"
	"github.com/aaditya/cs425mp3/internal/transport"
	"github.com/aaditya/cs425mp3/internal/wire"
)

const testClusterID = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"

func TestServiceReadyRequiresSnapshotListenerAndSnapshotUsesEventLoop(t *testing.T) {
	configuration := serviceTestConfig(t, 1)
	network := transport.NewMemoryNetwork()
	datagram := serviceMemoryDatagram(t, network, configuration)
	store := newServiceStore(1)
	service, err := NewService(ServiceOptions{
		Config:        configuration,
		Authenticator: wire.NewHMACAuthenticator(testServiceKey()),
		Clock:         clock.NewManual(time.Unix(1000, 0)),
		Random:        random.NewLockedSource(1),
		Store:         store,
		Datagram:      datagram,
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-service.Ready():
		t.Fatal("service reported ready during construction")
	default:
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- service.Run(ctx) }()
	waitServiceReady(t, service)

	snapshot, err := service.Snapshot(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	want := Member{NodeID: 1, Host: configuration.AdvertiseHost, BasePort: configuration.BasePort, Incarnation: 2, Status: Alive}
	if len(snapshot) != 1 || snapshot[0] != want {
		t.Fatalf("snapshot = %#v, want %#v", snapshot, []Member{want})
	}
	snapshot[0].Status = Dead
	fresh, err := service.Snapshot(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if fresh[0] != want {
		t.Fatalf("Snapshot exposed mutable state: %#v", fresh)
	}

	cancel()
	if err := waitServiceResult(t, result); err != nil {
		t.Fatalf("Run cancellation error = %v", err)
	}
}

func TestServiceDoesNotReportReadyWhenSnapshotBindFails(t *testing.T) {
	configuration := serviceTestConfig(t, 1)
	snapshotEndpoint, err := configuration.BindEndpoint(config.ServiceSWIMSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	occupied, err := net.Listen("tcp", snapshotEndpoint.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = occupied.Close() })

	network := transport.NewMemoryNetwork()
	service, err := NewService(ServiceOptions{
		Config:        configuration,
		Authenticator: wire.NewHMACAuthenticator(testServiceKey()),
		Clock:         clock.NewManual(time.Unix(1000, 0)),
		Random:        random.NewLockedSource(2),
		Store:         newServiceStore(1),
		Datagram:      serviceMemoryDatagram(t, network, configuration),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = service.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "listen") {
		t.Fatalf("Run bind error = %v, want listener failure", err)
	}
	select {
	case <-service.Ready():
		t.Fatal("service reported ready after listener failure")
	default:
	}
}

func TestNewServiceRejectsInjectedDatagramWithoutSourceSelection(t *testing.T) {
	configuration := serviceTestConfig(t, 1)
	_, err := NewService(ServiceOptions{
		Config:        configuration,
		Authenticator: wire.NewHMACAuthenticator(testServiceKey()),
		Clock:         clock.NewManual(time.Unix(1000, 0)),
		Random:        random.NewLockedSource(302),
		Store:         newServiceStore(1),
		Datagram:      datagramWithoutSourceSelection{},
	})
	if err == nil || !errors.Is(err, ErrInvalidServiceOptions) || !strings.Contains(err.Error(), "source selection") {
		t.Fatalf("NewService error = %v, want invalid source-selecting datagram", err)
	}
}

type datagramWithoutSourceSelection struct{}

func (datagramWithoutSourceSelection) Send(context.Context, config.Endpoint, []byte) error {
	return nil
}

func (datagramWithoutSourceSelection) Receive(context.Context) (transport.Packet, error) {
	return transport.Packet{}, transport.ErrDatagramClosed
}

func (datagramWithoutSourceSelection) Close() error { return nil }

func TestServiceDropsInvalidAuthenticatedDatagramBoundaries(t *testing.T) {
	now := time.Unix(2000, 0)
	manualClock := clock.NewManual(now)
	configuration := serviceTestConfig(t, 1)
	network := transport.NewMemoryNetwork()
	serviceDatagram := serviceMemoryDatagram(t, network, configuration)
	attackerAddress := config.Endpoint{Host: "attacker", Port: 9000}
	attacker, err := network.Endpoint(attackerAddress)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = attacker.Close() })
	authenticator := wire.NewHMACAuthenticator(testServiceKey())
	service, err := NewService(ServiceOptions{
		Config:        configuration,
		Authenticator: authenticator,
		Clock:         manualClock,
		Random:        random.NewLockedSource(3),
		Store:         newServiceStore(1),
		Datagram:      serviceDatagram,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- service.Run(ctx) }()
	waitServiceReady(t, service)
	destination, err := configuration.AdvertiseEndpoint(config.ServiceSWIMPing)
	if err != nil {
		t.Fatal(err)
	}
	clusterID := decodedTestClusterID(t, testClusterID)
	wrongCluster := clusterID
	wrongCluster[0] ^= 0xff
	validPing, err := wire.EncodeGob(PingMessage{Ping: Ping{OriginID: 1, Sequence: 1}})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name      string
		auth      wire.Authenticator
		clusterID [16]byte
		senderID  uint16
		requestID byte
		message   wire.MessageType
		payload   []byte
	}{
		{name: "wrong HMAC", auth: wire.NewHMACAuthenticator([]byte("wrong-wrong-wrong-wrong-wrong-key")), clusterID: clusterID, senderID: 1, requestID: 1, message: wire.MessageSWIMPing, payload: validPing},
		{name: "wrong cluster", auth: authenticator, clusterID: wrongCluster, senderID: 1, requestID: 2, message: wire.MessageSWIMPing, payload: validPing},
		{name: "unknown type", auth: authenticator, clusterID: clusterID, senderID: 1, requestID: 3, message: wire.MessageType(999), payload: validPing},
		{name: "invalid gob", auth: authenticator, clusterID: clusterID, senderID: 1, requestID: 4, message: wire.MessageSWIMGossip, payload: []byte("not-gob")},
		{name: "unknown sender", auth: authenticator, clusterID: clusterID, senderID: 77, requestID: 5, message: wire.MessageSWIMGossip, payload: mustEncodeGob(t, GossipMessage{Updates: []Update{{Member: Member{NodeID: 77, Host: "unknown", BasePort: 9000, Incarnation: 1, Status: Alive}, ReporterID: 77}}})},
	} {
		t.Run(test.name, func(t *testing.T) {
			frame := encodeServiceTestFrame(t, test.auth, test.clusterID, test.senderID, test.requestID, now, test.message, test.payload)
			if err := attacker.Send(context.Background(), destination, frame); err != nil {
				t.Fatal(err)
			}
		})
	}

	replayID := byte(6)
	first := encodeServiceTestFrame(t, authenticator, clusterID, 1, replayID, now, wire.MessageSWIMPing, validPing)
	replayedMutation := encodeServiceTestFrame(t, authenticator, clusterID, 1, replayID, now, wire.MessageSWIMGossip, mustEncodeGob(t, GossipMessage{Updates: []Update{{Member: Member{NodeID: 88, Host: "replay", BasePort: 8800, Incarnation: 1, Status: Alive}, ReporterID: 1}}}))
	if err := attacker.Send(context.Background(), destination, first); err != nil {
		t.Fatal(err)
	}
	if err := attacker.Send(context.Background(), destination, replayedMutation); err != nil {
		t.Fatal(err)
	}

	peer := Member{NodeID: 2, Host: "127.0.0.2", BasePort: 12000, Incarnation: 1, Status: Alive}
	barrierFrame := encodeServiceTestFrame(t, authenticator, clusterID, 1, 7, now, wire.MessageSWIMGossip, mustEncodeGob(t, GossipMessage{Updates: []Update{{Member: peer, ReporterID: 1}}}))
	if err := serviceDatagram.SendFrom(context.Background(), destination, destination, barrierFrame); err != nil {
		t.Fatal(err)
	}
	if got := network.Advance(); got != 8 {
		t.Fatalf("Advance delivered = %d, want eight test datagrams", got)
	}

	snapshot := waitForSnapshot(t, service, func(members []Member) bool {
		return len(members) == 2 && members[1] == peer
	})
	if len(snapshot) != 2 || snapshot[0].NodeID != 1 || snapshot[1] != peer {
		t.Fatalf("snapshot after invalid boundaries = %#v, want only self and barrier peer", snapshot)
	}
	select {
	case err := <-result:
		t.Fatalf("service stopped after invalid datagram: %v", err)
	default:
	}

	cancel()
	if err := waitServiceResult(t, result); err != nil {
		t.Fatalf("Run cancellation error = %v", err)
	}
}

func TestServiceDatagramBindsSenderIdentityToTypedSourceEndpoint(t *testing.T) {
	now := time.Unix(2050, 0)
	configuration := serviceTestConfig(t, 1)
	authenticator := wire.NewHMACAuthenticator(testServiceKey())
	service, err := NewService(ServiceOptions{
		Config:        configuration,
		Authenticator: authenticator,
		Clock:         clock.NewManual(now),
		Random:        random.NewLockedSource(300),
		Store:         newServiceStore(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	self := Member{NodeID: 1, Host: configuration.AdvertiseHost, BasePort: configuration.BasePort, Incarnation: 2, Status: Alive}
	seed := Member{NodeID: 2, Host: "127.0.0.2", BasePort: 12000, Incarnation: 3, Status: Alive}
	peer := Member{NodeID: 3, Host: "127.0.0.3", BasePort: 13000, Incarnation: 4, Status: Alive}
	service.admitted.Store(true)
	service.active.Store(map[uint16]Member{self.NodeID: self, seed.NodeID: seed, peer.NodeID: peer})
	clusterID := decodedTestClusterID(t, testClusterID)
	attacker := config.Endpoint{Host: "127.0.0.9", Port: 65000}

	for index, member := range []Member{seed, peer, self} {
		payload := mustEncodeGob(t, GossipMessage{Updates: []Update{{
			Member:     Member{NodeID: uint16(50 + index), Host: "forged.local", BasePort: uint16(15000 + index*10), Incarnation: 1, Status: Alive},
			ReporterID: member.NodeID,
		}}})
		frame := encodeServiceTestFrame(t, authenticator, clusterID, member.NodeID, byte(70+index), now, wire.MessageSWIMGossip, payload)
		if event, ok := service.decodeDatagram(transport.Packet{From: attacker, Data: frame}); ok {
			t.Fatalf("forged sender %d from %s accepted as %#v", member.NodeID, attacker, event)
		}
	}

	peerPing, _ := (config.NodeConfig{AdvertiseHost: peer.Host, BasePort: peer.BasePort}).AdvertiseEndpoint(config.ServiceSWIMPing)
	peerACK, _ := (config.NodeConfig{AdvertiseHost: peer.Host, BasePort: peer.BasePort}).AdvertiseEndpoint(config.ServiceSWIMACK)
	pingFrame := encodeServiceTestFrame(t, authenticator, clusterID, peer.NodeID, 80, now, wire.MessageSWIMPing, mustEncodeGob(t, PingMessage{Ping: Ping{OriginID: peer.NodeID, Sequence: 1}}))
	if event, ok := service.decodeDatagram(transport.Packet{From: peerACK, Data: pingFrame}); ok {
		t.Fatalf("Ping from ACK endpoint accepted as %#v", event)
	}
	if _, ok := service.decodeDatagram(transport.Packet{From: peerPing, Data: pingFrame}); !ok {
		t.Fatal("Ping from advertised ping endpoint was rejected")
	}

	ackFrame := encodeServiceTestFrame(t, authenticator, clusterID, peer.NodeID, 81, now, wire.MessageSWIMAck, mustEncodeGob(t, AckMessage{Ack: Ack{OriginID: self.NodeID, Sequence: 1}}))
	if event, ok := service.decodeDatagram(transport.Packet{From: peerPing, Data: ackFrame}); ok {
		t.Fatalf("ACK from ping endpoint accepted as %#v", event)
	}
	if _, ok := service.decodeDatagram(transport.Packet{From: peerACK, Data: ackFrame}); !ok {
		t.Fatal("ACK from advertised ACK endpoint was rejected")
	}
}

func TestServiceInvalidDatagramsCannotPoisonReplayCapacity(t *testing.T) {
	now := time.Unix(2060, 0)
	service, peer, source, authenticator := newDatagramDecoderService(t, now, 1)
	clusterID := decodedTestClusterID(t, testClusterID)

	for requestByte := byte(1); requestByte <= 8; requestByte++ {
		frame := encodeServiceTestFrame(t, authenticator, clusterID, peer.NodeID, requestByte, now, wire.MessageSWIMGossip, []byte("not-gob"))
		if event, ok := service.decodeDatagram(transport.Packet{From: source, Data: frame}); ok {
			t.Fatalf("invalid gob request %d accepted as %#v", requestByte, event)
		}
	}
	invalidPing := encodeServiceTestFrame(t, authenticator, clusterID, peer.NodeID, 9, now, wire.MessageSWIMPing, mustEncodeGob(t, PingMessage{Ping: Ping{OriginID: peer.NodeID}}))
	if event, ok := service.decodeDatagram(transport.Packet{From: source, Data: invalidPing}); ok {
		t.Fatalf("zero-sequence Ping accepted as %#v", event)
	}

	legitimate := encodeServiceTestFrame(t, authenticator, clusterID, peer.NodeID, 10, now, wire.MessageSWIMPing, mustEncodeGob(t, PingMessage{Ping: Ping{OriginID: peer.NodeID, Sequence: 1}}))
	if _, ok := service.decodeDatagram(transport.Packet{From: source, Data: legitimate}); !ok {
		t.Fatal("invalid traffic exhausted replay capacity before legitimate frame")
	}
	if event, ok := service.decodeDatagram(transport.Packet{From: source, Data: legitimate}); ok {
		t.Fatalf("duplicate legitimate frame accepted as %#v", event)
	}
}

func TestServiceRejectsZeroIncarnationBeforeReplayAcceptance(t *testing.T) {
	now := time.Unix(2070, 0)
	service, peer, source, authenticator := newDatagramDecoderService(t, now, 1)
	clusterID := decodedTestClusterID(t, testClusterID)
	requestByte := byte(20)
	poisoned := Member{NodeID: 50, Host: "zero.local", BasePort: 15000, Incarnation: 0, Status: Alive}
	zeroFrame := encodeServiceTestFrame(t, authenticator, clusterID, peer.NodeID, requestByte, now, wire.MessageSWIMGossip, mustEncodeGob(t, GossipMessage{Updates: []Update{{Member: poisoned, ReporterID: peer.NodeID}}}))
	if event, ok := service.decodeDatagram(transport.Packet{From: source, Data: zeroFrame}); ok {
		t.Fatalf("zero-incarnation update accepted as %#v", event)
	}

	valid := poisoned
	valid.Incarnation = 1
	validFrame := encodeServiceTestFrame(t, authenticator, clusterID, peer.NodeID, requestByte, now, wire.MessageSWIMGossip, mustEncodeGob(t, GossipMessage{Updates: []Update{{Member: valid, ReporterID: peer.NodeID}}}))
	if _, ok := service.decodeDatagram(transport.Packet{From: source, Data: validFrame}); !ok {
		t.Fatal("zero-incarnation update consumed the legitimate frame's replay ID")
	}
}

func TestServiceStateDependentACKsCannotPoisonReplayCapacity(t *testing.T) {
	now := time.Unix(2250, 0)
	for _, test := range []struct {
		name        string
		messageType wire.MessageType
		message     func(sequence uint64, target Member) any
		install     func(engine *Engine, sender, target Member, sequence uint64)
	}{
		{
			name:        "ACK",
			messageType: wire.MessageSWIMAck,
			message: func(sequence uint64, _ Member) any {
				return AckMessage{Ack: Ack{OriginID: 1, Sequence: sequence}}
			},
			install: func(engine *Engine, sender, _ Member, sequence uint64) {
				engine.activeProbes[sequence] = &activeProbe{target: sender, phase: probeDirect}
			},
		},
		{
			name:        "IndirectACK",
			messageType: wire.MessageSWIMIndirectAck,
			message: func(sequence uint64, target Member) any {
				return IndirectAckMessage{IndirectAck: IndirectAck{OriginID: 1, Target: target, Sequence: sequence}}
			},
			install: func(engine *Engine, sender, target Member, sequence uint64) {
				engine.activeProbes[sequence] = &activeProbe{target: target, phase: probeIndirect, relays: map[uint16]Member{sender.NodeID: sender}}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			configuration := serviceTestConfig(t, 1)
			manualClock := clock.NewManual(now)
			authenticator := wire.NewHMACAuthenticator(testServiceKey())
			service, err := NewService(ServiceOptions{
				Config:        configuration,
				Authenticator: authenticator,
				Clock:         manualClock,
				Random:        random.NewLockedSource(47),
				Store:         newServiceStore(1),
			})
			if err != nil {
				t.Fatal(err)
			}
			service.replay = wire.NewReplayGuard(manualClock, time.Minute, time.Minute, 1)
			service.admitted.Store(true)

			self := Member{NodeID: 1, Host: configuration.AdvertiseHost, BasePort: configuration.BasePort, Incarnation: 2, Status: Alive}
			sender := Member{NodeID: 2, Host: "127.0.0.2", BasePort: 12000, Incarnation: 1, Status: Alive}
			target := sender
			if test.messageType == wire.MessageSWIMIndirectAck {
				target = Member{NodeID: 3, Host: "127.0.0.3", BasePort: 13000, Incarnation: 1, Status: Alive}
			}
			dissemination := NewDisseminator(serviceDisseminationMax, serviceRetransmitFactor)
			engine, err := NewEngine(EngineConfig{
				SelfID:               1,
				ProbeInterval:        time.Second,
				DirectProbeTimeout:   300 * time.Millisecond,
				IndirectProbeTimeout: 200 * time.Millisecond,
				IndirectChecks:       3,
				SuspicionMultiplier:  5,
			}, NewTable(), dissemination, random.NewLockedSource(48))
			if err != nil {
				t.Fatal(err)
			}
			for _, member := range []Member{self, sender, target} {
				engine.table.Merge(Update{Member: member, ReporterID: self.NodeID})
			}
			service.active.Store(map[uint16]Member{self.NodeID: self, sender.NodeID: sender, target.NodeID: target})
			loop := &serviceLoop{service: service, engine: engine, dissemination: dissemination, admitted: true, runContext: context.Background()}
			source, _ := (config.NodeConfig{AdvertiseHost: sender.Host, BasePort: sender.BasePort}).AdvertiseEndpoint(config.ServiceSWIMACK)

			const validSequence = uint64(77)
			test.install(engine, sender, target, validSequence)
			for index := uint64(0); index < 32; index++ {
				frame := encodeSimulationDatagram(t, authenticator, now, sender.NodeID, 100+index, test.messageType, test.message(1_000+index, target))
				event, ok := service.decodeDatagram(transport.Packet{From: source, Data: frame})
				if !ok {
					t.Fatalf("uncorrelated %s %d was rejected before owner validation", test.name, index)
				}
				if err := loop.handleDatagram(event); err != nil {
					t.Fatal(err)
				}
			}
			if _, exists := engine.activeProbes[validSequence]; !exists {
				t.Fatalf("uncorrelated %s canceled the valid probe", test.name)
			}

			validFrame := encodeSimulationDatagram(t, authenticator, now, sender.NodeID, 100, test.messageType, test.message(validSequence, target))
			validEvent, ok := service.decodeDatagram(transport.Packet{From: source, Data: validFrame})
			if !ok {
				t.Fatalf("valid correlated %s was poisoned out of replay capacity", test.name)
			}
			if err := loop.handleDatagram(validEvent); err != nil {
				t.Fatal(err)
			}
			if _, exists := engine.activeProbes[validSequence]; exists {
				t.Fatalf("valid correlated %s did not cancel the exact probe", test.name)
			}

			test.install(engine, sender, target, validSequence)
			duplicateEvent, ok := service.decodeDatagram(transport.Packet{From: source, Data: validFrame})
			if !ok {
				t.Fatalf("duplicate %s did not reach owner replay validation", test.name)
			}
			if err := loop.handleDatagram(duplicateEvent); err != nil {
				t.Fatal(err)
			}
			if _, exists := engine.activeProbes[validSequence]; !exists {
				t.Fatalf("duplicate valid %s mutated probe state", test.name)
			}
		})
	}
}

func newDatagramDecoderService(t *testing.T, now time.Time, replayEntries int) (*Service, Member, config.Endpoint, wire.Authenticator) {
	t.Helper()
	configuration := serviceTestConfig(t, 1)
	authenticator := wire.NewHMACAuthenticator(testServiceKey())
	service, err := NewService(ServiceOptions{
		Config:        configuration,
		Authenticator: authenticator,
		Clock:         clock.NewManual(now),
		Random:        random.NewLockedSource(301),
		Store:         newServiceStore(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	self := Member{NodeID: 1, Host: configuration.AdvertiseHost, BasePort: configuration.BasePort, Incarnation: 2, Status: Alive}
	peer := Member{NodeID: 2, Host: "127.0.0.2", BasePort: 12000, Incarnation: 3, Status: Alive}
	service.admitted.Store(true)
	service.active.Store(map[uint16]Member{self.NodeID: self, peer.NodeID: peer})
	service.replay = wire.NewReplayGuard(service.options.Clock, time.Duration(configuration.Timing.ReplayWindow), serviceFutureSkew, replayEntries)
	source, err := (config.NodeConfig{AdvertiseHost: peer.Host, BasePort: peer.BasePort}).AdvertiseEndpoint(config.ServiceSWIMPing)
	if err != nil {
		t.Fatal(err)
	}
	return service, peer, source, authenticator
}

func TestServicePersistsSelfRefutationBeforePublishingOrSending(t *testing.T) {
	store := newBarrierServiceStore(1, 3, nil)
	harness := startPersistenceService(t, store)
	ctx, cancelSubscription := context.WithCancel(context.Background())
	defer cancelSubscription()
	events, err := harness.service.Subscribe(ctx, 4)
	if err != nil {
		t.Fatal(err)
	}
	baselineSends := harness.datagram.sendCount.Load()
	harness.datagram.armed.Store(true)
	harness.sendSelfSuspicion(t, 2)

	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("self-refutation persistence did not start")
	}
	select {
	case event := <-events:
		t.Fatalf("event published before persistence completed: %#v", event)
	default:
	}
	if got := harness.datagram.sendCount.Load(); got != baselineSends {
		t.Fatalf("datagram sends before persistence = %d, want %d", got, baselineSends)
	}
	close(store.release)

	select {
	case event := <-events:
		if event.Current.NodeID != 1 || event.Current.Incarnation != 3 || event.Current.Status != Alive {
			t.Fatalf("refutation event = %#v", event)
		}
		if !store.hasStored(3) {
			t.Fatal("refutation event arrived before incarnation 3 was durable")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for persisted refutation event")
	}
	if harness.datagram.violation.Load() {
		t.Fatal("service sent a datagram before persistence completed")
	}
	harness.stop(t)
}

func TestServicePersistenceFailureAbortsEffectsAndFailsRun(t *testing.T) {
	persistError := errors.New("durability barrier failed")
	store := newBarrierServiceStore(1, 3, persistError)
	close(store.release)
	harness := startPersistenceService(t, store)
	events, err := harness.service.Subscribe(context.Background(), 4)
	if err != nil {
		t.Fatal(err)
	}
	baselineSends := harness.datagram.sendCount.Load()
	harness.datagram.armed.Store(true)
	harness.sendSelfSuspicion(t, 2)

	err = waitServiceResult(t, harness.result)
	if !errors.Is(err, persistError) {
		t.Fatalf("Run error = %v, want persistence failure", err)
	}
	if got := harness.datagram.sendCount.Load(); got != baselineSends {
		t.Fatalf("datagram sends after persistence failure = %d, want %d", got, baselineSends)
	}
	if event, open := <-events; open {
		t.Fatalf("event published after persistence failure: %#v", event)
	}
}

func TestServiceCanceledSnapshotDoesNotResumeSlowSubscriber(t *testing.T) {
	store := newBarrierServiceStore(1, 3, nil)
	harness := startPersistenceService(t, store)
	subscriptionContext, cancelSubscription := context.WithCancel(context.Background())
	defer cancelSubscription()
	events, err := harness.service.Subscribe(subscriptionContext, 1)
	if err != nil {
		t.Fatal(err)
	}

	third := Member{NodeID: 3, Host: "127.0.0.3", BasePort: 13000, Incarnation: 1, Status: Alive}
	fourth := Member{NodeID: 4, Host: "127.0.0.4", BasePort: 14000, Incarnation: 1, Status: Alive}
	harness.enqueuePeerGossip([]Update{{Member: third, ReporterID: 2}, {Member: fourth, ReporterID: 2}})
	if _, err := harness.service.requestTCPSnapshot(testContext(t)); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-events:
		if event.Cause != EventResyncRequired {
			t.Fatalf("overflow event = %#v, want resync marker", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for resync marker")
	}

	self := Member{NodeID: 1, Host: harness.configuration.AdvertiseHost, BasePort: harness.configuration.BasePort, Incarnation: 2, Status: Suspect}
	harness.enqueuePeerGossip([]Update{{Member: self, ReporterID: 2}})
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("self-refutation persistence did not block")
	}

	snapshotContext, cancelSnapshot := context.WithCancel(context.Background())
	snapshotResult := make(chan error, 1)
	go func() {
		_, err := harness.service.Snapshot(snapshotContext)
		snapshotResult <- err
	}()
	waitServiceEventQueue(t, harness.service, 1)
	cancelSnapshot()
	if err := <-snapshotResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Snapshot error = %v, want context.Canceled", err)
	}
	close(store.release)
	if _, err := harness.service.requestTCPSnapshot(testContext(t)); err != nil {
		t.Fatal(err)
	}

	fifth := Member{NodeID: 5, Host: "127.0.0.5", BasePort: 15000, Incarnation: 1, Status: Alive}
	harness.enqueuePeerGossip([]Update{{Member: fifth, ReporterID: 2}})
	if _, err := harness.service.requestTCPSnapshot(testContext(t)); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-events:
		t.Fatalf("canceled Snapshot resumed slow subscriber: %#v", event)
	default:
	}

	if _, err := harness.service.Snapshot(testContext(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.service.requestTCPSnapshot(testContext(t)); err != nil {
		t.Fatal(err)
	}
	sixth := Member{NodeID: 6, Host: "127.0.0.6", BasePort: 16000, Incarnation: 1, Status: Alive}
	harness.enqueuePeerGossip([]Update{{Member: sixth, ReporterID: 2}})
	if _, err := harness.service.requestTCPSnapshot(testContext(t)); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-events:
		if event.Current != sixth {
			t.Fatalf("post-snapshot delta = %#v, want %#v", event, sixth)
		}
	case <-time.After(time.Second):
		t.Fatal("successful Snapshot did not resume delta delivery")
	}
	harness.stop(t)
}

func TestServiceSnapshotAcknowledgmentRequiresCurrentMembershipRevision(t *testing.T) {
	store := newBarrierServiceStore(1, 99, nil)
	harness := startPersistenceService(t, store)
	subscriptionContext, cancelSubscription := context.WithCancel(context.Background())
	defer cancelSubscription()
	events, err := harness.service.Subscribe(subscriptionContext, 1)
	if err != nil {
		t.Fatal(err)
	}

	third := Member{NodeID: 3, Host: "127.0.0.3", BasePort: 13000, Incarnation: 1, Status: Alive}
	fourth := Member{NodeID: 4, Host: "127.0.0.4", BasePort: 14000, Incarnation: 1, Status: Alive}
	harness.enqueuePeerGossip([]Update{{Member: third, ReporterID: 2}, {Member: fourth, ReporterID: 2}})
	if _, err := harness.service.requestTCPSnapshot(testContext(t)); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-events:
		if event.Cause != EventResyncRequired {
			t.Fatalf("overflow event = %#v, want resync marker", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for resync marker")
	}

	response := make(chan snapshotResult, 1)
	harness.service.events <- snapshotServiceEvent{response: response}
	captured := <-response
	if captured.revision == 0 {
		t.Fatal("captured snapshot has zero membership revision")
	}
	fifth := Member{NodeID: 5, Host: "127.0.0.5", BasePort: 15000, Incarnation: 1, Status: Alive}
	harness.enqueuePeerGossip([]Update{{Member: fifth, ReporterID: 2}})
	harness.service.events <- snapshotDeliveredServiceEvent{revision: captured.revision}
	if _, err := harness.service.requestTCPSnapshot(testContext(t)); err != nil {
		t.Fatal(err)
	}

	sixth := Member{NodeID: 6, Host: "127.0.0.6", BasePort: 16000, Incarnation: 1, Status: Alive}
	harness.enqueuePeerGossip([]Update{{Member: sixth, ReporterID: 2}})
	if _, err := harness.service.requestTCPSnapshot(testContext(t)); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-events:
		t.Fatalf("stale snapshot acknowledgment resumed subscriber: %#v", event)
	default:
	}

	if _, err := harness.service.Snapshot(testContext(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.service.requestTCPSnapshot(testContext(t)); err != nil {
		t.Fatal(err)
	}
	seventh := Member{NodeID: 7, Host: "127.0.0.7", BasePort: 17000, Incarnation: 1, Status: Alive}
	harness.enqueuePeerGossip([]Update{{Member: seventh, ReporterID: 2}})
	if _, err := harness.service.requestTCPSnapshot(testContext(t)); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-events:
		if event.Current != seventh {
			t.Fatalf("fresh snapshot post-delta = %#v, want %#v", event, seventh)
		}
	case <-time.After(time.Second):
		t.Fatal("fresh current-revision snapshot did not resume subscriber")
	}
	harness.stop(t)
}

func TestServiceCanceledSubscribeRequestsDoNotLeakOwnerState(t *testing.T) {
	store := newBarrierServiceStore(1, 3, nil)
	harness := startPersistenceService(t, store)
	self := Member{NodeID: 1, Host: harness.configuration.AdvertiseHost, BasePort: harness.configuration.BasePort, Incarnation: 2, Status: Suspect}
	harness.enqueuePeerGossip([]Update{{Member: self, ReporterID: 2}})
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("self-refutation persistence did not block")
	}

	const requests = 512
	cancels := make([]context.CancelFunc, 0, requests)
	results := make(chan error, requests)
	for range requests {
		requestContext, cancel := context.WithCancel(context.Background())
		cancels = append(cancels, cancel)
		go func() {
			_, err := harness.service.Subscribe(requestContext, 1)
			results <- err
		}()
	}
	waitServiceEventQueue(t, harness.service, requests)
	for _, cancel := range cancels {
		cancel()
	}
	for range requests {
		if err := <-results; !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled Subscribe error = %v, want context.Canceled", err)
		}
	}
	close(store.release)
	if got := serviceSubscriptionCount(t, harness.service); got != 0 {
		t.Fatalf("subscriptions after %d canceled queued requests = %d, want 0", requests, got)
	}
	harness.stop(t)
}

func TestServiceDigestSendRemainsRequiredWithoutSnapshotRepair(t *testing.T) {
	now := time.Unix(3900, 0)
	configuration := serviceTestConfig(t, 1)
	network := transport.NewMemoryNetwork()
	base := serviceMemoryDatagram(t, network, configuration)
	authenticator := wire.NewHMACAuthenticator(testServiceKey())
	recording := newRecordingDatagram(base, authenticator, serviceWireLimits(t))
	dissemination := NewDisseminator(1, serviceRetransmitFactor)
	dissemination.DigestRequired = true
	self := Member{NodeID: 1, Host: configuration.AdvertiseHost, BasePort: configuration.BasePort, Incarnation: 2, Status: Alive}
	peer := Member{NodeID: 2, Host: "127.0.0.2", BasePort: 12000, Incarnation: 1, Status: Alive}
	engine, err := NewEngine(EngineConfig{
		SelfID:               self.NodeID,
		ProbeInterval:        time.Second,
		DirectProbeTimeout:   300 * time.Millisecond,
		IndirectProbeTimeout: 200 * time.Millisecond,
		IndirectChecks:       3,
		SuspicionMultiplier:  5,
	}, NewTable(), dissemination, random.NewLockedSource(44))
	if err != nil {
		t.Fatal(err)
	}
	engine.table.Merge(Update{Member: self, ReporterID: self.NodeID})
	engine.table.Merge(Update{Member: peer, ReporterID: self.NodeID})
	clusterID := decodedTestClusterID(t, testClusterID)
	service := &Service{
		options:   ServiceOptions{Config: configuration, Authenticator: authenticator, Clock: clock.NewManual(now)},
		clusterID: clusterID,
		limits:    serviceWireLimits(t),
	}
	loop := &serviceLoop{
		service:       service,
		engine:        engine,
		dissemination: dissemination,
		activeMembers: []Member{self, peer},
		datagram:      recording,
		requestPrefix: 1,
	}

	for attempt := 0; attempt < 2; attempt++ {
		if err := loop.sendDigestRound(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if !dissemination.DigestRequired {
		t.Fatal("successful UDP sends cleared digest recovery without a TCP snapshot")
	}
	peerPing := config.Endpoint{Host: peer.Host, Port: peer.BasePort}
	if got := recording.count(wire.MessageSWIMDigest, peerPing); got != 2 {
		t.Fatalf("digest fallback sends = %d, want 2 retries", got)
	}
}

func TestServiceFailedSnapshotResponseDoesNotPublishRepairBarrier(t *testing.T) {
	now := time.Unix(3950, 0)
	configuration := serviceTestConfig(t, 1)
	manualClock := clock.NewManual(now)
	service, err := NewService(ServiceOptions{
		Config:        configuration,
		Authenticator: wire.NewHMACAuthenticator(testServiceKey()),
		Clock:         manualClock,
		Random:        random.NewLockedSource(45),
		Store:         newServiceStore(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	peer := Member{NodeID: 2, Host: "127.0.0.2", BasePort: 12000, Incarnation: 1, Status: Alive}
	service.active.Store(map[uint16]Member{peer.NodeID: peer})
	payload := mustEncodeGob(t, SnapshotRequest{})
	frame := tcpServiceTestFrameWithPayload(service.clusterID, peer.NodeID, wire.RequestID{92}, now, wire.MessageSWIMSnapshotRequest, payload)

	serverConnection, clientConnection := net.Pipe()
	if err := clientConnection.Close(); err != nil {
		t.Fatal(err)
	}
	stream := wire.NewTCPFrameStream(serverConnection, service.options.Authenticator, service.limits, time.Second)
	defer stream.Close()
	snapshotRequested := make(chan struct{})
	go func() {
		event := <-service.events
		request, ok := event.(tcpSnapshotServiceEvent)
		if !ok {
			close(snapshotRequested)
			return
		}
		request.response <- snapshotResult{
			members:          []Member{{NodeID: 1, Host: configuration.AdvertiseHost, BasePort: configuration.BasePort, Incarnation: 2, Status: Alive}, peer},
			digestGeneration: 7,
		}
		close(snapshotRequested)
	}()

	service.handleTCPSnapshot(testContext(t), stream, frame)
	<-snapshotRequested
	select {
	case event := <-service.events:
		t.Fatalf("failed snapshot response published repair barrier: %#v", event)
	default:
	}
}

func TestServiceWrittenSnapshotWithoutApplicationAckKeepsRepairPending(t *testing.T) {
	now := time.Unix(3975, 0)
	configuration := serviceTestConfig(t, 1)
	manualClock := clock.NewManual(now)
	authenticator := wire.NewHMACAuthenticator(testServiceKey())
	service, err := NewService(ServiceOptions{
		Config:        configuration,
		Authenticator: authenticator,
		Clock:         manualClock,
		Random:        random.NewLockedSource(46),
		Store:         newServiceStore(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	peer := Member{NodeID: 2, Host: "127.0.0.2", BasePort: 12000, Incarnation: 1, Status: Alive}
	service.active.Store(map[uint16]Member{peer.NodeID: peer})
	frame := tcpServiceTestFrameWithPayload(service.clusterID, peer.NodeID, wire.RequestID{93}, now, wire.MessageSWIMSnapshotRequest, mustEncodeGob(t, SnapshotRequest{}))

	serverConnection, clientConnection := net.Pipe()
	serverStream := wire.NewTCPFrameStream(serverConnection, authenticator, service.limits, time.Second)
	clientStream := wire.NewTCPFrameStream(clientConnection, authenticator, service.limits, time.Second)
	defer serverStream.Close()
	handled := make(chan struct{})
	go func() {
		service.handleTCPSnapshot(context.Background(), serverStream, frame)
		close(handled)
	}()
	snapshotRequested := make(chan struct{})
	go func() {
		event := <-service.events
		request := event.(tcpSnapshotServiceEvent)
		request.response <- snapshotResult{
			members:          []Member{{NodeID: 1, Host: configuration.AdvertiseHost, BasePort: configuration.BasePort, Incarnation: 2, Status: Alive}, peer},
			digestGeneration: 7,
		}
		close(snapshotRequested)
	}()

	response, err := clientStream.ReadFrame(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if response.Header.Message != wire.MessageSWIMSnapshotResponse {
		t.Fatalf("snapshot response type = %d", response.Header.Message)
	}
	select {
	case event := <-service.events:
		t.Fatalf("response write without application ack published repair barrier: %#v", event)
	case <-handled:
		select {
		case event := <-service.events:
			t.Fatalf("response write without application ack published repair barrier: %#v", event)
		default:
			t.Fatal("snapshot handler completed without waiting for application ack")
		}
	case <-time.After(100 * time.Millisecond):
		// The handler remains the stream owner while awaiting application.
	}
	if err := clientStream.Close(); err != nil {
		t.Fatal(err)
	}
	<-snapshotRequested
	select {
	case <-handled:
	case <-time.After(time.Second):
		t.Fatal("snapshot handler did not exit after requester closed without application ack")
	}
}

func TestServiceSnapshotApplicationAckRevalidatesRequesterGeneration(t *testing.T) {
	now := time.Unix(3980, 0)
	configuration := serviceTestConfig(t, 1)
	manualClock := clock.NewManual(now)
	authenticator := wire.NewHMACAuthenticator(testServiceKey())
	service, err := NewService(ServiceOptions{
		Config:        configuration,
		Authenticator: authenticator,
		Clock:         manualClock,
		Random:        random.NewLockedSource(49),
		Store:         newServiceStore(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	requester := Member{NodeID: 2, Host: "127.0.0.2", BasePort: 12000, Incarnation: 1, Status: Alive}
	service.active.Store(map[uint16]Member{requester.NodeID: requester})
	requestID := wire.RequestID{94}
	request := tcpServiceTestFrameWithPayload(service.clusterID, requester.NodeID, requestID, now, wire.MessageSWIMSnapshotRequest, mustEncodeGob(t, SnapshotRequest{}))

	serverConnection, clientConnection := net.Pipe()
	serverStream := wire.NewTCPFrameStream(serverConnection, authenticator, service.limits, time.Second)
	clientStream := wire.NewTCPFrameStream(clientConnection, authenticator, service.limits, time.Second)
	defer serverStream.Close()
	defer clientStream.Close()
	handled := make(chan struct{})
	go func() {
		service.handleTCPSnapshot(context.Background(), serverStream, request)
		close(handled)
	}()
	go func() {
		event := (<-service.events).(tcpSnapshotServiceEvent)
		event.response <- snapshotResult{
			members:          []Member{{NodeID: 1, Host: configuration.AdvertiseHost, BasePort: configuration.BasePort, Incarnation: 2, Status: Alive}, requester},
			digestGeneration: 7,
		}
	}()
	if response, err := clientStream.ReadFrame(testContext(t)); err != nil || response.Header.Message != wire.MessageSWIMSnapshotResponse {
		t.Fatalf("snapshot response = %#v, error = %v", response, err)
	}

	currentRequester := requester
	currentRequester.Incarnation++
	service.active.Store(map[uint16]Member{currentRequester.NodeID: currentRequester})
	applied := tcpServiceTestFrameWithPayload(service.clusterID, requester.NodeID, requestID, now, wire.MessageSWIMSnapshotApplied, mustEncodeGob(t, SnapshotApplied{}))
	if err := clientStream.WriteFrame(testContext(t), applied); err != nil {
		t.Fatal(err)
	}
	select {
	case <-handled:
	case <-time.After(time.Second):
		t.Fatal("snapshot handler did not accept correlated application ack")
	}
	event := (<-service.events).(snapshotServedServiceEvent)
	if event.requester != requester {
		t.Fatalf("captured requester = %#v, want %#v", event.requester, requester)
	}

	table := NewTable()
	table.Merge(Update{Member: currentRequester, ReporterID: currentRequester.NodeID})
	dissemination := NewDisseminator(1, 1)
	dissemination.DigestRequired = true
	dissemination.digestGeneration = 7
	loop := &serviceLoop{engine: &Engine{table: table}, dissemination: dissemination}
	loop.handleSnapshotServed(event)
	if !dissemination.DigestRequired {
		t.Fatal("application ack from stale requester generation cleared digest recovery")
	}
}

func TestServiceJoinUsesSnapshotPersistAnnounceAcceptOrder(t *testing.T) {
	now := time.Unix(4000, 0)
	network := transport.NewMemoryNetwork()
	seedConfig := serviceTestConfig(t, 1)
	seed := startRunningService(t, seedConfig, newServiceStore(1), clock.NewManual(now), network, 11)
	joinConfig := serviceTestConfig(t, 2)
	seedEndpoint, err := seedConfig.AdvertiseEndpoint(config.ServiceSWIMSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	joinConfig.Introducer = seedEndpoint.String()
	if err := joinConfig.Validate(); err != nil {
		t.Fatal(err)
	}
	joinStore := newServiceStore(1)
	joining := startRunningService(t, joinConfig, joinStore, clock.NewManual(now), network, 12)

	seedMembers := waitForSnapshot(t, seed.service, func(members []Member) bool {
		return len(members) == 2 && members[0].NodeID == 1 && members[1].NodeID == 2
	})
	joiningMembers := waitForSnapshot(t, joining.service, func(members []Member) bool {
		return len(members) == 2 && members[0].NodeID == 1 && members[1].NodeID == 2
	})
	if seedMembers[1] != joiningMembers[1] || seedMembers[1].Incarnation != 2 || seedMembers[1].Status != Alive {
		t.Fatalf("admitted member seed=%#v joining=%#v", seedMembers[1], joiningMembers[1])
	}
	if got, _ := joinStore.Load(); got != 2 {
		t.Fatalf("durable joining incarnation = %d, want 2", got)
	}

	joining.stop(t)
	seed.stop(t)
}

func TestServiceRejectsJoinAnnounceBeforeSnapshotWithoutMutation(t *testing.T) {
	now := time.Unix(4100, 0)
	network := transport.NewMemoryNetwork()
	seedConfig := serviceTestConfig(t, 1)
	seed := startRunningService(t, seedConfig, newServiceStore(1), clock.NewManual(now), network, 13)
	endpoint, err := seedConfig.AdvertiseEndpoint(config.ServiceSWIMSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	connection, err := net.Dial("tcp", endpoint.String())
	if err != nil {
		t.Fatal(err)
	}
	stream := wire.NewTCPFrameStream(connection, wire.NewHMACAuthenticator(testServiceKey()), serviceWireLimits(t), time.Second)
	defer stream.Close()
	announce := JoinAnnounce{Member: Member{NodeID: 2, Host: "127.0.0.2", BasePort: 12000, Incarnation: 2, Status: Alive}}
	requestID := wire.RequestID{31}
	if err := stream.WriteFrame(testContext(t), wire.Frame{Header: wire.Header{
		Version:         wire.Version1,
		Message:         wire.MessageSWIMJoinAnnounce,
		ClusterID:       decodedTestClusterID(t, testClusterID),
		SenderID:        2,
		RequestID:       requestID,
		TimestampMillis: now.UnixMilli(),
		Codec:           wire.CodecGob,
	}, Payload: mustEncodeGob(t, announce)}); err != nil {
		t.Fatal(err)
	}
	response, err := stream.ReadFrame(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if response.Header.Message != wire.MessageSWIMError || response.Header.RequestID != requestID {
		t.Fatalf("response header = %#v, want correlated protocol error", response.Header)
	}
	var protocolError ProtocolErrorMessage
	if err := wire.DecodeGob(response.Payload, &protocolError); err != nil {
		t.Fatal(err)
	}
	if protocolError.Code != protocolErrorUnexpectedMessage {
		t.Fatalf("protocol error = %#v, want unexpected-message code", protocolError)
	}
	snapshot, err := seed.service.Snapshot(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot) != 1 || snapshot[0].NodeID != 1 {
		t.Fatalf("announce-first mutated seed snapshot: %#v", snapshot)
	}
	seed.stop(t)
}

func TestServiceJoinPersistenceFailureNeverAnnounces(t *testing.T) {
	now := time.Unix(4200, 0)
	network := transport.NewMemoryNetwork()
	seedConfig := serviceTestConfig(t, 1)
	seed := startRunningService(t, seedConfig, newServiceStore(1), clock.NewManual(now), network, 14)
	joinConfig := serviceTestConfig(t, 2)
	seedEndpoint, _ := seedConfig.AdvertiseEndpoint(config.ServiceSWIMSnapshot)
	joinConfig.Introducer = seedEndpoint.String()
	if err := joinConfig.Validate(); err != nil {
		t.Fatal(err)
	}
	persistError := errors.New("join incarnation unavailable")
	joinStore := newBarrierServiceStore(1, 2, persistError)
	close(joinStore.release)
	joining := startRunningService(t, joinConfig, joinStore, clock.NewManual(now), network, 15)

	if err := waitServiceResult(t, joining.result); !errors.Is(err, persistError) {
		t.Fatalf("joining Run error = %v, want persistence failure", err)
	}
	joining.markStopped()
	select {
	case <-joinStore.started:
	default:
		t.Fatal("joiner never attempted its persistence barrier")
	}
	snapshot, err := seed.service.Snapshot(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot) != 1 || snapshot[0].NodeID != 1 {
		t.Fatalf("failed join appeared at seed: %#v", snapshot)
	}
	seed.stop(t)
}

func TestSnapshotClientRequiresActiveAuthenticatedSender(t *testing.T) {
	now := time.Unix(4300, 0)
	network := transport.NewMemoryNetwork()
	seedConfig := serviceTestConfig(t, 1)
	seed := startRunningService(t, seedConfig, newServiceStore(1), clock.NewManual(now), network, 16)
	endpoint, _ := seedConfig.AdvertiseEndpoint(config.ServiceSWIMSnapshot)
	client, err := NewSnapshotClient(SnapshotClientOptions{
		Config:        seedConfig,
		Authenticator: wire.NewHMACAuthenticator(testServiceKey()),
		Clock:         clock.NewManual(now),
		Random:        random.NewLockedSource(17),
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := client.Snapshot(testContext(t), endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot) != 1 || snapshot[0].NodeID != 1 || snapshot[0].Status != Alive {
		t.Fatalf("snapshot client response = %#v", snapshot)
	}
	snapshot[0].Status = Dead
	fresh, err := client.Snapshot(testContext(t), endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if fresh[0].Status != Alive {
		t.Fatalf("snapshot client exposed shared state: %#v", fresh)
	}

	unknownConfig := serviceTestConfig(t, 77)
	unknownClient, err := NewSnapshotClient(SnapshotClientOptions{
		Config:        unknownConfig,
		Authenticator: wire.NewHMACAuthenticator(testServiceKey()),
		Clock:         clock.NewManual(now),
		Random:        random.NewLockedSource(18),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unknownClient.Snapshot(testContext(t), endpoint); !errors.Is(err, ErrServiceNotAdmitted) {
		t.Fatalf("unknown sender snapshot error = %v, want ErrServiceNotAdmitted", err)
	}
	seed.stop(t)
}

func TestServiceTCPBoundaryFailuresDoNotMutateOrStopService(t *testing.T) {
	now := time.Unix(4400, 0)
	network := transport.NewMemoryNetwork()
	seedConfig := serviceTestConfig(t, 1)
	seed := startRunningService(t, seedConfig, newServiceStore(1), clock.NewManual(now), network, 19)
	endpoint, _ := seedConfig.AdvertiseEndpoint(config.ServiceSWIMSnapshot)
	clusterID := decodedTestClusterID(t, testClusterID)
	authenticator := wire.NewHMACAuthenticator(testServiceKey())

	wrongCluster := clusterID
	wrongCluster[0] ^= 0xff
	for _, test := range []struct {
		name      string
		auth      wire.Authenticator
		clusterID [16]byte
	}{
		{name: "wrong HMAC", auth: wire.NewHMACAuthenticator([]byte("wrong-wrong-wrong-wrong-wrong-key")), clusterID: clusterID},
		{name: "wrong cluster", auth: authenticator, clusterID: wrongCluster},
	} {
		t.Run(test.name, func(t *testing.T) {
			stream := dialServiceTestStream(t, endpoint, test.auth, test.clusterID)
			defer stream.Close()
			frame := tcpServiceTestFrame(t, test.clusterID, 1, 50, now, wire.MessageSWIMSnapshotRequest, SnapshotRequest{})
			if err := stream.WriteFrame(testContext(t), frame); err != nil {
				t.Fatal(err)
			}
			if _, err := stream.ReadFrame(testContext(t)); err == nil {
				t.Fatal("authentication failure received a detailed response")
			}
		})
	}

	invalidGobStream := dialServiceTestStream(t, endpoint, authenticator, clusterID)
	invalidGobID := wire.RequestID{51}
	invalidGobFrame := tcpServiceTestFrameWithPayload(clusterID, 1, invalidGobID, now, wire.MessageSWIMSnapshotRequest, []byte("not-gob"))
	if err := invalidGobStream.WriteFrame(testContext(t), invalidGobFrame); err != nil {
		t.Fatal(err)
	}
	invalidResponse, err := invalidGobStream.ReadFrame(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if got := decodeServiceProtocolError(t, invalidResponse); got.Code != protocolErrorInvalidPayload {
		t.Fatalf("invalid gob protocol error = %#v, want invalid-payload code", got)
	}
	_ = invalidGobStream.Close()

	unknownStream := dialServiceTestStream(t, endpoint, authenticator, clusterID)
	unknownFrame := tcpServiceTestFrame(t, clusterID, 1, 52, now, wire.MessageType(999), SnapshotRequest{})
	if err := unknownStream.WriteFrame(testContext(t), unknownFrame); err != nil {
		t.Fatal(err)
	}
	unknownResponse, err := unknownStream.ReadFrame(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if got := decodeServiceProtocolError(t, unknownResponse); got.Code != protocolErrorUnexpectedMessage {
		t.Fatalf("unknown type protocol error = %#v, want unexpected-message code", got)
	}
	_ = unknownStream.Close()

	replayID := byte(53)
	firstStream := dialServiceTestStream(t, endpoint, authenticator, clusterID)
	replayFrame := tcpServiceTestFrame(t, clusterID, 1, replayID, now, wire.MessageSWIMSnapshotRequest, SnapshotRequest{})
	if err := firstStream.WriteFrame(testContext(t), replayFrame); err != nil {
		t.Fatal(err)
	}
	if response, err := firstStream.ReadFrame(testContext(t)); err != nil || response.Header.Message != wire.MessageSWIMSnapshotResponse {
		t.Fatalf("first request response = %#v, error = %v", response, err)
	}
	_ = firstStream.Close()
	secondStream := dialServiceTestStream(t, endpoint, authenticator, clusterID)
	if err := secondStream.WriteFrame(testContext(t), replayFrame); err != nil {
		t.Fatal(err)
	}
	replayResponse, err := secondStream.ReadFrame(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if got := decodeServiceProtocolError(t, replayResponse); got.Code != protocolErrorReplay {
		t.Fatalf("replay protocol error = %#v, want replay code", got)
	}
	_ = secondStream.Close()

	snapshot, err := seed.service.Snapshot(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot) != 1 || snapshot[0].NodeID != 1 || snapshot[0].Status != Alive {
		t.Fatalf("TCP boundary failures mutated snapshot: %#v", snapshot)
	}
	select {
	case err := <-seed.result:
		t.Fatalf("TCP boundary failure stopped service: %v", err)
	default:
	}
	seed.stop(t)
}

func TestServiceBoundsTCPBodiesAndConnections(t *testing.T) {
	now := time.Unix(4425, 0)
	network := transport.NewMemoryNetwork()
	configuration := serviceTestConfig(t, 1)
	running := startRunningService(t, configuration, newServiceStore(1), clock.NewManual(now), network, 191)
	endpoint, _ := configuration.AdvertiseEndpoint(config.ServiceSWIMSnapshot)

	oversized, err := net.Dial("tcp", endpoint.String())
	if err != nil {
		t.Fatal(err)
	}
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(running.service.limits.MaxFrameSize+1))
	if _, err := oversized.Write(prefix[:]); err != nil {
		t.Fatal(err)
	}
	assertConnectionClosedPromptly(t, oversized, "oversized TCP body")
	waitServiceTCPConnectionCount(t, running.service, 0)

	connections := make([]net.Conn, 0, serviceTCPConnections)
	defer func() {
		for _, connection := range connections {
			_ = connection.Close()
		}
	}()
	for index := 0; index < serviceTCPConnections; index++ {
		connection, err := net.Dial("tcp", endpoint.String())
		if err != nil {
			t.Fatalf("dial held TCP connection %d: %v", index+1, err)
		}
		connections = append(connections, connection)
	}
	waitServiceTCPConnectionCount(t, running.service, serviceTCPConnections)

	overLimit, err := net.Dial("tcp", endpoint.String())
	if err != nil {
		t.Fatal(err)
	}
	assertConnectionClosedPromptly(t, overLimit, "65th TCP connection")
	if got := serviceTCPConnectionCount(running.service); got != serviceTCPConnections {
		t.Fatalf("held TCP connections after rejecting over-limit connection = %d, want %d", got, serviceTCPConnections)
	}

	for _, connection := range connections {
		_ = connection.Close()
	}
	connections = nil
	waitServiceTCPConnectionCount(t, running.service, 0)
	if _, err := running.service.Snapshot(testContext(t)); err != nil {
		t.Fatalf("service stopped after bounded TCP inputs: %v", err)
	}
	running.stop(t)
}

func TestServiceInvalidTCPRequestsCannotPoisonReplayCapacity(t *testing.T) {
	now := time.Unix(4450, 0)
	configuration := serviceTestConfig(t, 1)
	seed := startRunningService(t, configuration, newServiceStore(1), clock.NewManual(now), transport.NewMemoryNetwork(), 190)
	seed.service.replay = wire.NewReplayGuard(seed.service.options.Clock, time.Duration(configuration.Timing.ReplayWindow), serviceFutureSkew, 1)
	endpoint, _ := configuration.AdvertiseEndpoint(config.ServiceSWIMSnapshot)
	clusterID := decodedTestClusterID(t, testClusterID)
	authenticator := wire.NewHMACAuthenticator(testServiceKey())

	invalidGob := tcpServiceTestFrameWithPayload(clusterID, 1, wire.RequestID{60}, now, wire.MessageSWIMSnapshotRequest, []byte("not-gob"))
	invalidStream := dialServiceTestStream(t, endpoint, authenticator, clusterID)
	if err := invalidStream.WriteFrame(testContext(t), invalidGob); err != nil {
		t.Fatal(err)
	}
	if got := decodeServiceProtocolError(t, mustReadServiceFrame(t, invalidStream)); got.Code != protocolErrorInvalidPayload {
		t.Fatalf("invalid gob error = %#v", got)
	}
	_ = invalidStream.Close()

	unknown := tcpServiceTestFrame(t, clusterID, 77, 61, now, wire.MessageSWIMSnapshotRequest, SnapshotRequest{})
	unknownStream := dialServiceTestStream(t, endpoint, authenticator, clusterID)
	if err := unknownStream.WriteFrame(testContext(t), unknown); err != nil {
		t.Fatal(err)
	}
	if got := decodeServiceProtocolError(t, mustReadServiceFrame(t, unknownStream)); got.Code != protocolErrorNotAdmitted {
		t.Fatalf("unknown sender error = %#v, want not-admitted", got)
	}
	_ = unknownStream.Close()

	legitimate := tcpServiceTestFrame(t, clusterID, 1, 62, now, wire.MessageSWIMSnapshotRequest, SnapshotRequest{})
	validStream := dialServiceTestStream(t, endpoint, authenticator, clusterID)
	if err := validStream.WriteFrame(testContext(t), legitimate); err != nil {
		t.Fatal(err)
	}
	response := mustReadServiceFrame(t, validStream)
	if response.Header.Message != wire.MessageSWIMSnapshotResponse {
		t.Fatalf("legitimate response = %d, want snapshot; error=%#v", response.Header.Message, decodeServiceProtocolError(t, response))
	}
	_ = validStream.Close()

	duplicateStream := dialServiceTestStream(t, endpoint, authenticator, clusterID)
	if err := duplicateStream.WriteFrame(testContext(t), legitimate); err != nil {
		t.Fatal(err)
	}
	if got := decodeServiceProtocolError(t, mustReadServiceFrame(t, duplicateStream)); got.Code != protocolErrorReplay {
		t.Fatalf("duplicate valid request error = %#v, want replay", got)
	}
	_ = duplicateStream.Close()
	seed.stop(t)
}

func TestServiceDigestTriggersAuthenticatedSnapshotResync(t *testing.T) {
	now := time.Unix(4500, 0)
	network := transport.NewMemoryNetwork()
	seedConfig := serviceTestConfig(t, 1)
	seed := startRunningService(t, seedConfig, newServiceStore(1), clock.NewManual(now), network, 20)
	joinConfig := serviceTestConfig(t, 2)
	seedSnapshot, _ := seedConfig.AdvertiseEndpoint(config.ServiceSWIMSnapshot)
	joinConfig.Introducer = seedSnapshot.String()
	if err := joinConfig.Validate(); err != nil {
		t.Fatal(err)
	}
	joining := startRunningService(t, joinConfig, newServiceStore(1), clock.NewManual(now), network, 21)
	waitForSnapshot(t, joining.service, func(members []Member) bool { return len(members) == 2 })

	seedPing, _ := seedConfig.AdvertiseEndpoint(config.ServiceSWIMPing)
	joinPing, _ := joinConfig.AdvertiseEndpoint(config.ServiceSWIMPing)
	network.Drop(seedPing, joinPing)
	authenticator := wire.NewHMACAuthenticator(testServiceKey())
	clusterID := decodedTestClusterID(t, testClusterID)
	missing := Member{NodeID: 3, Host: "127.0.0.3", BasePort: 13000, Incarnation: 1, Status: Alive}
	zero := missing
	zero.Incarnation = 0
	zeroFrame := encodeServiceTestFrame(t, authenticator, clusterID, 1, 60, now, wire.MessageSWIMGossip, mustEncodeGob(t, GossipMessage{Updates: []Update{{Member: zero, ReporterID: 1}}}))
	updateFrame := encodeServiceTestFrame(t, authenticator, clusterID, 1, 60, now, wire.MessageSWIMGossip, mustEncodeGob(t, GossipMessage{Updates: []Update{{Member: missing, ReporterID: 1}}}))
	seedDatagram := seed.service.options.Datagram.(transport.SourceDatagram)
	if err := seedDatagram.SendFrom(context.Background(), seedPing, seedPing, zeroFrame); err != nil {
		t.Fatal(err)
	}
	network.Advance()
	if snapshot, err := seed.service.Snapshot(testContext(t)); err != nil || len(snapshot) != 2 {
		t.Fatalf("zero-incarnation poison snapshot = %#v, error = %v", snapshot, err)
	}
	if err := seedDatagram.SendFrom(context.Background(), seedPing, seedPing, updateFrame); err != nil {
		t.Fatal(err)
	}
	if got := network.Advance(); got < 1 {
		t.Fatalf("Advance delivered update packets = %d, want the injector packet", got)
	}
	waitForSnapshot(t, seed.service, func(members []Member) bool { return len(members) == 3 && members[2] == missing })
	joiningSnapshot, err := joining.service.Snapshot(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(joiningSnapshot) != 2 {
		t.Fatalf("dropped UDP update reached joining node: %#v", joiningSnapshot)
	}

	network.Heal(seedPing, joinPing)
	digestFrame := encodeServiceTestFrame(t, authenticator, clusterID, 1, 61, now, wire.MessageSWIMDigest, mustEncodeGob(t, DigestMessage{}))
	if err := seedDatagram.SendFrom(context.Background(), seedPing, joinPing, digestFrame); err != nil {
		t.Fatal(err)
	}
	if got := network.Advance(); got < 1 {
		t.Fatalf("Advance delivered digest packets = %d, want the injector packet", got)
	}
	resynchronized := waitForSnapshot(t, joining.service, func(members []Member) bool { return len(members) == 3 && members[2] == missing })
	if resynchronized[2] != missing {
		t.Fatalf("resynchronized snapshot = %#v", resynchronized)
	}
	joining.stop(t)
	seed.stop(t)
}

type runningService struct {
	service  *Service
	cancel   context.CancelFunc
	result   chan error
	stopOnce sync.Once
}

func startRunningService(t *testing.T, configuration config.NodeConfig, store IncarnationStore, serviceClock clock.Clock, network *transport.MemoryNetwork, seed int64) *runningService {
	t.Helper()
	return startRunningServiceWithDatagram(t, configuration, store, serviceClock, serviceMemoryDatagram(t, network, configuration), random.NewLockedSource(seed))
}

func startRunningServiceWithDatagram(t *testing.T, configuration config.NodeConfig, store IncarnationStore, serviceClock clock.Clock, datagram transport.Datagram, source random.Source) *runningService {
	t.Helper()
	service, err := NewService(ServiceOptions{
		Config:        configuration,
		Authenticator: wire.NewHMACAuthenticator(testServiceKey()),
		Clock:         serviceClock,
		Random:        source,
		Store:         store,
		Datagram:      datagram,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	running := &runningService{service: service, cancel: cancel, result: make(chan error, 1)}
	go func() { running.result <- service.Run(ctx) }()
	waitServiceReady(t, service)
	t.Cleanup(func() { running.stop(t) })
	return running
}

func (s *runningService) stop(t *testing.T) {
	t.Helper()
	s.stopOnce.Do(func() {
		s.cancel()
		if err := waitServiceResult(t, s.result); err != nil {
			t.Errorf("Run cancellation error = %v", err)
		}
	})
}

func (s *runningService) markStopped() {
	s.stopOnce.Do(func() {})
}

func serviceWireLimits(t *testing.T) wire.Limits {
	t.Helper()
	clusterID := decodedTestClusterID(t, testClusterID)
	limits := wire.DefaultLimits()
	limits.ExpectedClusterID = &clusterID
	return limits
}

func dialServiceTestStream(t *testing.T, endpoint config.Endpoint, authenticator wire.Authenticator, clusterID [16]byte) *wire.TCPFrameStream {
	t.Helper()
	connection, err := net.Dial("tcp", endpoint.String())
	if err != nil {
		t.Fatal(err)
	}
	limits := wire.DefaultLimits()
	limits.ExpectedClusterID = &clusterID
	return wire.NewTCPFrameStream(connection, authenticator, limits, time.Second)
}

func tcpServiceTestFrame(t *testing.T, clusterID [16]byte, senderID uint16, requestByte byte, now time.Time, message wire.MessageType, value any) wire.Frame {
	t.Helper()
	return tcpServiceTestFrameWithPayload(clusterID, senderID, wire.RequestID{requestByte}, now, message, mustEncodeGob(t, value))
}

func tcpServiceTestFrameWithPayload(clusterID [16]byte, senderID uint16, requestID wire.RequestID, now time.Time, message wire.MessageType, payload []byte) wire.Frame {
	return wire.Frame{Header: wire.Header{
		Version:         wire.Version1,
		Message:         message,
		ClusterID:       clusterID,
		SenderID:        senderID,
		RequestID:       requestID,
		TimestampMillis: now.UnixMilli(),
		Codec:           wire.CodecGob,
	}, Payload: payload}
}

func decodeServiceProtocolError(t *testing.T, frame wire.Frame) ProtocolErrorMessage {
	t.Helper()
	if frame.Header.Message != wire.MessageSWIMError {
		t.Fatalf("response type = %d, want MessageSWIMError", frame.Header.Message)
	}
	var response ProtocolErrorMessage
	if err := wire.DecodeGob(frame.Payload, &response); err != nil {
		t.Fatal(err)
	}
	return response
}

func mustReadServiceFrame(t *testing.T, stream *wire.TCPFrameStream) wire.Frame {
	t.Helper()
	frame, err := stream.ReadFrame(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

type serviceStore struct {
	mu    sync.Mutex
	value uint64
}

func newServiceStore(value uint64) *serviceStore {
	return &serviceStore{value: value}
}

func (s *serviceStore) Load() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.value, nil
}

func (s *serviceStore) Store(value uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if value < s.value {
		return ErrIncarnationRegression
	}
	s.value = value
	return nil
}

type barrierServiceStore struct {
	mu         sync.Mutex
	value      uint64
	blockValue uint64
	failure    error
	started    chan struct{}
	release    chan struct{}
	startOnce  sync.Once
}

func newBarrierServiceStore(value, blockValue uint64, failure error) *barrierServiceStore {
	return &barrierServiceStore{value: value, blockValue: blockValue, failure: failure, started: make(chan struct{}), release: make(chan struct{})}
}

func (s *barrierServiceStore) Load() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.value, nil
}

func (s *barrierServiceStore) Store(value uint64) error {
	if value == s.blockValue {
		s.startOnce.Do(func() { close(s.started) })
		<-s.release
		if s.failure != nil {
			return s.failure
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if value < s.value {
		return ErrIncarnationRegression
	}
	s.value = value
	return nil
}

func (s *barrierServiceStore) hasStored(value uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.value >= value
}

type observingDatagram struct {
	transport.Datagram
	store     *barrierServiceStore
	armed     atomic.Bool
	sendCount atomic.Int64
	violation atomic.Bool
}

func (d *observingDatagram) Send(ctx context.Context, destination config.Endpoint, payload []byte) error {
	d.observeSend()
	return d.Datagram.Send(ctx, destination, payload)
}

func (d *observingDatagram) SendFrom(ctx context.Context, source, destination config.Endpoint, payload []byte) error {
	d.observeSend()
	if datagram, ok := d.Datagram.(transport.SourceDatagram); ok {
		return datagram.SendFrom(ctx, source, destination, payload)
	}
	return d.Datagram.Send(ctx, destination, payload)
}

func (d *observingDatagram) observeSend() {
	if d.armed.Load() && !d.store.hasStored(d.store.blockValue) {
		d.violation.Store(true)
	}
	d.sendCount.Add(1)
}

type persistenceHarness struct {
	service       *Service
	network       *transport.MemoryNetwork
	sender        *transport.MemoryDatagram
	datagram      *observingDatagram
	configuration config.NodeConfig
	authenticator wire.Authenticator
	now           time.Time
	cancel        context.CancelFunc
	result        chan error
}

func startPersistenceService(t *testing.T, store *barrierServiceStore) *persistenceHarness {
	t.Helper()
	now := time.Unix(3000, 0)
	configuration := serviceTestConfig(t, 1)
	network := transport.NewMemoryNetwork()
	baseDatagram := serviceMemoryDatagram(t, network, configuration)
	observed := &observingDatagram{Datagram: baseDatagram, store: store}
	authenticator := wire.NewHMACAuthenticator(testServiceKey())
	service, err := NewService(ServiceOptions{
		Config:        configuration,
		Authenticator: authenticator,
		Clock:         clock.NewManual(now),
		Random:        random.NewLockedSource(4),
		Store:         store,
		Datagram:      observed,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- service.Run(ctx) }()
	waitServiceReady(t, service)

	peer := Member{NodeID: 2, Host: "127.0.0.2", BasePort: 12000, Incarnation: 1, Status: Alive}
	peerPing, _ := (config.NodeConfig{AdvertiseHost: peer.Host, BasePort: peer.BasePort}).AdvertiseEndpoint(config.ServiceSWIMPing)
	peerACK, _ := (config.NodeConfig{AdvertiseHost: peer.Host, BasePort: peer.BasePort}).AdvertiseEndpoint(config.ServiceSWIMACK)
	sender, err := network.Endpoint(peerPing, peerACK)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sender.Close() })
	clusterID := decodedTestClusterID(t, testClusterID)
	frame := encodeServiceTestFrame(t, authenticator, clusterID, 1, 20, now, wire.MessageSWIMGossip, mustEncodeGob(t, GossipMessage{Updates: []Update{{Member: peer, ReporterID: 1}}}))
	destination, _ := configuration.AdvertiseEndpoint(config.ServiceSWIMPing)
	if err := observed.SendFrom(context.Background(), destination, destination, frame); err != nil {
		t.Fatal(err)
	}
	network.Advance()
	waitForSnapshot(t, service, func(members []Member) bool { return len(members) == 2 && members[1] == peer })
	network.Advance()
	return &persistenceHarness{
		service:       service,
		network:       network,
		sender:        sender,
		datagram:      observed,
		configuration: configuration,
		authenticator: authenticator,
		now:           now,
		cancel:        cancel,
		result:        result,
	}
}

func (h *persistenceHarness) sendSelfSuspicion(t *testing.T, peerID uint16) {
	t.Helper()
	current := waitForSnapshot(t, h.service, func(members []Member) bool { return len(members) >= 2 })[0]
	current.Status = Suspect
	frame := encodeServiceTestFrame(t, h.authenticator, decodedTestClusterID(t, testClusterID), peerID, 21, h.now, wire.MessageSWIMGossip, mustEncodeGob(t, GossipMessage{Updates: []Update{{Member: current, ReporterID: peerID}}}))
	destination, _ := h.configuration.AdvertiseEndpoint(config.ServiceSWIMPing)
	if err := h.sender.Send(context.Background(), destination, frame); err != nil {
		h.cancel()
		t.Fatal(err)
	}
	if got := h.network.Advance(); got != 1 {
		h.cancel()
		t.Fatalf("Advance delivered = %d, want suspicion datagram", got)
	}
}

func (h *persistenceHarness) enqueuePeerGossip(updates []Update) {
	h.service.events <- datagramServiceEvent{
		senderID: 2,
		message:  GossipMessage{Updates: append([]Update(nil), updates...)},
		updates:  append([]Update(nil), updates...),
	}
}

func waitServiceEventQueue(t *testing.T, service *Service, minimum int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for len(service.events) < minimum && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if got := len(service.events); got < minimum {
		t.Fatalf("queued service events = %d, want at least %d", got, minimum)
	}
}

func serviceSubscriptionCount(t *testing.T, service *Service) int {
	t.Helper()
	response := make(chan int, 1)
	select {
	case service.events <- subscriptionCountServiceEvent{response: response}:
	case <-time.After(time.Second):
		t.Fatal("timed out enqueueing subscription-count query")
	}
	select {
	case count := <-response:
		return count
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for subscription-count query")
		return -1
	}
}

func (h *persistenceHarness) stop(t *testing.T) {
	t.Helper()
	h.cancel()
	if err := waitServiceResult(t, h.result); err != nil {
		t.Fatalf("Run cancellation error = %v", err)
	}
}

func serviceTestConfig(t *testing.T, nodeID uint16) config.NodeConfig {
	t.Helper()
	basePort := reserveServiceTestBase(t)
	secretPath := t.TempDir() + "/cluster.secret"
	if err := os.WriteFile(secretPath, testServiceKey(), 0o600); err != nil {
		t.Fatal(err)
	}
	configuration := config.NodeConfig{
		NodeID:            nodeID,
		ClusterID:         testClusterID,
		BindHost:          "127.0.0.1",
		AdvertiseHost:     "127.0.0.1",
		BasePort:          basePort,
		StorageDir:        t.TempDir(),
		ClusterSecretFile: secretPath,
		Timing:            config.DefaultTimingConfig(),
		RaftVoters: []config.RaftVoter{
			{NodeID: nodeID, Endpoint: config.Endpoint{Host: "127.0.0.1", Port: basePort + 8}.String()},
			{NodeID: nodeID + 100, Endpoint: "127.0.0.2:30008"},
			{NodeID: nodeID + 200, Endpoint: "127.0.0.3:30108"},
		},
	}
	snapshotEndpoint, err := configuration.AdvertiseEndpoint(config.ServiceSWIMSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	configuration.Introducer = snapshotEndpoint.String()
	if err := configuration.Validate(); err != nil {
		t.Fatal(err)
	}
	return configuration
}

var (
	serviceTestPortsMu sync.Mutex
	serviceTestBases   []uint16
)

func reserveServiceTestBase(t *testing.T) uint16 {
	t.Helper()
	for attempts := 0; attempts < 100; attempts++ {
		snapshotListener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		snapshotPort := snapshotListener.Addr().(*net.TCPAddr).Port
		if err := snapshotListener.Close(); err != nil {
			t.Fatal(err)
		}
		if snapshotPort < 1027 || snapshotPort > 65529 {
			continue
		}
		candidate := uint16(snapshotPort - 2)
		serviceTestPortsMu.Lock()
		overlaps := false
		for _, used := range serviceTestBases {
			if uint32(candidate) <= uint32(used)+8 && uint32(used) <= uint32(candidate)+8 {
				overlaps = true
				break
			}
		}
		if !overlaps {
			serviceTestBases = append(serviceTestBases, candidate)
		}
		serviceTestPortsMu.Unlock()
		if !overlaps {
			t.Cleanup(func() {
				serviceTestPortsMu.Lock()
				for index, base := range serviceTestBases {
					if base == candidate {
						serviceTestBases = append(serviceTestBases[:index], serviceTestBases[index+1:]...)
						break
					}
				}
				serviceTestPortsMu.Unlock()
			})
			return candidate
		}
	}
	t.Fatal("could not reserve a non-overlapping service test port range")
	return 0
}

func serviceMemoryDatagram(t *testing.T, network *transport.MemoryNetwork, configuration config.NodeConfig) *transport.MemoryDatagram {
	t.Helper()
	ping, err := configuration.AdvertiseEndpoint(config.ServiceSWIMPing)
	if err != nil {
		t.Fatal(err)
	}
	ack, err := configuration.AdvertiseEndpoint(config.ServiceSWIMACK)
	if err != nil {
		t.Fatal(err)
	}
	datagram, err := network.Endpoint(ping, ack)
	if err != nil {
		t.Fatal(err)
	}
	return datagram
}

func testServiceKey() []byte {
	return []byte("0123456789abcdef0123456789abcdef")
}

func decodedTestClusterID(t *testing.T, value string) [16]byte {
	t.Helper()
	decoded, err := hex.DecodeString(strings.ReplaceAll(value, "-", ""))
	if err != nil || len(decoded) != 16 {
		t.Fatalf("decode cluster ID %q: %v", value, err)
	}
	var clusterID [16]byte
	copy(clusterID[:], decoded)
	return clusterID
}

func mustEncodeGob(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := wire.EncodeGob(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func encodeServiceTestFrame(t *testing.T, authenticator wire.Authenticator, clusterID [16]byte, senderID uint16, requestByte byte, now time.Time, message wire.MessageType, payload []byte) []byte {
	t.Helper()
	requestID := wire.RequestID{requestByte}
	encoded, err := wire.Encode(wire.Header{
		Version:         wire.Version1,
		Message:         message,
		ClusterID:       clusterID,
		SenderID:        senderID,
		RequestID:       requestID,
		TimestampMillis: now.UnixMilli(),
		Codec:           wire.CodecGob,
	}, payload, authenticator, wire.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func waitServiceReady(t *testing.T, service *Service) {
	t.Helper()
	select {
	case <-service.Ready():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for service readiness")
	}
}

func serviceTCPConnectionCount(service *Service) int {
	count := 0
	service.connections.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

func waitServiceTCPConnectionCount(t *testing.T, service *Service, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for serviceTCPConnectionCount(service) != want && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if got := serviceTCPConnectionCount(service); got != want {
		t.Fatalf("owned TCP connections = %d, want %d", got, want)
	}
}

func assertConnectionClosedPromptly(t *testing.T, connection net.Conn, label string) {
	t.Helper()
	defer connection.Close()
	if err := connection.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Read(make([]byte, 1)); err == nil {
		t.Fatalf("%s remained open", label)
	} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatalf("%s was not closed before read deadline: %v", label, err)
	}
}

func waitForSnapshot(t *testing.T, service *Service, condition func([]Member) bool) []Member {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		snapshot, err := service.Snapshot(ctx)
		if err == nil && condition(snapshot) {
			return snapshot
		}
		runtime.Gosched()
	}
	t.Fatalf("snapshot condition not met: %v", ctx.Err())
	return nil
}

func TestEventsChangeActiveMembershipTreatsMissingPreviousAsInactive(t *testing.T) {
	newTerminal := MembershipEvent{Current: Member{NodeID: 2, Status: Dead}}
	if eventsChangeActiveMembership([]MembershipEvent{newTerminal}) {
		t.Fatal("new terminal member changed active membership")
	}

	newAlive := MembershipEvent{Current: Member{NodeID: 2, Status: Alive}}
	if !eventsChangeActiveMembership([]MembershipEvent{newAlive}) {
		t.Fatal("new alive member did not change active membership")
	}

	terminalProgression := MembershipEvent{
		Previous: Member{NodeID: 2, Status: Dead},
		Current:  Member{NodeID: 2, Status: Left},
	}
	if eventsChangeActiveMembership([]MembershipEvent{terminalProgression}) {
		t.Fatal("terminal-only progression changed active membership")
	}
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)
	return ctx
}

func waitServiceResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for service Run result")
		return nil
	}
}
