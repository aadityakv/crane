package swim

import (
	"context"
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
	if err := attacker.Send(context.Background(), destination, barrierFrame); err != nil {
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
	if d.armed.Load() && !d.store.hasStored(d.store.blockValue) {
		d.violation.Store(true)
	}
	d.sendCount.Add(1)
	return d.Datagram.Send(ctx, destination, payload)
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

	senderAddress := config.Endpoint{Host: "sender", Port: 9100}
	sender, err := network.Endpoint(senderAddress)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sender.Close() })
	peer := Member{NodeID: 2, Host: "127.0.0.2", BasePort: 12000, Incarnation: 1, Status: Alive}
	clusterID := decodedTestClusterID(t, testClusterID)
	frame := encodeServiceTestFrame(t, authenticator, clusterID, 1, 20, now, wire.MessageSWIMGossip, mustEncodeGob(t, GossipMessage{Updates: []Update{{Member: peer, ReporterID: 1}}}))
	destination, _ := configuration.AdvertiseEndpoint(config.ServiceSWIMPing)
	if err := sender.Send(context.Background(), destination, frame); err != nil {
		t.Fatal(err)
	}
	network.Advance()
	waitForSnapshot(t, service, func(members []Member) bool { return len(members) == 2 && members[1] == peer })
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

func (h *persistenceHarness) stop(t *testing.T) {
	t.Helper()
	h.cancel()
	if err := waitServiceResult(t, h.result); err != nil {
		t.Fatalf("Run cancellation error = %v", err)
	}
}

func serviceTestConfig(t *testing.T, nodeID uint16) config.NodeConfig {
	t.Helper()
	snapshotListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	snapshotPort := snapshotListener.Addr().(*net.TCPAddr).Port
	if err := snapshotListener.Close(); err != nil {
		t.Fatal(err)
	}
	if snapshotPort < 1027 || snapshotPort > 65529 {
		t.Fatalf("ephemeral snapshot port %d cannot produce a valid base", snapshotPort)
	}
	basePort := uint16(snapshotPort - 2)
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

func waitForSnapshot(t *testing.T, service *Service, condition func([]Member) bool) []Member {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
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
