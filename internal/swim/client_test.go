package swim

import (
	"context"
	"errors"
	"math"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/config"
	"github.com/aaditya/cs425mp3/internal/random"
	"github.com/aaditya/cs425mp3/internal/transport"
	"github.com/aaditya/cs425mp3/internal/wire"
)

func TestProtocolClientDialUsesConfiguredIOTimeout(t *testing.T) {
	configuration := config.NodeConfig{NodeID: 1, ClusterID: testClusterID, Timing: config.DefaultTimingConfig()}
	client, err := newProtocolClient(configuration, wire.NewHMACAuthenticator(testServiceKey()), clock.NewManual(time.Unix(4455, 0)), random.NewLockedSource(194), 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	client.dialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	started := time.Now()
	_, _, _, err = client.dial(context.Background(), config.Endpoint{Host: "192.0.2.1", Port: 12002})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("dial error = %v, want configured deadline", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("configured 20ms dial timeout took %s", elapsed)
	}
}

func TestSnapshotClientInvalidResponseCannotPoisonReplayCapacity(t *testing.T) {
	now := time.Unix(4460, 0)
	configuration := serviceTestConfig(t, 1)
	authenticator := wire.NewHMACAuthenticator(testServiceKey())
	clusterID := decodedTestClusterID(t, testClusterID)
	validMember := Member{NodeID: 2, Host: "127.0.0.2", BasePort: 12000, Incarnation: 1, Status: Alive}
	validPayload := mustEncodeGob(t, SnapshotResponse{Members: []Member{validMember}})
	endpoint := startScriptedTCPServer(t, authenticator, clusterID, []func(wire.Frame) wire.Frame{
		func(request wire.Frame) wire.Frame {
			return tcpServiceTestFrameWithPayload(clusterID, 2, request.Header.RequestID, now, wire.MessageSWIMSnapshotResponse, []byte("not-gob"))
		},
		func(request wire.Frame) wire.Frame {
			return tcpServiceTestFrameWithPayload(clusterID, 2, request.Header.RequestID, now, wire.MessageSWIMSnapshotResponse, validPayload)
		},
	})
	client, err := newProtocolClient(configuration, authenticator, clock.NewManual(now), random.NewLockedSource(191), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client.replay = wire.NewReplayGuard(client.clock, time.Duration(configuration.Timing.ReplayWindow), serviceFutureSkew, 1)

	if _, err := client.snapshot(testContext(t), endpoint); !errors.Is(err, ErrSnapshotProtocol) {
		t.Fatalf("invalid response error = %v, want ErrSnapshotProtocol", err)
	}
	if snapshot, err := client.snapshot(testContext(t), endpoint); err != nil || len(snapshot) != 1 || snapshot[0].NodeID != 2 {
		t.Fatalf("valid response after invalid = %#v, error = %v", snapshot, err)
	}
}

func TestSnapshotClientReplayPreflightPrecedesPayloadDecode(t *testing.T) {
	now := time.Unix(4462, 0)
	configuration := serviceTestConfig(t, 1)
	authenticator := wire.NewHMACAuthenticator(testServiceKey())
	clusterID := decodedTestClusterID(t, testClusterID)
	endpoint := startScriptedTCPServer(t, authenticator, clusterID, []func(wire.Frame) wire.Frame{
		func(request wire.Frame) wire.Frame {
			staleAt := now.Add(-time.Duration(configuration.Timing.ReplayWindow))
			return tcpServiceTestFrameWithPayload(clusterID, 2, request.Header.RequestID, staleAt, wire.MessageSWIMSnapshotResponse, []byte("not-gob"))
		},
	})
	client, err := newProtocolClient(configuration, authenticator, clock.NewManual(now), random.NewLockedSource(199), time.Second)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := client.snapshot(testContext(t), endpoint); !errors.Is(err, wire.ErrTimestamp) {
		t.Fatalf("stale malformed response error = %v, want timestamp preflight", err)
	}
}

func TestSnapshotClientRequiresResponderAsNonterminalSnapshotMember(t *testing.T) {
	now := time.Unix(4463, 0)
	configuration := serviceTestConfig(t, 1)
	authenticator := wire.NewHMACAuthenticator(testServiceKey())
	clusterID := decodedTestClusterID(t, testClusterID)
	other := Member{NodeID: 4, Host: "127.0.0.4", BasePort: 14000, Incarnation: 1, Status: Alive}
	terminalResponder := Member{NodeID: 2, Host: "127.0.0.2", BasePort: 12000, Incarnation: 1, Status: Dead}
	aliveResponder := terminalResponder
	aliveResponder.Status = Alive
	responses := []SnapshotResponse{
		{Members: []Member{other}},
		{Members: []Member{terminalResponder, other}},
		{Members: []Member{aliveResponder, other}},
	}
	scripts := make([]func(wire.Frame) wire.Frame, len(responses))
	for index := range responses {
		response := responses[index]
		scripts[index] = func(request wire.Frame) wire.Frame {
			return tcpServiceTestFrameWithPayload(clusterID, 2, request.Header.RequestID, now, wire.MessageSWIMSnapshotResponse, mustEncodeGob(t, response))
		}
	}
	endpoint := startScriptedTCPServer(t, authenticator, clusterID, scripts)
	client, err := newProtocolClient(configuration, authenticator, clock.NewManual(now), random.NewLockedSource(200), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client.replay = wire.NewReplayGuard(client.clock, time.Duration(configuration.Timing.ReplayWindow), serviceFutureSkew, 1)

	for _, name := range []string{"missing", "terminal"} {
		if snapshot, err := client.snapshot(testContext(t), endpoint); !errors.Is(err, ErrSnapshotProtocol) || snapshot != nil {
			t.Fatalf("%s responder snapshot = %#v, error = %v, want protocol rejection", name, snapshot, err)
		}
	}
	if snapshot, err := client.snapshot(testContext(t), endpoint); err != nil || len(snapshot) != 2 || snapshot[0] != aliveResponder {
		t.Fatalf("valid responder after invalid snapshots = %#v, error = %v", snapshot, err)
	}
}

func TestValidateSnapshotStateAcceptsOnlyUniqueTerminalFloors(t *testing.T) {
	member := Member{NodeID: 2, Host: "127.0.0.2", BasePort: 12000, Incarnation: 5, Status: Alive}
	floor := Member{NodeID: 3, Host: "127.0.0.3", BasePort: 13000, Incarnation: 7, Status: Dead}
	if err := validateSnapshotState([]Member{member}, []Member{floor}); err != nil {
		t.Fatalf("valid snapshot state error = %v", err)
	}

	for _, test := range []struct {
		name   string
		floors []Member
	}{
		{name: "nonterminal", floors: []Member{{NodeID: 4, Host: "127.0.0.4", BasePort: 14000, Incarnation: 8, Status: Alive}}},
		{name: "duplicate member", floors: []Member{{NodeID: member.NodeID, Host: member.Host, BasePort: member.BasePort, Incarnation: 4, Status: Dead}}},
		{name: "duplicate floor", floors: []Member{floor, floor}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validateSnapshotState([]Member{member}, test.floors); !errors.Is(err, ErrSnapshotProtocol) {
				t.Fatalf("validation error = %v, want ErrSnapshotProtocol", err)
			}
		})
	}
}

func TestInternalSnapshotResyncRejectsUnexpectedFirstResponderBeforeReplayAcceptance(t *testing.T) {
	now := time.Unix(4465, 0)
	configuration := serviceTestConfig(t, 1)
	authenticator := wire.NewHMACAuthenticator(testServiceKey())
	clusterID := decodedTestClusterID(t, testClusterID)
	validMember := Member{NodeID: 2, Host: "127.0.0.2", BasePort: 12000, Incarnation: 1, Status: Alive}
	validPayload := mustEncodeGob(t, SnapshotResponse{Members: []Member{validMember}})
	errorPayload := mustEncodeGob(t, encodeProtocolError(ErrServiceNotAdmitted))

	for _, test := range []struct {
		name        string
		messageType wire.MessageType
		payload     []byte
	}{
		{name: "normal response", messageType: wire.MessageSWIMSnapshotResponse, payload: validPayload},
		{name: "SWIMError", messageType: wire.MessageSWIMError, payload: errorPayload},
	} {
		t.Run(test.name, func(t *testing.T) {
			endpoint := startScriptedTCPServer(t, authenticator, clusterID, []func(wire.Frame) wire.Frame{
				func(request wire.Frame) wire.Frame {
					return tcpServiceTestFrameWithPayload(clusterID, 3, request.Header.RequestID, now, test.messageType, test.payload)
				},
				func(request wire.Frame) wire.Frame {
					return tcpServiceTestFrameWithPayload(clusterID, 2, request.Header.RequestID, now, wire.MessageSWIMSnapshotResponse, validPayload)
				},
			})
			client, err := newProtocolClient(configuration, authenticator, clock.NewManual(now), random.NewLockedSource(198), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			client.replay = wire.NewReplayGuard(client.clock, time.Duration(configuration.Timing.ReplayWindow), serviceFutureSkew, 1)
			service := &Service{events: make(chan serviceEvent, 2), done: make(chan struct{})}
			workerContext, cancelWorkers := context.WithCancel(context.Background())
			defer cancelWorkers()
			var workers sync.WaitGroup
			loop := &serviceLoop{
				service:       service,
				client:        client,
				workerContext: workerContext,
				workers:       &workers,
				resyncing:     make(map[uint16]bool),
				resyncJobs:    make(chan snapshotResyncJob, serviceResyncQueueSize),
				beginSnapshot: client.beginSnapshot,
			}
			loop.startSnapshotResyncWorkers()
			expected := Member{NodeID: 2, Host: endpoint.Host, BasePort: endpoint.Port - 2, Incarnation: 1, Status: Alive}

			loop.startSnapshotResync(expected)
			first := receiveSnapshotResyncEvent(t, service.events)
			if !errors.Is(first.err, ErrSnapshotProtocol) {
				t.Fatalf("unexpected %s sender error = %v, want ErrSnapshotProtocol", test.name, first.err)
			}
			delete(loop.resyncing, expected.NodeID)

			loop.startSnapshotResync(expected)
			second := receiveSnapshotResyncEvent(t, service.events)
			if second.err != nil || len(second.members) != 1 || second.members[0] != validMember || second.applied == nil {
				t.Fatalf("valid expected response after unexpected %s = %#v", test.name, second)
			}
			second.applied <- ErrSnapshotProtocol
			cancelWorkers()
			workers.Wait()
		})
	}
}

func receiveSnapshotResyncEvent(t *testing.T, events <-chan serviceEvent) snapshotResyncServiceEvent {
	t.Helper()
	select {
	case event := <-events:
		result, ok := event.(snapshotResyncServiceEvent)
		if !ok {
			t.Fatalf("snapshot resync event type = %T", event)
		}
		return result
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for snapshot resync event")
		return snapshotResyncServiceEvent{}
	}
}

func TestJoinClientRequiresOneResponderIdentity(t *testing.T) {
	now := time.Unix(4470, 0)
	configuration := serviceTestConfig(t, 1)
	authenticator := wire.NewHMACAuthenticator(testServiceKey())
	clusterID := decodedTestClusterID(t, testClusterID)
	seed := Member{NodeID: 2, Host: "127.0.0.2", BasePort: 12000, Incarnation: 1, Status: Alive}
	endpoint := startJoinResponderMismatchServer(t, authenticator, clusterID, now, seed)
	client, err := newProtocolClient(configuration, authenticator, clock.NewManual(now), random.NewLockedSource(192), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	self := Member{NodeID: configuration.NodeID, Host: configuration.AdvertiseHost, BasePort: configuration.BasePort}
	if _, err := client.join(testContext(t), endpoint, newServiceStore(1), self); !errors.Is(err, ErrSnapshotProtocol) {
		t.Fatalf("mismatched join responder error = %v, want ErrSnapshotProtocol", err)
	}
}

func TestJoinClientRejectsMismatchedErrorResponderBeforeReplayAcceptance(t *testing.T) {
	now := time.Unix(4475, 0)
	configuration := serviceTestConfig(t, 1)
	authenticator := wire.NewHMACAuthenticator(testServiceKey())
	clusterID := decodedTestClusterID(t, testClusterID)
	endpoint := startJoinResponderErrorMismatchServer(t, authenticator, clusterID, now)
	client, err := newProtocolClient(configuration, authenticator, clock.NewManual(now), random.NewLockedSource(196), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client.replay = wire.NewReplayGuard(client.clock, time.Duration(configuration.Timing.ReplayWindow), serviceFutureSkew, 2)
	self := Member{NodeID: configuration.NodeID, Host: configuration.AdvertiseHost, BasePort: configuration.BasePort}
	if _, err := client.join(testContext(t), endpoint, newServiceStore(1), self); !errors.Is(err, ErrSnapshotProtocol) {
		t.Fatalf("mismatched SWIMError responder error = %v, want ErrSnapshotProtocol", err)
	}

	validMember := Member{NodeID: 2, Host: "127.0.0.2", BasePort: 12000, Incarnation: 1, Status: Alive}
	validPayload := mustEncodeGob(t, SnapshotResponse{Members: []Member{validMember}})
	validEndpoint := startScriptedTCPServer(t, authenticator, clusterID, []func(wire.Frame) wire.Frame{
		func(request wire.Frame) wire.Frame {
			return tcpServiceTestFrameWithPayload(clusterID, 2, request.Header.RequestID, now, wire.MessageSWIMSnapshotResponse, validPayload)
		},
	})
	if snapshot, err := client.snapshot(testContext(t), validEndpoint); err != nil || len(snapshot) != 1 || snapshot[0] != validMember {
		t.Fatalf("valid response after mismatched SWIMError = %#v, error = %v", snapshot, err)
	}
}

func TestJoinClientBindsFirstResponderToDialedSeedBeforeReplayOrPersistence(t *testing.T) {
	now := time.Unix(4480, 0)
	joiningConfiguration := serviceTestConfig(t, 1)
	authenticator := wire.NewHMACAuthenticator(testServiceKey())
	clusterID := decodedTestClusterID(t, testClusterID)
	badEndpoint := startJoinSnapshotOnlyServer(t, authenticator, clusterID, now, func(endpoint config.Endpoint) Member {
		return Member{NodeID: 9, Host: endpoint.Host, BasePort: endpoint.Port - 3, Incarnation: 1, Status: Alive}
	})
	client, err := newProtocolClient(joiningConfiguration, authenticator, clock.NewManual(now), random.NewLockedSource(193), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client.replay = wire.NewReplayGuard(client.clock, time.Duration(joiningConfiguration.Timing.ReplayWindow), serviceFutureSkew, 2)
	store := newServiceStore(1)
	self := Member{NodeID: joiningConfiguration.NodeID, Host: joiningConfiguration.AdvertiseHost, BasePort: joiningConfiguration.BasePort}
	if _, err := client.join(testContext(t), badEndpoint, store, self); !errors.Is(err, ErrSnapshotProtocol) {
		t.Fatalf("misbound first responder error = %v, want ErrSnapshotProtocol", err)
	}
	if got, _ := store.Load(); got != 1 {
		t.Fatalf("misbound first responder persisted incarnation %d, want unchanged 1", got)
	}

	network := transport.NewMemoryNetwork()
	seedConfiguration := serviceTestConfig(t, 2)
	seed := startRunningService(t, seedConfiguration, newServiceStore(1), clock.NewManual(now), network, 194)
	seedEndpoint, _ := seedConfiguration.AdvertiseEndpoint(config.ServiceSWIMSnapshot)
	result, err := client.join(testContext(t), seedEndpoint, store, self)
	if err != nil {
		t.Fatalf("valid seed after misbound responder: %v", err)
	}
	if result.seedID != seedConfiguration.NodeID || result.accepted.NodeID != self.NodeID {
		t.Fatalf("valid seed result = %#v", result)
	}
	if got, _ := store.Load(); got != 2 {
		t.Fatalf("valid seed persisted incarnation %d, want 2", got)
	}
	seed.stop(t)
}

func TestJoinClientRejectsSelfAsFirstResponderBeforeReplayOrPersistence(t *testing.T) {
	now := time.Unix(4482, 0)
	configuration := serviceTestConfig(t, 1)
	authenticator := wire.NewHMACAuthenticator(testServiceKey())
	clusterID := decodedTestClusterID(t, testClusterID)
	selfEndpoint := startJoinSnapshotOnlyServer(t, authenticator, clusterID, now, func(endpoint config.Endpoint) Member {
		return Member{
			NodeID:      configuration.NodeID,
			Host:        endpoint.Host,
			BasePort:    endpoint.Port - 2,
			Incarnation: math.MaxUint64 - 1,
			Status:      Alive,
		}
	})
	client, err := newProtocolClient(configuration, authenticator, clock.NewManual(now), random.NewLockedSource(197), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client.replay = wire.NewReplayGuard(client.clock, time.Duration(configuration.Timing.ReplayWindow), serviceFutureSkew, 2)
	store := &recordingIncarnationStore{loaded: 1}
	self := Member{NodeID: configuration.NodeID, Host: configuration.AdvertiseHost, BasePort: configuration.BasePort}
	if _, err := client.join(testContext(t), selfEndpoint, store, self); !errors.Is(err, ErrSnapshotProtocol) {
		t.Fatalf("self first responder error = %v, want ErrSnapshotProtocol", err)
	}
	if store.loads != 0 || len(store.stored) != 0 {
		t.Fatalf("self first responder durable calls = loads:%d stores:%v, want none", store.loads, store.stored)
	}

	seedEndpoint := startSuccessfulJoinServer(t, authenticator, clusterID, now, func(endpoint config.Endpoint) Member {
		return Member{NodeID: 2, Host: endpoint.Host, BasePort: endpoint.Port - 2, Incarnation: 1, Status: Alive}
	})
	result, err := client.join(testContext(t), seedEndpoint, store, self)
	if err != nil {
		t.Fatalf("legitimate seed after self responder: %v", err)
	}
	if result.seedID != 2 || result.accepted.NodeID != self.NodeID || result.accepted.Incarnation != 2 {
		t.Fatalf("legitimate seed result = %#v", result)
	}
	if store.loads != 1 || len(store.stored) != 1 || store.stored[0] != 2 {
		t.Fatalf("legitimate seed durable calls = loads:%d stores:%v, want one load and Store(2)", store.loads, store.stored)
	}
}

func TestJoinClientAcceptsResolvedResponderMatchingNumericConnectionTarget(t *testing.T) {
	now := time.Unix(4485, 0)
	configuration := serviceTestConfig(t, 1)
	authenticator := wire.NewHMACAuthenticator(testServiceKey())
	clusterID := decodedTestClusterID(t, testClusterID)
	endpoint := startSuccessfulJoinServer(t, authenticator, clusterID, now, func(endpoint config.Endpoint) Member {
		return Member{NodeID: 2, Host: "localhost", BasePort: endpoint.Port - 2, Incarnation: 1, Status: Alive}
	})
	resolver := &staticAddressResolver{addresses: map[string][]netip.Addr{
		"localhost": {netip.MustParseAddr("127.0.0.1")},
	}}
	client, err := newProtocolClientWithAddressMatcher(configuration, authenticator, clock.NewManual(now), random.NewLockedSource(195), time.Second, newAddressMatcher(resolver))
	if err != nil {
		t.Fatal(err)
	}
	self := Member{NodeID: configuration.NodeID, Host: configuration.AdvertiseHost, BasePort: configuration.BasePort}
	result, err := client.join(testContext(t), endpoint, newServiceStore(1), self)
	if err != nil {
		t.Fatalf("join through resolved responder endpoint: %v", err)
	}
	if result.seedID != 2 || result.accepted.NodeID != self.NodeID {
		t.Fatalf("resolved responder join result = %#v", result)
	}
}

func startScriptedTCPServer(t *testing.T, authenticator wire.Authenticator, clusterID [16]byte, scripts []func(wire.Frame) wire.Frame) config.Endpoint {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for _, script := range scripts {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			stream := wire.NewTCPFrameStream(connection, authenticator, clientTestWireLimits(clusterID), time.Second)
			request, err := stream.ReadFrame(context.Background())
			if err == nil {
				_ = stream.WriteFrame(context.Background(), script(request))
			}
			_ = stream.Close()
		}
	}()
	address := listener.Addr().(*net.TCPAddr)
	return config.Endpoint{Host: address.IP.String(), Port: uint16(address.Port)}
}

func startJoinResponderMismatchServer(t *testing.T, authenticator wire.Authenticator, clusterID [16]byte, now time.Time, seed Member) config.Endpoint {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	address := listener.Addr().(*net.TCPAddr)
	endpoint := config.Endpoint{Host: address.IP.String(), Port: uint16(address.Port)}
	seed.Host = endpoint.Host
	seed.BasePort = endpoint.Port - 2
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		stream := wire.NewTCPFrameStream(connection, authenticator, clientTestWireLimits(clusterID), time.Second)
		defer stream.Close()
		request, err := stream.ReadFrame(context.Background())
		if err != nil {
			return
		}
		snapshotPayload, err := wire.EncodeGob(JoinSnapshot{Members: []Member{seed}})
		if err != nil {
			return
		}
		_ = stream.WriteFrame(context.Background(), tcpServiceTestFrameWithPayload(clusterID, seed.NodeID, request.Header.RequestID, now, wire.MessageSWIMJoinSnapshot, snapshotPayload))
		announceFrame, err := stream.ReadFrame(context.Background())
		if err != nil {
			return
		}
		var announce JoinAnnounce
		if wire.DecodeGob(announceFrame.Payload, &announce) != nil {
			return
		}
		acceptedPayload, err := wire.EncodeGob(JoinAccepted(announce))
		if err != nil {
			return
		}
		_ = stream.WriteFrame(context.Background(), tcpServiceTestFrameWithPayload(clusterID, seed.NodeID+1, announceFrame.Header.RequestID, now, wire.MessageSWIMJoinAccepted, acceptedPayload))
	}()
	return endpoint
}

func startJoinResponderErrorMismatchServer(t *testing.T, authenticator wire.Authenticator, clusterID [16]byte, now time.Time) config.Endpoint {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	address := listener.Addr().(*net.TCPAddr)
	endpoint := config.Endpoint{Host: address.IP.String(), Port: uint16(address.Port)}
	seed := Member{NodeID: 2, Host: endpoint.Host, BasePort: endpoint.Port - 2, Incarnation: 1, Status: Alive}
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		stream := wire.NewTCPFrameStream(connection, authenticator, clientTestWireLimits(clusterID), time.Second)
		defer stream.Close()
		request, err := stream.ReadFrame(context.Background())
		if err != nil {
			return
		}
		snapshotPayload, err := wire.EncodeGob(JoinSnapshot{Members: []Member{seed}})
		if err != nil {
			return
		}
		if stream.WriteFrame(context.Background(), tcpServiceTestFrameWithPayload(clusterID, seed.NodeID, request.Header.RequestID, now, wire.MessageSWIMJoinSnapshot, snapshotPayload)) != nil {
			return
		}
		announceFrame, err := stream.ReadFrame(context.Background())
		if err != nil {
			return
		}
		errorPayload, err := wire.EncodeGob(encodeProtocolError(ErrServiceNotAdmitted))
		if err != nil {
			return
		}
		_ = stream.WriteFrame(context.Background(), tcpServiceTestFrameWithPayload(clusterID, seed.NodeID+1, announceFrame.Header.RequestID, now, wire.MessageSWIMError, errorPayload))
	}()
	return endpoint
}

func startJoinSnapshotOnlyServer(t *testing.T, authenticator wire.Authenticator, clusterID [16]byte, now time.Time, member func(config.Endpoint) Member) config.Endpoint {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	address := listener.Addr().(*net.TCPAddr)
	endpoint := config.Endpoint{Host: address.IP.String(), Port: uint16(address.Port)}
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		stream := wire.NewTCPFrameStream(connection, authenticator, clientTestWireLimits(clusterID), time.Second)
		defer stream.Close()
		request, err := stream.ReadFrame(context.Background())
		if err != nil {
			return
		}
		claimed := member(endpoint)
		payload, err := wire.EncodeGob(JoinSnapshot{Members: []Member{claimed}})
		if err != nil {
			return
		}
		_ = stream.WriteFrame(context.Background(), tcpServiceTestFrameWithPayload(clusterID, claimed.NodeID, request.Header.RequestID, now, wire.MessageSWIMJoinSnapshot, payload))
	}()
	return endpoint
}

func startSuccessfulJoinServer(t *testing.T, authenticator wire.Authenticator, clusterID [16]byte, now time.Time, member func(config.Endpoint) Member) config.Endpoint {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	address := listener.Addr().(*net.TCPAddr)
	endpoint := config.Endpoint{Host: address.IP.String(), Port: uint16(address.Port)}
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		stream := wire.NewTCPFrameStream(connection, authenticator, clientTestWireLimits(clusterID), time.Second)
		defer stream.Close()
		request, err := stream.ReadFrame(context.Background())
		if err != nil {
			return
		}
		seed := member(endpoint)
		snapshotPayload, err := wire.EncodeGob(JoinSnapshot{Members: []Member{seed}})
		if err != nil {
			return
		}
		if stream.WriteFrame(context.Background(), tcpServiceTestFrameWithPayload(clusterID, seed.NodeID, request.Header.RequestID, now, wire.MessageSWIMJoinSnapshot, snapshotPayload)) != nil {
			return
		}
		announceFrame, err := stream.ReadFrame(context.Background())
		if err != nil {
			return
		}
		var announce JoinAnnounce
		if wire.DecodeGob(announceFrame.Payload, &announce) != nil {
			return
		}
		acceptedPayload, err := wire.EncodeGob(JoinAccepted(announce))
		if err != nil {
			return
		}
		_ = stream.WriteFrame(context.Background(), tcpServiceTestFrameWithPayload(clusterID, seed.NodeID, announceFrame.Header.RequestID, now, wire.MessageSWIMJoinAccepted, acceptedPayload))
	}()
	return endpoint
}

func clientTestWireLimits(clusterID [16]byte) wire.Limits {
	limits := wire.DefaultLimits()
	limits.ExpectedClusterID = &clusterID
	return limits
}
