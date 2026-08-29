package swim

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/config"
	"github.com/aaditya/cs425mp3/internal/random"
	"github.com/aaditya/cs425mp3/internal/wire"
)

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
		acceptedPayload, err := wire.EncodeGob(JoinAccepted{Member: announce.Member})
		if err != nil {
			return
		}
		_ = stream.WriteFrame(context.Background(), tcpServiceTestFrameWithPayload(clusterID, seed.NodeID+1, announceFrame.Header.RequestID, now, wire.MessageSWIMJoinAccepted, acceptedPayload))
	}()
	address := listener.Addr().(*net.TCPAddr)
	return config.Endpoint{Host: address.IP.String(), Port: uint16(address.Port)}
}

func clientTestWireLimits(clusterID [16]byte) wire.Limits {
	limits := wire.DefaultLimits()
	limits.ExpectedClusterID = &clusterID
	return limits
}
