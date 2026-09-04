package coordinator

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aadityakv/crane/internal/clock"
	"github.com/aadityakv/crane/internal/crane/membership"
	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
	"github.com/aadityakv/crane/internal/swim"
	"github.com/aadityakv/crane/internal/wire"
)

var testClusterID = [16]byte{0xC1}

// scriptedControlServer speaks the authenticated +3 framing for one worker.
type scriptedControlServer struct {
	t         *testing.T
	listener  net.Listener
	auth      wire.Authenticator
	nodeID    uint16
	epoch     model.WorkerEpoch
	ackNodeID uint16

	mu       sync.Mutex
	received []protocol.WorkerMessage
	respond  func(message protocol.WorkerMessage) protocol.WorkerMessage
}

func startControlServer(t *testing.T, nodeID uint16) *scriptedControlServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := &scriptedControlServer{
		t: t, listener: listener, auth: wire.NewHMACAuthenticator([]byte("task-18")),
		nodeID: nodeID, epoch: model.WorkerEpoch{byte(nodeID), 0x7}, ackNodeID: nodeID,
	}
	go server.acceptLoop()
	t.Cleanup(func() { _ = listener.Close() })
	return server
}

func (server *scriptedControlServer) acceptLoop() {
	for {
		connection, err := server.listener.Accept()
		if err != nil {
			return
		}
		go server.serve(connection)
	}
}

func (server *scriptedControlServer) serve(connection net.Conn) {
	defer connection.Close()
	limits := wire.DefaultLimits()
	limits.MaxFrameSize = int(model.WorkerControlMaxFrameBytesV1)
	expected := testClusterID
	limits.ExpectedClusterID = &expected
	stream := wire.NewTCPFrameStream(connection, server.auth, limits, time.Second)
	ctx := context.Background()
	for {
		frame, err := stream.ReadFrame(ctx)
		if err != nil {
			return
		}
		message, err := protocol.UnmarshalWorkerMessage(frame.Header.Message, frame.Payload)
		if err != nil {
			return
		}
		server.mu.Lock()
		server.received = append(server.received, message)
		respond := server.respond
		server.mu.Unlock()
		var response protocol.WorkerMessage
		if _, ok := message.(protocol.WorkerHandshake); ok {
			response = protocol.WorkerHandshakeAck{
				NodeID: server.ackNodeID, WorkerEpoch: server.epoch, SlotCapacity: 4,
				ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint(),
			}
		} else if respond != nil {
			response = respond(message)
		}
		if response == nil {
			return
		}
		payload, err := protocol.MarshalWorkerMessage(response)
		if err != nil {
			server.t.Errorf("marshal scripted response: %v", err)
			return
		}
		outbound := wire.Frame{Header: wire.Header{
			Version: wire.Version1, Message: response.MessageType(), ClusterID: testClusterID,
			SenderID: server.nodeID, RequestID: frame.Header.RequestID,
			TimestampMillis: time.Now().UnixMilli(), Codec: wire.CodecBinary,
		}, Payload: payload}
		if err := stream.WriteFrame(ctx, outbound); err != nil {
			return
		}
	}
}

func (server *scriptedControlServer) member() swim.Member {
	port := server.listener.Addr().(*net.TCPAddr).Port
	return swim.Member{NodeID: server.nodeID, Host: "127.0.0.1", BasePort: uint16(port - 3), Incarnation: 1, Status: swim.Alive}
}

func (server *scriptedControlServer) messages() []protocol.WorkerMessage {
	server.mu.Lock()
	defer server.mu.Unlock()
	return append([]protocol.WorkerMessage(nil), server.received...)
}

func (server *scriptedControlServer) setResponder(respond func(protocol.WorkerMessage) protocol.WorkerMessage) {
	server.mu.Lock()
	defer server.mu.Unlock()
	server.respond = respond
}

// fakeClientMembership resolves exactly one advertised member.
type fakeClientMembership struct {
	mu           sync.Mutex
	member       swim.Member
	authorizeErr error
}

func (m *fakeClientMembership) View() membership.View {
	m.mu.Lock()
	defer m.mu.Unlock()
	return membership.View{Revision: 1, Members: []swim.Member{m.member}}
}

func (m *fakeClientMembership) AuthorizeTCP(uint16, net.Addr) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.authorizeErr
}

func newTestDialClient(t *testing.T, server *scriptedControlServer) (*DialWorkerClient, *fakeClientMembership) {
	t.Helper()
	members := &fakeClientMembership{member: server.member()}
	client, err := NewDialWorkerClient(DialWorkerClientOptions{
		ClusterID: testClusterID, NodeID: 1, SessionEpoch: model.WorkerEpoch{0x1, 0x1},
		Authenticator: wire.NewHMACAuthenticator([]byte("task-18")),
		Clock:         clock.NewReal(), Membership: members, Timeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewDialWorkerClient: %v", err)
	}
	return client, members
}

func testClientEpoch() model.CoordinatorEpoch {
	return model.CoordinatorEpoch{Term: 2, BeginIndex: 5, Coordinator: 1, Nonce: [16]byte{0x9}}
}

func testClientAssignment(t *testing.T, node uint16, epoch model.WorkerEpoch) (model.ValidatedTopology, model.AssignmentSet) {
	t.Helper()
	topology, err := model.ValidateTopology(testTopologySpec(1))
	if err != nil {
		t.Fatalf("validate topology: %v", err)
	}
	job := model.JobID{0x18}
	placements := []model.WorkerPlacement{
		{NodeID: node, WorkerEpoch: epoch, SlotCapacity: 4},
		{NodeID: node + 1, WorkerEpoch: model.WorkerEpoch{byte(node + 1)}, SlotCapacity: 4},
	}
	assignment, err := model.BuildAssignmentSet(job, topology.Digest(), 1, topology, placements)
	if err != nil {
		t.Fatalf("build assignment: %v", err)
	}
	return topology, assignment
}

func TestDialClientHandshakeReturnsAuthenticatedIdentity(t *testing.T) {
	server := startControlServer(t, 2)
	client, _ := newTestDialClient(t, server)
	identity, err := client.Handshake(context.Background(), server.member())
	if err != nil {
		t.Fatalf("Handshake: %v", err)
	}
	want := WorkerIdentity{
		NodeID: 2, WorkerEpoch: server.epoch, Slots: 4,
		ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint(),
	}
	if identity != want {
		t.Fatalf("identity = %#v want %#v", identity, want)
	}
	messages := server.messages()
	if len(messages) != 1 {
		t.Fatalf("server messages = %#v", messages)
	}
	handshake, ok := messages[0].(protocol.WorkerHandshake)
	if !ok || handshake.NodeID != 1 || handshake.WorkerEpoch != (model.WorkerEpoch{0x1, 0x1}) {
		t.Fatalf("handshake = %#v", messages[0])
	}
}

func TestDialClientHandshakeRejectsForeignAck(t *testing.T) {
	server := startControlServer(t, 2)
	server.ackNodeID = 9
	client, _ := newTestDialClient(t, server)
	if _, err := client.Handshake(context.Background(), server.member()); err == nil {
		t.Fatal("foreign handshake ack accepted")
	}
}

func TestDialClientRejectsUnauthorizedEndpoint(t *testing.T) {
	server := startControlServer(t, 2)
	client, members := newTestDialClient(t, server)
	members.mu.Lock()
	members.authorizeErr = membership.ErrUnauthorized
	members.mu.Unlock()
	if err := client.Fence(context.Background(), 2, testClientEpoch()); err == nil {
		t.Fatal("unauthorized endpoint accepted")
	}
	if len(server.messages()) != 0 {
		t.Fatalf("unauthorized client still sent %#v", server.messages())
	}
}

func TestDialClientFenceRoundTrip(t *testing.T) {
	server := startControlServer(t, 2)
	epoch := testClientEpoch()
	server.setResponder(func(message protocol.WorkerMessage) protocol.WorkerMessage {
		request, ok := message.(protocol.FenceRequest)
		if !ok || request.CoordinatorEpoch != epoch {
			t.Errorf("unexpected fence request %#v", message)
			return nil
		}
		return protocol.FenceResponse{NodeID: 2, WorkerEpoch: server.epoch, CoordinatorEpoch: epoch}
	})
	client, _ := newTestDialClient(t, server)
	if err := client.Fence(context.Background(), 2, epoch); err != nil {
		t.Fatalf("Fence: %v", err)
	}
}

func TestDialClientInstallValidatesAck(t *testing.T) {
	server := startControlServer(t, 2)
	epoch := testClientEpoch()
	topology, assignment := testClientAssignment(t, 2, server.epoch)
	install := protocol.AssignmentSetInstall{
		Assignment: assignment, Specification: topology.Spec(), SpecificationDigest: topology.Digest(),
		JobControlRevision: 2, SchedulingState: model.Closed, CoordinatorEpoch: epoch,
	}
	server.setResponder(func(message protocol.WorkerMessage) protocol.WorkerMessage {
		if _, ok := message.(protocol.AssignmentSetInstall); !ok {
			t.Errorf("unexpected install request %#v", message)
			return nil
		}
		return protocol.AssignmentSetInstallAck{
			NodeID: 2, WorkerEpoch: server.epoch, JobID: assignment.JobID,
			AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest,
			JobControlRevision: 2, SchedulingState: model.Closed, CoordinatorEpoch: epoch,
		}
	})
	client, _ := newTestDialClient(t, server)
	if err := client.Install(context.Background(), 2, install); err != nil {
		t.Fatalf("Install: %v", err)
	}

	// A mismatched acknowledgment must not count as durable acceptance.
	server.setResponder(func(protocol.WorkerMessage) protocol.WorkerMessage {
		return protocol.AssignmentSetInstallAck{
			NodeID: 2, WorkerEpoch: server.epoch, JobID: assignment.JobID,
			AssignmentRevision: assignment.Revision + 1, AssignmentDigest: assignment.Digest,
			JobControlRevision: 2, SchedulingState: model.Closed, CoordinatorEpoch: epoch,
		}
	})
	if err := client.Install(context.Background(), 2, install); err == nil {
		t.Fatal("mismatched install ack accepted")
	}
}

func TestDialClientStatusAndCheckpointRoundTrip(t *testing.T) {
	server := startControlServer(t, 2)
	epoch := testClientEpoch()
	_, assignment := testClientAssignment(t, 2, server.epoch)
	notice := protocol.CheckpointNotice{
		Notice: model.CheckpointNotice{
			JobID: assignment.JobID, Source: model.TaskID{JobID: assignment.JobID, StageID: 1, Partition: 0},
			Watermark: 2, RaftIndex: 7, Epoch: epoch,
		},
		JobControlRevision: 3, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest,
	}
	server.setResponder(func(message protocol.WorkerMessage) protocol.WorkerMessage {
		switch request := message.(type) {
		case protocol.WorkerStatusRequest:
			return protocol.WorkerStatus{
				NodeID: 2, WorkerEpoch: server.epoch, CoordinatorEpoch: epoch,
				StoreTransactionID: 5, AfterTransactionID: request.AfterTransactionID,
				LastTransactionID: request.AfterTransactionID,
			}
		case protocol.CheckpointNotice:
			return protocol.CheckpointAck{
				NodeID: 2, WorkerEpoch: server.epoch, JobID: request.Notice.JobID, Source: request.Notice.Source,
				Watermark: request.Notice.Watermark, RaftIndex: request.Notice.RaftIndex,
				JobControlRevision: request.JobControlRevision, AssignmentRevision: request.AssignmentRevision,
				AssignmentDigest: request.AssignmentDigest, CoordinatorEpoch: epoch,
			}
		default:
			t.Errorf("unexpected request %#v", message)
			return nil
		}
	})
	client, _ := newTestDialClient(t, server)
	status, err := client.Status(context.Background(), 2, protocol.WorkerStatusRequest{CoordinatorEpoch: epoch, AfterTransactionID: 3, MaxEvents: 8})
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.NodeID != 2 || status.AfterTransactionID != 3 {
		t.Fatalf("status = %#v", status)
	}
	if err := client.Checkpoint(context.Background(), 2, notice); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
}

func TestDialClientSurfacesTypedWorkerRejection(t *testing.T) {
	server := startControlServer(t, 2)
	epoch := testClientEpoch()
	server.setResponder(func(message protocol.WorkerMessage) protocol.WorkerMessage {
		return protocol.WorkerError{
			NodeID: 2, WorkerEpoch: server.epoch, CoordinatorEpoch: epoch,
			RelatedMessage: message.MessageType(), Code: protocol.WorkerErrorStaleEpoch,
			Detail: []byte("stale coordinator epoch"),
		}
	})
	client, _ := newTestDialClient(t, server)
	err := client.Fence(context.Background(), 2, epoch)
	if err == nil || !strings.Contains(err.Error(), "rejected") {
		t.Fatalf("worker rejection not surfaced: %v", err)
	}
}
