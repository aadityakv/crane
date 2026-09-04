package coordinator

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/aadityakv/crane/internal/clock"
	"github.com/aadityakv/crane/internal/config"
	"github.com/aadityakv/crane/internal/crane/membership"
	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
	"github.com/aadityakv/crane/internal/swim"
	"github.com/aadityakv/crane/internal/wire"
)

var (
	// ErrWorkerUnavailable reports a worker that could not be reached or that
	// answered outside the authenticated protocol.
	ErrWorkerUnavailable = errors.New("crane worker unavailable")
	// ErrWorkerUnauthorized reports an endpoint or identity that failed the
	// membership-derived authentication checks.
	ErrWorkerUnauthorized = errors.New("crane worker unauthorized")
	// ErrWorkerRejected reports a typed worker-control rejection.
	ErrWorkerRejected = errors.New("crane worker rejected command")
)

// WorkerIdentity is the authenticated +5 handshake result for one worker.
type WorkerIdentity struct {
	NodeID               uint16            // NodeID is the worker's stable cluster identity.
	WorkerEpoch          model.WorkerEpoch // WorkerEpoch is the durable store incarnation.
	Slots                uint16            // Slots is the validated advertised slot capacity.
	ConsensusFingerprint [32]byte          // ConsensusFingerprint proves state compatibility.
	RegistryFingerprint  [32]byte          // RegistryFingerprint proves protocol compatibility.
}

// WorkerClient is the coordinator's bounded +5 worker-control surface.
type WorkerClient interface {
	// Handshake authenticates one advertised member and returns exactly the
	// worker's validated handshake response fields.
	Handshake(context.Context, swim.Member) (WorkerIdentity, error)
	// Fence installs one coordinator fence; repeating it is idempotent.
	Fence(context.Context, uint16, model.CoordinatorEpoch) error
	// Status performs one bounded cursor-based status exchange.
	Status(context.Context, uint16, protocol.WorkerStatusRequest) (protocol.WorkerStatus, error)
	// Install durably installs one complete fenced assignment set.
	Install(context.Context, uint16, protocol.AssignmentSetInstall) error
	// Checkpoint delivers one committed checkpoint notice.
	Checkpoint(context.Context, uint16, protocol.CheckpointNotice) error
}

// ClientMembership resolves and authorizes worker endpoints.
type ClientMembership interface {
	View() membership.View
	AuthorizeTCP(uint16, net.Addr) error
}

// DialWorkerClientOptions fixes the dial client's authenticated identity.
type DialWorkerClientOptions struct {
	ClusterID     [16]byte           // ClusterID scopes every authenticated frame.
	NodeID        uint16             // NodeID is the local coordinator voter identity.
	SessionEpoch  model.WorkerEpoch  // SessionEpoch identifies this process's control sessions.
	Authenticator wire.Authenticator // Authenticator signs and verifies frames.
	Clock         clock.Clock        // Clock supplies frame timestamps.
	Membership    ClientMembership   // Membership resolves advertised endpoints.
	Timeout       time.Duration      // Timeout bounds one complete exchange.
	// Dial overrides the TCP dialer; nil uses net.Dialer.
	Dial func(context.Context, string, string) (net.Conn, error)
}

// DialWorkerClient speaks the authenticated +5 protocol over one TCP
// connection per command.
type DialWorkerClient struct {
	options DialWorkerClientOptions
}

// NewDialWorkerClient validates the complete dial-client dependency set.
func NewDialWorkerClient(options DialWorkerClientOptions) (*DialWorkerClient, error) {
	if options.ClusterID == ([16]byte{}) || options.NodeID == 0 {
		return nil, errors.New("dial worker client requires cluster and node identity")
	}
	if err := options.SessionEpoch.Validate(); err != nil {
		return nil, fmt.Errorf("dial worker client session epoch: %w", err)
	}
	if options.Authenticator == nil || options.Clock == nil || options.Membership == nil {
		return nil, errors.New("dial worker client requires authenticator, clock, and membership")
	}
	if options.Timeout <= 0 {
		options.Timeout = 2 * time.Second
	}
	if options.Dial == nil {
		options.Dial = (&net.Dialer{}).DialContext
	}
	return &DialWorkerClient{options: options}, nil
}

// Handshake dials the member's advertised control endpoint and returns the
// validated handshake acknowledgment fields.
func (client *DialWorkerClient) Handshake(ctx context.Context, member swim.Member) (WorkerIdentity, error) {
	var identity WorkerIdentity
	err := client.withSession(ctx, member, func(context.Context, *wire.TCPFrameStream, WorkerIdentity) error { return nil }, &identity)
	return identity, err
}

// Fence installs one coordinator fence on the resolved worker.
func (client *DialWorkerClient) Fence(ctx context.Context, node uint16, epoch model.CoordinatorEpoch) error {
	return client.command(ctx, node, protocol.FenceRequest{CoordinatorEpoch: epoch}, func(response protocol.WorkerMessage) error {
		ack, ok := response.(protocol.FenceResponse)
		if !ok || ack.NodeID != node || ack.CoordinatorEpoch != epoch {
			return fmt.Errorf("%w: fence response mismatch", ErrWorkerUnauthorized)
		}
		return nil
	})
}

// Status performs one bounded cursor-based status exchange.
func (client *DialWorkerClient) Status(ctx context.Context, node uint16, request protocol.WorkerStatusRequest) (protocol.WorkerStatus, error) {
	var status protocol.WorkerStatus
	err := client.command(ctx, node, request, func(response protocol.WorkerMessage) error {
		report, ok := response.(protocol.WorkerStatus)
		if !ok || report.NodeID != node || report.AfterTransactionID != request.AfterTransactionID {
			return fmt.Errorf("%w: status response mismatch", ErrWorkerUnauthorized)
		}
		status = report
		return nil
	})
	return status, err
}

// Install installs one fenced assignment set and validates its durable ACK.
func (client *DialWorkerClient) Install(ctx context.Context, node uint16, install protocol.AssignmentSetInstall) error {
	return client.command(ctx, node, install, func(response protocol.WorkerMessage) error {
		ack, ok := response.(protocol.AssignmentSetInstallAck)
		if !ok || ack.NodeID != node || ack.JobID != install.Assignment.JobID ||
			ack.AssignmentRevision != install.Assignment.Revision || ack.AssignmentDigest != install.Assignment.Digest ||
			ack.JobControlRevision != install.JobControlRevision || ack.SchedulingState != install.SchedulingState ||
			ack.CoordinatorEpoch != install.CoordinatorEpoch {
			return fmt.Errorf("%w: assignment acknowledgment mismatch", ErrWorkerUnauthorized)
		}
		return nil
	})
}

// Checkpoint delivers one committed checkpoint notice and validates its ACK.
func (client *DialWorkerClient) Checkpoint(ctx context.Context, node uint16, notice protocol.CheckpointNotice) error {
	return client.command(ctx, node, notice, func(response protocol.WorkerMessage) error {
		ack, ok := response.(protocol.CheckpointAck)
		if !ok || ack.NodeID != node || ack.JobID != notice.Notice.JobID || ack.Source != notice.Notice.Source ||
			ack.Watermark != notice.Notice.Watermark || ack.RaftIndex != notice.Notice.RaftIndex ||
			ack.JobControlRevision != notice.JobControlRevision || ack.AssignmentRevision != notice.AssignmentRevision ||
			ack.AssignmentDigest != notice.AssignmentDigest || ack.CoordinatorEpoch != notice.Notice.Epoch {
			return fmt.Errorf("%w: checkpoint acknowledgment mismatch", ErrWorkerUnauthorized)
		}
		return nil
	})
}

// Fetch performs one authenticated +5 leader result-fetch exchange and
// returns the source-correlated chunk; the caller validates artifact
// identity, contiguity, and checksums exactly as the terminal drive requires.
func (client *DialWorkerClient) Fetch(ctx context.Context, node uint16, request protocol.ResultFetchRequest) (protocol.ResultFetchChunk, error) {
	var chunk protocol.ResultFetchChunk
	err := client.command(ctx, node, request, func(response protocol.WorkerMessage) error {
		result, ok := response.(protocol.ResultFetchChunk)
		if !ok || result.SourceNodeID != node {
			return fmt.Errorf("%w: result fetch response mismatch", ErrWorkerUnauthorized)
		}
		chunk = result
		return nil
	})
	return chunk, err
}

// ReceiveArtifact durably installs one sealed artifact chunk on the named
// worker and returns its acknowledged installation progress; the caller
// validates the complete ACK correlation.
func (client *DialWorkerClient) ReceiveArtifact(ctx context.Context, node uint16, chunk protocol.ResultArtifactChunk) (protocol.ResultArtifactAck, error) {
	var ack protocol.ResultArtifactAck
	err := client.command(ctx, node, chunk, func(response protocol.WorkerMessage) error {
		result, ok := response.(protocol.ResultArtifactAck)
		if !ok || result.NodeID != node {
			return fmt.Errorf("%w: result artifact acknowledgment mismatch", ErrWorkerUnauthorized)
		}
		ack = result
		return nil
	})
	return ack, err
}

// The production dial client carries the complete terminal-results surface.
var _ ResultTransferClient = (*DialWorkerClient)(nil)

// command resolves the target member, opens one authenticated session, and
// performs exactly one request/response exchange validated by check.
func (client *DialWorkerClient) command(ctx context.Context, node uint16, request protocol.WorkerMessage, check func(protocol.WorkerMessage) error) error {
	member, ok := findMember(client.options.Membership.View(), node)
	if !ok || !activeMemberStatus(member.Status) {
		return fmt.Errorf("%w: node %d is not an active member", ErrWorkerUnavailable, node)
	}
	return client.withSession(ctx, member, func(sessionContext context.Context, stream *wire.TCPFrameStream, _ WorkerIdentity) error {
		response, err := client.exchange(sessionContext, stream, request, node)
		if err != nil {
			return err
		}
		return check(response)
	}, nil)
}

// withSession dials, authorizes, and handshakes one connection, then invokes
// operate on the authenticated stream.
func (client *DialWorkerClient) withSession(ctx context.Context, member swim.Member, operate func(context.Context, *wire.TCPFrameStream, WorkerIdentity) error, identityOut *WorkerIdentity) error {
	if ctx == nil {
		return errors.New("nil worker client context")
	}
	endpoint, err := workerControlEndpoint(member)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrWorkerUnavailable, err)
	}
	sessionContext, cancel := context.WithTimeout(ctx, client.options.Timeout)
	defer cancel()
	connection, err := client.options.Dial(sessionContext, "tcp", endpoint.String())
	if err != nil {
		return fmt.Errorf("%w: dial %s: %v", ErrWorkerUnavailable, endpoint.String(), err)
	}
	defer connection.Close()
	if err := client.options.Membership.AuthorizeTCP(member.NodeID, connection.RemoteAddr()); err != nil {
		return fmt.Errorf("%w: %v", ErrWorkerUnauthorized, err)
	}
	limits := wire.DefaultLimits()
	limits.MaxFrameSize = int(model.WorkerControlMaxFrameBytesV1)
	expectedCluster := client.options.ClusterID
	limits.ExpectedClusterID = &expectedCluster
	stream := wire.NewTCPFrameStream(connection, client.options.Authenticator, limits, client.options.Timeout)

	handshake := protocol.WorkerHandshake{
		NodeID: client.options.NodeID, WorkerEpoch: client.options.SessionEpoch,
		ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint(),
	}
	response, err := client.exchange(sessionContext, stream, handshake, member.NodeID)
	if err != nil {
		return err
	}
	ack, ok := response.(protocol.WorkerHandshakeAck)
	if !ok || ack.NodeID != member.NodeID {
		return fmt.Errorf("%w: handshake acknowledgment mismatch", ErrWorkerUnauthorized)
	}
	if ack.ConsensusFingerprint == ([32]byte{}) || ack.ConsensusFingerprint != model.ConsensusFingerprint() {
		return fmt.Errorf("%w: consensus fingerprint mismatch", ErrWorkerUnauthorized)
	}
	if ack.RegistryFingerprint != model.RegistryFingerprint() {
		return fmt.Errorf("%w: registry fingerprint mismatch", ErrWorkerUnauthorized)
	}
	identity := WorkerIdentity{
		NodeID: ack.NodeID, WorkerEpoch: ack.WorkerEpoch, Slots: ack.SlotCapacity,
		ConsensusFingerprint: ack.ConsensusFingerprint, RegistryFingerprint: ack.RegistryFingerprint,
	}
	if identityOut != nil {
		*identityOut = identity
	}
	return operate(sessionContext, stream, identity)
}

// exchange writes one authenticated frame and validates the correlated reply.
func (client *DialWorkerClient) exchange(ctx context.Context, stream *wire.TCPFrameStream, request protocol.WorkerMessage, destination uint16) (protocol.WorkerMessage, error) {
	payload, err := protocol.MarshalWorkerMessage(request)
	if err != nil {
		return nil, err
	}
	var requestID wire.RequestID
	if _, err := rand.Read(requestID[:]); err != nil {
		return nil, fmt.Errorf("generate worker control request ID: %w", err)
	}
	frame := wire.Frame{Header: wire.Header{
		Version: wire.Version1, Message: request.MessageType(), ClusterID: client.options.ClusterID,
		SenderID: client.options.NodeID, RequestID: requestID,
		TimestampMillis: client.options.Clock.Now().UnixMilli(), Codec: wire.CodecBinary,
	}, Payload: payload}
	if err := stream.WriteFrame(ctx, frame); err != nil {
		return nil, fmt.Errorf("%w: write frame: %v", ErrWorkerUnavailable, err)
	}
	response, err := stream.ReadFrame(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: read frame: %v", ErrWorkerUnavailable, err)
	}
	if response.Header.SenderID != destination || response.Header.RequestID != requestID {
		return nil, fmt.Errorf("%w: uncorrelated response", ErrWorkerUnauthorized)
	}
	message, err := protocol.UnmarshalWorkerMessage(response.Header.Message, response.Payload)
	if err != nil {
		return nil, fmt.Errorf("%w: decode response: %v", ErrWorkerUnavailable, err)
	}
	if workerError, ok := message.(protocol.WorkerError); ok {
		return nil, fmt.Errorf("%w: message %d rejected with code %d", ErrWorkerRejected, workerError.RelatedMessage, workerError.Code)
	}
	return message, nil
}

// workerControlEndpoint derives the advertised +5 control endpoint.
func workerControlEndpoint(member swim.Member) (config.Endpoint, error) {
	specification, ok := config.LookupService(config.ServiceCraneWorker)
	if !ok || uint32(member.BasePort)+uint32(specification.Offset) > 65535 {
		return config.Endpoint{}, errors.New("member cannot derive the worker control endpoint")
	}
	return config.CanonicalEndpoint(config.Endpoint{Host: member.Host, Port: member.BasePort + specification.Offset})
}
