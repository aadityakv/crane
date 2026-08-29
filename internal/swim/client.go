package swim

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/config"
	"github.com/aaditya/cs425mp3/internal/random"
	"github.com/aaditya/cs425mp3/internal/wire"
)

const (
	defaultSnapshotIOTimeout       = 5 * time.Second
	protocolErrorUnexpectedMessage = "unexpected_message"
	protocolErrorInvalidPayload    = "invalid_payload"
	protocolErrorDuplicateNodeID   = "duplicate_node_id"
	protocolErrorStaleIncarnation  = "stale_incarnation"
	protocolErrorNotAdmitted       = "not_admitted"
	protocolErrorReplay            = "replay"
	protocolErrorInternal          = "internal"
)

var (
	// ErrSnapshotProtocol reports an authenticated but invalid TCP exchange.
	ErrSnapshotProtocol = errors.New("swim: invalid snapshot protocol")
	// ErrInvalidSnapshotPayload reports an authenticated snapshot payload that cannot be decoded.
	ErrInvalidSnapshotPayload = errors.New("swim: invalid snapshot payload")
)

// SnapshotClientOptions supplies authenticated identity and deterministic
// timestamp/request-ID seams for TCP snapshot requests.
type SnapshotClientOptions struct {
	Config        config.NodeConfig
	Authenticator wire.Authenticator
	Clock         clock.Clock
	Random        random.Source
	IOTimeout     time.Duration
}

// SnapshotClient fetches authenticated membership snapshots over one owned
// TCPFrameStream per request.
type SnapshotClient struct {
	client *protocolClient
}

// NewSnapshotClient validates dependencies and returns an authenticated client.
func NewSnapshotClient(options SnapshotClientOptions) (*SnapshotClient, error) {
	if err := options.Config.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidServiceOptions, err)
	}
	client, err := newProtocolClient(options.Config, options.Authenticator, options.Clock, options.Random, options.IOTimeout)
	if err != nil {
		return nil, err
	}
	return &SnapshotClient{client: client}, nil
}

// Snapshot requests one complete copied membership view from endpoint.
func (c *SnapshotClient) Snapshot(ctx context.Context, endpoint config.Endpoint) ([]Member, error) {
	if c == nil || c.client == nil {
		return nil, fmt.Errorf("%w: nil snapshot client", ErrInvalidServiceOptions)
	}
	return c.client.snapshot(ctx, endpoint)
}

type protocolClient struct {
	clusterID     [16]byte
	senderID      uint16
	authenticator wire.Authenticator
	clock         clock.Clock
	limits        wire.Limits
	replay        *wire.ReplayGuard
	ioTimeout     time.Duration
	requestMu     sync.Mutex
	requestPrefix uint64
	requestCount  uint64
}

type joinClientResult struct {
	seedID   uint16
	snapshot []Member
	accepted Member
}

type pendingSnapshot struct {
	client           *protocolClient
	stream           *wire.TCPFrameStream
	stopCancellation func()
	requestID        wire.RequestID
	senderID         uint16
	members          []Member
	closeOnce        sync.Once
}

func newProtocolClient(configuration config.NodeConfig, authenticator wire.Authenticator, sourceClock clock.Clock, source random.Source, ioTimeout time.Duration) (*protocolClient, error) {
	if authenticator == nil || sourceClock == nil || source == nil || configuration.NodeID == 0 {
		return nil, fmt.Errorf("%w: incomplete snapshot client dependencies", ErrInvalidServiceOptions)
	}
	if ioTimeout < 0 {
		return nil, fmt.Errorf("%w: negative snapshot I/O timeout", ErrInvalidServiceOptions)
	}
	if ioTimeout == 0 {
		ioTimeout = defaultSnapshotIOTimeout
	}
	clusterID, err := parseClusterID(configuration.ClusterID)
	if err != nil {
		return nil, err
	}
	limits := wire.DefaultLimits()
	limits.ExpectedClusterID = &clusterID
	return &protocolClient{
		clusterID:     clusterID,
		senderID:      configuration.NodeID,
		authenticator: authenticator,
		clock:         sourceClock,
		limits:        limits,
		replay:        wire.NewReplayGuard(sourceClock, time.Duration(configuration.Timing.ReplayWindow), serviceFutureSkew, serviceReplayEntries),
		ioTimeout:     ioTimeout,
		requestPrefix: source.Uint64(),
		requestCount:  source.Uint64(),
	}, nil
}

func (c *protocolClient) snapshot(ctx context.Context, endpoint config.Endpoint) ([]Member, error) {
	pending, err := c.beginSnapshot(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	defer pending.close()
	return append([]Member(nil), pending.members...), nil
}

func (c *protocolClient) beginSnapshot(ctx context.Context, endpoint config.Endpoint) (_ *pendingSnapshot, err error) {
	stream, stopCancellation, err := c.dial(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	pending := &pendingSnapshot{client: c, stream: stream, stopCancellation: stopCancellation}
	defer func() {
		if err != nil {
			pending.close()
		}
	}()

	requestID := c.nextRequestID()
	pending.requestID = requestID
	if err := c.writePayload(ctx, stream, wire.MessageSWIMSnapshotRequest, requestID, SnapshotRequest{}); err != nil {
		return nil, err
	}
	frame, err := c.readResponse(ctx, stream, requestID, wire.MessageSWIMSnapshotResponse)
	if err != nil {
		return nil, err
	}
	var response SnapshotResponse
	if err := wire.DecodeGob(frame.Payload, &response); err != nil {
		return nil, fmt.Errorf("%w: decode snapshot response: %v", ErrSnapshotProtocol, err)
	}
	if err := validateSnapshot(response.Members); err != nil {
		return nil, err
	}
	if err := c.acceptResponse(frame); err != nil {
		return nil, err
	}
	pending.senderID = frame.Header.SenderID
	pending.members = append([]Member(nil), response.Members...)
	return pending, nil
}

func (p *pendingSnapshot) acknowledge(ctx context.Context) error {
	if p == nil || p.client == nil || p.stream == nil {
		return fmt.Errorf("%w: nil pending snapshot", ErrSnapshotProtocol)
	}
	return p.client.writePayload(ctx, p.stream, wire.MessageSWIMSnapshotApplied, p.requestID, SnapshotApplied{})
}

func (p *pendingSnapshot) close() {
	if p == nil {
		return
	}
	p.closeOnce.Do(func() {
		if p.stopCancellation != nil {
			p.stopCancellation()
		}
		if p.stream != nil {
			_ = p.stream.Close()
		}
	})
}

func (c *protocolClient) join(ctx context.Context, endpoint config.Endpoint, store IncarnationStore, self Member) (joinClientResult, error) {
	stream, stopCancellation, err := c.dial(ctx, endpoint)
	if err != nil {
		return joinClientResult{}, err
	}
	defer stopCancellation()
	defer stream.Close()

	requestID := c.nextRequestID()
	if err := c.writePayload(ctx, stream, wire.MessageSWIMJoinRequest, requestID, JoinRequest{NodeID: self.NodeID}); err != nil {
		return joinClientResult{}, err
	}
	frame, err := c.readResponse(ctx, stream, requestID, wire.MessageSWIMJoinSnapshot)
	if err != nil {
		return joinClientResult{}, err
	}
	var snapshot JoinSnapshot
	if err := wire.DecodeGob(frame.Payload, &snapshot); err != nil {
		return joinClientResult{}, fmt.Errorf("%w: decode join snapshot: %v", ErrSnapshotProtocol, err)
	}
	if err := validateSnapshot(snapshot.Members); err != nil {
		return joinClientResult{}, err
	}
	if err := validateJoinResponder(endpoint, frame.Header.SenderID, snapshot.Members); err != nil {
		return joinClientResult{}, err
	}
	if err := c.acceptResponse(frame); err != nil {
		return joinClientResult{}, err
	}
	prepared, err := PrepareJoin(store, snapshot.Members, self)
	if err != nil {
		return joinClientResult{}, err
	}

	announceID := c.nextRequestID()
	if err := c.writePayload(ctx, stream, wire.MessageSWIMJoinAnnounce, announceID, JoinAnnounce{Member: prepared}); err != nil {
		return joinClientResult{}, err
	}
	acceptedFrame, err := c.readResponse(ctx, stream, announceID, wire.MessageSWIMJoinAccepted)
	if err != nil {
		return joinClientResult{}, err
	}
	if acceptedFrame.Header.SenderID != frame.Header.SenderID {
		return joinClientResult{}, fmt.Errorf("%w: join responder changed from %d to %d", ErrSnapshotProtocol, frame.Header.SenderID, acceptedFrame.Header.SenderID)
	}
	var accepted JoinAccepted
	if err := wire.DecodeGob(acceptedFrame.Payload, &accepted); err != nil {
		return joinClientResult{}, fmt.Errorf("%w: decode join acceptance: %v", ErrSnapshotProtocol, err)
	}
	if accepted.Member != prepared {
		return joinClientResult{}, fmt.Errorf("%w: accepted member %#v does not match announced %#v", ErrSnapshotProtocol, accepted.Member, prepared)
	}
	if err := c.acceptResponse(acceptedFrame); err != nil {
		return joinClientResult{}, err
	}
	return joinClientResult{
		seedID:   frame.Header.SenderID,
		snapshot: append([]Member(nil), snapshot.Members...),
		accepted: accepted.Member,
	}, nil
}

func validateJoinResponder(endpoint config.Endpoint, senderID uint16, members []Member) error {
	var responder Member
	found := false
	for _, member := range members {
		if member.NodeID == senderID {
			responder, found = member, true
			break
		}
	}
	if !found || (responder.Status != Alive && responder.Status != Suspect) {
		return fmt.Errorf("%w: first responder %d is not a nonterminal snapshot member", ErrSnapshotProtocol, senderID)
	}
	advertised, err := (config.NodeConfig{AdvertiseHost: responder.Host, BasePort: responder.BasePort}).AdvertiseEndpoint(config.ServiceSWIMSnapshot)
	if err != nil {
		return fmt.Errorf("%w: derive first responder endpoint: %v", ErrSnapshotProtocol, err)
	}
	if !sameCanonicalLiteralEndpoint(advertised, endpoint) {
		return fmt.Errorf("%w: first responder %d advertises %s, dialed %s", ErrSnapshotProtocol, senderID, advertised, endpoint)
	}
	return nil
}

func sameCanonicalLiteralEndpoint(left, right config.Endpoint) bool {
	if left.Port != right.Port {
		return false
	}
	leftAddress, leftError := netip.ParseAddr(left.Host)
	rightAddress, rightError := netip.ParseAddr(right.Host)
	if leftError == nil && rightError == nil {
		return leftAddress.Unmap() == rightAddress.Unmap()
	}
	return strings.EqualFold(strings.TrimSuffix(left.Host, "."), strings.TrimSuffix(right.Host, "."))
}

func (c *protocolClient) dial(ctx context.Context, endpoint config.Endpoint) (*wire.TCPFrameStream, func(), error) {
	if ctx == nil {
		return nil, func() {}, errors.New("dial SWIM snapshot: nil context")
	}
	if endpoint.Host == "" || endpoint.Port == 0 {
		return nil, func() {}, fmt.Errorf("%w: invalid snapshot endpoint %s", ErrSnapshotProtocol, endpoint)
	}
	connection, err := (&net.Dialer{}).DialContext(ctx, "tcp", endpoint.String())
	if err != nil {
		return nil, func() {}, fmt.Errorf("dial SWIM snapshot %s: %w", endpoint, err)
	}
	stream := wire.NewTCPFrameStream(connection, c.authenticator, c.limits, c.ioTimeout)
	stop := context.AfterFunc(ctx, func() { _ = stream.Close() })
	return stream, func() { _ = stop() }, nil
}

func (c *protocolClient) writePayload(ctx context.Context, stream *wire.TCPFrameStream, message wire.MessageType, requestID wire.RequestID, value any) error {
	payload, err := wire.EncodeGob(value)
	if err != nil {
		return err
	}
	frame := wire.Frame{Header: wire.Header{
		Version:         wire.Version1,
		Message:         message,
		ClusterID:       c.clusterID,
		SenderID:        c.senderID,
		RequestID:       requestID,
		TimestampMillis: c.clock.Now().UnixMilli(),
		Codec:           wire.CodecGob,
	}, Payload: payload}
	if err := stream.WriteFrame(ctx, frame); err != nil {
		return fmt.Errorf("write SWIM TCP message %d: %w", message, err)
	}
	return nil
}

func (c *protocolClient) readResponse(ctx context.Context, stream *wire.TCPFrameStream, requestID wire.RequestID, expected wire.MessageType) (wire.Frame, error) {
	frame, err := stream.ReadFrame(ctx)
	if err != nil {
		return wire.Frame{}, fmt.Errorf("read SWIM TCP response: %w", err)
	}
	if frame.Header.SenderID == 0 || frame.Header.RequestID != requestID {
		return wire.Frame{}, fmt.Errorf("%w: uncorrelated response", ErrSnapshotProtocol)
	}
	if frame.Header.Message == wire.MessageSWIMError {
		var response ProtocolErrorMessage
		if err := wire.DecodeGob(frame.Payload, &response); err != nil {
			return wire.Frame{}, fmt.Errorf("%w: decode protocol error: %v", ErrSnapshotProtocol, err)
		}
		if err := c.acceptResponse(frame); err != nil {
			return wire.Frame{}, err
		}
		return wire.Frame{}, decodeProtocolError(response)
	}
	if frame.Header.Message != expected {
		return wire.Frame{}, fmt.Errorf("%w: got message %d, want %d", ErrSnapshotProtocol, frame.Header.Message, expected)
	}
	return frame, nil
}

func (c *protocolClient) acceptResponse(frame wire.Frame) error {
	if err := c.replay.Accept(frame.Header.SenderID, frame.Header.RequestID, time.UnixMilli(frame.Header.TimestampMillis)); err != nil {
		return fmt.Errorf("%w: response replay validation: %v", ErrSnapshotProtocol, err)
	}
	return nil
}

func (c *protocolClient) nextRequestID() wire.RequestID {
	c.requestMu.Lock()
	defer c.requestMu.Unlock()
	c.requestCount++
	if c.requestCount == 0 {
		c.requestCount++
	}
	var requestID wire.RequestID
	binary.BigEndian.PutUint64(requestID[:8], c.requestPrefix)
	binary.BigEndian.PutUint64(requestID[8:], c.requestCount)
	if requestID == (wire.RequestID{}) {
		requestID[15] = 1
	}
	return requestID
}

func validateSnapshot(members []Member) error {
	seen := make(map[uint16]struct{}, len(members))
	for _, member := range members {
		if member.NodeID == 0 || member.Incarnation == 0 || !member.Status.valid() {
			return fmt.Errorf("%w: invalid snapshot member %#v", ErrSnapshotProtocol, member)
		}
		if _, duplicate := seen[member.NodeID]; duplicate {
			return fmt.Errorf("%w: duplicate snapshot member %d", ErrSnapshotProtocol, member.NodeID)
		}
		seen[member.NodeID] = struct{}{}
		if err := validateAdvertisedEndpoint(member); err != nil {
			return fmt.Errorf("%w: %v", ErrSnapshotProtocol, err)
		}
	}
	return nil
}

func decodeProtocolError(response ProtocolErrorMessage) error {
	var classification error
	switch response.Code {
	case protocolErrorDuplicateNodeID:
		classification = ErrDuplicateNodeID
	case protocolErrorStaleIncarnation:
		classification = ErrStaleJoinIncarnation
	case protocolErrorNotAdmitted:
		classification = ErrServiceNotAdmitted
	case protocolErrorReplay:
		classification = wire.ErrReplay
	case protocolErrorInvalidPayload:
		classification = ErrInvalidSnapshotPayload
	default:
		classification = ErrSnapshotProtocol
	}
	if response.Message == "" {
		return classification
	}
	return fmt.Errorf("%s: %w", response.Message, classification)
}
