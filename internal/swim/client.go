package swim

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/aadityakv/crane/internal/clock"
	"github.com/aadityakv/crane/internal/config"
	"github.com/aadityakv/crane/internal/random"
	"github.com/aadityakv/crane/internal/wire"
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
	Resolver      AddressResolver
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
	client, err := newProtocolClientWithAddressMatcher(options.Config, options.Authenticator, options.Clock, options.Random, options.IOTimeout, newAddressMatcherWithClock(options.Resolver, options.Clock))
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
	addresses     *addressMatcher
	dialContext   func(context.Context, string, string) (net.Conn, error)
}

type joinClientResult struct {
	seedID   uint16
	snapshot []Member
	floors   []Member
	accepted Member
}

type pendingSnapshot struct {
	client           *protocolClient
	stream           *wire.TCPFrameStream
	stopCancellation func()
	requestID        wire.RequestID
	senderID         uint16
	members          []Member
	floors           []Member
	closeOnce        sync.Once
}

func newProtocolClient(configuration config.NodeConfig, authenticator wire.Authenticator, sourceClock clock.Clock, source random.Source, ioTimeout time.Duration) (*protocolClient, error) {
	return newProtocolClientWithAddressMatcher(configuration, authenticator, sourceClock, source, ioTimeout, newAddressMatcher(nil))
}

func newProtocolClientWithAddressMatcher(configuration config.NodeConfig, authenticator wire.Authenticator, sourceClock clock.Clock, source random.Source, ioTimeout time.Duration, addresses *addressMatcher) (*protocolClient, error) {
	if authenticator == nil || sourceClock == nil || source == nil || configuration.NodeID == 0 {
		return nil, fmt.Errorf("%w: incomplete snapshot client dependencies", ErrInvalidServiceOptions)
	}
	if ioTimeout < 0 {
		return nil, fmt.Errorf("%w: negative snapshot I/O timeout", ErrInvalidServiceOptions)
	}
	if ioTimeout == 0 {
		ioTimeout = defaultSnapshotIOTimeout
	}
	if addresses == nil {
		return nil, fmt.Errorf("%w: address matcher is nil", ErrInvalidServiceOptions)
	}
	clusterID, err := parseClusterID(configuration.ClusterID)
	if err != nil {
		return nil, err
	}
	limits := wire.DefaultLimits()
	limits.ExpectedClusterID = &clusterID
	dialer := &net.Dialer{Timeout: ioTimeout}
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
		addresses:     addresses,
		dialContext:   dialer.DialContext,
	}, nil
}

func (c *protocolClient) snapshot(ctx context.Context, endpoint config.Endpoint) ([]Member, error) {
	pending, err := c.beginSnapshot(ctx, endpoint, 0)
	if err != nil {
		return nil, err
	}
	defer pending.close()
	return append([]Member(nil), pending.members...), nil
}

func (c *protocolClient) beginSnapshot(ctx context.Context, endpoint config.Endpoint, expectedSenderID uint16) (_ *pendingSnapshot, err error) {
	stream, _, stopCancellation, err := c.dial(ctx, endpoint)
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
	frame, err := c.readResponse(ctx, stream, requestID, wire.MessageSWIMSnapshotResponse, expectedSenderID)
	if err != nil {
		return nil, err
	}
	var response SnapshotResponse
	if err := wire.DecodeGob(frame.Payload, &response); err != nil {
		c.recordInvalidResponse(frame)
		return nil, fmt.Errorf("%w: decode snapshot response: %v", ErrSnapshotProtocol, err)
	}
	if err := validateSnapshotState(response.Members, response.Floors); err != nil {
		c.recordInvalidResponse(frame)
		return nil, err
	}
	if err := validateSnapshotResponder(frame.Header.SenderID, response.Members); err != nil {
		c.recordInvalidResponse(frame)
		return nil, err
	}
	if err := c.acceptResponse(frame); err != nil {
		return nil, err
	}
	pending.senderID = frame.Header.SenderID
	pending.members = append([]Member(nil), response.Members...)
	pending.floors = append([]Member(nil), response.Floors...)
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
	result, prepared, uncertain, err := c.joinExchange(ctx, endpoint, store, self, nil, 0)
	if !uncertain {
		return result, err
	}
	recovered, _, _, recoveryError := c.joinExchange(ctx, endpoint, nil, self, &prepared, result.seedID)
	if recoveryError != nil {
		return joinClientResult{}, errors.Join(
			fmt.Errorf("join acceptance was uncertain: %w", err),
			fmt.Errorf("recover join acceptance idempotently: %w", recoveryError),
		)
	}
	return recovered, nil
}

func (c *protocolClient) joinExchange(ctx context.Context, endpoint config.Endpoint, store IncarnationStore, self Member, retryPrepared *Member, expectedSeedID uint16) (joinClientResult, Member, bool, error) {
	stream, remoteEndpoint, stopCancellation, err := c.dial(ctx, endpoint)
	if err != nil {
		return joinClientResult{}, Member{}, false, err
	}
	defer stopCancellation()
	defer stream.Close()

	requestID := c.nextRequestID()
	if err := c.writePayload(ctx, stream, wire.MessageSWIMJoinRequest, requestID, JoinRequest{NodeID: self.NodeID}); err != nil {
		return joinClientResult{}, Member{}, false, err
	}
	frame, err := c.readResponse(ctx, stream, requestID, wire.MessageSWIMJoinSnapshot, expectedSeedID)
	if err != nil {
		return joinClientResult{}, Member{}, false, err
	}
	var snapshot JoinSnapshot
	if err := wire.DecodeGob(frame.Payload, &snapshot); err != nil {
		c.recordInvalidResponse(frame)
		return joinClientResult{}, Member{}, false, fmt.Errorf("%w: decode join snapshot: %v", ErrSnapshotProtocol, err)
	}
	if err := validateSnapshotState(snapshot.Members, snapshot.Floors); err != nil {
		c.recordInvalidResponse(frame)
		return joinClientResult{}, Member{}, false, err
	}
	if err := validateJoinResponder(ctx, c.addresses, remoteEndpoint, c.senderID, frame.Header.SenderID, snapshot.Members); err != nil {
		c.recordInvalidResponse(frame)
		return joinClientResult{}, Member{}, false, err
	}
	if err := c.acceptResponse(frame); err != nil {
		return joinClientResult{}, Member{}, false, err
	}
	partial := joinClientResult{
		seedID:   frame.Header.SenderID,
		snapshot: append([]Member(nil), snapshot.Members...),
		floors:   append([]Member(nil), snapshot.Floors...),
	}
	var prepared Member
	if retryPrepared == nil {
		prepared, err = PrepareJoinWithFloors(store, snapshot.Members, snapshot.Floors, self)
		if err != nil {
			return joinClientResult{}, Member{}, false, err
		}
	} else {
		prepared = *retryPrepared
		if prepared.NodeID != self.NodeID || prepared.Host != self.Host || prepared.BasePort != self.BasePort || prepared.Incarnation == 0 || prepared.Status != Alive {
			return joinClientResult{}, Member{}, false, fmt.Errorf("%w: invalid prepared join retry %#v", ErrSnapshotProtocol, prepared)
		}
	}

	announceID := c.nextRequestID()
	if err := c.writePayload(ctx, stream, wire.MessageSWIMJoinAnnounce, announceID, JoinAnnounce{Member: prepared}); err != nil {
		return partial, prepared, retryableJoinAcceptanceError(err), err
	}
	acceptedFrame, err := c.readResponse(ctx, stream, announceID, wire.MessageSWIMJoinAccepted, frame.Header.SenderID)
	if err != nil {
		return partial, prepared, retryableJoinAcceptanceError(err), err
	}
	if acceptedFrame.Header.SenderID != frame.Header.SenderID {
		c.recordInvalidResponse(acceptedFrame)
		return partial, prepared, false, fmt.Errorf("%w: join responder changed from %d to %d", ErrSnapshotProtocol, frame.Header.SenderID, acceptedFrame.Header.SenderID)
	}
	var accepted JoinAccepted
	if err := wire.DecodeGob(acceptedFrame.Payload, &accepted); err != nil {
		c.recordInvalidResponse(acceptedFrame)
		return partial, prepared, false, fmt.Errorf("%w: decode join acceptance: %v", ErrSnapshotProtocol, err)
	}
	if accepted.Member != prepared {
		c.recordInvalidResponse(acceptedFrame)
		return partial, prepared, false, fmt.Errorf("%w: accepted member %#v does not match announced %#v", ErrSnapshotProtocol, accepted.Member, prepared)
	}
	if err := c.acceptResponse(acceptedFrame); err != nil {
		return partial, prepared, false, err
	}
	return joinClientResult{
		seedID:   frame.Header.SenderID,
		snapshot: append([]Member(nil), snapshot.Members...),
		floors:   append([]Member(nil), snapshot.Floors...),
		accepted: accepted.Member,
	}, prepared, false, nil
}

func retryableJoinAcceptanceError(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	var networkError net.Error
	return errors.As(err, &networkError)
}

func validateJoinResponder(ctx context.Context, addresses *addressMatcher, endpoint config.Endpoint, localSenderID, senderID uint16, members []Member) error {
	if senderID == localSenderID {
		return fmt.Errorf("%w: first responder uses local node ID %d", ErrSnapshotProtocol, senderID)
	}
	responder, err := snapshotResponder(senderID, members)
	if err != nil {
		return err
	}
	advertised, err := (config.NodeConfig{AdvertiseHost: responder.Host, BasePort: responder.BasePort}).AdvertiseEndpoint(config.ServiceSWIMSnapshot)
	if err != nil {
		return fmt.Errorf("%w: derive first responder endpoint: %v", ErrSnapshotProtocol, err)
	}
	if !addresses.matchesSource(ctx, endpoint, advertised) {
		return fmt.Errorf("%w: first responder %d advertises %s, dialed %s", ErrSnapshotProtocol, senderID, advertised, endpoint)
	}
	return nil
}

func (c *protocolClient) dial(ctx context.Context, endpoint config.Endpoint) (*wire.TCPFrameStream, config.Endpoint, func(), error) {
	if ctx == nil {
		return nil, config.Endpoint{}, func() {}, errors.New("dial SWIM snapshot: nil context")
	}
	if endpoint.Host == "" || endpoint.Port == 0 {
		return nil, config.Endpoint{}, func() {}, fmt.Errorf("%w: invalid snapshot endpoint %s", ErrSnapshotProtocol, endpoint)
	}
	dialContext, cancelDial := context.WithTimeout(ctx, c.ioTimeout)
	defer cancelDial()
	connection, err := c.dialContext(dialContext, "tcp", endpoint.String())
	if err != nil {
		return nil, config.Endpoint{}, func() {}, fmt.Errorf("dial SWIM snapshot %s: %w", endpoint, err)
	}
	remote, ok := connection.RemoteAddr().(*net.TCPAddr)
	if !ok || remote.Port <= 0 || remote.Port > 65535 {
		_ = connection.Close()
		return nil, config.Endpoint{}, func() {}, fmt.Errorf("%w: invalid remote snapshot endpoint %s", ErrSnapshotProtocol, connection.RemoteAddr())
	}
	remoteEndpoint := config.Endpoint{Host: remote.IP.String(), Port: uint16(remote.Port)}
	stream := wire.NewTCPFrameStream(connection, c.authenticator, c.limits, c.ioTimeout)
	stop := context.AfterFunc(ctx, func() { _ = stream.Close() })
	return stream, remoteEndpoint, func() { _ = stop() }, nil
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

func (c *protocolClient) readResponse(ctx context.Context, stream *wire.TCPFrameStream, requestID wire.RequestID, expected wire.MessageType, expectedSenderID uint16) (wire.Frame, error) {
	frame, err := stream.ReadFrame(ctx)
	if err != nil {
		return wire.Frame{}, fmt.Errorf("read SWIM TCP response: %w", err)
	}
	if frame.Header.SenderID == 0 {
		return wire.Frame{}, fmt.Errorf("%w: uncorrelated response", ErrSnapshotProtocol)
	}
	if err := c.preflightResponse(frame); err != nil {
		return wire.Frame{}, err
	}
	if frame.Header.RequestID != requestID {
		c.recordInvalidResponse(frame)
		return wire.Frame{}, fmt.Errorf("%w: uncorrelated response", ErrSnapshotProtocol)
	}
	if expectedSenderID != 0 && frame.Header.SenderID != expectedSenderID {
		c.recordInvalidResponse(frame)
		return wire.Frame{}, fmt.Errorf("%w: responder changed from %d to %d", ErrSnapshotProtocol, expectedSenderID, frame.Header.SenderID)
	}
	if frame.Header.Message == wire.MessageSWIMError {
		var response ProtocolErrorMessage
		if err := wire.DecodeGob(frame.Payload, &response); err != nil {
			c.recordInvalidResponse(frame)
			return wire.Frame{}, fmt.Errorf("%w: decode protocol error: %v", ErrSnapshotProtocol, err)
		}
		if err := c.acceptResponse(frame); err != nil {
			return wire.Frame{}, err
		}
		return wire.Frame{}, decodeProtocolError(response)
	}
	if frame.Header.Message != expected {
		c.recordInvalidResponse(frame)
		return wire.Frame{}, fmt.Errorf("%w: got message %d, want %d", ErrSnapshotProtocol, frame.Header.Message, expected)
	}
	return frame, nil
}

func (c *protocolClient) acceptResponse(frame wire.Frame) error {
	if err := c.replay.Commit(frame.Header.SenderID, frame.Header.RequestID, time.UnixMilli(frame.Header.TimestampMillis)); err != nil {
		return fmt.Errorf("%w: response replay commit: %w", ErrSnapshotProtocol, err)
	}
	return nil
}

func (c *protocolClient) preflightResponse(frame wire.Frame) error {
	if err := c.replay.Preflight(frame.Header.SenderID, frame.Header.RequestID, time.UnixMilli(frame.Header.TimestampMillis)); err != nil {
		return fmt.Errorf("%w: response replay preflight: %w", ErrSnapshotProtocol, err)
	}
	return nil
}

func (c *protocolClient) recordInvalidResponse(frame wire.Frame) {
	c.replay.RecordInvalid(frame.Header.SenderID, frame.Header.RequestID, time.UnixMilli(frame.Header.TimestampMillis))
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

func validateSnapshotResponder(senderID uint16, members []Member) error {
	_, err := snapshotResponder(senderID, members)
	return err
}

func snapshotResponder(senderID uint16, members []Member) (Member, error) {
	for _, member := range members {
		if member.NodeID == senderID && (member.Status == Alive || member.Status == Suspect) {
			return member, nil
		}
	}
	return Member{}, fmt.Errorf("%w: responder %d is not a nonterminal snapshot member", ErrSnapshotProtocol, senderID)
}

func validateSnapshotState(members, floors []Member) error {
	if err := validateSnapshot(members); err != nil {
		return err
	}
	seen := make(map[uint16]struct{}, len(members)+len(floors))
	for _, member := range members {
		seen[member.NodeID] = struct{}{}
	}
	for _, floor := range floors {
		if floor.NodeID == 0 || floor.Incarnation == 0 || (floor.Status != Dead && floor.Status != Left) {
			return fmt.Errorf("%w: invalid incarnation floor %#v", ErrSnapshotProtocol, floor)
		}
		if _, duplicate := seen[floor.NodeID]; duplicate {
			return fmt.Errorf("%w: duplicate snapshot identity %d", ErrSnapshotProtocol, floor.NodeID)
		}
		seen[floor.NodeID] = struct{}{}
		if err := validateAdvertisedEndpoint(floor); err != nil {
			return fmt.Errorf("%w: invalid incarnation floor: %v", ErrSnapshotProtocol, err)
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
