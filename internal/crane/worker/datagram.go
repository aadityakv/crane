package worker

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"crane/internal/clock"
	"crane/internal/config"
	"crane/internal/crane/integrationhook"
	"crane/internal/crane/membership"
	"crane/internal/crane/protocol"
	"crane/internal/swim"
	"crane/internal/transport"
	"crane/internal/wire"
)

var (
	// ErrTupleEndpointNotReady reports a +7 send attempted before its owning
	// TupleService has activated the endpoint or after that service stopped.
	ErrTupleEndpointNotReady = errors.New("crane tuple endpoint is not ready")
	// ErrInvalidTupleEndpoint reports missing, mismatched, or unsafe +7 endpoint
	// construction and ownership dependencies.
	ErrInvalidTupleEndpoint = errors.New("invalid Crane tuple endpoint")
	// ErrTuplePeerUnavailable reports an outbound destination that is not an
	// active member with a derivable +7 endpoint.
	ErrTuplePeerUnavailable = errors.New("crane tuple peer unavailable")
	// ErrTupleEndpointInUse reports an endpoint already claimed by one service.
	ErrTupleEndpointInUse = errors.New("crane tuple endpoint already in use")
)

const tupleDatagramMaximumBytes = wire.MaxCraneDatagramBytesV1

const (
	tupleReplayEntries          = 8192
	tupleReplayEntriesPerSender = 512
)

type tuplePeerAuthorizer interface {
	View() membership.View
	AuthorizeUDP(uint16, net.Addr, config.Service) error
}

type tupleReplay struct {
	mu          sync.Mutex
	clock       clock.Clock
	window      time.Duration
	futureSkew  time.Duration
	globalLimit int
	perLimit    int
	global      *wire.ReplayGuard
	perSender   map[uint16]*wire.ReplayGuard
}

func newTupleReplay(source clock.Clock, window, futureSkew time.Duration, globalLimit, perLimit int) *tupleReplay {
	return &tupleReplay{clock: source, window: window, futureSkew: futureSkew, globalLimit: globalLimit, perLimit: perLimit, global: wire.NewReplayGuard(source, window, futureSkew, globalLimit), perSender: make(map[uint16]*wire.ReplayGuard)}
}

func (replay *tupleReplay) preflight(sender uint16, request wire.RequestID, timestamp time.Time) error {
	if replay == nil || replay.global == nil {
		return wire.ErrReplayConfiguration
	}
	replay.mu.Lock()
	defer replay.mu.Unlock()
	if err := replay.global.Preflight(sender, request, timestamp); err != nil {
		return err
	}
	if guard := replay.perSender[sender]; guard != nil {
		return guard.Preflight(sender, request, timestamp)
	}
	if len(replay.perSender) >= replay.globalLimit {
		return wire.ErrReplayCacheFull
	}
	return nil
}

func (replay *tupleReplay) commit(sender uint16, request wire.RequestID, timestamp time.Time) error {
	if replay == nil {
		// An unrecorded datagram (bounded cache exhausted by an authenticated
		// peer) is processed without replay retention.
		return nil
	}
	if replay.global == nil {
		return wire.ErrReplayConfiguration
	}
	replay.mu.Lock()
	defer replay.mu.Unlock()
	if err := replay.global.Commit(sender, request, timestamp); err != nil {
		return err
	}
	guard := replay.perSender[sender]
	if guard == nil {
		guard = wire.NewReplayGuard(replay.clock, replay.window, replay.futureSkew, replay.perLimit)
		replay.perSender[sender] = guard
	}
	return guard.Commit(sender, request, timestamp)
}

func (replay *tupleReplay) preflightInvalid(sender uint16, request wire.RequestID, timestamp time.Time) error {
	if replay == nil || replay.global == nil {
		return wire.ErrReplayConfiguration
	}
	replay.mu.Lock()
	defer replay.mu.Unlock()
	return replay.preflightInvalidLocked(sender, request, timestamp)
}

func (replay *tupleReplay) preflightInvalidLocked(sender uint16, request wire.RequestID, timestamp time.Time) error {
	if err := replay.global.PreflightInvalid(sender, request, timestamp); err != nil {
		return err
	}
	if guard := replay.perSender[sender]; guard != nil {
		return guard.PreflightInvalid(sender, request, timestamp)
	}
	if len(replay.perSender) >= replay.globalLimit {
		return wire.ErrReplayCacheFull
	}
	return nil
}

func (replay *tupleReplay) commitInvalid(sender uint16, request wire.RequestID, timestamp time.Time) error {
	if replay == nil {
		return nil
	}
	if replay.global == nil {
		return wire.ErrReplayConfiguration
	}
	replay.mu.Lock()
	defer replay.mu.Unlock()
	if err := replay.preflightInvalidLocked(sender, request, timestamp); err != nil {
		return err
	}
	if err := replay.global.CommitInvalid(sender, request, timestamp); err != nil {
		return err
	}
	guard := replay.perSender[sender]
	if guard == nil {
		guard = wire.NewReplayGuard(replay.clock, replay.window, replay.futureSkew, replay.perLimit)
		replay.perSender[sender] = guard
	}
	return guard.CommitInvalid(sender, request, timestamp)
}

func (replay *tupleReplay) recordInvalid(sender uint16, request wire.RequestID, timestamp time.Time) {
	_ = replay.commitInvalid(sender, request, timestamp)
}

// TupleEndpointOptions fixes the local +7 identity, authentication, clock,
// membership authority, and optional caller-provided datagram transport.
type TupleEndpointOptions struct {
	// Config is the fully validated local node and stable port registry model.
	Config config.NodeConfig
	// Authenticator signs and verifies complete canonical +7 frames.
	Authenticator wire.Authenticator
	// Clock timestamps outbound frames and enforces inbound replay windows.
	Clock clock.Clock
	// Membership authorizes current peer identities and exact +7 endpoints.
	Membership *membership.Authorizer
	// Datagram optionally supplies one already-created deterministic +7 seam.
	Datagram transport.SourceDatagram
	// Hook optionally observes the real send/receive paths; nil selects the
	// production no-op hook, which never alters a datagram.
	Hook integrationhook.Hook
}

// TupleEndpoint is the single socket owner and Engine Sender for one node's
// bounded authenticated tuple, ACK, and NACK datagrams.
type TupleEndpoint struct {
	configuration config.NodeConfig
	clusterID     [16]byte
	authenticator wire.Authenticator
	clock         clock.Clock
	peers         tuplePeerAuthorizer
	injected      transport.SourceDatagram
	bind          config.Endpoint
	hook          integrationhook.Hook

	mu            sync.RWMutex
	datagram      transport.SourceDatagram
	requestPrefix uint64
	requestCount  uint64
	claimed       atomic.Bool
}

// NewTupleEndpoint validates and retains dependencies without opening a
// socket, starting a goroutine, or reading from the supplied datagram.
func NewTupleEndpoint(options TupleEndpointOptions) (*TupleEndpoint, error) {
	if options.Authenticator == nil || options.Clock == nil || options.Membership == nil {
		return nil, fmt.Errorf("%w: authenticator, clock, and membership are required", ErrInvalidTupleEndpoint)
	}
	if options.Config.NodeID == 0 {
		return nil, fmt.Errorf("%w: zero local node ID", ErrInvalidTupleEndpoint)
	}
	replayWindow := time.Duration(options.Config.Timing.ReplayWindow)
	if replayWindow <= 0 || replayWindow > config.MaxReplayWindow {
		return nil, fmt.Errorf("%w: invalid replay window", ErrInvalidTupleEndpoint)
	}
	clusterID, err := decodeTupleClusterID(options.Config.ClusterID)
	if err != nil {
		return nil, fmt.Errorf("%w: cluster ID: %v", ErrInvalidTupleEndpoint, err)
	}
	bind, err := options.Config.BindEndpoint(config.ServiceCraneTupleACK)
	if err != nil {
		return nil, fmt.Errorf("%w: derive +7 bind endpoint: %v", ErrInvalidTupleEndpoint, err)
	}
	if _, err = options.Config.AdvertiseEndpoint(config.ServiceCraneTupleACK); err != nil {
		return nil, fmt.Errorf("%w: derive +7 advertise endpoint: %v", ErrInvalidTupleEndpoint, err)
	}
	hook := options.Hook
	if hook == nil {
		hook = integrationhook.Noop{}
	}
	return &TupleEndpoint{
		configuration: options.Config,
		clusterID:     clusterID,
		authenticator: options.Authenticator,
		clock:         options.Clock,
		peers:         options.Membership,
		injected:      options.Datagram,
		bind:          bind,
		hook:          hook,
	}, nil
}

// Send transmits one already-durable delivery from the owned +7 source socket.
func (endpoint *TupleEndpoint) Send(ctx context.Context, delivery protocol.TupleDelivery) error {
	if endpoint == nil {
		return ErrTupleEndpointNotReady
	}
	endpoint.mu.RLock()
	datagram := endpoint.datagram
	endpoint.mu.RUnlock()
	if datagram == nil {
		return ErrTupleEndpointNotReady
	}
	if delivery.Producer.WorkerID != endpoint.configuration.NodeID {
		return fmt.Errorf("%w: producer token names node %d, local node is %d", ErrInvalidTupleEndpoint, delivery.Producer.WorkerID, endpoint.configuration.NodeID)
	}
	payload, err := protocol.MarshalTupleDelivery(delivery)
	if err != nil {
		return err
	}
	destination, err := endpoint.peerEndpoint(delivery.Destination.WorkerID)
	if err != nil {
		return err
	}
	return endpoint.sendPayload(ctx, datagram, delivery.Destination.WorkerID, destination, wire.MessageCraneTupleDelivery, payload)
}

func (endpoint *TupleEndpoint) activate() (transport.SourceDatagram, error) {
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	if endpoint.datagram != nil {
		return nil, ErrTupleEndpointInUse
	}
	datagram := endpoint.injected
	if datagram == nil {
		var err error
		datagram, err = transport.ListenUDPBounded(tupleDatagramMaximumBytes, endpoint.bind)
		if err != nil {
			return nil, fmt.Errorf("activate Crane +7 datagram: %w", err)
		}
	}
	var prefix [8]byte
	if _, err := cryptorand.Read(prefix[:]); err != nil {
		if endpoint.injected == nil {
			_ = datagram.Close()
		}
		return nil, fmt.Errorf("activate Crane +7 request IDs: %w", err)
	}
	endpoint.requestPrefix = binary.BigEndian.Uint64(prefix[:])
	if endpoint.requestPrefix == 0 {
		endpoint.requestPrefix = 1
	}
	endpoint.requestCount = 0
	endpoint.datagram = datagram
	return datagram, nil
}

func (endpoint *TupleEndpoint) deactivate(datagram transport.SourceDatagram) error {
	endpoint.mu.Lock()
	if endpoint.datagram == datagram {
		endpoint.datagram = nil
	}
	endpoint.mu.Unlock()
	return datagram.Close()
}

func (endpoint *TupleEndpoint) sendPayload(ctx context.Context, datagram transport.SourceDatagram, destinationNode uint16, destination config.Endpoint, message wire.MessageType, payload []byte) error {
	if ctx == nil {
		return errors.New("send Crane tuple datagram: nil context")
	}
	requestID, err := endpoint.nextRequestID(datagram)
	if err != nil {
		return err
	}
	frame, err := wire.Encode(wire.Header{
		Version: wire.Version1, Message: message, ClusterID: endpoint.clusterID,
		SenderID: endpoint.configuration.NodeID, RequestID: requestID,
		TimestampMillis: endpoint.clock.Now().UnixMilli(), Codec: wire.CodecBinary,
	}, payload, endpoint.authenticator, wire.Limits{ExpectedClusterID: &endpoint.clusterID})
	if err != nil {
		return err
	}
	if len(frame) > tupleDatagramMaximumBytes {
		return wire.ErrTooLarge
	}
	// The integration hook decides the fate of this exact authenticated
	// frame; every copy it allows still leaves the node's own bound +7
	// socket, so the advertised source IP/port stays authentic. A held
	// frame is re-sent later from this same socket when the hook releases it.
	if holder, ok := endpoint.hook.(integrationhook.DatagramHolder); ok {
		held := append([]byte(nil), frame...)
		if holder.HoldDatagram(message, destinationNode, func() { _ = datagram.SendFrom(context.Background(), endpoint.bind, destination, held) }) {
			return nil
		}
	}
	switch endpoint.hook.DatagramAction(integrationhook.Send, message) {
	case integrationhook.Drop:
		return nil
	case integrationhook.Duplicate:
		if err := datagram.SendFrom(ctx, endpoint.bind, destination, frame); err != nil {
			return err
		}
	}
	return datagram.SendFrom(ctx, endpoint.bind, destination, frame)
}

func (endpoint *TupleEndpoint) nextRequestID(datagram transport.SourceDatagram) (wire.RequestID, error) {
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	if endpoint.datagram == nil || endpoint.datagram != datagram {
		return wire.RequestID{}, ErrTupleEndpointNotReady
	}
	endpoint.requestCount++
	if endpoint.requestCount == 0 {
		return wire.RequestID{}, errors.New("crane tuple request identity exhausted")
	}
	var requestID wire.RequestID
	binary.BigEndian.PutUint64(requestID[:8], endpoint.requestPrefix)
	binary.BigEndian.PutUint64(requestID[8:], endpoint.requestCount)
	return requestID, nil
}

func (endpoint *TupleEndpoint) peerEndpoint(nodeID uint16) (config.Endpoint, error) {
	for _, member := range endpoint.peers.View().Members {
		if member.NodeID != nodeID || member.Status != swim.Alive && member.Status != swim.Suspect {
			continue
		}
		result, err := (config.NodeConfig{AdvertiseHost: member.Host, BasePort: member.BasePort}).AdvertiseEndpoint(config.ServiceCraneTupleACK)
		if err != nil {
			return config.Endpoint{}, fmt.Errorf("%w: node %d: %v", ErrTuplePeerUnavailable, nodeID, err)
		}
		return result, nil
	}
	return config.Endpoint{}, fmt.Errorf("%w: node %d", ErrTuplePeerUnavailable, nodeID)
}

func decodeTupleClusterID(value string) ([16]byte, error) {
	decoded, err := hex.DecodeString(strings.ReplaceAll(value, "-", ""))
	if err != nil || len(decoded) != 16 {
		return [16]byte{}, errors.New("invalid UUID")
	}
	var result [16]byte
	copy(result[:], decoded)
	return result, nil
}
