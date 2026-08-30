package raft

import (
	"container/heap"
	"context"
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/config"
	"github.com/aaditya/cs425mp3/internal/wire"
)

const (
	// TransportReplayEntries bounds accepted request identities per remote voter across reconnects.
	TransportReplayEntries = 8192
	// TransportFutureSkew is the accepted authenticated wire-clock lead.
	TransportFutureSkew = config.ReplayFutureSkewAllowance
	// TransportMaxInboundConnections bounds concurrent authenticated and unauthenticated handlers.
	TransportMaxInboundConnections = 64
	transportBackoffMinimum        = 50 * time.Millisecond
	transportBackoffMaximum        = time.Second
)

const (
	tcpTransportNew uint32 = iota
	tcpTransportStarting
	tcpTransportRunning
	tcpTransportStopped
)

// TCPDialContext opens one context-bounded connection to an exact configured voter endpoint.
type TCPDialContext func(context.Context, string, string) (net.Conn, error)

// BackoffFunc waits for one bounded reconnect delay or returns on cancellation.
type BackoffFunc func(context.Context, time.Duration) error

// RequestIDSource supplies fresh nonzero wire request identities.
type RequestIDSource interface {
	NextRequestID() (wire.RequestID, error)
}

// RPCIngress is the bounded authenticated delivery seam into the serialized Node.
type RPCIngress interface {
	SubmitRPC(context.Context, uint16, RPC) error
}

// TCPTransportOptions fixes authentication, voters, timing, allocation limits, and deterministic seams.
type TCPTransportOptions struct {
	// LocalID is the configured local voter used in every outbound header and payload.
	LocalID uint16
	// Voters is the immutable endpoint and trust boundary.
	Voters VoterSet
	// ClusterID is the exact authenticated wire cluster identity.
	ClusterID [16]byte
	// Authenticator verifies and signs canonical frames.
	Authenticator wire.Authenticator
	// Clock timestamps frames and drives per-remote replay validation.
	Clock clock.Clock
	// ReplayWindow bounds accepted request age and replay retention.
	ReplayWindow time.Duration
	// RPCTimeout bounds dial, handshake, read, and write attempts.
	RPCTimeout time.Duration
	// CodecLimits applies the configured append and snapshot allocation bounds.
	CodecLimits CodecLimits
	// RequestIDs supplies unique wire identities; nil uses cryptographic randomness.
	RequestIDs RequestIDSource
	// DialContext optionally replaces the production TCP dialer.
	DialContext TCPDialContext
	// Backoff optionally replaces cancellation-aware exponential reconnect waiting.
	Backoff BackoffFunc
}

// TCPTransport owns one bounded outbound worker per remote voter and bounded inbound handlers.
type TCPTransport struct {
	localID         uint16
	voters          VoterSet
	clusterID       [16]byte
	authenticator   wire.Authenticator
	clock           clock.Clock
	replayWindow    time.Duration
	replayRetention time.Duration
	rpcTimeout      time.Duration
	codecLimits     CodecLimits
	limits          wire.Limits
	replayGuards    map[uint16]*wire.ReplayGuard
	requestIDs      RequestIDSource
	dialContext     TCPDialContext
	backoff         BackoffFunc

	state  atomic.Uint32
	ready  chan struct{}
	done   chan struct{}
	queues map[uint16]*peerIntentQueue

	requestMu       sync.Mutex
	requestTrackers map[uint16]*outgoingRequestTracker

	connections sync.Map
	fatalMu     sync.Mutex
	firstFatal  error

	// beforeOwnerStart is a same-package observation/failure seam invoked by
	// each owner before it acknowledges startup.
	beforeOwnerStart func(context.Context, string, uint16) error
	// afterFatalRecorded is a same-package causal observation seam.
	afterFatalRecorded func()
}

// NewTCPTransport validates and owns options without opening sockets or starting goroutines.
func NewTCPTransport(options TCPTransportOptions) (*TCPTransport, error) {
	if err := options.Voters.ValidateLocalID(options.LocalID); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrTransportInvariant, err)
	}
	if options.ClusterID == ([16]byte{}) || options.Authenticator == nil || options.Clock == nil || options.ReplayWindow <= 0 || options.ReplayWindow > config.MaxReplayWindow || options.RPCTimeout <= 0 {
		return nil, fmt.Errorf("%w: invalid TCP transport options", ErrTransportInvariant)
	}
	replayRetention := options.ReplayWindow + TransportFutureSkew
	resolvedCodec, err := resolveCodecLimits(options.CodecLimits)
	if err != nil {
		return nil, err
	}
	requestIDs := options.RequestIDs
	if requestIDs == nil {
		requestIDs = cryptoRequestIDSource{}
	}
	dialContext := options.DialContext
	if dialContext == nil {
		dialer := &net.Dialer{}
		dialContext = dialer.DialContext
	}
	backoff := options.Backoff
	if backoff == nil {
		backoff = waitTransportBackoff
	}
	limits := wire.DefaultLimits()
	clusterID := options.ClusterID
	limits.ExpectedClusterID = &clusterID
	queues := make(map[uint16]*peerIntentQueue, len(options.Voters.voters)-1)
	requestTrackers := make(map[uint16]*outgoingRequestTracker, len(options.Voters.voters)-1)
	replayGuards := make(map[uint16]*wire.ReplayGuard, len(options.Voters.voters)-1)
	for _, voter := range options.Voters.Voters() {
		if voter.ID != options.LocalID {
			queues[voter.ID] = newPeerIntentQueue(PeerQueueCapacity)
			requestTrackers[voter.ID] = &outgoingRequestTracker{issued: make(map[wire.RequestID]time.Time)}
			replayGuards[voter.ID] = wire.NewReplayGuard(options.Clock, options.ReplayWindow, TransportFutureSkew, TransportReplayEntries)
		}
	}
	transport := &TCPTransport{
		localID: options.LocalID, voters: options.Voters, clusterID: clusterID,
		authenticator: options.Authenticator, clock: options.Clock, replayWindow: options.ReplayWindow, replayRetention: replayRetention,
		rpcTimeout: options.RPCTimeout, codecLimits: resolvedCodec, limits: limits,
		replayGuards: replayGuards,
		requestIDs:   requestIDs, dialContext: dialContext, backoff: backoff,
		ready: make(chan struct{}), done: make(chan struct{}), queues: queues,
		requestTrackers: requestTrackers,
	}
	return transport, nil
}

// Ready closes after the accept owner and every peer worker have started.
func (transport *TCPTransport) Ready() <-chan struct{} {
	if transport == nil {
		return nil
	}
	return transport.ready
}

// Done closes after listener, streams, handlers, and peer workers have joined.
func (transport *TCPTransport) Done() <-chan struct{} {
	if transport == nil {
		return nil
	}
	return transport.done
}

// Handoff transfers an owned RPC copy to one peer queue without blocking.
func (transport *TCPTransport) Handoff(message PeerMessage) (TransportHandoff, error) {
	if transport == nil || transport.state.Load() != tcpTransportRunning {
		return TransportUnavailable, nil
	}
	queue := transport.queues[message.To]
	if queue == nil || message.RPC == nil {
		return TransportUnavailable, fmt.Errorf("%w: invalid remote voter %d", ErrTransportInvariant, message.To)
	}
	if err := ValidateRPCSender(message.RPC, transport.localID, transport.voters); err != nil {
		return TransportUnavailable, fmt.Errorf("%w: outbound sender binding: %v", ErrTransportInvariant, err)
	}
	return queue.offer(message), nil
}

// Run owns one listener, all outbound streams, and joined bounded handlers until cancellation or fatal accept failure.
func (transport *TCPTransport) Run(ctx context.Context, listener net.Listener, ingress RPCIngress) (runErr error) {
	if transport == nil || ctx == nil || listener == nil || ingress == nil {
		return fmt.Errorf("%w: nil TCP transport Run dependency", ErrTransportInvariant)
	}
	if !transport.state.CompareAndSwap(tcpTransportNew, tcpTransportStarting) {
		return ErrTransportStopped
	}
	workerContext, cancelWorkers := context.WithCancel(ctx)
	var workers sync.WaitGroup
	fatal := make(chan error, 1)
	started := make(chan struct{}, len(transport.queues)+1)
	for peerID, queue := range transport.queues {
		voter, _ := transport.voters.Voter(peerID)
		workers.Add(1)
		go transport.runPeerWorker(workerContext, voter, queue, started, fatal, &workers)
	}
	workers.Add(1)
	go transport.acceptConnections(workerContext, listener, ingress, started, fatal, &workers)

	owners := len(transport.queues) + 1
	for acknowledged := 0; acknowledged < owners; {
		select {
		case <-started:
			acknowledged++
		case <-ctx.Done():
			goto shutdown
		case runErr = <-fatal:
			goto shutdown
		}
	}
	select {
	case runErr = <-fatal:
		goto shutdown
	default:
	}
	transport.state.Store(tcpTransportRunning)
	close(transport.ready)
	select {
	case <-ctx.Done():
	case runErr = <-fatal:
	}

shutdown:
	transport.state.Store(tcpTransportStopped)
	cancelWorkers()
	_ = listener.Close()
	transport.closeConnections()
	for _, queue := range transport.queues {
		queue.close()
	}
	workers.Wait()
	close(transport.done)
	if recorded := transport.recordedFatal(); recorded != nil {
		return recorded
	}
	return runErr
}

func (transport *TCPTransport) runPeerWorker(ctx context.Context, voter Voter, queue *peerIntentQueue, started chan<- struct{}, fatal chan<- error, workers *sync.WaitGroup) {
	defer workers.Done()
	if err := transport.acknowledgeOwner(ctx, "peer", voter.ID, started); err != nil {
		if ctx.Err() == nil {
			transport.reportFatal(ctx, fatal, fmt.Errorf("start Raft peer worker %d: %w", voter.ID, err))
		}
		return
	}
	var stream *wire.TCPFrameStream
	var connection net.Conn
	defer func() {
		if stream != nil {
			_ = stream.Close()
			transport.connections.Delete(connection)
		}
	}()
	for {
		message, ok := queue.take(ctx)
		if !ok {
			return
		}
		messageType, payload, err := EncodeRPC(message.RPC, transport.codecLimits)
		if err != nil {
			transport.reportFatal(ctx, fatal, fmt.Errorf("encode outbound Raft RPC for voter %d: %w", voter.ID, err))
			return
		}
		attempt := uint(0)
		for {
			if ctx.Err() != nil {
				return
			}
			if stream == nil {
				stream, connection, err = transport.dialPeer(ctx, voter)
				if err != nil && errors.Is(err, ErrRequestIDExhausted) {
					transport.reportFatal(ctx, fatal, err)
					return
				}
			}
			if err == nil {
				var requestID wire.RequestID
				var timestamp time.Time
				requestID, timestamp, err = transport.nextRequestIDForPeer(ctx, voter.ID)
				if err == nil {
					frame := wire.Frame{Header: transport.outboundHeaderAt(messageType, requestID, timestamp), Payload: payload}
					err = stream.WriteFrame(ctx, frame)
				}
				if errors.Is(err, ErrRequestIDExhausted) {
					transport.reportFatal(ctx, fatal, err)
					return
				}
			}
			if err == nil {
				break
			}
			if stream != nil {
				_ = stream.Close()
				transport.connections.Delete(connection)
				stream = nil
				connection = nil
			}
			if waitErr := transport.backoff(ctx, reconnectDelay(attempt)); waitErr != nil {
				return
			}
			if attempt < 63 {
				attempt++
			}
			err = nil
		}
	}
}

func (transport *TCPTransport) dialPeer(ctx context.Context, voter Voter) (*wire.TCPFrameStream, net.Conn, error) {
	attemptContext, cancelAttempt := context.WithTimeout(ctx, transport.rpcTimeout)
	defer cancelAttempt()
	connection, err := transport.dialContext(attemptContext, "tcp", voter.Endpoint.String())
	if err != nil {
		return nil, nil, err
	}
	transport.connections.Store(connection, struct{}{})
	stream := wire.NewTCPFrameStream(connection, transport.authenticator, transport.limits, transport.rpcTimeout)
	requestID, timestamp, err := transport.nextRequestIDForPeer(attemptContext, voter.ID)
	if err != nil {
		transport.connections.Delete(connection)
		_ = stream.Close()
		return nil, nil, err
	}
	messageType, payload, err := EncodeRPC(Handshake{SenderID: transport.localID, VoterFingerprint: transport.voters.Fingerprint()}, transport.codecLimits)
	if err == nil {
		err = stream.WriteFrame(attemptContext, wire.Frame{Header: transport.outboundHeaderAt(messageType, requestID, timestamp), Payload: payload})
	}
	var response wire.Frame
	if err == nil {
		response, err = stream.ReadFrame(attemptContext)
	}
	if err == nil {
		err = transport.acceptHandshakeAck(response, voter.ID, requestID)
	}
	if err != nil {
		transport.connections.Delete(connection)
		_ = stream.Close()
		return nil, nil, err
	}
	return stream, connection, nil
}

func (transport *TCPTransport) acceptHandshakeAck(frame wire.Frame, peerID uint16, requestID wire.RequestID) error {
	preflighted, err := transport.preflightFrame(frame)
	if err != nil {
		return err
	}
	invalid := func(err error) error {
		if preflighted {
			transport.recordInvalidFrame(frame)
		}
		return err
	}
	if frame.Header.RequestID != requestID || frame.Header.SenderID != peerID || frame.Header.Message != wire.MessageRaftHandshakeAck || frame.Header.Codec != wire.CodecBinary {
		return invalid(fmt.Errorf("%w: invalid handshake acknowledgement header", ErrTransportProtocol))
	}
	rpc, err := DecodeRPC(frame.Header.Message, frame.Payload, transport.codecLimits)
	if err != nil {
		return invalid(err)
	}
	if err := ValidateRPCSender(rpc, peerID, transport.voters); err != nil {
		return invalid(err)
	}
	if err := transport.commitFrame(frame); err != nil {
		return err
	}
	return nil
}

func (transport *TCPTransport) outboundHeaderAt(message wire.MessageType, requestID wire.RequestID, timestamp time.Time) wire.Header {
	return wire.Header{
		Version: wire.Version1, Message: message, ClusterID: transport.clusterID,
		SenderID: transport.localID, RequestID: requestID,
		TimestampMillis: timestamp.UnixMilli(), Codec: wire.CodecBinary,
	}
}

func (transport *TCPTransport) acceptConnections(ctx context.Context, listener net.Listener, ingress RPCIngress, started chan<- struct{}, fatal chan<- error, workers *sync.WaitGroup) {
	defer workers.Done()
	if err := transport.acknowledgeOwner(ctx, "accept", 0, started); err != nil {
		if ctx.Err() == nil {
			transport.reportFatal(ctx, fatal, fmt.Errorf("start Raft accept owner: %w", err))
		}
		return
	}
	capacity := make(chan struct{}, TransportMaxInboundConnections)
	for {
		connection, err := listener.Accept()
		if err != nil {
			if ctx.Err() == nil {
				transport.reportFatal(ctx, fatal, fmt.Errorf("accept Raft TCP: %w", err))
			}
			return
		}
		select {
		case capacity <- struct{}{}:
			transport.connections.Store(connection, struct{}{})
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer func() { <-capacity }()
				defer transport.connections.Delete(connection)
				transport.handleInboundConnection(ctx, connection, ingress)
			}()
		default:
			_ = connection.Close()
		}
	}
}

func (transport *TCPTransport) acknowledgeOwner(ctx context.Context, owner string, peerID uint16, started chan<- struct{}) error {
	if transport.beforeOwnerStart != nil {
		if err := transport.beforeOwnerStart(ctx, owner, peerID); err != nil {
			return err
		}
	}
	select {
	case started <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (transport *TCPTransport) handleInboundConnection(ctx context.Context, connection net.Conn, ingress RPCIngress) {
	stream := wire.NewTCPFrameStream(connection, transport.authenticator, transport.limits, transport.rpcTimeout)
	defer stream.Close()
	frame, err := stream.ReadFrame(ctx)
	if err != nil {
		return
	}
	rpc, ok := transport.validateInboundFrame(frame, 0, wire.MessageRaftHandshake)
	if !ok {
		return
	}
	handshake := rpc.(Handshake)
	boundID := handshake.SenderID
	ackTimestamp, err := transport.reserveCorrelatedRequestIDForPeer(boundID, frame.Header.RequestID)
	if err != nil {
		return
	}
	messageType, payload, err := EncodeRPC(HandshakeAck{ResponderID: transport.localID, VoterFingerprint: transport.voters.Fingerprint()}, transport.codecLimits)
	if err != nil {
		return
	}
	ack := wire.Frame{Header: transport.outboundHeaderAt(messageType, frame.Header.RequestID, ackTimestamp), Payload: payload}
	if err := stream.WriteFrame(ctx, ack); err != nil {
		return
	}
	for {
		frame, err = stream.ReadFrame(ctx)
		if err != nil {
			return
		}
		rpc, ok = transport.validateInboundFrame(frame, boundID, 0)
		if !ok {
			return
		}
		deliveryContext, cancelDelivery := context.WithTimeout(ctx, transport.rpcTimeout)
		err = ingress.SubmitRPC(deliveryContext, boundID, rpc)
		cancelDelivery()
		if err != nil && ctx.Err() != nil {
			return
		}
	}
}

func (transport *TCPTransport) validateInboundFrame(frame wire.Frame, boundID uint16, required wire.MessageType) (RPC, bool) {
	preflighted, err := transport.preflightFrame(frame)
	if err != nil {
		return nil, false
	}
	reject := func() (RPC, bool) {
		if preflighted {
			transport.recordInvalidFrame(frame)
		}
		return nil, false
	}
	if frame.Header.RequestID == (wire.RequestID{}) || frame.Header.Codec != wire.CodecBinary || frame.Header.SenderID == transport.localID || !transport.voters.Contains(frame.Header.SenderID) {
		return reject()
	}
	if required != 0 && frame.Header.Message != required {
		return reject()
	}
	if boundID != 0 {
		if frame.Header.SenderID != boundID || !isNodeRPCMessage(frame.Header.Message) {
			return reject()
		}
	}
	rpc, err := DecodeRPC(frame.Header.Message, frame.Payload, transport.codecLimits)
	if err != nil {
		return reject()
	}
	if err := ValidateRPCSender(rpc, frame.Header.SenderID, transport.voters); err != nil {
		return reject()
	}
	if err := transport.commitFrame(frame); err != nil {
		return nil, false
	}
	return rpc, true
}

func isNodeRPCMessage(message wire.MessageType) bool {
	return message >= wire.MessageRaftPreVoteRequest && message <= wire.MessageRaftInstallSnapshotResponse
}

func (transport *TCPTransport) preflightFrame(frame wire.Frame) (bool, error) {
	guard, err := transport.replayGuard(frame.Header.SenderID)
	if err != nil {
		return false, err
	}
	err = guard.Preflight(frame.Header.SenderID, frame.Header.RequestID, time.UnixMilli(frame.Header.TimestampMillis))
	return err == nil, err
}

func (transport *TCPTransport) commitFrame(frame wire.Frame) error {
	guard, err := transport.replayGuard(frame.Header.SenderID)
	if err != nil {
		return err
	}
	return guard.Commit(frame.Header.SenderID, frame.Header.RequestID, time.UnixMilli(frame.Header.TimestampMillis))
}

func (transport *TCPTransport) recordInvalidFrame(frame wire.Frame) {
	guard, err := transport.replayGuard(frame.Header.SenderID)
	if err != nil {
		return
	}
	guard.RecordInvalid(frame.Header.SenderID, frame.Header.RequestID, time.UnixMilli(frame.Header.TimestampMillis))
}

func (transport *TCPTransport) replayGuard(senderID uint16) (*wire.ReplayGuard, error) {
	if senderID == transport.localID || !transport.voters.Contains(senderID) {
		return nil, fmt.Errorf("%w: invalid replay sender %d", ErrTransportProtocol, senderID)
	}
	guard := transport.replayGuards[senderID]
	if guard == nil {
		return nil, fmt.Errorf("%w: missing replay guard for voter %d", ErrTransportInvariant, senderID)
	}
	return guard, nil
}

func (transport *TCPTransport) nextRequestIDForPeer(ctx context.Context, peerID uint16) (wire.RequestID, time.Time, error) {
	for {
		transport.requestMu.Lock()
		tracker := transport.requestTrackers[peerID]
		if tracker == nil {
			transport.requestMu.Unlock()
			return wire.RequestID{}, time.Time{}, fmt.Errorf("%w: invalid request-ID destination %d", ErrTransportInvariant, peerID)
		}
		timestamp := time.UnixMilli(transport.clock.Now().UnixMilli())
		tracker.purgeExpired(timestamp)
		if len(tracker.issued) < TransportReplayEntries {
			id, err := transport.requestIDs.NextRequestID()
			if err != nil || id == (wire.RequestID{}) {
				transport.requestMu.Unlock()
				return wire.RequestID{}, time.Time{}, fmt.Errorf("%w: %v", ErrRequestIDExhausted, err)
			}
			if err := tracker.record(id, timestamp, transport.replayRetention); err != nil {
				transport.requestMu.Unlock()
				return wire.RequestID{}, time.Time{}, err
			}
			transport.requestMu.Unlock()
			return id, timestamp, nil
		}
		wait := tracker.expires[0].expiresAt.Sub(timestamp)
		transport.requestMu.Unlock()

		timer := transport.clock.NewTimer(wait)
		select {
		case <-timer.C():
		case <-ctx.Done():
			timer.Stop()
			return wire.RequestID{}, time.Time{}, ctx.Err()
		}
		timer.Stop()
	}
}

func (transport *TCPTransport) reserveCorrelatedRequestIDForPeer(peerID uint16, id wire.RequestID) (time.Time, error) {
	transport.requestMu.Lock()
	defer transport.requestMu.Unlock()
	tracker := transport.requestTrackers[peerID]
	if tracker == nil {
		return time.Time{}, fmt.Errorf("%w: invalid request-ID destination %d", ErrTransportInvariant, peerID)
	}
	timestamp := time.UnixMilli(transport.clock.Now().UnixMilli())
	tracker.purgeExpired(timestamp)
	if err := tracker.record(id, timestamp, transport.replayRetention); err != nil {
		return time.Time{}, err
	}
	return timestamp, nil
}

type outgoingRequestTracker struct {
	issued  map[wire.RequestID]time.Time
	expires outgoingRequestExpiryHeap
}

func (tracker *outgoingRequestTracker) record(id wire.RequestID, timestamp time.Time, retention time.Duration) error {
	if id == (wire.RequestID{}) {
		return ErrRequestIDExhausted
	}
	if _, reused := tracker.issued[id]; reused || len(tracker.issued) >= TransportReplayEntries {
		return ErrRequestIDExhausted
	}
	expiresAt := timestamp.Add(retention)
	tracker.issued[id] = expiresAt
	heap.Push(&tracker.expires, outgoingRequestExpiry{id: id, expiresAt: expiresAt})
	return nil
}

func (tracker *outgoingRequestTracker) purgeExpired(now time.Time) {
	for tracker.expires.Len() != 0 && !tracker.expires[0].expiresAt.After(now) {
		expired := heap.Pop(&tracker.expires).(outgoingRequestExpiry)
		if expiresAt, exists := tracker.issued[expired.id]; exists && expiresAt.Equal(expired.expiresAt) {
			delete(tracker.issued, expired.id)
		}
	}
}

type outgoingRequestExpiry struct {
	id        wire.RequestID
	expiresAt time.Time
}

type outgoingRequestExpiryHeap []outgoingRequestExpiry

func (expirations outgoingRequestExpiryHeap) Len() int { return len(expirations) }
func (expirations outgoingRequestExpiryHeap) Less(left, right int) bool {
	return expirations[left].expiresAt.Before(expirations[right].expiresAt)
}
func (expirations outgoingRequestExpiryHeap) Swap(left, right int) {
	expirations[left], expirations[right] = expirations[right], expirations[left]
}
func (expirations *outgoingRequestExpiryHeap) Push(value any) {
	*expirations = append(*expirations, value.(outgoingRequestExpiry))
}
func (expirations *outgoingRequestExpiryHeap) Pop() any {
	old := *expirations
	last := len(old) - 1
	value := old[last]
	old[last] = outgoingRequestExpiry{}
	*expirations = old[:last]
	return value
}

func (transport *TCPTransport) closeConnections() {
	transport.connections.Range(func(connection, _ any) bool {
		_ = connection.(net.Conn).Close()
		return true
	})
}

func (transport *TCPTransport) reportFatal(ctx context.Context, fatal chan<- error, err error) {
	if !transport.recordFatal(err) {
		return
	}
	select {
	case fatal <- err:
		return
	default:
	}
	select {
	case fatal <- err:
	case <-ctx.Done():
	default:
	}
}

func (transport *TCPTransport) recordFatal(err error) bool {
	if err == nil {
		return false
	}
	transport.fatalMu.Lock()
	if transport.firstFatal != nil {
		transport.fatalMu.Unlock()
		return false
	}
	transport.firstFatal = err
	transport.fatalMu.Unlock()
	if transport.afterFatalRecorded != nil {
		transport.afterFatalRecorded()
	}
	return true
}

func (transport *TCPTransport) recordedFatal() error {
	transport.fatalMu.Lock()
	defer transport.fatalMu.Unlock()
	return transport.firstFatal
}

func reconnectDelay(attempt uint) time.Duration {
	if attempt >= 5 {
		return transportBackoffMaximum
	}
	delay := transportBackoffMinimum << attempt
	if delay > transportBackoffMaximum {
		return transportBackoffMaximum
	}
	return delay
}

func waitTransportBackoff(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type cryptoRequestIDSource struct{}

func (cryptoRequestIDSource) NextRequestID() (wire.RequestID, error) {
	var id wire.RequestID
	_, err := cryptorand.Read(id[:])
	return id, err
}
