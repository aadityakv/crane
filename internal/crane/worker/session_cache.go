package worker

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/aadityakv/crane/internal/clock"
	"github.com/aadityakv/crane/internal/config"
	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
	"github.com/aadityakv/crane/internal/swim"
	"github.com/aadityakv/crane/internal/wire"
)

// controlSessionCacheOptions fixes the local identity and dependencies one
// worker-to-worker +3 session cache dials and authenticates with.
type controlSessionCacheOptions struct {
	ClusterID     [16]byte
	Authenticator wire.Authenticator
	Clock         clock.Clock
	Membership    controlMembership
	Identity      interface {
		LocalIdentity() (uint16, model.WorkerEpoch)
	}
	Timeout time.Duration
	Dial    func(context.Context, string, string) (net.Conn, error)
}

// controlSessionCache caches one authenticated +3 session per peer worker
// incarnation for high-rate peer-to-peer streams (result replication, repair
// pulls). Every handshake commits a request identity into the peer's bounded
// per-peer replay cache, so a session per record (or per retry) exhausts it
// and every later handshake is refused (Task 24 defects #8/#9): a session is
// dialed once, reused for every exchange and retry to that incarnation, and
// dropped on any transport failure.
type controlSessionCache struct {
	options controlSessionCacheOptions

	mu       sync.Mutex
	sessions map[replicaSessionKey]*replicaSession
	closed   bool
}

type replicaSessionKey struct {
	node  uint16
	epoch model.WorkerEpoch
}

type replicaSession struct {
	connection net.Conn
	stream     *wire.TCPFrameStream
	member     swim.Member
}

// controlPeerRejection is the peer's typed WorkerError answer to one
// correlated exchange: the session stayed healthy, the request was refused.
type controlPeerRejection struct {
	related   wire.MessageType
	code      protocol.WorkerErrorCode
	retryable bool
}

// Error describes the rejected message type and the peer's error code.
func (rejection *controlPeerRejection) Error() string {
	return fmt.Sprintf("remote worker rejected %d with code %d", rejection.related, rejection.code)
}

// transient reports whether the peer itself marked the refusal retryable or
// used a capacity/unavailability code, as opposed to a deterministic
// authority, epoch, assignment, or validation rejection.
func (rejection *controlPeerRejection) transient() bool {
	return rejection.retryable || rejection.code == protocol.WorkerErrorCapacity || rejection.code == protocol.WorkerErrorUnavailable
}

func newControlSessionCache(options controlSessionCacheOptions) *controlSessionCache {
	if options.Dial == nil {
		options.Dial = (&net.Dialer{}).DialContext
	}
	if options.Timeout <= 0 {
		options.Timeout = 2 * time.Second
	}
	return &controlSessionCache{options: options}
}

// session returns the cached authenticated session to one peer incarnation,
// dialing and handshaking once when absent.
func (cache *controlSessionCache) session(ctx context.Context, peer uint16, peerEpoch model.WorkerEpoch) (*replicaSession, error) {
	key := replicaSessionKey{node: peer, epoch: peerEpoch}
	cache.mu.Lock()
	if cache.closed {
		cache.mu.Unlock()
		return nil, ErrTransferUnauthorized
	}
	if cached, ok := cache.sessions[key]; ok {
		if sameActiveControlMember(cached.member, cache.options.Membership.View()) {
			cache.mu.Unlock()
			return cached, nil
		}
		delete(cache.sessions, key)
		_ = cached.connection.Close()
	}
	cache.mu.Unlock()

	member, ok := activeControlMember(cache.options.Membership.View(), peer)
	if !ok {
		return nil, ErrTransferUnauthorized
	}
	endpoint, err := memberServiceEndpoint(member.Host, member.BasePort, config.ServiceCraneWorker)
	if err != nil {
		return nil, err
	}
	dialContext, cancel := context.WithTimeout(ctx, cache.options.Timeout)
	defer cancel()
	connection, err := cache.options.Dial(dialContext, "tcp", endpoint.String())
	if err != nil {
		return nil, fmt.Errorf("dial worker control peer: %w", err)
	}
	if err := cache.options.Membership.AuthorizeTCP(peer, connection.RemoteAddr()); err != nil {
		_ = connection.Close()
		return nil, ErrTransferUnauthorized
	}
	limits := wire.DefaultLimits()
	limits.MaxFrameSize = int(model.WorkerControlMaxFrameBytesV1)
	expectedCluster := cache.options.ClusterID
	limits.ExpectedClusterID = &expectedCluster
	stream := wire.NewTCPFrameStream(connection, cache.options.Authenticator, limits, cache.options.Timeout)
	node, epoch := cache.options.Identity.LocalIdentity()
	handshake := protocol.WorkerHandshake{NodeID: node, WorkerEpoch: epoch, ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint()}
	handshakeResponse, err := cache.exchange(dialContext, stream, handshake, peer)
	if err != nil {
		_ = connection.Close()
		return nil, err
	}
	handshakeAck, ok := handshakeResponse.(protocol.WorkerHandshakeAck)
	if !ok || handshakeAck.NodeID != peer || handshakeAck.WorkerEpoch != peerEpoch || handshakeAck.ConsensusFingerprint != model.ConsensusFingerprint() || handshakeAck.RegistryFingerprint != model.RegistryFingerprint() {
		_ = connection.Close()
		return nil, ErrTransferUnauthorized
	}
	if !sameActiveControlMember(member, cache.options.Membership.View()) {
		_ = connection.Close()
		return nil, ErrTransferUnauthorized
	}
	created := &replicaSession{connection: connection, stream: stream, member: member}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.closed {
		_ = connection.Close()
		return nil, ErrTransferUnauthorized
	}
	if existing, ok := cache.sessions[key]; ok {
		// A concurrent exchange already established the session: keep one.
		_ = connection.Close()
		return existing, nil
	}
	if cache.sessions == nil {
		cache.sessions = make(map[replicaSessionKey]*replicaSession)
	}
	cache.sessions[key] = created
	return created, nil
}

// dropSession forgets and closes one failed session.
func (cache *controlSessionCache) dropSession(peer uint16, peerEpoch model.WorkerEpoch, failed *replicaSession) {
	key := replicaSessionKey{node: peer, epoch: peerEpoch}
	cache.mu.Lock()
	if cache.sessions[key] == failed {
		delete(cache.sessions, key)
	}
	cache.mu.Unlock()
	_ = failed.connection.Close()
}

// Close closes every cached session; later attempts fail closed.
func (cache *controlSessionCache) Close() {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.closed = true
	for key, session := range cache.sessions {
		_ = session.connection.Close()
		delete(cache.sessions, key)
	}
}

// exchange writes one authenticated frame and validates the correlated
// reply; a typed WorkerError reply is returned as *controlPeerRejection.
func (cache *controlSessionCache) exchange(ctx context.Context, stream *wire.TCPFrameStream, request protocol.WorkerMessage, peer uint16) (protocol.WorkerMessage, error) {
	payload, err := protocol.MarshalWorkerMessage(request)
	if err != nil {
		return nil, err
	}
	var requestID wire.RequestID
	if _, err := rand.Read(requestID[:]); err != nil {
		return nil, fmt.Errorf("generate Crane control request ID: %w", err)
	}
	node, _ := cache.options.Identity.LocalIdentity()
	frame := wire.Frame{Header: wire.Header{Version: wire.Version1, Message: request.MessageType(), ClusterID: cache.options.ClusterID, SenderID: node, RequestID: requestID, TimestampMillis: cache.options.Clock.Now().UnixMilli(), Codec: wire.CodecBinary}, Payload: payload}
	if err := stream.WriteFrame(ctx, frame); err != nil {
		return nil, err
	}
	response, err := stream.ReadFrame(ctx)
	if err != nil {
		return nil, err
	}
	if response.Header.SenderID != peer || response.Header.RequestID != requestID {
		return nil, ErrTransferUnauthorized
	}
	message, err := protocol.UnmarshalWorkerMessage(response.Header.Message, response.Payload)
	if err != nil {
		return nil, err
	}
	if workerError, ok := message.(protocol.WorkerError); ok {
		return nil, &controlPeerRejection{related: workerError.RelatedMessage, code: workerError.Code, retryable: workerError.Retryable}
	}
	return message, nil
}

// peerRejection extracts the typed peer refusal from an exchange error.
func peerRejection(err error) (*controlPeerRejection, bool) {
	var rejection *controlPeerRejection
	if errors.As(err, &rejection) {
		return rejection, true
	}
	return nil, false
}
