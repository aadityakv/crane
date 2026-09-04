package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/aadityakv/crane/internal/clock"
	"github.com/aadityakv/crane/internal/config"
	"github.com/aadityakv/crane/internal/crane/admission"
	"github.com/aadityakv/crane/internal/crane/membership"
	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
	"github.com/aadityakv/crane/internal/crane/store"
	"github.com/aadityakv/crane/internal/swim"
	"github.com/aadityakv/crane/internal/wire"
)

const (
	// DefaultMaxControlSessions bounds the concurrent inbound control sessions a worker accepts.
	DefaultMaxControlSessions = 128
	// DefaultMaxControlSessionsPerPeer bounds the concurrent control sessions accepted from one peer.
	DefaultMaxControlSessionsPerPeer = 4
	// DefaultMaxQueuedControlWork bounds the control requests waiting for the store.
	DefaultMaxQueuedControlWork = 256
	// DefaultMaxControlReplayEntries bounds the replay-protection entries retained across all peers.
	DefaultMaxControlReplayEntries = 65536
	// DefaultMaxControlReplayEntriesPerPeer bounds the replay-protection entries retained for one peer.
	DefaultMaxControlReplayEntriesPerPeer = 8192
)

var (
	// ErrControlHandshakeRequired reports a control message that arrived before the session handshake.
	ErrControlHandshakeRequired = errors.New("crane worker control handshake required")
	// ErrControlUnauthorized reports a control message from a peer that is not allowed to send it.
	ErrControlUnauthorized = errors.New("crane worker control unauthorized")
	// ErrControlStaleEpoch reports a control message fenced by a superseded coordinator or worker epoch.
	ErrControlStaleEpoch = errors.New("crane worker control stale epoch")
	// ErrControlStaleAssignment reports a control message bound to an assignment revision the worker no longer holds.
	ErrControlStaleAssignment = errors.New("crane worker control stale assignment")
	// ErrControlCapacity reports that a session or queue limit prevented accepting the request.
	ErrControlCapacity = errors.New("crane worker control capacity exhausted")
	// ErrControlClosed reports an operation attempted on a session that has already closed.
	ErrControlClosed = errors.New("crane worker control session closed")
)

type controlRepository interface {
	LocalIdentity() (uint16, model.WorkerEpoch)
	RecoverWork() (store.RecoveredWork, error)
	// RecoverWorkBounded is RecoverWork bounded by the control wait: a stalled
	// store answers store.ErrBusy (retryable) instead of blocking the handler.
	RecoverWorkBounded() (store.RecoveredWork, error)
	DurableTransactionID() (uint64, error)
	Fence(model.CoordinatorEpoch) error
	InstallAssignment(model.AssignmentSet, model.TopologySpec, uint64, model.SchedulingState, model.CoordinatorEpoch) error
	PendingEvents(uint64, uint16) ([]model.WorkerEvent, uint64, bool, error)
	UpsertRepair(store.ResultRepairRecord) error
	ObserveCheckpoint(protocol.CheckpointNotice) error
}

type controlEngine interface {
	ReconcileAssignment(context.Context, model.JobID) error
	ObserveFence(context.Context) error
	ApplyCheckpoint(context.Context, protocol.CheckpointNotice) error
	AcknowledgeEvents(context.Context, uint64) error
}

type controlTransfer interface {
	ReceiveResultRecord(context.Context, TransferPeer, protocol.ResultRecordChunk) (protocol.ResultRecordAck, error)
	ReceiveResultArtifact(context.Context, TransferPeer, protocol.ResultArtifactChunk) (protocol.ResultArtifactAck, error)
	OpenResultFetch(context.Context, TransferPeer, protocol.ResultFetchRequest) (protocol.ResultFetchChunk, error)
	ServeRepairPull(context.Context, TransferPeer, protocol.ResultRepairStatus) (protocol.ResultRecordChunk, bool, error)
}

// RepairScheduler receives durably installed destination-role repair grants
// for destination-driven streaming.
type RepairScheduler interface {
	Schedule(store.ResultRepairRecord)
}

type controlMembership interface {
	View() membership.View
	AuthorizeTCP(uint16, net.Addr) error
}

// ControlOptions fixes the transport-independent +3 command owner's bounded
// dependencies. Construction opens no file or socket and starts no goroutine.
type ControlOptions struct {
	Config                  config.NodeConfig
	ClusterID               [16]byte
	Repository              controlRepository
	Engine                  controlEngine
	Transfer                controlTransfer
	RepairScheduler         RepairScheduler
	Gate                    *admission.Gate
	Membership              controlMembership
	Clock                   clock.Clock
	MaxSessions             int
	MaxSessionsPerPeer      int
	MaxQueuedWork           int
	MaxReplayEntries        int
	MaxReplayEntriesPerPeer int
}

// ControlOwner serializes authority-changing worker commands and owns a
// bounded set of connection-scoped authenticated sessions.
type ControlOwner struct {
	configuration config.NodeConfig
	clusterID     [16]byte
	repository    controlRepository
	engine        controlEngine
	transfer      controlTransfer
	scheduler     RepairScheduler
	gate          *admission.Gate
	membership    controlMembership
	replay        *controlReplay
	maxSessions   int
	maxPerPeer    int
	work          chan struct{}

	mu         sync.Mutex
	sessions   map[*ControlSession]struct{}
	perPeer    map[controlPeer]int
	closed     bool
	mutations  sync.Mutex
	localNode  uint16
	localEpoch model.WorkerEpoch
	// beforeMutation is a deterministic test seam invoked at the final
	// serialized boundary; production owners leave it nil.
	beforeMutation func(string)
}

type controlPeer struct {
	node  uint16
	epoch model.WorkerEpoch
}

// ControlSession binds one TCP remote endpoint to exactly one handshake.
type ControlSession struct {
	owner  *ControlOwner
	remote net.Addr
	close  func() error
	done   context.Context
	cancel context.CancelFunc

	mu            sync.Mutex
	authenticated bool
	closing       bool
	closed        bool
	peer          controlPeer
	member        swim.Member
	lastCommand   model.CoordinatorEpoch
	lifecycle     sync.RWMutex
	closeOnce     sync.Once
}

// NewControlOwner validates positive resource limits and exact shared
// dependencies without recovering durable state or starting work.
func NewControlOwner(options ControlOptions) (*ControlOwner, error) {
	if options.Config.NodeID == 0 || options.Config.Crane.WorkerSlots == 0 || uint64(options.Config.Crane.WorkerSlots) > model.LimitsV1().MaxWorkerSlots {
		return nil, errors.New("invalid local worker control identity or slots")
	}
	if options.ClusterID == ([16]byte{}) || options.Repository == nil || options.Engine == nil || options.Transfer == nil || options.Gate == nil || options.Membership == nil || options.Clock == nil {
		return nil, errors.New("worker control requires cluster, repository, engine, transfer owner, shared gate, membership, and clock")
	}
	if options.MaxSessions == 0 {
		options.MaxSessions = DefaultMaxControlSessions
	}
	if options.MaxSessionsPerPeer == 0 {
		options.MaxSessionsPerPeer = DefaultMaxControlSessionsPerPeer
	}
	if options.MaxQueuedWork == 0 {
		options.MaxQueuedWork = DefaultMaxQueuedControlWork
	}
	if options.MaxReplayEntries == 0 {
		options.MaxReplayEntries = DefaultMaxControlReplayEntries
	}
	if options.MaxReplayEntriesPerPeer == 0 {
		options.MaxReplayEntriesPerPeer = DefaultMaxControlReplayEntriesPerPeer
	}
	if options.MaxSessions < 1 || options.MaxSessionsPerPeer < 1 || options.MaxSessionsPerPeer > options.MaxSessions || options.MaxQueuedWork < 1 || options.MaxReplayEntries < 1 || options.MaxReplayEntriesPerPeer < 1 || options.MaxReplayEntriesPerPeer > options.MaxReplayEntries {
		return nil, errors.New("invalid worker control resource limits")
	}
	window := time.Duration(options.Config.Timing.ReplayWindow)
	if window <= 0 || window > config.MaxReplayWindow {
		return nil, errors.New("invalid worker control replay window")
	}
	node, epoch := options.Repository.LocalIdentity()
	if node != options.Config.NodeID || epoch.Validate() != nil {
		return nil, errors.New("worker control repository identity mismatch")
	}
	configuration := cloneWorkerNodeConfig(options.Config)
	return &ControlOwner{
		configuration: configuration, clusterID: options.ClusterID, repository: options.Repository, engine: options.Engine,
		transfer: options.Transfer, scheduler: options.RepairScheduler, gate: options.Gate, membership: options.Membership,
		replay:      newControlReplay(options.Clock, window, config.ReplayFutureSkewAllowance, options.MaxReplayEntries, options.MaxReplayEntriesPerPeer),
		maxSessions: options.MaxSessions, maxPerPeer: options.MaxSessionsPerPeer, work: make(chan struct{}, options.MaxQueuedWork),
		sessions: make(map[*ControlSession]struct{}), perPeer: make(map[controlPeer]int), localNode: node, localEpoch: epoch,
	}, nil
}

func cloneWorkerNodeConfig(configuration config.NodeConfig) config.NodeConfig {
	configuration.RaftVoters = append([]config.RaftVoter(nil), configuration.RaftVoters...)
	return configuration
}

// NewSession reserves one bounded connection-scoped session. Per-peer capacity
// is reserved only after an exact authenticated handshake identifies the peer.
func (owner *ControlOwner) NewSession(remote net.Addr, closeConnection func() error) (*ControlSession, error) {
	if owner == nil || remote == nil || closeConnection == nil {
		return nil, errors.New("invalid worker control session")
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.closed {
		return nil, ErrControlClosed
	}
	if len(owner.sessions) >= owner.maxSessions {
		return nil, ErrControlCapacity
	}
	done, cancel := context.WithCancel(context.Background())
	session := &ControlSession{owner: owner, remote: remote, close: closeConnection, done: done, cancel: cancel}
	owner.sessions[session] = struct{}{}
	return session, nil
}

// Close invalidates the session, releases its exact capacity, and closes its
// connection once.
func (session *ControlSession) Close() error {
	if session == nil {
		return nil
	}
	owner := session.owner
	session.mu.Lock()
	if !session.closed {
		session.closing = true
	}
	session.mu.Unlock()
	session.cancel()
	session.lifecycle.Lock()
	defer session.lifecycle.Unlock()
	owner.mu.Lock()
	session.mu.Lock()
	if !session.closed {
		session.closed = true
		delete(owner.sessions, session)
		if session.authenticated {
			owner.perPeer[session.peer]--
			if owner.perPeer[session.peer] == 0 {
				delete(owner.perPeer, session.peer)
			}
		}
	}
	session.mu.Unlock()
	owner.mu.Unlock()
	var result error
	session.closeOnce.Do(func() { result = session.close() })
	return result
}

// Close invalidates and closes every owned session.
func (owner *ControlOwner) Close() error {
	if owner == nil {
		return nil
	}
	owner.mu.Lock()
	owner.closed = true
	sessions := make([]*ControlSession, 0, len(owner.sessions))
	for session := range owner.sessions {
		sessions = append(sessions, session)
	}
	owner.mu.Unlock()
	var result error
	for _, session := range sessions {
		result = errors.Join(result, session.Close())
	}
	return result
}

// Handle validates one already-authenticated wire frame, performs one bounded
// command, and returns an owned typed response.
func (session *ControlSession) Handle(ctx context.Context, frame wire.Frame) (protocol.WorkerMessage, error) {
	if ctx == nil {
		return nil, errors.New("nil worker control context")
	}
	operationContext, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(session.done, cancel)
	defer func() {
		stop()
		cancel()
	}()
	if err := session.owner.acquireWork(operationContext); err != nil {
		return nil, err
	}
	defer session.owner.releaseWork()
	if err := session.basicFrame(frame); err != nil {
		return nil, err
	}
	timestamp := time.UnixMilli(frame.Header.TimestampMillis)
	if err := session.owner.replay.preflight(frame.Header.SenderID, frame.Header.RequestID, timestamp); err != nil {
		return nil, err
	}
	message, err := protocol.UnmarshalWorkerMessage(frame.Header.Message, frame.Payload)
	if err != nil {
		session.owner.replay.recordInvalid(frame.Header.SenderID, frame.Header.RequestID, timestamp)
		return nil, fmt.Errorf("decode worker control request: %w", err)
	}

	session.mu.Lock()
	authenticated := session.authenticated
	closed := session.closing || session.closed
	session.mu.Unlock()
	if closed {
		return nil, ErrControlClosed
	}
	if !authenticated {
		handshake, ok := message.(protocol.WorkerHandshake)
		if !ok {
			session.owner.replay.recordInvalid(frame.Header.SenderID, frame.Header.RequestID, timestamp)
			return nil, ErrControlHandshakeRequired
		}
		if err := session.authenticate(frame.Header.SenderID, handshake); err != nil {
			session.owner.replay.recordInvalid(frame.Header.SenderID, frame.Header.RequestID, timestamp)
			return nil, err
		}
		if err := session.owner.replay.commit(frame.Header.SenderID, frame.Header.RequestID, timestamp); err != nil {
			_ = session.Close()
			return nil, err
		}
		if !controlMemberActive(session.owner.membership.View(), session.owner.localNode) {
			_ = session.Close()
			return nil, ErrControlUnauthorized
		}
		return protocol.WorkerHandshakeAck{NodeID: session.owner.localNode, WorkerEpoch: session.owner.localEpoch, SlotCapacity: session.owner.configuration.Crane.WorkerSlots, ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint()}, nil
	}
	if _, duplicate := message.(protocol.WorkerHandshake); duplicate {
		session.owner.replay.recordInvalid(frame.Header.SenderID, frame.Header.RequestID, timestamp)
		return nil, ErrControlUnauthorized
	}
	if err := session.revalidate(frame.Header.SenderID); err != nil {
		session.owner.replay.recordInvalid(frame.Header.SenderID, frame.Header.RequestID, timestamp)
		_ = session.Close()
		return nil, err
	}
	if err := session.owner.replay.commit(frame.Header.SenderID, frame.Header.RequestID, timestamp); err != nil {
		return nil, err
	}
	session.mu.Lock()
	peer := session.peer
	session.mu.Unlock()
	response, err := session.owner.dispatch(operationContext, session, peer, message)
	if err != nil {
		return nil, err
	}
	if !controlMemberActive(session.owner.membership.View(), session.owner.localNode) {
		return nil, ErrControlUnauthorized
	}
	return response, nil
}

func (session *ControlSession) basicFrame(frame wire.Frame) error {
	if frame.Header.Version != wire.Version1 || frame.Header.Codec != wire.CodecBinary || frame.Header.ClusterID != session.owner.clusterID || frame.Header.SenderID == 0 || frame.Header.RequestID == (wire.RequestID{}) || frame.Header.Message < wire.MessageCraneWorkerHandshake || frame.Header.Message > wire.MessageCraneWorkerError {
		return ErrControlUnauthorized
	}
	return nil
}

func (session *ControlSession) authenticate(sender uint16, handshake protocol.WorkerHandshake) error {
	if handshake.NodeID != sender || handshake.WorkerEpoch.Validate() != nil || handshake.ConsensusFingerprint != model.ConsensusFingerprint() || handshake.RegistryFingerprint != model.RegistryFingerprint() {
		return ErrControlUnauthorized
	}
	member, ok := activeControlMember(session.owner.membership.View(), sender)
	if !ok || session.owner.membership.AuthorizeTCP(sender, session.remote) != nil {
		return ErrControlUnauthorized
	}
	peer := controlPeer{node: sender, epoch: handshake.WorkerEpoch}
	session.owner.mu.Lock()
	defer session.owner.mu.Unlock()
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closing || session.closed || session.owner.closed {
		return ErrControlClosed
	}
	if session.authenticated {
		return ErrControlUnauthorized
	}
	if session.owner.perPeer[peer] >= session.owner.maxPerPeer {
		return ErrControlCapacity
	}
	session.owner.perPeer[peer]++
	session.authenticated = true
	session.peer = peer
	session.member = member
	return nil
}

func (session *ControlSession) revalidate(sender uint16) error {
	session.mu.Lock()
	peer, prior, closed, authenticated := session.peer, session.member, session.closing || session.closed, session.authenticated
	session.mu.Unlock()
	if closed || !authenticated {
		return ErrControlClosed
	}
	if sender != peer.node || session.owner.membership.AuthorizeTCP(sender, session.remote) != nil {
		return ErrControlUnauthorized
	}
	current, ok := activeControlMember(session.owner.membership.View(), sender)
	if !ok || current.NodeID != prior.NodeID || current.Host != prior.Host || current.BasePort != prior.BasePort || current.Incarnation != prior.Incarnation {
		return ErrControlUnauthorized
	}
	return nil
}

func activeControlMember(view membership.View, node uint16) (swim.Member, bool) {
	for _, member := range view.Members {
		if member.NodeID == node && (member.Status == swim.Alive || member.Status == swim.Suspect) {
			return member, true
		}
	}
	return swim.Member{}, false
}

func (owner *ControlOwner) acquireWork(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case owner.work <- struct{}{}:
		return nil
	default:
		return ErrControlCapacity
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (owner *ControlOwner) releaseWork() { <-owner.work }

func (owner *ControlOwner) dispatch(ctx context.Context, session *ControlSession, peer controlPeer, message protocol.WorkerMessage) (protocol.WorkerMessage, error) {
	owner.mutations.Lock()
	defer owner.mutations.Unlock()
	session.lifecycle.RLock()
	defer session.lifecycle.RUnlock()
	if err := session.revalidate(peer.node); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var response protocol.WorkerMessage
	var err error
	switch request := message.(type) {
	case protocol.FenceRequest:
		response, err = owner.handleFence(ctx, session, peer, request)
	case protocol.AssignmentSetInstall:
		response, err = owner.handleAssignment(ctx, session, peer, request)
	case protocol.WorkerStatusRequest:
		response, err = owner.handleStatus(ctx, session, peer, request)
	case protocol.CheckpointNotice:
		response, err = owner.handleCheckpoint(ctx, session, peer, request)
	case protocol.ResultRecordChunk:
		role := TransferNormalReplication
		if request.RepairID != ([16]byte{}) || request.RepairInstructionDigest != ([32]byte{}) {
			role = TransferHistoricalRepair
		}
		response, err = owner.handleResultRecord(ctx, session, peer, role, request)
	case protocol.ResultArtifactChunk:
		if err = owner.finalCurrentSession(ctx, session, peer, "result-artifact", request.CoordinatorEpoch); err == nil {
			response, err = owner.transfer.ReceiveResultArtifact(ctx, TransferPeer{NodeID: peer.node, WorkerEpoch: peer.epoch, Role: TransferNormalReplication}, request)
		}
	case protocol.ResultFetchRequest:
		if err = owner.finalMutation(ctx, session, peer, "result-fetch", request.CoordinatorEpoch, true); err == nil {
			response, err = owner.transfer.OpenResultFetch(ctx, TransferPeer{NodeID: peer.node, WorkerEpoch: peer.epoch, Role: TransferLeaderFetch}, request)
		}
	case protocol.WorkerStatus:
		response, err = owner.handleRepairPull(ctx, session, peer, request)
	case protocol.WorkerRegisterRequest, protocol.WorkerRegisterResponse, protocol.ResultRecordAck, protocol.ResultArtifactAck, protocol.ResultFetchChunk, protocol.WorkerError, protocol.WorkerHandshakeAck, protocol.FenceResponse, protocol.AssignmentSetInstallAck, protocol.CheckpointAck:
		return nil, protocol.ErrUnexpectedWorkerMessage
	default:
		return nil, protocol.ErrUnexpectedWorkerMessage
	}
	if err != nil {
		return nil, err
	}
	if generation, ok := controlMessageGeneration(message); ok {
		session.mu.Lock()
		if !session.closed {
			session.lastCommand = generation
		}
		session.mu.Unlock()
	}
	return response, nil
}

func controlMessageGeneration(message protocol.WorkerMessage) (model.CoordinatorEpoch, bool) {
	switch request := message.(type) {
	case protocol.FenceRequest:
		return request.CoordinatorEpoch, true
	case protocol.AssignmentSetInstall:
		return request.CoordinatorEpoch, true
	case protocol.WorkerStatusRequest:
		return request.CoordinatorEpoch, true
	case protocol.CheckpointNotice:
		return request.Notice.Epoch, true
	case protocol.ResultRecordChunk:
		return request.Provenance.CoordinatorEpoch, true
	case protocol.ResultArtifactChunk:
		return request.CoordinatorEpoch, true
	case protocol.ResultFetchRequest:
		return request.CoordinatorEpoch, true
	default:
		return model.CoordinatorEpoch{}, false
	}
}

// handleResultRecord receives one replicated result record. It does not take
// the process admission gate (Task 24 defect #6 ruling, applied to normal
// replication under defect #4): the gate guards admission of new work and is
// closed from a fence until the Running install that follows, which a
// Draining job never receives; the transfer's own validation (current fence,
// current replica set, Running or Draining, exact endpoints) authorises the
// record, and Closed stays refused there.
func (owner *ControlOwner) handleResultRecord(ctx context.Context, session *ControlSession, peer controlPeer, role TransferRole, request protocol.ResultRecordChunk) (protocol.WorkerMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := owner.finalCurrentSession(ctx, session, peer, "result-record", request.Provenance.CoordinatorEpoch); err != nil {
		return nil, err
	}
	return owner.transfer.ReceiveResultRecord(ctx, TransferPeer{NodeID: peer.node, WorkerEpoch: peer.epoch, Role: role}, request)
}

// handleRepairPull serves one destination-driven repair pull from an
// authenticated worker peer: the request is a WorkerStatus carrying the
// destination's durable repair grant status, and the response is either the
// source's exact next record chunk or, when the covered range is exhausted,
// the terminal WorkerStatus carrying the source's completed repair status.
// The process admission gate is deliberately not taken: a historical source
// whose assignment is no longer Running still owes its retained records.
func (owner *ControlOwner) handleRepairPull(ctx context.Context, session *ControlSession, peer controlPeer, request protocol.WorkerStatus) (protocol.WorkerMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Repair == nil || request.Repair.Role != protocol.RepairDestination || request.NodeID != peer.node || request.WorkerEpoch != peer.epoch {
		return nil, ErrControlUnauthorized
	}
	if request.Repair.Instruction.SourceNodeID != owner.localNode || request.Repair.Instruction.SourceWorkerEpoch != owner.localEpoch {
		return nil, ErrControlUnauthorized
	}
	if err := owner.finalCurrentSession(ctx, session, peer, "repair-pull", request.Repair.Instruction.CoordinatorEpoch); err != nil {
		return nil, err
	}
	chunk, complete, err := owner.transfer.ServeRepairPull(ctx, TransferPeer{NodeID: peer.node, WorkerEpoch: peer.epoch, Role: TransferHistoricalRepair}, *request.Repair)
	if err != nil {
		return nil, err
	}
	if !complete {
		return chunk, nil
	}
	work, err := owner.repository.RecoverWorkBounded()
	if err != nil {
		return nil, err
	}
	transaction, err := owner.repository.DurableTransactionID()
	if err != nil {
		return nil, err
	}
	for _, repair := range work.Repairs {
		if repair.Instruction.RepairID == request.Repair.RepairID && repair.Role == store.RepairSource {
			if transaction == 0 {
				return nil, ErrControlStaleAssignment
			}
			status := repairStatus(repair)
			return protocol.WorkerStatus{NodeID: owner.localNode, WorkerEpoch: owner.localEpoch, CoordinatorEpoch: work.Fence, StoreTransactionID: transaction, Repair: &status}, nil
		}
	}
	return nil, ErrControlStaleAssignment
}

func (owner *ControlOwner) finalCurrentSession(ctx context.Context, session *ControlSession, peer controlPeer, kind string, epoch model.CoordinatorEpoch) error {
	if owner.beforeMutation != nil {
		owner.beforeMutation(kind)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := session.done.Err(); err != nil {
		return ErrControlClosed
	}
	if err := session.revalidate(peer.node); err != nil {
		return err
	}
	work, err := owner.repository.RecoverWorkBounded()
	if err != nil {
		return err
	}
	if work.Fence == (model.CoordinatorEpoch{}) || epoch != work.Fence {
		return ErrControlStaleEpoch
	}
	return nil
}

func (owner *ControlOwner) finalMutation(ctx context.Context, session *ControlSession, peer controlPeer, kind string, epoch model.CoordinatorEpoch, exact bool) error {
	if owner.beforeMutation != nil {
		owner.beforeMutation(kind)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := session.done.Err(); err != nil {
		return ErrControlClosed
	}
	if err := session.revalidate(peer.node); err != nil {
		return err
	}
	return owner.authorizeCoordinator(peer, epoch, exact)
}

func (owner *ControlOwner) handleFence(ctx context.Context, session *ControlSession, peer controlPeer, request protocol.FenceRequest) (protocol.WorkerMessage, error) {
	if err := owner.authorizeCoordinator(peer, request.CoordinatorEpoch, false); err != nil {
		return nil, err
	}
	work, err := owner.repository.RecoverWorkBounded()
	if err != nil {
		return nil, err
	}
	if work.Fence != (model.CoordinatorEpoch{}) {
		comparison := compareControlEpoch(request.CoordinatorEpoch, work.Fence)
		if comparison < 0 || comparison == 0 && request.CoordinatorEpoch != work.Fence {
			return nil, ErrControlStaleEpoch
		}
		if comparison == 0 {
			return owner.fenceResponse(work.Fence), nil
		}
	}
	if err := owner.gate.CloseAndWait(ctx); err != nil {
		return nil, err
	}
	work, err = owner.repository.RecoverWorkBounded()
	if err != nil {
		return nil, err
	}
	if work.Fence != (model.CoordinatorEpoch{}) {
		comparison := compareControlEpoch(request.CoordinatorEpoch, work.Fence)
		if comparison <= 0 {
			if comparison == 0 && request.CoordinatorEpoch == work.Fence {
				return owner.fenceResponse(work.Fence), nil
			}
			return nil, ErrControlStaleEpoch
		}
	}
	if err := owner.finalMutation(ctx, session, peer, "fence", request.CoordinatorEpoch, false); err != nil {
		return nil, err
	}
	if err := owner.repository.Fence(request.CoordinatorEpoch); err != nil {
		return nil, err
	}
	// The durable fence changed: the serialized engine owner re-establishes
	// its installed view at this exact transition, exactly as an install
	// command does, so it never keeps serving the superseded fence.
	if err := owner.engine.ObserveFence(ctx); err != nil {
		return nil, err
	}
	owner.closeOlderCoordinatorSessions(session, request.CoordinatorEpoch)
	return owner.fenceResponse(request.CoordinatorEpoch), nil
}

func (owner *ControlOwner) authorizeCoordinator(peer controlPeer, epoch model.CoordinatorEpoch, exact bool) error {
	if _, voter := owner.configuration.RaftVoterByID(peer.node); !voter || epoch.Validate() != nil || epoch.Coordinator != peer.node {
		return ErrControlUnauthorized
	}
	if exact {
		work, err := owner.repository.RecoverWorkBounded()
		if err != nil {
			return err
		}
		if work.Fence == (model.CoordinatorEpoch{}) || epoch != work.Fence {
			return ErrControlStaleEpoch
		}
	}
	return nil
}

func (owner *ControlOwner) closeOlderCoordinatorSessions(current *ControlSession, generation model.CoordinatorEpoch) {
	owner.mu.Lock()
	sessions := make([]*ControlSession, 0)
	for session := range owner.sessions {
		if session == current {
			continue
		}
		session.mu.Lock()
		peer, lastCommand := session.peer, session.lastCommand
		authenticated := session.authenticated
		session.mu.Unlock()
		if authenticated {
			if _, voter := owner.configuration.RaftVoterByID(peer.node); voter && (lastCommand == (model.CoordinatorEpoch{}) || compareControlEpoch(lastCommand, generation) < 0) {
				sessions = append(sessions, session)
			}
		}
	}
	owner.mu.Unlock()
	for _, session := range sessions {
		_ = session.Close()
	}
}

func (owner *ControlOwner) fenceResponse(epoch model.CoordinatorEpoch) protocol.FenceResponse {
	return protocol.FenceResponse{NodeID: owner.localNode, WorkerEpoch: owner.localEpoch, CoordinatorEpoch: epoch}
}

func compareControlEpoch(left, right model.CoordinatorEpoch) int {
	if left.Term < right.Term {
		return -1
	}
	if left.Term > right.Term {
		return 1
	}
	if left.BeginIndex < right.BeginIndex {
		return -1
	}
	if left.BeginIndex > right.BeginIndex {
		return 1
	}
	return 0
}

func (owner *ControlOwner) handleAssignment(ctx context.Context, session *ControlSession, peer controlPeer, request protocol.AssignmentSetInstall) (protocol.WorkerMessage, error) {
	if err := owner.authorizeCoordinator(peer, request.CoordinatorEpoch, true); err != nil {
		return nil, err
	}
	work, err := owner.repository.RecoverWorkBounded()
	if err != nil {
		return nil, err
	}
	if prior, ok := controlAssignment(work, request.Assignment.JobID); ok && request.Assignment.Revision > prior.Assignment.Revision && request.Assignment.Revision-prior.Assignment.Revision > 1 {
		return nil, ErrControlStaleAssignment
	}
	if !assignmentTargetsWorker(request.Assignment, owner.localNode, owner.localEpoch) {
		if request.SchedulingState == model.Running || !owner.historicalResultsAuthorizeAssignment(work, request.Assignment, request.SpecificationDigest) {
			return nil, ErrControlStaleAssignment
		}
	}
	if err := owner.validateLocalSlots(work, request.Assignment, request.SchedulingState); err != nil {
		return nil, err
	}
	if err := owner.finalMutation(ctx, session, peer, "assignment", request.CoordinatorEpoch, true); err != nil {
		return nil, err
	}
	if err := owner.repository.InstallAssignment(request.Assignment, request.Specification, request.JobControlRevision, request.SchedulingState, request.CoordinatorEpoch); err != nil {
		return nil, err
	}
	if err := owner.finalMutation(ctx, session, peer, "assignment-reconcile", request.CoordinatorEpoch, true); err != nil {
		return nil, err
	}
	if err := owner.engine.ReconcileAssignment(ctx, request.Assignment.JobID); err != nil {
		return nil, err
	}
	if request.SchedulingState == model.Running {
		if err := owner.finalMutation(ctx, session, peer, "assignment-gate", request.CoordinatorEpoch, true); err != nil {
			return nil, err
		}
		if err := owner.gate.Open(request.CoordinatorEpoch); err != nil {
			return nil, err
		}
	}
	return protocol.AssignmentSetInstallAck{NodeID: owner.localNode, WorkerEpoch: owner.localEpoch, JobID: request.Assignment.JobID, AssignmentRevision: request.Assignment.Revision, AssignmentDigest: request.Assignment.Digest, JobControlRevision: request.JobControlRevision, SchedulingState: request.SchedulingState, CoordinatorEpoch: request.CoordinatorEpoch}, nil
}

func (owner *ControlOwner) historicalResultsAuthorizeAssignment(work store.RecoveredWork, candidate model.AssignmentSet, specification [32]byte) bool {
	for _, stored := range work.Results {
		if stored.Record.TupleID.JobID != candidate.JobID || stored.Record.SpecificationHash != specification || stored.Provenance.AssignmentRevision >= candidate.Revision || stored.Provenance.AssignmentDigest == candidate.Digest || stored.Provenance.Validate(stored.Record) != nil {
			continue
		}
		node, _, _, _, ok := endpointsForRole(stored.Provenance.ReplicaSet, stored.Provenance.DestinationRole)
		if ok && node == owner.localNode {
			return true
		}
	}
	return false
}

func (owner *ControlOwner) historicalResultHolder(work store.RecoveredWork, assignment store.InstalledAssignment) bool {
	return assignment.SchedulingState != model.Running && owner.historicalResultsAuthorizeAssignment(work, assignment.Assignment, assignment.Topology.Digest())
}

func (owner *ControlOwner) validateLocalSlots(work store.RecoveredWork, candidate model.AssignmentSet, scheduling model.SchedulingState) error {
	var used uint64
	for _, assignment := range work.Assignments {
		if assignment.Assignment.JobID == candidate.JobID || assignment.SchedulingState == model.Closed {
			continue
		}
		local := localTaskCount(assignment.Assignment, owner.localNode, owner.localEpoch)
		if used > ^uint64(0)-local {
			return ErrControlCapacity
		}
		used += local
	}
	if scheduling != model.Closed {
		local := localTaskCount(candidate, owner.localNode, owner.localEpoch)
		if used > ^uint64(0)-local {
			return ErrControlCapacity
		}
		used += local
	}
	if used > uint64(owner.configuration.Crane.WorkerSlots) {
		return ErrControlCapacity
	}
	return nil
}

func localTaskCount(set model.AssignmentSet, node uint16, epoch model.WorkerEpoch) uint64 {
	var count uint64
	for _, token := range set.Tasks {
		if token.WorkerID == node && token.WorkerEpoch == epoch {
			count++
		}
	}
	return count
}

func assignmentTargetsWorker(set model.AssignmentSet, node uint16, epoch model.WorkerEpoch) bool {
	for _, token := range set.Tasks {
		if token.WorkerID == node && token.WorkerEpoch == epoch {
			return true
		}
	}
	for _, replica := range set.ResultReplicas {
		if replica.PrimaryNodeID == node && replica.PrimaryEpoch == epoch || replica.SecondaryNodeID == node && replica.SecondaryEpoch == epoch {
			return true
		}
	}
	return false
}

func (owner *ControlOwner) handleStatus(ctx context.Context, session *ControlSession, peer controlPeer, request protocol.WorkerStatusRequest) (protocol.WorkerMessage, error) {
	if err := owner.authorizeCoordinator(peer, request.CoordinatorEpoch, true); err != nil {
		return nil, err
	}
	if request.AfterTransactionID != 0 {
		if err := owner.finalMutation(ctx, session, peer, "event-ack", request.CoordinatorEpoch, true); err != nil {
			return nil, err
		}
		if err := owner.engine.AcknowledgeEvents(ctx, request.AfterTransactionID); err != nil {
			return nil, err
		}
	}
	work, err := owner.repository.RecoverWorkBounded()
	if err != nil {
		return nil, err
	}
	var repair *protocol.ResultRepairStatus
	if request.Repair != nil {
		repair, err = owner.installRepair(ctx, session, peer, work, *request.Repair)
		if err != nil {
			return nil, err
		}
		work, err = owner.repository.RecoverWorkBounded()
		if err != nil {
			return nil, err
		}
	}
	transaction, err := owner.repository.DurableTransactionID()
	if err != nil {
		return nil, err
	}
	events, last, more, err := owner.repository.PendingEvents(request.AfterTransactionID, request.MaxEvents)
	if err != nil {
		return nil, err
	}
	if transaction == 0 || transaction < last || transaction < request.AfterTransactionID {
		return nil, ErrControlStaleAssignment
	}
	status := protocol.WorkerStatus{NodeID: owner.localNode, WorkerEpoch: owner.localEpoch, CoordinatorEpoch: work.Fence, StoreTransactionID: transaction, AfterTransactionID: request.AfterTransactionID, Events: cloneControlEvents(events), LastTransactionID: last, HasMore: more, Repair: repair}
	if admission, open := owner.gate.AdmissionEpoch(); open {
		// The process admission gate opens only on a Running install, so the
		// reported epoch distinguishes a restarted (closed-gate) process from
		// an admitted one for the leader's convergence decisions.
		status.AdmissionEpoch = admission
	}
	status.Assignments = make([]protocol.InstalledAssignmentStatus, len(work.Assignments))
	for index, assignment := range work.Assignments {
		status.Assignments[index] = protocol.InstalledAssignmentStatus{JobID: assignment.Assignment.JobID, JobControlRevision: assignment.JobControlRevision, AssignmentRevision: assignment.Assignment.Revision, AssignmentDigest: assignment.Assignment.Digest, SpecificationDigest: assignment.Topology.Digest(), SchedulingState: assignment.SchedulingState}
	}
	sort.Slice(status.Assignments, func(i, j int) bool {
		return bytes.Compare(status.Assignments[i].JobID[:], status.Assignments[j].JobID[:]) < 0
	})
	if request.Inventory != nil {
		summary, inventoryErr := owner.controlInventory(work, *request.Inventory)
		if inventoryErr != nil {
			return nil, inventoryErr
		}
		status.Inventory = &summary
	}
	if request.Repair == nil && repair == nil {
		status.Repair = nil
	}
	return status, nil
}

func cloneControlEvents(events []model.WorkerEvent) []model.WorkerEvent {
	result := make([]model.WorkerEvent, len(events))
	for index := range events {
		result[index] = cloneWorkerEvent(events[index])
	}
	return result
}

func (owner *ControlOwner) controlInventory(work store.RecoveredWork, query protocol.ResultInventoryQuery) (protocol.ResultInventorySummary, error) {
	assignment, ok := controlAssignment(work, query.JobID)
	replica, replicaOK := controlReplica(assignment.Assignment, query.SinkTask)
	localReplica := replicaOK && (replica.PrimaryNodeID == owner.localNode && replica.PrimaryEpoch == owner.localEpoch || replica.SecondaryNodeID == owner.localNode && replica.SecondaryEpoch == owner.localEpoch)
	historical := ok && !localReplica && owner.historicalResultHolder(work, assignment)
	if !ok || assignment.CoordinatorEpoch != work.Fence || assignment.Assignment.Revision != query.AssignmentRevision || assignment.Assignment.Digest != query.AssignmentDigest || assignment.Topology.Digest() != query.SpecificationHash || !localReplica && !historical {
		return protocol.ResultInventorySummary{}, ErrControlStaleAssignment
	}
	want := make([]protocol.SourceCheckpoint, 0)
	for _, token := range assignment.Assignment.Tasks {
		stage, stageOK := controlStage(assignment.Topology.Spec(), token.Task.StageID)
		if !stageOK || stage.Role != model.StageSource {
			continue
		}
		watermark, proofOK := owner.controlCheckpointProof(work, assignment, token)
		if !proofOK {
			return protocol.ResultInventorySummary{}, ErrControlStaleAssignment
		}
		want = append(want, protocol.SourceCheckpoint{Source: token.Task, Watermark: watermark})
	}
	sort.Slice(want, func(i, j int) bool { return controlTaskLess(want[i].Source, want[j].Source) })
	if !reflectSourceCheckpoints(want, query.Checkpoints) || protocol.CheckpointVectorDigest(want) != query.CheckpointDigest || protocol.InventoryQueryDigest(query) != query.QueryDigest {
		return protocol.ResultInventorySummary{}, ErrControlStaleAssignment
	}
	watermarks := make(map[model.TaskID]uint64, len(query.Checkpoints))
	for _, checkpoint := range query.Checkpoints {
		watermarks[checkpoint.Source] = checkpoint.Watermark
	}
	records := make([]model.ResultRecord, 0)
	historicalRecord := false
	for _, stored := range work.Results {
		if stored.Record.SinkTask != query.SinkTask || stored.Record.SpecificationHash != query.SpecificationHash {
			continue
		}
		watermark, covered := watermarks[stored.Record.TupleID.SourceTask]
		if covered && stored.Record.TupleID.SourceSequence <= watermark {
			if historical {
				node, _, _, _, endpointOK := endpointsForRole(stored.Provenance.ReplicaSet, stored.Provenance.DestinationRole)
				if stored.Record.TupleID.JobID != query.JobID || stored.Provenance.AssignmentRevision >= query.AssignmentRevision || stored.Provenance.AssignmentDigest == query.AssignmentDigest || stored.Provenance.Validate(stored.Record) != nil || !endpointOK || node != owner.localNode {
					return protocol.ResultInventorySummary{}, ErrControlStaleAssignment
				}
				historicalRecord = true
			}
			record := stored.Record
			record.Value = append([]byte(nil), record.Value...)
			records = append(records, record)
		}
	}
	if historical && !historicalRecord {
		return protocol.ResultInventorySummary{}, ErrControlStaleAssignment
	}
	count, total, digest, err := ResultInventoryAggregate(query.QueryDigest, records)
	if err != nil {
		return protocol.ResultInventorySummary{}, err
	}
	return protocol.ResultInventorySummary{QueryDigest: query.QueryDigest, RecordCount: count, TotalBytes: total, ContentDigest: digest}, nil
}

func (owner *ControlOwner) controlCheckpointProof(work store.RecoveredWork, assignment store.InstalledAssignment, token model.AssignmentToken) (uint64, bool) {
	if token.WorkerID == owner.localNode && token.WorkerEpoch == owner.localEpoch {
		cursor, ok := controlSource(work, token.Task)
		if !ok || cursor.Watermark == 0 && cursor.CheckpointRevision == 0 {
			// The zero watermark is trivially committed: an untouched source
			// covers no records, and the query's bound vector still rejects
			// any nonzero claim for it.
			return 0, true
		}
		// Task 24 defect #2 ruling: the retained durable checkpoint proof is
		// itself valid committed-watermark evidence — the watermark was
		// committed, an immutable historical fact — so a proof whose
		// epoch/JobControlRevision/assignment binding is historical still
		// proves it; only proof integrity (nonzero committed Raft index, source
		// binding) is required, not current-authority equality.
		return cursor.Watermark, cursor.RaftIndex != 0 && cursor.CheckpointAuthority.SourceToken.Task == token.Task
	}
	for _, observation := range work.Checkpoints {
		if observation.Notice.Source == token.Task && observation.Notice.JobID == assignment.Assignment.JobID && observation.Notice.Epoch == work.Fence && observation.JobControlRevision == assignment.JobControlRevision && observation.AssignmentRevision == assignment.Assignment.Revision && observation.AssignmentDigest == assignment.Assignment.Digest && observation.Notice.RaftIndex != 0 {
			return observation.Notice.Watermark, true
		}
	}
	// Without any durable observation only the trivially committed zero
	// watermark can be proven; a nonzero committed source fails the vector
	// equality and the query stays refused.
	return 0, true
}

func (owner *ControlOwner) installRepair(ctx context.Context, session *ControlSession, peer controlPeer, work store.RecoveredWork, grant protocol.RepairGrant) (*protocol.ResultRepairStatus, error) {
	if grant.Instruction.CoordinatorEpoch != work.Fence {
		return nil, ErrControlStaleEpoch
	}
	// Exact re-delivery of a durably known grant answers from durable state
	// before any current-authority admission and performs no mutation: a
	// superseded (RepairFailed) or completed grant reports its terminal state
	// without being revived, and a live grant is merely re-scheduled.
	for _, prior := range work.Repairs {
		if prior.Instruction.RepairID != grant.Instruction.RepairID {
			continue
		}
		if !equalRepairDefinition(prior.Instruction, protocolRepairDefinition(grant.Instruction)) || prior.InstructionDigest != grant.Instruction.InstructionDigest || prior.Role != grant.Role {
			return nil, model.ErrIdentityReuse
		}
		if prior.State != store.RepairComplete && prior.State != store.RepairFailed {
			owner.scheduleRepair(prior)
		}
		status := repairStatus(prior)
		return &status, nil
	}
	summary, err := owner.controlInventory(work, protocol.ResultInventoryQuery{JobID: grant.Instruction.JobID, SinkTask: grant.Instruction.SinkTask, SpecificationHash: grant.Instruction.SpecificationHash, AssignmentRevision: grant.Instruction.AssignmentRevision, AssignmentDigest: grant.Instruction.AssignmentDigest, Checkpoints: append([]protocol.SourceCheckpoint(nil), grant.Instruction.Checkpoints...), CheckpointDigest: grant.Instruction.CheckpointDigest, QueryDigest: grant.Instruction.InventoryQueryDigest})
	if err != nil {
		return nil, err
	}
	assignment, ok := controlAssignment(work, grant.Instruction.JobID)
	if !ok {
		return nil, ErrControlStaleAssignment
	}
	replica, ok := controlReplica(assignment.Assignment, grant.Instruction.SinkTask)
	if !ok || grant.Instruction.DestinationNodeID != replica.PrimaryNodeID && grant.Instruction.DestinationNodeID != replica.SecondaryNodeID || grant.Instruction.DestinationNodeID == replica.PrimaryNodeID && grant.Instruction.DestinationWorkerEpoch != replica.PrimaryEpoch || grant.Instruction.DestinationNodeID == replica.SecondaryNodeID && grant.Instruction.DestinationWorkerEpoch != replica.SecondaryEpoch {
		return nil, ErrControlStaleAssignment
	}
	if grant.Role == protocol.RepairDestination && !(replica.PrimaryNodeID == owner.localNode && replica.PrimaryEpoch == owner.localEpoch || replica.SecondaryNodeID == owner.localNode && replica.SecondaryEpoch == owner.localEpoch) {
		return nil, ErrControlStaleAssignment
	}
	localRole := grant.Role == protocol.RepairSource && grant.Instruction.SourceNodeID == owner.localNode && grant.Instruction.SourceWorkerEpoch == owner.localEpoch || grant.Role == protocol.RepairDestination && grant.Instruction.DestinationNodeID == owner.localNode && grant.Instruction.DestinationWorkerEpoch == owner.localEpoch
	if !localRole || !controlMemberActive(owner.membership.View(), grant.Instruction.SourceNodeID) || !controlMemberActive(owner.membership.View(), grant.Instruction.DestinationNodeID) {
		return nil, ErrControlUnauthorized
	}
	// One live grant per (epoch, job, sink, role) identity. A prior that is
	// terminal, or bound to an assignment revision the installed assignment
	// has advanced past (the reassignment made it dead and the coordinator
	// now re-repairs the sink under the current revision), never blocks the
	// replacement; superseded priors are durably marked failed below.
	var superseded []store.ResultRepairRecord
	for _, prior := range work.Repairs {
		if prior.Instruction.CoordinatorEpoch != grant.Instruction.CoordinatorEpoch || prior.Instruction.JobID != grant.Instruction.JobID || prior.Instruction.SinkTask != grant.Instruction.SinkTask || prior.Role != grant.Role {
			continue
		}
		switch {
		case prior.State == store.RepairComplete || prior.State == store.RepairFailed:
		case prior.Instruction.AssignmentRevision < assignment.Assignment.Revision:
			superseded = append(superseded, prior)
		default:
			return nil, model.ErrIdentityReuse
		}
	}
	if grant.Role == protocol.RepairSource && (summary.RecordCount != grant.Instruction.ExpectedRecordCount || summary.TotalBytes != grant.Instruction.ExpectedTotalBytes || summary.ContentDigest != grant.Instruction.ExpectedContentDigest) {
		return nil, ErrControlStaleAssignment
	}
	definition := protocolRepairDefinition(grant.Instruction)
	repair := store.ResultRepairRecord{Instruction: definition, InstructionDigest: grant.Instruction.InstructionDigest, Role: grant.Role, State: store.RepairPending, ContentDigest: model.EmptyResultInventoryDigest(grant.Instruction.InstructionDigest)}
	if err := owner.finalMutation(ctx, session, peer, "repair", grant.Instruction.CoordinatorEpoch, true); err != nil {
		return nil, err
	}
	for _, prior := range superseded {
		prior.State = store.RepairFailed
		prior.ErrorCode = protocol.WorkerErrorStaleAssignment
		if err := owner.repository.UpsertRepair(prior); err != nil {
			return nil, err
		}
	}
	if err := owner.repository.UpsertRepair(repair); err != nil {
		return nil, err
	}
	owner.scheduleRepair(repair)
	status := repairStatus(repair)
	return &status, nil
}

// scheduleRepair hands one durably installed destination-role grant to the
// repair driver; the destination endpoint owns driving every bilateral grant.
func (owner *ControlOwner) scheduleRepair(repair store.ResultRepairRecord) {
	if owner.scheduler != nil && repair.Role == store.RepairDestination {
		owner.scheduler.Schedule(repair)
	}
}

func repairStatus(repair store.ResultRepairRecord) protocol.ResultRepairStatus {
	instruction := repair.Instruction
	value := repairProtocolDefinition(instruction)
	return protocol.ResultRepairStatus{Instruction: value, RepairID: instruction.RepairID, InstructionDigest: repair.InstructionDigest, Role: repair.Role, State: repair.State, RecordCount: repair.RecordCount, TotalBytes: repair.TotalBytes, ContentDigest: repair.ContentDigest, ErrorCode: repair.ErrorCode}
}

func controlAssignment(work store.RecoveredWork, job model.JobID) (store.InstalledAssignment, bool) {
	for _, assignment := range work.Assignments {
		if assignment.Assignment.JobID == job {
			return assignment, true
		}
	}
	return store.InstalledAssignment{}, false
}

func controlSource(work store.RecoveredWork, source model.TaskID) (store.SourceCursor, bool) {
	for _, cursor := range work.Sources {
		if cursor.Source == source {
			return cursor, true
		}
	}
	return store.SourceCursor{}, false
}

func controlReplica(set model.AssignmentSet, sink model.TaskID) (model.ResultReplicaSet, bool) {
	for _, replica := range set.ResultReplicas {
		if replica.SinkTask == sink {
			return replica, true
		}
	}
	return model.ResultReplicaSet{}, false
}

func controlStage(spec model.TopologySpec, id uint16) (model.StageSpec, bool) {
	for _, stage := range spec.Stages {
		if stage.StageID == id {
			return stage, true
		}
	}
	return model.StageSpec{}, false
}

func controlMemberActive(view membership.View, node uint16) bool {
	_, ok := activeControlMember(view, node)
	return ok
}

func reflectSourceCheckpoints(left, right []protocol.SourceCheckpoint) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func controlTaskLess(left, right model.TaskID) bool {
	if left.JobID != right.JobID {
		return bytes.Compare(left.JobID[:], right.JobID[:]) < 0
	}
	if left.StageID != right.StageID {
		return left.StageID < right.StageID
	}
	return left.Partition < right.Partition
}

func (owner *ControlOwner) handleCheckpoint(ctx context.Context, session *ControlSession, peer controlPeer, request protocol.CheckpointNotice) (protocol.WorkerMessage, error) {
	if err := owner.authorizeCoordinator(peer, request.Notice.Epoch, true); err != nil {
		return nil, err
	}
	work, err := owner.repository.RecoverWorkBounded()
	if err != nil {
		return nil, err
	}
	assignment, ok := controlAssignment(work, request.Notice.JobID)
	if !ok || assignment.CoordinatorEpoch != work.Fence || request.Notice.Epoch != work.Fence || request.JobControlRevision != assignment.JobControlRevision || request.AssignmentRevision != assignment.Assignment.Revision || request.AssignmentDigest != assignment.Assignment.Digest || !assignmentTargetsWorker(assignment.Assignment, owner.localNode, owner.localEpoch) && !owner.historicalResultHolder(work, assignment) {
		return nil, ErrControlStaleAssignment
	}
	var source model.AssignmentToken
	for _, token := range assignment.Assignment.Tasks {
		if token.Task == request.Notice.Source {
			source = token
			break
		}
	}
	stage, stageOK := controlStage(assignment.Topology.Spec(), request.Notice.Source.StageID)
	if source == (model.AssignmentToken{}) || !stageOK || stage.Role != model.StageSource {
		return nil, ErrControlStaleAssignment
	}
	if source.WorkerID == owner.localNode && source.WorkerEpoch == owner.localEpoch {
		if confirmed := owner.checkpointAlreadyDurable(work, request); confirmed {
			// The committed checkpoint is already durable here; a redelivered
			// notice (a later leadership pass carries a higher Raft index, and
			// the trivially committed zero watermark carries nothing) is
			// confirmed without any engine or store mutation. Byte-exact
			// duplicates still flow through the serialized engine so the
			// durable store answers them.
			if err := owner.finalCurrentSession(ctx, session, peer, "checkpoint", request.Notice.Epoch); err != nil {
				return nil, err
			}
			return owner.checkpointAck(request), nil
		}
	}
	if err := owner.finalMutation(ctx, session, peer, "checkpoint", request.Notice.Epoch, true); err != nil {
		return nil, err
	}
	if source.WorkerID == owner.localNode && source.WorkerEpoch == owner.localEpoch {
		err = owner.engine.ApplyCheckpoint(ctx, request)
	} else {
		err = owner.repository.ObserveCheckpoint(request)
	}
	if err != nil {
		return nil, err
	}
	return owner.checkpointAck(request), nil
}

// checkpointAck builds the exact acknowledgment for one accepted or confirmed
// checkpoint notice.
func (owner *ControlOwner) checkpointAck(request protocol.CheckpointNotice) protocol.CheckpointAck {
	return protocol.CheckpointAck{NodeID: owner.localNode, WorkerEpoch: owner.localEpoch, JobID: request.Notice.JobID, Source: request.Notice.Source, Watermark: request.Notice.Watermark, RaftIndex: request.Notice.RaftIndex, JobControlRevision: request.JobControlRevision, AssignmentRevision: request.AssignmentRevision, AssignmentDigest: request.AssignmentDigest, CoordinatorEpoch: request.Notice.Epoch}
}

// checkpointAlreadyDurable reports whether a locally owned source already
// durably covers the exact committed notice so that a redelivery may be
// confirmed without mutation: either the trivially committed zero watermark on
// an untouched source, or a matching-authority duplicate whose only change is
// a strictly higher Raft index from a later leadership pass. A byte-exact
// duplicate (equal index) is never confirmed here so the durable store keeps
// answering it.
func (owner *ControlOwner) checkpointAlreadyDurable(work store.RecoveredWork, request protocol.CheckpointNotice) bool {
	cursor, ok := controlSource(work, request.Notice.Source)
	if !ok || cursor.Watermark == 0 && cursor.CheckpointRevision == 0 {
		return request.Notice.Watermark == 0
	}
	if request.Notice.Watermark != cursor.Watermark || cursor.CheckpointRevision == 0 || request.Notice.RaftIndex <= cursor.RaftIndex {
		return false
	}
	proof := cursor.CheckpointAuthority
	return proof.CoordinatorEpoch == request.Notice.Epoch && proof.JobControlRevision == request.JobControlRevision &&
		proof.AssignmentRevision == request.AssignmentRevision && proof.AssignmentDigest == request.AssignmentDigest &&
		proof.SourceToken.Task == request.Notice.Source
}

type controlReplay struct {
	mu          sync.Mutex
	clock       clock.Clock
	window      time.Duration
	future      time.Duration
	perLimit    int
	senderLimit int
	global      *wire.ReplayGuard
	perSender   map[uint16]*wire.ReplayGuard
}

func newControlReplay(source clock.Clock, window, future time.Duration, globalLimit, perLimit int) *controlReplay {
	return &controlReplay{clock: source, window: window, future: future, perLimit: perLimit, senderLimit: globalLimit, global: wire.NewReplayGuard(source, window, future, globalLimit), perSender: make(map[uint16]*wire.ReplayGuard)}
}

func (replay *controlReplay) preflight(sender uint16, request wire.RequestID, timestamp time.Time) error {
	replay.mu.Lock()
	defer replay.mu.Unlock()
	if err := replay.global.Preflight(sender, request, timestamp); err != nil {
		return err
	}
	if guard := replay.perSender[sender]; guard != nil {
		return guard.Preflight(sender, request, timestamp)
	}
	if len(replay.perSender) >= replay.senderLimit {
		return wire.ErrReplayCacheFull
	}
	return nil
}

func (replay *controlReplay) commit(sender uint16, request wire.RequestID, timestamp time.Time) error {
	replay.mu.Lock()
	defer replay.mu.Unlock()
	guard := replay.perSender[sender]
	if guard == nil {
		if len(replay.perSender) >= replay.senderLimit {
			return wire.ErrReplayCacheFull
		}
		guard = wire.NewReplayGuard(replay.clock, replay.window, replay.future, replay.perLimit)
		if err := guard.Preflight(sender, request, timestamp); err != nil {
			return err
		}
	}
	if err := replay.global.Commit(sender, request, timestamp); err != nil {
		return err
	}
	if err := guard.Commit(sender, request, timestamp); err != nil {
		return err
	}
	replay.perSender[sender] = guard
	return nil
}

func (replay *controlReplay) recordInvalid(sender uint16, request wire.RequestID, timestamp time.Time) {
	replay.mu.Lock()
	defer replay.mu.Unlock()
	replay.global.RecordInvalid(sender, request, timestamp)
	guard := replay.perSender[sender]
	if guard == nil {
		if len(replay.perSender) >= replay.senderLimit {
			return
		}
		guard = wire.NewReplayGuard(replay.clock, replay.window, replay.future, replay.perLimit)
		replay.perSender[sender] = guard
	}
	guard.RecordInvalid(sender, request, timestamp)
}
