package raft

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/config"
)

const (
	// MaxPendingLocalRequests bounds proposals and barriers accepted by one node.
	MaxPendingLocalRequests = 1024
	// MaxNodeEventQueue bounds serialized public and cancellation events.
	MaxNodeEventQueue = 1024
)

const (
	nodeLifecycleNew uint32 = iota
	nodeLifecycleStarting
	nodeLifecycleRunning
	nodeLifecycleStopped
)

// NodeOptions contains validated dependencies and timing for one serialized node.
type NodeOptions struct {
	// LocalID is the configured local voter identity.
	LocalID uint16
	// Voters is the immutable fixed quorum and trust boundary.
	Voters VoterSet
	// Identity is the exact storage identity expected during recovery.
	Identity StorageIdentity
	// Store owns durable safety-critical state for the complete Run lifetime.
	Store StableStore
	// StateMachine owns deterministic reconstructed application state.
	StateMachine StateMachine
	// Transport accepts bounded owned peer-message handoffs.
	Transport Transport
	// Clock supplies the one reusable owner timer.
	Clock clock.Clock
	// Random supplies deterministic election samples.
	Random interface{ Uint64() uint64 }
	// TransferIDs supplies deterministic nonzero peer-local snapshot identities.
	TransferIDs TransferIDSource
	// ElectionTimeoutMin is the inclusive randomized election lower bound.
	ElectionTimeoutMin time.Duration
	// ElectionTimeoutMax is the exclusive randomized election upper bound.
	ElectionTimeoutMax time.Duration
	// HeartbeatInterval is the leader replication cadence.
	HeartbeatInterval time.Duration
	// MaxAppendEntries optionally narrows the default append entry count.
	MaxAppendEntries uint16
	// MaxAppendBytes optionally narrows the default append encoded-byte bound.
	MaxAppendBytes uint64
	// SnapshotEntryThreshold triggers capture only after this many applied entries beyond the durable base.
	SnapshotEntryThreshold uint64
	// SnapshotByteThreshold triggers capture only after retained WAL bytes exceed this bound.
	SnapshotByteThreshold uint64
	// MaxSnapshotBytes bounds one encoded application snapshot.
	MaxSnapshotBytes uint64
	// MaxSnapshotChunkBytes bounds one outbound InstallSnapshot chunk; zero uses the protocol default.
	MaxSnapshotChunkBytes uint64
}

// TransferIDSource is the deterministic owner-provided identity seam for new
// peer-local snapshot transfer attempts.
type TransferIDSource interface {
	NextTransferID(peerID uint16) (TransferID, error)
}

type inboundRPCEvent struct {
	ctx      context.Context
	senderID uint16
	rpc      RPC
	response chan error
}

type localRequestEvent struct {
	ctx      context.Context
	kind     EntryKind
	command  []byte
	response chan localRequestResponse
}

type localRequestResponse struct {
	result ProposalResult
	err    error
}

type leadershipRequestEvent struct {
	ctx      context.Context
	capacity int
	response chan leadershipRequestResponse
}

type leadershipRequestResponse struct {
	subscription *LeadershipSubscription
	err          error
}

type leadershipCancelEvent struct {
	id  uint64
	err error
}

type leadershipSubscriber struct {
	id     uint64
	events chan LeadershipEvent
	done   chan struct{}
	stop   chan struct{}

	errMu sync.Mutex
	err   error
}

type pendingLocalRequest struct {
	entry    Entry
	response chan localRequestResponse
}

// Node is the serialized owner of one local Raft core and application.
type Node struct {
	options NodeOptions
	newCore func(CoreOptions) (*Core, error)

	lifecycle atomic.Uint32
	status    atomic.Value
	apiMu     sync.Mutex
	events    chan any
	ready     chan struct{}
	done      chan struct{}

	core         *Core
	timer        clock.Timer
	epoch        time.Time
	durableState RecoveredState

	pendingReservations atomic.Int64
	pendingLocal        map[ProposalID]pendingLocalRequest

	subscribers      map[uint64]*leadershipSubscriber
	nextSubscriberID uint64
	leadership       LeadershipEvent
	hasLeadership    bool
	captureInFlight  bool

	// afterAdvance is a same-package observation seam; it never influences behavior.
	afterAdvance func(ReadyToken)
	// afterResult is a same-package ordering seam invoked after a result is published.
	afterResult func(ProposalID)

	prepareOnce sync.Once
	prepareErr  error
	watchers    sync.WaitGroup
}

// NewNode validates options without opening storage, mutating the application,
// creating timers, or starting goroutines.
func NewNode(options NodeOptions) (*Node, error) {
	if err := options.Voters.ValidateLocalID(options.LocalID); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidCoreState, err)
	}
	expectedIdentity, err := NewStorageIdentity(options.Identity.FormatVersion, options.Identity.ClusterID, options.LocalID, options.Voters)
	if err != nil || expectedIdentity != options.Identity {
		return nil, fmt.Errorf("%w: node storage identity does not match local voter", ErrInvalidStorageIdentity)
	}
	if options.Store == nil || options.StateMachine == nil || options.Transport == nil || options.Clock == nil || options.Random == nil || options.TransferIDs == nil {
		return nil, fmt.Errorf("%w: node dependencies must be non-nil", ErrInvalidCoreState)
	}
	if options.HeartbeatInterval <= 0 || options.ElectionTimeoutMin <= 0 || options.ElectionTimeoutMax <= options.ElectionTimeoutMin || options.HeartbeatInterval >= options.ElectionTimeoutMin {
		return nil, fmt.Errorf("%w: invalid node timing", ErrInvalidCoreState)
	}
	if options.SnapshotEntryThreshold == 0 || options.SnapshotByteThreshold == 0 || options.MaxSnapshotBytes == 0 || options.MaxSnapshotBytes > config.MaxRaftSnapshotBytes {
		return nil, fmt.Errorf("%w: invalid node snapshot bounds", ErrInvalidCoreState)
	}
	if storeLimit := options.Store.SnapshotLimit(); storeLimit != options.MaxSnapshotBytes {
		return nil, fmt.Errorf("%w: node snapshot limit %d does not match store limit %d", ErrInvalidCoreState, options.MaxSnapshotBytes, storeLimit)
	}
	if options.MaxSnapshotChunkBytes == 0 {
		options.MaxSnapshotChunkBytes = config.MaxRaftSnapshotChunkBytes
	}
	if options.MaxSnapshotChunkBytes > config.MaxRaftSnapshotChunkBytes {
		return nil, fmt.Errorf("%w: invalid snapshot chunk bound %d", ErrInvalidCoreState, options.MaxSnapshotChunkBytes)
	}
	if options.ElectionTimeoutMin/options.HeartbeatInterval < 3 {
		return nil, fmt.Errorf("%w: election minimum must span at least three heartbeats", ErrInvalidCoreState)
	}
	appendLimits := DefaultCodecLimits()
	if options.MaxAppendEntries != 0 {
		appendLimits.MaxAppendEntries = options.MaxAppendEntries
	}
	if options.MaxAppendBytes != 0 {
		appendLimits.MaxEncodedBytes = options.MaxAppendBytes
	}
	if _, _, err := EncodeRPC(AppendEntriesRequest{LeaderID: options.LocalID, Term: 1, Generation: 1}, appendLimits); err != nil {
		return nil, fmt.Errorf("%w: append bounds: %v", ErrInvalidCoreState, err)
	}
	node := &Node{
		options: options,
		newCore: NewCore,
		events:  make(chan any, MaxNodeEventQueue),
		ready:   make(chan struct{}),
		done:    make(chan struct{}),
	}
	node.status.Store(Status{Role: RoleFollower})
	return node, nil
}

// Propose submits one opaque application command and waits for its exact apply.
func (node *Node) Propose(ctx context.Context, command []byte) (ProposalResult, error) {
	return node.submitLocal(ctx, EntryCommand, command)
}

// Barrier submits a current-term no-op application fence.
func (node *Node) Barrier(ctx context.Context) (uint64, error) {
	result, err := node.submitLocal(ctx, EntryNoOp, nil)
	return result.Index, err
}

// Status returns a race-safe, non-linearizable diagnostic snapshot.
func (node *Node) Status() Status {
	if node == nil {
		return Status{}
	}
	return node.status.Load().(Status)
}

// SubscribeLeadership returns an owner-linearized snapshot and bounded delta stream.
func (node *Node) SubscribeLeadership(ctx context.Context, capacity int) (*LeadershipSubscription, error) {
	if err := node.lifecycleError(); err != nil {
		return nil, err
	}
	if capacity < MinLeadershipSubscriptionCapacity || capacity > MaxLeadershipSubscriptionCapacity {
		return nil, fmt.Errorf("%w: %d", ErrInvalidLeadershipCapacity, capacity)
	}
	if ctx == nil {
		return nil, context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	event := leadershipRequestEvent{ctx: ctx, capacity: capacity, response: make(chan leadershipRequestResponse, 1)}
	if err := node.enqueuePublicEvent(ctx, event); err != nil {
		return nil, err
	}
	select {
	case response := <-event.response:
		return response.subscription, response.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-node.done:
		return nil, ErrStopped
	}
}

// SubmitRPC submits one already authenticated inbound RPC through the bounded owner queue.
func (node *Node) SubmitRPC(ctx context.Context, senderID uint16, rpc RPC) error {
	if err := node.lifecycleError(); err != nil {
		return err
	}
	if ctx == nil {
		return context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	event := inboundRPCEvent{
		ctx: ctx, senderID: senderID, rpc: CloneRPC(rpc), response: make(chan error, 1),
	}
	if err := node.enqueuePublicEvent(ctx, event); err != nil {
		return err
	}
	select {
	case err := <-event.response:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-node.done:
		return ErrStopped
	}
}

// Ready closes after recovery and application replay succeed.
func (node *Node) Ready() <-chan struct{} { return node.ready }

// Name returns the supervisor service name.
func (*Node) Name() string { return "raft" }

// Run recovers once and serializes every core, persistence, transport, and apply effect.
func (node *Node) Run(ctx context.Context) (runErr error) {
	if !node.lifecycle.CompareAndSwap(nodeLifecycleNew, nodeLifecycleStarting) {
		return ErrStopped
	}
	defer func() {
		node.stopTimer()
		node.apiMu.Lock()
		node.lifecycle.Store(nodeLifecycleStopped)
		node.releasePendingReservations()
		node.terminateLeadershipSubscriptions(ErrStopped)
		close(node.done)
		node.apiMu.Unlock()
		node.watchers.Wait()
		if closeErr := node.options.Store.Close(); closeErr != nil && runErr == nil {
			runErr = fmt.Errorf("close raft stable store: %w", closeErr)
		}
	}()
	if ctx == nil || ctx.Err() != nil {
		return nil
	}
	if err := node.prepare(); err != nil {
		return err
	}
	node.epoch = node.options.Clock.Now()
	node.timer = node.options.Clock.NewTimer(node.nextTimerDuration(0))
	node.lifecycle.Store(nodeLifecycleRunning)
	close(node.ready)

	for {
		if ctx.Err() != nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-node.timer.C():
			if ctx.Err() != nil {
				return nil
			}
			now, tickErr := node.logicalNow()
			if tickErr != nil {
				return tickErr
			}
			if ctx.Err() != nil {
				return nil
			}
			if tickErr = node.core.Tick(now); tickErr != nil {
				return tickErr
			}
			if tickErr = node.drainReady(ctx); tickErr != nil {
				return tickErr
			}
			if tickErr = node.publishStatus(); tickErr != nil {
				return tickErr
			}
			node.resetTimer(now)
		case rawEvent := <-node.events:
			switch event := rawEvent.(type) {
			case inboundRPCEvent:
				if ctx.Err() != nil {
					event.response <- ErrStopped
					return nil
				}
				if event.ctx.Err() != nil {
					event.response <- event.ctx.Err()
					continue
				}
				if ctx.Err() != nil {
					event.response <- ErrStopped
					return nil
				}
				stepErr := node.core.Step(event.senderID, event.rpc)
				if stepErr != nil {
					if fatalCoreError(stepErr) {
						return stepErr
					}
					event.response <- stepErr
					continue
				}
				if stepErr = node.drainReady(ctx); stepErr != nil {
					return stepErr
				}
				if stepErr = node.publishStatus(); stepErr != nil {
					return stepErr
				}
				now, clockErr := node.logicalNow()
				if clockErr != nil {
					return clockErr
				}
				node.resetTimer(now)
				event.response <- nil
			case localRequestEvent:
				if ctx.Err() != nil {
					node.releaseLocalReservation()
					event.response <- localRequestResponse{err: ErrStopped}
					return nil
				}
				if event.ctx.Err() != nil {
					node.releaseLocalReservation()
					event.response <- localRequestResponse{err: event.ctx.Err()}
					continue
				}
				if ctx.Err() != nil {
					node.releaseLocalReservation()
					event.response <- localRequestResponse{err: ErrStopped}
					return nil
				}
				proposalID, entry, proposalErr := node.core.proposeTracked(event.kind, event.command)
				if proposalErr != nil {
					node.releaseLocalReservation()
					event.response <- localRequestResponse{err: node.localRequestError(proposalErr)}
					continue
				}
				node.pendingLocal[proposalID] = pendingLocalRequest{entry: entry.Clone(), response: event.response}
				if proposalErr = node.drainReady(ctx); proposalErr != nil {
					return proposalErr
				}
				if proposalErr = node.publishStatus(); proposalErr != nil {
					return proposalErr
				}
				now, clockErr := node.logicalNow()
				if clockErr != nil {
					return clockErr
				}
				node.resetTimer(now)
			case leadershipRequestEvent:
				if ctx.Err() != nil {
					event.response <- leadershipRequestResponse{err: ErrStopped}
					return nil
				}
				if event.ctx.Err() != nil {
					event.response <- leadershipRequestResponse{err: event.ctx.Err()}
					continue
				}
				if ctx.Err() != nil {
					event.response <- leadershipRequestResponse{err: ErrStopped}
					return nil
				}
				subscription, subscribeErr := node.addLeadershipSubscription(event.ctx, event.capacity)
				event.response <- leadershipRequestResponse{subscription: subscription, err: subscribeErr}
			case leadershipCancelEvent:
				if ctx.Err() != nil {
					return nil
				}
				if subscriber := node.subscribers[event.id]; subscriber != nil {
					node.terminateLeadershipSubscriber(subscriber, event.err)
				}
			case snapshotCaptureResult:
				if ctx.Err() != nil {
					return nil
				}
				if captureErr := node.finishSnapshotCapture(event); captureErr != nil {
					return captureErr
				}
				if captureErr := node.drainReady(ctx); captureErr != nil {
					return captureErr
				}
				if publishErr := node.publishStatus(); publishErr != nil {
					return publishErr
				}
			}
		}
	}
}

// prepare recovers durable/application state exactly once without starting a
// timer, campaign, goroutine, or lifecycle-ready signal.
func (node *Node) prepare() error {
	node.prepareOnce.Do(func() {
		node.prepareErr = node.recoverPreparedState()
	})
	return node.prepareErr
}

func (node *Node) recoverPreparedState() error {
	recovered, err := node.options.Store.Recover()
	if err != nil {
		return fmt.Errorf("recover raft stable store: %w", err)
	}
	if err := validateRecoveredStateWithSnapshotLimit(recovered, node.options.Identity, node.options.Voters, node.options.MaxSnapshotBytes); err != nil {
		return err
	}
	if recovered.SnapshotBase.LastIncludedIndex != 0 && recovered.Snapshot == nil {
		return fmt.Errorf("%w: recovered snapshot index=%d schema=%d", ErrSnapshotUnavailable, recovered.SnapshotBase.LastIncludedIndex, recovered.SnapshotBase.StateMachineSchemaVersion)
	}
	if recovered.Snapshot != nil {
		decoded, snapshotErr := DecodeSnapshotEnvelope(recovered.Snapshot.EnvelopeBytes(), node.options.Identity, node.options.MaxSnapshotBytes)
		if snapshotErr != nil || decoded.Metadata != recovered.SnapshotBase || decoded.ID != recovered.Snapshot.ID {
			return fmt.Errorf("%w: recovered snapshot validation failed: %v", ErrSnapshotUnavailable, snapshotErr)
		}
		recovered.Snapshot = &decoded
	}
	recoveryApplied := recovered.SnapshotBase.LastIncludedIndex
	log, err := NewLog(
		recovered.SnapshotBase.LastIncludedIndex,
		recovered.SnapshotBase.LastIncludedTerm,
		recovered.HardState.CommitIndex,
		recoveryApplied,
		recovered.Entries,
	)
	if err != nil {
		return fmt.Errorf("build recovered raft log: %w", err)
	}
	node.core, err = node.newCore(CoreOptions{
		LocalID: node.options.LocalID, Voters: node.options.Voters,
		HardState: recovered.HardState, Log: log, AppliedIndex: recoveryApplied,
		ElectionTimeoutMin: uint64(node.options.ElectionTimeoutMin),
		ElectionTimeoutMax: uint64(node.options.ElectionTimeoutMax),
		HeartbeatInterval:  uint64(node.options.HeartbeatInterval),
		MaxAppendEntries:   node.options.MaxAppendEntries,
		MaxAppendBytes:     node.options.MaxAppendBytes,
		Random:             node.options.Random,
	})
	if err != nil {
		return err
	}
	node.durableState = recovered.Clone()
	restoreSchema := uint32(0)
	var restoreBytes []byte
	if recovered.Snapshot != nil {
		restoreSchema = recovered.Snapshot.Metadata.StateMachineSchemaVersion
		restoreBytes = recovered.Snapshot.StateBytes()
	}
	if err := node.options.StateMachine.Restore(restoreSchema, restoreBytes); err != nil {
		return fmt.Errorf("restore raft application snapshot: %w", err)
	}
	if err := node.replayRecovered(recovered); err != nil {
		return err
	}
	if err := node.core.log.AdvanceApplied(recovered.HardState.CommitIndex); err != nil {
		return fmt.Errorf("establish recovered application seam: %w", err)
	}
	node.pendingLocal = make(map[ProposalID]pendingLocalRequest)
	node.subscribers = make(map[uint64]*leadershipSubscriber)
	if err := node.publishStatus(); err != nil {
		return err
	}
	return nil
}

func (node *Node) replayRecovered(recovered RecoveredState) error {
	commitIndex := recovered.HardState.CommitIndex
	for _, entry := range recovered.Entries {
		if entry.Index > commitIndex {
			break
		}
		if entry.Kind == EntryNoOp {
			continue
		}
		if _, err := node.options.StateMachine.Apply(entry.Index, entry.Term, entry.CommandBytes()); err != nil {
			return fmt.Errorf("apply recovered entry %d: %w", entry.Index, err)
		}
	}
	return nil
}

type snapshotCaptureResult struct {
	snapshot Snapshot
	err      error
}

type retainedWALByteReporter interface {
	RetainedWALBytes() (uint64, error)
}

func (node *Node) maybeStartSnapshotCapture() (bool, error) {
	if node.captureInFlight {
		return false, nil
	}
	state := node.core.LogState()
	if state.AppliedIndex < state.SnapshotIndex {
		return false, fmt.Errorf("%w: applied index is below snapshot base", ErrLogInvariant)
	}
	if state.AppliedIndex == state.SnapshotIndex {
		return false, nil
	}
	entryTriggered := state.AppliedIndex-state.SnapshotIndex > node.options.SnapshotEntryThreshold
	byteTriggered := false
	if reporter, ok := node.options.Store.(retainedWALByteReporter); ok {
		retained, err := reporter.RetainedWALBytes()
		if err != nil {
			return false, fmt.Errorf("measure retained raft WAL: %w", err)
		}
		byteTriggered = retained > node.options.SnapshotByteThreshold
	}
	if !entryTriggered && !byteTriggered {
		return false, nil
	}
	term, err := node.core.log.Term(state.AppliedIndex)
	if err != nil {
		return false, err
	}
	capture, err := node.options.StateMachine.Capture(state.AppliedIndex, term)
	if err != nil {
		return false, fmt.Errorf("capture raft application at %d: %w", state.AppliedIndex, err)
	}
	if capture == nil {
		return false, fmt.Errorf("%w: application returned a nil snapshot capture", ErrInvalidSnapshot)
	}
	schema := capture.SchemaVersion()
	if schema == 0 {
		return false, fmt.Errorf("%w: application returned zero snapshot schema", ErrInvalidSnapshot)
	}
	metadata := SnapshotMetadata{LastIncludedIndex: state.AppliedIndex, LastIncludedTerm: term, StateMachineSchemaVersion: schema}
	node.captureInFlight = true
	node.watchers.Add(1)
	go func() {
		defer node.watchers.Done()
		stateBytes, marshalErr := capture.MarshalBinary()
		if marshalErr == nil && capture.SchemaVersion() != schema {
			marshalErr = fmt.Errorf("%w: snapshot schema changed during encoding", ErrInvalidSnapshot)
		}
		var snapshot Snapshot
		if marshalErr == nil {
			snapshot, marshalErr = NewSnapshot(node.options.Identity, metadata, stateBytes, node.options.MaxSnapshotBytes)
		}
		result := snapshotCaptureResult{snapshot: snapshot, err: marshalErr}
		select {
		case node.events <- result:
		case <-node.done:
		}
	}()
	return true, nil
}

func (node *Node) finishSnapshotCapture(result snapshotCaptureResult) error {
	if !node.captureInFlight {
		return fmt.Errorf("%w: no snapshot capture is active", ErrInvalidCoreState)
	}
	node.captureInFlight = false
	if result.err != nil {
		return fmt.Errorf("encode raft application snapshot: %w", result.err)
	}
	if result.snapshot.Metadata.LastIncludedIndex <= node.core.log.SnapshotIndex() {
		return nil
	}
	if result.snapshot.Metadata.LastIncludedIndex > node.core.log.AppliedIndex() {
		return fmt.Errorf("%w: completed snapshot is ahead of applied state", ErrInvalidCoreState)
	}
	prospective, err := compactRecoveredState(node.durableState, result.snapshot, node.options.Identity, node.options.Voters, node.options.MaxSnapshotBytes)
	if err != nil {
		return fmt.Errorf("validate completed raft snapshot: %w", err)
	}
	store, ok := node.options.Store.(interface{ PersistSnapshot(Snapshot) error })
	if !ok {
		return fmt.Errorf("%w: stable store lacks snapshot persistence", ErrInvalidCoreState)
	}
	if err := store.PersistSnapshot(result.snapshot.Clone()); err != nil {
		return fmt.Errorf("persist raft snapshot at %d: %w", result.snapshot.Metadata.LastIncludedIndex, err)
	}
	if err := node.core.CompactSnapshot(result.snapshot.Metadata); err != nil {
		return err
	}
	node.durableState = prospective
	return nil
}

func (node *Node) drainReady(ctx context.Context) error {
	for {
		ready, ok := node.core.Ready()
		if !ok {
			if err := node.core.Err(); err != nil {
				return err
			}
			started, err := node.maybeStartSnapshotTransfer()
			if err != nil {
				return err
			}
			if started {
				continue
			}
			_, err = node.maybeStartSnapshotCapture()
			return err
		}
		batch, prospectiveDurable, err := node.validateReadyAndDerive(ready)
		if err != nil {
			return err
		}
		if persistenceBatchHasEffects(batch) {
			if err := node.options.Store.Persist(batch); err != nil {
				return fmt.Errorf("persist raft Ready %d: %w", ready.Token, err)
			}
			node.durableState = prospectiveDurable
		}
		for _, action := range ready.SnapshotActions {
			message, err := node.executeSnapshotAction(ready.Token, action)
			if err != nil {
				return err
			}
			if err := node.handoffPeerMessage(message); err != nil {
				return err
			}
		}
		for _, message := range ready.Messages {
			if err := node.handoffPeerMessage(message); err != nil {
				return err
			}
		}
		applied := make(map[uint64]ProposalResult, len(ready.CommittedEntries))
		for _, entry := range ready.CommittedEntries {
			var result []byte
			if entry.Kind == EntryCommand {
				applicationResult, err := node.options.StateMachine.Apply(entry.Index, entry.Term, entry.CommandBytes())
				if err != nil {
					return fmt.Errorf("apply committed entry %d: %w", entry.Index, err)
				}
				result = cloneBytes(applicationResult)
			}
			applied[entry.Index] = ProposalResult{Index: entry.Index, Term: entry.Term, Result: result}
		}
		for _, committed := range ready.CommittedProposals {
			pending, exists := node.pendingLocal[committed.ID]
			if !exists || !sameEntry(pending.entry, committed.Entry) {
				return fmt.Errorf("%w: committed proposal %d has no exact local waiter", ErrInvalidCoreState, committed.ID)
			}
			result, exists := applied[committed.Entry.Index]
			if !exists || result.Term != committed.Entry.Term {
				return fmt.Errorf("%w: committed proposal %d has no exact apply result", ErrInvalidCoreState, committed.ID)
			}
			result.Result = cloneBytes(result.Result)
			pending.response <- localRequestResponse{result: result}
			if node.afterResult != nil {
				node.afterResult(committed.ID)
			}
			delete(node.pendingLocal, committed.ID)
			node.releaseLocalReservation()
		}
		for _, failed := range ready.FailedProposals {
			pending, exists := node.pendingLocal[failed.ID]
			if !exists || !sameEntry(pending.entry, failed.Entry) {
				return fmt.Errorf("%w: failed proposal %d has no exact local waiter", ErrInvalidCoreState, failed.ID)
			}
			pending.response <- localRequestResponse{err: failed.Err}
			delete(node.pendingLocal, failed.ID)
			node.releaseLocalReservation()
		}
		if err := node.core.Advance(ready.Token); err != nil {
			return err
		}
		if node.afterAdvance != nil {
			node.afterAdvance(ready.Token)
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

func (node *Node) maybeStartSnapshotTransfer() (bool, error) {
	if node.core.Status().Role != RoleLeader {
		return false, nil
	}
	for _, voter := range node.options.Voters.Voters() {
		if voter.ID == node.options.LocalID {
			continue
		}
		progress, ok := node.core.Progress(voter.ID)
		if !ok || !progress.SnapshotNeeded || !progress.ActiveTransferID.IsZero() {
			continue
		}
		snapshot := node.durableState.Snapshot
		if snapshot == nil || snapshot.Metadata != node.durableState.SnapshotBase ||
			snapshot.Metadata.LastIncludedIndex != node.core.log.SnapshotIndex() ||
			snapshot.Metadata.LastIncludedTerm != node.core.log.SnapshotTerm() {
			return false, fmt.Errorf("%w: snapshot-needed peer has no exact durable snapshot", ErrSnapshotUnavailable)
		}
		if uint64(len(snapshot.state)) > node.options.MaxSnapshotBytes {
			return false, fmt.Errorf("%w: durable snapshot exceeds configured maximum", ErrSnapshotUnavailable)
		}
		if node.options.TransferIDs == nil {
			return false, fmt.Errorf("%w: snapshot transfer source is nil", ErrTransferIDExhausted)
		}
		transferID, err := node.options.TransferIDs.NextTransferID(voter.ID)
		if err != nil || transferID.IsZero() {
			return false, fmt.Errorf("%w: peer %d: %v", ErrTransferIDExhausted, voter.ID, err)
		}
		chunkLimit := node.options.MaxSnapshotChunkBytes
		if chunkLimit == 0 {
			chunkLimit = config.MaxRaftSnapshotChunkBytes
		}
		if err := node.core.StartSnapshotTransfer(voter.ID, snapshot.Clone(), transferID, chunkLimit); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

type snapshotStagingStore interface {
	StageSnapshotChunk(InstallSnapshotRequest) (SnapshotStageResult, error)
	AbortSnapshotStage() error
}

func (node *Node) executeSnapshotAction(token ReadyToken, action SnapshotAction) (PeerMessage, error) {
	store, ok := node.options.Store.(snapshotStagingStore)
	if !ok {
		return PeerMessage{}, fmt.Errorf("%w: stable store lacks snapshot staging", ErrInvalidCoreState)
	}
	if action.Reset {
		if err := store.AbortSnapshotStage(); err != nil {
			return PeerMessage{}, fmt.Errorf("reset raft snapshot stage: %w", err)
		}
	}
	if action.Kind == SnapshotActionAbort {
		if err := store.AbortSnapshotStage(); err != nil {
			return PeerMessage{}, fmt.Errorf("abort raft snapshot stage: %w", err)
		}
		return node.core.CompleteSnapshotAction(token, SnapshotActionResult{Rejected: true})
	}
	if action.Kind != SnapshotActionStage {
		return PeerMessage{}, fmt.Errorf("%w: unknown snapshot action %d", ErrInvalidCoreState, action.Kind)
	}
	staged, err := store.StageSnapshotChunk(action.Request)
	if err != nil {
		if errors.Is(err, ErrSnapshotRejected) {
			return node.core.CompleteSnapshotAction(token, SnapshotActionResult{Rejected: true})
		}
		return PeerMessage{}, fmt.Errorf("stage raft snapshot at offset %d: %w", action.Request.Offset, err)
	}
	result := SnapshotActionResult{NextOffset: staged.NextOffset, Done: staged.Done}
	if staged.Done {
		if staged.State.Snapshot == nil {
			return PeerMessage{}, fmt.Errorf("%w: installed snapshot bytes are absent", ErrInvalidCoreState)
		}
		if err := validateRecoveredStateWithSnapshotLimit(staged.State, node.options.Identity, node.options.Voters, node.options.MaxSnapshotBytes); err != nil {
			return PeerMessage{}, fmt.Errorf("%w: installed snapshot state: %v", ErrInvalidCoreState, err)
		}
		installed := staged.State.Clone()
		if err := node.options.StateMachine.Restore(installed.Snapshot.Metadata.StateMachineSchemaVersion, installed.Snapshot.StateBytes()); err != nil {
			return PeerMessage{}, fmt.Errorf("restore installed raft snapshot: %w", err)
		}
		if err := node.replayRecovered(installed); err != nil {
			return PeerMessage{}, fmt.Errorf("replay installed raft snapshot suffix: %w", err)
		}
		coreState := installed.Clone()
		coreState.AppliedIndex = coreState.HardState.CommitIndex
		result.State = coreState
		node.durableState = installed
	}
	return node.core.CompleteSnapshotAction(token, result)
}

func (node *Node) handoffPeerMessage(message PeerMessage) error {
	owned := PeerMessage{To: message.To, RPC: CloneRPC(message.RPC), Requires: message.Requires}
	result, err := node.options.Transport.Handoff(owned)
	if err != nil {
		return fmt.Errorf("handoff raft message to voter %d: %w", message.To, err)
	}
	if result != TransportAccepted && result != TransportUnavailable {
		return fmt.Errorf("%w: %d", ErrTransportInvariant, result)
	}
	return nil
}

func persistenceBatchForReady(ready Ready) PersistenceBatch {
	batch := PersistenceBatch{}
	if ready.HardState != nil {
		hardState := *ready.HardState
		batch.HardState = &hardState
	}
	if len(ready.Entries) != 0 {
		batch.ReplaceFrom = ready.Entries[0].Index
		batch.Entries = cloneEntries(ready.Entries)
	}
	return batch
}

func (node *Node) validateReadyStructure(ready Ready) error {
	if ready.Token == 0 {
		return ErrAdvanceToken
	}
	for index, entry := range ready.Entries {
		if err := validateLogEntry(entry); err != nil {
			return err
		}
		if index != 0 {
			expected, ok := checkedNextIndex(ready.Entries[index-1].Index)
			if !ok || entry.Index != expected {
				return fmt.Errorf("%w: unstable entries are not contiguous", ErrLogGap)
			}
		}
	}
	for _, message := range ready.Messages {
		if !node.options.Voters.Contains(message.To) || message.To == node.options.LocalID || message.RPC == nil {
			return fmt.Errorf("%w: invalid outbound voter %d", ErrTransportInvariant, message.To)
		}
		if message.Requires.HardState && ready.HardState == nil {
			return fmt.Errorf("%w: message requires absent HardState", ErrInvalidCoreState)
		}
		if message.Requires.EntriesThrough != 0 {
			found := false
			for _, entry := range ready.Entries {
				if entry.Index == message.Requires.EntriesThrough {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("%w: message requires absent entry %d", ErrInvalidCoreState, message.Requires.EntriesThrough)
			}
		}
	}
	if len(ready.SnapshotActions) > 1 {
		return fmt.Errorf("%w: Ready contains multiple snapshot actions", ErrInvalidCoreState)
	}
	for _, action := range ready.SnapshotActions {
		if action.Kind != SnapshotActionStage && action.Kind != SnapshotActionAbort {
			return fmt.Errorf("%w: invalid snapshot action %d", ErrInvalidCoreState, action.Kind)
		}
		if action.Request.LeaderID == node.options.LocalID || !node.options.Voters.Contains(action.Request.LeaderID) {
			return fmt.Errorf("%w: invalid snapshot action leader %d", ErrInvalidCoreState, action.Request.LeaderID)
		}
		limits := DefaultCodecLimits()
		limits.MaxSnapshotBytes = node.options.MaxSnapshotBytes
		if err := validateSnapshotRequest(action.Request, limits); err != nil {
			return err
		}
	}
	expected, ok := checkedNextIndex(node.core.Status().AppliedIndex)
	if !ok && len(ready.CommittedEntries) != 0 {
		return ErrLogOverflow
	}
	for _, entry := range ready.CommittedEntries {
		if err := validateLogEntry(entry); err != nil {
			return err
		}
		if entry.Index != expected {
			return fmt.Errorf("%w: committed apply index=%d want=%d", ErrLogInvariant, entry.Index, expected)
		}
		next, nextOK := checkedNextIndex(expected)
		if !nextOK && entry.Index != ready.CommittedEntries[len(ready.CommittedEntries)-1].Index {
			return ErrLogOverflow
		}
		expected = next
	}
	seenProposals := make(map[ProposalID]struct{}, len(ready.CommittedProposals)+len(ready.FailedProposals))
	for _, proposal := range ready.CommittedProposals {
		if proposal.ID == 0 {
			return fmt.Errorf("%w: committed proposal has zero identity", ErrInvalidCoreState)
		}
		if _, duplicate := seenProposals[proposal.ID]; duplicate {
			return fmt.Errorf("%w: duplicate proposal identity %d", ErrInvalidCoreState, proposal.ID)
		}
		seenProposals[proposal.ID] = struct{}{}
		pending, exists := node.pendingLocal[proposal.ID]
		if !exists || !sameEntry(pending.entry, proposal.Entry) {
			return fmt.Errorf("%w: committed proposal %d has no exact local waiter", ErrInvalidCoreState, proposal.ID)
		}
		matched := false
		for _, entry := range ready.CommittedEntries {
			if sameEntry(proposal.Entry, entry) {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("%w: committed proposal %d has no exact apply entry", ErrInvalidCoreState, proposal.ID)
		}
	}
	for _, proposal := range ready.FailedProposals {
		if proposal.ID == 0 || proposal.Err == nil {
			return fmt.Errorf("%w: invalid failed proposal identity or error", ErrInvalidCoreState)
		}
		if _, duplicate := seenProposals[proposal.ID]; duplicate {
			return fmt.Errorf("%w: duplicate proposal identity %d", ErrInvalidCoreState, proposal.ID)
		}
		seenProposals[proposal.ID] = struct{}{}
		pending, exists := node.pendingLocal[proposal.ID]
		if !exists || !sameEntry(pending.entry, proposal.Entry) {
			return fmt.Errorf("%w: failed proposal %d has no exact local waiter", ErrInvalidCoreState, proposal.ID)
		}
	}
	return nil
}

func (node *Node) lifecycleError() error {
	if node == nil {
		return ErrNotRunning
	}
	switch node.lifecycle.Load() {
	case nodeLifecycleRunning:
		return nil
	case nodeLifecycleStopped:
		return ErrStopped
	default:
		return ErrNotRunning
	}
}

func (node *Node) submitLocal(ctx context.Context, kind EntryKind, command []byte) (ProposalResult, error) {
	if err := node.lifecycleError(); err != nil {
		return ProposalResult{}, err
	}
	if ctx == nil {
		return ProposalResult{}, context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return ProposalResult{}, err
	}
	if !node.reserveLocalRequest() {
		return ProposalResult{}, ErrOverloaded
	}
	event := localRequestEvent{
		ctx: ctx, kind: kind, command: cloneBytes(command), response: make(chan localRequestResponse, 1),
	}
	if err := node.enqueuePublicEvent(ctx, event); err != nil {
		node.releaseLocalReservation()
		return ProposalResult{}, err
	}
	select {
	case response := <-event.response:
		response.result.Result = cloneBytes(response.result.Result)
		return response.result, response.err
	case <-ctx.Done():
		return ProposalResult{}, ctx.Err()
	case <-node.done:
		return ProposalResult{}, ErrStopped
	}
}

func (node *Node) reserveLocalRequest() bool {
	for {
		current := node.pendingReservations.Load()
		if current >= MaxPendingLocalRequests {
			return false
		}
		if node.pendingReservations.CompareAndSwap(current, current+1) {
			return true
		}
	}
}

func (node *Node) enqueuePublicEvent(ctx context.Context, event any) error {
	node.apiMu.Lock()
	defer node.apiMu.Unlock()
	if err := node.lifecycleError(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case node.events <- event:
		return nil
	default:
		return ErrOverloaded
	}
}

func (node *Node) releaseLocalReservation() {
	if remaining := node.pendingReservations.Add(-1); remaining < 0 {
		panic("raft node local reservation underflow")
	}
}

func (node *Node) releasePendingReservations() {
	for range node.pendingLocal {
		node.releaseLocalReservation()
	}
	node.pendingLocal = nil
	for {
		select {
		case rawEvent := <-node.events:
			if _, ok := rawEvent.(localRequestEvent); ok {
				node.releaseLocalReservation()
			}
		default:
			return
		}
	}
}

func (node *Node) localRequestError(err error) error {
	if errors.Is(err, ErrNotLeader) {
		return &NotLeaderError{LeaderID: node.core.Status().LeaderID}
	}
	return err
}

func (node *Node) publishStatus() error {
	status := node.core.Status()
	next := LeadershipEvent{
		Term: status.Term, Role: status.Role, LeaderID: status.LeaderID, LocalID: node.options.LocalID,
	}
	if !node.hasLeadership {
		next.Sequence = 1
		node.leadership = next
		node.hasLeadership = true
		node.status.Store(status)
		return nil
	}
	if next.Term < node.leadership.Term {
		return fmt.Errorf("%w: leadership term regressed from %d to %d", ErrInvalidCoreState, node.leadership.Term, next.Term)
	}
	if next.Term == node.leadership.Term && next.Role == node.leadership.Role && next.LeaderID == node.leadership.LeaderID {
		node.status.Store(status)
		return nil
	}
	if node.leadership.Sequence == math.MaxUint64 {
		return ErrLeadershipSequenceOverflow
	}
	next.Sequence = node.leadership.Sequence + 1
	node.leadership = next
	for _, subscriber := range node.subscribers {
		select {
		case subscriber.events <- next:
		default:
			node.terminateLeadershipSubscriber(subscriber, ErrLeadershipResyncRequired)
		}
	}
	node.status.Store(status)
	return nil
}

func (node *Node) addLeadershipSubscription(ctx context.Context, capacity int) (*LeadershipSubscription, error) {
	if node.nextSubscriberID == math.MaxUint64 {
		return nil, ErrLeadershipSequenceOverflow
	}
	node.nextSubscriberID++
	subscriber := &leadershipSubscriber{
		id: node.nextSubscriberID, events: make(chan LeadershipEvent, capacity), done: make(chan struct{}), stop: make(chan struct{}),
	}
	node.subscribers[subscriber.id] = subscriber
	var stopOnce sync.Once
	subscription := &LeadershipSubscription{
		snapshot: node.leadership,
		events:   subscriber.events,
		done:     subscriber.done,
		mu:       &subscriber.errMu,
		err:      &subscriber.err,
		unsubscribe: func() {
			stopOnce.Do(func() { close(subscriber.stop) })
		},
	}
	node.watchers.Add(1)
	go func() {
		defer node.watchers.Done()
		var terminalErr error
		select {
		case <-ctx.Done():
			terminalErr = ctx.Err()
		case <-subscriber.stop:
		case <-subscriber.done:
			return
		case <-node.done:
			return
		}
		select {
		case node.events <- leadershipCancelEvent{id: subscriber.id, err: terminalErr}:
		case <-subscriber.done:
		case <-node.done:
		}
	}()
	return subscription, nil
}

func (node *Node) terminateLeadershipSubscriber(subscriber *leadershipSubscriber, err error) {
	if node.subscribers[subscriber.id] == nil {
		return
	}
	delete(node.subscribers, subscriber.id)
	subscriber.errMu.Lock()
	subscriber.err = err
	subscriber.errMu.Unlock()
	close(subscriber.events)
	close(subscriber.done)
}

func (node *Node) terminateLeadershipSubscriptions(err error) {
	for _, subscriber := range node.subscribers {
		node.terminateLeadershipSubscriber(subscriber, err)
	}
}

func (node *Node) logicalNow() (uint64, error) {
	now := node.options.Clock.Now()
	if now.Before(node.epoch) {
		return 0, fmt.Errorf("%w: clock moved before node epoch", ErrTickRegression)
	}
	logical := uint64(now.Sub(node.epoch))
	if node.core != nil && logical < node.core.now {
		return 0, fmt.Errorf("%w: clock=%d core=%d", ErrTickRegression, logical, node.core.now)
	}
	return logical, nil
}

func (node *Node) nextTimerDuration(now uint64) time.Duration {
	deadline := node.core.electionDeadline
	if node.core.role == RoleLeader {
		deadline = node.core.heartbeatDeadline
		if node.core.quorumDeadline < deadline {
			deadline = node.core.quorumDeadline
		}
	}
	if deadline <= now {
		return 0
	}
	delta := deadline - now
	if delta > uint64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(delta)
}

func (node *Node) resetTimer(now uint64) {
	if node.timer.Stop() {
		// No delivered value remains to drain.
	} else {
		select {
		case <-node.timer.C():
		default:
		}
	}
	node.timer.Reset(node.nextTimerDuration(now))
}

func (node *Node) stopTimer() {
	if node.timer == nil {
		return
	}
	if !node.timer.Stop() {
		select {
		case <-node.timer.C():
		default:
		}
	}
}

func fatalCoreError(err error) bool {
	return errors.Is(err, ErrCommittedConflict) ||
		errors.Is(err, ErrLogInvariant) ||
		errors.Is(err, ErrLogOverflow) ||
		errors.Is(err, ErrTermOverflow) ||
		errors.Is(err, ErrDeadlineOverflow) ||
		errors.Is(err, ErrReplicationGenerationOverflow) ||
		errors.Is(err, ErrReadyTokenExhausted) ||
		errors.Is(err, ErrInvalidCoreState)
}
