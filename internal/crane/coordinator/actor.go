// Package coordinator implements Crane's fenced leader actor. The actor is an
// idle follower until Raft leadership arrives, then owns exactly one
// epoch-scoped reconciliation loop that registers workers, fences the +3
// control plane, opens the caller-owned admission gate once the cluster phase
// has run, and then converges replicated assignments and repairs result
// replicas with the gate held open for the rest of the epoch.
package coordinator

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/aadityakv/crane/internal/clock"
	"github.com/aadityakv/crane/internal/crane/admission"
	"github.com/aadityakv/crane/internal/crane/membership"
	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/state"
	"github.com/aadityakv/crane/internal/raft"
	"github.com/aadityakv/crane/internal/swim"
)

const (
	// DefaultFailureGracePeriod bounds how long a continuously Dead/Left and
	// control-unreachable worker survives before conditional deactivation.
	DefaultFailureGracePeriod = 5 * time.Second
	// DefaultRescanInterval paces the guaranteed periodic full-view rescan.
	DefaultRescanInterval = 500 * time.Millisecond

	leadershipSubscriptionCapacity = 64
	membershipSubscriptionCapacity = 256
	statusPageEvents               = 64
	repairPollLimit                = 8
	resolveAttemptLimit            = 3
	establishAttemptLimit          = 64
	retryPause                     = 10 * time.Millisecond

	commandIdentityDomain = "crane/coordinator/command-id/v1\x00"
)

// errLeaderSessionAborted reports a leader loop that ended while leadership
// was still held: the nonce source failed or the coordinator epoch could not
// be established within establishAttemptLimit paced attempts.
var errLeaderSessionAborted = errors.New("coordinator leader session aborted")

// LeadershipSubscription is the leadership stream contract the actor consumes.
// *raft.LeadershipSubscription satisfies it directly.
type LeadershipSubscription interface {
	Snapshot() raft.LeadershipEvent
	Events() <-chan raft.LeadershipEvent
	Done() <-chan struct{}
	Err() error
	Unsubscribe()
}

// Raft is the bounded consensus surface the actor requires.
type Raft interface {
	Ready() <-chan struct{}
	Barrier(context.Context) (uint64, error)
	Propose(context.Context, []byte) (raft.ProposalResult, error)
	SubscribeLeadership(context.Context, int) (LeadershipSubscription, error)
}

// MembershipSubscription is one bounded authorizer delta stream with scoped
// complete-snapshot recovery. *membership.Subscription satisfies it directly.
type MembershipSubscription interface {
	Events() <-chan membership.Event
	Snapshot(context.Context) (membership.View, error)
}

// Membership is the authorizer surface the actor requires.
type Membership interface {
	View() membership.View
	Subscribe(context.Context, int) (MembershipSubscription, error)
}

// NonceSource yields leadership nonces: cryptographic in production and
// scripted in tests.
type NonceSource interface {
	Nonce() ([16]byte, error)
}

// CryptoNonceSource is the production cryptographic nonce source.
type CryptoNonceSource struct{}

// Nonce returns one uniformly random nonzero 16-byte nonce.
func (CryptoNonceSource) Nonce() ([16]byte, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return [16]byte{}, fmt.Errorf("generate coordinator nonce: %w", err)
	}
	if nonce == ([16]byte{}) {
		return [16]byte{}, errors.New("generated zero coordinator nonce")
	}
	return nonce, nil
}

// ActorOptions fixes every caller-owned dependency of one coordinator actor.
type ActorOptions struct {
	NodeID      uint16          // NodeID is the local configured voter identity.
	Raft        Raft            // Raft provides readiness, barriers, proposals, and leadership.
	Machine     *state.Machine  // Machine is the local replicated Crane state machine.
	WorkerReady <-chan struct{} // WorkerReady gates startup on the co-located worker service.
	Membership  Membership      // Membership provides the authorized SWIM projection.
	Workers     WorkerClient    // Workers speaks the authenticated +3 control protocol.
	Clock       clock.Clock     // Clock supplies time and rescan timers.
	Nonces      NonceSource     // Nonces yields the stable per-leadership epoch nonce.
	Gate        *admission.Gate // Gate is the caller-owned process admission gate.
	// Results optionally supplies the authenticated +3 result-artifact
	// transfer surface; without it the terminal seal workflow stays disabled
	// and terminal jobs simply remain Draining.
	Results ResultTransferClient

	// FailureGracePeriod overrides DefaultFailureGracePeriod when positive.
	FailureGracePeriod time.Duration
	// RescanInterval overrides DefaultRescanInterval when positive.
	RescanInterval time.Duration
}

// Actor is an idle follower owning at most one epoch-scoped leader loop.
type Actor struct {
	options ActorOptions
	ready   chan struct{}
	wake    chan struct{}
	started atomic.Bool
	// events holds the leader-local per-worker durable event cursors shared
	// by the reconciliation loop and the exported polling entry points.
	events eventCursors
}

// NewActor validates the complete dependency set without side effects.
func NewActor(options ActorOptions) (*Actor, error) {
	if options.NodeID == 0 {
		return nil, errors.New("coordinator actor requires a nonzero node ID")
	}
	if options.Raft == nil || options.Machine == nil || options.WorkerReady == nil || options.Membership == nil ||
		options.Workers == nil || options.Clock == nil || options.Nonces == nil || options.Gate == nil {
		return nil, errors.New("coordinator actor requires raft, machine, worker readiness, membership, workers, clock, nonces, and gate")
	}
	if options.FailureGracePeriod <= 0 {
		options.FailureGracePeriod = DefaultFailureGracePeriod
	}
	if options.RescanInterval <= 0 {
		options.RescanInterval = DefaultRescanInterval
	}
	actor := &Actor{options: options, ready: make(chan struct{}), wake: make(chan struct{}, 1)}
	actor.events.reset()
	return actor, nil
}

// Name returns the stable supervisor registration name.
func (actor *Actor) Name() string { return "crane-coordinator" }

// Ready closes once the leadership subscription exists, without requiring
// leadership itself.
func (actor *Actor) Ready() <-chan struct{} { return actor.ready }

// Wake is a nonblocking latency hint; correctness never depends on it because
// the injected-clock periodic rescan guarantees progress.
func (actor *Actor) Wake() {
	select {
	case actor.wake <- struct{}{}:
	default:
	}
}

// Run waits for Raft and worker readiness, subscribes to leadership, and then
// serially owns one epoch-scoped leader loop per acquired leadership.
func (actor *Actor) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("run coordinator actor: nil context")
	}
	if !actor.started.CompareAndSwap(false, true) {
		return errors.New("coordinator actor Run called more than once")
	}
	select {
	case <-actor.options.Raft.Ready():
	case <-ctx.Done():
		return nil
	}
	select {
	case <-actor.options.WorkerReady:
	case <-ctx.Done():
		return nil
	}
	subscription, err := actor.options.Raft.SubscribeLeadership(ctx, leadershipSubscriptionCapacity)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("subscribe coordinator leadership: %w", err)
	}
	if subscription == nil {
		return errors.New("subscribe coordinator leadership: nil subscription")
	}
	defer subscription.Unsubscribe()
	close(actor.ready)

	var session *leaderSession
	sessionDone := func() <-chan struct{} {
		if session == nil {
			return nil
		}
		return session.done
	}
	stopSession := func() {
		if session == nil {
			return
		}
		// Close the gate before joining so no new work is admitted while the
		// epoch-owned operations are canceled and drained.
		_ = actor.options.Gate.CloseAndWait(context.Background())
		session.cancel()
		<-session.done
		session = nil
	}
	defer stopSession()

	handle := func(event raft.LeadershipEvent) {
		leading := event.Role == raft.RoleLeader && event.LocalID == actor.options.NodeID
		if !leading {
			stopSession()
			return
		}
		if session != nil && session.term == event.Term {
			return
		}
		stopSession()
		session = actor.startLeaderSession(ctx, event.Term)
	}
	handle(subscription.Snapshot())
	for {
		select {
		case <-sessionDone():
			// The leader loop ended on its own while leadership is still held.
			// A leading node must never stay idle and silent, so the session
			// is joined, its failure is surfaced by the loop's return value,
			// and a fresh session (new nonce) restarts for the same term after
			// one pause; leadership events keep taking precedence.
			term, failure := session.term, session.err
			stopSession()
			if failure == nil || !actor.pause(ctx) {
				continue
			}
			session = actor.startLeaderSession(ctx, term)
		case event, open := <-subscription.Events():
			if !open {
				stopSession()
				if ctx.Err() != nil {
					return nil
				}
				return errors.New("coordinator leadership events closed")
			}
			handle(event)
		case <-subscription.Done():
			stopSession()
			if ctx.Err() != nil {
				return nil
			}
			if err := subscription.Err(); err != nil {
				return fmt.Errorf("coordinator leadership subscription ended: %w", err)
			}
			return errors.New("coordinator leadership subscription ended")
		case <-ctx.Done():
			return nil
		}
	}
}

// leaderSession owns the cancellation and join handle of one leader loop.
// err is written once by the loop goroutine before done closes.
type leaderSession struct {
	term   uint64
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

func (actor *Actor) startLeaderSession(parent context.Context, term uint64) *leaderSession {
	sessionContext, cancel := context.WithCancel(parent)
	session := &leaderSession{term: term, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(session.done)
		session.err = actor.runLeader(sessionContext)
	}()
	return session
}

// jobFence identifies the exact replicated job state one session reconciled.
type jobFence struct {
	jobControlRevision uint64
	assignmentRevision uint64
}

// sessionState is leader-local mutable observation state. It never outlives
// one leadership session and nothing in it is authoritative: every decision is
// re-derived from the replicated Machine view and the membership view.
type sessionState struct {
	observations  map[uint16]time.Time
	controlFailed map[uint16]bool
	reconciled    map[model.JobID]jobFence
	// terminal retains, per job, the exact fence and the set of workers whose
	// durable terminal Closed install this session has already confirmed, so
	// one unreachable or rejecting worker never forces the converged workers
	// to re-commit the identical terminal install on every pass.
	terminal map[model.JobID]terminalProgress
	// lostManifests retains the manifest subjects whose terminal
	// MarkManifestLost this session has already proposed, so the terminal
	// loss declaration is proposed exactly once per leadership session and a
	// later session re-derives it from the replicated view.
	lostManifests map[lostManifestKey]bool
	// admission retains each worker's last observed process admission epoch
	// from the durable status exchange. A missing or non-current entry means
	// the worker's gate was not observed open under this leadership epoch —
	// for example after a same-epoch process restart.
	admission map[uint16]model.CoordinatorEpoch
	// handshakes retains, per member, the membership view values and the
	// verified identity this session's last completed handshake observed. A
	// later pass skips the network dial only while the member's incarnation
	// and status are unchanged and the replicated worker record still
	// carries the recorded identity's worker epoch.
	handshakes map[uint16]handshakeMemory
	// fences retains, per worker, the coordinator epoch and exact worker
	// incarnation of this session's last acknowledged fence, so later passes
	// fence only workers whose durable fence at this epoch is unproven.
	fences map[uint16]fenceMemory
	// fenceFailed retains workers whose last fence attempt this session
	// failed; they are retried on every later pass.
	fenceFailed map[uint16]bool
	members     MembershipSubscription
}

// handshakeMemory records one member's completed this-session handshake: the
// membership incarnation and status the dial was validated against and the
// authenticated identity it returned. The skip's safety relies on a replaced
// worker process always arriving with a new SWIM incarnation, enforced by the
// runtime's single-process composition and swim.PrepareJoin's durable
// incarnation advance (swim/join.go).
type handshakeMemory struct {
	incarnation uint64
	status      swim.Status
	identity    WorkerIdentity
}

// fenceMemory records one exact worker incarnation's acknowledged fence at
// one coordinator epoch within this session.
type fenceMemory struct {
	epoch       model.CoordinatorEpoch
	workerEpoch model.WorkerEpoch
}

func newSessionState() *sessionState {
	return &sessionState{
		observations:  make(map[uint16]time.Time),
		reconciled:    make(map[model.JobID]jobFence),
		terminal:      make(map[model.JobID]terminalProgress),
		lostManifests: make(map[lostManifestKey]bool),
		admission:     make(map[uint16]model.CoordinatorEpoch),
		handshakes:    make(map[uint16]handshakeMemory),
		fences:        make(map[uint16]fenceMemory),
		fenceFailed:   make(map[uint16]bool),
	}
}

// lostManifestKey identifies one sink manifest subject proposed lost.
type lostManifestKey struct {
	job  model.JobID
	sink model.TaskID
}

// settledHandshake reports the recorded handshake result for one member when
// this pass may skip the network dial: an entry exists, the member's current
// membership entry is unchanged (same incarnation and status), and the
// replicated worker record still carries the recorded identity's worker
// epoch. Any membership mismatch clears the entry so the member is
// re-handshaken; a pending registration (no replicated record yet) never
// skips the dial.
func (session *sessionState) settledHandshake(member swim.Member, members membership.View, workers state.View) (WorkerIdentity, bool) {
	memory, ok := session.handshakes[member.NodeID]
	if !ok {
		return WorkerIdentity{}, false
	}
	current, present := findMember(members, member.NodeID)
	if !present || current.Incarnation != memory.incarnation || current.Status != memory.status {
		delete(session.handshakes, member.NodeID)
		return WorkerIdentity{}, false
	}
	record, exists := findWorker(workers, member.NodeID)
	if !exists || record.Epoch != memory.identity.WorkerEpoch {
		return WorkerIdentity{}, false
	}
	return memory.identity, true
}

// needsFence reports whether one non-Offline worker must be fenced this
// pass: no fence was acknowledged this session for the exact incarnation at
// this epoch, the worker was last observed admitting under a different
// nonzero coordinator epoch, or the previous fence attempt failed.
func (session *sessionState) needsFence(worker state.WorkerRecord, epoch model.CoordinatorEpoch) bool {
	if session.fenceFailed[worker.NodeID] {
		return true
	}
	if admitted, observed := session.admission[worker.NodeID]; observed && admitted != (model.CoordinatorEpoch{}) && admitted != epoch {
		return true
	}
	acknowledged, ok := session.fences[worker.NodeID]
	return !ok || acknowledged.epoch != epoch || acknowledged.workerEpoch != worker.Epoch
}

// pruneJobs drops the per-job reconciliation and terminal-install memory of
// jobs the replicated view no longer retains. Entries for retained jobs are
// untouched, so no decision changes; the maps merely stop carrying evicted
// identities for the rest of the leadership session.
func (session *sessionState) pruneJobs(view state.View) {
	retained := make(map[model.JobID]bool, len(view.Jobs))
	for _, job := range view.Jobs {
		retained[job.JobID] = true
	}
	for jobID := range session.reconciled {
		if !retained[jobID] {
			delete(session.reconciled, jobID)
		}
	}
	for jobID := range session.terminal {
		if !retained[jobID] {
			delete(session.terminal, jobID)
		}
	}
	for key := range session.lostManifests {
		if !retained[key.job] {
			delete(session.lostManifests, key)
		}
	}
}

// terminalProgress records which workers durably confirmed one job's terminal
// Closed install at one exact fence within the current leadership session.
type terminalProgress struct {
	fence jobFence
	nodes map[uint16]bool
}

// runLeader owns one leadership epoch: barrier, stable-nonce epoch creation,
// then repeated full reconciliation passes until the session context ends.
// It returns nil when the session context ended and errLeaderSessionAborted
// when the loop gave up while leadership was still held.
func (actor *Actor) runLeader(ctx context.Context) error {
	nonce, err := actor.options.Nonces.Nonce()
	if err != nil {
		return fmt.Errorf("%w: nonce: %w", errLeaderSessionAborted, err)
	}
	if nonce == ([16]byte{}) {
		return fmt.Errorf("%w: zero nonce", errLeaderSessionAborted)
	}
	session := newSessionState()
	// A new leadership session performs a complete repoll of every worker's
	// durable events; the replicated tombstones answer the late duplicates.
	actor.events.reset()
	if subscription, subscribeErr := actor.options.Membership.Subscribe(ctx, membershipSubscriptionCapacity); subscribeErr == nil {
		session.members = subscription
	}
	epoch, ok := actor.establishEpoch(ctx, nonce)
	if !ok {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("%w: coordinator epoch not established within %d attempts", errLeaderSessionAborted, establishAttemptLimit)
	}
	// The admission gate opens exactly once per epoch, unconditionally after
	// the first cluster phase completes: a failing worker-control exchange in
	// any later phase must never lock the control plane out of admission.
	// A session context that died mid-pass never opens the gate, because the
	// owning loop is ending and the fencing close may already have happened.
	// The cluster phase itself keeps running every pass so registration,
	// event draining, and failure resolution stay live for the whole epoch.
	gateOpened := false
	for {
		actor.reconcileCluster(ctx, epoch, session)
		if !gateOpened && ctx.Err() == nil {
			_ = actor.options.Gate.Open(epoch)
			gateOpened = true
		}
		actor.reconcileJobs(ctx, epoch, session)
		actor.driveTerminalResults(ctx, epoch)
		if !actor.awaitTrigger(ctx, session) {
			return nil
		}
	}
}

// awaitTrigger waits for a wake hint, a membership delta, or the guaranteed
// periodic rescan timer. It reports whether the session should continue.
func (actor *Actor) awaitTrigger(ctx context.Context, session *sessionState) bool {
	timer := actor.options.Clock.NewTimer(actor.options.RescanInterval)
	defer timer.Stop()
	var events <-chan membership.Event
	if session.members != nil {
		events = session.members.Events()
	}
	select {
	case <-ctx.Done():
		return false
	case <-actor.wake:
	case <-timer.C():
	case event, open := <-events:
		if !open {
			// Deltas are lost: fall back to full periodic rescans only.
			session.members = nil
			return ctx.Err() == nil
		}
		if event.Cause == membership.ResyncRequired {
			// Discard buffered deltas and acknowledge scoped recovery; the
			// next pass rescans the complete view before acting.
			_, _ = session.members.Snapshot(ctx)
		}
	}
	return ctx.Err() == nil
}

// establishEpoch records a barrier and commits the stable-nonce
// BeginCoordinatorEpoch, resolving ambiguity by command identity.
func (actor *Actor) establishEpoch(ctx context.Context, nonce [16]byte) (model.CoordinatorEpoch, bool) {
	subject := state.SubjectKey{Kind: state.SubjectCoordinator}
	for attempt := 0; attempt < establishAttemptLimit; attempt++ {
		if ctx.Err() != nil {
			return model.CoordinatorEpoch{}, false
		}
		if _, err := actor.options.Raft.Barrier(ctx); err != nil {
			if !actor.pause(ctx) {
				return model.CoordinatorEpoch{}, false
			}
			continue
		}
		view := actor.options.Machine.View()
		id := internalCommandID(state.CommandBeginCoordinatorEpoch, model.CoordinatorEpoch{}, subject, view.CoordinatorRevision, nonce[:])
		command, err := state.NewBeginCoordinatorEpoch(id, view.CoordinatorRevision, actor.options.NodeID, nonce)
		if err != nil {
			return model.CoordinatorEpoch{}, false
		}
		result, err := actor.proposeResolved(ctx, id, subject, command)
		if err != nil {
			if !actor.pause(ctx) {
				return model.CoordinatorEpoch{}, false
			}
			continue
		}
		switch result.Code {
		case state.ResultSuccess:
			return result.Epoch, true
		case state.ResultStaleEpoch:
			// A newer fence exists; this leadership is already superseded.
			return model.CoordinatorEpoch{}, false
		default:
			continue
		}
	}
	return model.CoordinatorEpoch{}, false
}

// pause waits briefly before a retry without ever spinning hot.
func (actor *Actor) pause(ctx context.Context) bool {
	timer := actor.options.Clock.NewTimer(retryPause)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C():
		return true
	case <-actor.wake:
		return true
	}
}

// proposeResolved proposes one identity-bound internal command and resolves an
// ambiguous transport outcome by command identity plus Barrier and View before
// any further mutation of the same subject.
func (actor *Actor) proposeResolved(ctx context.Context, id state.InternalCommandID, subject state.SubjectKey, command any) (state.CommandResult, error) {
	encoded, err := state.MarshalCommand(command)
	if err != nil {
		return state.CommandResult{}, err
	}
	for attempt := 0; attempt < resolveAttemptLimit; attempt++ {
		result, proposeErr := actor.options.Raft.Propose(ctx, encoded)
		if proposeErr == nil {
			return state.UnmarshalCommandResult(result.Result)
		}
		if ctx.Err() != nil {
			return state.CommandResult{}, ctx.Err()
		}
		if _, barrierErr := actor.options.Raft.Barrier(ctx); barrierErr != nil {
			return state.CommandResult{}, fmt.Errorf("resolve ambiguous proposal: %w", barrierErr)
		}
		view := actor.options.Machine.View()
		for _, history := range view.Subjects {
			if history.Subject == subject && history.ID == id {
				return state.UnmarshalCommandResult(history.Result)
			}
		}
		// Not committed: retrying the identical bytes is safe by identity.
	}
	return state.CommandResult{}, errors.New("ambiguous proposal remained unresolved")
}

// internalCommandID derives a stable command identity from the complete
// defining bytes of one coordinator operation under one leadership fence.
func internalCommandID(kind state.CommandKind, epoch model.CoordinatorEpoch, subject state.SubjectKey, expectedRevision uint64, defining ...[]byte) state.InternalCommandID {
	hash := sha256.New()
	hash.Write([]byte(commandIdentityDomain))
	var scratch [8]byte
	binary.BigEndian.PutUint16(scratch[:2], uint16(kind))
	hash.Write(scratch[:2])
	binary.BigEndian.PutUint64(scratch[:], epoch.Term)
	hash.Write(scratch[:])
	binary.BigEndian.PutUint64(scratch[:], epoch.BeginIndex)
	hash.Write(scratch[:])
	binary.BigEndian.PutUint16(scratch[:2], epoch.Coordinator)
	hash.Write(scratch[:2])
	hash.Write(epoch.Nonce[:])
	hash.Write([]byte{byte(subject.Kind)})
	hash.Write(subject.JobID[:])
	hash.Write(subject.TaskID.JobID[:])
	binary.BigEndian.PutUint16(scratch[:2], subject.TaskID.StageID)
	hash.Write(scratch[:2])
	binary.BigEndian.PutUint16(scratch[:2], subject.TaskID.Partition)
	hash.Write(scratch[:2])
	binary.BigEndian.PutUint16(scratch[:2], subject.WorkerID)
	hash.Write(scratch[:2])
	binary.BigEndian.PutUint64(scratch[:], expectedRevision)
	hash.Write(scratch[:])
	for _, part := range defining {
		binary.BigEndian.PutUint64(scratch[:], uint64(len(part)))
		hash.Write(scratch[:])
		hash.Write(part)
	}
	var id state.InternalCommandID
	copy(id[:], hash.Sum(nil))
	return id
}

// authorizerMembership adapts the concrete authorizer to the actor seam.
type authorizerMembership struct {
	authorizer *membership.Authorizer
}

// NewAuthorizerMembership adapts a *membership.Authorizer for ActorOptions.
func NewAuthorizerMembership(authorizer *membership.Authorizer) Membership {
	return authorizerMembership{authorizer: authorizer}
}

// View returns the authorizer's complete owned membership view.
func (m authorizerMembership) View() membership.View { return m.authorizer.View() }

// Subscribe opens one bounded authorizer subscription.
func (m authorizerMembership) Subscribe(ctx context.Context, capacity int) (MembershipSubscription, error) {
	subscription, err := m.authorizer.Subscribe(ctx, capacity)
	if err != nil {
		return nil, err
	}
	return subscription, nil
}

// nodeRaft adapts a concrete *raft.Node to the actor's Raft seam.
type nodeRaft struct {
	node *raft.Node
}

// NewNodeRaft adapts a *raft.Node for ActorOptions.
func NewNodeRaft(node *raft.Node) Raft { return nodeRaft{node: node} }

// Ready reports Raft recovery and replay completion.
func (r nodeRaft) Ready() <-chan struct{} { return r.node.Ready() }

// Barrier submits a current-term application fence.
func (r nodeRaft) Barrier(ctx context.Context) (uint64, error) { return r.node.Barrier(ctx) }

// Propose submits one command and waits for its exact apply.
func (r nodeRaft) Propose(ctx context.Context, command []byte) (raft.ProposalResult, error) {
	return r.node.Propose(ctx, command)
}

// SubscribeLeadership opens one bounded leadership subscription.
func (r nodeRaft) SubscribeLeadership(ctx context.Context, capacity int) (LeadershipSubscription, error) {
	subscription, err := r.node.SubscribeLeadership(ctx, capacity)
	if err != nil {
		return nil, err
	}
	return subscription, nil
}

// activeMemberStatus reports whether a status admits control-plane contact.
func activeMemberStatus(status swim.Status) bool {
	return status == swim.Alive || status == swim.Suspect
}

// findMember locates one member in an owned membership view.
func findMember(view membership.View, node uint16) (swim.Member, bool) {
	for _, member := range view.Members {
		if member.NodeID == node {
			return member, true
		}
	}
	return swim.Member{}, false
}

// memberActive reports whether a node is currently Alive or Suspect.
func memberActive(view membership.View, node uint16) bool {
	member, ok := findMember(view, node)
	return ok && activeMemberStatus(member.Status)
}

// findWorker locates one replicated worker record in an owned state view.
func findWorker(view state.View, node uint16) (state.WorkerRecord, bool) {
	for _, worker := range view.Workers {
		if worker.NodeID == node {
			return worker, true
		}
	}
	return state.WorkerRecord{}, false
}

// findJob locates one replicated job record in an owned state view.
func findJob(view state.View, job model.JobID) (state.JobRecord, bool) {
	for _, record := range view.Jobs {
		if record.JobID == job {
			return record, true
		}
	}
	return state.JobRecord{}, false
}

// terminalLifecycle mirrors the replicated terminal lifecycle classification.
func terminalLifecycle(lifecycle state.JobLifecycle) bool {
	return lifecycle == state.JobSucceeded || lifecycle == state.JobFailed || lifecycle == state.JobCanceled
}
