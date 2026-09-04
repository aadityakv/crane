package worker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aadityakv/crane/internal/clock"
	"github.com/aadityakv/crane/internal/crane/admission"
	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
	"github.com/aadityakv/crane/internal/crane/store"
	"github.com/aadityakv/crane/internal/wire"
)

// ErrRepairSourceUnavailable reports a repair source that could not be
// reached or answered outside the authenticated +3 protocol.
var ErrRepairSourceUnavailable = errors.New("crane repair source unavailable")

// ErrRepairSourceRejected reports a repair source that answered one pull with
// a deterministic typed refusal (authority, epoch, assignment, or validation)
// rather than a transport failure or a retryable capacity/unavailability code.
var ErrRepairSourceRejected = errors.New("crane repair source rejected pull")

// RepairDriverRepository is the durable authority the destination-side repair
// driver reads and the +3 pull client identifies itself from.
type RepairDriverRepository interface {
	RecoverWork() (store.RecoveredWork, error)
	LocalIdentity() (uint16, model.WorkerEpoch)
	CurrentFence() model.CoordinatorEpoch
	DurableTransactionID() (uint64, error)
}

// RepairInstaller durably installs one repair record through the existing
// validated receive path.
type RepairInstaller interface {
	ReceiveResultRecord(context.Context, TransferPeer, protocol.ResultRecordChunk) (protocol.ResultRecordAck, error)
}

// RepairSourceClient performs one authenticated +3 repair pull exchange with
// the source endpoint of one durable repair grant: it submits the
// destination's durable ResultRepairStatus and returns either the source's
// next ResultRecordChunk or the terminal WorkerStatus carrying the source's
// completed repair status.
type RepairSourceClient interface {
	PullRepair(ctx context.Context, sourceNode uint16, sourceEpoch model.WorkerEpoch, status protocol.ResultRepairStatus) (protocol.WorkerMessage, error)
}

// ServeRepairPull serves one destination-driven repair pull at the source:
// it first advances the durable source cursor for the prefix the
// destination's status cryptographically proves it already installed (the
// verified inventory digest chain over the exact covered records — the
// batched per-record acknowledgment), then returns the exact next record
// chunk, or reports completion once the covered range is exhausted.
func (owner *TransferOwner) ServeRepairPull(ctx context.Context, peer TransferPeer, destination protocol.ResultRepairStatus) (protocol.ResultRecordChunk, bool, error) {
	if destination.RepairID == ([16]byte{}) {
		return protocol.ResultRecordChunk{}, false, ErrTransferUnauthorized
	}
	work, repair, err := owner.currentRepair(destination.RepairID, store.RepairSource)
	if err != nil {
		return protocol.ResultRecordChunk{}, false, err
	}
	if peer.NodeID != repair.Instruction.DestinationNodeID || peer.WorkerEpoch != repair.Instruction.DestinationWorkerEpoch || peer.Role != TransferHistoricalRepair {
		return protocol.ResultRecordChunk{}, false, ErrTransferUnauthorized
	}
	if destination.RecordCount > repair.NextRecord {
		records, inventoryErr := repairInventory(work.Results, repair.Instruction)
		if inventoryErr != nil {
			return protocol.ResultRecordChunk{}, false, inventoryErr
		}
		if destination.RecordCount > uint64(len(records)) {
			return protocol.ResultRecordChunk{}, false, ErrTransferIdentityReuse
		}
		count, total, digest, aggregateErr := ResultInventoryAggregate(repair.Instruction.InventoryQueryDigest, records[:destination.RecordCount])
		if aggregateErr != nil || count != destination.RecordCount || total != destination.TotalBytes || digest != destination.ContentDigest {
			return protocol.ResultRecordChunk{}, false, ErrTransferIdentityReuse
		}
		for index := repair.NextRecord; index < destination.RecordCount; index++ {
			provenance, provenanceErr := owner.repairDestinationProvenance(repair, records[index])
			if provenanceErr != nil {
				return protocol.ResultRecordChunk{}, false, provenanceErr
			}
			chunk, buildErr := buildResultRecordChunk(TransferHistoricalRepair, records[index], provenance, repair.Instruction.DestinationNodeID, repair.Instruction.DestinationWorkerEpoch, repair.Instruction.RepairID, repair.InstructionDigest)
			if buildErr != nil {
				return protocol.ResultRecordChunk{}, false, buildErr
			}
			// The exact correlated durable ACK the destination issued for
			// this chunk when it installed the record; AcknowledgeRepairRecord
			// revalidates the full chunk/ack pair against durable state.
			ack := protocol.ResultRecordAck{TransferID: chunk.Transfer.TransferID, NodeID: chunk.DestinationNodeID, WorkerEpoch: chunk.DestinationWorkerEpoch, RepairID: chunk.RepairID, RepairInstructionDigest: chunk.RepairInstructionDigest, NextOffset: chunk.Transfer.TotalLength, TotalLength: chunk.Transfer.TotalLength, Checksum: chunk.Transfer.Checksum, Complete: true, CoordinatorEpoch: chunk.Provenance.CoordinatorEpoch}
			if ackErr := owner.AcknowledgeRepairRecord(ctx, peer, chunk, ack); ackErr != nil {
				return protocol.ResultRecordChunk{}, false, ackErr
			}
		}
	}
	return owner.NextRepairRecord(ctx, peer, destination.RepairID, destination)
}

// RepairDriverOptions fixes the destination-side repair driver's durable
// dependencies, authenticated +3 pull client, shared gate, deterministic
// clock, and bounded retry schedule.
type RepairDriverOptions struct {
	// Repository is the sole durable worker authority.
	Repository RepairDriverRepository
	// Transfer performs every durable local record install.
	Transfer RepairInstaller
	// Client performs each authenticated +3 pull exchange.
	Client RepairSourceClient
	// Clock schedules every retry deterministically.
	Clock clock.Clock
	// RetryInterval is the initial source-failure backoff.
	RetryInterval time.Duration
	// MaxRetryInterval caps the doubling backoff.
	MaxRetryInterval time.Duration
}

// RepairDriver drives every durable destination-role repair grant to
// completion. Constructors create no goroutine, timer, file, or socket.
//
// The driver deliberately does not take the process admission gate: the
// coordinator repairs a partition while the job is fenced Closed and, after
// any leadership fence, the gate stays closed until the Running install that
// follows the repair, so a gated install would deadlock with the coordinator
// polling the grant (Task 24 defect #6). The durable grant under the current
// fence plus the transfer's own authority, RepairID/digest, and coverage
// validation (ReceiveResultRecord → receiveRepair) is the authority.
type RepairDriver struct {
	repository RepairDriverRepository
	transfer   RepairInstaller
	client     RepairSourceClient
	clock      clock.Clock
	retry      time.Duration
	maxRetry   time.Duration

	scheduled chan store.ResultRepairRecord
	mu        sync.Mutex
	queued    map[[16]byte]struct{}
	driven    map[[16]byte]struct{}
	started   atomic.Bool
}

// NewRepairDriver validates and retains the caller's exact dependencies
// without recovering durable state, dialing, or starting work.
func NewRepairDriver(options RepairDriverOptions) (*RepairDriver, error) {
	if options.Repository == nil || options.Transfer == nil || options.Client == nil || options.Clock == nil {
		return nil, errors.New("repair driver requires repository, transfer, client, and clock")
	}
	if options.RetryInterval <= 0 || options.MaxRetryInterval < options.RetryInterval {
		return nil, errors.New("repair driver retry schedule is invalid")
	}
	return &RepairDriver{
		repository: options.Repository, transfer: options.Transfer, client: options.Client,
		clock: options.Clock, retry: options.RetryInterval, maxRetry: options.MaxRetryInterval,
		scheduled: make(chan store.ResultRepairRecord, 256),
		queued:    make(map[[16]byte]struct{}), driven: make(map[[16]byte]struct{}),
	}, nil
}

// Schedule registers one durable repair grant for destination-driven
// streaming. Grants this worker holds only as the source endpoint, and
// grants already observed terminal in this process, are ignored.
func (driver *RepairDriver) Schedule(repair store.ResultRepairRecord) {
	if repair.Role != store.RepairDestination {
		return
	}
	driver.mu.Lock()
	if _, complete := driver.driven[repair.Instruction.RepairID]; complete {
		driver.mu.Unlock()
		return
	}
	if _, pending := driver.queued[repair.Instruction.RepairID]; pending {
		driver.mu.Unlock()
		return
	}
	driver.queued[repair.Instruction.RepairID] = struct{}{}
	driver.mu.Unlock()
	select {
	case driver.scheduled <- repair:
	default:
		// The bounded queue is full; drop the entry so a later coordinator
		// redelivery reschedules it once capacity drains.
		driver.mu.Lock()
		delete(driver.queued, repair.Instruction.RepairID)
		driver.mu.Unlock()
	}
}

// Run recovers every retained destination grant and drives each to
// completion serially until cancellation.
func (driver *RepairDriver) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("nil repair driver context")
	}
	if !driver.started.CompareAndSwap(false, true) {
		return errors.New("repair driver Run called more than once")
	}
	work, err := driver.repository.RecoverWork()
	if err != nil {
		return err
	}
	for _, repair := range work.Repairs {
		driver.Schedule(repair)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case repair := <-driver.scheduled:
			driver.drive(ctx, repair)
		}
	}
}

// drive streams one grant until the source reports completion, the grant's
// durable authority disappears or is superseded, the source deterministically
// refuses it, or a deterministic local validation fails closed.
//
// A grant is bound to the exact assignment revision/digest it was issued
// under: once the installed assignment advances past it the grant is dead
// (the coordinator replaces it under the current revision) and is retired
// for this process. A typed source refusal likewise stops the loop — never
// a bounded-backoff retry that would head-of-line block every other grant —
// but does not retire the grant: the coordinator installs the destination
// grant before the source's, so the first pull may legitimately precede the
// source grant, and the coordinator's next idempotent re-delivery (Schedule)
// or recovery re-arms exactly one fresh attempt.
func (driver *RepairDriver) drive(ctx context.Context, scheduled store.ResultRepairRecord) {
	repairID := scheduled.Instruction.RepairID
	defer func() {
		driver.mu.Lock()
		delete(driver.queued, repairID)
		driver.mu.Unlock()
	}()
	backoff := driver.retry
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		work, grant, ok := driver.durableGrant(repairID)
		if !ok || grant.Role != store.RepairDestination || grant.State == store.RepairFailed || grant.Instruction.CoordinatorEpoch != driver.repository.CurrentFence() || repairGrantSuperseded(work, grant) {
			driver.markDriven(repairID)
			return
		}
		status := repairStatus(grant)
		response, err := driver.client.PullRepair(ctx, grant.Instruction.SourceNodeID, grant.Instruction.SourceWorkerEpoch, status)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if repairSourceRejected(err) {
				return
			}
			if !driver.backoff(ctx, &backoff) {
				return
			}
			continue
		}
		complete, err := driver.install(ctx, grant, response)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if repairPullTransient(err) && !repairPullTerminal(err) {
				if !driver.backoff(ctx, &backoff) {
					return
				}
				continue
			}
			// Deterministic rejection: the existing validation already
			// fail-closed the durable state; retrying cannot succeed.
			driver.markDriven(repairID)
			return
		}
		if complete {
			driver.markDriven(repairID)
			return
		}
		backoff = driver.retry
	}
}

// install applies one pull response locally: a record chunk is validated and
// durably installed through the existing receive path before the next pull
// acknowledges it; the terminal WorkerStatus reports source completion.
func (driver *RepairDriver) install(ctx context.Context, grant store.ResultRepairRecord, response protocol.WorkerMessage) (bool, error) {
	switch message := response.(type) {
	case protocol.ResultRecordChunk:
		if message.RepairID != grant.Instruction.RepairID || message.RepairInstructionDigest != grant.InstructionDigest {
			return false, ErrTransferIdentityReuse
		}
		localNode, localEpoch := driver.repository.LocalIdentity()
		if message.DestinationNodeID != localNode || message.DestinationWorkerEpoch != localEpoch {
			return false, ErrTransferUnauthorized
		}
		peer := TransferPeer{NodeID: grant.Instruction.SourceNodeID, WorkerEpoch: grant.Instruction.SourceWorkerEpoch, Role: TransferHistoricalRepair}
		if _, err := driver.transfer.ReceiveResultRecord(ctx, peer, message); err != nil {
			return false, err
		}
		return false, nil
	case protocol.WorkerStatus:
		if message.Repair == nil || message.Repair.Role != protocol.RepairSource || message.Repair.RepairID != grant.Instruction.RepairID || message.NodeID != grant.Instruction.SourceNodeID {
			return false, ErrTransferUnauthorized
		}
		source := message.Repair
		if source.State != protocol.RepairComplete || source.RecordCount != grant.Instruction.ExpectedRecordCount || source.TotalBytes != grant.Instruction.ExpectedTotalBytes || source.ContentDigest != grant.Instruction.ExpectedContentDigest {
			return false, ErrTransferIdentityReuse
		}
		_, local, ok := driver.durableGrant(grant.Instruction.RepairID)
		if !ok || local.State != store.RepairComplete {
			// The source claims a completion the local durable grant does not
			// reflect: fail closed without mutating anything.
			return false, ErrTransferIdentityReuse
		}
		return true, nil
	default:
		return false, ErrTransferUnauthorized
	}
}

// durableGrant rereads one durable grant by repair identity together with
// the durable work it was read from.
func (driver *RepairDriver) durableGrant(repairID [16]byte) (store.RecoveredWork, store.ResultRepairRecord, bool) {
	work, err := driver.repository.RecoverWork()
	if err != nil {
		return store.RecoveredWork{}, store.ResultRepairRecord{}, false
	}
	for _, repair := range work.Repairs {
		if repair.Instruction.RepairID == repairID {
			return work, repair, true
		}
	}
	return store.RecoveredWork{}, store.ResultRepairRecord{}, false
}

// repairGrantSuperseded reports whether the installed assignment for the
// grant's job no longer carries the exact revision/digest the grant was
// issued under (or is gone), which makes the grant dead.
func repairGrantSuperseded(work store.RecoveredWork, grant store.ResultRepairRecord) bool {
	assignment, ok := controlAssignment(work, grant.Instruction.JobID)
	return !ok || assignment.Assignment.Revision != grant.Instruction.AssignmentRevision || assignment.Assignment.Digest != grant.Instruction.AssignmentDigest
}

func (driver *RepairDriver) markDriven(repairID [16]byte) {
	driver.mu.Lock()
	defer driver.mu.Unlock()
	driver.driven[repairID] = struct{}{}
}

// backoff waits one bounded deterministic retry delay and doubles the
// schedule, reporting false only on cancellation.
func (driver *RepairDriver) backoff(ctx context.Context, current *time.Duration) bool {
	timer := driver.clock.NewTimer(*current)
	defer timer.Stop()
	select {
	case <-timer.C():
	case <-ctx.Done():
		return false
	}
	next := *current * 2
	if next > driver.maxRetry {
		next = driver.maxRetry
	}
	*current = next
	return true
}

// repairPullTransient classifies pull/install failures worth retrying:
// unreachable sources, exhausted capacity, closed admission, and unavailable
// stores recover; authorization and validation failures are deterministic.
func repairPullTransient(err error) bool {
	switch {
	case errors.Is(err, ErrRepairSourceUnavailable), errors.Is(err, ErrTransferCapacity), errors.Is(err, admission.ErrClosed), errors.Is(err, store.ErrUnavailable), errors.Is(err, store.ErrClosed):
		return true
	case errors.Is(err, context.DeadlineExceeded):
		return true
	default:
		return false
	}
}

// repairPullTerminal reports the deterministic authority and validation
// failures whose retry can never succeed.
func repairPullTerminal(err error) bool {
	return errors.Is(err, ErrTransferUnauthorized) || errors.Is(err, ErrTransferStaleAuthority) || errors.Is(err, ErrTransferIdentityReuse) || errors.Is(err, model.ErrIdentityReuse)
}

// repairSourceRejected reports a pull the source (or the client's own
// authentication of it) deterministically refused, as opposed to a transport
// failure or a retryable capacity/unavailability answer.
func repairSourceRejected(err error) bool {
	return errors.Is(err, ErrRepairSourceRejected) || repairPullTerminal(err)
}

// dialRepairSourceClientOptions fixes the destination's authenticated +3
// dial identity and dependencies.
type dialRepairSourceClientOptions struct {
	ClusterID     [16]byte
	Authenticator wire.Authenticator
	Clock         clock.Clock
	Membership    controlMembership
	Repository    RepairDriverRepository
	Timeout       time.Duration
	Dial          func(context.Context, string, string) (net.Conn, error)
}

// dialRepairSourceClient speaks the authenticated +3 worker-control protocol
// over one cached session per source incarnation — the reviewed
// controlResultReplicator session cache (dial, authorize, handshake once;
// correlated exchanges; drop on failure) — so a grant of N records costs the
// source's bounded per-peer replay cache one handshake plus one identity per
// pull rather than two per record.
type dialRepairSourceClient struct {
	options  dialRepairSourceClientOptions
	sessions *controlSessionCache
}

func newDialRepairSourceClient(options dialRepairSourceClientOptions) *dialRepairSourceClient {
	if options.Dial == nil {
		options.Dial = (&net.Dialer{}).DialContext
	}
	if options.Timeout <= 0 {
		options.Timeout = 2 * time.Second
	}
	sessions := newControlSessionCache(controlSessionCacheOptions{ClusterID: options.ClusterID, Authenticator: options.Authenticator, Clock: options.Clock, Membership: options.Membership, Identity: options.Repository, Timeout: options.Timeout, Dial: options.Dial})
	return &dialRepairSourceClient{options: options, sessions: sessions}
}

// Close closes every cached source session; later pulls fail closed.
func (client *dialRepairSourceClient) Close() {
	client.sessions.Close()
}

// PullRepair performs one correlated repair-pull exchange carrying the
// destination's durable status over the cached authenticated session to the
// exact grant-bound source incarnation, establishing it on first use.
func (client *dialRepairSourceClient) PullRepair(ctx context.Context, sourceNode uint16, sourceEpoch model.WorkerEpoch, status protocol.ResultRepairStatus) (protocol.WorkerMessage, error) {
	if ctx == nil {
		return nil, errors.New("nil repair pull context")
	}
	if _, ok := activeControlMember(client.options.Membership.View(), sourceNode); !ok {
		return nil, fmt.Errorf("%w: repair source %d is not an active member", ErrRepairSourceUnavailable, sourceNode)
	}
	transaction, err := client.options.Repository.DurableTransactionID()
	if err != nil {
		return nil, err
	}
	if transaction == 0 {
		return nil, fmt.Errorf("%w: repair pull requires a durable store transaction", ErrRepairSourceUnavailable)
	}
	session, err := client.sessions.session(ctx, sourceNode, sourceEpoch)
	if err != nil {
		return nil, repairSourceError(err)
	}
	localNode, localEpoch := client.options.Repository.LocalIdentity()
	request := protocol.WorkerStatus{NodeID: localNode, WorkerEpoch: localEpoch, CoordinatorEpoch: status.Instruction.CoordinatorEpoch, StoreTransactionID: transaction, Repair: &status}
	operationContext, cancel := context.WithTimeout(ctx, client.options.Timeout)
	defer cancel()
	response, err := client.sessions.exchange(operationContext, session.stream, request, sourceNode)
	if err != nil {
		if _, rejected := peerRejection(err); !rejected {
			// Only a transport or correlation failure invalidates the
			// session; a typed refusal was a complete healthy exchange.
			client.sessions.dropSession(sourceNode, sourceEpoch, session)
		}
		return nil, repairSourceError(err)
	}
	if !sameActiveControlMember(session.member, client.options.Membership.View()) {
		client.sessions.dropSession(sourceNode, sourceEpoch, session)
		return nil, ErrTransferUnauthorized
	}
	return response, nil
}

// repairSourceError classifies one session or exchange failure: a typed
// deterministic refusal is ErrRepairSourceRejected, a retryable refusal or
// any transport failure is ErrRepairSourceUnavailable, and the client's own
// authentication failures stay ErrTransferUnauthorized.
func repairSourceError(err error) error {
	if rejection, ok := peerRejection(err); ok {
		if rejection.transient() {
			return fmt.Errorf("%w: repair pull message %d refused with retryable code %d", ErrRepairSourceUnavailable, rejection.related, rejection.code)
		}
		return fmt.Errorf("%w: repair pull message %d refused with code %d", ErrRepairSourceRejected, rejection.related, rejection.code)
	}
	if errors.Is(err, ErrTransferUnauthorized) {
		return err
	}
	return fmt.Errorf("%w: %v", ErrRepairSourceUnavailable, err)
}
