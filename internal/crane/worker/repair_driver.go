package worker

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/config"
	"github.com/aaditya/cs425mp3/internal/crane/admission"
	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
	"github.com/aaditya/cs425mp3/internal/crane/store"
	"github.com/aaditya/cs425mp3/internal/wire"
)

// ErrRepairSourceUnavailable reports a repair source that could not be
// reached or answered outside the authenticated +5 protocol.
var ErrRepairSourceUnavailable = errors.New("crane repair source unavailable")

// RepairDriverRepository is the durable authority the destination-side repair
// driver reads and the +5 pull client identifies itself from.
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

// RepairSourceClient performs one authenticated +5 repair pull exchange with
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
// dependencies, authenticated +5 pull client, shared gate, deterministic
// clock, and bounded retry schedule.
type RepairDriverOptions struct {
	// Repository is the sole durable worker authority.
	Repository RepairDriverRepository
	// Transfer performs every durable local record install.
	Transfer RepairInstaller
	// Client performs each authenticated +5 pull exchange.
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
// durable authority disappears, or a deterministic validation fails closed.
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
		grant, ok := driver.durableGrant(repairID)
		if !ok || grant.Role != store.RepairDestination || grant.State == store.RepairFailed || grant.Instruction.CoordinatorEpoch != driver.repository.CurrentFence() {
			driver.markDriven(repairID)
			return
		}
		status := repairStatus(grant)
		response, err := driver.client.PullRepair(ctx, grant.Instruction.SourceNodeID, grant.Instruction.SourceWorkerEpoch, status)
		if err != nil {
			if ctx.Err() != nil {
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
		local, ok := driver.durableGrant(grant.Instruction.RepairID)
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

// durableGrant rereads one durable grant by repair identity.
func (driver *RepairDriver) durableGrant(repairID [16]byte) (store.ResultRepairRecord, bool) {
	work, err := driver.repository.RecoverWork()
	if err != nil {
		return store.ResultRepairRecord{}, false
	}
	for _, repair := range work.Repairs {
		if repair.Instruction.RepairID == repairID {
			return repair, true
		}
	}
	return store.ResultRepairRecord{}, false
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

// dialRepairSourceClientOptions fixes the destination's authenticated +5
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

// dialRepairSourceClient speaks the authenticated +5 worker-control protocol
// over one TCP connection per pull, mirroring the reviewed worker-side
// controlResultReplicator session pattern (dial, authorize, handshake,
// correlated exchange) instead of introducing any new protocol.
type dialRepairSourceClient struct {
	options dialRepairSourceClientOptions
}

func newDialRepairSourceClient(options dialRepairSourceClientOptions) *dialRepairSourceClient {
	if options.Dial == nil {
		options.Dial = (&net.Dialer{}).DialContext
	}
	if options.Timeout <= 0 {
		options.Timeout = 2 * time.Second
	}
	return &dialRepairSourceClient{options: options}
}

// PullRepair dials the source's advertised +5 endpoint, handshakes against
// the exact grant-bound source incarnation, and performs one correlated
// repair-pull exchange carrying the destination's durable status.
func (client *dialRepairSourceClient) PullRepair(ctx context.Context, sourceNode uint16, sourceEpoch model.WorkerEpoch, status protocol.ResultRepairStatus) (protocol.WorkerMessage, error) {
	if ctx == nil {
		return nil, errors.New("nil repair pull context")
	}
	member, ok := activeControlMember(client.options.Membership.View(), sourceNode)
	if !ok {
		return nil, fmt.Errorf("%w: repair source %d is not an active member", ErrRepairSourceUnavailable, sourceNode)
	}
	endpoint, err := memberServiceEndpoint(member.Host, member.BasePort, config.ServiceCraneWorker)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRepairSourceUnavailable, err)
	}
	operationContext, cancel := context.WithTimeout(ctx, client.options.Timeout)
	defer cancel()
	connection, err := client.options.Dial(operationContext, "tcp", endpoint.String())
	if err != nil {
		return nil, fmt.Errorf("%w: dial repair source: %v", ErrRepairSourceUnavailable, err)
	}
	defer connection.Close()
	if err := client.options.Membership.AuthorizeTCP(sourceNode, connection.RemoteAddr()); err != nil {
		return nil, ErrTransferUnauthorized
	}
	limits := wire.DefaultLimits()
	limits.MaxFrameSize = int(model.WorkerControlMaxFrameBytesV1)
	expectedCluster := client.options.ClusterID
	limits.ExpectedClusterID = &expectedCluster
	stream := wire.NewTCPFrameStream(connection, client.options.Authenticator, limits, client.options.Timeout)

	localNode, localEpoch := client.options.Repository.LocalIdentity()
	handshake := protocol.WorkerHandshake{NodeID: localNode, WorkerEpoch: localEpoch, ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint()}
	handshakeResponse, err := client.exchange(operationContext, stream, handshake, sourceNode)
	if err != nil {
		return nil, err
	}
	handshakeAck, ok := handshakeResponse.(protocol.WorkerHandshakeAck)
	if !ok || handshakeAck.NodeID != sourceNode || handshakeAck.WorkerEpoch != sourceEpoch || handshakeAck.ConsensusFingerprint != model.ConsensusFingerprint() || handshakeAck.RegistryFingerprint != model.RegistryFingerprint() {
		return nil, ErrTransferUnauthorized
	}
	if !sameActiveControlMember(member, client.options.Membership.View()) {
		return nil, ErrTransferUnauthorized
	}
	transaction, err := client.options.Repository.DurableTransactionID()
	if err != nil {
		return nil, err
	}
	if transaction == 0 {
		return nil, fmt.Errorf("%w: repair pull requires a durable store transaction", ErrRepairSourceUnavailable)
	}
	request := protocol.WorkerStatus{NodeID: localNode, WorkerEpoch: localEpoch, CoordinatorEpoch: status.Instruction.CoordinatorEpoch, StoreTransactionID: transaction, Repair: &status}
	response, err := client.exchange(operationContext, stream, request, sourceNode)
	if err != nil {
		return nil, err
	}
	if !sameActiveControlMember(member, client.options.Membership.View()) {
		return nil, ErrTransferUnauthorized
	}
	return response, nil
}

// exchange writes one authenticated frame and validates the correlated reply.
func (client *dialRepairSourceClient) exchange(ctx context.Context, stream *wire.TCPFrameStream, request protocol.WorkerMessage, destination uint16) (protocol.WorkerMessage, error) {
	payload, err := protocol.MarshalWorkerMessage(request)
	if err != nil {
		return nil, err
	}
	var requestID wire.RequestID
	if _, err := rand.Read(requestID[:]); err != nil {
		return nil, fmt.Errorf("generate repair pull request ID: %w", err)
	}
	node, _ := client.options.Repository.LocalIdentity()
	frame := wire.Frame{Header: wire.Header{Version: wire.Version1, Message: request.MessageType(), ClusterID: client.options.ClusterID, SenderID: node, RequestID: requestID, TimestampMillis: client.options.Clock.Now().UnixMilli(), Codec: wire.CodecBinary}, Payload: payload}
	if err := stream.WriteFrame(ctx, frame); err != nil {
		return nil, fmt.Errorf("%w: write repair pull frame: %v", ErrRepairSourceUnavailable, err)
	}
	response, err := stream.ReadFrame(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: read repair pull frame: %v", ErrRepairSourceUnavailable, err)
	}
	if response.Header.SenderID != destination || response.Header.RequestID != requestID {
		return nil, ErrTransferUnauthorized
	}
	message, err := protocol.UnmarshalWorkerMessage(response.Header.Message, response.Payload)
	if err != nil {
		return nil, fmt.Errorf("%w: decode repair pull response: %v", ErrRepairSourceUnavailable, err)
	}
	if workerError, ok := message.(protocol.WorkerError); ok {
		return nil, fmt.Errorf("%w: repair pull message %d rejected with code %d", ErrRepairSourceUnavailable, workerError.RelatedMessage, workerError.Code)
	}
	return message, nil
}
