package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sort"
	"sync"

	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
	"github.com/aaditya/cs425mp3/internal/crane/store"
)

const (
	// DefaultMaxActiveTransfersPerPeer bounds simultaneous +5 transfer work
	// attributed to one authenticated worker incarnation.
	DefaultMaxActiveTransfersPerPeer = 4
	// DefaultMaxActiveTransfers bounds simultaneous transfer work process-wide.
	DefaultMaxActiveTransfers = 64
	// DefaultMaxQueuedTransferWork bounds admitted +5 work awaiting execution.
	DefaultMaxQueuedTransferWork = 256
)

var (
	ErrTransferUnauthorized      = errors.New("crane transfer unauthorized")
	ErrTransferStaleAuthority    = errors.New("crane transfer stale authority")
	ErrTransferIdentityReuse     = errors.New("crane transfer identity reuse")
	ErrTransferCapacity          = errors.New("crane transfer capacity exhausted")
	ErrResultArtifactUnavailable = errors.New("crane result artifact storage unavailable until sealing")
	ErrResultFetchUnavailable    = errors.New("crane result fetch unavailable until artifacts are sealed")
)

// TransferRole keeps coordinator commands, current replication, bilateral
// historical repair, and leader fetch as distinct authenticated authorities.
type TransferRole uint8

const (
	TransferCoordinatorCommand TransferRole = iota + 1
	TransferNormalReplication
	TransferHistoricalRepair
	TransferLeaderFetch
)

// TransferPeer is the already-authenticated session identity supplied by the
// +5 session owner. Possession of the cluster HMAC is deliberately not a role.
type TransferPeer struct {
	NodeID      uint16
	WorkerEpoch model.WorkerEpoch
	Role        TransferRole
}

// TransferRepository is the durable worker state required by record transfer.
// WorkerService supplies its store-backed adapter; network ownership stays out
// of this component.
type TransferRepository interface {
	RecoverWork() (store.RecoveredWork, error)
	LocalIdentity() (uint16, model.WorkerEpoch)
	CurrentFence() model.CoordinatorEpoch
	InstalledAssignment(model.JobID) (store.InstalledAssignment, bool)
	UpsertResult(model.ResultRecord, model.ResultCopyProvenance) error
	UpsertRepair(store.ResultRepairRecord) error
}

// TransferOptions applies positive concurrency bounds to a transfer owner.
type TransferOptions struct {
	Repository    TransferRepository
	MaxPerPeer    int
	MaxActive     int
	MaxQueuedWork int
}

// TransferOwner validates authority and serializes durability transitions. It
// owns no socket, file, or goroutine and is safe to construct before binding.
type TransferOwner struct {
	repository TransferRepository
	localNode  uint16
	localEpoch model.WorkerEpoch
	maxPerPeer int
	maxActive  int
	maxQueued  int

	mu      sync.Mutex
	active  int
	queued  int
	perPeer map[transferPeerIdentity]int
	changed chan struct{}
	repairs map[[16]byte]chan struct{}
}

type transferPeerIdentity struct {
	node  uint16
	epoch model.WorkerEpoch
}

// NewTransferOwner recovers enough state to validate the local durable
// identity, but starts no work and performs no network or filesystem setup.
func NewTransferOwner(options TransferOptions) (*TransferOwner, error) {
	if options.Repository == nil {
		return nil, errors.New("nil transfer repository")
	}
	if options.MaxPerPeer == 0 {
		options.MaxPerPeer = DefaultMaxActiveTransfersPerPeer
	}
	if options.MaxActive == 0 {
		options.MaxActive = DefaultMaxActiveTransfers
	}
	if options.MaxQueuedWork == 0 {
		options.MaxQueuedWork = DefaultMaxQueuedTransferWork
	}
	if options.MaxPerPeer < 1 || options.MaxActive < 1 || options.MaxQueuedWork < 1 || options.MaxPerPeer > options.MaxActive {
		return nil, errors.New("invalid transfer limits")
	}
	node, epoch := options.Repository.LocalIdentity()
	if node == 0 || epoch.Validate() != nil {
		return nil, errors.New("invalid local transfer identity")
	}
	if _, err := options.Repository.RecoverWork(); err != nil {
		return nil, err
	}
	return &TransferOwner{repository: options.Repository, localNode: node, localEpoch: epoch, maxPerPeer: options.MaxPerPeer, maxActive: options.MaxActive, maxQueued: options.MaxQueuedWork, perPeer: make(map[transferPeerIdentity]int), changed: make(chan struct{}), repairs: make(map[[16]byte]chan struct{})}, nil
}

// DeriveResultRecordTransferID binds a record transfer to its complete role,
// logical bytes, copy authority, destination, fence, and optional repair grant.
func DeriveResultRecordTransferID(role TransferRole, record model.ResultRecord, provenance model.ResultCopyProvenance, destinationNode uint16, destinationEpoch model.WorkerEpoch, repairID [16]byte, instructionDigest [32]byte) (protocol.TransferID, error) {
	stream, err := model.MarshalResultRecord(record)
	if err != nil {
		return protocol.TransferID{}, err
	}
	if err := provenance.Validate(record); err != nil {
		return protocol.TransferID{}, err
	}
	if destinationNode == 0 || destinationEpoch.Validate() != nil {
		return protocol.TransferID{}, errors.New("invalid transfer destination")
	}
	if role != TransferNormalReplication && role != TransferHistoricalRepair {
		return protocol.TransferID{}, errors.New("role cannot carry a result record")
	}
	if role == TransferNormalReplication && (repairID != ([16]byte{}) || instructionDigest != ([32]byte{})) || role == TransferHistoricalRepair && (repairID == ([16]byte{}) || instructionDigest == ([32]byte{})) {
		return protocol.TransferID{}, errors.New("transfer role/grant binding mismatch")
	}
	encoded := []byte("crane-result-record-transfer-id-v1\x00")
	encoded = append(encoded, byte(role))
	encoded = appendUint16Transfer(encoded, destinationNode)
	encoded = append(encoded, destinationEpoch[:]...)
	encoded = appendUint64Transfer(encoded, provenance.AssignmentRevision)
	encoded = append(encoded, provenance.AssignmentDigest[:]...)
	encoded = appendTaskTransfer(encoded, provenance.ReplicaSet.SinkTask)
	encoded = appendUint16Transfer(encoded, provenance.ReplicaSet.PrimaryNodeID)
	encoded = appendUint16Transfer(encoded, provenance.ReplicaSet.SecondaryNodeID)
	encoded = append(encoded, provenance.ReplicaSet.PrimaryEpoch[:]...)
	encoded = append(encoded, provenance.ReplicaSet.SecondaryEpoch[:]...)
	encoded = append(encoded, byte(provenance.DestinationRole))
	encoded = appendEpochTransfer(encoded, provenance.CoordinatorEpoch)
	encoded = append(encoded, repairID[:]...)
	encoded = append(encoded, instructionDigest[:]...)
	encoded = appendUint64Transfer(encoded, uint64(len(stream)))
	encoded = append(encoded, stream...)
	sum := sha256.Sum256(encoded)
	var id protocol.TransferID
	copy(id[:], sum[:16])
	if id == (protocol.TransferID{}) {
		// Preserve deterministic nonzero identity even for the cryptographically
		// possible all-zero prefix.
		sum = sha256.Sum256(append(encoded, 1))
		copy(id[:], sum[:16])
		if id == (protocol.TransferID{}) {
			id[15] = 1
		}
	}
	return id, nil
}

// ReceiveResultRecord validates one durability-granular canonical record and
// returns an ACK only after the result and any repair progress are durable.
func (owner *TransferOwner) ReceiveResultRecord(ctx context.Context, peer TransferPeer, chunk protocol.ResultRecordChunk) (protocol.ResultRecordAck, error) {
	release, err := owner.begin(ctx, peer)
	if err != nil {
		return protocol.ResultRecordAck{}, err
	}
	defer release()
	if err := validateCompleteRecordChunk(chunk); err != nil {
		return protocol.ResultRecordAck{}, err
	}
	switch peer.Role {
	case TransferNormalReplication:
		if chunk.RepairID != ([16]byte{}) || chunk.RepairInstructionDigest != ([32]byte{}) {
			return protocol.ResultRecordAck{}, ErrTransferUnauthorized
		}
		if err := owner.validateCurrentReplication(peer, chunk); err != nil {
			return protocol.ResultRecordAck{}, err
		}
		if err := owner.persistResult(chunk); err != nil {
			return protocol.ResultRecordAck{}, err
		}
	case TransferHistoricalRepair:
		if err := owner.receiveRepair(ctx, peer, chunk); err != nil {
			return protocol.ResultRecordAck{}, err
		}
	default:
		return protocol.ResultRecordAck{}, ErrTransferUnauthorized
	}
	return owner.recordAck(chunk), nil
}

// NextRepairRecord returns the exact next whole record for a durable source
// grant. The supplied destination status proves that the peer grant was already
// persisted before source streaming begins.
func (owner *TransferOwner) NextRepairRecord(ctx context.Context, peer TransferPeer, repairID [16]byte, destination protocol.ResultRepairStatus) (protocol.ResultRecordChunk, bool, error) {
	release, err := owner.begin(ctx, peer)
	if err != nil {
		return protocol.ResultRecordChunk{}, false, err
	}
	defer release()
	work, repair, err := owner.currentRepair(repairID, store.RepairSource)
	if err != nil {
		return protocol.ResultRecordChunk{}, false, err
	}
	releaseRepair, err := owner.acquireRepair(ctx, repairID)
	if err != nil {
		return protocol.ResultRecordChunk{}, false, err
	}
	defer releaseRepair()
	work, repair, err = owner.currentRepair(repairID, store.RepairSource)
	if err != nil {
		return protocol.ResultRecordChunk{}, false, err
	}
	if err := validateDestinationGrant(peer, repair, destination); err != nil {
		return protocol.ResultRecordChunk{}, false, err
	}
	records, err := repairInventory(work.Results, repair.Instruction)
	if err != nil {
		return protocol.ResultRecordChunk{}, false, err
	}
	if err := validateRepairSourceCopies(work.Results, repair.Instruction); err != nil {
		return protocol.ResultRecordChunk{}, false, err
	}
	if err := validateRepairInventory(repair.Instruction, records); err != nil {
		return protocol.ResultRecordChunk{}, false, err
	}
	if err := validateRepairProgress(repair, records); err != nil {
		return protocol.ResultRecordChunk{}, false, err
	}
	if repair.NextRecord == repair.Instruction.ExpectedRecordCount {
		if repair.State != store.RepairComplete {
			repair.State = store.RepairComplete
			repair.ContentDigest = repair.Instruction.ExpectedContentDigest
			if err := owner.repository.UpsertRepair(repair); err != nil {
				return protocol.ResultRecordChunk{}, false, err
			}
		}
		return protocol.ResultRecordChunk{}, true, nil
	}
	if repair.NextRecord >= uint64(len(records)) {
		return protocol.ResultRecordChunk{}, false, ErrTransferIdentityReuse
	}
	record := records[repair.NextRecord]
	provenance, err := owner.repairDestinationProvenance(repair, record)
	if err != nil {
		return protocol.ResultRecordChunk{}, false, err
	}
	chunk, err := buildResultRecordChunk(TransferHistoricalRepair, record, provenance, repair.Instruction.DestinationNodeID, repair.Instruction.DestinationWorkerEpoch, repair.Instruction.RepairID, repair.InstructionDigest)
	return chunk, false, err
}

// AcknowledgeRepairRecord durably advances source progress only for the exact
// destination ACK of the exact next record.
func (owner *TransferOwner) AcknowledgeRepairRecord(ctx context.Context, peer TransferPeer, chunk protocol.ResultRecordChunk, ack protocol.ResultRecordAck) error {
	release, err := owner.begin(ctx, peer)
	if err != nil {
		return err
	}
	defer release()
	work, repair, err := owner.currentRepair(chunk.RepairID, store.RepairSource)
	if err != nil {
		return err
	}
	releaseRepair, err := owner.acquireRepair(ctx, chunk.RepairID)
	if err != nil {
		return err
	}
	defer releaseRepair()
	work, repair, err = owner.currentRepair(chunk.RepairID, store.RepairSource)
	if err != nil {
		return err
	}
	if peer.Role != TransferHistoricalRepair || peer.NodeID != repair.Instruction.DestinationNodeID || peer.WorkerEpoch != repair.Instruction.DestinationWorkerEpoch || chunk.RepairInstructionDigest != repair.InstructionDigest {
		return ErrTransferUnauthorized
	}
	records, err := repairInventory(work.Results, repair.Instruction)
	if err != nil {
		return ErrTransferIdentityReuse
	}
	if err := validateRepairSourceCopies(work.Results, repair.Instruction); err != nil {
		return err
	}
	if err := validateRepairInventory(repair.Instruction, records); err != nil {
		return err
	}
	if err := validateRepairProgress(repair, records); err != nil {
		return err
	}
	if err := protocol.ValidateResultRecordAckCorrelation(chunk, ack); err != nil {
		return err
	}
	if repair.NextRecord > 0 && repair.NextRecord-1 < uint64(len(records)) {
		priorProvenance, priorErr := owner.repairDestinationProvenance(repair, records[repair.NextRecord-1])
		prior, buildErr := buildResultRecordChunk(TransferHistoricalRepair, records[repair.NextRecord-1], priorProvenance, repair.Instruction.DestinationNodeID, repair.Instruction.DestinationWorkerEpoch, repair.Instruction.RepairID, repair.InstructionDigest)
		if priorErr == nil && buildErr == nil && equalRecordChunk(prior, chunk) {
			return nil
		}
	}
	if repair.NextRecord >= uint64(len(records)) {
		return ErrTransferIdentityReuse
	}
	wantProvenance, err := owner.repairDestinationProvenance(repair, records[repair.NextRecord])
	if err != nil {
		return err
	}
	want, err := buildResultRecordChunk(TransferHistoricalRepair, records[repair.NextRecord], wantProvenance, repair.Instruction.DestinationNodeID, repair.Instruction.DestinationWorkerEpoch, repair.Instruction.RepairID, repair.InstructionDigest)
	if err != nil || !equalRecordChunk(want, chunk) {
		return ErrTransferIdentityReuse
	}
	return owner.advanceRepair(repair, chunk.Record)
}

// RecoveredRepairs exposes owned durable repair status. It does not schedule or
// resume any recovered transfer.
func (owner *TransferOwner) RecoveredRepairs() ([]store.ResultRepairRecord, error) {
	work, err := owner.repository.RecoverWork()
	if err != nil {
		return nil, err
	}
	result := make([]store.ResultRepairRecord, len(work.Repairs))
	copy(result, work.Repairs)
	for i := range result {
		result[i].Instruction.Checkpoints = append([]model.SourceCheckpoint(nil), result[i].Instruction.Checkpoints...)
	}
	return result, nil
}

// ReceiveResultArtifact is a fail-closed Task20 extension seam.
func (owner *TransferOwner) ReceiveResultArtifact(context.Context, TransferPeer, protocol.ResultArtifactChunk) (protocol.ResultArtifactAck, error) {
	return protocol.ResultArtifactAck{}, ErrResultArtifactUnavailable
}

// OpenResultFetch is a fail-closed Task20 extension seam.
func (owner *TransferOwner) OpenResultFetch(context.Context, TransferPeer, protocol.ResultFetchRequest) (protocol.ResultFetchChunk, error) {
	return protocol.ResultFetchChunk{}, ErrResultFetchUnavailable
}

func (owner *TransferOwner) receiveRepair(ctx context.Context, peer TransferPeer, chunk protocol.ResultRecordChunk) error {
	_, repair, err := owner.currentRepair(chunk.RepairID, store.RepairDestination)
	if err != nil {
		return err
	}
	releaseRepair, err := owner.acquireRepair(ctx, chunk.RepairID)
	if err != nil {
		return err
	}
	defer releaseRepair()
	_, repair, err = owner.currentRepair(chunk.RepairID, store.RepairDestination)
	if err != nil {
		return err
	}
	if peer.NodeID != repair.Instruction.SourceNodeID || peer.WorkerEpoch != repair.Instruction.SourceWorkerEpoch || chunk.RepairInstructionDigest != repair.InstructionDigest {
		return ErrTransferUnauthorized
	}
	if err := owner.validateRepairDestination(repair, chunk); err != nil {
		return err
	}
	work, err := owner.repository.RecoverWork()
	if err != nil {
		return err
	}
	covered, err := repairInventory(work.Results, repair.Instruction)
	if err != nil {
		return err
	}
	if err := validateRepairProgress(repair, covered); err != nil {
		return err
	}
	position := sort.Search(len(covered), func(index int) bool {
		return !tupleTransferLess(covered[index].TupleID, chunk.Record.TupleID)
	})
	if position < len(covered) && covered[position].TupleID == chunk.Record.TupleID {
		prior, ok := findStoredResult(work.Results, chunk.Record)
		if !ok || prior.Record.Checksum != chunk.Record.Checksum || !bytes.Equal(prior.Record.Value, chunk.Record.Value) || prior.Provenance != chunk.Provenance {
			return ErrTransferIdentityReuse
		}
		if uint64(position) < repair.NextRecord {
			return nil
		}
		if uint64(position) != repair.NextRecord {
			return ErrTransferIdentityReuse
		}
		// Recovery after result durability but before progress durability:
		// advance from the exact recovered prefix before issuing the ACK.
		return owner.advanceRepair(repair, chunk.Record)
	}
	if uint64(position) != repair.NextRecord {
		return ErrTransferIdentityReuse
	}
	if err := preflightRepairAdvance(repair, chunk.Record); err != nil {
		return err
	}
	if err := owner.persistResult(chunk); err != nil {
		return err
	}
	return owner.advanceRepair(repair, chunk.Record)
}

func (owner *TransferOwner) persistResult(chunk protocol.ResultRecordChunk) error {
	record := chunk.Record
	record.Value = append([]byte(nil), chunk.Record.Value...)
	return owner.repository.UpsertResult(record, chunk.Provenance)
}

func (owner *TransferOwner) validateCurrentReplication(peer TransferPeer, chunk protocol.ResultRecordChunk) error {
	assignment, ok := owner.repository.InstalledAssignment(chunk.Transfer.JobID)
	if !ok || assignment.CoordinatorEpoch != owner.repository.CurrentFence() || chunk.Provenance.CoordinatorEpoch != assignment.CoordinatorEpoch {
		return ErrTransferStaleAuthority
	}
	if assignment.SchedulingState != model.Running || chunk.Provenance.AssignmentRevision != assignment.Assignment.Revision || chunk.Provenance.AssignmentDigest != assignment.Assignment.Digest || chunk.Record.SpecificationHash != assignment.Topology.Digest() {
		return ErrTransferUnauthorized
	}
	replica, ok := findResultReplica(assignment.Assignment, chunk.Record.SinkTask)
	if !ok || replica != chunk.Provenance.ReplicaSet {
		return ErrTransferUnauthorized
	}
	destinationNode, destinationEpoch, sourceNode, sourceEpoch, ok := endpointsForRole(replica, chunk.Provenance.DestinationRole)
	if !ok || destinationNode != owner.localNode || destinationEpoch != owner.localEpoch || chunk.DestinationNodeID != destinationNode || chunk.DestinationWorkerEpoch != destinationEpoch || peer.NodeID != sourceNode || peer.WorkerEpoch != sourceEpoch {
		return ErrTransferUnauthorized
	}
	id, err := DeriveResultRecordTransferID(TransferNormalReplication, chunk.Record, chunk.Provenance, chunk.DestinationNodeID, chunk.DestinationWorkerEpoch, [16]byte{}, [32]byte{})
	if err != nil || id != chunk.Transfer.TransferID {
		return ErrTransferIdentityReuse
	}
	return nil
}

func (owner *TransferOwner) validateRepairDestination(repair store.ResultRepairRecord, chunk protocol.ResultRecordChunk) error {
	if chunk.RepairID != repair.Instruction.RepairID || chunk.RepairInstructionDigest != repair.InstructionDigest || chunk.Record.TupleID.JobID != repair.Instruction.JobID || chunk.Record.SinkTask != repair.Instruction.SinkTask || chunk.Record.SpecificationHash != repair.Instruction.SpecificationHash || !repairCoversTuple(repair.Instruction, chunk.Record.TupleID) {
		return ErrTransferUnauthorized
	}
	want, err := owner.repairDestinationProvenance(repair, chunk.Record)
	if err != nil || want != chunk.Provenance || chunk.DestinationNodeID != owner.localNode || chunk.DestinationWorkerEpoch != owner.localEpoch {
		return ErrTransferUnauthorized
	}
	id, err := DeriveResultRecordTransferID(TransferHistoricalRepair, chunk.Record, chunk.Provenance, chunk.DestinationNodeID, chunk.DestinationWorkerEpoch, chunk.RepairID, chunk.RepairInstructionDigest)
	if err != nil || id != chunk.Transfer.TransferID {
		return ErrTransferIdentityReuse
	}
	return nil
}

func (owner *TransferOwner) repairDestinationProvenance(repair store.ResultRepairRecord, record model.ResultRecord) (model.ResultCopyProvenance, error) {
	assignment, ok := owner.repository.InstalledAssignment(repair.Instruction.JobID)
	if !ok || assignment.CoordinatorEpoch != owner.repository.CurrentFence() || repair.Instruction.CoordinatorEpoch != assignment.CoordinatorEpoch {
		return model.ResultCopyProvenance{}, ErrTransferStaleAuthority
	}
	if assignment.Assignment.Revision != repair.Instruction.AssignmentRevision || assignment.Assignment.Digest != repair.Instruction.AssignmentDigest || assignment.Topology.Digest() != repair.Instruction.SpecificationHash {
		return model.ResultCopyProvenance{}, ErrTransferUnauthorized
	}
	replica, ok := findResultReplica(assignment.Assignment, repair.Instruction.SinkTask)
	if !ok {
		return model.ResultCopyProvenance{}, ErrTransferUnauthorized
	}
	role := model.PrimaryReplica
	if repair.Instruction.DestinationNodeID == replica.SecondaryNodeID && repair.Instruction.DestinationWorkerEpoch == replica.SecondaryEpoch {
		role = model.SecondaryReplica
	} else if repair.Instruction.DestinationNodeID != replica.PrimaryNodeID || repair.Instruction.DestinationWorkerEpoch != replica.PrimaryEpoch {
		return model.ResultCopyProvenance{}, ErrTransferUnauthorized
	}
	provenance := model.ResultCopyProvenance{AssignmentRevision: assignment.Assignment.Revision, AssignmentDigest: assignment.Assignment.Digest, ReplicaSet: replica, DestinationRole: role, CoordinatorEpoch: assignment.CoordinatorEpoch}
	return provenance, provenance.Validate(record)
}

func (owner *TransferOwner) currentRepair(id [16]byte, role store.RepairEndpointRole) (store.RecoveredWork, store.ResultRepairRecord, error) {
	if id == ([16]byte{}) {
		return store.RecoveredWork{}, store.ResultRepairRecord{}, ErrTransferUnauthorized
	}
	work, err := owner.repository.RecoverWork()
	if err != nil {
		return store.RecoveredWork{}, store.ResultRepairRecord{}, err
	}
	for _, repair := range work.Repairs {
		if repair.Instruction.RepairID != id {
			continue
		}
		if repair.Role != role || repair.State == store.RepairFailed {
			return store.RecoveredWork{}, store.ResultRepairRecord{}, ErrTransferUnauthorized
		}
		if repair.Instruction.CoordinatorEpoch != work.Fence || repair.Instruction.CoordinatorEpoch != owner.repository.CurrentFence() {
			return store.RecoveredWork{}, store.ResultRepairRecord{}, ErrTransferStaleAuthority
		}
		if role == store.RepairSource && (repair.Instruction.SourceNodeID != owner.localNode || repair.Instruction.SourceWorkerEpoch != owner.localEpoch) || role == store.RepairDestination && (repair.Instruction.DestinationNodeID != owner.localNode || repair.Instruction.DestinationWorkerEpoch != owner.localEpoch) {
			return store.RecoveredWork{}, store.ResultRepairRecord{}, ErrTransferUnauthorized
		}
		return work, repair, nil
	}
	return store.RecoveredWork{}, store.ResultRepairRecord{}, ErrTransferUnauthorized
}

func (owner *TransferOwner) advanceRepair(repair store.ResultRepairRecord, record model.ResultRecord) error {
	entry, err := marshalResultInventoryEntry(record)
	if err != nil {
		return err
	}
	if repair.NextOffset > ^uint64(0)-uint64(len(entry)) || repair.NextRecord == ^uint64(0) {
		return ErrTransferIdentityReuse
	}
	base := repair.ContentDigest
	if repair.NextRecord == 0 {
		base = model.EmptyResultInventoryDigest(repair.Instruction.InventoryQueryDigest)
	}
	repair.ContentDigest = extendInventoryDigest(base, repair.NextRecord, entry)
	repair.NextRecord++
	repair.NextOffset += uint64(len(entry))
	repair.RecordCount = repair.NextRecord
	repair.TotalBytes = repair.NextOffset
	repair.State = store.RepairStreaming
	if repair.NextRecord == repair.Instruction.ExpectedRecordCount {
		if repair.NextOffset != repair.Instruction.ExpectedTotalBytes || repair.ContentDigest != repair.Instruction.ExpectedContentDigest {
			return ErrTransferIdentityReuse
		}
		repair.State = store.RepairComplete
	}
	return owner.repository.UpsertRepair(repair)
}

func preflightRepairAdvance(repair store.ResultRepairRecord, record model.ResultRecord) error {
	entry, err := marshalResultInventoryEntry(record)
	if err != nil {
		return err
	}
	definition := repair.Instruction
	length := uint64(len(entry))
	if repair.NextRecord >= definition.ExpectedRecordCount || repair.NextOffset > definition.ExpectedTotalBytes || length > definition.ExpectedTotalBytes-repair.NextOffset {
		return ErrTransferIdentityReuse
	}
	if repair.NextRecord+1 == definition.ExpectedRecordCount {
		base := repair.ContentDigest
		if repair.NextRecord == 0 {
			base = model.EmptyResultInventoryDigest(definition.InventoryQueryDigest)
		}
		digest := extendInventoryDigest(base, repair.NextRecord, entry)
		if repair.NextOffset+length != definition.ExpectedTotalBytes || digest != definition.ExpectedContentDigest {
			return ErrTransferIdentityReuse
		}
	}
	return nil
}

func (owner *TransferOwner) recordAck(chunk protocol.ResultRecordChunk) protocol.ResultRecordAck {
	return protocol.ResultRecordAck{TransferID: chunk.Transfer.TransferID, NodeID: owner.localNode, WorkerEpoch: owner.localEpoch, RepairID: chunk.RepairID, RepairInstructionDigest: chunk.RepairInstructionDigest, NextOffset: chunk.Transfer.TotalLength, TotalLength: chunk.Transfer.TotalLength, Checksum: chunk.Transfer.Checksum, Complete: true, CoordinatorEpoch: chunk.Provenance.CoordinatorEpoch}
}

func (owner *TransferOwner) begin(ctx context.Context, peer TransferPeer) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if peer.NodeID == 0 || peer.Role < TransferCoordinatorCommand || peer.Role > TransferLeaderFetch || peer.Role != TransferLeaderFetch && peer.WorkerEpoch.Validate() != nil {
		return nil, ErrTransferUnauthorized
	}
	key := transferPeerIdentity{node: peer.NodeID, epoch: peer.WorkerEpoch}
	owner.mu.Lock()
	if owner.queued >= owner.maxQueued {
		owner.mu.Unlock()
		return nil, ErrTransferCapacity
	}
	owner.queued++
	for {
		if err := ctx.Err(); err != nil {
			owner.queued--
			owner.mu.Unlock()
			return nil, err
		}
		if owner.active < owner.maxActive && owner.perPeer[key] < owner.maxPerPeer {
			owner.queued--
			owner.active++
			owner.perPeer[key]++
			owner.mu.Unlock()
			return func() {
				owner.mu.Lock()
				owner.active--
				owner.perPeer[key]--
				if owner.perPeer[key] == 0 {
					delete(owner.perPeer, key)
				}
				close(owner.changed)
				owner.changed = make(chan struct{})
				owner.mu.Unlock()
			}, nil
		}
		changed := owner.changed
		owner.mu.Unlock()
		select {
		case <-ctx.Done():
			owner.mu.Lock()
			owner.queued--
			owner.mu.Unlock()
			return nil, ctx.Err()
		case <-changed:
			owner.mu.Lock()
		}
	}
}

func (owner *TransferOwner) acquireRepair(ctx context.Context, id [16]byte) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	owner.mu.Lock()
	serial, ok := owner.repairs[id]
	if !ok {
		serial = make(chan struct{}, 1)
		serial <- struct{}{}
		owner.repairs[id] = serial
	}
	owner.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-serial:
		return func() { serial <- struct{}{} }, nil
	}
}

func validateCompleteRecordChunk(chunk protocol.ResultRecordChunk) error {
	if _, err := protocol.MarshalResultRecordChunk(chunk); err != nil {
		return err
	}
	stream, err := model.MarshalResultRecord(chunk.Record)
	if err != nil {
		return err
	}
	if chunk.Transfer.Offset != 0 || !chunk.Transfer.Final || chunk.Transfer.TotalLength != uint64(len(stream)) || len(chunk.Transfer.Data) != len(stream) || !bytes.Equal(chunk.Transfer.Data, stream) || chunk.Transfer.Checksum != sha256.Sum256(stream) {
		return errors.New("result record transfer is not one complete durability unit")
	}
	return nil
}

func buildResultRecordChunk(role TransferRole, record model.ResultRecord, provenance model.ResultCopyProvenance, destination uint16, destinationEpoch model.WorkerEpoch, repairID [16]byte, instruction [32]byte) (protocol.ResultRecordChunk, error) {
	stream, err := model.MarshalResultRecord(record)
	if err != nil {
		return protocol.ResultRecordChunk{}, err
	}
	id, err := DeriveResultRecordTransferID(role, record, provenance, destination, destinationEpoch, repairID, instruction)
	if err != nil {
		return protocol.ResultRecordChunk{}, err
	}
	return protocol.ResultRecordChunk{Transfer: protocol.TransferChunk{TransferID: id, JobID: record.TupleID.JobID, TotalLength: uint64(len(stream)), Checksum: sha256.Sum256(stream), Data: stream, Final: true}, Record: cloneResultRecord(record), Provenance: provenance, DestinationNodeID: destination, DestinationWorkerEpoch: destinationEpoch, RepairID: repairID, RepairInstructionDigest: instruction}, nil
}

func validateDestinationGrant(peer TransferPeer, repair store.ResultRepairRecord, status protocol.ResultRepairStatus) error {
	if peer.Role != TransferHistoricalRepair || peer.NodeID != repair.Instruction.DestinationNodeID || peer.WorkerEpoch != repair.Instruction.DestinationWorkerEpoch || status.Role != protocol.RepairDestination || status.RepairID != repair.Instruction.RepairID || status.InstructionDigest != repair.InstructionDigest || status.Instruction.InstructionDigest != repair.InstructionDigest || !equalRepairDefinition(protocolRepairDefinition(status.Instruction), repair.Instruction) || status.State < protocol.RepairPending || status.State > protocol.RepairComplete || status.ErrorCode != 0 {
		return ErrTransferUnauthorized
	}
	if status.RecordCount > repair.Instruction.ExpectedRecordCount || status.TotalBytes > repair.Instruction.ExpectedTotalBytes || (status.RecordCount == 0) != (status.TotalBytes == 0) {
		return ErrTransferUnauthorized
	}
	if status.State == protocol.RepairPending && (status.RecordCount != 0 || status.TotalBytes != 0 || status.ContentDigest != model.EmptyResultInventoryDigest(status.InstructionDigest)) {
		return ErrTransferUnauthorized
	}
	if status.State == protocol.RepairComplete && (status.RecordCount != repair.Instruction.ExpectedRecordCount || status.TotalBytes != repair.Instruction.ExpectedTotalBytes || status.ContentDigest != repair.Instruction.ExpectedContentDigest) {
		return ErrTransferUnauthorized
	}
	return nil
}

func repairInventory(stored []store.StoredResult, definition model.RepairResultPartitionDefinition) ([]model.ResultRecord, error) {
	watermarks := make(map[model.TaskID]uint64, len(definition.Checkpoints))
	for _, checkpoint := range definition.Checkpoints {
		watermarks[checkpoint.Source] = checkpoint.Watermark
	}
	result := make([]model.ResultRecord, 0)
	for _, candidate := range stored {
		record := candidate.Record
		if record.TupleID.JobID != definition.JobID || record.SinkTask != definition.SinkTask || record.SpecificationHash != definition.SpecificationHash {
			continue
		}
		if len(watermarks) != 0 {
			watermark, ok := watermarks[record.TupleID.SourceTask]
			if !ok || record.TupleID.SourceSequence > watermark {
				continue
			}
		}
		if err := record.Validate(); err != nil {
			return nil, err
		}
		result = append(result, cloneResultRecord(record))
	}
	sort.Slice(result, func(i, j int) bool { return tupleTransferLess(result[i].TupleID, result[j].TupleID) })
	for i := 1; i < len(result); i++ {
		if result[i-1].TupleID == result[i].TupleID {
			return nil, ErrTransferIdentityReuse
		}
	}
	return result, nil
}

func validateRepairSourceCopies(stored []store.StoredResult, definition model.RepairResultPartitionDefinition) error {
	for _, candidate := range stored {
		record := candidate.Record
		if record.TupleID.JobID != definition.JobID || record.SinkTask != definition.SinkTask || record.SpecificationHash != definition.SpecificationHash || !repairCoversTuple(definition, record.TupleID) {
			continue
		}
		if err := candidate.Provenance.Validate(record); err != nil {
			return err
		}
		node, _, _, _, ok := endpointsForRole(candidate.Provenance.ReplicaSet, candidate.Provenance.DestinationRole)
		if !ok || node != definition.SourceNodeID || candidate.Provenance.ReplicaSet.SinkTask != definition.SinkTask {
			return ErrTransferUnauthorized
		}
	}
	return nil
}

func validateRepairProgress(repair store.ResultRepairRecord, covered []model.ResultRecord) error {
	if repair.NextRecord > uint64(len(covered)) || repair.NextRecord != repair.RecordCount || repair.NextOffset != repair.TotalBytes {
		return ErrTransferIdentityReuse
	}
	if repair.NextRecord == 0 {
		wantDigest := model.EmptyResultInventoryDigest(repair.InstructionDigest)
		if repair.State == store.RepairComplete && repair.Instruction.ExpectedRecordCount == 0 {
			wantDigest = repair.Instruction.ExpectedContentDigest
		}
		if repair.NextOffset != 0 || repair.ContentDigest != wantDigest {
			return ErrTransferIdentityReuse
		}
		return nil
	}
	count, total, digest, err := ResultInventoryAggregate(repair.Instruction.InventoryQueryDigest, covered[:repair.NextRecord])
	if err != nil || count != repair.NextRecord || total != repair.NextOffset || digest != repair.ContentDigest {
		return ErrTransferIdentityReuse
	}
	return nil
}

func findStoredResult(stored []store.StoredResult, record model.ResultRecord) (store.StoredResult, bool) {
	for _, candidate := range stored {
		if candidate.Record.SinkTask == record.SinkTask && candidate.Record.TupleID == record.TupleID {
			return candidate, true
		}
	}
	return store.StoredResult{}, false
}

func repairCoversTuple(definition model.RepairResultPartitionDefinition, tuple model.TupleID) bool {
	if len(definition.Checkpoints) == 0 {
		return true
	}
	for _, checkpoint := range definition.Checkpoints {
		if checkpoint.Source == tuple.SourceTask {
			return tuple.SourceSequence <= checkpoint.Watermark
		}
	}
	return false
}

// ResultInventoryAggregate computes the canonical resumable inventory summary.
func ResultInventoryAggregate(queryDigest [32]byte, records []model.ResultRecord) (uint64, uint64, [32]byte, error) {
	if queryDigest == ([32]byte{}) {
		return 0, 0, [32]byte{}, errors.New("zero inventory query digest")
	}
	digest := model.EmptyResultInventoryDigest(queryDigest)
	var total uint64
	for index, record := range records {
		if index > 0 && !tupleTransferLess(records[index-1].TupleID, record.TupleID) {
			return 0, 0, [32]byte{}, errors.New("result inventory is not canonical and unique")
		}
		entry, err := marshalResultInventoryEntry(record)
		if err != nil || total > ^uint64(0)-uint64(len(entry)) {
			return 0, 0, [32]byte{}, errors.New("invalid or oversized result inventory")
		}
		digest = extendInventoryDigest(digest, uint64(index), entry)
		total += uint64(len(entry))
	}
	return uint64(len(records)), total, digest, nil
}

func marshalResultInventoryEntry(record model.ResultRecord) ([]byte, error) {
	logical, err := model.MarshalResultRecord(record)
	if err != nil {
		return nil, err
	}
	entryBytes := uint64(len(logical)) + 4
	if entryBytes < model.ResultArtifactMinRecordBytesV1 || entryBytes > model.ResultArtifactMaxRecordBytesV1 || uint64(len(logical)) > uint64(^uint32(0)) {
		return nil, errors.New("result logical bytes outside inventory entry bounds")
	}
	entry := make([]byte, int(entryBytes))
	binary.BigEndian.PutUint32(entry[:4], uint32(len(logical)))
	copy(entry[4:], logical)
	return entry, nil
}

func validateRepairInventory(definition model.RepairResultPartitionDefinition, records []model.ResultRecord) error {
	count, total, digest, err := ResultInventoryAggregate(definition.InventoryQueryDigest, records)
	if err != nil {
		return err
	}
	if count != definition.ExpectedRecordCount || total != definition.ExpectedTotalBytes || digest != definition.ExpectedContentDigest {
		return ErrTransferIdentityReuse
	}
	return nil
}

func extendInventoryDigest(prior [32]byte, index uint64, stream []byte) [32]byte {
	encoded := []byte("crane-result-inventory-chain-v1\x00")
	encoded = append(encoded, prior[:]...)
	encoded = appendUint64Transfer(encoded, index)
	encoded = appendUint64Transfer(encoded, uint64(len(stream)))
	encoded = append(encoded, stream...)
	return sha256.Sum256(encoded)
}

func endpointsForRole(replica model.ResultReplicaSet, destination model.ResultReplicaRole) (uint16, model.WorkerEpoch, uint16, model.WorkerEpoch, bool) {
	switch destination {
	case model.PrimaryReplica:
		return replica.PrimaryNodeID, replica.PrimaryEpoch, replica.SecondaryNodeID, replica.SecondaryEpoch, true
	case model.SecondaryReplica:
		return replica.SecondaryNodeID, replica.SecondaryEpoch, replica.PrimaryNodeID, replica.PrimaryEpoch, true
	default:
		return 0, model.WorkerEpoch{}, 0, model.WorkerEpoch{}, false
	}
}

func repairProtocolDefinition(definition model.RepairResultPartitionDefinition) protocol.RepairResultPartition {
	return protocol.RepairResultPartition{RepairID: definition.RepairID, CoordinatorEpoch: definition.CoordinatorEpoch, JobID: definition.JobID, AssignmentRevision: definition.AssignmentRevision, AssignmentDigest: definition.AssignmentDigest, SourceNodeID: definition.SourceNodeID, SourceWorkerEpoch: definition.SourceWorkerEpoch, DestinationNodeID: definition.DestinationNodeID, DestinationWorkerEpoch: definition.DestinationWorkerEpoch, SinkTask: definition.SinkTask, SpecificationHash: definition.SpecificationHash, Checkpoints: append([]model.SourceCheckpoint(nil), definition.Checkpoints...), CheckpointDigest: definition.CheckpointDigest, InventoryQueryDigest: definition.InventoryQueryDigest, ExpectedRecordCount: definition.ExpectedRecordCount, ExpectedTotalBytes: definition.ExpectedTotalBytes, ExpectedContentDigest: definition.ExpectedContentDigest, InstructionDigest: model.RepairInstructionDigest(definition)}
}

func protocolRepairDefinition(definition protocol.RepairResultPartition) model.RepairResultPartitionDefinition {
	return model.RepairResultPartitionDefinition{RepairID: definition.RepairID, CoordinatorEpoch: definition.CoordinatorEpoch, JobID: definition.JobID, AssignmentRevision: definition.AssignmentRevision, AssignmentDigest: definition.AssignmentDigest, SourceNodeID: definition.SourceNodeID, SourceWorkerEpoch: definition.SourceWorkerEpoch, DestinationNodeID: definition.DestinationNodeID, DestinationWorkerEpoch: definition.DestinationWorkerEpoch, SinkTask: definition.SinkTask, SpecificationHash: definition.SpecificationHash, Checkpoints: append([]model.SourceCheckpoint(nil), definition.Checkpoints...), CheckpointDigest: definition.CheckpointDigest, InventoryQueryDigest: definition.InventoryQueryDigest, ExpectedRecordCount: definition.ExpectedRecordCount, ExpectedTotalBytes: definition.ExpectedTotalBytes, ExpectedContentDigest: definition.ExpectedContentDigest}
}

func equalRepairDefinition(a, b model.RepairResultPartitionDefinition) bool {
	if a.RepairID != b.RepairID || a.CoordinatorEpoch != b.CoordinatorEpoch || a.JobID != b.JobID || a.AssignmentRevision != b.AssignmentRevision || a.AssignmentDigest != b.AssignmentDigest || a.SourceNodeID != b.SourceNodeID || a.SourceWorkerEpoch != b.SourceWorkerEpoch || a.DestinationNodeID != b.DestinationNodeID || a.DestinationWorkerEpoch != b.DestinationWorkerEpoch || a.SinkTask != b.SinkTask || a.SpecificationHash != b.SpecificationHash || a.CheckpointDigest != b.CheckpointDigest || a.InventoryQueryDigest != b.InventoryQueryDigest || a.ExpectedRecordCount != b.ExpectedRecordCount || a.ExpectedTotalBytes != b.ExpectedTotalBytes || a.ExpectedContentDigest != b.ExpectedContentDigest || len(a.Checkpoints) != len(b.Checkpoints) {
		return false
	}
	for index := range a.Checkpoints {
		if a.Checkpoints[index] != b.Checkpoints[index] {
			return false
		}
	}
	return true
}

func equalRecordChunk(a, b protocol.ResultRecordChunk) bool {
	return a.Transfer.TransferID == b.Transfer.TransferID && a.Transfer.JobID == b.Transfer.JobID && a.Transfer.TotalLength == b.Transfer.TotalLength && a.Transfer.Checksum == b.Transfer.Checksum && a.Transfer.Offset == b.Transfer.Offset && a.Transfer.Final == b.Transfer.Final && bytes.Equal(a.Transfer.Data, b.Transfer.Data) && a.Record.TupleID == b.Record.TupleID && a.Record.SinkTask == b.Record.SinkTask && a.Record.SpecificationHash == b.Record.SpecificationHash && a.Record.Checksum == b.Record.Checksum && bytes.Equal(a.Record.Value, b.Record.Value) && a.Provenance == b.Provenance && a.DestinationNodeID == b.DestinationNodeID && a.DestinationWorkerEpoch == b.DestinationWorkerEpoch && a.RepairID == b.RepairID && a.RepairInstructionDigest == b.RepairInstructionDigest
}

func tupleTransferLess(a, b model.TupleID) bool {
	if comparison := bytes.Compare(a.JobID[:], b.JobID[:]); comparison != 0 {
		return comparison < 0
	}
	if comparison := compareTask(a.SourceTask, b.SourceTask); comparison != 0 {
		return comparison < 0
	}
	if a.SourceSequence != b.SourceSequence {
		return a.SourceSequence < b.SourceSequence
	}
	return bytes.Compare(a.PathDigest[:], b.PathDigest[:]) < 0
}

func appendUint16Transfer(destination []byte, value uint16) []byte {
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], value)
	return append(destination, encoded[:]...)
}

func appendUint64Transfer(destination []byte, value uint64) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return append(destination, encoded[:]...)
}

func appendTaskTransfer(destination []byte, task model.TaskID) []byte {
	destination = append(destination, task.JobID[:]...)
	destination = appendUint16Transfer(destination, task.StageID)
	return appendUint16Transfer(destination, task.Partition)
}

func appendEpochTransfer(destination []byte, epoch model.CoordinatorEpoch) []byte {
	destination = appendUint64Transfer(destination, epoch.Term)
	destination = appendUint64Transfer(destination, epoch.BeginIndex)
	destination = appendUint16Transfer(destination, epoch.Coordinator)
	return append(destination, epoch.Nonce[:]...)
}
