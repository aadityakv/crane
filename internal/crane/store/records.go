package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
)

const (
	domainRecordSchema                uint16 = 1
	outboxRecordSchema                uint16 = 2
	sourceRecordSchema                uint16 = 2
	checkpointRecordSchema            uint16 = 2
	eventAckRecordSchema              uint16 = 2
	checkpointObservationRecordSchema uint16 = 2
)

const (
	recordFence RecordType = iota + 1
	recordAssignment
	recordDelivery
	recordDeliveryProcessed
	recordDeliveryCompleted
	recordCheckpoint
	recordResult
	recordEvent
	recordEventAck
	recordRepair
	recordSource
	recordOutboxAck
	recordOutboxRetry
	recordCheckpointObservation
)

// Durable boundary names published to the integration hook. Each fires
// exactly once per successful transaction of the named kind, after the WAL
// fsync and before the mutation returns.
const (
	// BoundaryFence follows a durable coordinator fence install.
	BoundaryFence = "fence"
	// BoundaryAssignmentClosed/Running/Draining follow a durable assignment
	// install that changed state, named by the installed scheduling state.
	BoundaryAssignmentClosed   = "assignment-closed"
	BoundaryAssignmentRunning  = "assignment-running"
	BoundaryAssignmentDraining = "assignment-draining"
	// BoundaryDeliveryReceived follows new durable Received custody; a
	// duplicate probe answered from prior custody publishes nothing.
	BoundaryDeliveryReceived = "delivery-received"
	// BoundaryDeliveryProcessed follows durable deterministic outputs/outboxes.
	BoundaryDeliveryProcessed = "delivery-processed"
	// BoundaryDeliveryCompleted follows durable downstream completion.
	BoundaryDeliveryCompleted = "delivery-completed"
	// BoundaryCheckpointApplied follows a durable owned-source watermark.
	BoundaryCheckpointApplied = "checkpoint-applied"
	// BoundaryCheckpointObserved follows a durable replica-side observation.
	BoundaryCheckpointObserved = "checkpoint-observed"
	// BoundaryResultUpserted follows a durable result copy.
	BoundaryResultUpserted = "result-upserted"
	// BoundaryEventPersisted follows a durable worker event.
	BoundaryEventPersisted = "event-persisted"
	// BoundaryEventsAcknowledged follows durable event retirement.
	BoundaryEventsAcknowledged = "events-acknowledged"
	// BoundaryRepairPending/Streaming/Complete/Failed follow a durable repair
	// record, named by its state.
	BoundaryRepairPending   = "repair-pending"
	BoundaryRepairStreaming = "repair-streaming"
	BoundaryRepairComplete  = "repair-complete"
	BoundaryRepairFailed    = "repair-failed"
	// BoundarySourceAdvanced follows a durable source cursor advance.
	BoundarySourceAdvanced = "source-advanced"
	// BoundaryOutboxDispatched/Accepted/Completed follow durable outbox
	// retry-state transitions.
	BoundaryOutboxDispatched = "outbox-dispatched"
	BoundaryOutboxAccepted   = "outbox-accepted"
	BoundaryOutboxCompleted  = "outbox-completed"
)

func assignmentBoundary(state model.SchedulingState) string {
	switch state {
	case model.SchedulingClosed:
		return BoundaryAssignmentClosed
	case model.SchedulingDraining:
		return BoundaryAssignmentDraining
	default:
		return BoundaryAssignmentRunning
	}
}

func repairBoundary(state RepairState) string {
	switch state {
	case RepairStreaming:
		return BoundaryRepairStreaming
	case RepairComplete:
		return BoundaryRepairComplete
	case RepairFailed:
		return BoundaryRepairFailed
	default:
		return BoundaryRepairPending
	}
}

// DeliveryState is the durable receiver state returned for duplicate custody.
type DeliveryState uint8

const (
	// Received means custody and its complete reservation are durable.
	Received DeliveryState = iota + 1
	// Processed means deterministic outputs and downstream outboxes are durable.
	Processed
	// Completed means the complete downstream tree is durable.
	Completed
	// Compacted is a permanent checkpoint tombstone for a completed delivery.
	Compacted
)

// DeliveryRecord is one owned durable delivery, reservation, and deterministic output.
type DeliveryRecord struct {
	// ID is the stable logical custody identity.
	ID model.DeliveryID
	// Tuple is the owned canonical input payload.
	Tuple model.Tuple
	// Producer is the exact sending-task assignment token.
	Producer model.AssignmentToken
	// Destination is the exact local receiving-task assignment token.
	Destination model.AssignmentToken
	// AssignmentRevision fences the complete installed placement revision.
	AssignmentRevision uint64
	// AssignmentDigest binds the complete installed placement.
	AssignmentDigest [32]byte
	// CoordinatorEpoch is the authority under which custody was accepted.
	CoordinatorEpoch model.CoordinatorEpoch
	// State is the current durable delivery state.
	State DeliveryState
	// Reservation is the exact worst-case durable byte reservation.
	Reservation uint64
	// Outputs are owned deterministic processed tuples.
	Outputs []model.Tuple
	// OutboxIDs bind processed output to its atomic downstream outboxes.
	OutboxIDs        []model.DeliveryID
	definitionDigest [32]byte
}

// Clone returns an independently owned delivery.
func (record DeliveryRecord) Clone() DeliveryRecord {
	record.Tuple = cloneTuple(record.Tuple)
	record.Outputs = cloneTuples(record.Outputs)
	record.OutboxIDs = append([]model.DeliveryID(nil), record.OutboxIDs...)
	return record
}

// OutboxRecord is one owned downstream delivery retained until completion/checkpoint.
type OutboxRecord struct {
	// ID is the stable downstream custody identity.
	ID model.DeliveryID
	// Tuple is the owned canonical downstream payload.
	Tuple model.Tuple
	// Producer is the exact local producing assignment token.
	Producer model.AssignmentToken
	// Destination is the exact remote receiving assignment token.
	Destination model.AssignmentToken
	// AssignmentRevision fences the complete placement revision.
	AssignmentRevision uint64
	// AssignmentDigest binds the complete placement.
	AssignmentDigest [32]byte
	// CoordinatorEpoch is the authority under which the outbox was created.
	CoordinatorEpoch model.CoordinatorEpoch
	// Completed records a durable downstream completion acknowledgment.
	Completed bool
	// Accepted records durable downstream custody and selects the longer
	// completion-wait retry phase.
	Accepted bool
	// RetryDeadlineUnixNano is the injected-clock absolute retry deadline. Zero
	// means the outbox has never been dispatched.
	RetryDeadlineUnixNano int64
}

// Clone returns an independently owned outbox.
func (record OutboxRecord) Clone() OutboxRecord {
	record.Tuple = cloneTuple(record.Tuple)
	return record
}

// SourceCursor is one durable source sequence/checkpoint position.
type SourceCursor struct {
	// Source identifies the source task partition.
	Source model.TaskID
	// NextSequence is the next source sequence not yet durably emitted.
	NextSequence uint64
	// EOF is the immutable finite source endpoint, or zero for empty/unknown.
	EOF uint64
	// Watermark is the greatest committed compactable source sequence.
	Watermark uint64
	// RaftIndex is the committed checkpoint notice index.
	RaftIndex uint64
	// CheckpointRevision is the exact committed source-checkpoint subject
	// revision. Zero means no positive checkpoint or a schema-v1 cursor whose
	// revision must be migrated from its exact pending completion proof.
	CheckpointRevision uint64
	// CheckpointAuthority durably binds the exact authority that produced the
	// last accepted checkpoint. It is zero only before the first checkpoint or
	// for a recovered schema-v1 cursor whose proof is unavailable.
	CheckpointAuthority CheckpointAuthority
}

// CheckpointAuthority is the bounded durable proof for one accepted source
// checkpoint, independent of whatever assignment is installed later.
type CheckpointAuthority struct {
	JobControlRevision uint64
	AssignmentRevision uint64
	AssignmentDigest   [32]byte
	SourceToken        model.AssignmentToken
	CoordinatorEpoch   model.CoordinatorEpoch
}

// CommittedCheckpoint is one durable committed-checkpoint observation retained
// by a participating worker that does not own the source cursor.
type CommittedCheckpoint struct {
	Notice             model.CheckpointNotice
	JobControlRevision uint64
	AssignmentRevision uint64
	AssignmentDigest   [32]byte
}

// StoredResult couples immutable logical result bytes to separate copy provenance.
type StoredResult struct {
	// Record is immutable logical collect output.
	Record model.ResultRecord
	// Provenance separately fences this physical copy.
	Provenance model.ResultCopyProvenance
	canonical  []byte
}

// RepairEndpointRole identifies which endpoint owns local repair progress.
type RepairEndpointRole = protocol.RepairEndpointRole

const (
	// RepairSource streams retained historical records.
	RepairSource RepairEndpointRole = protocol.RepairSource
	// RepairDestination idempotently installs current-copy records.
	RepairDestination RepairEndpointRole = protocol.RepairDestination
)

// RepairState is the durable lifecycle of one deterministic repair instruction.
type RepairState = protocol.ResultRepairState

const (
	// RepairPending has persisted authority but no network progress.
	RepairPending RepairState = protocol.RepairPending
	// RepairStreaming has durable next-record/next-offset progress.
	RepairStreaming RepairState = protocol.RepairStreaming
	// RepairComplete has durably matched the instruction summary.
	RepairComplete RepairState = protocol.RepairComplete
	// RepairFailed records a durable terminal error code.
	RepairFailed RepairState = protocol.RepairFailed
)

// ResultRepairRecord retains exact instruction identity and resumable progress.
type ResultRepairRecord struct {
	// Instruction is the complete deterministic historical-copy instruction.
	Instruction model.RepairResultPartitionDefinition
	// InstructionDigest binds every instruction byte.
	InstructionDigest [32]byte
	// Role selects the local endpoint authorized by this record.
	Role RepairEndpointRole
	// State is the durable repair lifecycle state.
	State RepairState
	// NextRecord is the next logical record to stream or install.
	NextRecord uint64
	// NextOffset is the next byte offset within the current stream.
	NextOffset uint64
	// RecordCount is the durable result summary count.
	RecordCount uint64
	// TotalBytes is the durable result summary byte count.
	TotalBytes uint64
	// ContentDigest is the durable result summary digest.
	ContentDigest [32]byte
	// ErrorCode is nonzero only for a durably failed repair.
	ErrorCode protocol.WorkerErrorCode
}

// InstalledAssignment owns a complete placement and its canonical specification.
type InstalledAssignment struct {
	// Assignment is the owned complete placement set.
	Assignment model.AssignmentSet
	// SpecificationBytes are the durable canonical TopologySpec bytes.
	SpecificationBytes []byte
	// Topology is the owned decoded and revalidated recovery product.
	Topology model.ValidatedTopology
	// JobControlRevision fences the coordinator job state.
	JobControlRevision uint64
	// SchedulingState controls new source/custody admission.
	SchedulingState model.SchedulingState
	// CoordinatorEpoch is the authority that installed this copy.
	CoordinatorEpoch model.CoordinatorEpoch
}

// RecoveredWork is the bounded validated high-level worker state reconstructed from WAL.
type RecoveredWork struct {
	// Fence is the highest durable coordinator authority.
	Fence model.CoordinatorEpoch
	// Assignments are sorted owned complete installed placements.
	Assignments []InstalledAssignment
	// Sources are durable source and checkpoint cursors.
	Sources []SourceCursor
	// Checkpoints are durable committed observations for assignment participants.
	Checkpoints []CommittedCheckpoint
	// Deliveries are live custody records and compact tombstones.
	Deliveries []DeliveryRecord
	// Outboxes are durable downstream replay records.
	Outboxes []OutboxRecord
	// Results are permanently retained logical records and copy provenance.
	Results []StoredResult
	// Repairs are unfinished and completed deterministic repair records.
	Repairs []ResultRepairRecord
	// PendingEvents are strictly increasing unacknowledged worker reports.
	PendingEvents []model.WorkerEvent
	// NextTransactionID is the next durable worker event identity.
	NextTransactionID uint64
	indexes           *workIndexes
}

type resultKey struct {
	SinkTask model.TaskID
	TupleID  model.TupleID
}

type workIndexes struct {
	results          *resultNode
	resultBytesByJob map[model.JobID]uint64
	resultCount      uint64
}

type resultNode struct {
	key         resultKey
	value       StoredResult
	height      uint16
	left, right *resultNode
}

// Clone returns a deeply owned high-level state.
func (work RecoveredWork) Clone() RecoveredWork {
	result := work
	result.Assignments = make([]InstalledAssignment, len(work.Assignments))
	for i, assignment := range work.Assignments {
		result.Assignments[i] = assignment
		result.Assignments[i].Assignment.Tasks = append([]model.AssignmentToken(nil), assignment.Assignment.Tasks...)
		result.Assignments[i].Assignment.ResultReplicas = append([]model.ResultReplicaSet(nil), assignment.Assignment.ResultReplicas...)
		result.Assignments[i].SpecificationBytes = append([]byte(nil), assignment.SpecificationBytes...)
		if len(assignment.SpecificationBytes) != 0 {
			result.Assignments[i].Topology, _ = model.DecodeTopology(assignment.SpecificationBytes)
		}
	}
	result.Sources = append([]SourceCursor(nil), work.Sources...)
	result.Checkpoints = append([]CommittedCheckpoint(nil), work.Checkpoints...)
	result.Deliveries = make([]DeliveryRecord, len(work.Deliveries))
	for i := range work.Deliveries {
		result.Deliveries[i] = work.Deliveries[i].Clone()
	}
	result.Outboxes = make([]OutboxRecord, len(work.Outboxes))
	for i := range work.Outboxes {
		result.Outboxes[i] = work.Outboxes[i].Clone()
	}
	result.Results = nil
	appendOwnedResults(work.indexes, &result.Results)
	if work.indexes == nil {
		for i := range work.Results {
			owned := work.Results[i]
			owned.Record.Value = append([]byte(nil), owned.Record.Value...)
			owned.canonical = nil
			result.Results = append(result.Results, owned)
		}
	}
	result.Repairs = make([]ResultRepairRecord, len(work.Repairs))
	copy(result.Repairs, work.Repairs)
	for i := range result.Repairs {
		result.Repairs[i].Instruction.Checkpoints = append([]model.SourceCheckpoint(nil), work.Repairs[i].Instruction.Checkpoints...)
	}
	result.PendingEvents = make([]model.WorkerEvent, len(work.PendingEvents))
	for i := range work.PendingEvents {
		result.PendingEvents[i] = cloneEvent(work.PendingEvents[i])
	}
	if result.NextTransactionID == 0 {
		result.NextTransactionID = 1
	}
	result.indexes = nil
	sort.Slice(result.Results, func(i, j int) bool {
		a, b := result.Results[i].Record, result.Results[j].Record
		if a.SinkTask != b.SinkTask {
			return taskLess(a.SinkTask, b.SinkTask)
		}
		return tupleLess(a.TupleID, b.TupleID)
	})
	return result
}

type workReducer struct {
	current, transaction RecoveredWork
	inTransaction        bool
	allowLegacy          bool
	prepared             [recordCheckpointObservation + 1]bool
}

func newRecoveredWork() RecoveredWork {
	return RecoveredWork{NextTransactionID: 1, indexes: &workIndexes{resultBytesByJob: make(map[model.JobID]uint64)}}
}

func newRecoveryWorkReducer() *workReducer {
	return &workReducer{current: newRecoveredWork(), allowLegacy: true}
}

// BeginTransaction starts one prospective atomic high-level reduction.
func (r *workReducer) BeginTransaction(uint32) error {
	if r.inTransaction {
		return errors.New("nested transaction")
	}
	r.transaction = r.current
	r.prepared = [recordCheckpointObservation + 1]bool{}
	r.inTransaction = true
	return nil
}

// ConsumeRecord validates and applies one canonical record to prospective state.
func (r *workReducer) ConsumeRecord(record Record) error {
	if !r.inTransaction {
		return errors.New("record outside transaction")
	}
	r.prepare(record.Type)
	return applyDomainRecord(&r.transaction, record, r.allowLegacy)
}

func (r *workReducer) prepare(recordType RecordType) {
	clone := func(kind RecordType) bool {
		if r.prepared[kind] {
			return false
		}
		r.prepared[kind] = true
		return true
	}
	switch recordType {
	case recordAssignment:
		if clone(recordAssignment) {
			r.transaction.Assignments = append([]InstalledAssignment(nil), r.transaction.Assignments...)
		}
	case recordDelivery:
		if clone(recordDelivery) {
			r.transaction.Deliveries = append([]DeliveryRecord(nil), r.transaction.Deliveries...)
		}
	case recordDeliveryProcessed:
		if clone(recordDelivery) {
			r.transaction.Deliveries = append([]DeliveryRecord(nil), r.transaction.Deliveries...)
		}
		if clone(recordOutboxAck) {
			r.transaction.Outboxes = append([]OutboxRecord(nil), r.transaction.Outboxes...)
		}
	case recordDeliveryCompleted:
		if clone(recordDelivery) {
			r.transaction.Deliveries = append([]DeliveryRecord(nil), r.transaction.Deliveries...)
		}
	case recordCheckpoint:
		if clone(recordSource) {
			r.transaction.Sources = append([]SourceCursor(nil), r.transaction.Sources...)
		}
		if clone(recordDelivery) {
			r.transaction.Deliveries = append([]DeliveryRecord(nil), r.transaction.Deliveries...)
		}
		if clone(recordOutboxAck) {
			r.transaction.Outboxes = append([]OutboxRecord(nil), r.transaction.Outboxes...)
		}
	case recordResult:
		if clone(recordResult) {
			r.transaction.indexes = cloneWorkIndexes(r.transaction.indexes)
		}
	case recordEvent, recordEventAck:
		if clone(recordEvent) {
			r.transaction.PendingEvents = append([]model.WorkerEvent(nil), r.transaction.PendingEvents...)
		}
	case recordRepair:
		if clone(recordRepair) {
			r.transaction.Repairs = append([]ResultRepairRecord(nil), r.transaction.Repairs...)
		}
	case recordSource:
		if clone(recordSource) {
			r.transaction.Sources = append([]SourceCursor(nil), r.transaction.Sources...)
		}
		if clone(recordOutboxAck) {
			r.transaction.Outboxes = append([]OutboxRecord(nil), r.transaction.Outboxes...)
		}
	case recordOutboxAck, recordOutboxRetry:
		if clone(recordOutboxAck) {
			r.transaction.Outboxes = append([]OutboxRecord(nil), r.transaction.Outboxes...)
		}
	case recordCheckpointObservation:
		if clone(recordCheckpointObservation) {
			r.transaction.Checkpoints = append([]CommittedCheckpoint(nil), r.transaction.Checkpoints...)
		}
	}
}

// CommitTransaction publishes one completely validated prospective transaction.
func (r *workReducer) CommitTransaction() error {
	if !r.inTransaction {
		return errors.New("commit outside transaction")
	}
	r.current = r.transaction
	r.inTransaction = false
	return nil
}

func applyDomainRecord(work *RecoveredWork, record Record, allowLegacy bool) error {
	switch record.Type {
	case recordFence:
		epoch, err := decodeFence(record.Payload)
		if err != nil {
			return err
		}
		return applyFence(work, epoch)
	case recordAssignment:
		assignment, err := decodeAssignment(record.Payload)
		if err != nil {
			return err
		}
		return applyAssignment(work, assignment)
	case recordDelivery, recordDeliveryProcessed:
		delivery, outboxes, err := decodeDeliveryRecord(record.Payload)
		if err != nil {
			return err
		}
		return applyDelivery(work, delivery, outboxes, record.Type == recordDeliveryProcessed)
	case recordDeliveryCompleted:
		id, err := decodeDeliveryIDPayload(record.Payload)
		if err != nil {
			return err
		}
		return applyCompleted(work, id)
	case recordCheckpoint:
		schema := recordPayloadSchema(record.Payload)
		notice, err := decodeCheckpoint(record.Payload)
		if err != nil {
			return err
		}
		if schema == domainRecordSchema {
			if !allowLegacy {
				return errors.New("legacy checkpoint schema is recovery-only")
			}
			if exactLegacyCheckpointProofAvailable(work, notice) {
				return applyCheckpoint(work, notice)
			}
			return applyLegacyCheckpoint(work, notice)
		}
		return applyCheckpoint(work, notice)
	case recordResult:
		result, err := decodeStoredResult(record.Payload)
		if err != nil {
			return err
		}
		return applyResult(work, result)
	case recordEvent:
		event, err := decodeEvent(record.Payload)
		if err != nil {
			return err
		}
		return applyEvent(work, event)
	case recordEventAck:
		schema := recordPayloadSchema(record.Payload)
		through, err := decodeUint64Payload(record.Payload)
		if err != nil {
			return err
		}
		if schema == domainRecordSchema {
			if !allowLegacy {
				return errors.New("legacy event acknowledgement schema is recovery-only")
			}
			return applyLegacyEventAck(work, through)
		}
		return applyEventAck(work, through)
	case recordRepair:
		repair, err := decodeRepair(record.Payload)
		if err != nil {
			return err
		}
		return applyRepair(work, repair)
	case recordSource:
		cursor, outboxes, err := decodeSource(record.Payload)
		if err != nil {
			return err
		}
		return applySource(work, cursor, outboxes)
	case recordOutboxAck:
		id, err := decodeDeliveryIDPayload(record.Payload)
		if err != nil {
			return err
		}
		return applyOutboxAck(work, id)
	case recordOutboxRetry:
		update, err := decodeOutboxRetry(record.Payload)
		if err != nil {
			return err
		}
		return applyOutboxRetry(work, update)
	case recordCheckpointObservation:
		observation, err := decodeCheckpointObservation(record.Payload)
		if err != nil {
			return err
		}
		return applyCheckpointObservation(work, observation)
	default:
		return fmt.Errorf("unknown domain record type %d", record.Type)
	}
}

func applyFence(work *RecoveredWork, epoch model.CoordinatorEpoch) error {
	if err := epoch.Validate(); err != nil {
		return err
	}
	comparison := compareEpochOrder(epoch, work.Fence)
	if work.Fence == (model.CoordinatorEpoch{}) {
		work.Fence = epoch
		return nil
	}
	if comparison < 0 || comparison == 0 && epoch != work.Fence {
		return errors.New("stale or colliding coordinator epoch")
	}
	if comparison > 0 {
		work.Fence = epoch
	}
	return nil
}

func applyAssignment(work *RecoveredWork, installed InstalledAssignment) error {
	if work.Fence == (model.CoordinatorEpoch{}) || installed.CoordinatorEpoch != work.Fence {
		return errors.New("assignment coordinator fence mismatch")
	}
	decoded, err := model.DecodeTopology(installed.SpecificationBytes)
	if err != nil {
		return err
	}
	if err := installed.Assignment.Validate(decoded); err != nil {
		return err
	}
	if installed.JobControlRevision == 0 || installed.SchedulingState < model.SchedulingClosed || installed.SchedulingState > model.SchedulingDraining {
		return errors.New("invalid installed assignment metadata")
	}
	installed.Topology = decoded
	index := assignmentIndex(work.Assignments, installed.Assignment.JobID)
	if index >= 0 {
		prior := work.Assignments[index]
		if installed.Assignment.Revision < prior.Assignment.Revision {
			return errors.New("stale assignment revision")
		}
		if installed.Assignment.Revision == prior.Assignment.Revision {
			if equalInstalledAssignment(prior, installed) {
				return nil
			}
			contentEqual := equalInstalledAssignmentContent(prior, installed)
			if contentEqual && compareEpochOrder(installed.CoordinatorEpoch, prior.CoordinatorEpoch) > 0 {
				// Leadership rebind: content identical under strictly newer
				// committed authority durably rebinds the fence owner and
				// records the incoming worker-local scheduling state.
				work.Assignments[index] = cloneInstalled(installed)
				return nil
			}
			if contentEqual && installed.CoordinatorEpoch == prior.CoordinatorEpoch && admissionSchedulingProgression(prior.SchedulingState, installed.SchedulingState) {
				// Admission progressions at the equal current fence: verified
				// activation (Closed→Running) and re-fence before
				// re-verification (Running→Closed) change only worker-local
				// admission state, never attempts or custody.
				work.Assignments[index] = cloneInstalled(installed)
				return nil
			}
			if installed.Assignment.Digest != prior.Assignment.Digest || !bytes.Equal(installed.SpecificationBytes, prior.SpecificationBytes) || !equalTokens(installed.Assignment.Tasks, prior.Assignment.Tasks) || !equalReplicas(installed.Assignment.ResultReplicas, prior.Assignment.ResultReplicas) || installed.JobControlRevision <= prior.JobControlRevision {
				return model.ErrIdentityReuse
			}
			// Lifecycle fencing changes JobControlRevision independently of the
			// complete immutable AssignmentSet revision.
			work.Assignments[index] = cloneInstalled(installed)
			return nil
		}
		if installed.JobControlRevision < prior.JobControlRevision || !bytes.Equal(installed.SpecificationBytes, prior.SpecificationBytes) {
			return errors.New("assignment replacement regresses job revision or changes immutable topology")
		}
		for _, oldToken := range prior.Assignment.Tasks {
			for _, newToken := range installed.Assignment.Tasks {
				if newToken.Task != oldToken.Task {
					continue
				}
				if newToken.Attempt < oldToken.Attempt || (newToken.WorkerID != oldToken.WorkerID || newToken.WorkerEpoch != oldToken.WorkerEpoch) && newToken.Attempt <= oldToken.Attempt {
					return errors.New("assignment replacement regresses task attempt")
				}
				break
			}
		}
		work.Assignments[index] = cloneInstalled(installed)
	} else {
		if uint64(len(work.Assignments)) >= model.LimitsV1().MaxRetainedJobs {
			return ErrCapacity
		}
		work.Assignments = append(work.Assignments, cloneInstalled(installed))
	}
	sort.Slice(work.Assignments, func(i, j int) bool {
		return bytes.Compare(work.Assignments[i].Assignment.JobID[:], work.Assignments[j].Assignment.JobID[:]) < 0
	})
	return nil
}

func applyDelivery(work *RecoveredWork, delivery DeliveryRecord, outboxes []OutboxRecord, processed bool) error {
	if uint64(len(delivery.Outputs)) > model.LimitsV1().MaxOperatorOutputs || uint64(len(outboxes)) > model.LimitsV1().MaxDerivedDeliveries {
		return errors.New("processed output/outbox count exceeds v1 bounds")
	}
	assignment, ok := findAssignment(work, delivery.ID.Tuple.JobID)
	if !ok {
		return errors.New("delivery references unknown assignment")
	}
	if err := validateDelivery(delivery, assignment, work.Fence); err != nil {
		return err
	}
	digest, err := deliveryDefinitionDigest(delivery)
	if err != nil {
		return err
	}
	delivery.definitionDigest = digest
	index := deliveryIndex(work.Deliveries, delivery.ID)
	if !processed {
		if delivery.State != Received || len(outboxes) != 0 || len(delivery.Outputs) != 0 || len(delivery.OutboxIDs) != 0 {
			return errors.New("invalid received delivery record")
		}
		if index >= 0 {
			if !equalDeliveryDefinition(work.Deliveries[index], delivery) {
				return model.ErrIdentityReuse
			}
			return nil
		}
		if len(work.Deliveries) >= MaxTransactionRecords {
			return ErrCapacity
		}
		work.Deliveries = append(work.Deliveries, delivery.Clone())
		return nil
	}
	if index >= 0 {
		if err := validateProcessedOutputs(work.Deliveries[index], delivery.Outputs, assignment); err != nil {
			return err
		}
	}
	if index >= 0 && (work.Deliveries[index].State == Processed || work.Deliveries[index].State == Completed) {
		prior := work.Deliveries[index]
		if !equalDeliveryDefinition(prior, delivery) || !equalTuples(prior.Outputs, delivery.Outputs) || len(prior.OutboxIDs) != len(outboxes) {
			return model.ErrIdentityReuse
		}
		for outboxIndexInRecord, outbox := range outboxes {
			stored := outboxIndex(work.Outboxes, prior.OutboxIDs[outboxIndexInRecord])
			if outbox.Completed || outbox.Accepted || outbox.RetryDeadlineUnixNano != 0 || stored < 0 || outbox.ID != prior.OutboxIDs[outboxIndexInRecord] || !equalOutboxDefinition(work.Outboxes[stored], outbox) {
				return model.ErrIdentityReuse
			}
		}
		return nil
	}
	if index < 0 || work.Deliveries[index].State != Received {
		return errors.New("processed delivery has no received predecessor")
	}
	if delivery.State != Processed || !equalDeliveryDefinition(work.Deliveries[index], delivery) {
		return errors.New("processed delivery identity changed")
	}
	expected, err := expectedProcessedOutboxes(delivery, assignment)
	if err != nil {
		return err
	}
	if len(outboxes) != len(expected) {
		return errors.New("processed outboxes are not the complete topology-derived set")
	}
	if !outboxesCanonical(outboxes) {
		return errors.New("processed outboxes are not in canonical identity order")
	}
	seen := make(map[model.DeliveryID]struct{}, len(outboxes))
	for _, outbox := range outboxes {
		if _, duplicate := seen[outbox.ID]; duplicate {
			return errors.New("duplicate outbox")
		}
		seen[outbox.ID] = struct{}{}
		if outbox.Completed || outbox.Accepted || outbox.RetryDeadlineUnixNano != 0 {
			return errors.New("new processed outbox has retry or completion state")
		}
		if err := validateOutbox(outbox, assignment, work.Fence); err != nil {
			return err
		}
		if outbox.Producer != currentProducerToken(delivery, assignment) {
			return errors.New("outbox producer is not processed destination")
		}
		want, exists := expected[outbox.ID]
		if !exists || !equalOutboxDefinition(want, outbox) {
			return errors.New("processed outbox does not match topology-derived definition")
		}
		if outboxIndex(work.Outboxes, outbox.ID) >= 0 {
			return errors.New("outbox identity already exists")
		}
	}
	delivery.OutboxIDs = delivery.OutboxIDs[:0]
	for _, outbox := range outboxes {
		delivery.OutboxIDs = append(delivery.OutboxIDs, outbox.ID)
		work.Outboxes = append(work.Outboxes, outbox.Clone())
	}
	work.Deliveries[index] = delivery.Clone()
	return nil
}

func validateProcessedOutputs(delivery DeliveryRecord, outputs []model.Tuple, assignment InstalledAssignment) error {
	canonical, err := validateOutputTupleBounds(outputs)
	if err != nil {
		return err
	}
	stage, ok := findStage(assignment.Topology, delivery.Destination.Task.StageID)
	if !ok {
		return errors.New("processed delivery destination stage is absent")
	}
	expected, err := model.ExecuteOperator(stage.Operator, delivery.Tuple)
	if err != nil {
		return fmt.Errorf("execute installed operator: %w", err)
	}
	if uint64(len(expected)) > model.LimitsV1().MaxOperatorOutputs || len(expected) != len(canonical) {
		return errors.New("processed outputs do not match installed operator count")
	}
	for index := range expected {
		encoded, err := model.MarshalTuple(expected[index])
		if err != nil {
			return fmt.Errorf("installed operator output %d: %w", index, err)
		}
		if !bytes.Equal(encoded, canonical[index]) {
			return errors.New("processed outputs do not match installed operator bytes and order")
		}
	}
	return nil
}

func validateOutputTupleBounds(outputs []model.Tuple) ([][]byte, error) {
	if uint64(len(outputs)) > model.LimitsV1().MaxOperatorOutputs {
		return nil, errors.New("processed output count exceeds v1 bound")
	}
	canonical := make([][]byte, len(outputs))
	for index := range outputs {
		encoded, err := model.MarshalTuple(outputs[index])
		if err != nil {
			return nil, fmt.Errorf("processed output %d: %w", index, err)
		}
		canonical[index] = encoded
	}
	return canonical, nil
}

func applyCompleted(work *RecoveredWork, id model.DeliveryID) error {
	index := deliveryIndex(work.Deliveries, id)
	if index < 0 {
		return errors.New("completion references unknown delivery")
	}
	if work.Deliveries[index].State == Compacted || work.Deliveries[index].State == Completed {
		return nil
	}
	if work.Deliveries[index].State != Processed {
		return errors.New("delivery completion before processing")
	}
	for _, outboxID := range work.Deliveries[index].OutboxIDs {
		outbox := outboxIndex(work.Outboxes, outboxID)
		if outbox < 0 || !work.Outboxes[outbox].Completed {
			return errors.New("delivery completion before downstream outbox completion")
		}
	}
	work.Deliveries[index].State = Completed
	return nil
}

func applyCheckpoint(work *RecoveredWork, notice model.CheckpointNotice) error {
	if err := notice.Validate(); err != nil {
		return err
	}
	index := sourceIndex(work.Sources, notice.Source)
	if index >= 0 {
		prior := work.Sources[index]
		if notice.Watermark == prior.Watermark {
			if notice.RaftIndex != prior.RaftIndex {
				return model.ErrIdentityReuse
			}
			if prior.CheckpointRevision == 0 {
				assignment, ok := findAssignment(work, notice.JobID)
				if !ok {
					return ErrCheckpointAuthorityUnavailable
				}
				report, err := matchingLegacyCompletionReport(work, assignment, notice, prior)
				if err != nil {
					return errors.Join(ErrCheckpointAuthorityUnavailable, err)
				}
				if report.ExpectedCheckpointRevision == math.MaxUint64 {
					return ErrCapacity
				}
				prior.CheckpointRevision = report.ExpectedCheckpointRevision + 1
				prior.CheckpointAuthority = checkpointAuthority(assignment, report)
				work.Sources[index] = prior
				return nil
			}
			if notice.Epoch != prior.CheckpointAuthority.CoordinatorEpoch {
				return model.ErrIdentityReuse
			}
			return nil
		}
		if notice.Watermark < prior.Watermark || notice.RaftIndex < prior.RaftIndex {
			return errors.New("checkpoint regression")
		}
		if notice.RaftIndex == prior.RaftIndex {
			return model.ErrIdentityReuse
		}
	}
	if notice.Epoch != work.Fence {
		return errors.New("checkpoint coordinator fence mismatch")
	}
	assignment, ok := findAssignment(work, notice.JobID)
	if !ok || assignment.CoordinatorEpoch != notice.Epoch {
		return errors.New("checkpoint references no current assignment authority")
	}
	eof, err := model.SourceEOF(assignment.Topology, notice.Source)
	if err != nil || notice.Watermark > eof {
		return errors.New("checkpoint source or watermark is outside installed topology")
	}
	prior := SourceCursor{Source: notice.Source, NextSequence: 1, EOF: eof}
	if index >= 0 {
		prior = work.Sources[index]
	}
	if notice.Watermark == 0 || notice.Watermark == math.MaxUint64 || notice.Watermark > model.LimitsV1().MaxSourceSequences || prior.CheckpointRevision == math.MaxUint64 {
		return errors.New("checkpoint watermark or revision outside v1 bounds")
	}
	// Committed-watermark adoption (Task 24 defect #2 ruling): the notice
	// arrived over the current fence (validated above), so the coordinator's
	// fail-closed committed-watermark validation is the commit proof. When no
	// exact pending CompletionReport correlates, the strictly higher watermark
	// (or first watermark of a reassigned owner) persists under the CURRENT
	// authority proof derived from the durable installed assignment.
	report, _ := matchingCompletionReport(work, assignment, notice, prior, eof)
	updated := prior
	updated.Watermark, updated.RaftIndex = notice.Watermark, notice.RaftIndex
	if report != nil {
		if report.ExpectedCheckpointRevision == math.MaxUint64 {
			return ErrCapacity
		}
		updated.CheckpointRevision = report.ExpectedCheckpointRevision + 1
		updated.CheckpointAuthority = checkpointAuthority(assignment, report)
	} else {
		token, tokenOK := findToken(assignment.Assignment, notice.Source)
		if !tokenOK {
			return errors.New("checkpoint source has no installed token")
		}
		if prior.CheckpointRevision == math.MaxUint64 {
			return ErrCapacity
		}
		updated.CheckpointRevision = prior.CheckpointRevision + 1
		updated.CheckpointAuthority = CheckpointAuthority{JobControlRevision: assignment.JobControlRevision, AssignmentRevision: assignment.Assignment.Revision, AssignmentDigest: assignment.Assignment.Digest, SourceToken: token, CoordinatorEpoch: notice.Epoch}
	}
	if updated.NextSequence <= notice.Watermark {
		updated.NextSequence = notice.Watermark + 1
	}
	if index >= 0 {
		work.Sources[index] = updated
	} else {
		work.Sources = append(work.Sources, updated)
	}
	for i := range work.Deliveries {
		delivery := work.Deliveries[i]
		if delivery.ID.Tuple.SourceTask == notice.Source && delivery.ID.Tuple.SourceSequence <= notice.Watermark {
			if delivery.State != Completed && delivery.State != Compacted {
				return errors.New("checkpoint covers incomplete delivery")
			}
		}
	}
	keptDeliveries := work.Deliveries[:0]
	for _, delivery := range work.Deliveries {
		if delivery.ID.Tuple.SourceTask != notice.Source || delivery.ID.Tuple.SourceSequence > notice.Watermark {
			keptDeliveries = append(keptDeliveries, delivery)
		}
	}
	work.Deliveries = keptDeliveries
	kept := work.Outboxes[:0]
	for _, outbox := range work.Outboxes {
		if outbox.ID.Tuple.SourceTask != notice.Source || outbox.ID.Tuple.SourceSequence > notice.Watermark {
			kept = append(kept, outbox)
		}
	}
	work.Outboxes = kept
	return nil
}

func applyCheckpointObservation(work *RecoveredWork, observation CommittedCheckpoint) error {
	if err := validateCheckpointObservation(observation); err != nil {
		return err
	}
	assignment, ok := findAssignment(work, observation.Notice.JobID)
	if !ok || assignment.CoordinatorEpoch != work.Fence || observation.Notice.Epoch != work.Fence || observation.JobControlRevision != assignment.JobControlRevision || observation.AssignmentRevision != assignment.Assignment.Revision || observation.AssignmentDigest != assignment.Assignment.Digest {
		return errors.New("checkpoint observation authority mismatch")
	}
	stage, ok := findStage(assignment.Topology, observation.Notice.Source.StageID)
	eof, eofErr := model.SourceEOF(assignment.Topology, observation.Notice.Source)
	if !ok || stage.Role != model.StageSource || eofErr != nil || observation.Notice.Watermark > eof {
		return errors.New("checkpoint observation source is not a source stage")
	}
	index := checkpointObservationIndex(work.Checkpoints, observation.Notice.Source)
	if index >= 0 {
		prior := work.Checkpoints[index]
		if prior.Notice.RaftIndex == observation.Notice.RaftIndex {
			if prior == observation {
				return nil
			}
			return model.ErrIdentityReuse
		}
		if observation.Notice.RaftIndex < prior.Notice.RaftIndex || observation.Notice.Watermark < prior.Notice.Watermark {
			return errors.New("stale checkpoint observation")
		}
		work.Checkpoints[index] = observation
		return nil
	}
	maxCheckpoints := model.LimitsV1().MaxRetainedJobs * model.LimitsV1().MaxTasksPerJob
	if uint64(len(work.Checkpoints)) >= maxCheckpoints {
		return ErrCapacity
	}
	work.Checkpoints = append(work.Checkpoints, observation)
	sort.Slice(work.Checkpoints, func(i, j int) bool {
		return taskLess(work.Checkpoints[i].Notice.Source, work.Checkpoints[j].Notice.Source)
	})
	return nil
}

func validateCheckpointObservation(observation CommittedCheckpoint) error {
	if err := observation.Notice.Validate(); err != nil {
		return err
	}
	if observation.JobControlRevision == 0 || observation.AssignmentRevision == 0 || observation.AssignmentDigest == ([32]byte{}) {
		return errors.New("invalid checkpoint observation assignment fence")
	}
	return nil
}

// applyLegacyCheckpoint replays only an already-durable schema-v1 Task14
// checkpoint. New writes use schema v2 and always enter applyCheckpoint.
func applyLegacyCheckpoint(work *RecoveredWork, notice model.CheckpointNotice) error {
	if err := notice.Validate(); err != nil {
		return err
	}
	if notice.Epoch != work.Fence {
		return errors.New("checkpoint coordinator fence mismatch")
	}
	assignment, ok := findAssignment(work, notice.JobID)
	if !ok {
		return errors.New("checkpoint references unknown assignment")
	}
	eof, err := model.SourceEOF(assignment.Topology, notice.Source)
	if err != nil || notice.Watermark > eof {
		return errors.New("checkpoint source or watermark is outside installed topology")
	}
	index := sourceIndex(work.Sources, notice.Source)
	if index >= 0 {
		prior := work.Sources[index]
		if notice.Watermark < prior.Watermark || notice.RaftIndex < prior.RaftIndex {
			return errors.New("checkpoint regression")
		}
		if notice.Watermark == prior.Watermark && notice.RaftIndex == prior.RaftIndex {
			return nil
		}
		prior.Watermark, prior.RaftIndex = notice.Watermark, notice.RaftIndex
		work.Sources[index] = prior
	} else {
		if notice.Watermark == math.MaxUint64 || notice.Watermark > model.LimitsV1().MaxSourceSequences {
			return errors.New("checkpoint watermark outside v1 bounds")
		}
		work.Sources = append(work.Sources, SourceCursor{Source: notice.Source, NextSequence: notice.Watermark + 1, EOF: eof, Watermark: notice.Watermark, RaftIndex: notice.RaftIndex})
	}
	for _, delivery := range work.Deliveries {
		if delivery.ID.Tuple.SourceTask == notice.Source && delivery.ID.Tuple.SourceSequence <= notice.Watermark && delivery.State != Completed && delivery.State != Compacted {
			return errors.New("checkpoint covers incomplete delivery")
		}
	}
	keptDeliveries := work.Deliveries[:0]
	for _, delivery := range work.Deliveries {
		if delivery.ID.Tuple.SourceTask != notice.Source || delivery.ID.Tuple.SourceSequence > notice.Watermark {
			keptDeliveries = append(keptDeliveries, delivery)
		}
	}
	work.Deliveries = keptDeliveries
	keptOutboxes := work.Outboxes[:0]
	for _, outbox := range work.Outboxes {
		if outbox.ID.Tuple.SourceTask != notice.Source || outbox.ID.Tuple.SourceSequence > notice.Watermark {
			keptOutboxes = append(keptOutboxes, outbox)
		}
	}
	work.Outboxes = keptOutboxes
	return nil
}

func exactLegacyCheckpointProofAvailable(work *RecoveredWork, notice model.CheckpointNotice) bool {
	if notice.Validate() != nil {
		return false
	}
	index := sourceIndex(work.Sources, notice.Source)
	if index >= 0 && notice.Watermark == work.Sources[index].Watermark {
		prior := work.Sources[index]
		if prior.CheckpointRevision != 0 {
			return notice.RaftIndex == prior.RaftIndex && notice.Epoch == prior.CheckpointAuthority.CoordinatorEpoch
		}
		assignment, ok := findAssignment(work, notice.JobID)
		if !ok || assignment.CoordinatorEpoch != notice.Epoch {
			return false
		}
		_, err := matchingLegacyCompletionReport(work, assignment, notice, prior)
		return err == nil
	}
	if notice.Epoch != work.Fence {
		return false
	}
	assignment, ok := findAssignment(work, notice.JobID)
	if !ok || assignment.CoordinatorEpoch != notice.Epoch {
		return false
	}
	eof, err := model.SourceEOF(assignment.Topology, notice.Source)
	if err != nil || notice.Watermark > eof {
		return false
	}
	prior := SourceCursor{Source: notice.Source, NextSequence: 1, EOF: eof}
	if index >= 0 {
		prior = work.Sources[index]
	}
	_, err = matchingCompletionReport(work, assignment, notice, prior, eof)
	return err == nil
}

func matchingCompletionReport(work *RecoveredWork, assignment InstalledAssignment, notice model.CheckpointNotice, prior SourceCursor, eof uint64) (*model.CompletionReport, error) {
	source, exists := findToken(assignment.Assignment, notice.Source)
	if !exists {
		return nil, errors.New("checkpoint source has no installed token")
	}
	for index := range work.PendingEvents {
		event := &work.PendingEvents[index]
		report := event.Completion
		if report == nil || report.JobID != notice.JobID || report.Source != notice.Source || report.New != notice.Watermark {
			continue
		}
		legacyRevision := prior.Watermark != 0 && prior.CheckpointRevision == 0 && prior.CheckpointAuthority == (CheckpointAuthority{})
		if report.Validate() != nil || report.JobControlRevision != assignment.JobControlRevision || report.AssignmentRevision != assignment.Assignment.Revision || report.Token != source || report.Epoch != notice.Epoch || !legacyRevision && report.ExpectedCheckpointRevision != prior.CheckpointRevision || report.Prior != prior.Watermark || report.EOF != eof || event.WorkerID != source.WorkerID || event.WorkerEpoch != source.WorkerEpoch || event.TransactionID != report.WorkerTransactionID {
			return nil, errors.New("checkpoint completion event authority mismatch")
		}
		return report, nil
	}
	return nil, errors.New("checkpoint has no exact durable completion event")
}

func matchingLegacyCompletionReport(work *RecoveredWork, assignment InstalledAssignment, notice model.CheckpointNotice, prior SourceCursor) (*model.CompletionReport, error) {
	source, exists := findToken(assignment.Assignment, notice.Source)
	if !exists {
		return nil, errors.New("legacy checkpoint source has no installed token")
	}
	for index := range work.PendingEvents {
		event := &work.PendingEvents[index]
		report := event.Completion
		if report == nil || report.JobID != notice.JobID || report.Source != notice.Source || report.New != notice.Watermark {
			continue
		}
		if report.Validate() != nil || report.JobControlRevision != assignment.JobControlRevision || report.AssignmentRevision != assignment.Assignment.Revision || report.Token != source || report.Epoch != notice.Epoch || report.New != prior.Watermark || report.EOF != prior.EOF || event.WorkerID != source.WorkerID || event.WorkerEpoch != source.WorkerEpoch || event.TransactionID != report.WorkerTransactionID {
			return nil, errors.New("legacy checkpoint completion authority mismatch")
		}
		return report, nil
	}
	return nil, errors.New("legacy checkpoint has no exact durable completion event")
}

func checkpointAuthority(assignment InstalledAssignment, report *model.CompletionReport) CheckpointAuthority {
	return CheckpointAuthority{JobControlRevision: report.JobControlRevision, AssignmentRevision: report.AssignmentRevision, AssignmentDigest: assignment.Assignment.Digest, SourceToken: report.Token, CoordinatorEpoch: report.Epoch}
}

func applyResult(work *RecoveredWork, result StoredResult) error {
	assignment, ok := findAssignment(work, result.Record.TupleID.JobID)
	if !ok {
		return errors.New("result references unknown assignment")
	}
	if err := result.Provenance.Validate(result.Record); err != nil {
		return err
	}
	if result.Provenance.CoordinatorEpoch != work.Fence || result.Provenance.AssignmentRevision != assignment.Assignment.Revision || result.Provenance.AssignmentDigest != assignment.Assignment.Digest {
		return errors.New("result provenance assignment fence mismatch")
	}
	want, ok := findReplica(assignment.Assignment, result.Record.SinkTask)
	if !ok || want != result.Provenance.ReplicaSet {
		return errors.New("result replica set mismatch")
	}
	if result.canonical == nil {
		encoded, err := model.MarshalResultRecord(result.Record)
		if err != nil {
			return err
		}
		result.canonical = encoded
	}
	if err := ensureWorkIndexes(work); err != nil {
		return err
	}
	key := resultKey{SinkTask: result.Record.SinkTask, TupleID: result.Record.TupleID}
	priorResult := findResultNode(work.indexes.results, key)
	if priorResult != nil {
		if equalStoredResult(priorResult.value, result) {
			return nil
		}
		if !rebindableResultProvenance(priorResult.value, result) {
			return model.ErrIdentityReuse
		}
		// Copy-provenance rebind (Task 24 defect #4 ruling): the identical
		// logical record retained under a superseded envelope re-binds to the
		// current pair (validated above against the current fence, assignment
		// and replica set). The logical record and its byte accounting are
		// unchanged; the prospective tree is path-copied like every insert.
		result.Record.Value = append([]byte(nil), result.Record.Value...)
		work.indexes.results = replaceResultNode(work.indexes.results, key, result)
		return nil
	}
	entryBytes, err := resultArtifactEntryBytes(uint64(len(result.canonical)))
	if err != nil {
		return err
	}
	jobBytes := work.indexes.resultBytesByJob[result.Record.TupleID.JobID]
	if jobBytes > model.LimitsV1().MaxResultRecordsBytesPerJob || entryBytes > model.LimitsV1().MaxResultRecordsBytesPerJob-jobBytes {
		return ErrCapacity
	}
	if work.indexes.resultCount >= maxStoredResultCount() {
		return ErrCapacity
	}
	result.Record.Value = append([]byte(nil), result.Record.Value...)
	inserted, err := insertResultNode(work.indexes.results, &resultNode{key: key, value: result, height: 1})
	if err != nil {
		return err
	}
	work.indexes.results = inserted
	work.indexes.resultBytesByJob[result.Record.TupleID.JobID] = jobBytes + entryBytes
	work.indexes.resultCount++
	return nil
}

func applyEvent(work *RecoveredWork, event model.WorkerEvent) error {
	if err := event.Validate(); err != nil {
		return err
	}
	var job model.JobID
	var revision, jobRevision uint64
	var token model.AssignmentToken
	var epoch model.CoordinatorEpoch
	if event.Completion != nil {
		job = event.Completion.JobID
		revision = event.Completion.AssignmentRevision
		jobRevision = event.Completion.JobControlRevision
		token = event.Completion.Token
		epoch = event.Completion.Epoch
	} else {
		job = event.Failure.JobID
		revision = event.Failure.AssignmentRevision
		jobRevision = event.Failure.JobControlRevision
		token = event.Failure.Task
		epoch = event.Failure.Epoch
	}
	assignment, ok := findAssignment(work, job)
	if !ok || epoch != work.Fence || revision != assignment.Assignment.Revision || jobRevision != assignment.JobControlRevision || !containsToken(assignment.Assignment, token) {
		return errors.New("event assignment or coordinator cross-reference mismatch")
	}
	if event.Completion != nil {
		stageFound := false
		for _, stage := range assignment.Topology.Spec().Stages {
			if stage.StageID == event.Completion.Source.StageID {
				stageFound = stage.Role == model.StageSource
				break
			}
		}
		if !stageFound {
			return errors.New("completion event source is not a source task")
		}
	}
	if event.TransactionID != work.NextTransactionID {
		return errors.New("worker event transaction is not next")
	}
	if work.NextTransactionID == math.MaxUint64 {
		return ErrCapacity
	}
	if len(work.PendingEvents) > 0 && event.TransactionID <= work.PendingEvents[len(work.PendingEvents)-1].TransactionID {
		return errors.New("worker event order regression")
	}
	if len(work.PendingEvents) >= MaxTransactionRecords {
		return ErrCapacity
	}
	work.PendingEvents = append(work.PendingEvents, cloneEvent(event))
	work.NextTransactionID++
	return nil
}
func applyEventAck(work *RecoveredWork, through uint64) error {
	if through >= work.NextTransactionID {
		return errors.New("event ack exceeds durable sequence")
	}
	index := 0
	for index < len(work.PendingEvents) && work.PendingEvents[index].TransactionID <= through {
		event := work.PendingEvents[index]
		if event.Completion != nil {
			report := event.Completion
			cursorIndex := sourceIndex(work.Sources, report.Source)
			applied := false
			if cursorIndex >= 0 {
				cursor := work.Sources[cursorIndex]
				proof := cursor.CheckpointAuthority
				applied = report.ExpectedCheckpointRevision != math.MaxUint64 && cursor.Watermark == report.New && cursor.CheckpointRevision == report.ExpectedCheckpointRevision+1 && proof.JobControlRevision == report.JobControlRevision && proof.AssignmentRevision == report.AssignmentRevision && proof.SourceToken == report.Token && proof.CoordinatorEpoch == report.Epoch
			}
			if !applied && !completionReportSuperseded(work, report) {
				return errors.New("completion event acknowledged before exact checkpoint authority")
			}
		} else {
			assignment, ok := findAssignment(work, event.Failure.JobID)
			if !ok || assignment.SchedulingState != model.Closed {
				return errors.New("failure event acknowledged before durable job closure")
			}
		}
		index++
	}
	work.PendingEvents = append([]model.WorkerEvent(nil), work.PendingEvents[index:]...)
	return nil
}

// applyLegacyEventAck replays only an already-durable schema-v1 Task14 event
// acknowledgment. New schema-v2 writes require checkpoint/terminal proof.
func applyLegacyEventAck(work *RecoveredWork, through uint64) error {
	if through >= work.NextTransactionID {
		return errors.New("event ack exceeds durable sequence")
	}
	index := 0
	for index < len(work.PendingEvents) && work.PendingEvents[index].TransactionID <= through {
		index++
	}
	work.PendingEvents = append([]model.WorkerEvent(nil), work.PendingEvents[index:]...)
	return nil
}

func completionReportSuperseded(work *RecoveredWork, report *model.CompletionReport) bool {
	assignment, ok := findAssignment(work, report.JobID)
	if !ok || assignment.JobControlRevision < report.JobControlRevision || assignment.Assignment.Revision < report.AssignmentRevision || assignment.JobControlRevision == report.JobControlRevision && assignment.Assignment.Revision == report.AssignmentRevision {
		return false
	}
	current, ok := findToken(assignment.Assignment, report.Source)
	if !ok {
		return false
	}
	if assignment.Assignment.Revision == report.AssignmentRevision {
		return current == report.Token
	}
	return current.AssignmentRevision == assignment.Assignment.Revision
}

func applyRepair(work *RecoveredWork, repair ResultRepairRecord) error {
	index := repairIndex(work.Repairs, repair.Instruction.RepairID)
	if index >= 0 && !equalRepairInstruction(work.Repairs[index], repair) {
		return model.ErrIdentityReuse
	}
	if err := validateRepair(repair); err != nil {
		return err
	}
	if repair.Instruction.CoordinatorEpoch != work.Fence {
		return errors.New("repair coordinator fence mismatch")
	}
	assignment, ok := findAssignment(work, repair.Instruction.JobID)
	if !ok || repair.Instruction.SpecificationHash != assignment.Topology.Digest() {
		return errors.New("repair references stale or unknown assignment")
	}
	current := repair.Instruction.AssignmentRevision == assignment.Assignment.Revision && repair.Instruction.AssignmentDigest == assignment.Assignment.Digest
	if !current && !supersededRepairFailure(index, repair, assignment) {
		return errors.New("repair references stale or unknown assignment")
	}
	d := repair.Instruction
	if current {
		replica, ok := findReplica(assignment.Assignment, d.SinkTask)
		if !ok {
			return errors.New("repair sink has no installed replica set")
		}
		if !repairDestinationMatchesReplica(d, replica) {
			return errors.New("repair destination is not a current assigned replica")
		}
	}
	for index, checkpoint := range d.Checkpoints {
		if checkpoint.Source.JobID != d.JobID {
			return errors.New("repair checkpoint references another job")
		}
		if index > 0 && !taskLess(d.Checkpoints[index-1].Source, checkpoint.Source) {
			return errors.New("repair checkpoints are not canonical and unique")
		}
		eof, err := model.SourceEOF(assignment.Topology, checkpoint.Source)
		if err != nil || checkpoint.Watermark > eof {
			return errors.New("repair checkpoint is outside installed topology")
		}
	}
	if index < 0 {
		if len(work.Repairs) >= 64 {
			return ErrCapacity
		}
		work.Repairs = append(work.Repairs, cloneRepair(repair))
		return nil
	}
	prior := work.Repairs[index]
	if prior.State == RepairComplete || prior.State == RepairFailed {
		if !equalRepairInstruction(prior, repair) || repair.State != prior.State || repair.NextRecord != prior.NextRecord || repair.NextOffset != prior.NextOffset || repair.RecordCount != prior.RecordCount || repair.TotalBytes != prior.TotalBytes || repair.ContentDigest != prior.ContentDigest || repair.ErrorCode != prior.ErrorCode {
			return errors.New("terminal repair cannot change")
		}
		return nil
	}
	if repair.State < prior.State || repair.NextRecord < prior.NextRecord || repair.NextOffset < prior.NextOffset {
		return errors.New("repair progress regression")
	}
	work.Repairs[index] = cloneRepair(repair)
	return nil
}

// supersededRepairFailure admits the one mutation a retained grant bound to a
// revision the installed assignment has advanced past may still receive: the
// worker durably marking it RepairFailed. Such a grant can neither progress
// nor be re-issued (the coordinator replaces it under the current revision),
// but leaving it non-terminal would block the replacement grant's identity.
func supersededRepairFailure(index int, repair ResultRepairRecord, assignment InstalledAssignment) bool {
	return index >= 0 && repair.State == RepairFailed && repair.Instruction.AssignmentRevision < assignment.Assignment.Revision
}

func applySource(work *RecoveredWork, cursor SourceCursor, outboxes []OutboxRecord) error {
	if uint64(len(outboxes)) > model.LimitsV1().MaxDerivedDeliveries {
		return errors.New("source outbox count exceeds v1 bounds")
	}
	if err := cursor.Source.Validate(); err != nil {
		return err
	}
	assignment, ok := findAssignment(work, cursor.Source.JobID)
	if !ok {
		return errors.New("source references unknown assignment")
	}
	wantEOF, err := model.SourceEOF(assignment.Topology, cursor.Source)
	if err != nil || cursor.EOF != wantEOF {
		return errors.New("source cursor EOF does not match installed topology")
	}
	if cursor.NextSequence == 0 || cursor.NextSequence > model.LimitsV1().MaxSourceSequences+1 || cursor.Watermark > model.LimitsV1().MaxSourceSequences || cursor.EOF > model.LimitsV1().MaxSourceSequences || cursor.EOF != 0 && cursor.NextSequence > cursor.EOF+1 || cursor.Watermark >= cursor.NextSequence || cursor.Watermark != 0 && cursor.RaftIndex == 0 || cursor.Watermark == 0 && cursor.CheckpointRevision != 0 || !validCheckpointAuthority(cursor) {
		return errors.New("source cursor outside bounds")
	}
	source, ok := findToken(assignment.Assignment, cursor.Source)
	if !ok {
		return errors.New("source cursor has no installed assignment token")
	}
	expected, err := expectedSourceOutboxes(cursor, source, assignment)
	if err != nil {
		return err
	}
	index := sourceIndex(work.Sources, cursor.Source)
	exactRetry := index >= 0 && cursor == work.Sources[index]
	if len(outboxes) != len(expected) {
		if exactRetry {
			return model.ErrIdentityReuse
		}
		return errors.New("source outboxes are not the complete topology-derived set")
	}
	if !outboxesCanonical(outboxes) {
		if exactRetry {
			return model.ErrIdentityReuse
		}
		return errors.New("source outboxes are not in canonical identity order")
	}
	seen := make(map[model.DeliveryID]struct{}, len(outboxes))
	for _, outbox := range outboxes {
		if _, duplicate := seen[outbox.ID]; duplicate {
			return errors.New("duplicate source outbox")
		}
		seen[outbox.ID] = struct{}{}
		want, exists := expected[outbox.ID]
		if !exists || outbox.Completed || outbox.Accepted || outbox.RetryDeadlineUnixNano != 0 || !equalOutboxDefinition(want, outbox) {
			if exactRetry {
				return model.ErrIdentityReuse
			}
			return errors.New("source outbox does not match immutable source route")
		}
		if err := validateOutbox(outbox, assignment, work.Fence); err != nil {
			return err
		}
	}
	if index >= 0 {
		prior := work.Sources[index]
		if cursor == prior {
			for _, outbox := range outboxes {
				stored := outboxIndex(work.Outboxes, outbox.ID)
				if stored < 0 || !equalOutboxDefinition(work.Outboxes[stored], outbox) {
					return model.ErrIdentityReuse
				}
			}
			return nil
		}
		if cursor.NextSequence <= prior.NextSequence || cursor.NextSequence != prior.NextSequence+1 || cursor.Watermark != prior.Watermark || cursor.RaftIndex != prior.RaftIndex || cursor.CheckpointRevision != prior.CheckpointRevision || cursor.CheckpointAuthority != prior.CheckpointAuthority {
			return errors.New("source cursor regression or sequence gap")
		}
	} else if cursor.NextSequence > 2 || cursor.Watermark != 0 || cursor.RaftIndex != 0 || cursor.CheckpointRevision != 0 || cursor.CheckpointAuthority != (CheckpointAuthority{}) {
		return errors.New("initial source cursor skips durable sequence or checkpoint state")
	}
	for _, outbox := range outboxes {
		if outboxIndex(work.Outboxes, outbox.ID) >= 0 {
			return model.ErrIdentityReuse
		}
	}
	if index < 0 {
		work.Sources = append(work.Sources, cursor)
	} else {
		work.Sources[index] = cursor
	}
	for _, outbox := range outboxes {
		work.Outboxes = append(work.Outboxes, outbox.Clone())
	}
	return nil
}

func validCheckpointAuthority(cursor SourceCursor) bool {
	proof := cursor.CheckpointAuthority
	if cursor.CheckpointRevision == 0 {
		return proof == (CheckpointAuthority{})
	}
	return proof.JobControlRevision != 0 && proof.AssignmentRevision != 0 && proof.AssignmentDigest != ([32]byte{}) && proof.SourceToken.Validate() == nil && proof.SourceToken.Task == cursor.Source && proof.SourceToken.AssignmentRevision == proof.AssignmentRevision && proof.CoordinatorEpoch.Validate() == nil
}
func applyOutboxAck(work *RecoveredWork, id model.DeliveryID) error {
	index := outboxIndex(work.Outboxes, id)
	if index < 0 {
		return errors.New("unknown outbox")
	}
	work.Outboxes[index].Completed = true
	return nil
}

type outboxRetryUpdate struct {
	ID               model.DeliveryID
	Accepted         bool
	AcceptTransition bool
	DeadlineUnixNano int64
}

func applyOutboxRetry(work *RecoveredWork, update outboxRetryUpdate) error {
	index := outboxIndex(work.Outboxes, update.ID)
	if index < 0 {
		return errors.New("retry references unknown outbox")
	}
	if update.DeadlineUnixNano == 0 {
		return errors.New("retry deadline is unset")
	}
	record := &work.Outboxes[index]
	if record.Completed {
		return errors.New("retry references completed outbox")
	}
	if update.AcceptTransition {
		if !update.Accepted {
			return errors.New("accepted transition regresses retry phase")
		}
		if record.Accepted {
			if record.RetryDeadlineUnixNano != update.DeadlineUnixNano {
				return model.ErrIdentityReuse
			}
			return nil
		}
		record.Accepted = true
		record.RetryDeadlineUnixNano = update.DeadlineUnixNano
		return nil
	}
	if update.Accepted != record.Accepted {
		return errors.New("dispatch retry phase mismatch")
	}
	if record.RetryDeadlineUnixNano == update.DeadlineUnixNano {
		return nil
	}
	record.RetryDeadlineUnixNano = update.DeadlineUnixNano
	return nil
}

// readoptedDeliveryAuthority reports whether one retained record published
// under a superseded fence may re-enter under the current fence: its
// assignment identity must still match the current installed assignment
// byte-exactly and its retained epoch must be strictly ordered before the
// current committed fence. Genuinely replaced assignments never re-adopt.
func readoptedDeliveryAuthority(record DeliveryRecord, assignment InstalledAssignment, fence model.CoordinatorEpoch) bool {
	if assignment.CoordinatorEpoch != fence || compareEpochOrder(record.CoordinatorEpoch, fence) > 0 {
		return false
	}
	if record.AssignmentRevision == assignment.Assignment.Revision {
		return record.AssignmentDigest == assignment.Assignment.Digest && compareEpochOrder(record.CoordinatorEpoch, fence) < 0
	}
	// A superseded assignment revision re-adopts only when the custody's own
	// destination task incarnation is unchanged in the current set (Task 24
	// defect #4 ruling: retained custody re-envelopes under the current
	// assignment; a replaced task never re-enters).
	return record.AssignmentRevision < assignment.Assignment.Revision && sameTaskIncarnation(assignment.Assignment, record.Destination)
}

// sameTaskIncarnation reports whether token's task is placed on the identical
// worker incarnation (worker, epoch, attempt, specification) in set; only the
// token's AssignmentRevision may differ.
func sameTaskIncarnation(set model.AssignmentSet, token model.AssignmentToken) bool {
	current, ok := findToken(set, token.Task)
	return ok && current.WorkerID == token.WorkerID && current.WorkerEpoch == token.WorkerEpoch && current.Attempt == token.Attempt && current.SpecificationHash == token.SpecificationHash
}

// equalDeliveryDefinitionModuloEnvelope compares the logical custody of two
// delivery definitions ignoring the assignment envelope (producer incarnation,
// revision, digest, coordinator epoch): identity, payload bytes, reservation,
// producer task and destination task incarnation must agree.
func equalDeliveryDefinitionModuloEnvelope(a, b DeliveryRecord) bool {
	if a.ID != b.ID || a.Producer.Task != b.Producer.Task || a.Reservation != b.Reservation ||
		a.Destination.Task != b.Destination.Task || a.Destination.WorkerID != b.Destination.WorkerID || a.Destination.WorkerEpoch != b.Destination.WorkerEpoch || a.Destination.Attempt != b.Destination.Attempt || a.Destination.SpecificationHash != b.Destination.SpecificationHash {
		return false
	}
	aa, err := model.MarshalTuple(a.Tuple)
	if err != nil {
		return false
	}
	bb, err := model.MarshalTuple(b.Tuple)
	return err == nil && bytes.Equal(aa, bb)
}

// equalDeliveryDefinitionModuloEpoch compares one delivery definition against
// another while ignoring only the coordinator-epoch branding.
func equalDeliveryDefinitionModuloEpoch(a, b DeliveryRecord) bool {
	if a.ID != b.ID || a.Producer != b.Producer || a.Destination != b.Destination || a.AssignmentRevision != b.AssignmentRevision || a.AssignmentDigest != b.AssignmentDigest || a.Reservation != b.Reservation {
		return false
	}
	aa, err := model.MarshalTuple(a.Tuple)
	if err != nil {
		return false
	}
	bb, err := model.MarshalTuple(b.Tuple)
	return err == nil && bytes.Equal(aa, bb)
}

func validateDelivery(record DeliveryRecord, assignment InstalledAssignment, fence model.CoordinatorEpoch) error {
	if record.State < Received || record.State > Compacted || record.AssignmentRevision == 0 || record.AssignmentDigest == ([32]byte{}) {
		return errors.New("invalid delivery metadata")
	}
	readopted := record.CoordinatorEpoch != fence || record.AssignmentRevision != assignment.Assignment.Revision || record.AssignmentDigest != assignment.Assignment.Digest
	if readopted && !readoptedDeliveryAuthority(record, assignment, fence) {
		return errors.New("delivery assignment fence mismatch")
	}
	message := protocol.TupleDelivery{DeliveryID: record.ID, Tuple: record.Tuple, Producer: record.Producer, Destination: record.Destination, Assignment: protocol.AssignmentSetIdentity{JobID: record.ID.Tuple.JobID, Revision: record.AssignmentRevision, Digest: record.AssignmentDigest}, Coordinator: record.CoordinatorEpoch}
	if _, err := protocol.MarshalTupleDelivery(message); err != nil {
		return err
	}
	supersededRevision := readopted && record.AssignmentRevision != assignment.Assignment.Revision
	if supersededRevision {
		if _, ok := findToken(assignment.Assignment, record.Producer.Task); !ok || !sameTaskIncarnation(assignment.Assignment, record.Destination) {
			return errors.New("delivery token not in installed assignment")
		}
	} else if !containsToken(assignment.Assignment, record.Producer) || !containsToken(assignment.Assignment, record.Destination) {
		return errors.New("delivery token not in installed assignment")
	}
	if err := validateRoute(assignment.Topology, record.ID, record.Tuple, record.Producer.Task); err != nil {
		return err
	}
	if record.Destination.WorkerID == 0 {
		return errors.New("zero destination")
	}
	want, err := assignment.Topology.WorstCaseCustodyBytes(record.Destination.Task)
	if err != nil {
		return err
	}
	if record.Reservation != want {
		return errors.New("delivery reservation does not match topology worst case")
	}
	derived, exists, err := deriveDeliveryDefinition(assignment, fence, record.ID)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("delivery definition does not match deterministic source path")
	}
	if supersededRevision {
		// The reconstruction derives under the current assignment; a
		// re-adopted retained record differs from it only by its envelope.
		if !equalDeliveryDefinitionModuloEnvelope(derived, record) {
			return errors.New("delivery definition does not match deterministic source path")
		}
	} else if readopted {
		// The reconstruction derives under the current fence; a re-adopted
		// retained record differs from it only by the epoch branding.
		if !equalDeliveryDefinitionModuloEpoch(derived, record) {
			return errors.New("delivery definition does not match deterministic source path")
		}
	} else if !equalDeliveryDefinition(derived, record) {
		return errors.New("delivery definition does not match deterministic source path")
	}
	return nil
}

func validateOutbox(record OutboxRecord, assignment InstalledAssignment, fence model.CoordinatorEpoch) error {
	delivery := DeliveryRecord{ID: record.ID, Tuple: record.Tuple, Producer: record.Producer, Destination: record.Destination, AssignmentRevision: record.AssignmentRevision, AssignmentDigest: record.AssignmentDigest, CoordinatorEpoch: record.CoordinatorEpoch, State: Received}
	if record.CoordinatorEpoch != fence {
		return errors.New("outbox fence mismatch")
	}
	if record.Accepted && record.RetryDeadlineUnixNano == 0 {
		return errors.New("accepted outbox has no retry deadline")
	}
	message := protocol.TupleDelivery{DeliveryID: delivery.ID, Tuple: delivery.Tuple, Producer: delivery.Producer, Destination: delivery.Destination, Assignment: protocol.AssignmentSetIdentity{JobID: delivery.ID.Tuple.JobID, Revision: delivery.AssignmentRevision, Digest: delivery.AssignmentDigest}, Coordinator: delivery.CoordinatorEpoch}
	if _, err := protocol.MarshalTupleDelivery(message); err != nil {
		return err
	}
	if delivery.AssignmentRevision != assignment.Assignment.Revision || delivery.AssignmentDigest != assignment.Assignment.Digest || !containsToken(assignment.Assignment, delivery.Producer) || !containsToken(assignment.Assignment, delivery.Destination) {
		return errors.New("outbox assignment mismatch")
	}
	if err := validateRoute(assignment.Topology, record.ID, record.Tuple, record.Producer.Task); err != nil {
		return err
	}
	return nil
}

func validateRoute(topology model.ValidatedTopology, id model.DeliveryID, tuple model.Tuple, producer model.TaskID) error {
	var edge model.EdgeSpec
	found := false
	for _, candidate := range topology.Spec().Edges {
		if candidate.EdgeID == id.EdgeID {
			edge = candidate
			found = true
			break
		}
	}
	if !found || edge.SourceStageID != producer.StageID || edge.DestinationStageID != id.DestinationTask.StageID {
		return errors.New("delivery route does not match topology edge")
	}
	partitions, err := model.Route(topology, edge, id.Tuple, tuple)
	if err != nil {
		return err
	}
	for _, partition := range partitions {
		if partition == id.DestinationTask.Partition {
			return nil
		}
	}
	return errors.New("delivery destination partition does not match deterministic route")
}

func deliveryDefinitionDigest(record DeliveryRecord) ([32]byte, error) {
	message := protocol.TupleDelivery{DeliveryID: record.ID, Tuple: record.Tuple, Producer: record.Producer, Destination: record.Destination, Assignment: protocol.AssignmentSetIdentity{JobID: record.ID.Tuple.JobID, Revision: record.AssignmentRevision, Digest: record.AssignmentDigest}, Coordinator: record.CoordinatorEpoch}
	encoded, err := protocol.MarshalTupleDelivery(message)
	if err != nil {
		return [32]byte{}, err
	}
	var reservation [8]byte
	binary.BigEndian.PutUint64(reservation[:], record.Reservation)
	return sha256.Sum256(append(encoded, reservation[:]...)), nil
}

func expectedProcessedOutboxes(delivery DeliveryRecord, assignment InstalledAssignment) (map[model.DeliveryID]OutboxRecord, error) {
	result := make(map[model.DeliveryID]OutboxRecord)
	producer := currentProducerToken(delivery, assignment)
	for outputIndex, tuple := range delivery.Outputs {
		for _, edge := range assignment.Topology.Spec().Edges {
			if edge.SourceStageID != delivery.Destination.Task.StageID {
				continue
			}
			if outputIndex > math.MaxUint16 {
				return nil, errors.New("output ordinal exceeds v1 identity")
			}
			child := model.DeriveChildTupleID(delivery.ID.Tuple, delivery.Destination.Task, edge.EdgeID, uint16(outputIndex))
			partitions, err := model.Route(assignment.Topology, edge, child, tuple)
			if err != nil {
				return nil, err
			}
			for _, partition := range partitions {
				task := model.TaskID{JobID: delivery.ID.Tuple.JobID, StageID: edge.DestinationStageID, Partition: partition}
				destination, ok := findToken(assignment.Assignment, task)
				if !ok {
					return nil, errors.New("derived outbox destination has no assignment token")
				}
				id := model.DeliveryID{Tuple: child, EdgeID: edge.EdgeID, DestinationTask: task}
				if _, duplicate := result[id]; duplicate {
					return nil, errors.New("topology derived duplicate outbox identity")
				}
				// Emissions are always branded with the CURRENT installed
				// assignment identity and fence: a re-adopted retained
				// delivery derives its outboxes under the fence it re-entered
				// through, never the superseded one it was published under.
				result[id] = OutboxRecord{ID: id, Tuple: cloneTuple(tuple), Producer: producer, Destination: destination, AssignmentRevision: assignment.Assignment.Revision, AssignmentDigest: assignment.Assignment.Digest, CoordinatorEpoch: assignment.CoordinatorEpoch}
			}
		}
	}
	if uint64(len(result)) > model.LimitsV1().MaxDerivedDeliveries {
		return nil, errors.New("topology-derived outboxes exceed v1 bound")
	}
	return result, nil
}

// currentProducerToken returns the token a retained delivery's destination
// task carries in the current assignment — the same incarnation under the
// current revision — falling back to the retained token when absent.
func currentProducerToken(delivery DeliveryRecord, assignment InstalledAssignment) model.AssignmentToken {
	if current, ok := findToken(assignment.Assignment, delivery.Destination.Task); ok && sameTaskIncarnation(assignment.Assignment, delivery.Destination) {
		return current
	}
	return delivery.Destination
}

func expectedSourceOutboxes(cursor SourceCursor, source model.AssignmentToken, assignment InstalledAssignment) (map[model.DeliveryID]OutboxRecord, error) {
	result := make(map[model.DeliveryID]OutboxRecord)
	if cursor.NextSequence <= 1 {
		return result, nil
	}
	sequence := cursor.NextSequence - 1
	tuple, exists, err := model.SourceTuple(assignment.Topology, cursor.Source, sequence)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.New("source cursor advances beyond immutable EOF")
	}
	tupleID := model.DeriveSourceTupleID(cursor.Source.JobID, cursor.Source, sequence)
	for _, edge := range assignment.Topology.Spec().Edges {
		if edge.SourceStageID != cursor.Source.StageID {
			continue
		}
		partitions, err := model.Route(assignment.Topology, edge, tupleID, tuple)
		if err != nil {
			return nil, err
		}
		for _, partition := range partitions {
			task := model.TaskID{JobID: cursor.Source.JobID, StageID: edge.DestinationStageID, Partition: partition}
			destination, ok := findToken(assignment.Assignment, task)
			if !ok {
				return nil, errors.New("source outbox destination has no assignment token")
			}
			id := model.DeliveryID{Tuple: tupleID, EdgeID: edge.EdgeID, DestinationTask: task}
			if _, duplicate := result[id]; duplicate {
				return nil, errors.New("source route derived duplicate outbox")
			}
			result[id] = OutboxRecord{ID: id, Tuple: cloneTuple(tuple), Producer: source, Destination: destination, AssignmentRevision: assignment.Assignment.Revision, AssignmentDigest: assignment.Assignment.Digest, CoordinatorEpoch: assignment.CoordinatorEpoch}
		}
	}
	return result, nil
}

func deriveDeliveryDefinition(assignment InstalledAssignment, fence model.CoordinatorEpoch, target model.DeliveryID) (DeliveryRecord, bool, error) {
	if err := target.Validate(); err != nil {
		return DeliveryRecord{}, false, nil
	}
	if assignment.CoordinatorEpoch != fence {
		return DeliveryRecord{}, false, nil
	}
	source, ok := findToken(assignment.Assignment, target.Tuple.SourceTask)
	if !ok {
		return DeliveryRecord{}, false, nil
	}
	eof, err := model.SourceEOF(assignment.Topology, target.Tuple.SourceTask)
	if err != nil || target.Tuple.SourceSequence == 0 || target.Tuple.SourceSequence > eof {
		return DeliveryRecord{}, false, nil
	}
	initial, err := expectedSourceOutboxes(SourceCursor{Source: target.Tuple.SourceTask, NextSequence: target.Tuple.SourceSequence + 1, EOF: eof}, source, assignment)
	if err != nil {
		return DeliveryRecord{}, false, err
	}
	queue := make([]DeliveryRecord, 0, len(initial))
	for _, outbox := range initial {
		delivery, err := deliveryFromOutbox(outbox, assignment.Topology)
		if err != nil {
			return DeliveryRecord{}, false, err
		}
		queue = append(queue, delivery)
	}
	seen := make(map[model.DeliveryID]struct{})
	for len(queue) != 0 {
		delivery := queue[0]
		queue = queue[1:]
		if _, duplicate := seen[delivery.ID]; duplicate {
			continue
		}
		seen[delivery.ID] = struct{}{}
		if delivery.ID == target {
			return delivery, true, nil
		}
		if uint64(len(seen)) >= model.LimitsV1().MaxDerivedDeliveries {
			break
		}
		stage, ok := findStage(assignment.Topology, delivery.Destination.Task.StageID)
		if !ok {
			return DeliveryRecord{}, false, errors.New("derived delivery stage is absent")
		}
		outputs, err := model.ExecuteOperator(stage.Operator, delivery.Tuple)
		if err != nil {
			return DeliveryRecord{}, false, err
		}
		delivery.State, delivery.Outputs = Processed, outputs
		next, err := expectedProcessedOutboxes(delivery, assignment)
		if err != nil {
			return DeliveryRecord{}, false, err
		}
		for _, outbox := range next {
			child, err := deliveryFromOutbox(outbox, assignment.Topology)
			if err != nil {
				return DeliveryRecord{}, false, err
			}
			queue = append(queue, child)
		}
	}
	return DeliveryRecord{}, false, nil
}

func deliveryFromOutbox(outbox OutboxRecord, topology model.ValidatedTopology) (DeliveryRecord, error) {
	reservation, err := topology.WorstCaseCustodyBytes(outbox.Destination.Task)
	if err != nil {
		return DeliveryRecord{}, err
	}
	record := DeliveryRecord{ID: outbox.ID, Tuple: cloneTuple(outbox.Tuple), Producer: outbox.Producer, Destination: outbox.Destination, AssignmentRevision: outbox.AssignmentRevision, AssignmentDigest: outbox.AssignmentDigest, CoordinatorEpoch: outbox.CoordinatorEpoch, State: Received, Reservation: reservation}
	record.definitionDigest, err = deliveryDefinitionDigest(record)
	return record, err
}

func findStage(topology model.ValidatedTopology, id uint16) (model.StageSpec, bool) {
	for _, stage := range topology.Spec().Stages {
		if stage.StageID == id {
			return stage, true
		}
	}
	return model.StageSpec{}, false
}

func validateRepair(repair ResultRepairRecord) error {
	d := repair.Instruction
	if len(d.Checkpoints) > int(model.WorkerControlMaxCheckpointsV1) {
		return errors.New("repair checkpoints exceed bound")
	}
	if d.RepairID == ([16]byte{}) || d.RepairID != model.DeriveRepairID(d) || repair.InstructionDigest == ([32]byte{}) || repair.InstructionDigest != model.RepairInstructionDigest(d) {
		return errors.New("repair identity or digest mismatch")
	}
	if err := d.CoordinatorEpoch.Validate(); err != nil {
		return err
	}
	if err := d.JobID.Validate(); err != nil {
		return err
	}
	if err := d.SinkTask.Validate(); err != nil || d.SinkTask.JobID != d.JobID {
		return errors.New("repair sink mismatch")
	}
	if d.AssignmentRevision == 0 || d.AssignmentDigest == ([32]byte{}) || d.SourceNodeID == 0 || d.DestinationNodeID == 0 || d.SourceNodeID == d.DestinationNodeID || d.SourceWorkerEpoch.Validate() != nil || d.DestinationWorkerEpoch.Validate() != nil || d.SpecificationHash == ([32]byte{}) {
		return errors.New("invalid repair instruction")
	}
	if d.CheckpointDigest != model.CheckpointVectorDigest(d.Checkpoints) {
		return errors.New("repair checkpoint digest mismatch")
	}
	wantQuery := model.ResultInventoryQueryDigest(model.ResultInventoryQueryDefinition{JobID: d.JobID, SinkTask: d.SinkTask, SpecificationHash: d.SpecificationHash, AssignmentRevision: d.AssignmentRevision, AssignmentDigest: d.AssignmentDigest, Checkpoints: d.Checkpoints, CheckpointDigest: d.CheckpointDigest})
	if d.InventoryQueryDigest != wantQuery {
		return errors.New("repair inventory query digest mismatch")
	}
	if (d.ExpectedRecordCount == 0) != (d.ExpectedTotalBytes == 0) {
		return errors.New("repair expected count/bytes mismatch")
	}
	if d.ExpectedRecordCount == 0 && d.ExpectedContentDigest != model.EmptyResultInventoryDigest(d.InventoryQueryDigest) {
		return errors.New("repair empty content digest mismatch")
	}
	if d.ExpectedRecordCount > model.ResultArtifactMaxRecordCountV1 || d.ExpectedTotalBytes > model.LimitsV1().MaxResultRecordsBytesPerJob {
		return errors.New("repair expected inventory exceeds v1 bounds")
	}
	if d.ExpectedRecordCount != 0 {
		if d.ExpectedContentDigest == ([32]byte{}) {
			return errors.New("repair nonempty inventory has zero digest")
		}
		if d.ExpectedRecordCount > math.MaxUint64/model.ResultArtifactMinRecordBytesV1 || d.ExpectedTotalBytes < d.ExpectedRecordCount*model.ResultArtifactMinRecordBytesV1 {
			return errors.New("repair inventory is smaller than its declared record count")
		}
		if d.ExpectedRecordCount <= math.MaxUint64/model.ResultArtifactMaxRecordBytesV1 && d.ExpectedTotalBytes > d.ExpectedRecordCount*model.ResultArtifactMaxRecordBytesV1 {
			return errors.New("repair inventory is larger than its declared record count")
		}
	}
	if repair.Role < RepairSource || repair.Role > RepairDestination || repair.State < RepairPending || repair.State > RepairFailed {
		return errors.New("invalid repair state")
	}
	if repair.NextRecord > d.ExpectedRecordCount || repair.NextOffset > d.ExpectedTotalBytes {
		return errors.New("repair progress exceeds instruction")
	}
	if repair.State == RepairComplete && (repair.RecordCount != d.ExpectedRecordCount || repair.TotalBytes != d.ExpectedTotalBytes || repair.ContentDigest != d.ExpectedContentDigest || repair.NextRecord != d.ExpectedRecordCount || repair.NextOffset != d.ExpectedTotalBytes) {
		return errors.New("completed repair does not match instruction summary")
	}
	if repair.State == RepairFailed && repair.ErrorCode == 0 || repair.State != RepairFailed && repair.ErrorCode != 0 {
		return errors.New("repair error/state mismatch")
	}
	return nil
}

// Fence durably advances the sole coordinator authority.
func (store *Store) Fence(epoch model.CoordinatorEpoch) error {
	payload, err := encodeFence(epoch)
	if err != nil {
		return err
	}
	return store.applyWorkTransaction(Transaction{Records: []Record{{Type: recordFence, Payload: payload}}}, BoundaryFence)
}

// InstallAssignment atomically validates, owns, and replaces one complete assignment.
func (store *Store) InstallAssignment(set model.AssignmentSet, specification model.TopologySpec, jobRevision uint64, scheduling model.SchedulingState, epoch model.CoordinatorEpoch) error {
	validated, err := model.ValidateTopology(specification)
	if err != nil {
		return err
	}
	if err = set.Validate(validated); err != nil {
		return err
	}
	installed := InstalledAssignment{Assignment: set, SpecificationBytes: validated.CanonicalBytes(), Topology: validated, JobControlRevision: jobRevision, SchedulingState: scheduling, CoordinatorEpoch: epoch}
	payload, err := encodeAssignment(installed)
	if err != nil {
		return err
	}
	return store.applyWorkTransaction(Transaction{Records: []Record{{Type: recordAssignment, Payload: payload}}}, assignmentBoundary(scheduling))
}

// Receive durably accepts exact custody or returns the prior duplicate state.
func (store *Store) Receive(record DeliveryRecord) (DeliveryState, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return 0, ErrClosed
	}
	if store.failed {
		return 0, ErrUnavailable
	}
	if record.Destination.WorkerID != store.state.Identity.NodeID || record.Destination.WorkerEpoch != store.state.WorkerEpoch {
		return 0, errors.New("delivery destination is not this worker incarnation")
	}
	if state, found, err := probeDelivery(&store.work, record); found || err != nil {
		return state, err
	}
	payload, err := encodeDeliveryRecord(record, nil)
	if err != nil {
		return 0, err
	}
	tx := Transaction{Records: []Record{{Type: recordDelivery, Payload: payload}}}
	prospective, err := store.reduceWorkLocked(tx)
	if err != nil {
		return 0, err
	}
	if err = store.commitWorkLocked(tx, prospective); err != nil {
		return 0, err
	}
	store.durable(BoundaryDeliveryReceived)
	return Received, nil
}

// ProbeDelivery non-mutatingly returns exact prior custody even when current
// admission authority has advanced. Unknown identities remain distinguishable
// from changed bytes under a known durable identity.
func (store *Store) ProbeDelivery(record DeliveryRecord) (DeliveryState, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return 0, false, ErrClosed
	}
	if store.failed {
		return 0, false, ErrUnavailable
	}
	if record.Destination.WorkerID != store.state.Identity.NodeID || record.Destination.WorkerEpoch != store.state.WorkerEpoch {
		return 0, false, errors.New("delivery destination is not this worker incarnation")
	}
	return probeDelivery(&store.work, record)
}

func probeDelivery(work *RecoveredWork, record DeliveryRecord) (DeliveryState, bool, error) {
	if index := deliveryIndex(work.Deliveries, record.ID); index >= 0 {
		prior := work.Deliveries[index]
		if prior.State == Compacted {
			digest, err := deliveryDefinitionDigest(record)
			if err != nil {
				return 0, true, err
			}
			if digest != prior.definitionDigest {
				return 0, true, model.ErrIdentityReuse
			}
			return Compacted, true, nil
		}
		if !equalDeliveryDefinition(prior, record) {
			// A current-fence rebrand of one's own exact retained definition
			// re-adopts the retained custody (defect #5 ruling): the delivery
			// re-enters under the fence it was re-validated against.
			assignment, ok := findAssignment(work, record.ID.Tuple.JobID)
			if ok && record.CoordinatorEpoch == work.Fence &&
				compareEpochOrder(prior.CoordinatorEpoch, work.Fence) < 0 &&
				assignment.CoordinatorEpoch == work.Fence &&
				prior.AssignmentRevision == assignment.Assignment.Revision &&
				prior.AssignmentDigest == assignment.Assignment.Digest &&
				equalDeliveryDefinitionModuloEpoch(prior, record) {
				return prior.State, true, nil
			}
			// The current assignment's exact derivation of the same logical
			// custody re-delivered by a (possibly replaced) producer answers
			// from custody retained under a superseded revision when this
			// destination task's incarnation is unchanged (Task 24 defect #4
			// ruling: retained custody re-envelopes under the current
			// assignment).
			if ok && record.CoordinatorEpoch == work.Fence && assignment.CoordinatorEpoch == work.Fence &&
				prior.AssignmentRevision < assignment.Assignment.Revision &&
				record.AssignmentRevision == assignment.Assignment.Revision && record.AssignmentDigest == assignment.Assignment.Digest &&
				sameTaskIncarnation(assignment.Assignment, prior.Destination) {
				derived, exists, err := deriveDeliveryDefinition(assignment, work.Fence, record.ID)
				if err != nil {
					return 0, true, err
				}
				if exists && equalDeliveryDefinition(derived, record) && equalDeliveryDefinitionModuloEnvelope(derived, prior) {
					return prior.State, true, nil
				}
			}
			return 0, true, model.ErrIdentityReuse
		}
		return prior.State, true, nil
	}
	cursor := sourceIndex(work.Sources, record.ID.Tuple.SourceTask)
	if cursor < 0 || record.ID.Tuple.SourceSequence > work.Sources[cursor].Watermark {
		return 0, false, nil
	}
	assignment, ok := findAssignment(work, record.ID.Tuple.JobID)
	if !ok {
		return 0, true, model.ErrIdentityReuse
	}
	expected, exists, err := deriveDeliveryDefinition(assignment, assignment.CoordinatorEpoch, record.ID)
	if err != nil {
		return 0, true, err
	}
	if !exists {
		return 0, false, nil
	}
	if !equalCompactedLogicalDefinition(expected, record) {
		return 0, true, model.ErrIdentityReuse
	}
	if record.AssignmentRevision < expected.AssignmentRevision {
		// A committed checkpoint proves every upstream outbox completion was
		// already durable, so no correct sender depends on an ACK beyond this
		// causal-safe frontier. Once assignment replacement retires the old
		// tokens, fail closed instead of accepting unverifiable authority.
		return 0, true, ErrHistoricalAuthorityUnavailable
	}
	if !equalDeliveryDefinition(expected, record) {
		return 0, true, model.ErrIdentityReuse
	}
	return Compacted, true, nil
}

// MarkProcessed atomically persists deterministic outputs and every downstream outbox.
func (store *Store) MarkProcessed(id model.DeliveryID, outputs []model.Tuple, outboxes []OutboxRecord) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrClosed
	}
	if store.failed {
		return ErrUnavailable
	}
	index := deliveryIndex(store.work.Deliveries, id)
	if index < 0 {
		return errors.New("unknown delivery")
	}
	if uint64(len(outboxes)) > model.LimitsV1().MaxDerivedDeliveries {
		return errors.New("processed outbox count exceeds v1 bounds")
	}
	assignment, ok := findAssignment(&store.work, id.Tuple.JobID)
	if !ok {
		return errors.New("processed delivery references unknown assignment")
	}
	if _, err := validateOutputTupleBounds(outputs); err != nil {
		return err
	}
	stored := store.work.Deliveries[index]
	if stored.State == Processed || stored.State == Completed {
		if !equalTuples(stored.Outputs, outputs) || len(stored.OutboxIDs) != len(outboxes) {
			return model.ErrIdentityReuse
		}
		for outIndex, id := range stored.OutboxIDs {
			storedIndex := outboxIndex(store.work.Outboxes, id)
			candidate := outboxes[outIndex]
			if candidate.Completed || candidate.Accepted || candidate.RetryDeadlineUnixNano != 0 || storedIndex < 0 || !equalOutboxDefinition(store.work.Outboxes[storedIndex], candidate) {
				return model.ErrIdentityReuse
			}
		}
		return nil
	}
	if err := validateProcessedOutputs(stored, outputs, assignment); err != nil {
		return err
	}
	record := stored.Clone()
	if record.State != Received {
		return errors.New("delivery not received")
	}
	record.State = Processed
	record.Outputs = cloneTuples(outputs)
	payload, err := encodeDeliveryRecord(record, outboxes)
	if err != nil {
		return err
	}
	tx := Transaction{Records: []Record{{Type: recordDeliveryProcessed, Payload: payload}}}
	prospective, err := store.reduceWorkLocked(tx)
	if err != nil {
		return err
	}
	if err := store.commitWorkLocked(tx, prospective); err != nil {
		return err
	}
	store.durable(BoundaryDeliveryProcessed)
	return nil
}

// MarkCompleted durably closes a processed delivery.
func (store *Store) MarkCompleted(id model.DeliveryID) error {
	payload, err := encodeDeliveryIDPayload(id)
	if err != nil {
		return err
	}
	return store.applyWorkTransaction(Transaction{Records: []Record{{Type: recordDeliveryCompleted, Payload: payload}}}, BoundaryDeliveryCompleted)
}

// ApplyCheckpoint persists a monotonic watermark before compacting covered replay state.
func (store *Store) ApplyCheckpoint(notice model.CheckpointNotice) error {
	payload, err := encodeCheckpoint(notice)
	if err != nil {
		return err
	}
	return store.applyWorkTransaction(Transaction{Records: []Record{{Type: recordCheckpoint, Payload: payload}}}, BoundaryCheckpointApplied)
}

// ObserveCheckpoint durably records one authenticated committed checkpoint for
// a local participant that does not own the source cursor.
func (store *Store) ObserveCheckpoint(notice protocol.CheckpointNotice) error {
	observation := CommittedCheckpoint{Notice: notice.Notice, JobControlRevision: notice.JobControlRevision, AssignmentRevision: notice.AssignmentRevision, AssignmentDigest: notice.AssignmentDigest}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrClosed
	}
	if store.failed {
		return ErrUnavailable
	}
	if index := checkpointObservationIndex(store.work.Checkpoints, observation.Notice.Source); index >= 0 && store.work.Checkpoints[index].Notice.RaftIndex == observation.Notice.RaftIndex {
		if store.work.Checkpoints[index] == observation {
			return nil
		}
		return model.ErrIdentityReuse
	}
	payload, err := encodeCheckpointObservation(observation)
	if err != nil {
		return err
	}
	tx := Transaction{Records: []Record{{Type: recordCheckpointObservation, Payload: payload}}}
	prospective, err := store.reduceWorkLocked(tx)
	if err != nil {
		return err
	}
	if err := store.commitWorkLocked(tx, prospective); err != nil {
		return err
	}
	store.durable(BoundaryCheckpointObserved)
	return nil
}

// UpsertResult idempotently persists one logical record and exact current-copy provenance.
func (store *Store) UpsertResult(record model.ResultRecord, provenance model.ResultCopyProvenance) error {
	store.mu.Lock()
	if store.closed {
		store.mu.Unlock()
		return ErrClosed
	}
	if store.failed {
		store.mu.Unlock()
		return ErrUnavailable
	}
	if !provenanceTargets(provenance, store.state.Identity.NodeID, store.state.WorkerEpoch) {
		store.mu.Unlock()
		return errors.New("result provenance does not designate this worker incarnation")
	}
	payload, err := encodeStoredResult(StoredResult{Record: record, Provenance: provenance})
	if err != nil {
		store.mu.Unlock()
		return err
	}
	tx := Transaction{Records: []Record{{Type: recordResult, Payload: payload}}}
	prospective, err := store.reduceWorkLocked(tx)
	if err == nil {
		err = store.commitWorkLocked(tx, prospective)
	}
	if err == nil {
		store.durable(BoundaryResultUpserted)
	}
	store.mu.Unlock()
	return err
}

// PersistEvent appends the exact next globally identified completion/failure event.
func (store *Store) PersistEvent(event model.WorkerEvent) error {
	store.mu.Lock()
	if store.closed {
		store.mu.Unlock()
		return ErrClosed
	}
	if store.failed {
		store.mu.Unlock()
		return ErrUnavailable
	}
	if event.WorkerID != store.state.Identity.NodeID || event.WorkerEpoch != store.state.WorkerEpoch {
		store.mu.Unlock()
		return errors.New("event worker incarnation mismatch")
	}
	payload, err := encodeEvent(event)
	if err != nil {
		store.mu.Unlock()
		return err
	}
	tx := Transaction{Records: []Record{{Type: recordEvent, Payload: payload}}}
	prospective, err := store.reduceWorkLocked(tx)
	if err == nil {
		err = store.commitWorkLocked(tx, prospective)
	}
	if err == nil {
		store.durable(BoundaryEventPersisted)
	}
	store.mu.Unlock()
	return err
}

// PendingEvents returns an owned strictly increasing page after the supplied cursor.
func (store *Store) PendingEvents(after uint64, max uint16) ([]model.WorkerEvent, uint64, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return nil, after, false, ErrClosed
	}
	if max == 0 || max > model.WorkerControlMaxStatusEventsV1 {
		return nil, after, false, errors.New("event page bound")
	}
	result := make([]model.WorkerEvent, 0, max)
	more := false
	for _, event := range store.work.PendingEvents {
		if event.TransactionID <= after {
			continue
		}
		if len(result) == int(max) {
			more = true
			break
		}
		result = append(result, cloneEvent(event))
	}
	last := after
	if len(result) > 0 {
		last = result[len(result)-1].TransactionID
	}
	return result, last, more, nil
}

// AcknowledgeEvents durably removes pending events through a proven response cursor.
func (store *Store) AcknowledgeEvents(through uint64) error {
	payload := encodeUint64Payload(through)
	return store.applyWorkTransaction(Transaction{Records: []Record{{Type: recordEventAck, Payload: payload}}}, BoundaryEventsAcknowledged)
}

// UpsertRepair persists exact instructions and monotonic resumable progress.
func (store *Store) UpsertRepair(repair ResultRepairRecord) error {
	store.mu.Lock()
	if store.closed {
		store.mu.Unlock()
		return ErrClosed
	}
	if store.failed {
		store.mu.Unlock()
		return ErrUnavailable
	}
	if !repairTargets(repair, store.state.Identity.NodeID, store.state.WorkerEpoch) {
		store.mu.Unlock()
		return errors.New("repair role does not designate this worker incarnation")
	}
	payload, err := encodeRepair(repair)
	if err != nil {
		store.mu.Unlock()
		return err
	}
	tx := Transaction{Records: []Record{{Type: recordRepair, Payload: payload}}}
	prospective, err := store.reduceWorkLocked(tx)
	if err == nil {
		err = store.commitWorkLocked(tx, prospective)
	}
	if err == nil {
		store.durable(repairBoundary(repair.State))
	}
	store.mu.Unlock()
	return err
}

// AdvanceSource atomically persists a source cursor and the outboxes it created.
func (store *Store) AdvanceSource(cursor SourceCursor, outboxes []OutboxRecord) error {
	payload, err := encodeSource(cursor, outboxes)
	if err != nil {
		return err
	}
	return store.applyWorkTransaction(Transaction{Records: []Record{{Type: recordSource, Payload: payload}}}, BoundarySourceAdvanced)
}

// MarkOutboxCompleted durably records one downstream completion.
func (store *Store) MarkOutboxCompleted(id model.DeliveryID) error {
	payload, err := encodeDeliveryIDPayload(id)
	if err != nil {
		return err
	}
	return store.applyWorkTransaction(Transaction{Records: []Record{{Type: recordOutboxAck, Payload: payload}}}, BoundaryOutboxCompleted)
}

// MarkOutboxDispatched durably records the retry deadline chosen from the
// injected clock at actual sender dispatch start.
func (store *Store) MarkOutboxDispatched(id model.DeliveryID, deadlineUnixNano int64) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrClosed
	}
	if store.failed {
		return ErrUnavailable
	}
	index := outboxIndex(store.work.Outboxes, id)
	if index < 0 {
		return errors.New("unknown outbox")
	}
	update := outboxRetryUpdate{ID: id, Accepted: store.work.Outboxes[index].Accepted, DeadlineUnixNano: deadlineUnixNano}
	payload, err := encodeOutboxRetry(update)
	if err != nil {
		return err
	}
	tx := Transaction{Records: []Record{{Type: recordOutboxRetry, Payload: payload}}}
	prospective, err := store.reduceWorkLocked(tx)
	if err != nil {
		return err
	}
	if err := store.commitWorkLocked(tx, prospective); err != nil {
		return err
	}
	store.durable(BoundaryOutboxDispatched)
	return nil
}

// MarkOutboxAccepted durably enters the completion-wait retry phase.
func (store *Store) MarkOutboxAccepted(id model.DeliveryID, deadlineUnixNano int64) error {
	update := outboxRetryUpdate{ID: id, Accepted: true, AcceptTransition: true, DeadlineUnixNano: deadlineUnixNano}
	payload, err := encodeOutboxRetry(update)
	if err != nil {
		return err
	}
	return store.applyWorkTransaction(Transaction{Records: []Record{{Type: recordOutboxRetry, Payload: payload}}}, BoundaryOutboxAccepted)
}

// RecoverWork returns a complete independently owned validated worker view.
func (store *Store) RecoverWork() (RecoveredWork, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return RecoveredWork{}, ErrClosed
	}
	if store.failed {
		return RecoveredWork{}, ErrUnavailable
	}
	return store.work.Clone(), nil
}

// applyWorkTransaction commits one registered transaction and, only after its
// durable commit succeeded, publishes the named boundary.
func (store *Store) applyWorkTransaction(tx Transaction, boundary string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrClosed
	}
	if store.failed {
		return ErrUnavailable
	}
	prospective, err := store.reduceWorkLocked(tx)
	if err != nil {
		return err
	}
	if err := store.commitWorkLocked(tx, prospective); err != nil {
		return err
	}
	if boundary != "" {
		store.durable(boundary)
	}
	return nil
}
func (store *Store) reduceWorkLocked(tx Transaction) (RecoveredWork, error) {
	reducer := &workReducer{current: store.work}
	if err := reducer.BeginTransaction(uint32(len(tx.Records))); err != nil {
		return RecoveredWork{}, err
	}
	for _, record := range tx.Records {
		if err := reducer.ConsumeRecord(record); err != nil {
			return RecoveredWork{}, err
		}
	}
	if err := reducer.CommitTransaction(); err != nil {
		return RecoveredWork{}, err
	}
	return reducer.current, nil
}
func (store *Store) commitWorkLocked(tx Transaction, prospective RecoveredWork) error {
	if err := validateRecoveredWorkLocal(prospective, store.state.Identity.NodeID, store.state.WorkerEpoch); err != nil {
		return err
	}
	encodedBytes, err := transactionEncodedSize(tx)
	if err != nil {
		return err
	}
	reserved, err := reservedBytes(prospective)
	if err != nil {
		return err
	}
	used, ok := checkedAdd(store.state.SnapshotBytes, store.state.WALBytes)
	if !ok || used > store.options.MaxBytes || encodedBytes > store.options.MaxBytes-used || reserved > store.options.MaxBytes-used-encodedBytes {
		return ErrCapacity
	}
	if err := store.commitLocked(tx); err != nil {
		return err
	}
	store.work = prospective
	return nil
}

func checkedAdd(left, right uint64) (uint64, bool) {
	if left > math.MaxUint64-right {
		return 0, false
	}
	return left + right, true
}

func validateRegisteredTransaction(transaction Transaction) error {
	if err := transaction.Validate(); err != nil {
		return err
	}
	for index, record := range transaction.Records {
		if record.Type < recordFence || record.Type > recordCheckpointObservation {
			return fmt.Errorf("%w: record %d has unregistered type %d", ErrInvalidTransaction, index, record.Type)
		}
	}
	return nil
}
func reservedBytes(work RecoveredWork) (uint64, error) {
	var total uint64
	for _, delivery := range work.Deliveries {
		if total > math.MaxUint64-delivery.Reservation {
			return 0, ErrCapacity
		}
		total += delivery.Reservation
	}
	return total, nil
}

func validateRecoveredWorkLocal(work RecoveredWork, nodeID uint16, workerEpoch model.WorkerEpoch) error {
	for _, cursor := range work.Sources {
		assignment, ok := findAssignment(&work, cursor.Source.JobID)
		token, tokenOK := findToken(assignment.Assignment, cursor.Source)
		eof, eofErr := model.SourceEOF(assignment.Topology, cursor.Source)
		if !ok || !tokenOK || token.WorkerID != nodeID || token.WorkerEpoch != workerEpoch || eofErr != nil || cursor.EOF != eof {
			return errors.New("recovered source cursor is not the current local assigned source")
		}
	}
	for _, observation := range work.Checkpoints {
		assignment, ok := findAssignment(&work, observation.Notice.JobID)
		if !ok || !assignmentTargetsWorker(assignment.Assignment, nodeID, workerEpoch) && !historicalResultHolder(work, assignment, nodeID, workerEpoch) {
			return errors.New("recovered checkpoint observation is not local assignment participation")
		}
	}
	for _, delivery := range work.Deliveries {
		if delivery.Destination.WorkerID != nodeID || delivery.Destination.WorkerEpoch != workerEpoch {
			return errors.New("recovered delivery targets another worker incarnation")
		}
	}
	for _, outbox := range work.Outboxes {
		assignment, ok := findAssignment(&work, outbox.ID.Tuple.JobID)
		if !ok || outbox.Producer.WorkerID != nodeID || outbox.Producer.WorkerEpoch != workerEpoch {
			return errors.New("recovered outbox producer is not this worker incarnation")
		}
		if err := validateSnapshotOutbox(outbox, assignment, work.Fence); err != nil {
			return fmt.Errorf("recovered outbox cross-reference: %w", err)
		}
	}
	var resultTargetErr error
	visitResults(work, func(result StoredResult) bool {
		if !provenanceTargets(result.Provenance, nodeID, workerEpoch) {
			resultTargetErr = errors.New("recovered result provenance targets another worker incarnation")
			return false
		}
		return true
	})
	if resultTargetErr != nil {
		return resultTargetErr
	}
	for _, event := range work.PendingEvents {
		if event.WorkerID != nodeID || event.WorkerEpoch != workerEpoch {
			return errors.New("recovered event targets another worker incarnation")
		}
	}
	for _, repair := range work.Repairs {
		if !repairTargets(repair, nodeID, workerEpoch) {
			return errors.New("recovered repair role targets another worker incarnation")
		}
	}
	return nil
}

func provenanceTargets(provenance model.ResultCopyProvenance, nodeID uint16, workerEpoch model.WorkerEpoch) bool {
	switch provenance.DestinationRole {
	case model.PrimaryReplica:
		return provenance.ReplicaSet.PrimaryNodeID == nodeID && provenance.ReplicaSet.PrimaryEpoch == workerEpoch
	case model.SecondaryReplica:
		return provenance.ReplicaSet.SecondaryNodeID == nodeID && provenance.ReplicaSet.SecondaryEpoch == workerEpoch
	default:
		return false
	}
}

func historicalResultHolder(work RecoveredWork, assignment InstalledAssignment, nodeID uint16, workerEpoch model.WorkerEpoch) bool {
	if assignment.SchedulingState == model.Running {
		return false
	}
	found := false
	visitResults(work, func(result StoredResult) bool {
		if result.Record.TupleID.JobID == assignment.Assignment.JobID && result.Record.SpecificationHash == assignment.Topology.Digest() && result.Provenance.AssignmentRevision < assignment.Assignment.Revision && result.Provenance.AssignmentDigest != assignment.Assignment.Digest && result.Provenance.Validate(result.Record) == nil && provenanceTargets(result.Provenance, nodeID, workerEpoch) {
			found = true
			return false
		}
		return true
	})
	return found
}
func repairTargets(repair ResultRepairRecord, nodeID uint16, workerEpoch model.WorkerEpoch) bool {
	switch repair.Role {
	case RepairSource:
		return repair.Instruction.SourceNodeID == nodeID && repair.Instruction.SourceWorkerEpoch == workerEpoch
	case RepairDestination:
		return repair.Instruction.DestinationNodeID == nodeID && repair.Instruction.DestinationWorkerEpoch == workerEpoch
	default:
		return false
	}
}

func repairDestinationMatchesReplica(repair model.RepairResultPartitionDefinition, replica model.ResultReplicaSet) bool {
	return repair.DestinationNodeID == replica.PrimaryNodeID && repair.DestinationWorkerEpoch == replica.PrimaryEpoch || repair.DestinationNodeID == replica.SecondaryNodeID && repair.DestinationWorkerEpoch == replica.SecondaryEpoch
}

// --- canonical record codecs ---

func encodeFence(epoch model.CoordinatorEpoch) ([]byte, error) {
	if err := epoch.Validate(); err != nil {
		return nil, err
	}
	w := newRecordWriter()
	w.u16(domainRecordSchema)
	w.epoch(epoch)
	return w.bytes(), nil
}
func decodeFence(payload []byte) (model.CoordinatorEpoch, error) {
	r := newRecordReader(payload)
	if err := r.schema(); err != nil {
		return model.CoordinatorEpoch{}, err
	}
	epoch, err := r.epoch()
	if err != nil || !r.done() {
		return model.CoordinatorEpoch{}, errors.New("invalid fence record")
	}
	return epoch, epoch.Validate()
}

func encodeAssignment(a InstalledAssignment) ([]byte, error) {
	decoded, err := model.DecodeTopology(a.SpecificationBytes)
	if err != nil {
		return nil, err
	}
	message := protocol.AssignmentSetInstall{Assignment: a.Assignment, Specification: decoded.Spec(), SpecificationDigest: decoded.Digest(), JobControlRevision: a.JobControlRevision, SchedulingState: a.SchedulingState, CoordinatorEpoch: a.CoordinatorEpoch}
	encoded, err := protocol.MarshalAssignmentSetInstall(message)
	if err != nil {
		return nil, err
	}
	w := newRecordWriter()
	w.u16(domainRecordSchema)
	w.blob(encoded)
	return w.bytes(), nil
}
func decodeAssignment(payload []byte) (InstalledAssignment, error) {
	r := newRecordReader(payload)
	if err := r.schema(); err != nil {
		return InstalledAssignment{}, err
	}
	encoded, err := r.blob(MaxRecordPayloadBytes)
	if err != nil || !r.done() {
		return InstalledAssignment{}, errors.New("invalid assignment record")
	}
	message, err := protocol.UnmarshalAssignmentSetInstall(encoded)
	if err != nil {
		return InstalledAssignment{}, err
	}
	topology, err := model.ValidateTopology(message.Specification)
	if err != nil {
		return InstalledAssignment{}, err
	}
	if topology.Digest() != message.SpecificationDigest {
		return InstalledAssignment{}, errors.New("assignment specification digest mismatch")
	}
	return InstalledAssignment{Assignment: message.Assignment, SpecificationBytes: topology.CanonicalBytes(), Topology: topology, JobControlRevision: message.JobControlRevision, SchedulingState: message.SchedulingState, CoordinatorEpoch: message.CoordinatorEpoch}, nil
}

func encodeDeliveryRecord(record DeliveryRecord, outboxes []OutboxRecord) ([]byte, error) {
	message := protocol.TupleDelivery{DeliveryID: record.ID, Tuple: record.Tuple, Producer: record.Producer, Destination: record.Destination, Assignment: protocol.AssignmentSetIdentity{JobID: record.ID.Tuple.JobID, Revision: record.AssignmentRevision, Digest: record.AssignmentDigest}, Coordinator: record.CoordinatorEpoch}
	encoded, err := protocol.MarshalTupleDelivery(message)
	if err != nil {
		return nil, err
	}
	w := newRecordWriter()
	w.u16(domainRecordSchema)
	w.u8(uint8(record.State))
	w.u64(record.Reservation)
	w.blob(encoded)
	if uint64(len(record.Outputs)) > model.LimitsV1().MaxOperatorOutputs || uint64(len(outboxes)) > model.LimitsV1().MaxDerivedDeliveries {
		return nil, errors.New("delivery collections exceed bounds")
	}
	w.u16(uint16(len(record.Outputs)))
	for _, tuple := range record.Outputs {
		b, e := model.MarshalTuple(tuple)
		if e != nil {
			return nil, e
		}
		w.blob(b)
	}
	w.u16(uint16(len(outboxes)))
	for _, outbox := range outboxes {
		b, e := encodeOutbox(outbox)
		if e != nil {
			return nil, e
		}
		w.blob(b)
	}
	return w.bytes(), nil
}
func decodeDeliveryRecord(payload []byte) (DeliveryRecord, []OutboxRecord, error) {
	r := newRecordReader(payload)
	if err := r.schema(); err != nil {
		return DeliveryRecord{}, nil, err
	}
	state, err := r.u8()
	if err != nil {
		return DeliveryRecord{}, nil, err
	}
	reservation, err := r.u64()
	if err != nil {
		return DeliveryRecord{}, nil, err
	}
	encoded, err := r.blob(MaxRecordPayloadBytes)
	if err != nil {
		return DeliveryRecord{}, nil, err
	}
	message, err := protocol.UnmarshalTupleDelivery(encoded)
	if err != nil {
		return DeliveryRecord{}, nil, err
	}
	record := DeliveryRecord{ID: message.DeliveryID, Tuple: message.Tuple, Producer: message.Producer, Destination: message.Destination, AssignmentRevision: message.Assignment.Revision, AssignmentDigest: message.Assignment.Digest, CoordinatorEpoch: message.Coordinator, State: DeliveryState(state), Reservation: reservation}
	count, err := r.u16()
	if err != nil {
		return DeliveryRecord{}, nil, err
	}
	if uint64(count) > model.LimitsV1().MaxOperatorOutputs || r.remaining() < int(count)*4+2 {
		return DeliveryRecord{}, nil, errors.New("delivery output collection exceeds bounds or remaining bytes")
	}
	record.Outputs = make([]model.Tuple, int(count))
	for i := range record.Outputs {
		b, e := r.blob(model.LimitsV1().MaxTuplePayloadBytes)
		if e != nil {
			return DeliveryRecord{}, nil, e
		}
		record.Outputs[i], e = model.UnmarshalTuple(b)
		if e != nil {
			return DeliveryRecord{}, nil, e
		}
	}
	outCount, err := r.u16()
	if err != nil {
		return DeliveryRecord{}, nil, err
	}
	if uint64(outCount) > model.LimitsV1().MaxDerivedDeliveries || r.remaining() < int(outCount)*4 {
		return DeliveryRecord{}, nil, errors.New("delivery outbox collection exceeds bounds or remaining bytes")
	}
	outboxes := make([]OutboxRecord, int(outCount))
	record.OutboxIDs = make([]model.DeliveryID, 0, int(outCount))
	for i := range outboxes {
		b, e := r.blob(MaxRecordPayloadBytes)
		if e != nil {
			return DeliveryRecord{}, nil, e
		}
		outboxes[i], e = decodeOutbox(b)
		if e != nil {
			return DeliveryRecord{}, nil, e
		}
		record.OutboxIDs = append(record.OutboxIDs, outboxes[i].ID)
	}
	if !r.done() {
		return DeliveryRecord{}, nil, errors.New("trailing delivery bytes")
	}
	return record, outboxes, nil
}
func encodeOutbox(record OutboxRecord) ([]byte, error) {
	message := protocol.TupleDelivery{DeliveryID: record.ID, Tuple: record.Tuple, Producer: record.Producer, Destination: record.Destination, Assignment: protocol.AssignmentSetIdentity{JobID: record.ID.Tuple.JobID, Revision: record.AssignmentRevision, Digest: record.AssignmentDigest}, Coordinator: record.CoordinatorEpoch}
	encoded, err := protocol.MarshalTupleDelivery(message)
	if err != nil {
		return nil, err
	}
	w := newRecordWriter()
	w.u16(outboxRecordSchema)
	w.u8(boolByte(record.Completed))
	w.u8(boolByte(record.Accepted))
	w.u64(uint64(record.RetryDeadlineUnixNano))
	w.blob(encoded)
	return w.bytes(), nil
}
func decodeOutbox(payload []byte) (OutboxRecord, error) {
	r := newRecordReader(payload)
	schema, err := r.u16()
	if err != nil || schema != domainRecordSchema && schema != outboxRecordSchema {
		return OutboxRecord{}, errors.New("unsupported outbox schema")
	}
	complete, err := r.u8()
	if err != nil || complete > 1 {
		return OutboxRecord{}, errors.New("invalid outbox status")
	}
	var accepted uint8
	var deadline int64
	if schema == outboxRecordSchema {
		accepted, err = r.u8()
		if err != nil || accepted > 1 {
			return OutboxRecord{}, errors.New("invalid outbox retry phase")
		}
		raw, deadlineErr := r.u64()
		if deadlineErr != nil {
			return OutboxRecord{}, deadlineErr
		}
		deadline = int64(raw)
		if accepted == 1 && deadline == 0 {
			return OutboxRecord{}, errors.New("accepted outbox has unset retry deadline")
		}
	}
	encoded, err := r.blob(MaxRecordPayloadBytes)
	if err != nil || !r.done() {
		return OutboxRecord{}, errors.New("invalid outbox record")
	}
	message, err := protocol.UnmarshalTupleDelivery(encoded)
	if err != nil {
		return OutboxRecord{}, err
	}
	return OutboxRecord{ID: message.DeliveryID, Tuple: message.Tuple, Producer: message.Producer, Destination: message.Destination, AssignmentRevision: message.Assignment.Revision, AssignmentDigest: message.Assignment.Digest, CoordinatorEpoch: message.Coordinator, Completed: complete == 1, Accepted: accepted == 1, RetryDeadlineUnixNano: deadline}, nil
}

func encodeOutboxRetry(update outboxRetryUpdate) ([]byte, error) {
	if err := update.ID.Validate(); err != nil {
		return nil, err
	}
	if update.DeadlineUnixNano == 0 || update.AcceptTransition && !update.Accepted {
		return nil, errors.New("invalid outbox retry update")
	}
	w := newRecordWriter()
	w.u16(domainRecordSchema)
	w.u8(boolByte(update.Accepted))
	w.u8(boolByte(update.AcceptTransition))
	w.u64(uint64(update.DeadlineUnixNano))
	w.deliveryID(update.ID)
	return w.bytes(), nil
}

func decodeOutboxRetry(payload []byte) (outboxRetryUpdate, error) {
	r := newRecordReader(payload)
	if err := r.schema(); err != nil {
		return outboxRetryUpdate{}, err
	}
	accepted, err := r.u8()
	if err != nil || accepted > 1 {
		return outboxRetryUpdate{}, errors.New("invalid outbox retry phase")
	}
	transition, err := r.u8()
	if err != nil || transition > 1 {
		return outboxRetryUpdate{}, errors.New("invalid outbox retry transition")
	}
	deadline, err := r.u64()
	if err != nil {
		return outboxRetryUpdate{}, err
	}
	id, err := r.deliveryID()
	if err != nil || !r.done() {
		return outboxRetryUpdate{}, errors.New("invalid outbox retry record")
	}
	update := outboxRetryUpdate{ID: id, Accepted: accepted == 1, AcceptTransition: transition == 1, DeadlineUnixNano: int64(deadline)}
	if update.DeadlineUnixNano == 0 || update.AcceptTransition && !update.Accepted {
		return outboxRetryUpdate{}, errors.New("invalid outbox retry update")
	}
	return update, nil
}

func encodeDeliveryIDPayload(id model.DeliveryID) ([]byte, error) {
	if err := id.Validate(); err != nil {
		return nil, err
	}
	w := newRecordWriter()
	w.u16(domainRecordSchema)
	w.deliveryID(id)
	return w.bytes(), nil
}
func decodeDeliveryIDPayload(payload []byte) (model.DeliveryID, error) {
	r := newRecordReader(payload)
	if err := r.schema(); err != nil {
		return model.DeliveryID{}, err
	}
	id, err := r.deliveryID()
	if err != nil || !r.done() {
		return model.DeliveryID{}, errors.New("invalid delivery identity record")
	}
	return id, id.Validate()
}

func encodeCheckpoint(n model.CheckpointNotice) ([]byte, error) {
	if err := n.Validate(); err != nil {
		return nil, err
	}
	w := newRecordWriter()
	w.u16(checkpointRecordSchema)
	w.job(n.JobID)
	w.task(n.Source)
	w.u64(n.Watermark)
	w.u64(n.RaftIndex)
	w.epoch(n.Epoch)
	return w.bytes(), nil
}
func decodeCheckpoint(payload []byte) (model.CheckpointNotice, error) {
	r := newRecordReader(payload)
	schema, err := r.u16()
	if err != nil {
		return model.CheckpointNotice{}, err
	}
	if schema != domainRecordSchema && schema != checkpointRecordSchema {
		return model.CheckpointNotice{}, errors.New("unsupported checkpoint record schema")
	}
	var n model.CheckpointNotice
	if n.JobID, err = r.job(); err != nil {
		return n, err
	}
	if n.Source, err = r.task(); err != nil {
		return n, err
	}
	if n.Watermark, err = r.u64(); err != nil {
		return n, err
	}
	if n.RaftIndex, err = r.u64(); err != nil {
		return n, err
	}
	if n.Epoch, err = r.epoch(); err != nil || !r.done() {
		return n, errors.New("invalid checkpoint record")
	}
	return n, n.Validate()
}

func encodeCheckpointObservation(observation CommittedCheckpoint) ([]byte, error) {
	if err := validateCheckpointObservation(observation); err != nil {
		return nil, err
	}
	w := newRecordWriter()
	w.u16(checkpointObservationRecordSchema)
	w.job(observation.Notice.JobID)
	w.task(observation.Notice.Source)
	w.u64(observation.Notice.Watermark)
	w.u64(observation.Notice.RaftIndex)
	w.epoch(observation.Notice.Epoch)
	w.u64(observation.JobControlRevision)
	w.u64(observation.AssignmentRevision)
	w.fixed32(observation.AssignmentDigest)
	return w.bytes(), nil
}

func decodeCheckpointObservation(payload []byte) (CommittedCheckpoint, error) {
	r := newRecordReader(payload)
	schema, err := r.u16()
	if err != nil || schema != checkpointObservationRecordSchema {
		return CommittedCheckpoint{}, errors.New("unsupported checkpoint observation record schema")
	}
	var observation CommittedCheckpoint
	if observation.Notice.JobID, err = r.job(); err != nil {
		return observation, err
	}
	if observation.Notice.Source, err = r.task(); err != nil {
		return observation, err
	}
	if observation.Notice.Watermark, err = r.u64(); err != nil {
		return observation, err
	}
	if observation.Notice.RaftIndex, err = r.u64(); err != nil {
		return observation, err
	}
	if observation.Notice.Epoch, err = r.epoch(); err != nil {
		return observation, err
	}
	if observation.JobControlRevision, err = r.u64(); err != nil {
		return observation, err
	}
	if observation.AssignmentRevision, err = r.u64(); err != nil {
		return observation, err
	}
	if observation.AssignmentDigest, err = r.fixed32(); err != nil || !r.done() {
		return observation, errors.New("invalid checkpoint observation record")
	}
	return observation, validateCheckpointObservation(observation)
}

func encodeStoredResult(result StoredResult) ([]byte, error) {
	if err := result.Provenance.Validate(result.Record); err != nil {
		return nil, err
	}
	logical, err := model.MarshalResultRecord(result.Record)
	if err != nil {
		return nil, err
	}
	w := newRecordWriter()
	w.u16(domainRecordSchema)
	w.blob(logical)
	w.u64(result.Provenance.AssignmentRevision)
	w.fixed32(result.Provenance.AssignmentDigest)
	w.replica(result.Provenance.ReplicaSet)
	w.u8(uint8(result.Provenance.DestinationRole))
	w.epoch(result.Provenance.CoordinatorEpoch)
	return w.bytes(), nil
}
func decodeStoredResult(payload []byte) (StoredResult, error) {
	r := newRecordReader(payload)
	if err := r.schema(); err != nil {
		return StoredResult{}, err
	}
	logical, err := r.blob(MaxRecordPayloadBytes)
	if err != nil {
		return StoredResult{}, err
	}
	record, err := model.UnmarshalResultRecord(logical)
	if err != nil {
		return StoredResult{}, err
	}
	var p model.ResultCopyProvenance
	if p.AssignmentRevision, err = r.u64(); err != nil {
		return StoredResult{}, err
	}
	if p.AssignmentDigest, err = r.fixed32(); err != nil {
		return StoredResult{}, err
	}
	if p.ReplicaSet, err = r.replica(); err != nil {
		return StoredResult{}, err
	}
	role, err := r.u8()
	p.DestinationRole = model.ResultReplicaRole(role)
	if err != nil {
		return StoredResult{}, err
	}
	if p.CoordinatorEpoch, err = r.epoch(); err != nil || !r.done() {
		return StoredResult{}, errors.New("invalid result record")
	}
	return StoredResult{Record: record, Provenance: p, canonical: logical}, p.Validate(record)
}

func encodeEvent(event model.WorkerEvent) ([]byte, error) {
	if err := event.Validate(); err != nil {
		return nil, err
	}
	w := newRecordWriter()
	w.u16(domainRecordSchema)
	w.u16(event.WorkerID)
	w.fixed16([16]byte(event.WorkerEpoch))
	w.u64(event.TransactionID)
	w.u8(uint8(event.Kind))
	switch event.Kind {
	case model.WorkerEventCompletion:
		w.completion(*event.Completion)
	case model.WorkerEventFailure:
		w.failure(*event.Failure)
	}
	return w.bytes(), nil
}
func decodeEvent(payload []byte) (model.WorkerEvent, error) {
	r := newRecordReader(payload)
	if err := r.schema(); err != nil {
		return model.WorkerEvent{}, err
	}
	node, err := r.u16()
	if err != nil {
		return model.WorkerEvent{}, err
	}
	epoch16, err := r.fixed16()
	if err != nil {
		return model.WorkerEvent{}, err
	}
	id, err := r.u64()
	if err != nil {
		return model.WorkerEvent{}, err
	}
	kind, err := r.u8()
	if err != nil {
		return model.WorkerEvent{}, err
	}
	event := model.WorkerEvent{WorkerID: node, WorkerEpoch: model.WorkerEpoch(epoch16), TransactionID: id, Kind: model.WorkerEventKind(kind)}
	switch event.Kind {
	case model.WorkerEventCompletion:
		v, e := r.completion()
		if e != nil {
			return event, e
		}
		event.Completion = &v
	case model.WorkerEventFailure:
		v, e := r.failure()
		if e != nil {
			return event, e
		}
		event.Failure = &v
	default:
		return event, errors.New("unknown event kind")
	}
	if !r.done() {
		return event, errors.New("trailing event bytes")
	}
	return event, event.Validate()
}

func encodeUint64Payload(v uint64) []byte {
	w := newRecordWriter()
	w.u16(eventAckRecordSchema)
	w.u64(v)
	return w.bytes()
}
func decodeUint64Payload(payload []byte) (uint64, error) {
	r := newRecordReader(payload)
	schema, err := r.u16()
	if err != nil {
		return 0, err
	}
	if schema != domainRecordSchema && schema != eventAckRecordSchema {
		return 0, errors.New("unsupported event acknowledgement schema")
	}
	v, err := r.u64()
	if err != nil || !r.done() {
		return 0, errors.New("invalid uint64 record")
	}
	return v, nil
}

func recordPayloadSchema(payload []byte) uint16 {
	if len(payload) < 2 {
		return 0
	}
	return binary.BigEndian.Uint16(payload[:2])
}

func encodeRepair(repair ResultRepairRecord) ([]byte, error) {
	if err := validateRepair(repair); err != nil {
		return nil, err
	}
	w := newRecordWriter()
	w.u16(domainRecordSchema)
	w.repairDefinition(repair.Instruction)
	w.fixed32(repair.InstructionDigest)
	w.u8(uint8(repair.Role))
	w.u8(uint8(repair.State))
	w.u64(repair.NextRecord)
	w.u64(repair.NextOffset)
	w.u64(repair.RecordCount)
	w.u64(repair.TotalBytes)
	w.fixed32(repair.ContentDigest)
	w.u16(uint16(repair.ErrorCode))
	return w.bytes(), nil
}
func decodeRepair(payload []byte) (ResultRepairRecord, error) {
	r := newRecordReader(payload)
	if err := r.schema(); err != nil {
		return ResultRepairRecord{}, err
	}
	var v ResultRepairRecord
	var err error
	if v.Instruction, err = r.repairDefinition(); err != nil {
		return v, err
	}
	if v.InstructionDigest, err = r.fixed32(); err != nil {
		return v, err
	}
	role, e := r.u8()
	if e != nil {
		return v, e
	}
	v.Role = protocol.RepairEndpointRole(role)
	state, e := r.u8()
	if e != nil {
		return v, e
	}
	v.State = protocol.ResultRepairState(state)
	if v.NextRecord, err = r.u64(); err != nil {
		return v, err
	}
	if v.NextOffset, err = r.u64(); err != nil {
		return v, err
	}
	if v.RecordCount, err = r.u64(); err != nil {
		return v, err
	}
	if v.TotalBytes, err = r.u64(); err != nil {
		return v, err
	}
	if v.ContentDigest, err = r.fixed32(); err != nil {
		return v, err
	}
	errorCode, readErr := r.u16()
	if readErr != nil || !r.done() {
		return v, errors.New("invalid repair record")
	}
	v.ErrorCode = protocol.WorkerErrorCode(errorCode)
	return v, validateRepair(v)
}

func encodeSource(cursor SourceCursor, outboxes []OutboxRecord) ([]byte, error) {
	return encodeSourceSchema(cursor, outboxes, sourceRecordSchema)
}

func encodeSourceSchema(cursor SourceCursor, outboxes []OutboxRecord, schema uint16) ([]byte, error) {
	if err := cursor.Source.Validate(); err != nil {
		return nil, err
	}
	if schema != domainRecordSchema && schema != sourceRecordSchema {
		return nil, errors.New("unsupported source record schema")
	}
	if uint64(len(outboxes)) > model.LimitsV1().MaxDerivedDeliveries {
		return nil, errors.New("too many source outboxes")
	}
	w := newRecordWriter()
	w.u16(schema)
	w.task(cursor.Source)
	w.u64(cursor.NextSequence)
	w.u64(cursor.EOF)
	w.u64(cursor.Watermark)
	w.u64(cursor.RaftIndex)
	if schema == sourceRecordSchema {
		w.u64(cursor.CheckpointRevision)
		w.u64(cursor.CheckpointAuthority.JobControlRevision)
		w.u64(cursor.CheckpointAuthority.AssignmentRevision)
		w.fixed32(cursor.CheckpointAuthority.AssignmentDigest)
		w.token(cursor.CheckpointAuthority.SourceToken)
		w.epoch(cursor.CheckpointAuthority.CoordinatorEpoch)
	}
	w.u16(uint16(len(outboxes)))
	for _, outbox := range outboxes {
		b, e := encodeOutbox(outbox)
		if e != nil {
			return nil, e
		}
		w.blob(b)
	}
	return w.bytes(), nil
}
func decodeSource(payload []byte) (SourceCursor, []OutboxRecord, error) {
	r := newRecordReader(payload)
	schema, err := r.u16()
	if err != nil || schema != domainRecordSchema && schema != sourceRecordSchema {
		return SourceCursor{}, nil, errors.New("unsupported source record schema")
	}
	var cursor SourceCursor
	if cursor.Source, err = r.task(); err != nil {
		return cursor, nil, err
	}
	if cursor.NextSequence, err = r.u64(); err != nil {
		return cursor, nil, err
	}
	if cursor.EOF, err = r.u64(); err != nil {
		return cursor, nil, err
	}
	if cursor.Watermark, err = r.u64(); err != nil {
		return cursor, nil, err
	}
	if cursor.RaftIndex, err = r.u64(); err != nil {
		return cursor, nil, err
	}
	if schema == sourceRecordSchema {
		if cursor.CheckpointRevision, err = r.u64(); err != nil {
			return cursor, nil, err
		}
		if cursor.CheckpointAuthority.JobControlRevision, err = r.u64(); err != nil {
			return cursor, nil, err
		}
		if cursor.CheckpointAuthority.AssignmentRevision, err = r.u64(); err != nil {
			return cursor, nil, err
		}
		if cursor.CheckpointAuthority.AssignmentDigest, err = r.fixed32(); err != nil {
			return cursor, nil, err
		}
		if cursor.CheckpointAuthority.SourceToken, err = r.token(); err != nil {
			return cursor, nil, err
		}
		if cursor.CheckpointAuthority.CoordinatorEpoch, err = r.epoch(); err != nil {
			return cursor, nil, err
		}
	}
	count, err := r.u16()
	if err != nil {
		return cursor, nil, err
	}
	if uint64(count) > model.LimitsV1().MaxDerivedDeliveries || r.remaining() < int(count)*4 {
		return cursor, nil, errors.New("source outbox collection exceeds bounds or remaining bytes")
	}
	outboxes := make([]OutboxRecord, int(count))
	for i := range outboxes {
		b, e := r.blob(MaxRecordPayloadBytes)
		if e != nil {
			return cursor, nil, e
		}
		outboxes[i], e = decodeOutbox(b)
		if e != nil {
			return cursor, nil, e
		}
	}
	if !r.done() {
		return cursor, nil, errors.New("trailing source bytes")
	}
	return cursor, outboxes, nil
}

type recordWriter struct{ data []byte }

func newRecordWriter() *recordWriter  { return &recordWriter{data: make([]byte, 0, 256)} }
func (w *recordWriter) bytes() []byte { return append([]byte(nil), w.data...) }
func (w *recordWriter) u8(v uint8)    { w.data = append(w.data, v) }
func (w *recordWriter) u16(v uint16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	w.data = append(w.data, b[:]...)
}
func (w *recordWriter) u32(v uint32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	w.data = append(w.data, b[:]...)
}
func (w *recordWriter) u64(v uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	w.data = append(w.data, b[:]...)
}
func (w *recordWriter) fixed16(v [16]byte)  { w.data = append(w.data, v[:]...) }
func (w *recordWriter) fixed32(v [32]byte)  { w.data = append(w.data, v[:]...) }
func (w *recordWriter) blob(v []byte)       { w.u32(uint32(len(v))); w.data = append(w.data, v...) }
func (w *recordWriter) job(v model.JobID)   { w.fixed16([16]byte(v)) }
func (w *recordWriter) task(v model.TaskID) { w.job(v.JobID); w.u16(v.StageID); w.u16(v.Partition) }
func (w *recordWriter) tupleID(v model.TupleID) {
	w.job(v.JobID)
	w.task(v.SourceTask)
	w.u64(v.SourceSequence)
	w.fixed32(v.PathDigest)
}
func (w *recordWriter) deliveryID(v model.DeliveryID) {
	w.tupleID(v.Tuple)
	w.u16(v.EdgeID)
	w.task(v.DestinationTask)
}
func (w *recordWriter) epoch(v model.CoordinatorEpoch) {
	w.u64(v.Term)
	w.u64(v.BeginIndex)
	w.u16(v.Coordinator)
	w.fixed16(v.Nonce)
}
func (w *recordWriter) token(v model.AssignmentToken) {
	w.task(v.Task)
	w.u16(v.WorkerID)
	w.fixed16([16]byte(v.WorkerEpoch))
	w.u64(v.Attempt)
	w.fixed32(v.SpecificationHash)
	w.u64(v.AssignmentRevision)
}
func (w *recordWriter) replica(v model.ResultReplicaSet) {
	w.task(v.SinkTask)
	w.u16(v.PrimaryNodeID)
	w.u16(v.SecondaryNodeID)
	w.fixed16([16]byte(v.PrimaryEpoch))
	w.fixed16([16]byte(v.SecondaryEpoch))
}
func (w *recordWriter) completion(v model.CompletionReport) {
	w.job(v.JobID)
	w.u64(v.JobControlRevision)
	w.u64(v.AssignmentRevision)
	w.task(v.Source)
	w.token(v.Token)
	w.epoch(v.Epoch)
	w.u64(v.ExpectedCheckpointRevision)
	w.u64(v.Prior)
	w.u64(v.New)
	w.u64(v.EOF)
	w.u64(v.WorkerTransactionID)
	w.fixed32(v.Digest)
}
func (w *recordWriter) failure(v model.JobFailureReport) {
	w.job(v.JobID)
	w.u64(v.JobControlRevision)
	w.u64(v.AssignmentRevision)
	w.token(v.Task)
	w.epoch(v.Epoch)
	w.u64(v.TransactionID)
	w.u16(uint16(v.Code))
	w.fixed32(v.DetailDigest)
}
func (w *recordWriter) repairDefinition(v model.RepairResultPartitionDefinition) {
	w.fixed16(v.RepairID)
	w.epoch(v.CoordinatorEpoch)
	w.job(v.JobID)
	w.u64(v.AssignmentRevision)
	w.fixed32(v.AssignmentDigest)
	w.u16(v.SourceNodeID)
	w.fixed16([16]byte(v.SourceWorkerEpoch))
	w.u16(v.DestinationNodeID)
	w.fixed16([16]byte(v.DestinationWorkerEpoch))
	w.task(v.SinkTask)
	w.fixed32(v.SpecificationHash)
	w.u16(uint16(len(v.Checkpoints)))
	for _, c := range v.Checkpoints {
		w.task(c.Source)
		w.u64(c.Watermark)
	}
	w.fixed32(v.CheckpointDigest)
	w.fixed32(v.InventoryQueryDigest)
	w.u64(v.ExpectedRecordCount)
	w.u64(v.ExpectedTotalBytes)
	w.fixed32(v.ExpectedContentDigest)
}

type recordReader struct {
	data   []byte
	offset int
}

func newRecordReader(v []byte) *recordReader { return &recordReader{data: v} }
func (r *recordReader) remaining() int       { return len(r.data) - r.offset }
func (r *recordReader) done() bool           { return r.offset == len(r.data) }
func (r *recordReader) take(n int) ([]byte, error) {
	if n < 0 || r.remaining() < n {
		return nil, errors.New("truncated domain record")
	}
	v := r.data[r.offset : r.offset+n]
	r.offset += n
	return v, nil
}
func (r *recordReader) u8() (uint8, error) {
	b, e := r.take(1)
	if e != nil {
		return 0, e
	}
	return b[0], nil
}
func (r *recordReader) u16() (uint16, error) {
	b, e := r.take(2)
	if e != nil {
		return 0, e
	}
	return binary.BigEndian.Uint16(b), nil
}
func (r *recordReader) u32() (uint32, error) {
	b, e := r.take(4)
	if e != nil {
		return 0, e
	}
	return binary.BigEndian.Uint32(b), nil
}
func (r *recordReader) u64() (uint64, error) {
	b, e := r.take(8)
	if e != nil {
		return 0, e
	}
	return binary.BigEndian.Uint64(b), nil
}
func (r *recordReader) fixed16() ([16]byte, error) {
	b, e := r.take(16)
	var v [16]byte
	copy(v[:], b)
	return v, e
}
func (r *recordReader) fixed32() ([32]byte, error) {
	b, e := r.take(32)
	var v [32]byte
	copy(v[:], b)
	return v, e
}
func (r *recordReader) blob(max uint64) ([]byte, error) {
	n, e := r.u32()
	if e != nil {
		return nil, e
	}
	if uint64(n) > max {
		return nil, errors.New("domain blob exceeds bound")
	}
	b, e := r.take(int(n))
	if e != nil {
		return nil, e
	}
	return append([]byte(nil), b...), nil
}
func (r *recordReader) schema() error {
	v, e := r.u16()
	if e != nil || v != domainRecordSchema {
		return errors.New("unsupported domain record schema")
	}
	return nil
}
func (r *recordReader) job() (model.JobID, error) { v, e := r.fixed16(); return model.JobID(v), e }
func (r *recordReader) task() (model.TaskID, error) {
	job, e := r.job()
	if e != nil {
		return model.TaskID{}, e
	}
	stage, e := r.u16()
	if e != nil {
		return model.TaskID{}, e
	}
	partition, e := r.u16()
	return model.TaskID{JobID: job, StageID: stage, Partition: partition}, e
}
func (r *recordReader) tupleID() (model.TupleID, error) {
	job, e := r.job()
	if e != nil {
		return model.TupleID{}, e
	}
	source, e := r.task()
	if e != nil {
		return model.TupleID{}, e
	}
	seq, e := r.u64()
	if e != nil {
		return model.TupleID{}, e
	}
	digest, e := r.fixed32()
	return model.TupleID{JobID: job, SourceTask: source, SourceSequence: seq, PathDigest: digest}, e
}
func (r *recordReader) deliveryID() (model.DeliveryID, error) {
	tuple, e := r.tupleID()
	if e != nil {
		return model.DeliveryID{}, e
	}
	edge, e := r.u16()
	if e != nil {
		return model.DeliveryID{}, e
	}
	task, e := r.task()
	return model.DeliveryID{Tuple: tuple, EdgeID: edge, DestinationTask: task}, e
}
func (r *recordReader) epoch() (model.CoordinatorEpoch, error) {
	term, e := r.u64()
	if e != nil {
		return model.CoordinatorEpoch{}, e
	}
	index, e := r.u64()
	if e != nil {
		return model.CoordinatorEpoch{}, e
	}
	node, e := r.u16()
	if e != nil {
		return model.CoordinatorEpoch{}, e
	}
	nonce, e := r.fixed16()
	return model.CoordinatorEpoch{Term: term, BeginIndex: index, Coordinator: node, Nonce: nonce}, e
}
func (r *recordReader) token() (model.AssignmentToken, error) {
	task, e := r.task()
	if e != nil {
		return model.AssignmentToken{}, e
	}
	node, e := r.u16()
	if e != nil {
		return model.AssignmentToken{}, e
	}
	epoch, e := r.fixed16()
	if e != nil {
		return model.AssignmentToken{}, e
	}
	attempt, e := r.u64()
	if e != nil {
		return model.AssignmentToken{}, e
	}
	hash, e := r.fixed32()
	if e != nil {
		return model.AssignmentToken{}, e
	}
	revision, e := r.u64()
	return model.AssignmentToken{Task: task, WorkerID: node, WorkerEpoch: model.WorkerEpoch(epoch), Attempt: attempt, SpecificationHash: hash, AssignmentRevision: revision}, e
}
func (r *recordReader) replica() (model.ResultReplicaSet, error) {
	task, e := r.task()
	if e != nil {
		return model.ResultReplicaSet{}, e
	}
	p, e := r.u16()
	if e != nil {
		return model.ResultReplicaSet{}, e
	}
	s, e := r.u16()
	if e != nil {
		return model.ResultReplicaSet{}, e
	}
	pe, e := r.fixed16()
	if e != nil {
		return model.ResultReplicaSet{}, e
	}
	se, e := r.fixed16()
	return model.ResultReplicaSet{SinkTask: task, PrimaryNodeID: p, SecondaryNodeID: s, PrimaryEpoch: model.WorkerEpoch(pe), SecondaryEpoch: model.WorkerEpoch(se)}, e
}
func (r *recordReader) completion() (model.CompletionReport, error) {
	var v model.CompletionReport
	var e error
	if v.JobID, e = r.job(); e != nil {
		return v, e
	}
	if v.JobControlRevision, e = r.u64(); e != nil {
		return v, e
	}
	if v.AssignmentRevision, e = r.u64(); e != nil {
		return v, e
	}
	if v.Source, e = r.task(); e != nil {
		return v, e
	}
	if v.Token, e = r.token(); e != nil {
		return v, e
	}
	if v.Epoch, e = r.epoch(); e != nil {
		return v, e
	}
	if v.ExpectedCheckpointRevision, e = r.u64(); e != nil {
		return v, e
	}
	if v.Prior, e = r.u64(); e != nil {
		return v, e
	}
	if v.New, e = r.u64(); e != nil {
		return v, e
	}
	if v.EOF, e = r.u64(); e != nil {
		return v, e
	}
	if v.WorkerTransactionID, e = r.u64(); e != nil {
		return v, e
	}
	v.Digest, e = r.fixed32()
	return v, e
}
func (r *recordReader) failure() (model.JobFailureReport, error) {
	var v model.JobFailureReport
	var e error
	if v.JobID, e = r.job(); e != nil {
		return v, e
	}
	if v.JobControlRevision, e = r.u64(); e != nil {
		return v, e
	}
	if v.AssignmentRevision, e = r.u64(); e != nil {
		return v, e
	}
	if v.Task, e = r.token(); e != nil {
		return v, e
	}
	if v.Epoch, e = r.epoch(); e != nil {
		return v, e
	}
	if v.TransactionID, e = r.u64(); e != nil {
		return v, e
	}
	code, e := r.u16()
	if e != nil {
		return v, e
	}
	v.Code = model.FailureCode(code)
	v.DetailDigest, e = r.fixed32()
	return v, e
}
func (r *recordReader) repairDefinition() (model.RepairResultPartitionDefinition, error) {
	var v model.RepairResultPartitionDefinition
	var e error
	if v.RepairID, e = r.fixed16(); e != nil {
		return v, e
	}
	if v.CoordinatorEpoch, e = r.epoch(); e != nil {
		return v, e
	}
	if v.JobID, e = r.job(); e != nil {
		return v, e
	}
	if v.AssignmentRevision, e = r.u64(); e != nil {
		return v, e
	}
	if v.AssignmentDigest, e = r.fixed32(); e != nil {
		return v, e
	}
	if v.SourceNodeID, e = r.u16(); e != nil {
		return v, e
	}
	source, e := r.fixed16()
	if e != nil {
		return v, e
	}
	v.SourceWorkerEpoch = model.WorkerEpoch(source)
	if v.DestinationNodeID, e = r.u16(); e != nil {
		return v, e
	}
	destination, e := r.fixed16()
	if e != nil {
		return v, e
	}
	v.DestinationWorkerEpoch = model.WorkerEpoch(destination)
	if v.SinkTask, e = r.task(); e != nil {
		return v, e
	}
	if v.SpecificationHash, e = r.fixed32(); e != nil {
		return v, e
	}
	count, e := r.u16()
	if e != nil {
		return v, e
	}
	if count > model.WorkerControlMaxCheckpointsV1 {
		return v, errors.New("repair checkpoints exceed bound")
	}
	if r.remaining() < int(count)*28+112 {
		return v, errors.New("repair checkpoints exceed remaining bytes")
	}
	v.Checkpoints = make([]model.SourceCheckpoint, int(count))
	for i := range v.Checkpoints {
		if v.Checkpoints[i].Source, e = r.task(); e != nil {
			return v, e
		}
		if v.Checkpoints[i].Watermark, e = r.u64(); e != nil {
			return v, e
		}
	}
	if v.CheckpointDigest, e = r.fixed32(); e != nil {
		return v, e
	}
	if v.InventoryQueryDigest, e = r.fixed32(); e != nil {
		return v, e
	}
	if v.ExpectedRecordCount, e = r.u64(); e != nil {
		return v, e
	}
	if v.ExpectedTotalBytes, e = r.u64(); e != nil {
		return v, e
	}
	v.ExpectedContentDigest, e = r.fixed32()
	return v, e
}

func cloneTuple(v model.Tuple) model.Tuple {
	result := model.Tuple{Fields: make([]model.Field, len(v.Fields))}
	for i, f := range v.Fields {
		result.Fields[i] = f
		result.Fields[i].Value.Bytes = append([]byte(nil), f.Value.Bytes...)
	}
	return result
}
func cloneTuples(v []model.Tuple) []model.Tuple {
	result := make([]model.Tuple, len(v))
	for i := range v {
		result[i] = cloneTuple(v[i])
	}
	return result
}
func cloneEvent(v model.WorkerEvent) model.WorkerEvent {
	result := v
	if v.Completion != nil {
		x := *v.Completion
		result.Completion = &x
	}
	if v.Failure != nil {
		x := *v.Failure
		result.Failure = &x
	}
	return result
}
func cloneRepair(v ResultRepairRecord) ResultRepairRecord {
	v.Instruction.Checkpoints = append([]model.SourceCheckpoint(nil), v.Instruction.Checkpoints...)
	return v
}
func cloneInstalled(v InstalledAssignment) InstalledAssignment {
	v.Assignment.Tasks = append([]model.AssignmentToken(nil), v.Assignment.Tasks...)
	v.Assignment.ResultReplicas = append([]model.ResultReplicaSet(nil), v.Assignment.ResultReplicas...)
	v.SpecificationBytes = append([]byte(nil), v.SpecificationBytes...)
	v.Topology, _ = model.DecodeTopology(v.SpecificationBytes)
	return v
}

// rebindableResultProvenance reports whether an incoming result may re-bind
// the copy provenance of a retained prior copy: the logical record must be
// byte-identical and the prior provenance strictly historical against the
// incoming (already current-validated) one — a lower assignment revision, or
// the same revision/replica set/role under a coordinator epoch ordered
// strictly before. Any other difference is an identity reuse.
func rebindableResultProvenance(prior, incoming StoredResult) bool {
	priorBytes := prior.canonical
	if priorBytes == nil {
		priorBytes, _ = model.MarshalResultRecord(prior.Record)
	}
	incomingBytes := incoming.canonical
	if incomingBytes == nil {
		incomingBytes, _ = model.MarshalResultRecord(incoming.Record)
	}
	if len(priorBytes) == 0 || !bytes.Equal(priorBytes, incomingBytes) {
		return false
	}
	return ResultProvenanceOrderedBefore(prior.Provenance, incoming.Provenance)
}

// ResultProvenanceOrderedBefore reports whether prior is a strictly superseded
// copy envelope of current: a lower assignment revision, or the identical
// revision, digest, replica set and role under a coordinator epoch ordered
// strictly before current's.
func ResultProvenanceOrderedBefore(prior, current model.ResultCopyProvenance) bool {
	if prior.AssignmentRevision < current.AssignmentRevision {
		return true
	}
	return prior.AssignmentRevision == current.AssignmentRevision && prior.AssignmentDigest == current.AssignmentDigest &&
		prior.ReplicaSet == current.ReplicaSet && prior.DestinationRole == current.DestinationRole &&
		compareEpochOrder(prior.CoordinatorEpoch, current.CoordinatorEpoch) < 0
}

// replaceResultNode returns a path-copied tree in which the node holding key
// carries value; shape and heights are unchanged. The key must exist.
func replaceResultNode(root *resultNode, key resultKey, value StoredResult) *resultNode {
	if root == nil {
		return nil
	}
	copyRoot := *root
	switch comparison := compareResultKey(key, root.key); {
	case comparison < 0:
		copyRoot.left = replaceResultNode(root.left, key, value)
	case comparison > 0:
		copyRoot.right = replaceResultNode(root.right, key, value)
	default:
		copyRoot.value = value
	}
	return &copyRoot
}

func compareEpochOrder(a, b model.CoordinatorEpoch) int {
	if a.Term < b.Term {
		return -1
	}
	if a.Term > b.Term {
		return 1
	}
	if a.BeginIndex < b.BeginIndex {
		return -1
	}
	if a.BeginIndex > b.BeginIndex {
		return 1
	}
	return 0
}
func assignmentIndex(v []InstalledAssignment, id model.JobID) int {
	for i := range v {
		if v[i].Assignment.JobID == id {
			return i
		}
	}
	return -1
}
func findAssignment(w *RecoveredWork, id model.JobID) (InstalledAssignment, bool) {
	i := assignmentIndex(w.Assignments, id)
	if i < 0 {
		return InstalledAssignment{}, false
	}
	return w.Assignments[i], true
}
func deliveryIndex(v []DeliveryRecord, id model.DeliveryID) int {
	for i := range v {
		if v[i].ID == id {
			return i
		}
	}
	return -1
}
func outboxIndex(v []OutboxRecord, id model.DeliveryID) int {
	for i := range v {
		if v[i].ID == id {
			return i
		}
	}
	return -1
}
func sourceIndex(v []SourceCursor, id model.TaskID) int {
	for i := range v {
		if v[i].Source == id {
			return i
		}
	}
	return -1
}

func checkpointObservationIndex(v []CommittedCheckpoint, id model.TaskID) int {
	for i := range v {
		if v[i].Notice.Source == id {
			return i
		}
	}
	return -1
}

func assignmentTargetsWorker(assignment model.AssignmentSet, nodeID uint16, workerEpoch model.WorkerEpoch) bool {
	for _, token := range assignment.Tasks {
		if token.WorkerID == nodeID && token.WorkerEpoch == workerEpoch {
			return true
		}
	}
	for _, replica := range assignment.ResultReplicas {
		if replica.PrimaryNodeID == nodeID && replica.PrimaryEpoch == workerEpoch || replica.SecondaryNodeID == nodeID && replica.SecondaryEpoch == workerEpoch {
			return true
		}
	}
	return false
}
func ensureWorkIndexes(work *RecoveredWork) error {
	if work.indexes != nil {
		return nil
	}
	indexes := &workIndexes{resultBytesByJob: make(map[model.JobID]uint64)}
	for index := range work.Results {
		result := work.Results[index]
		if result.canonical == nil {
			encoded, err := model.MarshalResultRecord(result.Record)
			if err != nil {
				return err
			}
			result.canonical = encoded
		}
		key := resultKey{SinkTask: result.Record.SinkTask, TupleID: result.Record.TupleID}
		if findResultNode(indexes.results, key) != nil {
			return model.ErrIdentityReuse
		}
		if indexes.resultCount >= maxStoredResultCount() {
			return ErrCapacity
		}
		inserted, err := insertResultNode(indexes.results, &resultNode{key: key, value: result, height: 1})
		if err != nil {
			return err
		}
		indexes.results = inserted
		indexes.resultCount++
		entryBytes, err := resultArtifactEntryBytes(uint64(len(result.canonical)))
		if err != nil {
			return err
		}
		prior := indexes.resultBytesByJob[result.Record.TupleID.JobID]
		if prior > math.MaxUint64-entryBytes {
			return ErrCapacity
		}
		indexes.resultBytesByJob[result.Record.TupleID.JobID] = prior + entryBytes
	}
	work.Results = nil
	work.indexes = indexes
	return nil
}

func cloneWorkIndexes(indexes *workIndexes) *workIndexes {
	if indexes == nil {
		return nil
	}
	result := &workIndexes{results: indexes.results, resultBytesByJob: make(map[model.JobID]uint64, len(indexes.resultBytesByJob)), resultCount: indexes.resultCount}
	for job, total := range indexes.resultBytesByJob {
		result.resultBytesByJob[job] = total
	}
	return result
}

func compareResultKey(a, b resultKey) int {
	if a.SinkTask != b.SinkTask {
		if taskLess(a.SinkTask, b.SinkTask) {
			return -1
		}
		return 1
	}
	if a.TupleID == b.TupleID {
		return 0
	}
	if tupleLess(a.TupleID, b.TupleID) {
		return -1
	}
	return 1
}

func findResultNode(root *resultNode, key resultKey) *resultNode {
	for root != nil {
		switch comparison := compareResultKey(key, root.key); {
		case comparison < 0:
			root = root.left
		case comparison > 0:
			root = root.right
		default:
			return root
		}
	}
	return nil
}

func insertResultNode(root, inserted *resultNode) (*resultNode, error) {
	if root == nil {
		return inserted, nil
	}
	copyRoot := *root
	comparison := compareResultKey(inserted.key, root.key)
	if comparison == 0 {
		return nil, model.ErrIdentityReuse
	}
	var err error
	if comparison < 0 {
		copyRoot.left, err = insertResultNode(root.left, inserted)
	} else {
		copyRoot.right, err = insertResultNode(root.right, inserted)
	}
	if err != nil {
		return nil, err
	}
	return rebalanceResultNode(&copyRoot)
}

func rebalanceResultNode(node *resultNode) (*resultNode, error) {
	if err := updateResultNodeHeight(node); err != nil {
		return nil, err
	}
	balance := int(resultNodeHeight(node.left)) - int(resultNodeHeight(node.right))
	if balance > 1 {
		if resultNodeHeight(node.left.left) < resultNodeHeight(node.left.right) {
			rotated, err := rotateResultNodeLeft(node.left)
			if err != nil {
				return nil, err
			}
			node.left = rotated
		}
		return rotateResultNodeRight(node)
	}
	if balance < -1 {
		if resultNodeHeight(node.right.right) < resultNodeHeight(node.right.left) {
			rotated, err := rotateResultNodeRight(node.right)
			if err != nil {
				return nil, err
			}
			node.right = rotated
		}
		return rotateResultNodeLeft(node)
	}
	return node, nil
}

func rotateResultNodeRight(root *resultNode) (*resultNode, error) {
	if root == nil || root.left == nil {
		return nil, errors.New("invalid result index right rotation")
	}
	newRoot, moved := *root.left, root.left.right
	oldRoot := *root
	oldRoot.left = moved
	if err := updateResultNodeHeight(&oldRoot); err != nil {
		return nil, err
	}
	newRoot.right = &oldRoot
	if err := updateResultNodeHeight(&newRoot); err != nil {
		return nil, err
	}
	return &newRoot, nil
}

func rotateResultNodeLeft(root *resultNode) (*resultNode, error) {
	if root == nil || root.right == nil {
		return nil, errors.New("invalid result index left rotation")
	}
	newRoot, moved := *root.right, root.right.left
	oldRoot := *root
	oldRoot.right = moved
	if err := updateResultNodeHeight(&oldRoot); err != nil {
		return nil, err
	}
	newRoot.left = &oldRoot
	if err := updateResultNodeHeight(&newRoot); err != nil {
		return nil, err
	}
	return &newRoot, nil
}

func resultNodeHeight(node *resultNode) uint16 {
	if node == nil {
		return 0
	}
	return node.height
}

func updateResultNodeHeight(node *resultNode) error {
	height := resultNodeHeight(node.left)
	if right := resultNodeHeight(node.right); right > height {
		height = right
	}
	if height == math.MaxUint16 {
		return ErrCapacity
	}
	node.height = height + 1
	return nil
}

func maxStoredResultCount() uint64 {
	jobs := model.LimitsV1().MaxRetainedJobs
	if jobs > math.MaxUint64/model.ResultArtifactMaxRecordCountV1 {
		return math.MaxUint64
	}
	return jobs * model.ResultArtifactMaxRecordCountV1
}

const resultArtifactEntryPrefixBytes uint64 = 4

func resultArtifactEntryBytes(logicalBytes uint64) (uint64, error) {
	entryBytes, ok := checkedAdd(logicalBytes, resultArtifactEntryPrefixBytes)
	if !ok || entryBytes < model.ResultArtifactMinRecordBytesV1 || entryBytes > model.ResultArtifactMaxRecordBytesV1 {
		return 0, errors.New("result logical bytes outside artifact entry bounds")
	}
	return entryBytes, nil
}

func appendOwnedResults(indexes *workIndexes, destination *[]StoredResult) {
	if indexes == nil {
		return
	}
	stack := make([]*resultNode, 0, resultNodeHeight(indexes.results))
	current := indexes.results
	for current != nil || len(stack) != 0 {
		for current != nil {
			stack = append(stack, current)
			current = current.left
		}
		current = stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		owned := current.value
		owned.Record.Value = append([]byte(nil), owned.Record.Value...)
		owned.canonical = nil
		*destination = append(*destination, owned)
		current = current.right
	}
}

func visitResults(work RecoveredWork, visit func(StoredResult) bool) bool {
	if work.indexes == nil {
		for _, result := range work.Results {
			if !visit(result) {
				return false
			}
		}
		return true
	}
	stack := make([]*resultNode, 0, resultNodeHeight(work.indexes.results))
	current := work.indexes.results
	for current != nil || len(stack) != 0 {
		for current != nil {
			stack, current = append(stack, current), current.left
		}
		current = stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if !visit(current.value) {
			return false
		}
		current = current.right
	}
	return true
}
func repairIndex(v []ResultRepairRecord, id [16]byte) int {
	for i := range v {
		if v[i].Instruction.RepairID == id {
			return i
		}
	}
	return -1
}
func containsToken(set model.AssignmentSet, token model.AssignmentToken) bool {
	for _, v := range set.Tasks {
		if v == token {
			return true
		}
	}
	return false
}
func findReplica(set model.AssignmentSet, task model.TaskID) (model.ResultReplicaSet, bool) {
	for _, v := range set.ResultReplicas {
		if v.SinkTask == task {
			return v, true
		}
	}
	return model.ResultReplicaSet{}, false
}
func findToken(set model.AssignmentSet, task model.TaskID) (model.AssignmentToken, bool) {
	for _, token := range set.Tasks {
		if token.Task == task {
			return token, true
		}
	}
	return model.AssignmentToken{}, false
}
func equalInstalledAssignment(a, b InstalledAssignment) bool {
	return equalInstalledAssignmentContent(a, b) && a.SchedulingState == b.SchedulingState && a.CoordinatorEpoch == b.CoordinatorEpoch
}
func equalInstalledAssignmentContent(a, b InstalledAssignment) bool {
	return a.Assignment.JobID == b.Assignment.JobID && a.Assignment.Revision == b.Assignment.Revision && a.Assignment.Digest == b.Assignment.Digest && a.JobControlRevision == b.JobControlRevision && bytes.Equal(a.SpecificationBytes, b.SpecificationBytes) && equalTokens(a.Assignment.Tasks, b.Assignment.Tasks) && equalReplicas(a.Assignment.ResultReplicas, b.Assignment.ResultReplicas)
}

// admissionSchedulingProgression reports whether one scheduling change at an
// equal fence and revision is one of the two admitted worker-local admission
// progressions of the current coordinator's install protocol.
func admissionSchedulingProgression(prior, incoming model.SchedulingState) bool {
	return prior == model.Running && incoming == model.Closed || prior == model.Closed && incoming == model.Running
}
func equalTokens(a, b []model.AssignmentToken) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
func equalReplicas(a, b []model.ResultReplicaSet) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
func equalDeliveryDefinition(a, b DeliveryRecord) bool {
	if a.definitionDigest != ([32]byte{}) {
		digest := b.definitionDigest
		if digest == ([32]byte{}) {
			digest, _ = deliveryDefinitionDigest(b)
		}
		return digest == a.definitionDigest
	}
	if a.ID != b.ID || a.Producer != b.Producer || a.Destination != b.Destination || a.AssignmentRevision != b.AssignmentRevision || a.AssignmentDigest != b.AssignmentDigest || a.CoordinatorEpoch != b.CoordinatorEpoch || a.Reservation != b.Reservation {
		return false
	}
	aa, _ := model.MarshalTuple(a.Tuple)
	bb, _ := model.MarshalTuple(b.Tuple)
	return bytes.Equal(aa, bb)
}

func equalCompactedLogicalDefinition(a, b DeliveryRecord) bool {
	if a.ID != b.ID || a.Producer.Task != b.Producer.Task || a.Destination.Task != b.Destination.Task || a.Producer.SpecificationHash != b.Producer.SpecificationHash || a.Destination.SpecificationHash != b.Destination.SpecificationHash || a.Reservation != b.Reservation {
		return false
	}
	aa, err := model.MarshalTuple(a.Tuple)
	if err != nil {
		return false
	}
	bb, err := model.MarshalTuple(b.Tuple)
	return err == nil && bytes.Equal(aa, bb)
}
func equalTuples(a, b []model.Tuple) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		aa, _ := model.MarshalTuple(a[i])
		bb, _ := model.MarshalTuple(b[i])
		if !bytes.Equal(aa, bb) {
			return false
		}
	}
	return true
}
func equalOutboxDefinition(a, b OutboxRecord) bool {
	if a.ID != b.ID || a.Producer != b.Producer || a.Destination != b.Destination || a.AssignmentRevision != b.AssignmentRevision || a.AssignmentDigest != b.AssignmentDigest || a.CoordinatorEpoch != b.CoordinatorEpoch {
		return false
	}
	aa, _ := model.MarshalTuple(a.Tuple)
	bb, _ := model.MarshalTuple(b.Tuple)
	return bytes.Equal(aa, bb)
}
func equalStoredResult(a, b StoredResult) bool {
	aa := a.canonical
	if aa == nil {
		aa, _ = model.MarshalResultRecord(a.Record)
	}
	bb := b.canonical
	if bb == nil {
		bb, _ = model.MarshalResultRecord(b.Record)
	}
	return bytes.Equal(aa, bb) && a.Provenance == b.Provenance
}
func equalRepairInstruction(a, b ResultRepairRecord) bool {
	x, y := a.Instruction, b.Instruction
	if x.RepairID != y.RepairID || x.CoordinatorEpoch != y.CoordinatorEpoch || x.JobID != y.JobID || x.AssignmentRevision != y.AssignmentRevision || x.AssignmentDigest != y.AssignmentDigest || x.SourceNodeID != y.SourceNodeID || x.SourceWorkerEpoch != y.SourceWorkerEpoch || x.DestinationNodeID != y.DestinationNodeID || x.DestinationWorkerEpoch != y.DestinationWorkerEpoch || x.SinkTask != y.SinkTask || x.SpecificationHash != y.SpecificationHash || x.CheckpointDigest != y.CheckpointDigest || x.InventoryQueryDigest != y.InventoryQueryDigest || x.ExpectedRecordCount != y.ExpectedRecordCount || x.ExpectedTotalBytes != y.ExpectedTotalBytes || x.ExpectedContentDigest != y.ExpectedContentDigest || len(x.Checkpoints) != len(y.Checkpoints) {
		return false
	}
	for index := range x.Checkpoints {
		if x.Checkpoints[index] != y.Checkpoints[index] {
			return false
		}
	}
	return a.InstructionDigest == b.InstructionDigest && a.Role == b.Role
}
func taskLess(a, b model.TaskID) bool {
	if c := bytes.Compare(a.JobID[:], b.JobID[:]); c != 0 {
		return c < 0
	}
	if a.StageID != b.StageID {
		return a.StageID < b.StageID
	}
	return a.Partition < b.Partition
}
func tupleLess(a, b model.TupleID) bool {
	if c := bytes.Compare(a.JobID[:], b.JobID[:]); c != 0 {
		return c < 0
	}
	if a.SourceTask != b.SourceTask {
		return taskLess(a.SourceTask, b.SourceTask)
	}
	if a.SourceSequence != b.SourceSequence {
		return a.SourceSequence < b.SourceSequence
	}
	return bytes.Compare(a.PathDigest[:], b.PathDigest[:]) < 0
}
func deliveryIDLess(a, b model.DeliveryID) bool {
	if a.Tuple != b.Tuple {
		return tupleLess(a.Tuple, b.Tuple)
	}
	if a.EdgeID != b.EdgeID {
		return a.EdgeID < b.EdgeID
	}
	return taskLess(a.DestinationTask, b.DestinationTask)
}
func outboxesCanonical(outboxes []OutboxRecord) bool {
	for index := 1; index < len(outboxes); index++ {
		if !deliveryIDLess(outboxes[index-1].ID, outboxes[index].ID) {
			return false
		}
	}
	return true
}
func outboxIDsCanonical(ids []model.DeliveryID) bool {
	for index := 1; index < len(ids); index++ {
		if !deliveryIDLess(ids[index-1], ids[index]) {
			return false
		}
	}
	return true
}
func boolByte(v bool) uint8 {
	if v {
		return 1
	}
	return 0
}
