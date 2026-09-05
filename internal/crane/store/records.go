package store

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
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
	// proven carries the owning store's validatedOutboxes set so outbox
	// creation proofs are executed (and recorded) at most once per record
	// even across retries of the same transaction.
	proven   map[model.DeliveryID]struct{}
	prepared [recordCheckpointObservation + 1]bool
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
	return applyDomainRecord(&r.transaction, record, r.allowLegacy, r.proven)
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

func applyDomainRecord(work *RecoveredWork, record Record, allowLegacy bool, proven map[model.DeliveryID]struct{}) error {
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
		return applyDelivery(work, delivery, outboxes, record.Type == recordDeliveryProcessed, proven)
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
		return applySource(work, cursor, outboxes, proven)
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

// RecoverWorkWithin behaves like RecoverWork but gives up with ErrBusy when
// the store lock cannot be taken within wait. The control path uses it so a
// stalled store (a hung disk, or a durable write held at a boundary) fails
// control requests fast instead of piling blocked handlers up to the node's
// connection budget; recovery and execution paths keep the unbounded read.
func (store *Store) RecoverWorkWithin(wait time.Duration) (RecoveredWork, error) {
	if wait <= 0 {
		return store.RecoverWork()
	}
	deadline := time.Now().Add(wait)
	for !store.mu.TryLock() {
		if time.Now().After(deadline) {
			return RecoveredWork{}, fmt.Errorf("%w: %s", ErrBusy, wait)
		}
		time.Sleep(time.Millisecond)
	}
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
	reducer := &workReducer{current: store.work, proven: store.validatedOutboxes}
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
	if err := validateRecoveredWorkLocal(prospective, store.state.Identity.NodeID, store.state.WorkerEpoch, store.validatedOutboxes); err != nil {
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

// validateRecoveredWorkLocal enforces the per-commit invariants of the whole
// recovered set. The outbox walk passes proven so already-proven outboxes pay
// only the cheap structural checks; a nil set (recovery-time validation)
// forces the full proof of every outbox, exactly as before the proof cache
// existed.
func validateRecoveredWorkLocal(work RecoveredWork, nodeID uint16, workerEpoch model.WorkerEpoch, proven map[model.DeliveryID]struct{}) error {
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
		if err := validateSnapshotOutbox(outbox, assignment, work.Fence, proven); err != nil {
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

func taskLess(a, b model.TaskID) bool {
	if c := bytes.Compare(a.JobID[:], b.JobID[:]); c != 0 {
		return c < 0
	}
	if a.StageID != b.StageID {
		return a.StageID < b.StageID
	}
	return a.Partition < b.Partition
}
