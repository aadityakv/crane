package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
)

const domainRecordSchema uint16 = 1

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
)

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
	OutboxIDs []model.DeliveryID
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
}

// StoredResult couples immutable logical result bytes to separate copy provenance.
type StoredResult struct {
	// Record is immutable logical collect output.
	Record model.ResultRecord
	// Provenance separately fences this physical copy.
	Provenance model.ResultCopyProvenance
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
	result.Deliveries = make([]DeliveryRecord, len(work.Deliveries))
	for i := range work.Deliveries {
		result.Deliveries[i] = work.Deliveries[i].Clone()
	}
	result.Outboxes = make([]OutboxRecord, len(work.Outboxes))
	for i := range work.Outboxes {
		result.Outboxes[i] = work.Outboxes[i].Clone()
	}
	result.Results = make([]StoredResult, len(work.Results))
	for i := range work.Results {
		result.Results[i] = work.Results[i]
		result.Results[i].Record.Value = append([]byte(nil), work.Results[i].Record.Value...)
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
	return result
}

type workReducer struct {
	current, transaction RecoveredWork
	inTransaction        bool
}

func newWorkReducer() *workReducer { return &workReducer{current: RecoveredWork{NextTransactionID: 1}} }

// BeginTransaction starts one prospective atomic high-level reduction.
func (r *workReducer) BeginTransaction(uint32) error {
	if r.inTransaction {
		return errors.New("nested transaction")
	}
	r.transaction = r.current.Clone()
	r.inTransaction = true
	return nil
}

// ConsumeRecord validates and applies one canonical record to prospective state.
func (r *workReducer) ConsumeRecord(record Record) error {
	if !r.inTransaction {
		return errors.New("record outside transaction")
	}
	return applyDomainRecord(&r.transaction, record)
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

func applyDomainRecord(work *RecoveredWork, record Record) error {
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
		notice, err := decodeCheckpoint(record.Payload)
		if err != nil {
			return err
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
		through, err := decodeUint64Payload(record.Payload)
		if err != nil {
			return err
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
			if !equalInstalledAssignment(prior, installed) {
				return model.ErrIdentityReuse
			}
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
	assignment, ok := findAssignment(work, delivery.ID.Tuple.JobID)
	if !ok {
		return errors.New("delivery references unknown assignment")
	}
	if err := validateDelivery(delivery, assignment, work.Fence); err != nil {
		return err
	}
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
	if index < 0 || work.Deliveries[index].State != Received {
		return errors.New("processed delivery has no received predecessor")
	}
	if delivery.State != Processed || !equalDeliveryDefinition(work.Deliveries[index], delivery) {
		return errors.New("processed delivery identity changed")
	}
	seen := make(map[model.DeliveryID]struct{}, len(outboxes))
	for _, outbox := range outboxes {
		if _, duplicate := seen[outbox.ID]; duplicate {
			return errors.New("duplicate outbox")
		}
		seen[outbox.ID] = struct{}{}
		if err := validateOutbox(outbox, assignment, work.Fence); err != nil {
			return err
		}
		if outbox.Producer != delivery.Destination {
			return errors.New("outbox producer is not processed destination")
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
		if notice.Watermark >= math.MaxUint64 || notice.Watermark > model.LimitsV1().MaxSourceSequences {
			return errors.New("checkpoint watermark outside v1 bounds")
		}
		work.Sources = append(work.Sources, SourceCursor{Source: notice.Source, NextSequence: notice.Watermark + 1, Watermark: notice.Watermark, RaftIndex: notice.RaftIndex})
	}
	for i := range work.Deliveries {
		delivery := &work.Deliveries[i]
		if delivery.ID.Tuple.SourceTask == notice.Source && delivery.ID.Tuple.SourceSequence <= notice.Watermark {
			if delivery.State != Completed && delivery.State != Compacted {
				return errors.New("checkpoint covers incomplete delivery")
			}
			delivery.State, delivery.Reservation, delivery.Outputs, delivery.OutboxIDs = Compacted, 0, nil, nil
			delivery.Tuple = model.Tuple{}
		}
	}
	kept := work.Outboxes[:0]
	for _, outbox := range work.Outboxes {
		if outbox.ID.Tuple.SourceTask != notice.Source || outbox.ID.Tuple.SourceSequence > notice.Watermark {
			kept = append(kept, outbox)
		}
	}
	work.Outboxes = kept
	return nil
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
	index := resultIndex(work.Results, result.Record.TupleID)
	if index >= 0 {
		if !equalStoredResult(work.Results[index], result) {
			return model.ErrIdentityReuse
		}
		return nil
	}
	var jobBytes uint64
	for _, existing := range work.Results {
		if existing.Record.TupleID.JobID != result.Record.TupleID.JobID {
			continue
		}
		encoded, err := model.MarshalResultRecord(existing.Record)
		if err != nil {
			return err
		}
		if jobBytes > math.MaxUint64-uint64(len(encoded)) {
			return ErrCapacity
		}
		jobBytes += uint64(len(encoded))
	}
	encoded, err := model.MarshalResultRecord(result.Record)
	if err != nil {
		return err
	}
	if jobBytes > model.LimitsV1().MaxResultRecordsBytesPerJob || uint64(len(encoded)) > model.LimitsV1().MaxResultRecordsBytesPerJob-jobBytes {
		return ErrCapacity
	}
	result.Record.Value = append([]byte(nil), result.Record.Value...)
	work.Results = append(work.Results, result)
	sort.Slice(work.Results, func(i, j int) bool {
		a, b := work.Results[i].Record, work.Results[j].Record
		if a.SinkTask != b.SinkTask {
			return taskLess(a.SinkTask, b.SinkTask)
		}
		return bytes.Compare(tupleKey(a.TupleID), tupleKey(b.TupleID)) < 0
	})
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
		index++
	}
	work.PendingEvents = append([]model.WorkerEvent(nil), work.PendingEvents[index:]...)
	return nil
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

func applySource(work *RecoveredWork, cursor SourceCursor, outboxes []OutboxRecord) error {
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
	if cursor.NextSequence == 0 || cursor.NextSequence > model.LimitsV1().MaxSourceSequences+1 || cursor.Watermark > model.LimitsV1().MaxSourceSequences || cursor.EOF > model.LimitsV1().MaxSourceSequences || cursor.EOF != 0 && cursor.NextSequence > cursor.EOF+1 || cursor.Watermark >= cursor.NextSequence {
		return errors.New("source cursor outside bounds")
	}
	index := sourceIndex(work.Sources, cursor.Source)
	if index >= 0 && cursor.NextSequence < work.Sources[index].NextSequence {
		return errors.New("source cursor regression")
	}
	for _, outbox := range outboxes {
		if err := validateOutbox(outbox, assignment, work.Fence); err != nil {
			return err
		}
		if outboxIndex(work.Outboxes, outbox.ID) >= 0 {
			return errors.New("duplicate source outbox")
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
func applyOutboxAck(work *RecoveredWork, id model.DeliveryID) error {
	index := outboxIndex(work.Outboxes, id)
	if index < 0 {
		return errors.New("unknown outbox")
	}
	work.Outboxes[index].Completed = true
	return nil
}

func validateDelivery(record DeliveryRecord, assignment InstalledAssignment, fence model.CoordinatorEpoch) error {
	if record.State < Received || record.State > Compacted || record.AssignmentRevision == 0 || record.AssignmentDigest == ([32]byte{}) {
		return errors.New("invalid delivery metadata")
	}
	if record.CoordinatorEpoch != fence || record.AssignmentRevision != assignment.Assignment.Revision || record.AssignmentDigest != assignment.Assignment.Digest {
		return errors.New("delivery assignment fence mismatch")
	}
	message := protocol.TupleDelivery{DeliveryID: record.ID, Tuple: record.Tuple, Producer: record.Producer, Destination: record.Destination, Assignment: protocol.AssignmentSetIdentity{JobID: record.ID.Tuple.JobID, Revision: record.AssignmentRevision, Digest: record.AssignmentDigest}, Coordinator: record.CoordinatorEpoch}
	if _, err := protocol.MarshalTupleDelivery(message); err != nil {
		return err
	}
	if !containsToken(assignment.Assignment, record.Producer) || !containsToken(assignment.Assignment, record.Destination) {
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
	return nil
}

func validateOutbox(record OutboxRecord, assignment InstalledAssignment, fence model.CoordinatorEpoch) error {
	delivery := DeliveryRecord{ID: record.ID, Tuple: record.Tuple, Producer: record.Producer, Destination: record.Destination, AssignmentRevision: record.AssignmentRevision, AssignmentDigest: record.AssignmentDigest, CoordinatorEpoch: record.CoordinatorEpoch, State: Received}
	if record.CoordinatorEpoch != fence {
		return errors.New("outbox fence mismatch")
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

func validateRepair(repair ResultRepairRecord) error {
	d := repair.Instruction
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
	return store.applyWorkTransaction(Transaction{Records: []Record{{Type: recordFence, Payload: payload}}}, 0)
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
	return store.applyWorkTransaction(Transaction{Records: []Record{{Type: recordAssignment, Payload: payload}}}, 0)
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
	if index := deliveryIndex(store.work.Deliveries, record.ID); index >= 0 {
		prior := store.work.Deliveries[index]
		if prior.State == Compacted {
			return Compacted, nil
		}
		if !equalDeliveryDefinition(prior, record) {
			return 0, model.ErrIdentityReuse
		}
		return prior.State, nil
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
	if err = store.commitWorkLocked(tx, prospective, record.Reservation); err != nil {
		return 0, err
	}
	return Received, nil
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
	record := store.work.Deliveries[index].Clone()
	if record.State == Processed {
		if !equalTuples(record.Outputs, outputs) || len(record.OutboxIDs) != len(outboxes) {
			return model.ErrIdentityReuse
		}
		for outIndex, id := range record.OutboxIDs {
			storedIndex := outboxIndex(store.work.Outboxes, id)
			if storedIndex < 0 || !equalOutboxDefinition(store.work.Outboxes[storedIndex], outboxes[outIndex]) {
				return model.ErrIdentityReuse
			}
		}
		return nil
	}
	if record.State != Received {
		return errors.New("delivery not received")
	}
	record.State = Processed
	if uint64(len(outputs)) > model.LimitsV1().MaxOperatorOutputs || uint64(len(outboxes)) > model.LimitsV1().MaxDerivedDeliveries {
		return errors.New("processed output/outbox count exceeds v1 bounds")
	}
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
	return store.commitWorkLocked(tx, prospective, 0)
}

// MarkCompleted durably closes a processed delivery.
func (store *Store) MarkCompleted(id model.DeliveryID) error {
	payload, err := encodeDeliveryIDPayload(id)
	if err != nil {
		return err
	}
	return store.applyWorkTransaction(Transaction{Records: []Record{{Type: recordDeliveryCompleted, Payload: payload}}}, 0)
}

// ApplyCheckpoint persists a monotonic watermark before compacting covered replay state.
func (store *Store) ApplyCheckpoint(notice model.CheckpointNotice) error {
	payload, err := encodeCheckpoint(notice)
	if err != nil {
		return err
	}
	return store.applyWorkTransaction(Transaction{Records: []Record{{Type: recordCheckpoint, Payload: payload}}}, 0)
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
		err = store.commitWorkLocked(tx, prospective, 0)
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
		err = store.commitWorkLocked(tx, prospective, 0)
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
	return store.applyWorkTransaction(Transaction{Records: []Record{{Type: recordEventAck, Payload: payload}}}, 0)
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
		err = store.commitWorkLocked(tx, prospective, 0)
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
	return store.applyWorkTransaction(Transaction{Records: []Record{{Type: recordSource, Payload: payload}}}, 0)
}

// MarkOutboxCompleted durably records one downstream completion.
func (store *Store) MarkOutboxCompleted(id model.DeliveryID) error {
	payload, err := encodeDeliveryIDPayload(id)
	if err != nil {
		return err
	}
	return store.applyWorkTransaction(Transaction{Records: []Record{{Type: recordOutboxAck, Payload: payload}}}, 0)
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

func (store *Store) applyWorkTransaction(tx Transaction, reservation uint64) error {
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
	return store.commitWorkLocked(tx, prospective, reservation)
}
func (store *Store) reduceWorkLocked(tx Transaction) (RecoveredWork, error) {
	reducer := &workReducer{current: store.work.Clone()}
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
func (store *Store) commitWorkLocked(tx Transaction, prospective RecoveredWork, reservation uint64) error {
	encodedBytes, err := transactionEncodedSize(tx)
	if err != nil {
		return err
	}
	reserved, err := reservedBytes(store.work)
	if err != nil {
		return err
	}
	if reserved > math.MaxUint64-reservation {
		return ErrCapacity
	}
	reserved += reservation
	if store.state.WALBytes > store.options.MaxBytes || encodedBytes > store.options.MaxBytes-store.state.WALBytes || reserved > store.options.MaxBytes-store.state.WALBytes-encodedBytes {
		return ErrCapacity
	}
	if err := store.commitLocked(tx); err != nil {
		return err
	}
	store.work = prospective
	return nil
}

func validateRegisteredTransaction(transaction Transaction) error {
	if err := transaction.Validate(); err != nil {
		return err
	}
	for index, record := range transaction.Records {
		if record.Type < recordFence || record.Type > recordOutboxAck {
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
	for _, delivery := range work.Deliveries {
		if delivery.Destination.WorkerID != nodeID || delivery.Destination.WorkerEpoch != workerEpoch {
			return errors.New("recovered delivery targets another worker incarnation")
		}
	}
	for _, result := range work.Results {
		if !provenanceTargets(result.Provenance, nodeID, workerEpoch) {
			return errors.New("recovered result provenance targets another worker incarnation")
		}
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
	if len(record.Outputs) > math.MaxUint16 || len(outboxes) > math.MaxUint16 {
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
	record.Outputs = make([]model.Tuple, count)
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
	outboxes := make([]OutboxRecord, outCount)
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
	w.u16(domainRecordSchema)
	w.u8(boolByte(record.Completed))
	w.blob(encoded)
	return w.bytes(), nil
}
func decodeOutbox(payload []byte) (OutboxRecord, error) {
	r := newRecordReader(payload)
	if err := r.schema(); err != nil {
		return OutboxRecord{}, err
	}
	complete, err := r.u8()
	if err != nil || complete > 1 {
		return OutboxRecord{}, errors.New("invalid outbox status")
	}
	encoded, err := r.blob(MaxRecordPayloadBytes)
	if err != nil || !r.done() {
		return OutboxRecord{}, errors.New("invalid outbox record")
	}
	message, err := protocol.UnmarshalTupleDelivery(encoded)
	if err != nil {
		return OutboxRecord{}, err
	}
	return OutboxRecord{ID: message.DeliveryID, Tuple: message.Tuple, Producer: message.Producer, Destination: message.Destination, AssignmentRevision: message.Assignment.Revision, AssignmentDigest: message.Assignment.Digest, CoordinatorEpoch: message.Coordinator, Completed: complete == 1}, nil
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
	w.u16(domainRecordSchema)
	w.job(n.JobID)
	w.task(n.Source)
	w.u64(n.Watermark)
	w.u64(n.RaftIndex)
	w.epoch(n.Epoch)
	return w.bytes(), nil
}
func decodeCheckpoint(payload []byte) (model.CheckpointNotice, error) {
	r := newRecordReader(payload)
	if err := r.schema(); err != nil {
		return model.CheckpointNotice{}, err
	}
	var n model.CheckpointNotice
	var err error
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
	return StoredResult{Record: record, Provenance: p}, p.Validate(record)
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
	w.u16(domainRecordSchema)
	w.u64(v)
	return w.bytes()
}
func decodeUint64Payload(payload []byte) (uint64, error) {
	r := newRecordReader(payload)
	if err := r.schema(); err != nil {
		return 0, err
	}
	v, err := r.u64()
	if err != nil || !r.done() {
		return 0, errors.New("invalid uint64 record")
	}
	return v, nil
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
	if err := cursor.Source.Validate(); err != nil {
		return nil, err
	}
	if len(outboxes) > math.MaxUint16 {
		return nil, errors.New("too many source outboxes")
	}
	w := newRecordWriter()
	w.u16(domainRecordSchema)
	w.task(cursor.Source)
	w.u64(cursor.NextSequence)
	w.u64(cursor.EOF)
	w.u64(cursor.Watermark)
	w.u64(cursor.RaftIndex)
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
	if err := r.schema(); err != nil {
		return SourceCursor{}, nil, err
	}
	var cursor SourceCursor
	var err error
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
	count, err := r.u16()
	if err != nil {
		return cursor, nil, err
	}
	outboxes := make([]OutboxRecord, count)
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
	v.Checkpoints = make([]model.SourceCheckpoint, count)
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
func resultIndex(v []StoredResult, id model.TupleID) int {
	for i := range v {
		if v[i].Record.TupleID == id {
			return i
		}
	}
	return -1
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
func equalInstalledAssignment(a, b InstalledAssignment) bool {
	return a.Assignment.JobID == b.Assignment.JobID && a.Assignment.Revision == b.Assignment.Revision && a.Assignment.Digest == b.Assignment.Digest && a.JobControlRevision == b.JobControlRevision && a.SchedulingState == b.SchedulingState && a.CoordinatorEpoch == b.CoordinatorEpoch && bytes.Equal(a.SpecificationBytes, b.SpecificationBytes) && equalTokens(a.Assignment.Tasks, b.Assignment.Tasks) && equalReplicas(a.Assignment.ResultReplicas, b.Assignment.ResultReplicas)
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
	if a.ID != b.ID || a.Producer != b.Producer || a.Destination != b.Destination || a.AssignmentRevision != b.AssignmentRevision || a.AssignmentDigest != b.AssignmentDigest || a.CoordinatorEpoch != b.CoordinatorEpoch || a.Reservation != b.Reservation {
		return false
	}
	aa, _ := model.MarshalTuple(a.Tuple)
	bb, _ := model.MarshalTuple(b.Tuple)
	return bytes.Equal(aa, bb)
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
	aa, _ := model.MarshalResultRecord(a.Record)
	bb, _ := model.MarshalResultRecord(b.Record)
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
func tupleKey(v model.TupleID) []byte { w := newRecordWriter(); w.tupleID(v); return w.data }
func boolByte(v bool) uint8 {
	if v {
		return 1
	}
	return 0
}
