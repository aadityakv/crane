package store

import (
	"bytes"
	"errors"
	"math"
	"sort"

	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
)

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
		// Keep the in-memory order identical to the snapshot/recovery order
		// (ascending RepairID) so a reopen never reorders the recovered work.
		sort.Slice(work.Repairs, func(i, j int) bool {
			return bytes.Compare(work.Repairs[i].Instruction.RepairID[:], work.Repairs[j].Instruction.RepairID[:]) < 0
		})
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

func cloneRepair(v ResultRepairRecord) ResultRepairRecord {
	v.Instruction.Checkpoints = append([]model.SourceCheckpoint(nil), v.Instruction.Checkpoints...)
	return v
}

func repairIndex(v []ResultRepairRecord, id [16]byte) int {
	for i := range v {
		if v[i].Instruction.RepairID == id {
			return i
		}
	}
	return -1
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
