package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
	"github.com/aaditya/cs425mp3/internal/crane/store"
)

type ownedResult struct {
	record     model.ResultRecord
	provenance model.ResultCopyProvenance
	parents    []model.DeliveryID
	sending    bool
	// target is the partner-copy provenance of the in-flight replication.
	target *model.ResultCopyProvenance
	// retained marks a superseded-envelope copy deliberately left to the
	// bilateral repair grant that covers it under the current envelope.
	retained bool
}

// resultEnvelope identifies the installed assignment envelope a retained
// result readoption pass was last evaluated against.
type resultEnvelope struct {
	revision uint64
	digest   [32]byte
	epoch    model.CoordinatorEpoch
}

type resultJob struct {
	key        resultIdentity
	record     model.ResultRecord
	provenance model.ResultCopyProvenance
}

type resultResponse struct {
	key     resultIdentity
	receipt ResultReplicationReceipt
	err     error
}

type resultIdentity struct {
	sink  model.TaskID
	tuple model.TupleID
}

func resultID(record model.ResultRecord) resultIdentity {
	return resultIdentity{sink: record.SinkTask, tuple: record.TupleID}
}

func (engine *Engine) resultWorker(ctx context.Context) {
	defer engine.workers.Done()
	for job := range engine.resultJobs {
		receipt, err := engine.replicator.ReplicateRecord(ctx, cloneResultRecord(job.record), job.provenance)
		select {
		case engine.resultResponses <- resultResponse{key: job.key, receipt: receipt, err: err}:
		case <-ctx.Done():
		}
	}
}

func (engine *Engine) reconcileResults(ctx context.Context) error {
	for _, result := range engine.results {
		result.parents = result.parents[:0]
	}
	deliveryIDs := make([]model.DeliveryID, 0, len(engine.deliveries))
	for id := range engine.deliveries {
		deliveryIDs = append(deliveryIDs, id)
	}
	sort.Slice(deliveryIDs, func(i, j int) bool { return deliveryIDLess(deliveryIDs[i], deliveryIDs[j]) })
	for _, id := range deliveryIDs {
		delivery := engine.deliveries[id]
		if delivery.State != store.Processed {
			continue
		}
		assignment, ok := engine.repository.InstalledAssignment(id.Tuple.JobID)
		if !ok {
			return errors.New("processed sink references missing assignment")
		}
		stage, ok := findStage(assignment.Topology, delivery.Destination.Task.StageID)
		if !ok || stage.Role != model.StageSink {
			continue
		}
		if !engine.currentSinkAuthority(assignment, delivery) {
			// A retained old-revision delivery/result may only move under the
			// bilateral repair grant owned by Tasks 16/17/19.
			continue
		}
		if len(delivery.Outputs) != 1 {
			return errors.New("processed sink does not contain one canonical output")
		}
		encoded, err := model.MarshalTuple(delivery.Outputs[0])
		if err != nil {
			return err
		}
		record, err := model.NewResultRecord(delivery.ID.Tuple, delivery.Destination.Task, delivery.Destination.SpecificationHash, encoded)
		if err != nil {
			return err
		}
		replica, ok := findResultReplica(assignment.Assignment, record.SinkTask)
		if !ok || replica.PrimaryNodeID != engine.localNode || replica.PrimaryEpoch != engine.localEpoch || replica.SecondaryNodeID == engine.localNode {
			return errors.New("sink task is not the exact local primary result replica")
		}
		provenance := model.ResultCopyProvenance{AssignmentRevision: assignment.Assignment.Revision, AssignmentDigest: assignment.Assignment.Digest, ReplicaSet: replica, DestinationRole: model.PrimaryReplica, CoordinatorEpoch: assignment.CoordinatorEpoch}
		key := resultIdentity{sink: delivery.Destination.Task, tuple: delivery.ID.Tuple}
		owned, exists := engine.results[key]
		if !exists {
			if err := engine.repository.UpsertResult(record, provenance); err != nil {
				return engine.ownerError("persist primary result", err)
			}
			owned = &ownedResult{record: cloneResultRecord(record), provenance: provenance}
			engine.results[key] = owned
		} else if !equalOwnedResult(owned, record, provenance) && !(equalResultRecord(owned.record, record) && store.ResultProvenanceOrderedBefore(owned.provenance, provenance)) {
			// A retained copy under a strictly superseded envelope is the same
			// logical record; it re-binds on its next durable partner receipt.
			return model.ErrIdentityReuse
		}
		owned.parents = append(owned.parents, id)
	}
	if err := engine.readoptRetainedResults(ctx); err != nil {
		return err
	}

	type scheduledResult struct {
		key    resultIdentity
		parent model.DeliveryID
	}
	ordered := make([]scheduledResult, 0, len(engine.results))
	for key, result := range engine.results {
		if len(result.parents) > 0 {
			ordered = append(ordered, scheduledResult{key: key, parent: result.parents[0]})
		}
	}
	sort.Slice(ordered, func(i, j int) bool { return deliveryIDLess(ordered[i].parent, ordered[j].parent) })
	for _, candidate := range ordered {
		key, result := candidate.key, engine.results[candidate.key]
		if result.sending || len(result.parents) == 0 {
			continue
		}
		parent, ok := engine.deliveries[result.parents[0]]
		if !ok || parent.State != store.Processed {
			continue
		}
		assignment, ok := engine.repository.InstalledAssignment(result.record.TupleID.JobID)
		if !ok || assignment.SchedulingState != model.Running {
			continue
		}
		local, partner, envelopeOK := engine.currentCopyEnvelope(assignment, result.record.SinkTask)
		if !envelopeOK || local.DestinationRole != model.PrimaryReplica || result.provenance != local && !store.ResultProvenanceOrderedBefore(result.provenance, local) {
			// Normal replication stays fail-closed for any copy that is not the
			// current primary's own — current, or strictly superseded and
			// re-bound on receipt (Task 24 defect #4 ruling).
			continue
		}
		if !engine.dispatchResult(key, result, partner) {
			return nil
		}
	}
	return nil
}

// dispatchResult hands one exact record to the replication workers under the
// partner provenance and reports whether it was queued.
func (engine *Engine) dispatchResult(key resultIdentity, result *ownedResult, target model.ResultCopyProvenance) bool {
	select {
	case engine.resultJobs <- resultJob{key: key, record: cloneResultRecord(result.record), provenance: target}:
		result.sending = true
		sent := target
		result.target = &sent
		return true
	default:
		return false
	}
}

// readoptRetainedResults re-establishes the second copy of every result this
// worker retains under a superseded copy envelope while it is a member of the
// CURRENT replica pair (Task 24 defect #4 ruling): the record is
// re-replicated to the current partner as ordinary two-copy replication under
// the current assignment envelope (current fence, revision, digest, replica
// set and worker epochs), and both copies re-bind to the current pair on
// receipt. It runs on every reconcile, so a partner-changing install and
// recovery both drive it. Records the durable current-epoch bilateral repair
// grant covers stay with the grant. Senders that are not current members and
// copies whose envelope is not strictly superseded never move.
func (engine *Engine) readoptRetainedResults(ctx context.Context) error {
	fence := engine.repository.CurrentFence()
	if err := engine.adoptDurableResults(); err != nil {
		return err
	}
	type jobWork struct {
		assignment store.InstalledAssignment
		admitted   bool
		keys       []resultIdentity
	}
	jobs := make(map[model.JobID]*jobWork)
	for key, result := range engine.results {
		job := key.tuple.JobID
		work, seen := jobs[job]
		if !seen {
			work = &jobWork{}
			jobs[job] = work
			assignment, ok := engine.repository.InstalledAssignment(job)
			if ok && assignment.CoordinatorEpoch == fence && replicationAdmitted(assignment.SchedulingState) {
				work.assignment, work.admitted = assignment, true
				envelope := resultEnvelope{revision: assignment.Assignment.Revision, digest: assignment.Assignment.Digest, epoch: assignment.CoordinatorEpoch}
				if engine.readoptEnvelopes[job] != envelope {
					// A new envelope re-evaluates every retained copy of the job.
					engine.readoptEnvelopes[job] = envelope
					for other, candidate := range engine.results {
						if other.tuple.JobID == job {
							candidate.retained = false
						}
					}
				}
			}
		}
		if !work.admitted || result.sending || result.retained || len(result.parents) > 0 {
			continue
		}
		local, _, ok := engine.currentCopyEnvelope(work.assignment, result.record.SinkTask)
		if !ok || result.provenance == local || !store.ResultProvenanceOrderedBefore(result.provenance, local) || result.record.SpecificationHash != work.assignment.Topology.Digest() {
			continue
		}
		work.keys = append(work.keys, key)
	}
	jobIDs := make([]model.JobID, 0, len(jobs))
	for job, work := range jobs {
		if len(work.keys) > 0 {
			jobIDs = append(jobIDs, job)
		}
	}
	sort.Slice(jobIDs, func(i, j int) bool { return bytes.Compare(jobIDs[i][:], jobIDs[j][:]) < 0 })
	for _, job := range jobIDs {
		work := jobs[job]
		recovered, err := engine.repository.RecoverWork()
		if err != nil {
			return engine.ownerError("recover retained results", err)
		}
		sort.Slice(work.keys, func(i, j int) bool { return tupleTransferLess(work.keys[i].tuple, work.keys[j].tuple) })
		for _, key := range work.keys {
			result := engine.results[key]
			if grantCoversRetainedResult(recovered.Repairs, fence, work.assignment.Assignment, result.record, engine.localNode, engine.localEpoch) {
				tracef("readopt node=%d: seq=%d left to the repair grant", engine.localNode, result.record.TupleID.SourceSequence)
				result.retained = true
				continue
			}
			_, partner, ok := engine.currentCopyEnvelope(work.assignment, result.record.SinkTask)
			if !ok {
				continue
			}
			if !engine.dispatchResult(key, result, partner) {
				return nil
			}
			tracef("readopt node=%d: seq=%d re-replicating to node %d (rev=%d state=%d)", engine.localNode, result.record.TupleID.SourceSequence, partner.ReplicaSet.PrimaryNodeID+partner.ReplicaSet.SecondaryNodeID-engine.localNode, work.assignment.Assignment.Revision, work.assignment.SchedulingState)
		}
	}
	return nil
}

// adoptDurableResults loads, for every job whose installation changed since
// the last pass, the result copies the durable store retains that this engine
// does not yet own — a secondary receives its copies through the transfer
// owner, never through the engine — so a partner-changing install can
// re-replicate them. Recovery itself already owns every durable copy.
func (engine *Engine) adoptDurableResults() error {
	if len(engine.readoptPending) == 0 {
		return nil
	}
	pending := engine.readoptPending
	engine.readoptPending = make(map[model.JobID]struct{})
	needed := false
	for job := range pending {
		if assignment, ok := engine.repository.InstalledAssignment(job); ok && assignment.CoordinatorEpoch == engine.repository.CurrentFence() && replicationAdmitted(assignment.SchedulingState) {
			needed = true
		}
	}
	if !needed {
		return nil
	}
	work, err := engine.repository.RecoverWork()
	if err != nil {
		return engine.ownerError("recover retained results", err)
	}
	for _, stored := range work.Results {
		if _, wanted := pending[stored.Record.TupleID.JobID]; !wanted {
			continue
		}
		key := resultID(stored.Record)
		if _, owned := engine.results[key]; owned {
			continue
		}
		engine.results[key] = &ownedResult{record: cloneResultRecord(stored.Record), provenance: stored.Provenance}
	}
	return nil
}

// grantCoversRetainedResult reports whether a live durable bilateral repair
// grant — at the current fence and bound to the exact installed assignment
// revision/digest — naming this worker as an endpoint covers the record; such
// records are re-established by the grant, never by holder-driven
// re-replication. A grant the installed assignment has advanced past, or one
// durably failed, is dead and covers nothing, so the holder pass resumes.
func grantCoversRetainedResult(repairs []store.ResultRepairRecord, fence model.CoordinatorEpoch, assignment model.AssignmentSet, record model.ResultRecord, localNode uint16, localEpoch model.WorkerEpoch) bool {
	for _, repair := range repairs {
		instruction := repair.Instruction
		if instruction.CoordinatorEpoch != fence || instruction.JobID != record.TupleID.JobID || instruction.SinkTask != record.SinkTask || instruction.SpecificationHash != record.SpecificationHash {
			continue
		}
		if repair.State == store.RepairFailed || instruction.AssignmentRevision != assignment.Revision || instruction.AssignmentDigest != assignment.Digest {
			continue
		}
		local := instruction.SourceNodeID == localNode && instruction.SourceWorkerEpoch == localEpoch || instruction.DestinationNodeID == localNode && instruction.DestinationWorkerEpoch == localEpoch
		if local && repairCoversTuple(instruction, record.TupleID) {
			return true
		}
	}
	return false
}

// currentCopyEnvelope returns the provenance this worker's own copy of a sink
// partition's records carries under the current installed assignment and the
// provenance of the partner copy it replicates to. It reports false unless the
// assignment is at the current fence in a replication-admitted state and this
// exact worker incarnation is one endpoint of a two-distinct-node replica set.
func (engine *Engine) currentCopyEnvelope(assignment store.InstalledAssignment, sink model.TaskID) (model.ResultCopyProvenance, model.ResultCopyProvenance, bool) {
	replica, ok := findResultReplica(assignment.Assignment, sink)
	if !ok || assignment.CoordinatorEpoch != engine.repository.CurrentFence() || !replicationAdmitted(assignment.SchedulingState) || replica.PrimaryNodeID == replica.SecondaryNodeID {
		return model.ResultCopyProvenance{}, model.ResultCopyProvenance{}, false
	}
	envelope := model.ResultCopyProvenance{AssignmentRevision: assignment.Assignment.Revision, AssignmentDigest: assignment.Assignment.Digest, ReplicaSet: replica, CoordinatorEpoch: assignment.CoordinatorEpoch}
	local, partner := envelope, envelope
	switch {
	case replica.PrimaryNodeID == engine.localNode && replica.PrimaryEpoch == engine.localEpoch:
		local.DestinationRole, partner.DestinationRole = model.PrimaryReplica, model.SecondaryReplica
	case replica.SecondaryNodeID == engine.localNode && replica.SecondaryEpoch == engine.localEpoch:
		local.DestinationRole, partner.DestinationRole = model.SecondaryReplica, model.PrimaryReplica
	default:
		return model.ResultCopyProvenance{}, model.ResultCopyProvenance{}, false
	}
	return local, partner, true
}

// replicationAdmitted reports the scheduling states under which result copies
// move between the current replicas: Running, and Draining — a drained job
// whose replica pair changed after its records completed still needs its
// second current copy before it can seal (Task 24 defect #4 ruling). Closed is
// the re-fence window and stays excluded.
func replicationAdmitted(state model.SchedulingState) bool {
	return state == model.Running || state == model.Draining
}

func (engine *Engine) handleResultResponse(response resultResponse) error {
	result, ok := engine.results[response.key]
	if !ok {
		return errors.New("result replication completed for unknown result")
	}
	result.sending = false
	target := result.target
	result.target = nil
	if errors.Is(response.err, context.Canceled) {
		return nil
	}
	if response.err != nil {
		return engine.ownerError("replicate result", response.err)
	}
	assignment, ok := engine.repository.InstalledAssignment(result.record.TupleID.JobID)
	if !ok {
		return nil
	}
	local, partner, envelopeOK := engine.currentCopyEnvelope(assignment, result.record.SinkTask)
	tracef("receipt node=%d: seq=%d envelope-ok=%t target-current=%t", engine.localNode, result.record.TupleID.SourceSequence, envelopeOK, target != nil && *target == partner)
	if !envelopeOK || target == nil || *target != partner {
		// Assignment/fence closure may race an in-flight durable ACK. Retain the
		// local bytes and Processed parents; a later authorized reconcile or
		// bilateral repair decides the next action without killing the owner.
		return nil
	}
	encoded, err := model.MarshalResultRecord(result.record)
	if err != nil {
		return err
	}
	destinationNode, destinationEpoch, _, _, _ := endpointsForRole(partner.ReplicaSet, partner.DestinationRole)
	if response.receipt.DestinationNodeID != destinationNode || response.receipt.DestinationWorkerEpoch != destinationEpoch || response.receipt.StreamChecksum != model.ResultRecordStreamChecksum(result.record) || response.receipt.StreamLength != uint64(len(encoded)) || response.receipt.CoordinatorEpoch != assignment.CoordinatorEpoch {
		return errors.New("result replication receipt does not bind exact durable partner copy")
	}
	if result.provenance != local {
		// The partner now holds a current-provenance copy: re-bind the local
		// copy to the current pair (Task 24 defect #4 ruling).
		if err := engine.repository.UpsertResult(cloneResultRecord(result.record), local); err != nil {
			return engine.ownerError("rebind retained result", err)
		}
		result.provenance = local
	}
	if len(result.parents) == 0 || !engine.currentResultProvenance(assignment, result) {
		return nil
	}
	parents := append([]model.DeliveryID(nil), result.parents...)
	for _, parentID := range parents {
		parent, exists := engine.deliveries[parentID]
		if !exists || parent.State != store.Processed || !engine.currentSinkAuthority(assignment, parent) {
			// An authority change between dispatch and receipt retains every
			// parent for later authorized reconciliation.
			return nil
		}
	}
	for _, parentID := range parents {
		if err := engine.repository.MarkCompleted(parentID); err != nil {
			return engine.ownerError("complete replicated sink delivery", err)
		}
		parent := engine.deliveries[parentID]
		parent.State = store.Completed
		engine.deliveries[parentID] = parent
	}
	return nil
}

// currentSinkAuthority reports whether a Processed sink delivery is current
// custody under the installed assignment. Retained custody published under a
// superseded envelope that re-adopts byte-exactly under the current assignment
// (Task 24 defect #5 and #4 rulings) is current custody; a replaced sink task
// never qualifies.
func (engine *Engine) currentSinkAuthority(assignment store.InstalledAssignment, delivery store.DeliveryRecord) bool {
	fence := engine.repository.CurrentFence()
	if assignment.SchedulingState != model.Running || assignment.CoordinatorEpoch != fence {
		return false
	}
	if retainedEnvelope(assignment, fence, delivery.AssignmentRevision, delivery.AssignmentDigest, delivery.CoordinatorEpoch) {
		readopted, ok := engine.readoptRetainedRecord(assignment, fence, delivery)
		if !ok {
			return false
		}
		delivery = readopted
	}
	return delivery.Destination.WorkerID == engine.localNode && delivery.Destination.WorkerEpoch == engine.localEpoch && containsAssignmentToken(assignment.Assignment, delivery.Destination)
}

func (engine *Engine) currentResultProvenance(assignment store.InstalledAssignment, result *ownedResult) bool {
	replica, ok := findResultReplica(assignment.Assignment, result.record.SinkTask)
	return ok && assignment.SchedulingState == model.Running && assignment.CoordinatorEpoch == engine.repository.CurrentFence() && result.provenance.AssignmentRevision == assignment.Assignment.Revision && result.provenance.AssignmentDigest == assignment.Assignment.Digest && result.provenance.CoordinatorEpoch == assignment.CoordinatorEpoch && result.provenance.ReplicaSet == replica && result.provenance.DestinationRole == model.PrimaryReplica && replica.PrimaryNodeID == engine.localNode && replica.PrimaryEpoch == engine.localEpoch && replica.SecondaryNodeID != engine.localNode
}

func findResultReplica(set model.AssignmentSet, task model.TaskID) (model.ResultReplicaSet, bool) {
	for _, replica := range set.ResultReplicas {
		if replica.SinkTask == task {
			return replica, true
		}
	}
	return model.ResultReplicaSet{}, false
}

func cloneResultRecord(record model.ResultRecord) model.ResultRecord {
	record.Value = append([]byte(nil), record.Value...)
	return record
}

func equalOwnedResult(owned *ownedResult, record model.ResultRecord, provenance model.ResultCopyProvenance) bool {
	return owned != nil && equalResultRecord(owned.record, record) && owned.provenance == provenance
}

func equalResultRecord(a, b model.ResultRecord) bool {
	return a.TupleID == b.TupleID && a.SinkTask == b.SinkTask && a.SpecificationHash == b.SpecificationHash && a.Checksum == b.Checksum && bytes.Equal(a.Value, b.Value)
}

// SealResultPartition derives the canonical sealed artifact identity and
// complete canonical stream for one sink partition's covered records. The
// stream is the TupleID-sorted concatenation of u32-length-prefixed canonical
// result records, so its SHA-256 authenticates the exact logical record set
// independent of any copy provenance.
func SealResultPartition(job model.JobID, sink model.TaskID, specificationHash [32]byte, records []model.ResultRecord) (protocol.ResultArtifact, []byte, error) {
	sorted := append([]model.ResultRecord(nil), records...)
	sort.Slice(sorted, func(i, j int) bool { return tupleTransferLess(sorted[i].TupleID, sorted[j].TupleID) })
	stream := make([]byte, 0)
	for index, record := range sorted {
		if index > 0 && sorted[index-1].TupleID == record.TupleID {
			return protocol.ResultArtifact{}, nil, model.ErrIdentityReuse
		}
		if record.TupleID.JobID != job || record.SinkTask != sink {
			return protocol.ResultArtifact{}, nil, fmt.Errorf("record %d does not belong to the sealed partition", index)
		}
		if err := record.Validate(); err != nil {
			return protocol.ResultArtifact{}, nil, err
		}
		if record.SpecificationHash != specificationHash {
			return protocol.ResultArtifact{}, nil, fmt.Errorf("record %d carries a foreign specification hash", index)
		}
		entry, err := protocol.EncodedResultPageRecordBytes(record)
		if err != nil {
			return protocol.ResultArtifact{}, nil, err
		}
		if uint64(len(stream)) > ^uint64(0)-uint64(len(entry)) || uint64(len(stream))+uint64(len(entry)) > protocol.MaxTransferTotalBytes {
			return protocol.ResultArtifact{}, nil, errors.New("sealed artifact exceeds the protocol bound")
		}
		stream = append(stream, entry...)
	}
	artifact := protocol.ResultArtifact{
		JobID: job, SinkTask: sink, SpecificationHash: specificationHash,
		RecordCount: uint64(len(sorted)), TotalLength: uint64(len(stream)), Checksum: sha256.Sum256(stream),
	}
	if err := validateSealedArtifact(artifact); err != nil {
		return protocol.ResultArtifact{}, nil, err
	}
	return artifact, stream, nil
}

// validateSealedArtifact enforces the complete sealed-artifact identity and
// count/length bounds before any durable mutation or transfer.
func validateSealedArtifact(artifact protocol.ResultArtifact) error {
	if artifact.JobID.Validate() != nil || artifact.SinkTask.Validate() != nil || artifact.SinkTask.JobID != artifact.JobID ||
		artifact.SpecificationHash == ([32]byte{}) || artifact.TotalLength > protocol.MaxTransferTotalBytes || artifact.Checksum == ([32]byte{}) {
		return errors.New("invalid sealed artifact identity")
	}
	if artifact.TotalLength == 0 && (artifact.RecordCount != 0 || artifact.Checksum != sha256.Sum256(nil)) ||
		artifact.TotalLength > 0 && artifact.RecordCount == 0 {
		return errors.New("sealed artifact count/length mismatch")
	}
	if artifact.RecordCount > model.ResultArtifactMaxRecordCountV1 {
		return errors.New("sealed artifact record count exceeds the bound")
	}
	if artifact.RecordCount > 0 {
		if artifact.TotalLength < artifact.RecordCount*model.ResultArtifactMinRecordBytesV1 ||
			artifact.TotalLength > artifact.RecordCount*model.ResultArtifactMaxRecordBytesV1 {
			return errors.New("sealed artifact bytes cannot contain the declared records")
		}
	}
	return nil
}

// FinalCheckpointVector computes the complete committed checkpoint vector of
// one installed assignment when every source task has a durable final
// watermark proof (a local authoritative cursor or a committed checkpoint
// observation). It reports false whenever any source's finality cannot be
// proven, so sealing never proceeds on unproven coverage.
func FinalCheckpointVector(work store.RecoveredWork, assignment store.InstalledAssignment) ([]model.SourceCheckpoint, bool) {
	vector := make([]model.SourceCheckpoint, 0)
	for _, stage := range assignment.Topology.Spec().Stages {
		if stage.Role != model.StageSource {
			continue
		}
		for partition := uint16(0); partition < stage.Parallelism; partition++ {
			task := model.TaskID{JobID: assignment.Assignment.JobID, StageID: stage.StageID, Partition: partition}
			eof, err := model.SourceEOF(assignment.Topology, task)
			if err != nil {
				return nil, false
			}
			watermark, proven := finalSourceWatermark(work, assignment, task)
			if !proven || watermark != eof {
				return nil, false
			}
			vector = append(vector, model.SourceCheckpoint{Source: task, Watermark: watermark})
		}
	}
	if len(vector) == 0 {
		return nil, false
	}
	sort.Slice(vector, func(i, j int) bool { return compareTask(vector[i].Source, vector[j].Source) < 0 })
	return vector, true
}

// finalSourceWatermark proves one source task's committed watermark from
// durable local state. A local source cursor with a retained durable
// checkpoint proof is authoritative committed-watermark evidence — the
// watermark was committed, an immutable historical fact, so the proof's
// epoch/JobControlRevision/assignment binding may be historical (Task 24
// defect #2 ruling) — otherwise a matching committed-checkpoint observation
// proves the watermark. Without either, only the trivially committed zero
// watermark of an untouched source is provable; an unproven existing cursor
// proves nothing.
func finalSourceWatermark(work store.RecoveredWork, assignment store.InstalledAssignment, task model.TaskID) (uint64, bool) {
	cursorExists := false
	for _, cursor := range work.Sources {
		if cursor.Source != task {
			continue
		}
		cursorExists = true
		if cursor.Watermark == 0 && cursor.CheckpointRevision == 0 {
			return 0, true
		}
		authority := cursor.CheckpointAuthority
		if cursor.RaftIndex != 0 && authority.SourceToken.Task == task {
			return cursor.Watermark, true
		}
	}
	for _, observation := range work.Checkpoints {
		if observation.Notice.Source == task && observation.Notice.JobID == assignment.Assignment.JobID && observation.Notice.Epoch == work.Fence &&
			observation.JobControlRevision == assignment.JobControlRevision && observation.AssignmentRevision == assignment.Assignment.Revision &&
			observation.AssignmentDigest == assignment.Assignment.Digest && observation.Notice.RaftIndex != 0 {
			return observation.Notice.Watermark, true
		}
	}
	if cursorExists {
		return 0, false
	}
	// Without any durable observation only the trivially committed zero
	// watermark of an untouched source can be proven; finality then demands a
	// zero EOF, which the caller checks.
	return 0, true
}

// SealingRecords selects the checkpoint-covered canonical record set of one
// sink partition from retained durable results. Retained historical copy
// provenance is accepted: the immutable logical record, not its envelope,
// defines the sealed artifact, which itself becomes the new authenticated
// envelope for any destination copy.
func SealingRecords(work store.RecoveredWork, assignment store.InstalledAssignment, sink model.TaskID, vector []model.SourceCheckpoint) ([]model.ResultRecord, error) {
	watermarks := make(map[model.TaskID]uint64, len(vector))
	for _, entry := range vector {
		watermarks[entry.Source] = entry.Watermark
	}
	records := make([]model.ResultRecord, 0)
	for _, stored := range work.Results {
		record := stored.Record
		if record.TupleID.JobID != assignment.Assignment.JobID || record.SinkTask != sink || record.SpecificationHash != assignment.Topology.Digest() {
			continue
		}
		watermark, covered := watermarks[record.TupleID.SourceTask]
		if !covered || record.TupleID.SourceSequence > watermark {
			continue
		}
		if err := record.Validate(); err != nil {
			return nil, err
		}
		records = append(records, cloneResultRecord(record))
	}
	sort.Slice(records, func(i, j int) bool { return tupleTransferLess(records[i].TupleID, records[j].TupleID) })
	for index := 1; index < len(records); index++ {
		if records[index-1].TupleID == records[index].TupleID {
			return nil, model.ErrIdentityReuse
		}
	}
	return records, nil
}

// ArtifactStore durably retains sealed result artifacts under one directory
// with temporary-file, fsync, and atomic-rename semantics. Every mutation is
// serialized under one mutex and only complete checksum-verified artifacts
// become visible.
type ArtifactStore struct {
	directory string
	mu        sync.Mutex
}

// NewArtifactStore validates the artifact directory without creating or
// moving any file.
func NewArtifactStore(directory string) (*ArtifactStore, error) {
	if directory == "" {
		return nil, errors.New("empty artifact directory")
	}
	return &ArtifactStore{directory: directory}, nil
}

// artifactStorePath derives the canonical durable name of one exact artifact
// identity.
func artifactStorePath(directory string, artifact protocol.ResultArtifact) string {
	encoded := []byte("crane-result-artifact-name-v1\x00")
	encoded = append(encoded, artifact.JobID[:]...)
	encoded = appendTaskTransfer(encoded, artifact.SinkTask)
	encoded = append(encoded, artifact.SpecificationHash[:]...)
	encoded = appendUint64Transfer(encoded, artifact.RecordCount)
	encoded = appendUint64Transfer(encoded, artifact.TotalLength)
	encoded = append(encoded, artifact.Checksum[:]...)
	sum := sha256.Sum256(encoded)
	return directory + string(os.PathSeparator) + hex.EncodeToString(sum[:]) + ".artifact"
}

// Sealed reports whether one exact artifact identity is durably installed.
func (store *ArtifactStore) Sealed(artifact protocol.ResultArtifact) (bool, error) {
	if err := validateSealedArtifact(artifact); err != nil {
		return false, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return artifactFileSealed(store.directory, artifact)
}

// Read returns up to max sealed artifact bytes at offset and reports whether
// more bytes remain after the returned slice.
func (store *ArtifactStore) Read(artifact protocol.ResultArtifact, offset, max uint64) ([]byte, bool, error) {
	if err := validateSealedArtifact(artifact); err != nil {
		return nil, false, err
	}
	if offset > artifact.TotalLength {
		return nil, false, errors.New("artifact read offset outside bounds")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	sealed, err := artifactFileSealed(store.directory, artifact)
	if err != nil {
		return nil, false, err
	}
	if !sealed {
		return nil, false, errors.New("sealed artifact is not installed")
	}
	file, err := os.Open(artifactStorePath(store.directory, artifact))
	if err != nil {
		return nil, false, err
	}
	defer file.Close()
	length := artifact.TotalLength - offset
	if length > max {
		length = max
	}
	if length == 0 {
		return nil, offset < artifact.TotalLength, nil
	}
	data := make([]byte, length)
	if _, err := file.ReadAt(data, int64(offset)); err != nil {
		return nil, false, err
	}
	return data, offset+length < artifact.TotalLength, nil
}

// Seal durably installs one complete artifact stream. Re-sealing the exact
// bytes is idempotent; any different bytes under the same identity are
// rejected without mutation.
func (store *ArtifactStore) Seal(artifact protocol.ResultArtifact, stream []byte) error {
	if err := validateSealedArtifact(artifact); err != nil {
		return err
	}
	if uint64(len(stream)) != artifact.TotalLength || sha256.Sum256(stream) != artifact.Checksum {
		return errors.New("sealed stream does not match the artifact identity")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	sealed, err := artifactFileSealed(store.directory, artifact)
	if err != nil {
		return err
	}
	if sealed {
		existing, readErr := os.ReadFile(artifactStorePath(store.directory, artifact))
		if readErr != nil {
			return readErr
		}
		if bytes.Equal(existing, stream) {
			return nil
		}
		return model.ErrIdentityReuse
	}
	if err := os.MkdirAll(store.directory, 0o700); err != nil {
		return err
	}
	return writeArtifactDurable(store.directory, artifactStorePath(store.directory, artifact), stream)
}

// Install advances one resumable durable artifact receive by an exact
// transfer chunk. Chunks must continue the durable next offset, exact
// duplicates are idempotent, and the complete artifact becomes visible only
// after its whole-stream checksum verifies.
func (store *ArtifactStore) Install(artifact protocol.ResultArtifact, chunk protocol.TransferChunk) (uint64, bool, error) {
	if err := validateSealedArtifact(artifact); err != nil {
		return 0, false, err
	}
	if chunk.TransferID == ([16]byte{}) || chunk.JobID != artifact.JobID || chunk.TotalLength != artifact.TotalLength || chunk.Checksum != artifact.Checksum {
		return 0, false, errors.New("artifact chunk does not bind the artifact identity")
	}
	if chunk.Offset > artifact.TotalLength || uint64(len(chunk.Data)) > artifact.TotalLength-chunk.Offset {
		return 0, false, errors.New("artifact chunk outside total bounds")
	}
	end := chunk.Offset + uint64(len(chunk.Data))
	if chunk.Final != (end == artifact.TotalLength) {
		return 0, false, errors.New("artifact chunk final flag mismatch")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	sealed, err := artifactFileSealed(store.directory, artifact)
	if err != nil {
		return 0, false, err
	}
	if sealed {
		return artifact.TotalLength, true, nil
	}
	if err := os.MkdirAll(store.directory, 0o700); err != nil {
		return 0, false, err
	}
	finalPath := artifactStorePath(store.directory, artifact)
	partialPath := finalPath + ".part"
	next := uint64(0)
	if info, statErr := os.Stat(partialPath); statErr == nil {
		next = uint64(info.Size())
	} else if !os.IsNotExist(statErr) {
		return 0, false, statErr
	}
	if chunk.Offset > next {
		return next, false, errors.New("artifact chunk leaves a gap")
	}
	if chunk.Offset < next && end > chunk.Offset {
		overlapEnd := next
		if end < next {
			overlapEnd = end
		}
		file, openErr := os.Open(partialPath)
		if openErr != nil {
			return next, false, openErr
		}
		existing := make([]byte, overlapEnd-chunk.Offset)
		_, readErr := file.ReadAt(existing, int64(chunk.Offset))
		closeErr := file.Close()
		if readErr != nil {
			return next, false, readErr
		}
		if closeErr != nil {
			return next, false, closeErr
		}
		if !bytes.Equal(existing, chunk.Data[:overlapEnd-chunk.Offset]) {
			return next, false, model.ErrIdentityReuse
		}
	}
	if end > next {
		file, openErr := os.OpenFile(partialPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if openErr != nil {
			return next, false, openErr
		}
		if _, writeErr := file.Write(chunk.Data[next-chunk.Offset:]); writeErr != nil {
			file.Close()
			return next, false, writeErr
		}
		if syncErr := file.Sync(); syncErr != nil {
			file.Close()
			return next, false, syncErr
		}
		if closeErr := file.Close(); closeErr != nil {
			return next, false, closeErr
		}
		next = end
	}
	if next != artifact.TotalLength {
		return next, false, nil
	}
	var stream []byte
	if artifact.TotalLength > 0 {
		var readErr error
		stream, readErr = os.ReadFile(partialPath)
		if readErr != nil {
			return next, false, readErr
		}
	}
	if uint64(len(stream)) != artifact.TotalLength || sha256.Sum256(stream) != artifact.Checksum {
		_ = os.Remove(partialPath)
		return 0, false, errors.New("installed artifact bytes fail the checksum")
	}
	if err := writeArtifactDurable(store.directory, finalPath, stream); err != nil {
		return next, false, err
	}
	if artifact.TotalLength > 0 {
		_ = os.Remove(partialPath)
	}
	return next, true, nil
}

// artifactFileSealed reports whether the exact artifact file exists.
func artifactFileSealed(directory string, artifact protocol.ResultArtifact) (bool, error) {
	_, err := os.Stat(artifactStorePath(directory, artifact))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// writeArtifactDurable publishes exact bytes through a temporary file, fsync,
// atomic rename, and a directory sync barrier.
func writeArtifactDurable(directory, finalPath string, stream []byte) error {
	temporary, err := os.CreateTemp(directory, ".artifact-temp-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	remove := true
	defer func() {
		if remove {
			_ = os.Remove(temporaryName)
		}
	}()
	if _, err := temporary.Write(stream); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryName, finalPath); err != nil {
		return err
	}
	remove = false
	directoryFile, err := os.Open(directory)
	if err != nil {
		return err
	}
	syncErr := directoryFile.Sync()
	closeErr := directoryFile.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}
