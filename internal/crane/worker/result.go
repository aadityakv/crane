package worker

import (
	"context"
	"errors"
	"sort"

	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/store"
)

type ownedResult struct {
	record     model.ResultRecord
	provenance model.ResultCopyProvenance
	parent     model.DeliveryID
	sending    bool
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
		receipt, err := engine.replicator.ReplicateRecord(ctx, job.record, job.provenance)
		select {
		case engine.resultResponses <- resultResponse{key: job.key, receipt: receipt, err: err}:
		case <-ctx.Done():
		}
	}
}

func (engine *Engine) reconcileResults(ctx context.Context) error {
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
		key := resultIdentity{sink: delivery.Destination.Task, tuple: delivery.ID.Tuple}
		if _, exists := engine.results[key]; exists {
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
		if err := engine.repository.UpsertResult(record, provenance); err != nil {
			return engine.ownerError("persist primary result", err)
		}
		engine.results[key] = &ownedResult{record: record, provenance: provenance, parent: id}
	}

	type scheduledResult struct {
		key    resultIdentity
		parent model.DeliveryID
	}
	ordered := make([]scheduledResult, 0, len(engine.results))
	for key, result := range engine.results {
		if result.parent != (model.DeliveryID{}) {
			ordered = append(ordered, scheduledResult{key: key, parent: result.parent})
		}
	}
	sort.Slice(ordered, func(i, j int) bool { return deliveryIDLess(ordered[i].parent, ordered[j].parent) })
	for _, candidate := range ordered {
		key, result := candidate.key, engine.results[candidate.key]
		if result.sending || result.parent == (model.DeliveryID{}) {
			continue
		}
		parent, ok := engine.deliveries[result.parent]
		if !ok || parent.State != store.Processed {
			continue
		}
		assignment, ok := engine.repository.InstalledAssignment(result.record.TupleID.JobID)
		if !ok || !engine.currentResultProvenance(assignment, result) {
			// Historical bytes are permanent, but normal replication is
			// deliberately fail-closed without a bilateral repair grant.
			continue
		}
		target := result.provenance
		target.DestinationRole = model.SecondaryReplica
		select {
		case engine.resultJobs <- resultJob{key: key, record: result.record, provenance: target}:
			result.sending = true
		default:
			return nil
		}
	}
	return nil
}

func (engine *Engine) handleResultResponse(response resultResponse) error {
	result, ok := engine.results[response.key]
	if !ok {
		return errors.New("result replication completed for unknown result")
	}
	result.sending = false
	if errors.Is(response.err, context.Canceled) {
		return nil
	}
	if response.err != nil {
		return engine.ownerError("replicate result", response.err)
	}
	assignment, ok := engine.repository.InstalledAssignment(result.record.TupleID.JobID)
	if !ok || !engine.currentResultProvenance(assignment, result) {
		// Assignment/fence closure may race an in-flight durable ACK. Retain the
		// primary bytes and Processed parent; a later authorized reconcile or
		// bilateral repair decides the next action without killing the owner.
		return nil
	}
	encoded, err := model.MarshalResultRecord(result.record)
	if err != nil {
		return err
	}
	replica := result.provenance.ReplicaSet
	if response.receipt.DestinationNodeID != replica.SecondaryNodeID || response.receipt.DestinationWorkerEpoch != replica.SecondaryEpoch || response.receipt.StreamChecksum != model.ResultRecordStreamChecksum(result.record) || response.receipt.StreamLength != uint64(len(encoded)) || response.receipt.CoordinatorEpoch != assignment.CoordinatorEpoch {
		return errors.New("result replication receipt does not bind exact durable secondary copy")
	}
	if err := engine.repository.MarkCompleted(result.parent); err != nil {
		return engine.ownerError("complete replicated sink delivery", err)
	}
	parent := engine.deliveries[result.parent]
	parent.State = store.Completed
	engine.deliveries[result.parent] = parent
	return nil
}

func (engine *Engine) currentSinkAuthority(assignment store.InstalledAssignment, delivery store.DeliveryRecord) bool {
	return assignment.SchedulingState == model.Running && assignment.CoordinatorEpoch == engine.repository.CurrentFence() && delivery.CoordinatorEpoch == assignment.CoordinatorEpoch && delivery.AssignmentRevision == assignment.Assignment.Revision && delivery.AssignmentDigest == assignment.Assignment.Digest && delivery.Destination.WorkerID == engine.localNode && delivery.Destination.WorkerEpoch == engine.localEpoch && containsAssignmentToken(assignment.Assignment, delivery.Destination)
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
