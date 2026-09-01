package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math"

	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
	"github.com/aaditya/cs425mp3/internal/crane/store"
)

type deliveryCommand struct {
	message  protocol.TupleDelivery
	response chan deliveryResponse
	release  func()
}

type deliveryResponse struct {
	ack protocol.TupleACK
	err error
}

type ackCommand struct {
	ack      protocol.TupleACK
	response chan error
}

type executionJob struct {
	record   store.DeliveryRecord
	operator model.OperatorSpec
	release  func()
}

type executionResult struct {
	job     executionJob
	outputs []model.Tuple
	err     error
}

// HandleDelivery validates an owned message, enters the shared process gate,
// and waits for the serialized owner to persist custody before acknowledging.
func (engine *Engine) HandleDelivery(ctx context.Context, message protocol.TupleDelivery) (protocol.TupleACK, error) {
	if ctx == nil {
		return protocol.TupleACK{}, errors.New("nil delivery context")
	}
	encoded, err := protocol.MarshalTupleDelivery(message)
	if err != nil {
		return protocol.TupleACK{}, err
	}
	owned, err := protocol.UnmarshalTupleDelivery(encoded)
	if err != nil {
		return protocol.TupleACK{}, err
	}
	if !engine.readyState.Load() {
		return protocol.TupleACK{}, ErrNotReady
	}
	if ack, found, probeErr := engine.probeDelivery(owned); probeErr != nil || found {
		return ack, probeErr
	}
	release, err := engine.gate.Enter()
	if err != nil {
		return protocol.TupleACK{}, err
	}
	response := make(chan deliveryResponse, 1)
	if err = engine.enqueue(deliveryCommand{message: owned, response: response, release: release}, ctx); err != nil {
		release()
		return protocol.TupleACK{}, err
	}
	select {
	case result := <-response:
		return result.ack, result.err
	case <-ctx.Done():
		return protocol.TupleACK{}, ctx.Err()
	case <-engine.done:
		return protocol.TupleACK{}, ErrNotReady
	}
}

// HandleACK applies an exact durable downstream custody/completion response.
// It remains available for already-durable custody while new admission is
// closed, allowing safe drain without reviving execution.
func (engine *Engine) HandleACK(ctx context.Context, ack protocol.TupleACK) error {
	if ctx == nil {
		return errors.New("nil ACK context")
	}
	encoded, err := protocol.MarshalTupleACK(ack)
	if err != nil {
		return err
	}
	owned, err := protocol.UnmarshalTupleACK(encoded)
	if err != nil {
		return err
	}
	response := make(chan error, 1)
	if err = engine.enqueue(ackCommand{ack: owned, response: response}, ctx); err != nil {
		return err
	}
	select {
	case err = <-response:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-engine.done:
		return ErrNotReady
	}
}

func (engine *Engine) probeDelivery(message protocol.TupleDelivery) (protocol.TupleACK, bool, error) {
	if message.Destination.WorkerID != engine.localNode || message.Destination.WorkerEpoch != engine.localEpoch {
		return protocol.TupleACK{}, true, errors.New("delivery destination is not the durable local worker")
	}
	assignment, ok := engine.repository.InstalledAssignment(message.DeliveryID.Tuple.JobID)
	if !ok {
		return protocol.TupleACK{}, false, nil
	}
	reservation, err := assignment.Topology.WorstCaseCustodyBytes(message.Destination.Task)
	if err != nil {
		return protocol.TupleACK{}, true, err
	}
	record := store.DeliveryRecord{ID: message.DeliveryID, Tuple: message.Tuple, Producer: message.Producer, Destination: message.Destination, AssignmentRevision: message.Assignment.Revision, AssignmentDigest: message.Assignment.Digest, CoordinatorEpoch: message.Coordinator, State: store.Received, Reservation: reservation}
	state, found, err := engine.repository.ProbeDelivery(record)
	if err != nil || !found {
		return protocol.TupleACK{}, found, err
	}
	status := protocol.TupleAccepted
	if state == store.Completed || state == store.Compacted {
		status = protocol.TupleCompleted
	} else if state != store.Received && state != store.Processed {
		return protocol.TupleACK{}, true, errors.New("repository returned unknown probed delivery state")
	}
	return protocol.TupleACK{DeliveryID: record.ID, Destination: record.Destination, Assignment: message.Assignment, Coordinator: record.CoordinatorEpoch, Status: status}, true, nil
}

func (engine *Engine) receiveDelivery(ctx context.Context, message protocol.TupleDelivery) (protocol.TupleACK, error) {
	if message.Destination.WorkerID != engine.localNode || message.Destination.WorkerEpoch != engine.localEpoch {
		return protocol.TupleACK{}, errors.New("delivery destination is not the durable local worker")
	}
	assignment, ok := engine.currentRunning(message.DeliveryID.Tuple.JobID)
	if !ok {
		return protocol.TupleACK{}, ErrNotRunning
	}
	if err := validateDeliveryAuthority(assignment, engine.repository.CurrentFence(), message); err != nil {
		return protocol.TupleACK{}, err
	}
	reservation, err := assignment.Topology.WorstCaseCustodyBytes(message.Destination.Task)
	if err != nil {
		return protocol.TupleACK{}, err
	}
	record := store.DeliveryRecord{
		ID: message.DeliveryID, Tuple: message.Tuple, Producer: message.Producer, Destination: message.Destination,
		AssignmentRevision: message.Assignment.Revision, AssignmentDigest: message.Assignment.Digest,
		CoordinatorEpoch: message.Coordinator, State: store.Received, Reservation: reservation,
	}.Clone()
	state, err := engine.repository.Receive(record)
	if err != nil {
		return protocol.TupleACK{}, err
	}
	status := protocol.TupleAccepted
	switch state {
	case store.Received:
		if _, exists := engine.deliveries[record.ID]; !exists {
			engine.deliveries[record.ID] = record
		}
		engine.scheduleExecution(ctx, record)
	case store.Processed:
	case store.Completed, store.Compacted:
		status = protocol.TupleCompleted
	default:
		return protocol.TupleACK{}, errors.New("repository returned unknown delivery state")
	}
	return protocol.TupleACK{DeliveryID: record.ID, Destination: record.Destination, Assignment: message.Assignment, Coordinator: record.CoordinatorEpoch, Status: status}, nil
}

func validateDeliveryAuthority(assignment store.InstalledAssignment, fence model.CoordinatorEpoch, message protocol.TupleDelivery) error {
	set := assignment.Assignment
	if assignment.CoordinatorEpoch != fence || message.Coordinator != fence {
		return ErrNotRunning
	}
	if message.Assignment.JobID != set.JobID || message.Assignment.Revision != set.Revision || message.Assignment.Digest != set.Digest {
		return errors.New("delivery assignment set does not match durable installation")
	}
	if !containsAssignmentToken(set, message.Producer) || !containsAssignmentToken(set, message.Destination) {
		return errors.New("delivery token does not match durable installation")
	}
	if message.Destination.Task != message.DeliveryID.DestinationTask {
		return errors.New("delivery destination token does not match identity")
	}
	var edge model.EdgeSpec
	found := false
	for _, candidate := range assignment.Topology.Spec().Edges {
		if candidate.EdgeID == message.DeliveryID.EdgeID {
			edge, found = candidate, true
			break
		}
	}
	if !found || edge.SourceStageID != message.Producer.Task.StageID || edge.DestinationStageID != message.Destination.Task.StageID {
		return errors.New("delivery does not follow an installed edge")
	}
	partitions, err := model.Route(assignment.Topology, edge, message.DeliveryID.Tuple, message.Tuple)
	if err != nil {
		return err
	}
	routed := false
	for _, partition := range partitions {
		if partition == message.Destination.Task.Partition {
			routed = true
			break
		}
	}
	if !routed {
		return errors.New("delivery destination does not match deterministic route")
	}

	// A direct source delivery has a fully reconstructible payload and identity;
	// check it before asking the durable repository to mutate.
	if message.Producer.Task == message.DeliveryID.Tuple.SourceTask {
		wantID := model.DeriveSourceTupleID(set.JobID, message.Producer.Task, message.DeliveryID.Tuple.SourceSequence)
		if wantID != message.DeliveryID.Tuple {
			return errors.New("source delivery tuple identity mismatch")
		}
		wantTuple, exists, sourceErr := model.SourceTuple(assignment.Topology, message.Producer.Task, message.DeliveryID.Tuple.SourceSequence)
		if sourceErr != nil || !exists {
			return errors.New("source delivery is outside immutable EOF")
		}
		wantBytes, _ := model.MarshalTuple(wantTuple)
		gotBytes, marshalErr := model.MarshalTuple(message.Tuple)
		if marshalErr != nil || !bytes.Equal(wantBytes, gotBytes) {
			return errors.New("source delivery payload mismatch")
		}
	}
	return nil
}

func containsAssignmentToken(set model.AssignmentSet, token model.AssignmentToken) bool {
	for _, candidate := range set.Tasks {
		if candidate == token {
			return true
		}
	}
	return false
}

func (engine *Engine) scheduleRecoveredExecutions(ctx context.Context) {
	for _, record := range engine.deliveries {
		if record.State == store.Received {
			engine.scheduleExecution(ctx, record)
		}
	}
}

func (engine *Engine) scheduleExecution(ctx context.Context, record store.DeliveryRecord) {
	if _, exists := engine.executing[record.ID]; exists {
		return
	}
	if _, failed := engine.failedTasks[record.Destination.Task]; failed {
		return
	}
	release, err := engine.gate.Enter()
	if err != nil {
		return
	}
	assignment, ok := engine.currentRunning(record.ID.Tuple.JobID)
	if !ok || record.CoordinatorEpoch != engine.repository.CurrentFence() || record.AssignmentRevision != assignment.Assignment.Revision || record.AssignmentDigest != assignment.Assignment.Digest || record.Destination.WorkerID != engine.localNode || record.Destination.WorkerEpoch != engine.localEpoch || !containsAssignmentToken(assignment.Assignment, record.Destination) {
		release()
		return
	}
	stage, ok := findStage(assignment.Topology, record.Destination.Task.StageID)
	if !ok || stage.Role == model.StageSource {
		release()
		return
	}
	job := executionJob{record: record.Clone(), operator: stage.Operator, release: release}
	select {
	case engine.executorJobs <- job:
		engine.executing[record.ID] = struct{}{}
	default:
		release()
	}
}

func (engine *Engine) executorWorker(ctx context.Context) {
	defer engine.workers.Done()
	for job := range engine.executorJobs {
		if ctx.Err() != nil {
			job.release()
			continue
		}
		outputs, err := engine.execute(ctx, job.operator, job.record.Tuple)
		result := executionResult{job: job, outputs: outputs, err: err}
		select {
		case engine.executorResults <- result:
		case <-ctx.Done():
			job.release()
		}
	}
}

func (engine *Engine) handleExecutionResult(result executionResult) error {
	defer result.job.release()
	delete(engine.executing, result.job.record.ID)
	if errors.Is(result.err, context.Canceled) {
		return nil
	}
	if result.err != nil {
		return engine.persistFailure(result.job.record, model.FailureOperator, result.err)
	}
	assignment, ok := engine.repository.InstalledAssignment(result.job.record.ID.Tuple.JobID)
	if !ok {
		return errors.New("execution result lost installed assignment")
	}
	stage, stageOK := findStage(assignment.Topology, result.job.record.Destination.Task.StageID)
	if !stageOK {
		return errors.New("execution result lost installed stage")
	}
	if stage.Role == model.StageSink && !engine.currentSinkAuthority(assignment, result.job.record) {
		// The deterministic output is volatile until MarkProcessed. An authority
		// change therefore discards it without creating stale result custody.
		return nil
	}
	outboxes, err := deriveOutboxes(assignment, result.job.record, result.outputs)
	if err != nil {
		return engine.persistFailure(result.job.record, model.FailureOperator, err)
	}
	if err = engine.repository.MarkProcessed(result.job.record.ID, result.outputs, outboxes); err != nil {
		return engine.ownerError("persist processed delivery", err)
	}
	record := result.job.record.Clone()
	record.State = store.Processed
	record.Outputs = cloneTuples(result.outputs)
	record.OutboxIDs = make([]model.DeliveryID, 0, len(outboxes))
	for _, outbox := range outboxes {
		record.OutboxIDs = append(record.OutboxIDs, outbox.ID)
		owned := outbox.Clone()
		engine.outboxes[owned.ID] = &ownedOutbox{record: owned}
		if engine.parents[owned.ID] == nil {
			engine.parents[owned.ID] = make(map[model.DeliveryID]struct{})
		}
		engine.parents[owned.ID][record.ID] = struct{}{}
	}
	engine.deliveries[record.ID] = record
	if stage.Role != model.StageSink && len(outboxes) == 0 {
		if err = engine.repository.MarkCompleted(record.ID); err != nil {
			return engine.ownerError("complete empty transform", err)
		}
		record.State = store.Completed
		engine.deliveries[record.ID] = record
	}
	return nil
}

func (engine *Engine) persistFailure(record store.DeliveryRecord, code model.FailureCode, cause error) error {
	if _, exists := engine.failedTasks[record.Destination.Task]; exists {
		return nil
	}
	if engine.nextTransactionID == 0 || engine.nextTransactionID == math.MaxUint64 {
		return errors.New("worker transaction identity exhausted")
	}
	report := &model.JobFailureReport{
		JobID: record.ID.Tuple.JobID, AssignmentRevision: record.AssignmentRevision, Task: record.Destination,
		Epoch: record.CoordinatorEpoch, TransactionID: engine.nextTransactionID, Code: code,
		DetailDigest: sha256.Sum256([]byte(cause.Error())),
	}
	assignment, ok := engine.repository.InstalledAssignment(report.JobID)
	if !ok {
		return errors.New("failure references missing assignment")
	}
	report.JobControlRevision = assignment.JobControlRevision
	event := model.WorkerEvent{WorkerID: record.Destination.WorkerID, WorkerEpoch: record.Destination.WorkerEpoch, TransactionID: engine.nextTransactionID, Kind: model.WorkerEventFailure, Failure: report}
	if err := engine.repository.PersistEvent(event); err != nil {
		return engine.ownerError("persist failure event", err)
	}
	engine.nextTransactionID++
	engine.failedTasks[record.Destination.Task] = struct{}{}
	engine.eventQueue = append(engine.eventQueue, event)
	return nil
}

func findStage(topology model.ValidatedTopology, stageID uint16) (model.StageSpec, bool) {
	for _, stage := range topology.Spec().Stages {
		if stage.StageID == stageID {
			return stage, true
		}
	}
	return model.StageSpec{}, false
}

func cloneTuples(input []model.Tuple) []model.Tuple {
	result := make([]model.Tuple, len(input))
	for index, tuple := range input {
		encoded, _ := model.MarshalTuple(tuple)
		result[index], _ = model.UnmarshalTuple(encoded)
	}
	return result
}
