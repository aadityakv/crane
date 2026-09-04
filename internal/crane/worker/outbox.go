package worker

import (
	"bytes"
	"context"
	"errors"
	"math"
	"sort"
	"time"

	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
	"github.com/aadityakv/crane/internal/crane/store"
)

type ownedOutbox struct {
	record  store.OutboxRecord
	sending bool
}

type sendJob struct {
	record store.OutboxRecord
}

type sendResult struct {
	id model.DeliveryID
}

type dispatchStart struct {
	id       model.DeliveryID
	started  time.Time
	response chan dispatchResponse
}

type dispatchDisposition uint8

const (
	dispatchSend dispatchDisposition = iota + 1
	dispatchSkip
)

type dispatchResponse struct {
	disposition dispatchDisposition
	err         error
}

func (engine *Engine) scheduleOutboxes(ctx context.Context, now time.Time) {
	ids := make([]model.DeliveryID, 0, len(engine.outboxes))
	for id := range engine.outboxes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return deliveryIDLess(ids[i], ids[j]) })
	for _, id := range ids {
		outbox := engine.outboxes[id]
		if outbox.record.Completed || outbox.sending || retryDeadlineAfter(outbox.record, now) {
			continue
		}
		select {
		case engine.sendJobs <- sendJob{record: outbox.record.Clone()}:
			outbox.sending = true
		default:
			return
		}
	}
}

func (engine *Engine) senderWorker(ctx context.Context) {
	defer engine.workers.Done()
	for job := range engine.sendJobs {
		if ctx.Err() != nil {
			continue
		}
		response := make(chan dispatchResponse, 1)
		start := dispatchStart{id: job.record.ID, started: engine.clock.Now(), response: response}
		select {
		case engine.dispatchStarts <- start:
		case <-ctx.Done():
			continue
		}
		select {
		case decision := <-response:
			if decision.err != nil || decision.disposition != dispatchSend {
				continue
			}
		case <-ctx.Done():
			continue
		}
		message := engine.emissionForOutbox(job.record)
		_ = engine.sender.Send(ctx, message)
		select {
		case engine.sendResults <- sendResult{id: job.record.ID}:
		case <-ctx.Done():
		}
	}
}

func (engine *Engine) handleDispatchStart(start dispatchStart) dispatchResponse {
	outbox, ok := engine.outboxes[start.id]
	if !ok {
		if engine.compactedIdentity(start.id) {
			// A committed checkpoint compacted this outbox after its sender
			// job was queued. The checkpoint owns the durable outcome, so
			// the overtaken dispatch is a benign no-send exactly like a
			// Completed ACK overtaking a queued handshake (Task 25).
			return dispatchResponse{disposition: dispatchSkip}
		}
		return dispatchResponse{err: errors.New("sender dispatched unknown outbox")}
	}
	if outbox.record.Completed {
		// A Completed ACK may overtake a sender handshake that was already
		// queued. Completion owns the durable outcome, so this stale dispatch
		// is a benign no-send rather than an owner-fatal invariant violation.
		return dispatchResponse{disposition: dispatchSkip}
	}
	if !outbox.sending {
		return dispatchResponse{err: errors.New("sender dispatched inactive outbox")}
	}
	interval := engine.acceptedRetry
	if outbox.record.Accepted {
		interval = engine.completedRetry
	}
	deadline, err := retryDeadline(start.started, interval)
	if err != nil {
		return dispatchResponse{err: err}
	}
	if err = engine.repository.MarkOutboxDispatched(start.id, deadline); err != nil {
		return dispatchResponse{err: engine.ownerError("persist outbox dispatch", err)}
	}
	outbox.record.RetryDeadlineUnixNano = deadline
	return dispatchResponse{disposition: dispatchSend}
}

// compactedIdentity reports whether the durable source cursor's committed
// watermark already covers the delivery identity, i.e. compactCheckpoint
// retired it from the owner maps.
func (engine *Engine) compactedIdentity(id model.DeliveryID) bool {
	cursor, exists := engine.sources[id.Tuple.SourceTask]
	return exists && id.Tuple.SourceSequence != 0 && id.Tuple.SourceSequence <= cursor.Watermark
}

func (engine *Engine) handleSendResult(result sendResult) {
	outbox, ok := engine.outboxes[result.id]
	if !ok {
		return
	}
	outbox.sending = false
}

func (engine *Engine) receiveACK(ack protocol.TupleACK) error {
	outbox, ok := engine.outboxes[ack.DeliveryID]
	if !ok {
		return errors.New("ACK references unknown durable outbox")
	}
	if !engine.outboxACKEnvelope(outbox.record, ack) {
		return errors.New("ACK envelope does not match durable outbox")
	}
	if outbox.record.Completed {
		return nil
	}
	if ack.Status == protocol.TupleAccepted {
		if outbox.record.Accepted {
			return nil
		}
		deadline, err := retryDeadline(engine.clock.Now(), engine.completedRetry)
		if err != nil {
			return err
		}
		if err = engine.repository.MarkOutboxAccepted(outbox.record.ID, deadline); err != nil {
			return err
		}
		outbox.record.Accepted = true
		outbox.record.RetryDeadlineUnixNano = deadline
		return nil
	}
	if ack.Status != protocol.TupleCompleted {
		return errors.New("unknown ACK status")
	}
	if err := engine.repository.MarkOutboxCompleted(outbox.record.ID); err != nil {
		return err
	}
	outbox.record.Completed = true
	outbox.sending = false
	return engine.completeSatisfiedParents()
}

// completeSatisfiedParents closes the crash window between durable downstream
// completion and durable upstream completion. It also repairs a Processed
// transform with no downstream edges after restart; sinks remain owned by the
// independently reviewed result-completion slice.
func (engine *Engine) completeSatisfiedParents() error {
	ids := make([]model.DeliveryID, 0, len(engine.deliveries))
	for id, parent := range engine.deliveries {
		if parent.State == store.Processed {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return deliveryIDLess(ids[i], ids[j]) })
	for _, id := range ids {
		parent := engine.deliveries[id]
		complete := true
		for _, childID := range parent.OutboxIDs {
			child, present := engine.outboxes[childID]
			if !present || !child.record.Completed {
				complete = false
				break
			}
		}
		if !complete {
			continue
		}
		assignment, present := engine.installedAssignment(parent.ID.Tuple.JobID)
		if !present {
			return errors.New("completed outbox parent has no installed assignment")
		}
		stage, present := findStage(assignment.Topology, parent.Destination.Task.StageID)
		if !present {
			return errors.New("completed outbox parent has no installed stage")
		}
		if stage.Role == model.StageSink {
			continue
		}
		if err := engine.repository.MarkCompleted(parent.ID); err != nil {
			return err
		}
		parent.State = store.Completed
		engine.deliveries[parent.ID] = parent
	}
	return nil
}

func deriveOutboxes(assignment store.InstalledAssignment, parent store.DeliveryRecord, outputs []model.Tuple) ([]store.OutboxRecord, error) {
	if uint64(len(outputs)) > model.LimitsV1().MaxOperatorOutputs {
		return nil, errors.New("operator output count exceeds v1 bound")
	}
	result := make([]store.OutboxRecord, 0)
	for outputIndex, tuple := range outputs {
		if outputIndex > math.MaxUint16 {
			return nil, errors.New("operator output ordinal overflow")
		}
		for _, edge := range assignment.Topology.Spec().Edges {
			if edge.SourceStageID != parent.Destination.Task.StageID {
				continue
			}
			child := model.DeriveChildTupleID(parent.ID.Tuple, parent.Destination.Task, edge.EdgeID, uint16(outputIndex))
			partitions, err := model.Route(assignment.Topology, edge, child, tuple)
			if err != nil {
				return nil, err
			}
			for _, partition := range partitions {
				task := model.TaskID{JobID: parent.ID.Tuple.JobID, StageID: edge.DestinationStageID, Partition: partition}
				destination, ok := findAssignmentToken(assignment.Assignment, task)
				if !ok {
					return nil, errors.New("derived route has no destination assignment")
				}
				result = append(result, store.OutboxRecord{ID: model.DeliveryID{Tuple: child, EdgeID: edge.EdgeID, DestinationTask: task}, Tuple: cloneTuple(tuple), Producer: parent.Destination, Destination: destination, AssignmentRevision: parent.AssignmentRevision, AssignmentDigest: parent.AssignmentDigest, CoordinatorEpoch: parent.CoordinatorEpoch})
			}
		}
	}
	if uint64(len(result)) > model.LimitsV1().MaxDerivedDeliveries {
		return nil, errors.New("derived outbox count exceeds v1 bound")
	}
	sort.Slice(result, func(i, j int) bool { return deliveryIDLess(result[i].ID, result[j].ID) })
	for index := 1; index < len(result); index++ {
		if result[index-1].ID == result[index].ID {
			return nil, errors.New("duplicate derived outbox identity")
		}
	}
	return result, nil
}

func (engine *Engine) emitSources(ctx context.Context) error {
	for engine.incompleteOutboxes() < engine.maxPendingOutboxes {
		emitted := false
		jobs := make([]model.JobID, 0, len(engine.jobs))
		for job := range engine.jobs {
			jobs = append(jobs, job)
		}
		sort.Slice(jobs, func(i, j int) bool { return bytes.Compare(jobs[i][:], jobs[j][:]) < 0 })
		for _, job := range jobs {
			assignment, ok := engine.currentRunning(job)
			if !ok {
				continue
			}
			for _, token := range assignment.Assignment.Tasks {
				if token.WorkerID != engine.localNode || token.WorkerEpoch != engine.localEpoch {
					continue
				}
				stage, exists := findStage(assignment.Topology, token.Task.StageID)
				if !exists || stage.Role != model.StageSource {
					continue
				}
				cursor, exists := engine.sources[token.Task]
				if !exists {
					eof, err := model.SourceEOF(assignment.Topology, token.Task)
					if err != nil {
						return err
					}
					cursor = store.SourceCursor{Source: token.Task, NextSequence: 1, EOF: eof}
					if eof == 0 {
						release, enterErr := engine.gate.Enter()
						if enterErr != nil {
							continue
						}
						current, currentOK := engine.currentRunning(job)
						if !currentOK || current.Assignment.Revision != assignment.Assignment.Revision || current.Assignment.Digest != assignment.Assignment.Digest || !containsAssignmentToken(current.Assignment, token) {
							release()
							continue
						}
						if persistErr := engine.repository.AdvanceSource(cursor, nil); persistErr != nil {
							release()
							return engine.ownerError("record empty source", persistErr)
						}
						engine.sources[token.Task] = cursor
						release()
						continue
					}
				}
				if cursor.NextSequence == 0 || cursor.NextSequence > cursor.EOF {
					continue
				}
				release, err := engine.gate.Enter()
				if err != nil {
					continue
				}
				current, currentOK := engine.currentRunning(job)
				if !currentOK || current.Assignment.Revision != assignment.Assignment.Revision || current.Assignment.Digest != assignment.Assignment.Digest || !containsAssignmentToken(current.Assignment, token) {
					release()
					continue
				}
				tuple, present, tupleErr := model.SourceTuple(current.Topology, token.Task, cursor.NextSequence)
				if tupleErr != nil {
					release()
					return tupleErr
				}
				if !present {
					release()
					continue
				}
				outboxes, deriveErr := deriveSourceOutboxes(current, token, cursor.NextSequence, tuple)
				if deriveErr != nil {
					release()
					return deriveErr
				}
				if len(outboxes) > engine.maxPendingOutboxes-engine.incompleteOutboxes() {
					release()
					continue
				}
				if cursor.NextSequence == math.MaxUint64 {
					release()
					return errors.New("source sequence overflow")
				}
				next := cursor
				next.NextSequence++
				if persistErr := engine.repository.AdvanceSource(next, outboxes); persistErr != nil {
					release()
					return engine.ownerError("advance source", persistErr)
				}
				engine.sources[token.Task] = next
				for _, record := range outboxes {
					owned := record.Clone()
					engine.outboxes[owned.ID] = &ownedOutbox{record: owned}
				}
				release()
				emitted = true
				if engine.incompleteOutboxes() >= engine.maxPendingOutboxes {
					break
				}
			}
			if engine.incompleteOutboxes() >= engine.maxPendingOutboxes {
				break
			}
		}
		if !emitted {
			break
		}
	}
	return nil
}

func retryDeadlineAfter(record store.OutboxRecord, now time.Time) bool {
	return record.RetryDeadlineUnixNano != 0 && time.Unix(0, record.RetryDeadlineUnixNano).After(now)
}

func retryDeadline(start time.Time, interval time.Duration) (int64, error) {
	wall := start.Round(0)
	nano := wall.UnixNano()
	if !time.Unix(0, nano).Equal(wall) {
		return 0, errors.New("dispatch clock is outside UnixNano range")
	}
	if interval <= 0 || nano > math.MaxInt64-int64(interval) {
		return 0, errors.New("dispatch retry deadline overflows")
	}
	deadline := nano + int64(interval)
	if deadline == 0 {
		return 0, errors.New("dispatch retry deadline collides with unset value")
	}
	return deadline, nil
}

func deriveSourceOutboxes(assignment store.InstalledAssignment, source model.AssignmentToken, sequence uint64, tuple model.Tuple) ([]store.OutboxRecord, error) {
	tupleID := model.DeriveSourceTupleID(assignment.Assignment.JobID, source.Task, sequence)
	if err := tupleID.Validate(); err != nil {
		return nil, err
	}
	result := make([]store.OutboxRecord, 0)
	for _, edge := range assignment.Topology.Spec().Edges {
		if edge.SourceStageID != source.Task.StageID {
			continue
		}
		partitions, err := model.Route(assignment.Topology, edge, tupleID, tuple)
		if err != nil {
			return nil, err
		}
		for _, partition := range partitions {
			task := model.TaskID{JobID: source.Task.JobID, StageID: edge.DestinationStageID, Partition: partition}
			destination, ok := findAssignmentToken(assignment.Assignment, task)
			if !ok {
				return nil, errors.New("source route has no destination assignment")
			}
			result = append(result, store.OutboxRecord{ID: model.DeliveryID{Tuple: tupleID, EdgeID: edge.EdgeID, DestinationTask: task}, Tuple: cloneTuple(tuple), Producer: source, Destination: destination, AssignmentRevision: assignment.Assignment.Revision, AssignmentDigest: assignment.Assignment.Digest, CoordinatorEpoch: assignment.CoordinatorEpoch})
		}
	}
	sort.Slice(result, func(i, j int) bool { return deliveryIDLess(result[i].ID, result[j].ID) })
	return result, nil
}

func (engine *Engine) incompleteOutboxes() int {
	count := 0
	for _, outbox := range engine.outboxes {
		if !outbox.record.Completed {
			count++
		}
	}
	return count
}

func findAssignmentToken(set model.AssignmentSet, task model.TaskID) (model.AssignmentToken, bool) {
	for _, token := range set.Tasks {
		if token.Task == task {
			return token, true
		}
	}
	return model.AssignmentToken{}, false
}

// emissionForOutbox builds one durable outbox's outbound message. A retained
// outbox whose assignment identity still matches the current installation
// but whose durable branding is strictly ordered before the current fence
// re-derives its emission under the CURRENT envelope (Task 24 defect #5 and
// #4 rulings): emissions carry the current coordinator and assignment, never
// the stale ones.
func (engine *Engine) emissionForOutbox(record store.OutboxRecord) protocol.TupleDelivery {
	message := deliveryMessageForOutbox(record)
	fence := engine.repository.CurrentFence()
	assignment, ok := engine.repository.InstalledAssignment(record.ID.Tuple.JobID)
	if !ok || assignment.CoordinatorEpoch != fence || assignment.SchedulingState != model.Running {
		return message
	}
	if !retainedEnvelope(assignment, fence, record.AssignmentRevision, record.AssignmentDigest, record.CoordinatorEpoch) {
		return message
	}
	// A retained outbox re-derives its emission under the current envelope
	// (fence, revision, digest, current producer and destination tokens) when
	// its own producing task incarnation is unchanged and the definition
	// re-validates byte-exactly (Task 24 defect #5 and #4 rulings).
	readopted, ok := readoptedDeliveryMessage(assignment, fence, true, record.ID, record.Tuple, record.Producer, record.Destination, record.AssignmentRevision, record.AssignmentDigest, record.CoordinatorEpoch)
	if !ok {
		return message
	}
	return readopted
}

// outboxACKEnvelope reports whether an ACK's envelope binds the durable outbox:
// either the outbox's own retained envelope or the current-envelope emission
// it re-derives to.
func (engine *Engine) outboxACKEnvelope(record store.OutboxRecord, ack protocol.TupleACK) bool {
	if ack.Assignment.JobID != record.ID.Tuple.JobID {
		return false
	}
	if ack.Destination == record.Destination && ack.Assignment.Revision == record.AssignmentRevision && ack.Assignment.Digest == record.AssignmentDigest && ack.Coordinator == record.CoordinatorEpoch {
		return true
	}
	emission := engine.emissionForOutbox(record)
	return ack.Destination == emission.Destination && ack.Assignment == emission.Assignment && ack.Coordinator == emission.Coordinator
}

func deliveryMessageForOutbox(record store.OutboxRecord) protocol.TupleDelivery {
	return protocol.TupleDelivery{DeliveryID: record.ID, Tuple: record.Tuple, Producer: record.Producer, Destination: record.Destination, Assignment: protocol.AssignmentSetIdentity{JobID: record.ID.Tuple.JobID, Revision: record.AssignmentRevision, Digest: record.AssignmentDigest}, Coordinator: record.CoordinatorEpoch}
}

func cloneTuple(tuple model.Tuple) model.Tuple {
	encoded, _ := model.MarshalTuple(tuple)
	owned, _ := model.UnmarshalTuple(encoded)
	return owned
}

func deliveryIDLess(left, right model.DeliveryID) bool {
	if comparison := bytes.Compare(left.Tuple.JobID[:], right.Tuple.JobID[:]); comparison != 0 {
		return comparison < 0
	}
	if comparison := compareTask(left.Tuple.SourceTask, right.Tuple.SourceTask); comparison != 0 {
		return comparison < 0
	}
	if left.Tuple.SourceSequence != right.Tuple.SourceSequence {
		return left.Tuple.SourceSequence < right.Tuple.SourceSequence
	}
	if comparison := bytes.Compare(left.Tuple.PathDigest[:], right.Tuple.PathDigest[:]); comparison != 0 {
		return comparison < 0
	}
	if left.EdgeID != right.EdgeID {
		return left.EdgeID < right.EdgeID
	}
	return compareTask(left.DestinationTask, right.DestinationTask) < 0
}

func compareTask(left, right model.TaskID) int {
	if comparison := bytes.Compare(left.JobID[:], right.JobID[:]); comparison != 0 {
		return comparison
	}
	if left.StageID < right.StageID {
		return -1
	}
	if left.StageID > right.StageID {
		return 1
	}
	if left.Partition < right.Partition {
		return -1
	}
	if left.Partition > right.Partition {
		return 1
	}
	return 0
}
