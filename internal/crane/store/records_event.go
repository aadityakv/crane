package store

import (
	"errors"
	"math"

	"github.com/aadityakv/crane/internal/crane/model"
)

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
