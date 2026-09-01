package worker

import (
	"context"
	"errors"
	"math"
	"sort"

	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
	"github.com/aaditya/cs425mp3/internal/crane/store"
)

type checkpointCommand struct {
	notice   protocol.CheckpointNotice
	response chan error
}

type eventAckCommand struct {
	through  uint64
	response chan error
}

// AcknowledgeEvents durably retires coordinator-resolved worker events on the
// owner. The repository independently proves completion notices were applied
// and failures reached durable Closed state before removing any event.
func (engine *Engine) AcknowledgeEvents(ctx context.Context, through uint64) error {
	if ctx == nil {
		return errors.New("nil event acknowledgment context")
	}
	if through == 0 {
		return errors.New("zero event acknowledgment cursor")
	}
	response := make(chan error, 1)
	if err := engine.enqueue(eventAckCommand{through: through, response: response}, ctx); err != nil {
		return err
	}
	select {
	case err := <-response:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-engine.done:
		return ErrNotReady
	}
}

// ApplyCheckpoint applies one authenticated committed notice on the serialized
// owner. The durable repository independently correlates the notice to the
// exact pending CompletionReport before it compacts any replay state.
func (engine *Engine) ApplyCheckpoint(ctx context.Context, notice protocol.CheckpointNotice) error {
	if ctx == nil {
		return errors.New("nil checkpoint context")
	}
	encoded, err := protocol.MarshalCheckpointNotice(notice)
	if err != nil {
		return err
	}
	owned, err := protocol.UnmarshalCheckpointNotice(encoded)
	if err != nil {
		return err
	}
	response := make(chan error, 1)
	if err := engine.enqueue(checkpointCommand{notice: owned, response: response}, ctx); err != nil {
		return err
	}
	select {
	case err := <-response:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-engine.done:
		return ErrNotReady
	}
}

func (engine *Engine) publishContiguousCompletions() error {
	sources := make([]model.TaskID, 0, len(engine.sources))
	for source := range engine.sources {
		sources = append(sources, source)
	}
	sort.Slice(sources, func(i, j int) bool { return taskIDLess(sources[i], sources[j]) })
	for _, source := range sources {
		cursor := engine.sources[source]
		if cursor.EOF == 0 || cursor.Watermark >= cursor.EOF || engine.pendingCompletion(source) {
			continue
		}
		assignment, ok := engine.repository.InstalledAssignment(source.JobID)
		if !ok || assignment.CoordinatorEpoch != engine.repository.CurrentFence() {
			continue
		}
		token, ok := findAssignmentToken(assignment.Assignment, source)
		if !ok || token.WorkerID != engine.localNode || token.WorkerEpoch != engine.localEpoch {
			continue
		}
		if cursor.NextSequence == 0 {
			return errors.New("source cursor has zero next sequence")
		}
		last := cursor.NextSequence - 1
		if last > cursor.EOF {
			last = cursor.EOF
		}
		watermark := cursor.Watermark
		for sequence := cursor.Watermark + 1; sequence <= last; sequence++ {
			tuple, exists, err := model.SourceTuple(assignment.Topology, source, sequence)
			if err != nil {
				return err
			}
			if !exists {
				break
			}
			outboxes, err := deriveSourceOutboxes(assignment, token, sequence, tuple)
			if err != nil {
				return err
			}
			complete := true
			for _, expected := range outboxes {
				owned, present := engine.outboxes[expected.ID]
				if !present || !owned.record.Completed {
					complete = false
					break
				}
			}
			if !complete {
				break
			}
			watermark = sequence
			if sequence == math.MaxUint64 {
				break
			}
		}
		if watermark == cursor.Watermark {
			continue
		}
		if engine.nextTransactionID == 0 || engine.nextTransactionID == math.MaxUint64 {
			return errors.New("worker transaction identity exhausted")
		}
		report := &model.CompletionReport{JobID: source.JobID, JobControlRevision: assignment.JobControlRevision, AssignmentRevision: assignment.Assignment.Revision, Source: source, Token: token, Epoch: assignment.CoordinatorEpoch, ExpectedCheckpointRevision: cursor.CheckpointRevision, Prior: cursor.Watermark, New: watermark, EOF: cursor.EOF, WorkerTransactionID: engine.nextTransactionID}
		report.Digest = model.CompletionReportDigest(*report)
		event := model.WorkerEvent{WorkerID: engine.localNode, WorkerEpoch: engine.localEpoch, TransactionID: engine.nextTransactionID, Kind: model.WorkerEventCompletion, Completion: report}
		if err := engine.repository.PersistEvent(event); err != nil {
			return engine.ownerError("persist completion event", err)
		}
		engine.nextTransactionID++
		engine.completionReports[source] = event
		engine.eventQueue = append(engine.eventQueue, event)
	}
	return nil
}

func (engine *Engine) pendingCompletion(source model.TaskID) bool {
	_, ok := engine.completionReports[source]
	return ok
}

func (engine *Engine) applyCheckpoint(notice protocol.CheckpointNotice) error {
	cursor, ok := engine.sources[notice.Notice.Source]
	if !ok {
		return errors.New("checkpoint notice references unknown source")
	}
	if notice.Notice.Watermark == cursor.Watermark {
		proof := cursor.CheckpointAuthority
		if cursor.CheckpointRevision == 0 && proof == (store.CheckpointAuthority{}) {
			assignment, current := engine.repository.InstalledAssignment(notice.Notice.JobID)
			if !current || assignment.CoordinatorEpoch != engine.repository.CurrentFence() || notice.Notice.Epoch != assignment.CoordinatorEpoch || notice.JobControlRevision != assignment.JobControlRevision || notice.AssignmentRevision != assignment.Assignment.Revision || notice.AssignmentDigest != assignment.Assignment.Digest {
				return errors.New("legacy checkpoint authority does not match durable installation")
			}
			event, exists := engine.completionReports[notice.Notice.Source]
			report := event.Completion
			if !exists || report == nil || report.ExpectedCheckpointRevision == math.MaxUint64 || report.New != cursor.Watermark || report.EOF != cursor.EOF || report.JobControlRevision != notice.JobControlRevision || report.AssignmentRevision != notice.AssignmentRevision || report.Token.Task != notice.Notice.Source || report.Epoch != notice.Notice.Epoch || report.Digest != model.CompletionReportDigest(*report) {
				return store.ErrCheckpointAuthorityUnavailable
			}
			if err := engine.repository.ApplyCheckpoint(notice.Notice); err != nil {
				return engine.ownerError("migrate legacy checkpoint proof", err)
			}
			cursor.CheckpointRevision = report.ExpectedCheckpointRevision + 1
			cursor.CheckpointAuthority = store.CheckpointAuthority{JobControlRevision: report.JobControlRevision, AssignmentRevision: report.AssignmentRevision, AssignmentDigest: notice.AssignmentDigest, SourceToken: report.Token, CoordinatorEpoch: report.Epoch}
			engine.sources[cursor.Source] = cursor
			return nil
		}
		if notice.Notice.RaftIndex != cursor.RaftIndex || notice.Notice.Epoch != proof.CoordinatorEpoch || notice.JobControlRevision != proof.JobControlRevision || notice.AssignmentRevision != proof.AssignmentRevision || notice.AssignmentDigest != proof.AssignmentDigest || proof.SourceToken.Task != notice.Notice.Source {
			return model.ErrIdentityReuse
		}
		return engine.repository.ApplyCheckpoint(notice.Notice)
	}
	assignment, ok := engine.repository.InstalledAssignment(notice.Notice.JobID)
	if !ok || assignment.CoordinatorEpoch != engine.repository.CurrentFence() || notice.Notice.Epoch != assignment.CoordinatorEpoch || notice.JobControlRevision != assignment.JobControlRevision || notice.AssignmentRevision != assignment.Assignment.Revision || notice.AssignmentDigest != assignment.Assignment.Digest {
		return errors.New("checkpoint notice authority does not match durable installation")
	}
	event, exists := engine.completionReports[notice.Notice.Source]
	report := event.Completion
	if !exists || report == nil || report.Digest != model.CompletionReportDigest(*report) || report.JobID != notice.Notice.JobID || report.JobControlRevision != notice.JobControlRevision || report.AssignmentRevision != notice.AssignmentRevision || report.Token.Task != notice.Notice.Source || report.Token.WorkerID != engine.localNode || report.Token.WorkerEpoch != engine.localEpoch || report.Epoch != notice.Notice.Epoch || report.ExpectedCheckpointRevision != cursor.CheckpointRevision || report.Prior != cursor.Watermark || report.New != notice.Notice.Watermark || report.EOF != cursor.EOF {
		return errors.New("checkpoint notice lacks exact pending completion proof")
	}
	if err := engine.repository.ApplyCheckpoint(notice.Notice); err != nil {
		return engine.ownerError("persist checkpoint notice", err)
	}
	cursor.Watermark = notice.Notice.Watermark
	cursor.RaftIndex = notice.Notice.RaftIndex
	cursor.CheckpointRevision++
	cursor.CheckpointAuthority = store.CheckpointAuthority{JobControlRevision: report.JobControlRevision, AssignmentRevision: report.AssignmentRevision, AssignmentDigest: notice.AssignmentDigest, SourceToken: report.Token, CoordinatorEpoch: report.Epoch}
	engine.sources[cursor.Source] = cursor
	delete(engine.completionReports, cursor.Source)
	engine.compactCheckpoint(cursor.Source, cursor.Watermark)
	return nil
}

func (engine *Engine) compactCheckpoint(source model.TaskID, watermark uint64) {
	for id := range engine.deliveries {
		if id.Tuple.SourceTask == source && id.Tuple.SourceSequence <= watermark {
			delete(engine.deliveries, id)
		}
	}
	for id := range engine.outboxes {
		if id.Tuple.SourceTask == source && id.Tuple.SourceSequence <= watermark {
			delete(engine.outboxes, id)
			delete(engine.parents, id)
		}
	}
}

func (engine *Engine) acknowledgeEvents(through uint64) error {
	for source, event := range engine.completionReports {
		if event.TransactionID <= through && !engine.completionEventResolved(source, event) {
			return errors.New("completion event acknowledgment lacks applied or superseded proof")
		}
	}
	if err := engine.repository.AcknowledgeEvents(through); err != nil {
		return engine.ownerError("acknowledge worker events", err)
	}
	retained := engine.eventQueue[:0]
	for _, event := range engine.eventQueue {
		if event.TransactionID > through {
			retained = append(retained, event)
		}
	}
	engine.eventQueue = retained
	for source, event := range engine.completionReports {
		if event.TransactionID <= through {
			delete(engine.completionReports, source)
		}
	}
	return nil
}

func (engine *Engine) completionEventResolved(source model.TaskID, event model.WorkerEvent) bool {
	report := event.Completion
	if report == nil {
		return false
	}
	if cursor, ok := engine.sources[source]; ok {
		proof := cursor.CheckpointAuthority
		if report.ExpectedCheckpointRevision != math.MaxUint64 && cursor.Watermark == report.New && cursor.CheckpointRevision == report.ExpectedCheckpointRevision+1 && proof.JobControlRevision == report.JobControlRevision && proof.AssignmentRevision == report.AssignmentRevision && proof.SourceToken == report.Token && proof.CoordinatorEpoch == report.Epoch {
			return true
		}
	}
	assignment, ok := engine.repository.InstalledAssignment(report.JobID)
	if !ok || assignment.JobControlRevision < report.JobControlRevision || assignment.Assignment.Revision < report.AssignmentRevision || assignment.JobControlRevision == report.JobControlRevision && assignment.Assignment.Revision == report.AssignmentRevision {
		return false
	}
	current, ok := findAssignmentToken(assignment.Assignment, source)
	if !ok {
		return false
	}
	if assignment.Assignment.Revision == report.AssignmentRevision {
		return current == report.Token
	}
	return current.AssignmentRevision == assignment.Assignment.Revision
}

func taskIDLess(left, right model.TaskID) bool {
	if left.JobID != right.JobID {
		for index := range left.JobID {
			if left.JobID[index] != right.JobID[index] {
				return left.JobID[index] < right.JobID[index]
			}
		}
	}
	if left.StageID != right.StageID {
		return left.StageID < right.StageID
	}
	return left.Partition < right.Partition
}
