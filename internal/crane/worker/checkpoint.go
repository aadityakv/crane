package worker

import (
	"context"
	"errors"
	"math"
	"sort"

	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
	"github.com/aadityakv/crane/internal/crane/store"
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
		assignment, ok := engine.installedAssignment(source.JobID)
		if !ok || assignment.CoordinatorEpoch != engine.installedFence() {
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

// applyCheckpoint applies one authenticated committed-watermark notice on the
// serialized owner under the Task 24 defect #2 ruling. The notice arrived over
// a valid current-fence authenticated +3 session, so it is the current
// coordinator's authoritative statement of the replicated committed watermark:
// CONFIRM (equal) answers an equal-watermark resend without mutation
// regardless of authority age, CONFIRM (below) answers a stale resend without
// mutation, byte-exact duplicates still flow through the durable store, and
// ADOPT persists a strictly higher watermark (or a first watermark for a
// reassigned owner) under the current authority proof with no pending
// CompletionReport required.
func (engine *Engine) applyCheckpoint(notice protocol.CheckpointNotice) error {
	cursor, hasCursor := engine.sources[notice.Notice.Source]
	if hasCursor && notice.Notice.Watermark == cursor.Watermark {
		proof := cursor.CheckpointAuthority
		if cursor.CheckpointRevision == 0 && proof == (store.CheckpointAuthority{}) {
			assignment, current := engine.installedAssignment(notice.Notice.JobID)
			if !current || assignment.CoordinatorEpoch != engine.installedFence() || notice.Notice.Epoch != assignment.CoordinatorEpoch || notice.JobControlRevision != assignment.JobControlRevision || notice.AssignmentRevision != assignment.Assignment.Revision || notice.AssignmentDigest != assignment.Assignment.Digest {
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
		if notice.Notice.RaftIndex == cursor.RaftIndex && notice.Notice.Epoch == proof.CoordinatorEpoch && notice.JobControlRevision == proof.JobControlRevision && notice.AssignmentRevision == proof.AssignmentRevision && notice.AssignmentDigest == proof.AssignmentDigest && proof.SourceToken.Task == notice.Notice.Source {
			return engine.repository.ApplyCheckpoint(notice.Notice)
		}
		// CONFIRM (equal): the committed watermark is already durable; the
		// resent send-time authority may legitimately differ from the retained
		// proof, so the notice is confirmed without any mutation.
		return nil
	}
	if hasCursor && notice.Notice.Watermark < cursor.Watermark {
		// CONFIRM (below): a stale resend under the durable committed cursor.
		return nil
	}
	assignment, ok := engine.installedAssignment(notice.Notice.JobID)
	if !ok || assignment.CoordinatorEpoch != engine.installedFence() || notice.Notice.Epoch != assignment.CoordinatorEpoch || notice.JobControlRevision != assignment.JobControlRevision || notice.AssignmentRevision != assignment.Assignment.Revision || notice.AssignmentDigest != assignment.Assignment.Digest {
		return errors.New("checkpoint notice authority does not match durable installation")
	}
	token, tokenOK := findAssignmentToken(assignment.Assignment, notice.Notice.Source)
	if !tokenOK {
		return errors.New("checkpoint notice source is not installed")
	}
	eof, err := model.SourceEOF(assignment.Topology, notice.Notice.Source)
	if err != nil || notice.Notice.Watermark > eof {
		return errors.New("checkpoint notice watermark is outside the installed topology")
	}
	if notice.Notice.Watermark == 0 {
		// The zero watermark is trivially committed (Task 19 ruling): nothing
		// to persist or compact.
		return nil
	}
	if !hasCursor {
		cursor = store.SourceCursor{Source: notice.Notice.Source, NextSequence: 1, EOF: eof}
	}
	if event, exists := engine.completionReports[notice.Notice.Source]; exists && event.Completion != nil {
		report := event.Completion
		if report.Digest == model.CompletionReportDigest(*report) && report.JobID == notice.Notice.JobID && report.JobControlRevision == notice.JobControlRevision && report.AssignmentRevision == notice.AssignmentRevision && report.Token.Task == notice.Notice.Source && report.Token.WorkerID == engine.localNode && report.Token.WorkerEpoch == engine.localEpoch && report.Epoch == notice.Notice.Epoch && report.ExpectedCheckpointRevision == cursor.CheckpointRevision && report.Prior == cursor.Watermark && report.New == notice.Notice.Watermark && report.EOF == cursor.EOF {
			if err := engine.repository.ApplyCheckpoint(notice.Notice); err != nil {
				return engine.ownerError("persist checkpoint notice", err)
			}
			cursor.Watermark = notice.Notice.Watermark
			cursor.RaftIndex = notice.Notice.RaftIndex
			cursor.CheckpointRevision = report.ExpectedCheckpointRevision + 1
			cursor.CheckpointAuthority = store.CheckpointAuthority{JobControlRevision: report.JobControlRevision, AssignmentRevision: report.AssignmentRevision, AssignmentDigest: notice.AssignmentDigest, SourceToken: report.Token, CoordinatorEpoch: report.Epoch}
			engine.sources[cursor.Source] = cursor
			delete(engine.completionReports, cursor.Source)
			engine.compactCheckpoint(cursor.Source, cursor.Watermark)
			return nil
		}
	}
	// ADOPT: the notice watermark strictly exceeds the durable cursor (or no
	// cursor exists), so the coordinator's fail-closed committed-watermark
	// validation is the commit proof; the cursor persists under the current
	// authority with no pending CompletionReport required.
	if cursor.CheckpointRevision == math.MaxUint64 {
		return errors.New("checkpoint revision exhausted")
	}
	if err := engine.repository.ApplyCheckpoint(notice.Notice); err != nil {
		return engine.ownerError("persist checkpoint notice", err)
	}
	pending, pendingExists := engine.completionReports[notice.Notice.Source]
	cursor.Watermark = notice.Notice.Watermark
	cursor.RaftIndex = notice.Notice.RaftIndex
	cursor.CheckpointRevision++
	cursor.CheckpointAuthority = store.CheckpointAuthority{JobControlRevision: notice.JobControlRevision, AssignmentRevision: notice.AssignmentRevision, AssignmentDigest: notice.AssignmentDigest, SourceToken: token, CoordinatorEpoch: notice.Notice.Epoch}
	if cursor.NextSequence <= cursor.Watermark {
		cursor.NextSequence = cursor.Watermark + 1
	}
	engine.sources[cursor.Source] = cursor
	if pendingExists && pending.Completion != nil && pending.Completion.New <= notice.Notice.Watermark {
		delete(engine.completionReports, cursor.Source)
	}
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
	assignment, ok := engine.installedAssignment(report.JobID)
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
