package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"crane/internal/crane/model"
	"crane/internal/crane/protocol"
	"crane/internal/crane/state"
)

// maxStatusPagesPerPoll bounds one worker's HasMore chain so a misbehaving
// worker can never pin a reconciliation pass forever.
const maxStatusPagesPerPoll = 1024

var (
	// ErrInvalidStatusPage reports a worker status page whose identity, epoch,
	// or event ordering contradicts the replicated worker record or cursor.
	ErrInvalidStatusPage = errors.New("crane worker status page is invalid")
	// ErrInvalidWorkerEvent reports a durable worker event whose own bytes are
	// inconsistent; it is never proposed and never acknowledged.
	ErrInvalidWorkerEvent = errors.New("crane worker event is invalid")
	// ErrCheckpointNotCommitted reports an attempt to announce a checkpoint
	// that does not exactly match the committed replicated record.
	ErrCheckpointNotCommitted = errors.New("crane checkpoint notice does not match committed state")
	// ErrEpochSuperseded reports that the replicated fence no longer belongs
	// to this leadership session.
	ErrEpochSuperseded = errors.New("crane coordinator epoch superseded")
)

// eventCursor is the leader-local last handled durable worker transaction for
// one exact worker incarnation. It is never authoritative: a new leadership
// session resets it and repolls the complete durable event history.
type eventCursor struct {
	epoch       model.WorkerEpoch
	transaction uint64
}

// eventCursors serializes leader-local per-worker event cursor state shared by
// the reconciliation loop and the exported polling entry points.
type eventCursors struct {
	mu     sync.Mutex
	byNode map[uint16]eventCursor
}

// reset discards every cursor so the next poll performs a complete repoll.
func (cursors *eventCursors) reset() {
	cursors.mu.Lock()
	defer cursors.mu.Unlock()
	cursors.byNode = make(map[uint16]eventCursor)
}

// aligned returns the cursor for one worker, resetting it to zero whenever the
// replicated worker epoch replaced the one the cursor was built against.
func (cursors *eventCursors) aligned(node uint16, epoch model.WorkerEpoch) eventCursor {
	cursors.mu.Lock()
	defer cursors.mu.Unlock()
	cursor, ok := cursors.byNode[node]
	if !ok || cursor.epoch != epoch {
		cursor = eventCursor{epoch: epoch}
		cursors.byNode[node] = cursor
	}
	return cursor
}

// advance records a handled transaction, keeping the highest handled value for
// the exact epoch so concurrent idempotent polls never regress the cursor.
func (cursors *eventCursors) advance(node uint16, epoch model.WorkerEpoch, transaction uint64) {
	cursors.mu.Lock()
	defer cursors.mu.Unlock()
	cursor, ok := cursors.byNode[node]
	if !ok || cursor.epoch != epoch || cursor.transaction < transaction {
		cursors.byNode[node] = eventCursor{epoch: epoch, transaction: transaction}
	}
}

// rewind restarts one worker's cursor at zero for the exact epoch so the next
// status request repolls (and stops acknowledging) the complete history.
func (cursors *eventCursors) rewind(node uint16, epoch model.WorkerEpoch) {
	cursors.mu.Lock()
	defer cursors.mu.Unlock()
	cursors.byNode[node] = eventCursor{epoch: epoch}
}

// PollWorkerEvents drains one worker's durable event pages through the paged
// +5 WorkerStatus exchange, using the last handled durable transaction ID for
// the worker's exact current epoch. Every event is fully handled — committed,
// its committed effect delivered back to the workers — before the leader-local
// cursor advances past it, so the cursor doubles as the acknowledgment the
// worker requires proof for. A lost status response is retried once from the
// zero cursor before the worker is reported unavailable.
func (actor *Actor) PollWorkerEvents(ctx context.Context, node uint16) error {
	return actor.pollWorkerEvents(ctx, node, nil)
}

// pollWorkerEvents performs one worker's bounded event drain and records the
// worker's reported process admission epoch into the leadership session so
// per-job convergence derives from worker-observed state.
func (actor *Actor) pollWorkerEvents(ctx context.Context, node uint16, session *sessionState) error {
	if ctx == nil {
		return errors.New("poll worker events: nil context")
	}
	if node == 0 {
		return errors.New("poll worker events: zero node")
	}
	view := actor.options.Machine.View()
	if view.CoordinatorEpoch == (model.CoordinatorEpoch{}) {
		return errors.New("poll worker events: no committed coordinator epoch")
	}
	record, ok := findWorker(view, node)
	if !ok || record.State == state.WorkerOffline {
		return fmt.Errorf("poll worker events: node %d has no active replicated record", node)
	}
	cursor := actor.events.aligned(node, record.Epoch)
	retried := false
	for page := 0; page < maxStatusPagesPerPoll; page++ {
		request := protocol.WorkerStatusRequest{
			CoordinatorEpoch:   view.CoordinatorEpoch,
			AfterTransactionID: cursor.transaction,
			MaxEvents:          statusPageEvents,
		}
		status, err := actor.options.Workers.Status(ctx, node, request)
		if err != nil {
			if cursor.transaction != 0 && !retried && ctx.Err() == nil {
				// A worker may refuse the acknowledgment while it still lacks
				// its deletion proof; repolling from zero keeps event reading
				// and duplicate re-handling alive without acknowledging.
				retried = true
				actor.events.rewind(node, record.Epoch)
				cursor = eventCursor{epoch: record.Epoch}
				continue
			}
			return fmt.Errorf("%w: status poll of node %d: %v", ErrWorkerUnavailable, node, err)
		}
		if !validWorkerStatusPage(node, record.Epoch, cursor.transaction, status) {
			return fmt.Errorf("%w: node %d after transaction %d", ErrInvalidStatusPage, node, cursor.transaction)
		}
		if session != nil {
			session.admission[node] = status.AdmissionEpoch
		}
		for _, event := range status.Events {
			if err := actor.HandleWorkerEvent(ctx, event); err != nil {
				return err
			}
			cursor.transaction = event.TransactionID
			actor.events.advance(node, record.Epoch, event.TransactionID)
		}
		if !status.HasMore {
			return nil
		}
	}
	return fmt.Errorf("%w: node %d exceeded the page bound", ErrInvalidStatusPage, node)
}

// validWorkerStatusPage enforces the exact worker identity and epoch, strictly
// increasing event transactions above the requested cursor, and a consistent
// page tail.
func validWorkerStatusPage(node uint16, epoch model.WorkerEpoch, after uint64, status protocol.WorkerStatus) bool {
	if status.NodeID != node || status.WorkerEpoch != epoch || status.AfterTransactionID != after || status.LastTransactionID < after {
		return false
	}
	prior := after
	for _, event := range status.Events {
		if event.WorkerID != node || event.WorkerEpoch != epoch || event.TransactionID <= prior {
			return false
		}
		prior = event.TransactionID
	}
	if len(status.Events) > 0 && status.LastTransactionID != prior {
		return false
	}
	return true
}

// HandleWorkerEvent resolves one durable worker event completely: it proposes
// the replicated effect under a stable command identity, resolves transport
// ambiguity by Barrier and View, and only reports success once the committed
// consequence — the committed checkpoint notice for a completion, the terminal
// Closed installation for a failure — has been durably delivered so the
// originating event can later prove its own acknowledgment.
func (actor *Actor) HandleWorkerEvent(ctx context.Context, event model.WorkerEvent) error {
	if ctx == nil {
		return errors.New("handle worker event: nil context")
	}
	if event.WorkerID == 0 || event.WorkerEpoch.Validate() != nil || event.TransactionID == 0 {
		return fmt.Errorf("%w: incomplete event identity", ErrInvalidWorkerEvent)
	}
	view := actor.options.Machine.View()
	epoch := view.CoordinatorEpoch
	if epoch == (model.CoordinatorEpoch{}) {
		return errors.New("handle worker event: no committed coordinator epoch")
	}
	switch event.Kind {
	case model.WorkerEventCompletion:
		if event.Completion == nil {
			return fmt.Errorf("%w: completion event without report", ErrInvalidWorkerEvent)
		}
		report := *event.Completion
		if report.WorkerTransactionID != event.TransactionID || report.Token.WorkerID != event.WorkerID ||
			report.Token.WorkerEpoch != event.WorkerEpoch || report.Digest != model.CompletionReportDigest(report) {
			return fmt.Errorf("%w: completion report contradicts its event", ErrInvalidWorkerEvent)
		}
		return actor.handleCompletionEvent(ctx, epoch, report)
	case model.WorkerEventFailure:
		if event.Failure == nil {
			return fmt.Errorf("%w: failure event without report", ErrInvalidWorkerEvent)
		}
		report := *event.Failure
		if report.TransactionID != event.TransactionID || report.Task.WorkerID != event.WorkerID ||
			report.Task.WorkerEpoch != event.WorkerEpoch {
			return fmt.Errorf("%w: failure report contradicts its event", ErrInvalidWorkerEvent)
		}
		return actor.handleFailureEvent(ctx, epoch, report)
	default:
		return fmt.Errorf("%w: unknown event kind %d", ErrInvalidWorkerEvent, event.Kind)
	}
}

// handleCompletionEvent commits one completion report and then announces the
// committed watermark. A deterministic rejection is classified by re-reading
// the committed View: when the job is gone or terminal, or the report's token
// no longer matches the current assignment, the event is permanently stale
// and resolves as handled without any state mutation; when the committed
// watermark already covers the report, the committed notice is still
// redelivered so the originating worker gains its deletion proof; every other
// rejection on a live current assignment was transiently false (for example
// an apply-time Offline worker) and stays a retryable error with the cursor
// pinned.
func (actor *Actor) handleCompletionEvent(ctx context.Context, epoch model.CoordinatorEpoch, report model.CompletionReport) error {
	subject := state.SubjectKey{Kind: state.SubjectSourceCheckpoint, JobID: report.JobID, TaskID: report.Source}
	id := internalCommandID(state.CommandAdvanceCheckpoint, epoch, subject, report.ExpectedCheckpointRevision, report.Digest[:], uint64Bytes(report.WorkerTransactionID))
	command, err := state.NewAdvanceCheckpoint(id, report.ExpectedCheckpointRevision, report, epoch)
	if err != nil {
		// The report is well formed for its worker but not proposable under
		// the current fence; it can never commit and resolves as rejected.
		return nil
	}
	result, err := actor.proposeResolved(ctx, id, subject, command)
	if err != nil {
		return fmt.Errorf("resolve completion event: %w", err)
	}
	switch result.Code {
	case state.ResultStaleEpoch:
		return fmt.Errorf("%w: completion proposal", ErrEpochSuperseded)
	case state.ResultSuccess, state.ResultRevisionMismatch:
		view := actor.options.Machine.View()
		record, ok := findJob(view, report.JobID)
		if !ok || terminalLifecycle(record.Lifecycle) || record.Assignment == nil {
			// Terminal propagation owns the remaining closure; nothing above
			// the committed watermark exists to announce.
			return nil
		}
		if current, currentOK := currentAssignmentToken(record, report.Source); !currentOK || current != report.Token {
			// The re-read committed View proves the event can never apply:
			// its assignment was replaced under a new token.
			return nil
		}
		committed := record.Checkpoints[report.Source]
		if result.Code == state.ResultSuccess && committed.Watermark < report.New {
			return fmt.Errorf("%w: committed advance not visible", ErrCheckpointNotCommitted)
		}
		if committed.Watermark >= report.New {
			return actor.ApplyCommittedCheckpoint(ctx, report.JobID, report.Source, committed.Watermark, view.AppliedIndex)
		}
		// The job is live under the report's exact token, so the deterministic
		// rejection was transiently false and the retained event stays
		// retryable until it commits, its assignment is replaced, or the job
		// turns terminal.
		return fmt.Errorf("completion proposal for a current assignment rejected with code %d", result.Code)
	default:
		return fmt.Errorf("completion proposal rejected with code %d", result.Code)
	}
}

// handleFailureEvent commits one FailJob under a stable command identity and
// then installs the exact committed Closed terminal state on every assigned
// active worker. The originating event stays durable — the cursor never
// advances — until that terminal installation succeeded. A deterministic
// rejection against a live job is classified by re-reading the committed
// View: the event is permanently stale and resolves as handled exactly when
// its task's token no longer matches the current assignment; a live current
// assignment leaves the rejection transiently false and retryable.
func (actor *Actor) handleFailureEvent(ctx context.Context, epoch model.CoordinatorEpoch, report model.JobFailureReport) error {
	subject := state.SubjectKey{Kind: state.SubjectJobControl, JobID: report.JobID}
	id := internalCommandID(state.CommandFailJob, epoch, subject, report.JobControlRevision, report.DetailDigest[:], uint64Bytes(report.TransactionID))
	command, err := state.NewFailJob(id, report.JobControlRevision, report, epoch)
	if err != nil {
		return nil
	}
	result, err := actor.proposeResolved(ctx, id, subject, command)
	if err != nil {
		return fmt.Errorf("resolve failure event: %w", err)
	}
	switch result.Code {
	case state.ResultStaleEpoch:
		return fmt.Errorf("%w: failure proposal", ErrEpochSuperseded)
	case state.ResultSuccess, state.ResultRevisionMismatch:
		view := actor.options.Machine.View()
		record, ok := findJob(view, report.JobID)
		if !ok {
			return nil
		}
		if terminalLifecycle(record.Lifecycle) {
			if record.Assignment == nil {
				return nil
			}
			if !actor.installAssignment(ctx, epoch, record, model.Closed, false, 0) {
				return errors.New("terminal Closed installation incomplete for failed job")
			}
			return nil
		}
		if record.Assignment == nil {
			return nil
		}
		if current, currentOK := currentAssignmentToken(record, report.Task.Task); !currentOK || current != report.Task {
			// The re-read committed View proves the failure can never apply:
			// its assignment was replaced under a new token.
			return nil
		}
		// The job is live under the report's exact token, so the
		// deterministic rejection was transiently false (an apply-time
		// Offline worker, a reassignment-marker window, or a fence the job
		// will outgrow) and the retained event stays retryable.
		return fmt.Errorf("failure proposal for a current assignment rejected with code %d", result.Code)
	default:
		return fmt.Errorf("failure proposal rejected with code %d", result.Code)
	}
}

// currentAssignmentToken mirrors the replicated current-token lookup for one
// task of one job's committed assignment.
func currentAssignmentToken(record state.JobRecord, task model.TaskID) (model.AssignmentToken, bool) {
	if record.Assignment == nil {
		return model.AssignmentToken{}, false
	}
	for _, token := range record.Assignment.Tasks {
		if token.Task == task {
			return token, true
		}
	}
	return model.AssignmentToken{}, false
}

// ApplyCommittedCheckpoint validates one committed source checkpoint against
// the replicated state and delivers the exact committed notice to every
// current worker of the job. It fails closed before any send: the watermark
// must equal the committed record (or be the trivially committed zero for a
// source without one), the Raft index must be nonzero, and the job must be
// live under the current fence.
func (actor *Actor) ApplyCommittedCheckpoint(ctx context.Context, job model.JobID, source model.TaskID, watermark, raftIndex uint64) error {
	if ctx == nil {
		return errors.New("apply committed checkpoint: nil context")
	}
	if err := job.Validate(); err != nil {
		return fmt.Errorf("apply committed checkpoint: %w", err)
	}
	if err := source.Validate(); err != nil || source.JobID != job {
		return fmt.Errorf("%w: invalid source task", ErrCheckpointNotCommitted)
	}
	if raftIndex == 0 {
		return fmt.Errorf("%w: zero Raft index", ErrCheckpointNotCommitted)
	}
	view := actor.options.Machine.View()
	epoch := view.CoordinatorEpoch
	if epoch == (model.CoordinatorEpoch{}) {
		return errors.New("apply committed checkpoint: no committed coordinator epoch")
	}
	record, ok := findJob(view, job)
	if !ok || record.Assignment == nil {
		return fmt.Errorf("%w: job has no committed assignment", ErrCheckpointNotCommitted)
	}
	if record.Lifecycle != state.JobRunning && record.Lifecycle != state.JobDraining {
		return fmt.Errorf("%w: job lifecycle %d", ErrCheckpointNotCommitted, record.Lifecycle)
	}
	committed, committedExists := record.Checkpoints[source]
	if committedExists && committed.Watermark != watermark || !committedExists && watermark != 0 {
		return fmt.Errorf("%w: watermark %d is not the committed record", ErrCheckpointNotCommitted, watermark)
	}
	set := *record.Assignment
	members := actor.options.Membership.View()
	for _, node := range assignmentNodes(set) {
		if !memberActive(members, node) {
			return fmt.Errorf("%w: node %d is not an active member", ErrWorkerUnavailable, node)
		}
		notice := protocol.CheckpointNotice{
			Notice: model.CheckpointNotice{
				JobID: job, Source: source, Watermark: watermark,
				RaftIndex: raftIndex, Epoch: epoch,
			},
			JobControlRevision: record.JobControlRevision,
			AssignmentRevision: set.Revision, AssignmentDigest: set.Digest,
		}
		if err := actor.options.Workers.Checkpoint(ctx, node, notice); err != nil {
			return fmt.Errorf("deliver committed checkpoint to node %d: %w", node, err)
		}
	}
	return nil
}
