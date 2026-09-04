package state

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/aadityakv/crane/internal/crane/model"
)

const failureEventDigestDomain = "cs425/crane/job-failure-event/v1\x00"

// SourceEOFRecord retains one immutable source bound and its subject revision.
type SourceEOFRecord struct {
	EOF      uint64 // EOF is the immutable exclusive sequence bound.
	Revision uint64 // Revision is the independent source-EOF revision.
}

// CheckpointRecord retains monotonic source progress and its subject revision.
type CheckpointRecord struct {
	Watermark uint64 // Watermark is the highest durably completed source sequence.
	Revision  uint64 // Revision is the independent checkpoint revision.
}

type workerEventKey struct {
	WorkerID    uint16
	WorkerEpoch model.WorkerEpoch
}

type workerEventCursor struct {
	TransactionID uint64
	Digest        [32]byte
}

// RecordSourceEOF commits the topology-derived source bound exactly once.
type RecordSourceEOF struct {
	Envelope Envelope     // Envelope carries source-EOF and coordinator fences.
	Source   model.TaskID // Source selects an immutable topology source task.
	EOF      uint64       // EOF is the topology-derived exclusive bound.
}

// AdvanceCheckpoint applies one globally ordered, current-token completion event.
type AdvanceCheckpoint struct {
	Envelope Envelope               // Envelope carries checkpoint and coordinator fences.
	Report   model.CompletionReport // Report binds token, cursor, prior, and new progress.
}

// NewRecordSourceEOF builds the immutable topology-derived bound for one source.
func NewRecordSourceEOF(id InternalCommandID, expectedRevision uint64, source model.TaskID, eof uint64, fence ...model.CoordinatorEpoch) (RecordSourceEOF, error) {
	command := RecordSourceEOF{Source: source, EOF: eof}
	command.Envelope = newInternalEnvelope(CommandRecordSourceEOF, SubjectKey{Kind: SubjectSourceEOF, JobID: source.JobID, TaskID: source}, id, expectedRevision, fence...)
	command.Envelope.Internal.Digest = internalDigest(command.Envelope, recordSourceEOFTarget(command))
	return command, command.Validate()
}

// NewAdvanceCheckpoint builds one current-token globally ordered progress event.
func NewAdvanceCheckpoint(id InternalCommandID, expectedRevision uint64, report model.CompletionReport, fence ...model.CoordinatorEpoch) (AdvanceCheckpoint, error) {
	command := AdvanceCheckpoint{Report: report}
	command.Envelope = newInternalEnvelope(CommandAdvanceCheckpoint, SubjectKey{Kind: SubjectSourceCheckpoint, JobID: report.JobID, TaskID: report.Source}, id, expectedRevision, fence...)
	command.Envelope.Internal.Digest = internalDigest(command.Envelope, advanceCheckpointTarget(command))
	return command, command.Validate()
}

func (command RecordSourceEOF) Validate() error {
	if err := command.Envelope.Validate(); err != nil {
		return err
	}
	key := SubjectKey{Kind: SubjectSourceEOF, JobID: command.Source.JobID, TaskID: command.Source}
	if command.Envelope.Kind != CommandRecordSourceEOF || command.Envelope.Internal == nil || command.Envelope.Internal.Subject != key || command.Envelope.Internal.ExpectedRevision != 0 {
		return fmt.Errorf("%w: source EOF subject mismatch", ErrInvalidCommandSubject)
	}
	if err := command.Source.Validate(); err != nil {
		return err
	}
	if command.Envelope.Internal.Digest != internalDigest(command.Envelope, recordSourceEOFTarget(command)) {
		return ErrCommandDigestMismatch
	}
	return nil
}

func (command AdvanceCheckpoint) Validate() error {
	if err := command.Envelope.Validate(); err != nil {
		return err
	}
	key := SubjectKey{Kind: SubjectSourceCheckpoint, JobID: command.Report.JobID, TaskID: command.Report.Source}
	if command.Envelope.Kind != CommandAdvanceCheckpoint || command.Envelope.Internal == nil || command.Envelope.Internal.Subject != key || command.Envelope.Internal.ExpectedRevision != command.Report.ExpectedCheckpointRevision {
		return fmt.Errorf("%w: checkpoint subject mismatch", ErrInvalidCommandSubject)
	}
	if err := command.Report.Validate(); err != nil {
		return err
	}
	if command.Envelope.Internal.Digest != internalDigest(command.Envelope, advanceCheckpointTarget(command)) {
		return ErrCommandDigestMismatch
	}
	return nil
}

func failureEventDigest(report model.JobFailureReport) [32]byte {
	return sha256.Sum256(append([]byte(failureEventDigestDomain), failureReportBytes(report)...))
}

func validateNextWorkerEvent(cursors map[workerEventKey]workerEventCursor, workerID uint16, epoch model.WorkerEpoch, transaction uint64, digest [32]byte) error {
	if workerID == 0 || epoch.Validate() != nil || transaction == 0 || digest == ([32]byte{}) {
		return errors.New("invalid worker event cursor")
	}
	current := cursors[workerEventKey{WorkerID: workerID, WorkerEpoch: epoch}]
	if transaction <= current.TransactionID {
		return errors.New("worker event transaction is not globally increasing")
	}
	return nil
}

func (machine *Machine) applyRecordSourceEOFLocked(command RecordSourceEOF) ([]byte, error) {
	record, exists := machine.jobs[command.Source.JobID]
	currentRevision := uint64(0)
	if exists {
		currentRevision = record.SourceEOFs[command.Source].Revision
	}
	key := command.Envelope.Internal.Subject
	target := recordSourceEOFTarget(command)
	return machine.applyInternalAtRevisionLocked(command.Envelope, target, currentRevision, func(nextRevision uint64) (mutationPlan, error) {
		if !exists {
			result, err := marshalBusinessResult(ResultNotFound, key, currentRevision, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, err
		}
		if record.Lifecycle != JobPending || record.Assignment != nil {
			result, err := marshalBusinessResult(ResultInvalidTransition, key, currentRevision, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, err
		}
		topology, err := model.DecodeTopology(record.TopologyBytes)
		if err != nil {
			return mutationPlan{}, fmt.Errorf("impossible retained topology: %w", err)
		}
		want, err := model.SourceEOF(topology, command.Source)
		if err != nil || want != command.EOF {
			result, resultErr := marshalBusinessResult(ResultInvalidTarget, key, currentRevision, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, resultErr
		}
		candidate := cloneJobRecord(record)
		candidate.SourceEOFs[command.Source] = SourceEOFRecord{EOF: command.EOF, Revision: nextRevision}
		result, err := marshalBusinessResult(ResultSuccess, key, nextRevision, model.CoordinatorEpoch{})
		return mutationPlan{result: result, stateDelta: int64(sourceEOFEntryEstimatedBytes), commit: func() { machine.jobs[candidate.JobID] = candidate }}, err
	})
}

// coordinatorEpochOrderedBefore reports the established coordinator-epoch
// order (term, then begin index): left was committed strictly before right.
func coordinatorEpochOrderedBefore(left, right model.CoordinatorEpoch) bool {
	if left.Term != right.Term {
		return left.Term < right.Term
	}
	return left.BeginIndex < right.BeginIndex
}

func (machine *Machine) applyAdvanceCheckpointLocked(command AdvanceCheckpoint) ([]byte, error) {
	report := command.Report
	record, exists := machine.jobs[report.JobID]
	currentRevision := uint64(0)
	if exists {
		currentRevision = record.Checkpoints[report.Source].Revision
	}
	key := command.Envelope.Internal.Subject
	target := advanceCheckpointTarget(command)
	return machine.applyInternalAtRevisionLocked(command.Envelope, target, currentRevision, func(nextRevision uint64) (mutationPlan, error) {
		if !exists {
			result, err := marshalBusinessResult(ResultNotFound, key, currentRevision, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, err
		}
		checkpoint, checkpointExists := record.Checkpoints[report.Source]
		eof, eofExists := record.SourceEOFs[report.Source]
		token, tokenExists := assignmentToken(record.Assignment, report.Source)
		validLifecycle := record.Lifecycle == JobRunning || record.Lifecycle == JobDraining
		// The epoch fence prevents a superseded coordinator from mutating
		// replicated state; it does not invalidate work a worker durably
		// proved under an assignment that is still current. A report whose
		// epoch was committed at or before the current one re-enters when
		// every other condition holds exactly; anything ordered after the
		// committed epoch is a superseded coordinator's mutation attempt.
		currentEpoch := report.Epoch == machine.coordinatorEpoch || coordinatorEpochOrderedBefore(report.Epoch, machine.coordinatorEpoch)
		validFence := record.Assignment != nil && len(record.NeedsReassignment) == 0 && report.JobControlRevision == record.JobControlRevision && report.AssignmentRevision == record.Assignment.Revision && currentEpoch && tokenExists && token == report.Token
		validAdvance := eofExists && report.EOF == eof.EOF && report.ExpectedCheckpointRevision == checkpoint.Revision && report.Prior == checkpoint.Watermark && report.New > checkpoint.Watermark && report.New <= eof.EOF
		worker, workerExists := machine.workers[report.Token.WorkerID]
		validWorker := workerExists && worker.Epoch == report.Token.WorkerEpoch && worker.State != WorkerOffline
		if !validLifecycle || !validFence || !validAdvance || !validWorker {
			result, err := marshalBusinessResult(ResultInvalidTarget, key, currentRevision, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, err
		}
		cursorKey := workerEventKey{WorkerID: report.Token.WorkerID, WorkerEpoch: report.Token.WorkerEpoch}
		if err := validateNextWorkerEvent(machine.workerEvents, cursorKey.WorkerID, cursorKey.WorkerEpoch, report.WorkerTransactionID, report.Digest); err != nil {
			result, resultErr := marshalBusinessResult(ResultStaleWorkerEvent, key, currentRevision, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, resultErr
		}
		candidate := cloneJobRecord(record)
		candidate.Checkpoints[report.Source] = CheckpointRecord{Watermark: report.New, Revision: nextRevision}
		_, cursorExists := machine.workerEvents[cursorKey]
		delta := int64(0)
		if !checkpointExists {
			delta += int64(checkpointEntryEstimatedBytes)
		}
		if !cursorExists {
			delta += int64(workerEventEntryEstimatedBytes)
		}
		result, err := marshalBusinessResult(ResultSuccess, key, nextRevision, model.CoordinatorEpoch{})
		return mutationPlan{result: result, stateDelta: delta, commit: func() {
			machine.jobs[candidate.JobID] = candidate
			machine.workerEvents[cursorKey] = workerEventCursor{TransactionID: report.WorkerTransactionID, Digest: report.Digest}
		}}, err
	})
}

func assignmentToken(set *model.AssignmentSet, task model.TaskID) (model.AssignmentToken, bool) {
	if set == nil {
		return model.AssignmentToken{}, false
	}
	for _, token := range set.Tasks {
		comparison := bytes.Compare(taskBytes(token.Task), taskBytes(task))
		if comparison == 0 {
			return token, true
		}
		if comparison > 0 {
			break
		}
	}
	return model.AssignmentToken{}, false
}
