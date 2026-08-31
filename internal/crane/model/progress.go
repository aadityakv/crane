package model

import (
	"crypto/sha256"
	"errors"
)

const completionReportDigestDomain = "cs425/crane/completion-report/v1\x00"

// SchedulingState is the committed admission state for a job.
type SchedulingState uint8

const (
	SchedulingClosed SchedulingState = iota + 1
	SchedulingRunning
	SchedulingDraining
)

// Canonical scheduling-state names.
const (
	Closed   = SchedulingClosed
	Running  = SchedulingRunning
	Draining = SchedulingDraining
)

// CompletionReport advances one source checkpoint under explicit fencing.
type CompletionReport struct {
	JobID                      JobID
	JobControlRevision         uint64
	AssignmentRevision         uint64
	Source                     TaskID
	Token                      AssignmentToken
	Epoch                      CoordinatorEpoch
	ExpectedCheckpointRevision uint64
	Prior, New, EOF            uint64
	WorkerTransactionID        uint64
	Digest                     [32]byte
}

// CheckpointNotice is the committed durable checkpoint observed by workers.
type CheckpointNotice struct {
	JobID     JobID
	Source    TaskID
	Watermark uint64
	RaftIndex uint64
	Epoch     CoordinatorEpoch
}

// FailureCode is a stable deterministic worker failure category.
type FailureCode uint16

const (
	FailureOperator FailureCode = iota + 1
	FailureTupleInvalid
	FailureStorage
)

// JobFailureReport records one fenced deterministic worker failure.
type JobFailureReport struct {
	JobID              JobID
	JobControlRevision uint64
	AssignmentRevision uint64
	Task               AssignmentToken
	Epoch              CoordinatorEpoch
	TransactionID      uint64
	Code               FailureCode
	DetailDigest       [32]byte
}

// WorkerEventKind selects the sole body of one globally ordered worker event.
type WorkerEventKind uint8

const (
	WorkerEventCompletion WorkerEventKind = iota + 1
	WorkerEventFailure
)

// WorkerEvent is one transaction in a worker epoch's global event stream.
type WorkerEvent struct {
	WorkerID      uint16
	WorkerEpoch   WorkerEpoch
	TransactionID uint64
	Kind          WorkerEventKind
	Completion    *CompletionReport
	Failure       *JobFailureReport
}

// CompletionReportDigest hashes every canonical completion field except itself.
func CompletionReportDigest(report CompletionReport) [32]byte {
	encoded := appendJobAndRevision([]byte(completionReportDigestDomain), report.JobID, report.JobControlRevision, report.AssignmentRevision)
	encoded = appendTaskID(encoded, report.Source)
	encoded = appendAssignmentToken(encoded, report.Token)
	encoded = appendCoordinatorEpoch(encoded, report.Epoch)
	encoded = appendUint64(encoded, report.ExpectedCheckpointRevision)
	encoded = appendUint64(encoded, report.Prior)
	encoded = appendUint64(encoded, report.New)
	encoded = appendUint64(encoded, report.EOF)
	encoded = appendUint64(encoded, report.WorkerTransactionID)
	return sha256.Sum256(encoded)
}

// Validate checks all completion cross-references and monotonic fields.
func (report CompletionReport) Validate() error {
	if err := report.JobID.Validate(); err != nil {
		return err
	}
	if report.JobControlRevision == 0 || report.AssignmentRevision == 0 || report.WorkerTransactionID == 0 {
		return errors.New("zero completion revision or transaction")
	}
	if err := report.Source.Validate(); err != nil || report.Source.JobID != report.JobID {
		return errors.New("invalid or foreign completion source")
	}
	if err := report.Token.Validate(); err != nil || report.Token.Task != report.Source || report.Token.AssignmentRevision != report.AssignmentRevision {
		return errors.New("completion token mismatch")
	}
	if err := report.Epoch.Validate(); err != nil {
		return err
	}
	if report.New <= report.Prior || report.EOF != 0 && report.New > report.EOF {
		return errors.New("invalid checkpoint advance")
	}
	if report.Digest == ([32]byte{}) || report.Digest != CompletionReportDigest(report) {
		return errors.New("completion digest mismatch")
	}
	return nil
}

func appendJobAndRevision(destination []byte, job JobID, jobRevision, assignmentRevision uint64) []byte {
	destination = append(destination, job[:]...)
	destination = appendUint64(destination, jobRevision)
	return appendUint64(destination, assignmentRevision)
}

func appendAssignmentToken(destination []byte, token AssignmentToken) []byte {
	destination = appendTaskID(destination, token.Task)
	destination = appendUint16(destination, token.WorkerID)
	destination = append(destination, token.WorkerEpoch[:]...)
	destination = appendUint64(destination, token.Attempt)
	destination = append(destination, token.SpecificationHash[:]...)
	return appendUint64(destination, token.AssignmentRevision)
}

func appendCoordinatorEpoch(destination []byte, epoch CoordinatorEpoch) []byte {
	destination = appendUint64(destination, epoch.Term)
	destination = appendUint64(destination, epoch.BeginIndex)
	destination = appendUint16(destination, epoch.Coordinator)
	return append(destination, epoch.Nonce[:]...)
}

// Validate checks one committed checkpoint notice.
func (notice CheckpointNotice) Validate() error {
	if err := notice.JobID.Validate(); err != nil {
		return err
	}
	if err := notice.Source.Validate(); err != nil || notice.Source.JobID != notice.JobID {
		return errors.New("invalid checkpoint source")
	}
	if notice.RaftIndex == 0 {
		return errors.New("zero checkpoint Raft index")
	}
	return notice.Epoch.Validate()
}

// Validate checks one failure report and its assignment/coordinator fences.
func (report JobFailureReport) Validate() error {
	if err := report.JobID.Validate(); err != nil {
		return err
	}
	if report.JobControlRevision == 0 || report.AssignmentRevision == 0 || report.TransactionID == 0 {
		return errors.New("zero failure revision or transaction")
	}
	if err := report.Task.Validate(); err != nil || report.Task.Task.JobID != report.JobID || report.Task.AssignmentRevision != report.AssignmentRevision {
		return errors.New("failure token mismatch")
	}
	if err := report.Epoch.Validate(); err != nil {
		return err
	}
	if report.Code < FailureOperator || report.Code > FailureStorage {
		return errors.New("unknown failure code")
	}
	if report.DetailDigest == ([32]byte{}) {
		return errors.New("zero failure detail digest")
	}
	return nil
}

// Validate checks event-body exclusivity and worker transaction fencing.
func (event WorkerEvent) Validate() error {
	if event.WorkerID == 0 || event.TransactionID == 0 {
		return errors.New("zero worker event identity")
	}
	if err := event.WorkerEpoch.Validate(); err != nil {
		return err
	}
	switch event.Kind {
	case WorkerEventCompletion:
		if event.Completion == nil || event.Failure != nil {
			return errors.New("completion event body mismatch")
		}
		if err := event.Completion.Validate(); err != nil {
			return err
		}
		if event.Completion.Token.WorkerID != event.WorkerID || event.Completion.Token.WorkerEpoch != event.WorkerEpoch || event.Completion.WorkerTransactionID != event.TransactionID {
			return errors.New("completion event worker fence mismatch")
		}
	case WorkerEventFailure:
		if event.Failure == nil || event.Completion != nil {
			return errors.New("failure event body mismatch")
		}
		if err := event.Failure.Validate(); err != nil {
			return err
		}
		if event.Failure.Task.WorkerID != event.WorkerID || event.Failure.Task.WorkerEpoch != event.WorkerEpoch || event.Failure.TransactionID != event.TransactionID {
			return errors.New("failure event worker fence mismatch")
		}
	default:
		return errors.New("unknown worker event kind")
	}
	return nil
}
