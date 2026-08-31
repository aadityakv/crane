package model

import "testing"

func TestProgressSchemasRejectCrossJobAndIncompleteEvents(t *testing.T) {
	job := JobID{1}
	task := TaskID{JobID: job, StageID: 1}
	token := AssignmentToken{Task: task, WorkerID: 2, WorkerEpoch: WorkerEpoch{2}, Attempt: 1, SpecificationHash: [32]byte{3}, AssignmentRevision: 4}
	epoch := CoordinatorEpoch{Term: 1, BeginIndex: 2, Coordinator: 3, Nonce: [16]byte{4}}
	report := CompletionReport{JobID: job, JobControlRevision: 1, AssignmentRevision: 4, Source: task, Token: token, Epoch: epoch, ExpectedCheckpointRevision: 0, Prior: 5, New: 7, EOF: 9, WorkerTransactionID: 1}
	report.Digest = CompletionReportDigest(report)
	if err := report.Validate(); err != nil {
		t.Fatal(err)
	}
	event := WorkerEvent{WorkerID: 2, WorkerEpoch: token.WorkerEpoch, TransactionID: 1, Kind: WorkerEventCompletion, Completion: &report}
	if err := event.Validate(); err != nil {
		t.Fatal(err)
	}
	event.Failure = &JobFailureReport{JobID: job}
	if err := event.Validate(); err == nil {
		t.Fatal("event with two bodies accepted")
	}
	report.Source.JobID = JobID{9}
	if err := report.Validate(); err == nil {
		t.Fatal("cross-job source accepted")
	}
}

func TestCompletionReportRequiresAdvanceAndExactCanonicalDigest(t *testing.T) {
	job := JobID{1}
	task := TaskID{JobID: job, StageID: 1}
	report := CompletionReport{
		JobID: job, JobControlRevision: 1, AssignmentRevision: 2,
		Source: task,
		Token:  AssignmentToken{Task: task, WorkerID: 2, WorkerEpoch: WorkerEpoch{2}, Attempt: 1, SpecificationHash: [32]byte{3}, AssignmentRevision: 2},
		Epoch:  CoordinatorEpoch{Term: 1, BeginIndex: 2, Coordinator: 3, Nonce: [16]byte{4}},
		Prior:  5, New: 5, EOF: 9, WorkerTransactionID: 1,
	}
	report.Digest = CompletionReportDigest(report)
	if err := report.Validate(); err == nil {
		t.Fatal("completion without a checkpoint advance accepted")
	}
	report.New = 6
	report.Digest = CompletionReportDigest(report)
	if err := report.Validate(); err != nil {
		t.Fatalf("initial checkpoint revision rejected: %v", err)
	}
	report.Digest[0] ^= 1
	if err := report.Validate(); err == nil {
		t.Fatal("completion digest mismatch accepted")
	}
	report.Token.Task.Partition = 1
	report.Digest = CompletionReportDigest(report)
	if err := report.Validate(); err == nil {
		t.Fatal("completion source and assignment token task mismatch accepted")
	}
}

func TestCheckpointNoticeAndFailureValidation(t *testing.T) {
	job := JobID{1}
	task := TaskID{JobID: job, StageID: 1}
	epoch := CoordinatorEpoch{Term: 1, BeginIndex: 2, Coordinator: 3, Nonce: [16]byte{4}}
	notice := CheckpointNotice{JobID: job, Source: task, Watermark: 8, RaftIndex: 10, Epoch: epoch}
	if err := notice.Validate(); err != nil {
		t.Fatal(err)
	}
	failure := JobFailureReport{JobID: job, JobControlRevision: 1, AssignmentRevision: 2, Task: AssignmentToken{Task: task, WorkerID: 2, WorkerEpoch: WorkerEpoch{2}, Attempt: 1, SpecificationHash: [32]byte{3}, AssignmentRevision: 2}, Epoch: epoch, TransactionID: 3, Code: FailureOperator, DetailDigest: [32]byte{4}}
	if err := failure.Validate(); err != nil {
		t.Fatal(err)
	}
	failure.Code = 0
	if err := failure.Validate(); err == nil {
		t.Fatal("zero failure code accepted")
	}
}
