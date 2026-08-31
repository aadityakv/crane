package model

import (
	"encoding/hex"
	"testing"
)

func TestProgressSchemasRejectCrossJobAndIncompleteEvents(t *testing.T) {
	job := JobID{1}
	task := TaskID{JobID: job, StageID: 1}
	token := AssignmentToken{Task: task, WorkerID: 2, WorkerEpoch: WorkerEpoch{2}, Attempt: 1, SpecificationHash: [32]byte{3}, AssignmentRevision: 4}
	epoch := CoordinatorEpoch{Term: 1, BeginIndex: 2, Coordinator: 3, Nonce: [16]byte{4}}
	report := CompletionReport{JobID: job, JobControlRevision: 1, AssignmentRevision: 4, Source: task, Token: token, Epoch: epoch, ExpectedCheckpointRevision: 0, Prior: 5, New: 7, EOF: 9, WorkerTransactionID: 1}
	report.Digest = CompletionReportDigest(report)
	if got := hex.EncodeToString(report.Digest[:]); got != "2e7bcaea7016bc9a34074b80dadc01ed33b477d6b0f8dedcaf162b6c135e12a2" {
		t.Fatalf("completion digest = %s", got)
	}
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

func TestCompletionReportDigestChangesForEveryCanonicalField(t *testing.T) {
	job := JobID{1}
	task := TaskID{JobID: job, StageID: 1}
	base := CompletionReport{JobID: job, JobControlRevision: 1, AssignmentRevision: 4, Source: task, Token: AssignmentToken{Task: task, WorkerID: 2, WorkerEpoch: WorkerEpoch{2}, Attempt: 1, SpecificationHash: [32]byte{3}, AssignmentRevision: 4}, Epoch: CoordinatorEpoch{Term: 1, BeginIndex: 2, Coordinator: 3, Nonce: [16]byte{4}}, Prior: 5, New: 7, EOF: 9, WorkerTransactionID: 1}
	want := CompletionReportDigest(base)
	mutations := map[string]func(*CompletionReport){
		"job": func(r *CompletionReport) { r.JobID[0]++ }, "job revision": func(r *CompletionReport) { r.JobControlRevision++ }, "assignment revision": func(r *CompletionReport) { r.AssignmentRevision++ },
		"source": func(r *CompletionReport) { r.Source.Partition++ }, "token task": func(r *CompletionReport) { r.Token.Task.Partition++ }, "worker": func(r *CompletionReport) { r.Token.WorkerID++ }, "worker epoch": func(r *CompletionReport) { r.Token.WorkerEpoch[0]++ }, "attempt": func(r *CompletionReport) { r.Token.Attempt++ }, "specification": func(r *CompletionReport) { r.Token.SpecificationHash[0]++ }, "token revision": func(r *CompletionReport) { r.Token.AssignmentRevision++ },
		"term": func(r *CompletionReport) { r.Epoch.Term++ }, "begin index": func(r *CompletionReport) { r.Epoch.BeginIndex++ }, "coordinator": func(r *CompletionReport) { r.Epoch.Coordinator++ }, "nonce": func(r *CompletionReport) { r.Epoch.Nonce[0]++ },
		"checkpoint revision": func(r *CompletionReport) { r.ExpectedCheckpointRevision++ }, "prior": func(r *CompletionReport) { r.Prior++ }, "new": func(r *CompletionReport) { r.New++ }, "eof": func(r *CompletionReport) { r.EOF++ }, "transaction": func(r *CompletionReport) { r.WorkerTransactionID++ },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := base
			mutate(&changed)
			if CompletionReportDigest(changed) == want {
				t.Fatal("field mutation did not change digest")
			}
		})
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
	notice.Watermark = 0
	if err := notice.Validate(); err != nil {
		t.Fatalf("empty-source checkpoint zero rejected: %v", err)
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
