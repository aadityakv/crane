package coordinator

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"crane/internal/crane/model"
	"crane/internal/crane/state"
)

// failureWorkerEvent builds one valid failure event for the job's current
// assignment token under the current committed fence.
func failureWorkerEvent(t *testing.T, h *harness, job model.JobID, token model.AssignmentToken, transaction uint64) model.WorkerEvent {
	t.Helper()
	view := h.view()
	record, ok := h.job(job)
	if !ok {
		t.Fatal("job missing")
	}
	report := &model.JobFailureReport{
		JobID: job, JobControlRevision: record.JobControlRevision, AssignmentRevision: record.Assignment.Revision,
		Task: token, Epoch: view.CoordinatorEpoch, TransactionID: transaction,
		Code: model.FailureOperator, DetailDigest: sha256.Sum256([]byte("task-19-failure")),
	}
	return model.WorkerEvent{
		WorkerID: token.WorkerID, WorkerEpoch: token.WorkerEpoch, TransactionID: transaction,
		Kind: model.WorkerEventFailure, Failure: report,
	}
}

// committedWatermark reads the committed checkpoint watermark for one source.
func committedWatermark(h *harness, job model.JobID, source model.TaskID) uint64 {
	record, ok := h.job(job)
	if !ok {
		return 0
	}
	return record.Checkpoints[source].Watermark
}

// checkpointNoticesFor lists the recorded notice watermarks sent to one node
// for one source.
func checkpointNoticesFor(h *harness, node uint16, source model.TaskID) []uint64 {
	h.workers.mu.Lock()
	defer h.workers.mu.Unlock()
	watermarks := make([]uint64, 0)
	for index, notice := range h.workers.checkpoints {
		if h.workers.checkpointNodes[index] == node && notice.Notice.Source == source {
			watermarks = append(watermarks, notice.Notice.Watermark)
		}
	}
	return watermarks
}

func TestScheduleCommitsEveryPartitionEOFBeforeAssignmentAndEmission(t *testing.T) {
	h := newHarness(t)
	h.seedEpoch()
	h.seedWorker(2, model.WorkerEpoch{2}, 4)
	h.seedWorker(3, model.WorkerEpoch{3}, 4)
	h.addWorkerMember(2, model.WorkerEpoch{2}, 4)
	h.addWorkerMember(3, model.WorkerEpoch{3}, 4)
	topology, err := model.ValidateTopology(testTopologySpec(2))
	if err != nil {
		t.Fatalf("validate topology: %v", err)
	}
	view := h.machine.View()
	submit, err := state.NewSubmitJob(model.ClientRequestID{ClientID: model.ClientID{0x99}, Sequence: 1}, topology.Spec(), view.CoordinatorEpoch)
	if err != nil {
		t.Fatalf("seed submit: %v", err)
	}
	h.raft.applySeed(t, submit)
	job := submit.JobID()

	h.start()
	h.markReady()
	h.lead(2)
	h.waitGateOpen()

	record, ok := h.job(job)
	if !ok || record.Lifecycle != state.JobRunning {
		t.Fatalf("job = %#v", record)
	}
	for partition := uint16(0); partition < 2; partition++ {
		source := model.TaskID{JobID: job, StageID: 1, Partition: partition}
		want, eofErr := model.SourceEOF(topology, source)
		if eofErr != nil {
			t.Fatalf("recompute EOF: %v", eofErr)
		}
		got, exists := record.SourceEOFs[source]
		if !exists || got.EOF != want || got.Revision != 1 {
			t.Fatalf("partition %d EOF = %#v want %d", partition, got, want)
		}
	}
	entries := h.log.snapshot()
	if got := h.log.count("propose:eof"); got != 2 {
		t.Fatalf("EOF proposals = %d: %v", got, entries)
	}
	assertSubsequence(t, entries, "propose:eof", "propose:eof", "propose:install-assignments", "propose:transition")
	// No assignment distribution or source emission may precede the committed
	// per-partition EOFs and the committed complete assignment set.
	sawInstallProposal := false
	for _, entry := range entries {
		if entry == "propose:install-assignments" {
			sawInstallProposal = true
		}
		if strings.HasPrefix(entry, "install:") && !sawInstallProposal {
			t.Fatalf("distribution before committed assignment set: %v", entries)
		}
	}
}

func TestPollWorkerEventsDrainsPagedStatusWithPerEpochCheckpointCursor(t *testing.T) {
	h, job, _, assignment := runningHarness(t)
	token := sourceToken(t, assignment)
	node := token.WorkerID
	script := h.workers.script(node)
	events := []model.WorkerEvent{
		completionEvent(t, h, job, token, 1, 0, 0, 1, 4),
		completionEvent(t, h, job, token, 2, 1, 1, 2, 4),
		completionEvent(t, h, job, token, 3, 2, 2, 3, 4),
	}
	h.workers.mu.Lock()
	script.events = events
	script.pageSize = 1
	h.workers.mu.Unlock()

	if err := h.actor.PollWorkerEvents(context.Background(), node); err != nil {
		t.Fatalf("PollWorkerEvents: %v", err)
	}
	if got := committedWatermark(h, job, token.Task); got != 3 {
		t.Fatalf("committed watermark = %d", got)
	}
	if got := h.log.count("propose:advance"); got != 3 {
		t.Fatalf("advance proposals = %d: %v", got, h.log.snapshot())
	}
	// Every committed advance is announced to every current worker before the
	// cursor moves past the producing event.
	for _, target := range []uint16{2, 3} {
		notices := checkpointNoticesFor(h, target, token.Task)
		if len(notices) != 3 || notices[0] != 1 || notices[1] != 2 || notices[2] != 3 {
			t.Fatalf("node %d notices = %v", target, notices)
		}
	}
	// The next poll acknowledges the fully handled durable cursor.
	if err := h.actor.PollWorkerEvents(context.Background(), node); err != nil {
		t.Fatalf("second PollWorkerEvents: %v", err)
	}
	if got := h.workers.lastAck(node); got != 3 {
		t.Fatalf("acknowledged cursor = %d", got)
	}
}

func TestPollWorkerEventsEpochReplacementResetsCheckpointCursor(t *testing.T) {
	h, job, _, assignment := runningHarness(t)
	h.seedWorker(4, model.WorkerEpoch{4}, 4)
	h.addWorkerMember(4, model.WorkerEpoch{4}, 4)
	token := sourceToken(t, assignment)
	// Worker 4 reports a completion for a token it does not hold: the machine
	// rejects it deterministically and the cursor still advances.
	foreignToken := token
	foreignToken.WorkerID = 4
	foreignToken.WorkerEpoch = model.WorkerEpoch{4}
	foreign := completionEvent(t, h, job, foreignToken, 7, 0, 0, 1, 4)
	script := h.workers.script(4)
	h.workers.mu.Lock()
	script.events = []model.WorkerEvent{foreign}
	h.workers.mu.Unlock()

	if err := h.actor.PollWorkerEvents(context.Background(), 4); err != nil {
		t.Fatalf("first poll: %v", err)
	}
	if err := h.actor.PollWorkerEvents(context.Background(), 4); err != nil {
		t.Fatalf("second poll: %v", err)
	}
	if got := h.workers.lastAck(4); got != 7 {
		t.Fatalf("acknowledged cursor before replacement = %d", got)
	}

	// Replace worker 4's durable epoch; its transaction identities restart, so
	// the leader-local cursor must reset to zero for the new incarnation.
	view := h.machine.View()
	record, ok := findWorker(view, 4)
	if !ok {
		t.Fatal("worker 4 missing")
	}
	replacement := record
	replacement.Epoch = model.WorkerEpoch{4, 1}
	replacement.Revision = record.Revision + 1
	replace, err := state.NewReplaceWorkerEpoch(testCommandID("replace-4"), record.Revision, 4, record.Epoch, replacement, nil, view.CoordinatorEpoch)
	if err != nil {
		t.Fatalf("replace worker epoch: %v", err)
	}
	h.raft.applySeed(t, replace)
	freshToken := token
	freshToken.WorkerID = 4
	freshToken.WorkerEpoch = model.WorkerEpoch{4, 1}
	fresh := completionEvent(t, h, job, freshToken, 1, 0, 0, 1, 4)
	h.workers.mu.Lock()
	script.identity.WorkerEpoch = model.WorkerEpoch{4, 1}
	script.events = []model.WorkerEvent{fresh}
	h.workers.mu.Unlock()

	if err := h.actor.PollWorkerEvents(context.Background(), 4); err != nil {
		t.Fatalf("post-replacement poll: %v", err)
	}
	if got := h.workers.lastAck(4); got != 0 {
		t.Fatalf("replacement poll acknowledged stale cursor %d", got)
	}
	if err := h.actor.PollWorkerEvents(context.Background(), 4); err != nil {
		t.Fatalf("post-replacement second poll: %v", err)
	}
	if got := h.workers.lastAck(4); got != 1 {
		t.Fatalf("replacement cursor = %d", got)
	}
}

func TestPollWorkerEventsRejectsForeignEpochCheckpointPage(t *testing.T) {
	h, job, _, assignment := runningHarness(t)
	token := sourceToken(t, assignment)
	node := token.WorkerID
	script := h.workers.script(node)
	h.workers.mu.Lock()
	script.events = []model.WorkerEvent{completionEvent(t, h, job, token, 1, 0, 0, 1, 4)}
	script.identity.WorkerEpoch = model.WorkerEpoch{0xEE}
	h.workers.mu.Unlock()
	err := h.actor.PollWorkerEvents(context.Background(), node)
	if err == nil {
		t.Fatal("foreign-epoch status page accepted")
	}
	if h.log.count("propose:advance") != 0 {
		t.Fatalf("foreign-epoch page proposed progress: %v", h.log.snapshot())
	}
}

func TestHandleWorkerEventCompletionRedeliversNoticeAfterLostCheckpointSend(t *testing.T) {
	h, job, _, assignment := runningHarness(t)
	token := sourceToken(t, assignment)
	node := token.WorkerID
	script := h.workers.script(node)
	event := completionEvent(t, h, job, token, 1, 0, 0, 1, 4)
	h.workers.mu.Lock()
	script.events = []model.WorkerEvent{event}
	script.checkpointErrs = 1
	h.workers.mu.Unlock()

	// The advance commits, but the notice send is lost: the cursor must stay
	// pinned so the event is re-handled.
	if err := h.actor.PollWorkerEvents(context.Background(), node); err == nil {
		t.Fatal("lost notice delivery reported success")
	}
	if got := committedWatermark(h, job, token.Task); got != 1 {
		t.Fatalf("committed watermark = %d", got)
	}
	if err := h.actor.PollWorkerEvents(context.Background(), node); err != nil {
		t.Fatalf("retry poll: %v", err)
	}
	record, _ := h.job(job)
	if record.Checkpoints[token.Task].Revision != 1 {
		t.Fatalf("duplicate handling re-mutated checkpoint: %#v", record.Checkpoints)
	}
	notices := checkpointNoticesFor(h, node, token.Task)
	if len(notices) == 0 || notices[len(notices)-1] != 1 {
		t.Fatalf("committed notice never redelivered: %v", notices)
	}
	if err := h.actor.PollWorkerEvents(context.Background(), node); err != nil {
		t.Fatalf("ack poll: %v", err)
	}
	if got := h.workers.lastAck(node); got != 1 {
		t.Fatalf("acknowledged cursor = %d", got)
	}
}

func TestHandleWorkerEventFailureInstallsClosedBeforeCheckpointCursorAdvance(t *testing.T) {
	h, job, _, assignment := runningHarness(t)
	token := sourceToken(t, assignment)
	node := token.WorkerID
	script := h.workers.script(node)
	event := failureWorkerEvent(t, h, job, token, 1)
	h.workers.mu.Lock()
	script.events = []model.WorkerEvent{event}
	script.installErrs = 1
	h.workers.mu.Unlock()

	// The FailJob commits, but the terminal Closed installation fails: the
	// originating event stays durable and unacknowledged.
	if err := h.actor.PollWorkerEvents(context.Background(), node); err == nil {
		t.Fatal("failed terminal installation reported success")
	}
	record, ok := h.job(job)
	if !ok || record.Lifecycle != state.JobFailed {
		t.Fatalf("job after failure event = %#v", record)
	}
	failedRevision := record.JobControlRevision
	if err := h.actor.PollWorkerEvents(context.Background(), node); err != nil {
		t.Fatalf("retry poll: %v", err)
	}
	closed := h.workers.installsFor(node, model.Closed)
	if len(closed) == 0 || closed[len(closed)-1].JobControlRevision != failedRevision {
		t.Fatalf("terminal Closed installation missing: %#v", closed)
	}
	if err := h.actor.PollWorkerEvents(context.Background(), node); err != nil {
		t.Fatalf("ack poll: %v", err)
	}
	if got := h.workers.lastAck(node); got != 1 {
		t.Fatalf("acknowledged cursor = %d", got)
	}

	// Duplicate polling under a fresh leader-local cursor never creates
	// another transition.
	second, err := NewActor(ActorOptions{
		NodeID: 1, Raft: h.raft, Machine: h.machine, WorkerReady: h.workerReady,
		Membership: h.members, Workers: h.workers, Clock: h.clk,
		Nonces: &scriptedNonces{counter: 0x40}, Gate: h.gate,
		FailureGracePeriod: 100 * time.Millisecond, RescanInterval: time.Second,
	})
	if err != nil {
		t.Fatalf("second actor: %v", err)
	}
	if err := second.PollWorkerEvents(context.Background(), node); err != nil {
		t.Fatalf("duplicate poll: %v", err)
	}
	record, _ = h.job(job)
	if record.JobControlRevision != failedRevision || record.Lifecycle != state.JobFailed {
		t.Fatalf("duplicate polling created another transition: %#v", record)
	}
}

func TestHandleWorkerEventFailureAmbiguityResolvedByBarrierAndView(t *testing.T) {
	h, job, _, assignment := runningHarness(t)
	token := sourceToken(t, assignment)
	node := token.WorkerID
	script := h.workers.script(node)
	event := failureWorkerEvent(t, h, job, token, 1)
	h.workers.mu.Lock()
	script.events = []model.WorkerEvent{event}
	h.workers.mu.Unlock()
	injected := errors.New("injected ambiguous transport failure")
	h.raft.setProposeHook(func(command any) (bool, error) {
		if _, ok := command.(state.FailJob); ok {
			h.raft.proposeHook = nil
			return true, injected
		}
		return true, nil
	})

	if err := h.actor.PollWorkerEvents(context.Background(), node); err != nil {
		t.Fatalf("ambiguous failure poll: %v", err)
	}
	record, ok := h.job(job)
	if !ok || record.Lifecycle != state.JobFailed {
		t.Fatalf("job = %#v", record)
	}
	assertSubsequence(t, h.log.snapshot(), "propose:fail", "barrier")
	if got := h.log.count("propose:fail"); got != 1 {
		t.Fatalf("ambiguity retried different bytes: %v", h.log.snapshot())
	}
}

func TestApplyCommittedCheckpointFailsClosedBeforeAnySend(t *testing.T) {
	h, job, _, assignment := runningHarness(t)
	token := sourceToken(t, assignment)
	source := token.Task
	ctx := context.Background()
	view := h.machine.View()

	if err := h.actor.ApplyCommittedCheckpoint(ctx, job, source, 1, view.AppliedIndex); err == nil {
		t.Fatal("uncommitted watermark announced")
	}
	if err := h.actor.ApplyCommittedCheckpoint(ctx, job, source, 0, 0); err == nil {
		t.Fatal("zero Raft index accepted")
	}
	if len(checkpointNoticesFor(h, 2, source))+len(checkpointNoticesFor(h, 3, source)) != 0 {
		t.Fatalf("failed validation still sent notices: %#v", h.workers.checkpoints)
	}

	// The zero watermark is trivially committed for a source without a record.
	if err := h.actor.ApplyCommittedCheckpoint(ctx, job, source, 0, view.AppliedIndex); err != nil {
		t.Fatalf("trivially committed zero watermark: %v", err)
	}
	for _, node := range []uint16{2, 3} {
		notices := checkpointNoticesFor(h, node, source)
		if len(notices) != 1 || notices[0] != 0 {
			t.Fatalf("node %d zero notices = %v", node, notices)
		}
	}

	report := model.CompletionReport{
		JobID: job, JobControlRevision: 3, AssignmentRevision: assignment.Revision,
		Source: source, Token: token, Epoch: view.CoordinatorEpoch,
		ExpectedCheckpointRevision: 0, Prior: 0, New: 1, EOF: 4, WorkerTransactionID: 1,
	}
	report.Digest = model.CompletionReportDigest(report)
	advance, err := state.NewAdvanceCheckpoint(testCommandID("seed-advance", job[:]), 0, report, view.CoordinatorEpoch)
	if err != nil {
		t.Fatalf("seed advance: %v", err)
	}
	h.raft.applySeed(t, advance)
	view = h.machine.View()
	if err := h.actor.ApplyCommittedCheckpoint(ctx, job, source, 1, view.AppliedIndex); err != nil {
		t.Fatalf("exact committed watermark: %v", err)
	}
	if err := h.actor.ApplyCommittedCheckpoint(ctx, job, source, 2, view.AppliedIndex); err == nil {
		t.Fatal("watermark beyond committed record announced")
	}

	// Terminal jobs never receive checkpoint notices.
	record, _ := h.job(job)
	failure := model.JobFailureReport{
		JobID: job, JobControlRevision: record.JobControlRevision, AssignmentRevision: assignment.Revision,
		Task: token, Epoch: view.CoordinatorEpoch, TransactionID: 9,
		Code: model.FailureOperator, DetailDigest: sha256.Sum256([]byte("terminal")),
	}
	fail, err := state.NewFailJob(testCommandID("seed-fail", job[:]), record.JobControlRevision, failure, view.CoordinatorEpoch)
	if err != nil {
		t.Fatalf("seed fail: %v", err)
	}
	h.raft.applySeed(t, fail)
	if err := h.actor.ApplyCommittedCheckpoint(ctx, job, source, 1, view.AppliedIndex); err == nil {
		t.Fatal("terminal job received a checkpoint notice")
	}
}

func TestCheckpointReplayAfterLeadershipChangeAnswersDuplicatesWithoutRecommit(t *testing.T) {
	h, job, _, assignment := runningHarness(t)
	token := sourceToken(t, assignment)
	node := token.WorkerID
	h.start()
	h.markReady()
	h.lead(2)
	h.waitGateOpen()

	script := h.workers.script(node)
	event := completionEvent(t, h, job, token, 1, 0, 0, 1, 4)
	h.workers.mu.Lock()
	script.events = []model.WorkerEvent{event}
	h.workers.mu.Unlock()
	h.actor.Wake()
	h.waitFor(func() bool { return committedWatermark(h, job, token.Task) == 1 }, "committed watermark 1")
	h.actor.Wake()
	h.waitFor(func() bool { return h.workers.lastAck(node) == 1 }, "acknowledged cursor 1")
	record, _ := h.job(job)
	committedRevision := record.Checkpoints[token.Task].Revision

	// Leadership changes while the worker still durably retains the event
	// (crash after the worker notice but before deletion): the next leader
	// repolls from zero, the replicated tombstone answers the duplicate, the
	// committed notice is redelivered, and nothing above the watermark moves.
	h.follow(2)
	h.waitGateClosed()
	priorNotices := len(checkpointNoticesFor(h, node, token.Task))
	h.lead(3)
	h.waitGateOpen()
	h.actor.Wake()
	h.waitFor(func() bool { return h.workers.lastAck(node) == 1 }, "cursor reacknowledged after repoll")
	record, _ = h.job(job)
	if record.Checkpoints[token.Task].Watermark != 1 || record.Checkpoints[token.Task].Revision != committedRevision {
		t.Fatalf("duplicate replay re-committed progress: %#v", record.Checkpoints)
	}
	if got := len(checkpointNoticesFor(h, node, token.Task)); got <= priorNotices {
		t.Fatal("new leader never redelivered the committed notice")
	}

	// Crash after deletion: the worker no longer serves the event and the next
	// leader proposes nothing new.
	h.workers.mu.Lock()
	script.events = nil
	h.workers.mu.Unlock()
	priorAdvances := h.log.count("propose:advance")
	h.follow(3)
	h.waitGateClosed()
	h.lead(4)
	h.waitGateOpen()
	if got := h.log.count("propose:advance"); got != priorAdvances {
		t.Fatalf("deleted event replayed: %d -> %d", priorAdvances, got)
	}
}

func TestRescanDrivesCheckpointProgressWithoutWakeHint(t *testing.T) {
	h, job, _, assignment := runningHarness(t)
	token := sourceToken(t, assignment)
	node := token.WorkerID
	h.start()
	h.markReady()
	h.lead(2)
	h.waitGateOpen()
	script := h.workers.script(node)
	h.workers.mu.Lock()
	script.events = []model.WorkerEvent{completionEvent(t, h, job, token, 1, 0, 0, 1, 4)}
	h.workers.mu.Unlock()
	// No Wake: the guaranteed periodic rescan alone must commit the progress,
	// because the engine's local event channel is only a latency hint.
	h.rescan()
	h.waitFor(func() bool { return committedWatermark(h, job, token.Task) == 1 }, "rescan-driven checkpoint")
	if got := fmt.Sprint(h.workers.lastAck(node)); got == "" {
		t.Fatal("impossible")
	}
}

// seedFailJob commits one terminal FailJob under the current fence.
func seedFailJob(t *testing.T, h *harness, job model.JobID, token model.AssignmentToken) {
	t.Helper()
	view := h.view()
	record, ok := h.job(job)
	if !ok {
		t.Fatal("job missing")
	}
	failure := model.JobFailureReport{
		JobID: job, JobControlRevision: record.JobControlRevision, AssignmentRevision: record.Assignment.Revision,
		Task: token, Epoch: view.CoordinatorEpoch, TransactionID: 9,
		Code: model.FailureOperator, DetailDigest: sha256.Sum256([]byte("terminal-before-poll")),
	}
	fail, err := state.NewFailJob(testCommandID("seed-stale-fail", job[:]), record.JobControlRevision, failure, view.CoordinatorEpoch)
	if err != nil {
		t.Fatalf("seed fail: %v", err)
	}
	h.raft.applySeed(t, fail)
}

func TestReconcileResolvesPermanentlyStaleCompletionEventAndOpensGate(t *testing.T) {
	h, job, _, assignment := runningHarness(t)
	token := sourceToken(t, assignment)
	node := token.WorkerID
	script := h.workers.script(node)
	h.workers.mu.Lock()
	script.events = []model.WorkerEvent{completionEvent(t, h, job, token, 1, 0, 0, 1, 4)}
	h.workers.mu.Unlock()
	// The job failed while the worker durably retained its completion event:
	// the repolled event is deterministically rejected forever, and only the
	// committed View proves that staleness.
	seedFailJob(t, h, job, token)
	record, _ := h.job(job)
	failedRevision := record.JobControlRevision

	h.start()
	h.markReady()
	h.lead(2)
	h.waitGateOpen()

	record, _ = h.job(job)
	if record.Lifecycle != state.JobFailed || record.JobControlRevision != failedRevision {
		t.Fatalf("stale event mutated terminal job: %#v", record)
	}
	if got := committedWatermark(h, job, token.Task); got != 0 {
		t.Fatalf("stale event committed watermark %d", got)
	}
	if len(checkpointNoticesFor(h, node, token.Task)) != 0 {
		t.Fatalf("stale event announced a checkpoint: %v", checkpointNoticesFor(h, node, token.Task))
	}
	// The per-WorkerEpoch cursor advances past the resolved event and the
	// worker gains its deletion proof.
	h.actor.Wake()
	h.waitFor(func() bool { return h.workers.lastAck(node) == 1 }, "stale event acknowledged")
}

func TestPollWorkerEventsKeepsTransientlyRejectedCompletionRetryable(t *testing.T) {
	h, job, _, assignment := runningHarness(t)
	token := sourceToken(t, assignment)
	node := token.WorkerID
	other := uint16(3)
	if token.WorkerID == 3 {
		other = 2
	}
	script := h.workers.script(node)
	h.workers.mu.Lock()
	script.events = []model.WorkerEvent{completionEvent(t, h, job, token, 1, 0, 0, 1, 4)}
	h.workers.mu.Unlock()
	// Deactivate the other assigned worker: the live job carries reassignment
	// markers, so the completion is deterministically rejected while its job
	// stays live and its token still matches — a transiently-false rejection
	// that must stay retryable with the cursor pinned.
	view := h.view()
	record, ok := h.workerRecord(other)
	if !ok || record.State != state.WorkerEligible {
		t.Fatalf("other worker = %#v", record)
	}
	deactivate, err := state.NewDeactivateWorker(
		testCommandID("seed-deactivate", []byte{byte(other)}), record.Revision, other, record.Epoch,
		affectedForWorker(view, other, record.Epoch), view.CoordinatorEpoch,
	)
	if err != nil {
		t.Fatalf("seed deactivate: %v", err)
	}
	h.raft.applySeed(t, deactivate)

	for attempt := 0; attempt < 2; attempt++ {
		if err := h.actor.PollWorkerEvents(context.Background(), node); err == nil {
			t.Fatal("transiently rejected completion reported handled")
		}
	}
	if got := h.workers.lastAck(node); got != 0 {
		t.Fatalf("cursor advanced past a transient rejection: %d", got)
	}
	jobRecord, _ := h.job(job)
	if jobRecord.Lifecycle != state.JobRunning || len(jobRecord.Checkpoints) != 0 {
		t.Fatalf("rejected event mutated the live job: %#v", jobRecord)
	}
}

// staleTokenCompletionEvent builds one retained completion event whose token
// predates the current committed assignment (an older attempt of the same
// worker's duty).
func staleTokenCompletionEvent(t *testing.T, h *harness, job model.JobID, token model.AssignmentToken) model.WorkerEvent {
	t.Helper()
	event := completionEvent(t, h, job, token, 1, 0, 0, 1, 4)
	report := *event.Completion
	report.Token.Attempt++
	report.Digest = model.CompletionReportDigest(report)
	return model.WorkerEvent{
		WorkerID: token.WorkerID, WorkerEpoch: token.WorkerEpoch, TransactionID: 1,
		Kind: model.WorkerEventCompletion, Completion: &report,
	}
}

func TestPollWorkerEventsResolvesCompletionEventWithReplacedToken(t *testing.T) {
	h, job, _, assignment := runningHarness(t)
	token := sourceToken(t, assignment)
	node := token.WorkerID
	script := h.workers.script(node)
	h.workers.mu.Lock()
	script.events = []model.WorkerEvent{staleTokenCompletionEvent(t, h, job, token)}
	h.workers.mu.Unlock()

	// The live job replaced the reported task's token before the poll: the
	// re-read committed View proves the retained event permanently stale and
	// it resolves as handled without any state mutation.
	if err := h.actor.PollWorkerEvents(context.Background(), node); err != nil {
		t.Fatalf("permanently stale completion after token replacement: %v", err)
	}
	record, _ := h.job(job)
	if record.Lifecycle != state.JobRunning || len(record.Checkpoints) != 0 {
		t.Fatalf("stale token event mutated the live job: %#v", record)
	}
	if err := h.actor.PollWorkerEvents(context.Background(), node); err != nil {
		t.Fatalf("ack poll: %v", err)
	}
	if got := h.workers.lastAck(node); got != 1 {
		t.Fatalf("cursor = %d", got)
	}
}

func TestPollWorkerEventsKeepsTransientlyRejectedFailureRetryable(t *testing.T) {
	h, job, _, assignment := runningHarness(t)
	token := sourceToken(t, assignment)
	node := token.WorkerID
	other := uint16(3)
	if token.WorkerID == 3 {
		other = 2
	}
	script := h.workers.script(node)
	h.workers.mu.Lock()
	script.events = []model.WorkerEvent{failureWorkerEvent(t, h, job, token, 1)}
	h.workers.mu.Unlock()
	// Deactivate the other assigned worker first: the failure report then
	// carries a fence the live job has outgrown, while its token still
	// matches — a transiently-false rejection that must stay retryable.
	view := h.view()
	record, ok := h.workerRecord(other)
	if !ok || record.State != state.WorkerEligible {
		t.Fatalf("other worker = %#v", record)
	}
	deactivate, err := state.NewDeactivateWorker(
		testCommandID("seed-deactivate-failure", []byte{byte(other)}), record.Revision, other, record.Epoch,
		affectedForWorker(view, other, record.Epoch), view.CoordinatorEpoch,
	)
	if err != nil {
		t.Fatalf("seed deactivate: %v", err)
	}
	h.raft.applySeed(t, deactivate)

	if err := h.actor.PollWorkerEvents(context.Background(), node); err == nil {
		t.Fatal("transiently rejected failure reported handled")
	}
	if got := h.workers.lastAck(node); got != 0 {
		t.Fatalf("cursor advanced past a transient rejection: %d", got)
	}
	jobRecord, _ := h.job(job)
	if jobRecord.Lifecycle != state.JobRunning {
		t.Fatalf("rejected failure mutated the live job: %#v", jobRecord)
	}
}
