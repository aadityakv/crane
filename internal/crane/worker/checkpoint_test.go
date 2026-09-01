package worker

import (
	"context"
	"testing"

	"github.com/aaditya/cs425mp3/internal/crane/admission"
	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
	"github.com/aaditya/cs425mp3/internal/crane/store"
)

func TestCheckpointPublishesOnlyContiguousDurableSourceCompletion(t *testing.T) {
	fixture := workerFixtureWithRange(t, "1", "4")
	repository := newFakeRepository(fixture)
	repository.work.Sources = []store.SourceCursor{{Source: fixture.source.Task, NextSequence: 4, EOF: 3}}
	for sequence := uint64(1); sequence <= 3; sequence++ {
		tuple, exists, err := model.SourceTuple(fixture.topology, fixture.source.Task, sequence)
		if err != nil || !exists {
			t.Fatal(err)
		}
		outboxes, err := deriveSourceOutboxes(fixture.assignment, fixture.source, sequence, tuple)
		if err != nil {
			t.Fatal(err)
		}
		for _, outbox := range outboxes {
			outbox.Completed = sequence != 2
			repository.outboxes[outbox.ID] = outbox
			repository.work.Outboxes = append(repository.work.Outboxes, outbox)
		}
	}
	gate := admission.NewGate()
	if err := gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(testEngineOptions(repository, gate, &fakeSender{}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	waitFor(t, func() bool {
		repository.mu.Lock()
		defer repository.mu.Unlock()
		return repository.persistEventCalls == 1
	})
	repository.mu.Lock()
	event := repository.work.PendingEvents[0]
	repository.mu.Unlock()
	if event.Completion == nil || event.Completion.Prior != 0 || event.Completion.New != 1 || event.Completion.EOF != 3 || event.Completion.ExpectedCheckpointRevision != 0 || event.Completion.WorkerTransactionID != 1 || event.Completion.Digest != model.CompletionReportDigest(*event.Completion) {
		t.Fatalf("completion event=%+v", event)
	}
	cancel()
	<-done
}

func TestCheckpointEventConsumerCannotMutateOwnedCompletionProof(t *testing.T) {
	fixture := workerFixtureWithRange(t, "1", "2")
	repository := newFakeRepository(fixture)
	repository.work.Sources = []store.SourceCursor{{Source: fixture.source.Task, NextSequence: 2, EOF: 1}}
	tuple, exists, err := model.SourceTuple(fixture.topology, fixture.source.Task, 1)
	if err != nil || !exists {
		t.Fatalf("source tuple: exists=%v err=%v", exists, err)
	}
	outboxes, err := deriveSourceOutboxes(fixture.assignment, fixture.source, 1, tuple)
	if err != nil {
		t.Fatal(err)
	}
	for _, outbox := range outboxes {
		outbox.Completed = true
		repository.outboxes[outbox.ID] = outbox
		repository.work.Outboxes = append(repository.work.Outboxes, outbox)
	}
	gate := admission.NewGate()
	if err := gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(testEngineOptions(repository, gate, &fakeSender{}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	published := <-engine.Events()
	exact := *published.Completion
	published.Completion.New++
	published.Completion.Digest[0]++
	notice := protocol.CheckpointNotice{Notice: model.CheckpointNotice{JobID: exact.JobID, Source: exact.Source, Watermark: exact.New, RaftIndex: 9, Epoch: exact.Epoch}, JobControlRevision: exact.JobControlRevision, AssignmentRevision: exact.AssignmentRevision, AssignmentDigest: fixture.assignment.Assignment.Digest}
	if err := engine.ApplyCheckpoint(ctx, notice); err != nil {
		t.Fatalf("consumer mutation corrupted owner checkpoint proof: %v", err)
	}
	if err := engine.AcknowledgeEvents(ctx, exact.WorkerTransactionID); err != nil {
		t.Fatalf("consumer mutation corrupted event acknowledgement: %v", err)
	}
	cancel()
	<-done
}

func TestCheckpointNoticeRequiresExactPendingReportAndCompactsAfterPersistence(t *testing.T) {
	fixture := workerFixtureWithRange(t, "1", "3")
	repository := newFakeRepository(fixture)
	cursor := store.SourceCursor{Source: fixture.source.Task, NextSequence: 3, EOF: 2}
	repository.work.Sources = []store.SourceCursor{cursor}
	repository.sources = map[model.TaskID]store.SourceCursor{cursor.Source: cursor}
	report := model.CompletionReport{JobID: fixture.assignment.Assignment.JobID, JobControlRevision: 1, AssignmentRevision: fixture.assignment.Assignment.Revision, Source: fixture.source.Task, Token: fixture.source, Epoch: fixture.epoch, Prior: 0, New: 1, EOF: 2, WorkerTransactionID: 1}
	report.Digest = model.CompletionReportDigest(report)
	event := model.WorkerEvent{WorkerID: fixture.localNode, WorkerEpoch: fixture.localEpoch, TransactionID: 1, Kind: model.WorkerEventCompletion, Completion: &report}
	repository.work.PendingEvents = []model.WorkerEvent{event}
	repository.work.NextTransactionID = 2
	gate := admission.NewGate()
	if err := gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(testEngineOptions(repository, gate, &fakeSender{}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	notice := protocol.CheckpointNotice{Notice: model.CheckpointNotice{JobID: report.JobID, Source: report.Source, Watermark: 1, RaftIndex: 9, Epoch: fixture.epoch}, JobControlRevision: 1, AssignmentRevision: fixture.assignment.Assignment.Revision, AssignmentDigest: fixture.assignment.Assignment.Digest}
	if err := engine.AcknowledgeEvents(ctx, 1); err == nil {
		t.Fatal("completion event acknowledged before checkpoint notice")
	}
	if err := engine.ApplyCheckpoint(ctx, notice); err != nil {
		t.Fatal(err)
	}
	if err := engine.AcknowledgeEvents(ctx, 1); err != nil {
		t.Fatal(err)
	}
	repository.mu.Lock()
	got := repository.sources[report.Source]
	log := append([]string(nil), repository.log...)
	repository.mu.Unlock()
	if got.Watermark != 1 || got.RaftIndex != 9 || got.CheckpointRevision != 1 {
		t.Fatalf("cursor=%+v", got)
	}
	if indexOf(log, "checkpoint") < 0 {
		t.Fatalf("checkpoint not persisted: %v", log)
	}
	changed := notice
	changed.Notice.RaftIndex++
	if err := engine.ApplyCheckpoint(ctx, changed); err == nil {
		t.Fatal("changed duplicate checkpoint notice accepted")
	}
	tokens := append([]model.AssignmentToken(nil), fixture.assignment.Assignment.Tasks...)
	for index := range tokens {
		tokens[index].AssignmentRevision++
	}
	replacement, err := model.NewAssignmentSet(fixture.assignment.Assignment.JobID, fixture.assignment.Assignment.Revision+1, tokens, fixture.assignment.Assignment.ResultReplicas, fixture.topology)
	if err != nil {
		t.Fatal(err)
	}
	newEpoch := fixture.epoch
	newEpoch.Term++
	newEpoch.BeginIndex++
	newEpoch.Nonce[0]++
	installed := fixture.assignment
	installed.Assignment = replacement
	installed.JobControlRevision++
	installed.CoordinatorEpoch = newEpoch
	repository.mu.Lock()
	repository.assignments[replacement.JobID] = installed
	repository.work.Fence = newEpoch
	repository.mu.Unlock()
	if err := engine.ApplyCheckpoint(ctx, notice); err != nil {
		t.Fatalf("exact historical checkpoint duplicate=%v", err)
	}
	changedWrapper := notice
	changedWrapper.AssignmentDigest[0]++
	if err := engine.ApplyCheckpoint(ctx, changedWrapper); err == nil {
		t.Fatal("historical checkpoint accepted changed protocol authority")
	}
	cancel()
	<-done
}

func TestCheckpointLegacyProofMigrationRequiresExactInstalledDigestBeforeMutation(t *testing.T) {
	fixture := workerFixtureWithRange(t, "1", "3")
	repository := newFakeRepository(fixture)
	cursor := store.SourceCursor{Source: fixture.source.Task, NextSequence: 3, EOF: 2, Watermark: 1, RaftIndex: 9}
	repository.work.Sources = []store.SourceCursor{cursor}
	repository.sources = map[model.TaskID]store.SourceCursor{cursor.Source: cursor}
	report := model.CompletionReport{JobID: fixture.assignment.Assignment.JobID, JobControlRevision: 1, AssignmentRevision: fixture.assignment.Assignment.Revision, Source: fixture.source.Task, Token: fixture.source, Epoch: fixture.epoch, ExpectedCheckpointRevision: 0, Prior: 0, New: 1, EOF: 2, WorkerTransactionID: 1}
	report.Digest = model.CompletionReportDigest(report)
	repository.work.PendingEvents = []model.WorkerEvent{{WorkerID: fixture.localNode, WorkerEpoch: fixture.localEpoch, TransactionID: 1, Kind: model.WorkerEventCompletion, Completion: &report}}
	repository.work.NextTransactionID = 2
	gate := admission.NewGate()
	if err := gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(testEngineOptions(repository, gate, &fakeSender{}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()

	exact := protocol.CheckpointNotice{Notice: model.CheckpointNotice{JobID: report.JobID, Source: report.Source, Watermark: 1, RaftIndex: 9, Epoch: fixture.epoch}, JobControlRevision: 1, AssignmentRevision: fixture.assignment.Assignment.Revision, AssignmentDigest: fixture.assignment.Assignment.Digest}
	changed := exact
	changed.AssignmentDigest[0]++
	if err := engine.ApplyCheckpoint(ctx, changed); err == nil {
		t.Fatal("legacy migration accepted a changed assignment digest")
	}
	repository.mu.Lock()
	storeCalls := repository.applyCheckpointCalls
	durable := repository.sources[cursor.Source]
	repository.mu.Unlock()
	if storeCalls != 0 || durable != cursor || engine.sources[cursor.Source] != cursor {
		t.Fatalf("rejected migration mutated state: calls=%d durable=%+v memory=%+v", storeCalls, durable, engine.sources[cursor.Source])
	}
	if err := engine.ApplyCheckpoint(ctx, exact); err != nil {
		t.Fatalf("exact legacy migration: %v", err)
	}
	repository.mu.Lock()
	storeCalls = repository.applyCheckpointCalls
	repository.mu.Unlock()
	if storeCalls != 1 || engine.sources[cursor.Source].CheckpointAuthority.AssignmentDigest != exact.AssignmentDigest {
		t.Fatalf("exact migration calls=%d cursor=%+v", storeCalls, engine.sources[cursor.Source])
	}
	cancel()
	<-done
}

func TestCheckpointEmptySourceEmitsNoZeroAdvance(t *testing.T) {
	fixture := workerFixtureWithRange(t, "1", "1")
	repository := newFakeRepository(fixture)
	repository.work.Sources = []store.SourceCursor{{Source: fixture.source.Task, NextSequence: 1, EOF: 0}}
	gate := admission.NewGate()
	if err := gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(testEngineOptions(repository, gate, &fakeSender{}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	for i := 0; i < 10_000; i++ {
		repository.mu.Lock()
		calls := repository.persistEventCalls
		repository.mu.Unlock()
		if calls != 0 {
			t.Fatal("empty source emitted a 0->0 completion")
		}
	}
	cancel()
	<-done
}

func TestCheckpointNoticeUnlocksNextGapFreeReportWithDurableRevisionAndTransaction(t *testing.T) {
	fixture := workerFixtureWithRange(t, "1", "4")
	repository := newFakeRepository(fixture)
	repository.work.Sources = []store.SourceCursor{{Source: fixture.source.Task, NextSequence: 4, EOF: 3}}
	var second store.OutboxRecord
	for sequence := uint64(1); sequence <= 3; sequence++ {
		tuple, exists, err := model.SourceTuple(fixture.topology, fixture.source.Task, sequence)
		if err != nil || !exists {
			t.Fatal(err)
		}
		outboxes, err := deriveSourceOutboxes(fixture.assignment, fixture.source, sequence, tuple)
		if err != nil {
			t.Fatal(err)
		}
		for _, outbox := range outboxes {
			outbox.Completed = sequence != 2
			if sequence == 2 {
				second = outbox
			}
			repository.outboxes[outbox.ID] = outbox
			repository.work.Outboxes = append(repository.work.Outboxes, outbox)
		}
	}
	gate := admission.NewGate()
	if err := gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(testEngineOptions(repository, gate, &fakeSender{}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	waitFor(t, func() bool {
		repository.mu.Lock()
		defer repository.mu.Unlock()
		return repository.persistEventCalls == 1
	})
	notice := protocol.CheckpointNotice{Notice: model.CheckpointNotice{JobID: fixture.assignment.Assignment.JobID, Source: fixture.source.Task, Watermark: 1, RaftIndex: 9, Epoch: fixture.epoch}, JobControlRevision: 1, AssignmentRevision: fixture.assignment.Assignment.Revision, AssignmentDigest: fixture.assignment.Assignment.Digest}
	if err := engine.ApplyCheckpoint(ctx, notice); err != nil {
		t.Fatal(err)
	}
	if err := engine.AcknowledgeEvents(ctx, 1); err != nil {
		t.Fatal(err)
	}
	tokens := append([]model.AssignmentToken(nil), fixture.assignment.Assignment.Tasks...)
	for index := range tokens {
		tokens[index].AssignmentRevision++
	}
	replacement, err := model.NewAssignmentSet(fixture.assignment.Assignment.JobID, fixture.assignment.Assignment.Revision+1, tokens, fixture.assignment.Assignment.ResultReplicas, fixture.topology)
	if err != nil {
		t.Fatal(err)
	}
	installed := fixture.assignment
	installed.Assignment = replacement
	installed.JobControlRevision++
	repository.mu.Lock()
	repository.assignments[replacement.JobID] = installed
	repository.mu.Unlock()
	if err := engine.ReconcileAssignment(ctx, replacement.JobID); err != nil {
		t.Fatal(err)
	}
	ack := protocol.TupleACK{DeliveryID: second.ID, Destination: second.Destination, Assignment: protocol.AssignmentSetIdentity{JobID: second.ID.Tuple.JobID, Revision: second.AssignmentRevision, Digest: second.AssignmentDigest}, Coordinator: second.CoordinatorEpoch, Status: protocol.TupleCompleted}
	if err := engine.HandleACK(ctx, ack); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		repository.mu.Lock()
		defer repository.mu.Unlock()
		return repository.persistEventCalls == 2
	})
	repository.mu.Lock()
	report := repository.work.PendingEvents[len(repository.work.PendingEvents)-1].Completion
	repository.mu.Unlock()
	if report == nil || report.Prior != 1 || report.New != 3 || report.ExpectedCheckpointRevision != 1 || report.WorkerTransactionID != 2 || report.JobControlRevision != 2 || report.AssignmentRevision != replacement.Revision || report.Token.AssignmentRevision != replacement.Revision || report.Digest != model.CompletionReportDigest(*report) {
		t.Fatalf("second completion=%+v", report)
	}
	cancel()
	<-done
}

func TestCheckpointPublishesAlreadyDurableCompletionWhileAdmissionClosed(t *testing.T) {
	fixture := workerFixtureWithRange(t, "1", "2")
	fixture.assignment.SchedulingState = model.Closed
	repository := newFakeRepository(fixture)
	repository.work.Assignments = []store.InstalledAssignment{fixture.assignment}
	repository.assignments[fixture.assignment.Assignment.JobID] = fixture.assignment
	repository.work.Sources = []store.SourceCursor{{Source: fixture.source.Task, NextSequence: 2, EOF: 1}}
	tuple, exists, err := model.SourceTuple(fixture.topology, fixture.source.Task, 1)
	if err != nil || !exists {
		t.Fatal(err)
	}
	outboxes, err := deriveSourceOutboxes(fixture.assignment, fixture.source, 1, tuple)
	if err != nil {
		t.Fatal(err)
	}
	for _, outbox := range outboxes {
		outbox.Completed = true
		repository.outboxes[outbox.ID] = outbox
		repository.work.Outboxes = append(repository.work.Outboxes, outbox)
	}
	engine, err := NewEngine(testEngineOptions(repository, admission.NewGate(), &fakeSender{}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	waitFor(t, func() bool {
		repository.mu.Lock()
		defer repository.mu.Unlock()
		return repository.persistEventCalls == 1
	})
	cancel()
	<-done
}

func TestCheckpointAcknowledgeCancellationReturnsWhileOwnerSafelyFinishes(t *testing.T) {
	fixture := workerFixture(t)
	fixture.assignment.SchedulingState = model.Closed
	repository := newFakeRepository(fixture)
	event := fixture.failureEvent(1)
	repository.work.Assignments = []store.InstalledAssignment{fixture.assignment}
	repository.assignments[fixture.assignment.Assignment.JobID] = fixture.assignment
	repository.work.PendingEvents = []model.WorkerEvent{event}
	repository.work.NextTransactionID = 2
	repository.eventAckStarted = make(chan struct{})
	repository.eventAckRelease = make(chan struct{})
	engine, err := NewEngine(testEngineOptions(repository, admission.NewGate(), &fakeSender{}))
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	done := runEngine(t, runCtx, engine)
	<-engine.Ready()
	ackCtx, cancelAck := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- engine.AcknowledgeEvents(ackCtx, 1) }()
	<-repository.eventAckStarted
	cancelAck()
	if err := <-result; err != context.Canceled {
		t.Fatalf("canceled acknowledgment=%v", err)
	}
	close(repository.eventAckRelease)
	waitFor(t, func() bool {
		repository.mu.Lock()
		defer repository.mu.Unlock()
		return len(repository.work.PendingEvents) == 0
	})
	if err := engine.ReconcileAssignment(runCtx, fixture.assignment.Assignment.JobID); err != nil {
		t.Fatalf("owner did not survive canceled caller: %v", err)
	}
	cancelRun()
	<-done
}

func TestCheckpointSupersededUnappliedReportCanBeAcknowledgedAndRepublished(t *testing.T) {
	fixture := workerFixtureWithRange(t, "1", "2")
	repository := newFakeRepository(fixture)
	repository.work.Sources = []store.SourceCursor{{Source: fixture.source.Task, NextSequence: 2, EOF: 1}}
	tuple, exists, err := model.SourceTuple(fixture.topology, fixture.source.Task, 1)
	if err != nil || !exists {
		t.Fatal(err)
	}
	outboxes, err := deriveSourceOutboxes(fixture.assignment, fixture.source, 1, tuple)
	if err != nil {
		t.Fatal(err)
	}
	for _, outbox := range outboxes {
		outbox.Completed = true
		repository.outboxes[outbox.ID] = outbox
		repository.work.Outboxes = append(repository.work.Outboxes, outbox)
	}
	gate := admission.NewGate()
	if err := gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(testEngineOptions(repository, gate, &fakeSender{}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	waitFor(t, func() bool {
		repository.mu.Lock()
		defer repository.mu.Unlock()
		return repository.persistEventCalls == 1
	})
	if err := engine.AcknowledgeEvents(ctx, 1); err == nil {
		t.Fatal("unapplied current report acknowledged without safe proof")
	}
	tokens := append([]model.AssignmentToken(nil), fixture.assignment.Assignment.Tasks...)
	for index := range tokens {
		tokens[index].AssignmentRevision++
	}
	replacement, err := model.NewAssignmentSet(fixture.assignment.Assignment.JobID, fixture.assignment.Assignment.Revision+1, tokens, fixture.assignment.Assignment.ResultReplicas, fixture.topology)
	if err != nil {
		t.Fatal(err)
	}
	installed := fixture.assignment
	installed.Assignment = replacement
	installed.JobControlRevision++
	repository.mu.Lock()
	repository.assignments[replacement.JobID] = installed
	repository.mu.Unlock()
	if err := engine.AcknowledgeEvents(ctx, 1); err != nil {
		t.Fatalf("strict successor did not prove stale report: %v", err)
	}
	if err := engine.ReconcileAssignment(ctx, replacement.JobID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		repository.mu.Lock()
		defer repository.mu.Unlock()
		return repository.persistEventCalls == 2
	})
	repository.mu.Lock()
	report := repository.work.PendingEvents[0].Completion
	repository.mu.Unlock()
	if report == nil || report.JobControlRevision != 2 || report.AssignmentRevision != replacement.Revision || report.WorkerTransactionID != 2 || report.Prior != 0 || report.New != 1 {
		t.Fatalf("replacement completion=%+v", report)
	}
	cancel()
	<-done
}
