package worker

import (
	"context"
	"testing"

	"github.com/aadityakv/crane/internal/crane/admission"
	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
	"github.com/aadityakv/crane/internal/crane/store"
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
	// A redelivered notice with the same committed watermark and a strictly
	// higher Raft index (a later leadership pass) is an idempotent confirm
	// without any engine or store mutation (defect #2 ruling).
	beforeConfirm := engine.sources[report.Source]
	if err := engine.ApplyCheckpoint(ctx, changed); err != nil {
		t.Fatalf("higher-index equal-watermark resend rejected: %v", err)
	}
	if engine.sources[report.Source] != beforeConfirm {
		t.Fatalf("idempotent confirm mutated the cursor: %+v", engine.sources[report.Source])
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
	// An equal-watermark resend whose wrapper authority differs from the
	// retained proof confirms idempotently without mutation regardless of
	// authority age (defect #2 ruling); the durable store is not consulted.
	repository.mu.Lock()
	callsBeforeWrapper := repository.applyCheckpointCalls
	repository.mu.Unlock()
	if err := engine.ApplyCheckpoint(ctx, changedWrapper); err != nil {
		t.Fatalf("changed-authority equal-watermark resend rejected: %v", err)
	}
	repository.mu.Lock()
	callsAfterWrapper := repository.applyCheckpointCalls
	repository.mu.Unlock()
	if callsAfterWrapper != callsBeforeWrapper {
		t.Fatalf("changed-authority confirm consulted the durable store: %d -> %d", callsBeforeWrapper, callsAfterWrapper)
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
	// engine cache: the replacement reaches the serialized owner as an
	// assignment command before any later status exchange, exactly as the
	// control session persists and reconciles it.
	if err := engine.ReconcileAssignment(ctx, replacement.JobID); err != nil {
		t.Fatal(err)
	}
	if err := engine.AcknowledgeEvents(ctx, 1); err != nil {
		t.Fatalf("strict successor did not prove stale report: %v", err)
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

func TestCheckpointCompactionNeverCollectsAboveWatermarkOrResults(t *testing.T) {
	fixture := workerFixtureWithRange(t, "1", "3")
	repository := newFakeRepository(fixture)
	engine, err := NewEngine(testEngineOptions(repository, admission.NewGate(), &fakeSender{}))
	if err != nil {
		t.Fatal(err)
	}
	engine.localNode, engine.localEpoch = fixture.localNode, fixture.localEpoch
	// engine cache: the serialized view is seeded by recovery before any
	// direct-drive checkpoint application.
	recovered, recoverErr := repository.RecoverWork()
	if recoverErr != nil {
		t.Fatal(recoverErr)
	}
	if err := engine.consumeRecovery(recovered); err != nil {
		t.Fatal(err)
	}
	source := fixture.source.Task
	cursor := store.SourceCursor{Source: source, NextSequence: 3, EOF: 2}
	engine.sources[source] = cursor
	repository.mu.Lock()
	repository.sources[source] = cursor
	repository.mu.Unlock()

	report := model.CompletionReport{JobID: fixture.assignment.Assignment.JobID, JobControlRevision: fixture.assignment.JobControlRevision, AssignmentRevision: fixture.assignment.Assignment.Revision, Source: source, Token: fixture.source, Epoch: fixture.epoch, Prior: 0, New: 1, EOF: 2, WorkerTransactionID: 1}
	report.Digest = model.CompletionReportDigest(report)
	event := model.WorkerEvent{WorkerID: fixture.localNode, WorkerEpoch: fixture.localEpoch, TransactionID: 1, Kind: model.WorkerEventCompletion, Completion: &report}
	engine.completionReports[source] = event
	repository.mu.Lock()
	repository.work.PendingEvents = []model.WorkerEvent{event}
	repository.work.NextTransactionID = 2
	repository.mu.Unlock()

	makeID := func(sequence uint64) model.DeliveryID {
		return model.DeliveryID{Tuple: model.TupleID{JobID: source.JobID, SourceTask: source, SourceSequence: sequence}, EdgeID: 1, DestinationTask: fixture.transform.Task}
	}
	engine.deliveries[makeID(1)] = store.DeliveryRecord{ID: makeID(1)}
	engine.deliveries[makeID(2)] = store.DeliveryRecord{ID: makeID(2)}
	engine.outboxes[makeID(1)] = &ownedOutbox{record: store.OutboxRecord{ID: makeID(1), Completed: true}}
	engine.outboxes[makeID(2)] = &ownedOutbox{record: store.OutboxRecord{ID: makeID(2)}}
	result := model.ResultRecord{TupleID: makeID(1).Tuple, SinkTask: fixture.transform.Task, SpecificationHash: fixture.assignment.Topology.Digest()}
	engine.results[resultID(result)] = &ownedResult{record: result}

	notice := protocol.CheckpointNotice{Notice: model.CheckpointNotice{JobID: report.JobID, Source: source, Watermark: 1, RaftIndex: 9, Epoch: fixture.epoch}, JobControlRevision: report.JobControlRevision, AssignmentRevision: report.AssignmentRevision, AssignmentDigest: fixture.assignment.Assignment.Digest}
	if err := engine.applyCheckpoint(notice); err != nil {
		t.Fatalf("apply checkpoint: %v", err)
	}
	if _, retained := engine.deliveries[makeID(1)]; retained {
		t.Fatal("covered delivery survived compaction")
	}
	if _, retained := engine.outboxes[makeID(1)]; retained {
		t.Fatal("covered outbox survived compaction")
	}
	if _, retained := engine.deliveries[makeID(2)]; !retained {
		t.Fatal("compaction collected delivery state above the watermark")
	}
	if _, retained := engine.outboxes[makeID(2)]; !retained {
		t.Fatal("compaction collected outbox state above the watermark")
	}
	if _, retained := engine.results[resultID(result)]; !retained {
		t.Fatal("compaction collected a result record")
	}
}

// TestCheckpointCompactionEvictsMaterializedDeliveries pins that compacting
// a checkpoint-covered source stream also evicts the covered deliveries'
// materialized-skip index entries, so the index cannot grow without bound
// on long-running jobs at a stable assignment revision, while entries above
// the watermark survive.
func TestCheckpointCompactionEvictsMaterializedDeliveries(t *testing.T) {
	fixture := workerFixtureWithRange(t, "1", "3")
	repository := newFakeRepository(fixture)
	engine, err := NewEngine(testEngineOptions(repository, admission.NewGate(), &fakeSender{}))
	if err != nil {
		t.Fatal(err)
	}
	engine.localNode, engine.localEpoch = fixture.localNode, fixture.localEpoch
	recovered, recoverErr := repository.RecoverWork()
	if recoverErr != nil {
		t.Fatal(recoverErr)
	}
	if err := engine.consumeRecovery(recovered); err != nil {
		t.Fatal(err)
	}
	source := fixture.source.Task
	cursor := store.SourceCursor{Source: source, NextSequence: 3, EOF: 2}
	engine.sources[source] = cursor
	repository.mu.Lock()
	repository.sources[source] = cursor
	repository.mu.Unlock()

	report := model.CompletionReport{JobID: fixture.assignment.Assignment.JobID, JobControlRevision: fixture.assignment.JobControlRevision, AssignmentRevision: fixture.assignment.Assignment.Revision, Source: source, Token: fixture.source, Epoch: fixture.epoch, Prior: 0, New: 1, EOF: 2, WorkerTransactionID: 1}
	report.Digest = model.CompletionReportDigest(report)
	event := model.WorkerEvent{WorkerID: fixture.localNode, WorkerEpoch: fixture.localEpoch, TransactionID: 1, Kind: model.WorkerEventCompletion, Completion: &report}
	engine.completionReports[source] = event
	repository.mu.Lock()
	repository.work.PendingEvents = []model.WorkerEvent{event}
	repository.work.NextTransactionID = 2
	repository.mu.Unlock()

	makeID := func(sequence uint64) model.DeliveryID {
		return model.DeliveryID{Tuple: model.TupleID{JobID: source.JobID, SourceTask: source, SourceSequence: sequence}, EdgeID: 1, DestinationTask: fixture.transform.Task}
	}
	engine.deliveries[makeID(1)] = store.DeliveryRecord{ID: makeID(1), State: store.Processed}
	engine.deliveries[makeID(2)] = store.DeliveryRecord{ID: makeID(2), State: store.Processed}
	provenance := model.ResultCopyProvenance{AssignmentRevision: fixture.assignment.Assignment.Revision, AssignmentDigest: fixture.assignment.Assignment.Digest, CoordinatorEpoch: fixture.epoch, DestinationRole: model.PrimaryReplica}
	engine.materialized[makeID(1)] = provenance
	engine.materialized[makeID(2)] = provenance

	notice := protocol.CheckpointNotice{Notice: model.CheckpointNotice{JobID: report.JobID, Source: source, Watermark: 1, RaftIndex: 9, Epoch: fixture.epoch}, JobControlRevision: report.JobControlRevision, AssignmentRevision: report.AssignmentRevision, AssignmentDigest: fixture.assignment.Assignment.Digest}
	if err := engine.applyCheckpoint(notice); err != nil {
		t.Fatalf("apply checkpoint: %v", err)
	}
	if _, retained := engine.materialized[makeID(1)]; retained {
		t.Fatal("covered delivery's materialized entry survived compaction")
	}
	if _, retained := engine.materialized[makeID(2)]; !retained {
		t.Fatal("compaction collected a materialized entry above the watermark")
	}
}

func TestCheckpointFailureEventAcknowledgmentRequiresDurableClosedInstallation(t *testing.T) {
	fixture := workerFixture(t)
	repository := newFakeRepository(fixture)
	event := fixture.failureEvent(1)
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
	// The job is durably Running: the originating failure event must stay
	// durable until the terminal Closed installation exists.
	if err := engine.AcknowledgeEvents(ctx, 1); err == nil {
		t.Fatal("failure event acknowledged before durable closure")
	}
	closed := fixture.assignment
	closed.SchedulingState = model.Closed
	repository.mu.Lock()
	repository.assignments[closed.Assignment.JobID] = closed
	repository.mu.Unlock()
	if err := engine.AcknowledgeEvents(ctx, 1); err != nil {
		t.Fatalf("acknowledgment after durable closure: %v", err)
	}
	repository.mu.Lock()
	remaining := len(repository.work.PendingEvents)
	repository.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("acknowledged failure event retained: %d", remaining)
	}
	cancel()
	<-done
}

// newerCheckpointAuthority installs one strictly newer committed authority
// (coordinator epoch, JobControlRevision, assignment revision, digest) over
// the fixture, exactly as a leadership change plus lifecycle transition does,
// and returns the resend notice wrapper fields of that current authority.
func newerCheckpointAuthority(t *testing.T, repository *fakeRepository, fixture workerTestFixture, watermark, raftIndex uint64) protocol.CheckpointNotice {
	t.Helper()
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
	return protocol.CheckpointNotice{Notice: model.CheckpointNotice{JobID: replacement.JobID, Source: fixture.source.Task, Watermark: watermark, RaftIndex: raftIndex, Epoch: newEpoch}, JobControlRevision: installed.JobControlRevision, AssignmentRevision: replacement.Revision, AssignmentDigest: replacement.Digest}
}

// TestCheckpointNoticeEqualWatermarkUnderNewerAuthorityConfirmsWithoutMutation
// pins the defect #2 ruling CONFIRM (equal) branch: the coordinator's resend
// carries send-time authority, so once any authority component advances past
// the retained durable proof the equal-watermark notice is an idempotent
// confirm with no store mutation, never identity reuse.
func TestCheckpointNoticeEqualWatermarkUnderNewerAuthorityConfirmsWithoutMutation(t *testing.T) {
	fixture := workerFixtureWithRange(t, "1", "3")
	repository := newFakeRepository(fixture)
	engine, err := NewEngine(testEngineOptions(repository, admission.NewGate(), &fakeSender{}))
	if err != nil {
		t.Fatal(err)
	}
	engine.localNode, engine.localEpoch = fixture.localNode, fixture.localEpoch
	source := fixture.source.Task
	proof := store.CheckpointAuthority{JobControlRevision: fixture.assignment.JobControlRevision, AssignmentRevision: fixture.assignment.Assignment.Revision, AssignmentDigest: fixture.assignment.Assignment.Digest, SourceToken: fixture.source, CoordinatorEpoch: fixture.epoch}
	cursor := store.SourceCursor{Source: source, NextSequence: 2, EOF: 2, Watermark: 1, RaftIndex: 9, CheckpointRevision: 1, CheckpointAuthority: proof}
	engine.sources[source] = cursor
	repository.mu.Lock()
	repository.sources[source] = cursor
	repository.mu.Unlock()

	resend := newerCheckpointAuthority(t, repository, fixture, 1, 18)
	if err := engine.applyCheckpoint(resend); err != nil {
		t.Fatalf("equal-watermark resend under newer authority rejected: %v", err)
	}
	repository.mu.Lock()
	calls := repository.applyCheckpointCalls
	repository.mu.Unlock()
	if calls != 0 {
		t.Fatalf("idempotent confirm mutated the durable store: %d calls", calls)
	}
	if engine.sources[source] != cursor {
		t.Fatalf("idempotent confirm mutated the in-memory cursor: %+v", engine.sources[source])
	}
	// A byte-exact duplicate of the retained proof still flows through the
	// serialized engine so the durable store answers it.
	exact := protocol.CheckpointNotice{Notice: model.CheckpointNotice{JobID: cursor.Source.JobID, Source: source, Watermark: 1, RaftIndex: 9, Epoch: fixture.epoch}, JobControlRevision: proof.JobControlRevision, AssignmentRevision: proof.AssignmentRevision, AssignmentDigest: proof.AssignmentDigest}
	if err := engine.applyCheckpoint(exact); err != nil {
		t.Fatalf("byte-exact duplicate rejected: %v", err)
	}
	repository.mu.Lock()
	calls = repository.applyCheckpointCalls
	repository.mu.Unlock()
	if calls != 1 {
		t.Fatalf("byte-exact duplicate bypassed the durable store: %d calls", calls)
	}
}

// TestCheckpointNoticeBelowDurableCursorConfirmsWithoutMutation pins the
// ruling CONFIRM (below) branch: a stale resend below the durable committed
// cursor confirms without mutation instead of demanding a completion proof.
func TestCheckpointNoticeBelowDurableCursorConfirmsWithoutMutation(t *testing.T) {
	fixture := workerFixtureWithRange(t, "1", "3")
	repository := newFakeRepository(fixture)
	engine, err := NewEngine(testEngineOptions(repository, admission.NewGate(), &fakeSender{}))
	if err != nil {
		t.Fatal(err)
	}
	engine.localNode, engine.localEpoch = fixture.localNode, fixture.localEpoch
	source := fixture.source.Task
	proof := store.CheckpointAuthority{JobControlRevision: fixture.assignment.JobControlRevision, AssignmentRevision: fixture.assignment.Assignment.Revision, AssignmentDigest: fixture.assignment.Assignment.Digest, SourceToken: fixture.source, CoordinatorEpoch: fixture.epoch}
	cursor := store.SourceCursor{Source: source, NextSequence: 3, EOF: 2, Watermark: 2, RaftIndex: 9, CheckpointRevision: 1, CheckpointAuthority: proof}
	engine.sources[source] = cursor
	repository.mu.Lock()
	repository.sources[source] = cursor
	repository.mu.Unlock()
	stale := protocol.CheckpointNotice{Notice: model.CheckpointNotice{JobID: cursor.Source.JobID, Source: source, Watermark: 1, RaftIndex: 20, Epoch: fixture.epoch}, JobControlRevision: proof.JobControlRevision, AssignmentRevision: proof.AssignmentRevision, AssignmentDigest: proof.AssignmentDigest}
	if err := engine.applyCheckpoint(stale); err != nil {
		t.Fatalf("below-cursor resend rejected: %v", err)
	}
	repository.mu.Lock()
	calls := repository.applyCheckpointCalls
	repository.mu.Unlock()
	if calls != 0 {
		t.Fatalf("below-cursor confirm mutated the durable store: %d calls", calls)
	}
	if engine.sources[source] != cursor {
		t.Fatalf("below-cursor confirm mutated the in-memory cursor: %+v", engine.sources[source])
	}
}

// TestCheckpointNoticeAboveCursorAdoptsCommittedWatermarkWithoutPendingReport
// pins the ruling ADOPT branch over an existing cursor: a strictly higher
// watermark under the current fence persists under the current authority proof
// with no pending CompletionReport, bumps the revision, resumes above the
// watermark, and applies the compaction rules.
func TestCheckpointNoticeAboveCursorAdoptsCommittedWatermarkWithoutPendingReport(t *testing.T) {
	fixture := workerFixtureWithRange(t, "1", "3")
	repository := newFakeRepository(fixture)
	engine, err := NewEngine(testEngineOptions(repository, admission.NewGate(), &fakeSender{}))
	if err != nil {
		t.Fatal(err)
	}
	engine.localNode, engine.localEpoch = fixture.localNode, fixture.localEpoch
	// engine cache: the serialized view is seeded by recovery before any
	// direct-drive checkpoint application.
	recovered, recoverErr := repository.RecoverWork()
	if recoverErr != nil {
		t.Fatal(recoverErr)
	}
	if err := engine.consumeRecovery(recovered); err != nil {
		t.Fatal(err)
	}
	source := fixture.source.Task
	proof := store.CheckpointAuthority{JobControlRevision: fixture.assignment.JobControlRevision, AssignmentRevision: fixture.assignment.Assignment.Revision, AssignmentDigest: fixture.assignment.Assignment.Digest, SourceToken: fixture.source, CoordinatorEpoch: fixture.epoch}
	cursor := store.SourceCursor{Source: source, NextSequence: 2, EOF: 2, Watermark: 1, RaftIndex: 9, CheckpointRevision: 1, CheckpointAuthority: proof}
	engine.sources[source] = cursor
	repository.mu.Lock()
	repository.sources[source] = cursor
	repository.mu.Unlock()
	makeID := func(sequence uint64) model.DeliveryID {
		return model.DeliveryID{Tuple: model.TupleID{JobID: source.JobID, SourceTask: source, SourceSequence: sequence}, EdgeID: 1, DestinationTask: fixture.transform.Task}
	}
	engine.deliveries[makeID(1)] = store.DeliveryRecord{ID: makeID(1)}
	engine.outboxes[makeID(1)] = &ownedOutbox{record: store.OutboxRecord{ID: makeID(1), Completed: true}}

	adopt := protocol.CheckpointNotice{Notice: model.CheckpointNotice{JobID: cursor.Source.JobID, Source: source, Watermark: 2, RaftIndex: 18, Epoch: fixture.epoch}, JobControlRevision: proof.JobControlRevision, AssignmentRevision: proof.AssignmentRevision, AssignmentDigest: proof.AssignmentDigest}

	// Adoption never weakens authority validation: a wrapper from a different
	// current authority is rejected before any mutation.
	wrongAuthority := adopt
	wrongAuthority.JobControlRevision++
	if err := engine.applyCheckpoint(wrongAuthority); err == nil {
		t.Fatal("adoption accepted a wrapper from a different current authority")
	}
	if err := engine.applyCheckpoint(adopt); err != nil {
		t.Fatalf("committed-watermark adoption above the cursor: %v", err)
	}
	adopted := engine.sources[source]
	if adopted.Watermark != 2 || adopted.RaftIndex != 18 || adopted.CheckpointRevision != 2 || adopted.NextSequence != 3 || adopted.EOF != 2 {
		t.Fatalf("adopted cursor = %+v", adopted)
	}
	if adopted.CheckpointAuthority != proof {
		t.Fatalf("adopted authority = %+v", adopted.CheckpointAuthority)
	}
	if _, retained := engine.deliveries[makeID(1)]; retained {
		t.Fatal("adoption did not compact the covered delivery")
	}
	if _, retained := engine.outboxes[makeID(1)]; retained {
		t.Fatal("adoption did not compact the covered outbox")
	}
	repository.mu.Lock()
	durable := repository.sources[source]
	calls := repository.applyCheckpointCalls
	repository.mu.Unlock()
	if calls != 1 || durable != adopted {
		t.Fatalf("durable adoption calls=%d cursor=%+v", calls, durable)
	}

	// Adoption never weakens bounds validation.
	beyondEOF := adopt
	beyondEOF.Notice.Watermark = 3
	if err := engine.applyCheckpoint(beyondEOF); err == nil {
		t.Fatal("adoption accepted a watermark beyond the installed EOF")
	}
}

// TestCheckpointNoticeAdoptsCommittedWatermarkForReassignedOwnerWithoutCursor
// pins the ruling ADOPT branch for a reassigned source owner: with no local
// cursor and no CompletionReport at all, the committed-watermark notice
// creates the durable cursor at the committed watermark so emission resumes
// strictly above it.
func TestCheckpointNoticeAdoptsCommittedWatermarkForReassignedOwnerWithoutCursor(t *testing.T) {
	fixture := workerFixtureWithRange(t, "1", "3")
	repository := newFakeRepository(fixture)
	repository.mu.Lock()
	repository.work.Sources = nil
	repository.sources = make(map[model.TaskID]store.SourceCursor)
	repository.mu.Unlock()
	engine, err := NewEngine(testEngineOptions(repository, admission.NewGate(), &fakeSender{}))
	if err != nil {
		t.Fatal(err)
	}
	engine.localNode, engine.localEpoch = fixture.localNode, fixture.localEpoch
	// engine cache: the serialized view is seeded by recovery before any
	// direct-drive checkpoint application.
	recovered, recoverErr := repository.RecoverWork()
	if recoverErr != nil {
		t.Fatal(recoverErr)
	}
	if err := engine.consumeRecovery(recovered); err != nil {
		t.Fatal(err)
	}
	source := fixture.source.Task
	if _, exists := engine.sources[source]; exists {
		t.Fatal("reassigned-owner fixture must start without a local cursor")
	}
	adopt := protocol.CheckpointNotice{Notice: model.CheckpointNotice{JobID: fixture.assignment.Assignment.JobID, Source: source, Watermark: 2, RaftIndex: 18, Epoch: fixture.epoch}, JobControlRevision: fixture.assignment.JobControlRevision, AssignmentRevision: fixture.assignment.Assignment.Revision, AssignmentDigest: fixture.assignment.Assignment.Digest}
	if err := engine.applyCheckpoint(adopt); err != nil {
		t.Fatalf("reassigned-owner adoption: %v", err)
	}
	adopted := engine.sources[source]
	proof := store.CheckpointAuthority{JobControlRevision: fixture.assignment.JobControlRevision, AssignmentRevision: fixture.assignment.Assignment.Revision, AssignmentDigest: fixture.assignment.Assignment.Digest, SourceToken: fixture.source, CoordinatorEpoch: fixture.epoch}
	if adopted.Watermark != 2 || adopted.RaftIndex != 18 || adopted.CheckpointRevision != 1 || adopted.NextSequence != 3 || adopted.EOF != 2 || adopted.CheckpointAuthority != proof {
		t.Fatalf("reassigned-owner adopted cursor = %+v", adopted)
	}
	repository.mu.Lock()
	durable, durableOK := repository.sources[source]
	repository.mu.Unlock()
	if !durableOK || durable != adopted {
		t.Fatalf("reassigned-owner durable cursor = %+v ok=%v", durable, durableOK)
	}
}
