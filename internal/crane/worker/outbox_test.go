package worker

import (
	"bytes"
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/aadityakv/crane/internal/clock"
	"github.com/aadityakv/crane/internal/crane/admission"
	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
	"github.com/aadityakv/crane/internal/crane/store"
)

func TestOutboxProcessingPersistsCanonicalChildrenBeforeSendAndCompletionOrder(t *testing.T) {
	fixture := workerFixture(t)
	repository := newFakeRepository(fixture)
	record := fixture.delivery(t, 7)
	repository.work.Deliveries = []store.DeliveryRecord{record}
	repository.deliveries[record.ID] = record
	sender := &fakeSender{notify: make(chan protocol.TupleDelivery, 4), before: func() { repository.mu.Lock(); repository.log = append(repository.log, "send"); repository.mu.Unlock() }}
	gate := admission.NewGate()
	_ = gate.Open(fixture.epoch)
	engine, err := NewEngine(testEngineOptions(repository, gate, sender))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	waitFor(t, func() bool { return sender.count() == 1 })

	repository.mu.Lock()
	if len(repository.log) < 3 || repository.log[0] != "processed" || repository.log[1] != "outbox-dispatched" || repository.log[2] != "send" {
		t.Fatalf("durable/send order = %v", repository.log)
	}
	parent := repository.deliveries[record.ID]
	if parent.State != store.Processed || len(parent.Outputs) != 1 || len(parent.OutboxIDs) != 1 {
		t.Fatalf("processed parent = %+v", parent)
	}
	outbox := repository.outboxes[parent.OutboxIDs[0]]
	repository.mu.Unlock()
	wantChild := model.DeriveChildTupleID(record.ID.Tuple, record.Destination.Task, 2, 0)
	if outbox.ID.Tuple != wantChild || outbox.ID.EdgeID != 2 || outbox.Producer != record.Destination {
		t.Fatalf("canonical child = %+v, want tuple %+v", outbox, wantChild)
	}

	ack := protocol.TupleACK{DeliveryID: outbox.ID, Destination: outbox.Destination, Assignment: protocol.AssignmentSetIdentity{JobID: fixture.assignment.Assignment.JobID, Revision: fixture.assignment.Assignment.Revision, Digest: fixture.assignment.Assignment.Digest}, Coordinator: fixture.epoch, Status: protocol.TupleCompleted}
	if err := engine.HandleACK(ctx, ack); err != nil {
		t.Fatal(err)
	}
	repository.mu.Lock()
	completionLog := append([]string(nil), repository.log...)
	completionState := repository.deliveries[record.ID].State
	repository.mu.Unlock()
	if got := completionLog[len(completionLog)-2:]; got[0] != "outbox-completed" || got[1] != "completed" {
		t.Fatalf("completion durable order = %v", completionLog)
	}
	if completionState != store.Completed {
		t.Fatal("parent completed before/without downstream completion")
	}
	cancel()
	<-done
}

func TestOutboxRecoveryCompletesProcessedParentAfterDurableChildCompletion(t *testing.T) {
	fixture := workerFixture(t)
	repository := newFakeRepository(fixture)
	parent := fixture.delivery(t, 8)
	outputs, err := model.ExecuteOperator(fixture.topology.Spec().Stages[1].Operator, parent.Tuple)
	if err != nil {
		t.Fatal(err)
	}
	outboxes, err := deriveOutboxes(fixture.assignment, parent, outputs)
	if err != nil {
		t.Fatal(err)
	}
	parent.State, parent.Outputs = store.Processed, outputs
	for index := range outboxes {
		outboxes[index].Completed = true
		parent.OutboxIDs = append(parent.OutboxIDs, outboxes[index].ID)
		repository.outboxes[outboxes[index].ID] = outboxes[index]
	}
	repository.work.Deliveries = []store.DeliveryRecord{parent}
	repository.work.Outboxes = outboxes
	repository.deliveries[parent.ID] = parent
	gate := admission.NewGate()
	if err = gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(testEngineOptions(repository, gate, &fakeSender{}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	select {
	case <-engine.Ready():
	case err = <-done:
		t.Fatalf("Run before Ready = %v", err)
	}
	repository.mu.Lock()
	state := repository.deliveries[parent.ID].State
	log := append([]string(nil), repository.log...)
	repository.mu.Unlock()
	if state != store.Completed {
		t.Fatalf("recovered parent state = %v, durable log %v", state, log)
	}
	cancel()
	<-done
}

func TestOutboxACKRejectsWrongEnvelopeBeforeDurableMutation(t *testing.T) {
	fixture := workerFixture(t)
	repository := newFakeRepository(fixture)
	record := fixture.delivery(t, 8)
	outputs, err := model.ExecuteOperator(fixture.topology.Spec().Stages[1].Operator, record.Tuple)
	if err != nil {
		t.Fatal(err)
	}
	outboxes, err := deriveOutboxes(fixture.assignment, record, outputs)
	if err != nil {
		t.Fatal(err)
	}
	record.State, record.Outputs = store.Processed, outputs
	for _, outbox := range outboxes {
		record.OutboxIDs = append(record.OutboxIDs, outbox.ID)
		repository.outboxes[outbox.ID] = outbox
	}
	repository.work.Deliveries = []store.DeliveryRecord{record}
	repository.work.Outboxes = outboxes
	repository.deliveries[record.ID] = record
	gate := admission.NewGate()
	_ = gate.Open(fixture.epoch)
	sender := &fakeSender{}
	engine, err := NewEngine(testEngineOptions(repository, gate, sender))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	waitFor(t, func() bool { return sender.count() == 1 })
	base := protocol.TupleACK{DeliveryID: outboxes[0].ID, Destination: outboxes[0].Destination, Assignment: protocol.AssignmentSetIdentity{JobID: fixture.assignment.Assignment.JobID, Revision: fixture.assignment.Assignment.Revision, Digest: fixture.assignment.Assignment.Digest}, Coordinator: fixture.epoch, Status: protocol.TupleCompleted}
	for name, mutate := range map[string]func(*protocol.TupleACK){
		"destination": func(ack *protocol.TupleACK) { ack.Destination.Attempt++ },
		"revision":    func(ack *protocol.TupleACK) { ack.Assignment.Revision++ },
		"digest":      func(ack *protocol.TupleACK) { ack.Assignment.Digest[0] ^= 1 },
		"epoch":       func(ack *protocol.TupleACK) { ack.Coordinator.Term++ },
	} {
		t.Run(name, func(t *testing.T) {
			ack := base
			mutate(&ack)
			repository.mu.Lock()
			before := len(repository.log)
			repository.mu.Unlock()
			if err := engine.HandleACK(ctx, ack); err == nil {
				t.Fatal("invalid ACK accepted")
			}
			repository.mu.Lock()
			after := len(repository.log)
			repository.mu.Unlock()
			if after != before {
				t.Fatal("invalid ACK mutated repository")
			}
		})
	}
	cancel()
	<-done
}

func TestOutboxRestartReusesProcessedBytesAndOriginalLogicalIdentity(t *testing.T) {
	fixture := workerFixture(t)
	repository := newFakeRepository(fixture)
	received := fixture.delivery(t, 9)
	processed := fixture.delivery(t, 10)
	outputs, err := model.ExecuteOperator(fixture.topology.Spec().Stages[1].Operator, processed.Tuple)
	if err != nil {
		t.Fatal(err)
	}
	outboxes, err := deriveOutboxes(fixture.assignment, processed, outputs)
	if err != nil {
		t.Fatal(err)
	}
	processed.State, processed.Outputs = store.Processed, outputs
	for _, outbox := range outboxes {
		processed.OutboxIDs = append(processed.OutboxIDs, outbox.ID)
		repository.outboxes[outbox.ID] = outbox
	}
	repository.work.Deliveries = []store.DeliveryRecord{received, processed}
	repository.work.Outboxes = outboxes
	repository.work.Sources = []store.SourceCursor{{Source: fixture.source.Task, NextSequence: 16, EOF: 15}}
	repository.deliveries[received.ID], repository.deliveries[processed.ID] = received, processed
	sender := &fakeSender{}
	gate := admission.NewGate()
	_ = gate.Open(fixture.epoch)
	options := testEngineOptions(repository, gate, sender)
	executed := make(chan model.Tuple, 1)
	options.Execute = func(ctx context.Context, operator model.OperatorSpec, tuple model.Tuple) ([]model.Tuple, error) {
		executed <- tuple
		return model.ExecuteOperator(operator, tuple)
	}
	engine, err := NewEngine(options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	waitFor(t, func() bool { return sender.count() >= 1 })
	waitFor(t, func() bool { return len(executed) > 0 })
	got := <-executed
	if got.Fields[0].Value.Int64 != received.Tuple.Fields[0].Value.Int64 {
		t.Fatal("recovery executed processed delivery")
	}
	sender.mu.Lock()
	sent := append([]protocol.TupleDelivery(nil), sender.deliveries...)
	sender.mu.Unlock()
	found := false
	for _, delivery := range sent {
		if delivery.DeliveryID == outboxes[0].ID {
			found = delivery.Tuple.Fields[0].Value.Int64 == outputs[0].Fields[0].Value.Int64
		}
	}
	if !found {
		t.Fatal("processed durable bytes/logical identity were not resent")
	}
	repository.mu.Lock()
	sourceCalls := 0
	for _, entry := range repository.log {
		if entry == "source" {
			sourceCalls++
		}
	}
	repository.mu.Unlock()
	if sourceCalls != 0 {
		t.Fatal("recovery reset or re-emitted an exhausted source cursor")
	}
	cancel()
	<-done
}

func TestOutboxAcceptedAndCompletedRetryIntervalsRemainDistinct(t *testing.T) {
	fixture := workerFixture(t)
	repository := newFakeRepository(fixture)
	record := fixture.delivery(t, 11)
	outputs, _ := model.ExecuteOperator(fixture.topology.Spec().Stages[1].Operator, record.Tuple)
	outboxes, _ := deriveOutboxes(fixture.assignment, record, outputs)
	record.State, record.Outputs = store.Processed, outputs
	for _, outbox := range outboxes {
		record.OutboxIDs = append(record.OutboxIDs, outbox.ID)
		repository.outboxes[outbox.ID] = outbox
	}
	repository.work.Deliveries, repository.work.Outboxes = []store.DeliveryRecord{record}, outboxes
	repository.deliveries[record.ID] = record
	manual := clock.NewManual(time.Unix(0, 0))
	sender := &fakeSender{now: manual.Now}
	gate := admission.NewGate()
	_ = gate.Open(fixture.epoch)
	options := testEngineOptions(repository, gate, sender)
	options.Clock = manual
	options.AcceptedRetryInterval = 10 * time.Millisecond
	options.CompletedRetryInterval = 100 * time.Millisecond
	engine, err := NewEngine(options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	waitFor(t, func() bool { return sender.count() == 1 })
	advanceManualUntil(t, manual, func() bool { return sender.count() >= 2 })
	firstTimes := sender.sendTimes()
	if len(firstTimes) < 2 || firstTimes[1].Sub(firstTimes[0]) < 10*time.Millisecond {
		t.Fatalf("custody retry times = %v", firstTimes)
	}
	outbox := outboxes[0]
	accepted := protocol.TupleACK{DeliveryID: outbox.ID, Destination: outbox.Destination, Assignment: protocol.AssignmentSetIdentity{JobID: fixture.assignment.Assignment.JobID, Revision: fixture.assignment.Assignment.Revision, Digest: fixture.assignment.Assignment.Digest}, Coordinator: fixture.epoch, Status: protocol.TupleAccepted}
	if err := engine.HandleACK(ctx, accepted); err != nil {
		t.Fatal(err)
	}
	// The engine applies the acceptance on its owner goroutine; advancing the
	// clock before the outbox is durably Accepted would legitimately fire the
	// custody retry that is still armed (observed under -race on shared
	// runners), so wait for the durable transition first.
	waitFor(t, func() bool {
		repository.mu.Lock()
		defer repository.mu.Unlock()
		return repository.outboxes[outbox.ID].Accepted
	})
	acceptedAt := manual.Now()
	manual.Advance(99 * time.Millisecond)
	runtimeYield()
	if sender.count() != 2 {
		t.Fatal("accepted delivery used custody retry interval")
	}
	advanceManualUntil(t, manual, func() bool { return sender.count() >= 3 })
	completedTimes := sender.sendTimes()
	if completedTimes[2].Before(acceptedAt.Add(100 * time.Millisecond)) {
		t.Fatalf("completion retry times = %v after accepted at %v", completedTimes, acceptedAt)
	}
	cancel()
	<-done
}

func TestOutboxRecoveryRestoresDurableAcceptedDeadline(t *testing.T) {
	fixture := workerFixture(t)
	repository := newFakeRepository(fixture)
	parent := fixture.delivery(t, 11)
	outputs, err := model.ExecuteOperator(fixture.topology.Spec().Stages[1].Operator, parent.Tuple)
	if err != nil {
		t.Fatal(err)
	}
	outboxes, err := deriveOutboxes(fixture.assignment, parent, outputs)
	if err != nil {
		t.Fatal(err)
	}
	manual := clock.NewManual(time.Unix(0, 0))
	outboxes[0].Accepted = true
	outboxes[0].RetryDeadlineUnixNano = int64(50 * time.Millisecond)
	repository.work.Outboxes = outboxes
	repository.outboxes[outboxes[0].ID] = outboxes[0]
	sender := &fakeSender{}
	gate := admission.NewGate()
	if err = gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	options := testEngineOptions(repository, gate, sender)
	options.Clock = manual
	engine, err := NewEngine(options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	runtimeYield()
	if sender.count() != 0 {
		t.Fatal("recovered Accepted outbox sent before durable deadline")
	}
	manual.Advance(49 * time.Millisecond)
	runtimeYield()
	if sender.count() != 0 {
		t.Fatal("recovered Accepted outbox sent early")
	}
	manual.Advance(time.Millisecond)
	waitFor(t, func() bool { return sender.count() == 1 })
	cancel()
	<-done
}

func TestOutboxRecoveryDispatchesAnExpiredDurableDeadline(t *testing.T) {
	fixture := workerFixture(t)
	repository := newFakeRepository(fixture)
	parent := fixture.delivery(t, 12)
	outputs, err := model.ExecuteOperator(fixture.topology.Spec().Stages[1].Operator, parent.Tuple)
	if err != nil {
		t.Fatal(err)
	}
	outboxes, err := deriveOutboxes(fixture.assignment, parent, outputs)
	if err != nil {
		t.Fatal(err)
	}
	outboxes[0].Accepted = true
	outboxes[0].RetryDeadlineUnixNano = int64(50 * time.Millisecond)
	repository.work.Outboxes = outboxes
	repository.outboxes[outboxes[0].ID] = outboxes[0]
	manual := clock.NewManual(time.Unix(0, int64(100*time.Millisecond)))
	sender := &fakeSender{}
	gate := admission.NewGate()
	if err = gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	options := testEngineOptions(repository, gate, sender)
	options.Clock = manual
	engine, err := NewEngine(options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	waitFor(t, func() bool { return sender.count() == 1 })
	cancel()
	<-done
}

func TestRetryDeadlineRejectsUnrepresentableOrUnsetValues(t *testing.T) {
	for _, test := range []struct {
		name     string
		start    time.Time
		interval time.Duration
		want     int64
		wantErr  bool
	}{
		{name: "negative epoch supported", start: time.Unix(0, -20), interval: 10, want: -10},
		{name: "zero is reserved for unset", start: time.Unix(0, -10), interval: 10, wantErr: true},
		{name: "positive overflow", start: time.Unix(0, math.MaxInt64-5), interval: 10, wantErr: true},
		{name: "nonpositive interval", start: time.Unix(0, 1), wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := retryDeadline(test.start, test.interval)
			if test.wantErr {
				if err == nil {
					t.Fatalf("retryDeadline = %d,nil", got)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("retryDeadline = %d,%v, want %d,nil", got, err, test.want)
			}
		})
	}
}

func TestOutboxRetryDeadlineStartsAtActualSerialSenderDispatch(t *testing.T) {
	fixture := workerFixture(t)
	repository := newFakeRepository(fixture)
	repository.work.Sources = nil
	first, err := deriveSourceOutboxes(fixture.assignment, fixture.source, 1, mustSourceTuple(t, fixture, 1))
	if err != nil {
		t.Fatal(err)
	}
	second, err := deriveSourceOutboxes(fixture.assignment, fixture.source, 2, mustSourceTuple(t, fixture, 2))
	if err != nil {
		t.Fatal(err)
	}
	outboxes := append(first, second...)
	if len(outboxes) != 2 {
		t.Fatalf("fixture outboxes = %d", len(outboxes))
	}
	repository.work.Outboxes = outboxes
	for _, outbox := range outboxes {
		repository.outboxes[outbox.ID] = outbox
	}
	manual := clock.NewManual(time.Unix(0, 0))
	sender := &serialBlockingSender{clock: manual, firstStarted: make(chan struct{}), firstRelease: make(chan struct{})}
	gate := admission.NewGate()
	if err = gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	options := testEngineOptions(repository, gate, sender)
	options.Clock = manual
	options.MaxPendingOutboxes = 2
	engine, err := NewEngine(options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	<-sender.firstStarted
	manual.Advance(100 * time.Millisecond)
	close(sender.firstRelease)
	waitFor(t, func() bool { return sender.countFor(outboxes[1].ID) == 1 })
	repository.mu.Lock()
	secondDeadline := repository.outboxes[outboxes[1].ID].RetryDeadlineUnixNano
	repository.mu.Unlock()
	if want := manual.Now().Add(options.AcceptedRetryInterval).UnixNano(); secondDeadline != want {
		t.Fatalf("second deadline = %d, want actual-dispatch deadline %d", secondDeadline, want)
	}
	runtimeYield()
	if got := sender.countFor(outboxes[1].ID); got != 1 {
		t.Fatalf("queue delay consumed retry interval: second sends=%d", got)
	}
	manual.Advance(options.AcceptedRetryInterval - time.Millisecond)
	runtimeYield()
	if got := sender.countFor(outboxes[1].ID); got != 1 {
		t.Fatalf("second outbox retried early: %d", got)
	}
	manual.Advance(time.Millisecond)
	waitFor(t, func() bool { return sender.countFor(outboxes[1].ID) == 2 })
	cancel()
	<-done
}

func TestOutboxCompletedACKCancelsAStaleQueuedDispatch(t *testing.T) {
	fixture := workerFixture(t)
	repository := newFakeRepository(fixture)
	first, err := deriveSourceOutboxes(fixture.assignment, fixture.source, 1, mustSourceTuple(t, fixture, 1))
	if err != nil {
		t.Fatal(err)
	}
	second, err := deriveSourceOutboxes(fixture.assignment, fixture.source, 2, mustSourceTuple(t, fixture, 2))
	if err != nil {
		t.Fatal(err)
	}
	outboxes := append(first, second...)
	if len(outboxes) != 2 {
		t.Fatalf("fixture outboxes = %d", len(outboxes))
	}
	repository.work.Outboxes = outboxes
	for _, outbox := range outboxes {
		repository.outboxes[outbox.ID] = outbox
	}
	repository.outboxCompleteStarted = make(chan struct{})
	repository.outboxCompleteRelease = make(chan struct{})
	manual := clock.NewManual(time.Unix(0, 1))
	sender := &serialBlockingSender{clock: manual, firstStarted: make(chan struct{}), firstRelease: make(chan struct{})}
	gate := admission.NewGate()
	if err = gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	options := testEngineOptions(repository, gate, sender)
	options.Clock = manual
	options.MaxPendingOutboxes = 2
	engine, err := NewEngine(options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	<-sender.firstStarted
	waitFor(t, func() bool { return len(engine.sendJobs) == 1 })
	blockedID := sender.firstID()
	queued := outboxes[0]
	if queued.ID == blockedID {
		queued = outboxes[1]
	}
	if got := sender.countFor(queued.ID); got != 0 {
		t.Fatalf("queued outbox was already sent before ACK race: %d", got)
	}

	completed := protocol.TupleACK{DeliveryID: queued.ID, Destination: queued.Destination, Assignment: protocol.AssignmentSetIdentity{JobID: fixture.assignment.Assignment.JobID, Revision: fixture.assignment.Assignment.Revision, Digest: fixture.assignment.Assignment.Digest}, Coordinator: fixture.epoch, Status: protocol.TupleCompleted}
	ackDone := make(chan error, 1)
	go func() { ackDone <- engine.HandleACK(ctx, completed) }()
	<-repository.outboxCompleteStarted
	close(sender.firstRelease)
	waitFor(t, func() bool { return len(engine.dispatchStarts) == 1 })
	if got := sender.countFor(queued.ID); got != 0 {
		t.Fatalf("queued outbox sent while completion was blocked: %d", got)
	}
	close(repository.outboxCompleteRelease)
	if err = <-ackDone; err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(engine.dispatchStarts) == 0 })
	if err = engine.ReconcileAssignment(ctx, fixture.assignment.Assignment.JobID); err != nil {
		t.Fatalf("stale completed dispatch terminated owner: %v", err)
	}
	if got := sender.countFor(queued.ID); got != 0 {
		t.Fatalf("completed queued outbox sequence %d (blocked %d) was sent %d times", queued.ID.Tuple.SourceSequence, blockedID.Tuple.SourceSequence, got)
	}
	repository.mu.Lock()
	completedRecord := repository.outboxes[queued.ID]
	repository.mu.Unlock()
	if !completedRecord.Completed || completedRecord.RetryDeadlineUnixNano != 0 {
		t.Fatalf("completed queued outbox mutated by stale dispatch: %+v", completedRecord)
	}
	cancel()
	if err = <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run cancellation = %v", err)
	}
}

func mustSourceTuple(t *testing.T, fixture workerTestFixture, sequence uint64) model.Tuple {
	t.Helper()
	tuple, exists, err := model.SourceTuple(fixture.topology, fixture.source.Task, sequence)
	if err != nil || !exists {
		t.Fatalf("SourceTuple(%d) = %+v,%v,%v", sequence, tuple, exists, err)
	}
	return tuple
}

type serialBlockingSender struct {
	mu           sync.Mutex
	clock        clock.Clock
	ids          []model.DeliveryID
	times        []time.Time
	firstStarted chan struct{}
	firstRelease chan struct{}
}

func (sender *serialBlockingSender) Send(ctx context.Context, delivery protocol.TupleDelivery) error {
	sender.mu.Lock()
	sender.ids = append(sender.ids, delivery.DeliveryID)
	sender.times = append(sender.times, sender.clock.Now())
	first := len(sender.ids) == 1
	sender.mu.Unlock()
	if first {
		close(sender.firstStarted)
		select {
		case <-sender.firstRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (sender *serialBlockingSender) countFor(id model.DeliveryID) int {
	sender.mu.Lock()
	defer sender.mu.Unlock()
	count := 0
	for _, sent := range sender.ids {
		if sent == id {
			count++
		}
	}
	return count
}

func (sender *serialBlockingSender) firstID() model.DeliveryID {
	sender.mu.Lock()
	defer sender.mu.Unlock()
	return sender.ids[0]
}

func advanceManualUntil(t *testing.T, manual *clock.Manual, condition func() bool) {
	t.Helper()
	for step := 0; step < 1_000; step++ {
		if condition() {
			return
		}
		manual.Advance(time.Millisecond)
		runtimeYield()
	}
	t.Fatal("manual-clock condition was not reached")
}

func TestOutboxSourcePersistsCursorAndOutboxBeforeSendAndHonorsEOFAndFence(t *testing.T) {
	fixture := workerFixture(t)
	repository := newFakeRepository(fixture)
	repository.work.Sources = nil
	sender := &fakeSender{before: func() { repository.mu.Lock(); repository.log = append(repository.log, "send"); repository.mu.Unlock() }}
	gate := admission.NewGate()
	_ = gate.Open(fixture.epoch)
	options := testEngineOptions(repository, gate, sender)
	options.MaxPendingOutboxes = 1
	engine, err := NewEngine(options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	waitFor(t, func() bool { return sender.count() >= 1 })
	repository.mu.Lock()
	if len(repository.log) < 3 || repository.log[0] != "source" || repository.log[1] != "outbox-dispatched" || repository.log[2] != "send" {
		t.Fatalf("source durability order = %v", repository.log)
	}
	if len(repository.work.Sources) != 1 || repository.work.Sources[0].NextSequence != 2 || repository.work.Sources[0].EOF != 15 {
		t.Fatalf("source cursor = %+v", repository.work.Sources)
	}
	repository.mu.Unlock()
	cancel()
	<-done

	// A gate opened for a newer fence cannot revive an old Running install.
	stale := newFakeRepository(fixture)
	stale.work.Sources = nil
	newer := fixture.epoch
	newer.Term++
	newer.BeginIndex++
	newer.Nonce[0]++
	stale.work.Fence = newer
	staleGate := admission.NewGate()
	_ = staleGate.Open(newer)
	staleSender := &fakeSender{}
	staleEngine, err := NewEngine(testEngineOptions(stale, staleGate, staleSender))
	if err != nil {
		t.Fatal(err)
	}
	staleCtx, staleCancel := context.WithCancel(context.Background())
	staleDone := runEngine(t, staleCtx, staleEngine)
	<-staleEngine.Ready()
	runtimeYield()
	if staleSender.count() != 0 {
		t.Fatal("new gate revived old-fence source")
	}
	staleCancel()
	<-staleDone
}

func TestOutboxBoundedBackpressurePausesSourceUntilDurableCompletion(t *testing.T) {
	fixture := workerFixture(t)
	repository := newFakeRepository(fixture)
	repository.work.Sources = nil
	parent := fixture.delivery(t, 12)
	outputs, _ := model.ExecuteOperator(fixture.topology.Spec().Stages[1].Operator, parent.Tuple)
	recovered, _ := deriveOutboxes(fixture.assignment, parent, outputs)
	repository.work.Outboxes = recovered
	for _, outbox := range recovered {
		repository.outboxes[outbox.ID] = outbox
	}
	manual := clock.NewManual(time.Unix(0, 0))
	sender := &fakeSender{}
	gate := admission.NewGate()
	_ = gate.Open(fixture.epoch)
	options := testEngineOptions(repository, gate, sender)
	options.Clock = manual
	options.MaxPendingOutboxes = 1
	engine, err := NewEngine(options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	waitFor(t, func() bool { return sender.count() == 1 })
	repository.mu.Lock()
	sourceCalls := 0
	for _, entry := range repository.log {
		if entry == "source" {
			sourceCalls++
		}
	}
	repository.mu.Unlock()
	if sourceCalls != 0 {
		t.Fatal("source advanced while durable outbox bound was full")
	}
	ack := protocol.TupleACK{DeliveryID: recovered[0].ID, Destination: recovered[0].Destination, Assignment: protocol.AssignmentSetIdentity{JobID: fixture.assignment.Assignment.JobID, Revision: fixture.assignment.Assignment.Revision, Digest: fixture.assignment.Assignment.Digest}, Coordinator: fixture.epoch, Status: protocol.TupleCompleted}
	if err := engine.HandleACK(ctx, ack); err != nil {
		t.Fatal(err)
	}
	manual.Advance(10 * time.Millisecond)
	waitFor(t, func() bool {
		repository.mu.Lock()
		defer repository.mu.Unlock()
		for _, entry := range repository.log {
			if entry == "source" {
				return true
			}
		}
		return false
	})
	cancel()
	<-done
}

func TestOutboxClosedAssignmentNeverEmitsSource(t *testing.T) {
	fixture := workerFixture(t)
	repository := newFakeRepository(fixture)
	closed := repository.assignments[fixture.assignment.Assignment.JobID]
	closed.SchedulingState = model.Closed
	repository.assignments[closed.Assignment.JobID] = closed
	repository.work.Assignments[0] = closed
	gate := admission.NewGate()
	_ = gate.Open(fixture.epoch)
	sender := &fakeSender{}
	engine, err := NewEngine(testEngineOptions(repository, gate, sender))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	runtimeYield()
	if sender.count() != 0 {
		t.Fatal("Closed assignment emitted source work")
	}
	repository.mu.Lock()
	if len(repository.log) != 0 {
		t.Fatalf("Closed assignment mutated repository: %v", repository.log)
	}
	repository.mu.Unlock()
	cancel()
	<-done
}

func TestOutboxEmptySourceDurablyRecordsEOFWithoutSending(t *testing.T) {
	fixture := workerFixtureWithRange(t, "5", "5")
	repository := newFakeRepository(fixture)
	repository.work.Sources = nil
	gate := admission.NewGate()
	_ = gate.Open(fixture.epoch)
	sender := &fakeSender{}
	options := testEngineOptions(repository, gate, sender)
	options.MaxPendingOutboxes = 1
	engine, err := NewEngine(options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	waitFor(t, func() bool {
		repository.mu.Lock()
		defer repository.mu.Unlock()
		return len(repository.work.Sources) == 1
	})
	repository.mu.Lock()
	cursor := repository.work.Sources[0]
	repository.mu.Unlock()
	if cursor.NextSequence != 1 || cursor.EOF != 0 || sender.count() != 0 {
		t.Fatalf("empty source cursor/send = %+v/%d", cursor, sender.count())
	}
	if tuple, exists, err := model.SourceTuple(fixture.topology, fixture.source.Task, cursor.NextSequence); err != nil || exists || len(tuple.Fields) != 0 {
		t.Fatalf("post-empty EOF parity = %+v,%v,%v", tuple, exists, err)
	}
	cancel()
	<-done
}

func TestOutboxFinalSourceSequenceAndPostEOFMatchModel(t *testing.T) {
	fixture := workerFixtureWithRange(t, "5", "6")
	repository := newFakeRepository(fixture)
	repository.work.Sources = nil
	manual := clock.NewManual(time.Unix(0, 0))
	sender := &fakeSender{}
	gate := admission.NewGate()
	_ = gate.Open(fixture.epoch)
	options := testEngineOptions(repository, gate, sender)
	options.Clock = manual
	options.MaxPendingOutboxes = 1
	engine, err := NewEngine(options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	waitFor(t, func() bool { return sender.count() == 1 })
	sender.mu.Lock()
	sent := sender.deliveries[0]
	sender.mu.Unlock()
	want, exists, err := model.SourceTuple(fixture.topology, fixture.source.Task, 1)
	if err != nil || !exists || sent.Tuple.Fields[0].Value.Int64 != want.Fields[0].Value.Int64 {
		t.Fatalf("final source tuple parity = %+v/%+v,%v,%v", sent.Tuple, want, exists, err)
	}
	ack := protocol.TupleACK{DeliveryID: sent.DeliveryID, Destination: sent.Destination, Assignment: sent.Assignment, Coordinator: sent.Coordinator, Status: protocol.TupleCompleted}
	if err := engine.HandleACK(ctx, ack); err != nil {
		t.Fatal(err)
	}
	manual.Advance(time.Second)
	runtimeYield()
	if sender.count() != 1 {
		t.Fatal("source emitted after immutable EOF")
	}
	repository.mu.Lock()
	cursor := repository.work.Sources[0]
	repository.mu.Unlock()
	if cursor.NextSequence != 2 || cursor.EOF != 1 {
		t.Fatalf("final source cursor = %+v", cursor)
	}
	if tuple, present, err := model.SourceTuple(fixture.topology, fixture.source.Task, cursor.NextSequence); err != nil || present || len(tuple.Fields) != 0 {
		t.Fatalf("post-EOF parity = %+v,%v,%v", tuple, present, err)
	}
	cancel()
	<-done
}

func TestOutboxMaximumSourceSequenceAndPostEOFMatchModel(t *testing.T) {
	fixture := workerFixtureWithRange(t, "0", "1000000")
	limit := model.LimitsV1().MaxSourceSequences
	repository := newFakeRepository(fixture)
	repository.work.Sources = []store.SourceCursor{{Source: fixture.source.Task, NextSequence: limit, EOF: limit}}
	manual := clock.NewManual(time.Unix(0, 0))
	sender := &fakeSender{}
	gate := admission.NewGate()
	if err := gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	options := testEngineOptions(repository, gate, sender)
	options.Clock = manual
	options.MaxPendingOutboxes = 1
	engine, err := NewEngine(options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	waitFor(t, func() bool { return sender.count() == 1 })
	sender.mu.Lock()
	sent := sender.deliveries[0]
	sender.mu.Unlock()
	want, exists, err := model.SourceTuple(fixture.topology, fixture.source.Task, limit)
	if err != nil || !exists {
		t.Fatalf("maximum SourceTuple = %+v,%v,%v", want, exists, err)
	}
	gotBytes, _ := model.MarshalTuple(sent.Tuple)
	wantBytes, _ := model.MarshalTuple(want)
	if !bytes.Equal(gotBytes, wantBytes) {
		t.Fatalf("maximum source tuple differs: got %x want %x", gotBytes, wantBytes)
	}
	ack := protocol.TupleACK{DeliveryID: sent.DeliveryID, Destination: sent.Destination, Assignment: sent.Assignment, Coordinator: sent.Coordinator, Status: protocol.TupleCompleted}
	if err = engine.HandleACK(ctx, ack); err != nil {
		t.Fatal(err)
	}
	manual.Advance(time.Second)
	runtimeYield()
	if sender.count() != 1 {
		t.Fatal("maximum source emitted after immutable EOF")
	}
	repository.mu.Lock()
	cursor := repository.work.Sources[0]
	repository.mu.Unlock()
	if cursor.NextSequence != limit+1 || cursor.EOF != limit {
		t.Fatalf("maximum source cursor = %+v", cursor)
	}
	if tuple, present, tupleErr := model.SourceTuple(fixture.topology, fixture.source.Task, cursor.NextSequence); tupleErr != nil || present || len(tuple.Fields) != 0 {
		t.Fatalf("maximum post-EOF parity = %+v,%v,%v", tuple, present, tupleErr)
	}
	cancel()
	<-done
}
