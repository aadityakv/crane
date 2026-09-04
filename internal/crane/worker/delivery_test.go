package worker

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"crane/internal/clock"
	"crane/internal/crane/admission"
	"crane/internal/crane/model"
	"crane/internal/crane/protocol"
	"crane/internal/crane/store"
)

func TestDeliveryReceiverStateTableAndVolatileExecutingDeduplication(t *testing.T) {
	fixture := workerFixture(t)
	for _, test := range []struct {
		name  string
		state store.DeliveryState
		want  protocol.TupleACKStatus
	}{
		{name: "unknown", want: protocol.TupleAccepted},
		{name: "received", state: store.Received, want: protocol.TupleAccepted},
		{name: "processed", state: store.Processed, want: protocol.TupleAccepted},
		{name: "completed", state: store.Completed, want: protocol.TupleCompleted},
		{name: "compacted", state: store.Compacted, want: protocol.TupleCompleted},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository := newFakeRepository(fixture)
			record := fixture.delivery(t, 2)
			if test.state != 0 {
				record.State = test.state
				repository.deliveries[record.ID] = record
			}
			gate := admission.NewGate()
			if err := gate.Open(fixture.epoch); err != nil {
				t.Fatal(err)
			}
			block := make(chan struct{})
			options := testEngineOptions(repository, gate, &fakeSender{})
			options.Execute = func(ctx context.Context, operator model.OperatorSpec, tuple model.Tuple) ([]model.Tuple, error) {
				select {
				case <-block:
					return model.ExecuteOperator(operator, tuple)
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			engine, err := NewEngine(options)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := runEngine(t, ctx, engine)
			<-engine.Ready()
			ack, err := engine.HandleDelivery(ctx, deliveryMessage(record))
			if err != nil || ack.Status != test.want || ack.DeliveryID != record.ID || ack.Destination != record.Destination || ack.Coordinator != record.CoordinatorEpoch {
				t.Fatalf("HandleDelivery = %+v,%v", ack, err)
			}
			cancel()
			close(block)
			<-done
		})
	}

	// Executing is volatile: duplicate custody still returns Accepted, while only
	// one deterministic operator invocation owns the delivery.
	repository := newFakeRepository(fixture)
	gate := admission.NewGate()
	_ = gate.Open(fixture.epoch)
	started := make(chan struct{})
	options := testEngineOptions(repository, gate, &fakeSender{})
	var calls atomic.Int32
	options.Execute = func(ctx context.Context, _ model.OperatorSpec, _ model.Tuple) ([]model.Tuple, error) {
		calls.Add(1)
		select {
		case <-started:
		default:
			close(started)
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	engine, err := NewEngine(options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	message := fixture.message(t, 3)
	if _, err := engine.HandleDelivery(ctx, message); err != nil {
		t.Fatal(err)
	}
	<-started
	if ack, err := engine.HandleDelivery(ctx, message); err != nil || ack.Status != protocol.TupleAccepted {
		t.Fatalf("executing duplicate = %+v,%v", ack, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("executing calls = %d", calls.Load())
	}
	cancel()
	<-done
}

func TestDeliveryDuplicateProbeDoesNotBypassReadyLifecycle(t *testing.T) {
	fixture := workerFixture(t)
	repository := newFakeRepository(fixture)
	record := fixture.delivery(t, 7)
	repository.deliveries[record.ID] = record
	gate := admission.NewGate()
	if err := gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(testEngineOptions(repository, gate, &fakeSender{}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = engine.HandleDelivery(context.Background(), deliveryMessage(record)); !errors.Is(err, ErrNotReady) {
		t.Fatalf("duplicate before Ready = %v", err)
	}
	if repository.probeCalls != 0 {
		t.Fatal("duplicate probed durable state before recovery established local identity")
	}
}

func TestDeliveryExactDurableDuplicateBypassesClosedAdvancedAdmission(t *testing.T) {
	fixture := workerFixture(t)
	for _, test := range []struct {
		name  string
		state store.DeliveryState
		want  protocol.TupleACKStatus
	}{
		{name: "received", state: store.Received, want: protocol.TupleAccepted},
		{name: "processed", state: store.Processed, want: protocol.TupleAccepted},
		{name: "completed", state: store.Completed, want: protocol.TupleCompleted},
		{name: "compacted", state: store.Compacted, want: protocol.TupleCompleted},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository := newFakeRepository(fixture)
			record := fixture.delivery(t, 2)
			record.State = test.state
			if test.state == store.Processed {
				outputs, err := model.ExecuteOperator(fixture.topology.Spec().Stages[1].Operator, record.Tuple)
				if err != nil {
					t.Fatal(err)
				}
				outboxes, err := deriveOutboxes(fixture.assignment, record, outputs)
				if err != nil {
					t.Fatal(err)
				}
				for _, outbox := range outboxes {
					record.OutboxIDs = append(record.OutboxIDs, outbox.ID)
					repository.outboxes[outbox.ID] = outbox
				}
				record.Outputs = outputs
				repository.work.Outboxes = outboxes
			}
			repository.deliveries[record.ID] = record
			repository.work.Deliveries = []store.DeliveryRecord{record}
			newer := fixture.epoch
			newer.Term++
			newer.BeginIndex++
			newer.Nonce[0]++
			repository.work.Fence = newer
			gate := admission.NewGate()
			engine, err := NewEngine(testEngineOptions(repository, gate, &fakeSender{}))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := runEngine(t, ctx, engine)
			<-engine.Ready()
			ack, err := engine.HandleDelivery(ctx, deliveryMessage(record))
			if err != nil || ack.Status != test.want {
				t.Fatalf("exact durable duplicate = %+v,%v", ack, err)
			}
			changed := deliveryMessage(record)
			encoded, marshalErr := protocol.MarshalTupleDelivery(changed)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			changed, marshalErr = protocol.UnmarshalTupleDelivery(encoded)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			changed.Tuple.Fields[0].Value.Int64++
			if _, err = engine.HandleDelivery(ctx, changed); !errors.Is(err, model.ErrIdentityReuse) {
				t.Fatalf("changed durable duplicate = %v", err)
			}
			unknown := deliveryMessage(record)
			unknown.DeliveryID.Tuple.SourceSequence++
			if _, err = engine.HandleDelivery(ctx, unknown); !errors.Is(err, admission.ErrClosed) {
				t.Fatalf("unknown closed-gate delivery = %v", err)
			}
			if repository.receiveCalls != 0 {
				t.Fatalf("duplicate path called Receive %d times", repository.receiveCalls)
			}
			cancel()
			<-done
		})
	}
}

func TestDeliveryRejectsEveryAuthorityAndRouteMismatchBeforeReceive(t *testing.T) {
	fixture := workerFixture(t)
	base := fixture.message(t, 4)
	mutations := map[string]func(*protocol.TupleDelivery){
		"job":               func(message *protocol.TupleDelivery) { message.Assignment.JobID[15] ^= 1 },
		"route":             func(message *protocol.TupleDelivery) { message.DeliveryID.EdgeID = 2 },
		"producer token":    func(message *protocol.TupleDelivery) { message.Producer.Attempt++ },
		"destination token": func(message *protocol.TupleDelivery) { message.Destination.Attempt++ },
		"attempt":           func(message *protocol.TupleDelivery) { message.Destination.WorkerEpoch[15] ^= 1 },
		"epoch":             func(message *protocol.TupleDelivery) { message.Coordinator.Term++ },
		"set revision":      func(message *protocol.TupleDelivery) { message.Assignment.Revision++ },
		"set digest":        func(message *protocol.TupleDelivery) { message.Assignment.Digest[0] ^= 1 },
		"tuple bytes":       func(message *protocol.TupleDelivery) { message.Tuple.Fields[0].Value.Int64++ },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			repository := newFakeRepository(fixture)
			gate := admission.NewGate()
			_ = gate.Open(fixture.epoch)
			engine, err := NewEngine(testEngineOptions(repository, gate, &fakeSender{}))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := runEngine(t, ctx, engine)
			<-engine.Ready()
			message := base
			message.Tuple = cloneTestTuples([]model.Tuple{base.Tuple})[0]
			mutate(&message)
			if _, err := engine.HandleDelivery(ctx, message); err == nil {
				t.Fatal("invalid delivery accepted")
			}
			if repository.receiveCalls != 0 {
				t.Fatalf("Receive calls = %d", repository.receiveCalls)
			}
			cancel()
			<-done
		})
	}
}

func TestDeliveryRequiresGateThenExactDurableRunningFence(t *testing.T) {
	fixture := workerFixture(t)
	for _, test := range []struct {
		name      string
		configure func(*fakeRepository, *admission.Gate)
	}{
		{name: "closed gate", configure: func(_ *fakeRepository, _ *admission.Gate) {}},
		{name: "closed install", configure: func(repository *fakeRepository, gate *admission.Gate) {
			_ = gate.Open(fixture.epoch)
			value := repository.assignments[fixture.assignment.Assignment.JobID]
			value.SchedulingState = model.Closed
			repository.assignments[value.Assignment.JobID] = value
		}},
		{name: "old running install", configure: func(repository *fakeRepository, gate *admission.Gate) {
			newer := fixture.epoch
			newer.Term++
			newer.BeginIndex++
			newer.Nonce[0]++
			repository.work.Fence = newer
			_ = gate.Open(newer)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository := newFakeRepository(fixture)
			gate := admission.NewGate()
			test.configure(repository, gate)
			engine, err := NewEngine(testEngineOptions(repository, gate, &fakeSender{}))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := runEngine(t, ctx, engine)
			<-engine.Ready()
			if _, err := engine.HandleDelivery(ctx, fixture.message(t, 5)); err == nil {
				t.Fatal("delivery entered without current Running authority")
			}
			if repository.receiveCalls != 0 {
				t.Fatalf("Receive calls = %d", repository.receiveCalls)
			}
			cancel()
			<-done
		})
	}
}

func TestDeliveryRejectsValidNonLocalDestinationBeforeReceive(t *testing.T) {
	fixture := workerFixture(t)
	repository := newFakeRepository(fixture)
	repository.localNode = 2
	repository.localEpoch = model.WorkerEpoch{2}
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
	if _, err = engine.HandleDelivery(ctx, fixture.message(t, 5)); err == nil {
		t.Fatal("delivery for another durable worker identity was accepted")
	}
	if repository.receiveCalls != 0 {
		t.Fatalf("Receive calls = %d", repository.receiveCalls)
	}
	cancel()
	<-done
}

func TestDeliveryOperatorFailurePersistsOneDurableDeduplicatedEvent(t *testing.T) {
	fixture := workerFixture(t)
	repository := newFakeRepository(fixture)
	gate := admission.NewGate()
	_ = gate.Open(fixture.epoch)
	options := testEngineOptions(repository, gate, &fakeSender{})
	options.Execute = func(context.Context, model.OperatorSpec, model.Tuple) ([]model.Tuple, error) {
		return nil, errors.New("deterministic overflow")
	}
	engine, err := NewEngine(options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	message := fixture.message(t, 6)
	if _, err := engine.HandleDelivery(ctx, message); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		repository.mu.Lock()
		defer repository.mu.Unlock()
		return repository.persistEventCalls == 1
	})
	if _, err := engine.HandleDelivery(ctx, message); err != nil {
		t.Fatal(err)
	}
	testClock := options.Clock.(*clock.Manual)
	testClock.Advance(time.Second)
	runtimeYield()
	repository.mu.Lock()
	if repository.persistEventCalls != 1 || len(repository.work.PendingEvents) != 1 || repository.work.NextTransactionID != 2 {
		t.Fatalf("failure persistence = calls %d pending %d next %d", repository.persistEventCalls, len(repository.work.PendingEvents), repository.work.NextTransactionID)
	}
	repository.mu.Unlock()
	cancel()
	<-done
}

func TestDeliveryCanceledHandlerTransfersAdmissionUntilQueuedCommandFinishes(t *testing.T) {
	fixture := workerFixture(t)
	repository := newFakeRepository(fixture)
	repository.receiveStarted = make(chan struct{})
	repository.receiveRelease = make(chan struct{})
	gate := admission.NewGate()
	if err := gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(testEngineOptions(repository, gate, &fakeSender{}))
	if err != nil {
		t.Fatal(err)
	}
	engineCtx, cancelEngine := context.WithCancel(context.Background())
	done := runEngine(t, engineCtx, engine)
	<-engine.Ready()
	first := make(chan error, 1)
	go func() {
		_, callErr := engine.HandleDelivery(context.Background(), fixture.message(t, 7))
		first <- callErr
	}()
	<-repository.receiveStarted
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	second := make(chan error, 1)
	go func() {
		_, callErr := engine.HandleDelivery(requestCtx, fixture.message(t, 8))
		second <- callErr
	}()
	waitFor(t, func() bool { return len(engine.commands) == 1 })
	cancelRequest()
	returned := false
	for index := 0; index < 100_000; index++ {
		select {
		case err = <-second:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled delivery handler = %v", err)
			}
			returned = true
		default:
			runtime.Gosched()
		}
		if returned {
			break
		}
	}
	if !returned {
		close(repository.receiveRelease)
		<-second
		cancelEngine()
		<-done
		t.Fatal("canceled delivery handler did not return promptly")
	}
	closed := make(chan error, 1)
	go func() { closed <- gate.CloseAndWait(context.Background()) }()
	for index := 0; index < 10_000; index++ {
		select {
		case err = <-closed:
			t.Fatalf("CloseAndWait returned while admitted commands were unresolved: %v", err)
		default:
			runtime.Gosched()
		}
	}
	close(repository.receiveRelease)
	if err = <-first; err != nil {
		t.Fatal(err)
	}
	if err = <-closed; err != nil {
		t.Fatal(err)
	}
	repository.mu.Lock()
	callsAtClose := repository.receiveCalls
	repository.mu.Unlock()
	runtimeYield()
	repository.mu.Lock()
	callsAfterClose := repository.receiveCalls
	repository.mu.Unlock()
	if callsAtClose != 2 || callsAfterClose != callsAtClose {
		t.Fatalf("Receive calls at/after close = %d/%d", callsAtClose, callsAfterClose)
	}
	cancelEngine()
	<-done
}

func TestACKCanceledHandlerReturnsWhileDurableOwnerTransitionContinues(t *testing.T) {
	fixture := workerFixture(t)
	repository := newFakeRepository(fixture)
	parent := fixture.delivery(t, 9)
	outputs, err := model.ExecuteOperator(fixture.topology.Spec().Stages[1].Operator, parent.Tuple)
	if err != nil {
		t.Fatal(err)
	}
	outboxes, err := deriveOutboxes(fixture.assignment, parent, outputs)
	if err != nil {
		t.Fatal(err)
	}
	parent.State, parent.Outputs = store.Processed, outputs
	for _, outbox := range outboxes {
		parent.OutboxIDs = append(parent.OutboxIDs, outbox.ID)
		repository.outboxes[outbox.ID] = outbox
	}
	repository.work.Deliveries = []store.DeliveryRecord{parent}
	repository.work.Outboxes = outboxes
	repository.deliveries[parent.ID] = parent
	repository.outboxCompleteStarted = make(chan struct{})
	repository.outboxCompleteRelease = make(chan struct{})
	gate := admission.NewGate()
	if err = gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(testEngineOptions(repository, gate, &fakeSender{}))
	if err != nil {
		t.Fatal(err)
	}
	engineCtx, cancelEngine := context.WithCancel(context.Background())
	done := runEngine(t, engineCtx, engine)
	<-engine.Ready()
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	result := make(chan error, 1)
	ack := protocol.TupleACK{DeliveryID: outboxes[0].ID, Destination: outboxes[0].Destination, Assignment: protocol.AssignmentSetIdentity{JobID: fixture.assignment.Assignment.JobID, Revision: fixture.assignment.Assignment.Revision, Digest: fixture.assignment.Assignment.Digest}, Coordinator: fixture.epoch, Status: protocol.TupleCompleted}
	go func() { result <- engine.HandleACK(requestCtx, ack) }()
	<-repository.outboxCompleteStarted
	cancelRequest()
	returned := false
	for index := 0; index < 100_000; index++ {
		select {
		case err = <-result:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled ACK handler = %v", err)
			}
			returned = true
		default:
			runtime.Gosched()
		}
		if returned {
			break
		}
	}
	close(repository.outboxCompleteRelease)
	if !returned {
		<-result
		cancelEngine()
		<-done
		t.Fatal("canceled ACK handler did not return promptly")
	}
	waitFor(t, func() bool {
		repository.mu.Lock()
		defer repository.mu.Unlock()
		return repository.outboxes[outboxes[0].ID].Completed
	})
	cancelEngine()
	<-done
}

func runtimeYield() {
	for i := 0; i < 100; i++ {
		runtime.Gosched()
	}
}
