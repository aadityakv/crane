package worker

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/crane/admission"
	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
	"github.com/aaditya/cs425mp3/internal/crane/store"
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

func runtimeYield() {
	for i := 0; i < 100; i++ {
		runtime.Gosched()
	}
}
