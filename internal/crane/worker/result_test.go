package worker

import (
	"context"
	"sync"
	"testing"

	"github.com/aaditya/cs425mp3/internal/crane/admission"
	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/store"
)

func TestResultCurrentPrimaryPersistsReplicatesExactSecondaryThenCompletes(t *testing.T) {
	fixture, sink, replica := workerFixtureWithLocalPrimarySink(t)
	repository := newFakeRepository(fixture)
	delivery := fixture.sinkDelivery(t, sink, 1)
	repository.work.Deliveries = []store.DeliveryRecord{delivery}
	repository.deliveries[delivery.ID] = delivery
	replicator := &fakeResultReplicator{calls: make(chan resultReplicationCall, 1)}
	gate := admission.NewGate()
	if err := gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	options := testEngineOptions(repository, gate, &fakeSender{})
	options.Replicator = replicator
	engine, err := NewEngine(options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	call := <-replicator.calls
	if call.provenance.DestinationRole != model.SecondaryReplica || call.provenance.ReplicaSet != replica || call.provenance.CoordinatorEpoch != fixture.epoch {
		t.Fatalf("replication provenance=%+v replica=%+v", call.provenance, replica)
	}
	replicator.ack(call)
	waitFor(t, func() bool {
		repository.mu.Lock()
		defer repository.mu.Unlock()
		return len(repository.results) == 1 && repository.deliveries[delivery.ID].State == store.Completed
	})
	repository.mu.Lock()
	if got := repository.log; indexOf(got, "result") < 0 || indexOf(got, "result") > indexOf(got, "completed") {
		t.Fatalf("durable ordering=%v", got)
	}
	stored := repository.results[0]
	repository.mu.Unlock()
	if stored.Provenance.DestinationRole != model.PrimaryReplica || stored.Provenance.ReplicaSet != replica {
		t.Fatalf("local result=%+v", stored)
	}
	cancel()
	<-done
}

func TestResultRejectsWrongReplicationReceiptBeforeCompletion(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ResultReplicationReceipt, workerTestFixture)
	}{
		{name: "same node", mutate: func(receipt *ResultReplicationReceipt, fixture workerTestFixture) {
			receipt.DestinationNodeID = fixture.localNode
		}},
		{name: "worker epoch", mutate: func(receipt *ResultReplicationReceipt, _ workerTestFixture) { receipt.DestinationWorkerEpoch[0]++ }},
		{name: "stream checksum", mutate: func(receipt *ResultReplicationReceipt, _ workerTestFixture) { receipt.StreamChecksum[0]++ }},
		{name: "stream length", mutate: func(receipt *ResultReplicationReceipt, _ workerTestFixture) { receipt.StreamLength++ }},
		{name: "coordinator epoch", mutate: func(receipt *ResultReplicationReceipt, _ workerTestFixture) { receipt.CoordinatorEpoch.Nonce[0]++ }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture, sink, _ := workerFixtureWithLocalPrimarySink(t)
			repository := newFakeRepository(fixture)
			delivery := fixture.sinkDelivery(t, sink, 1)
			repository.work.Deliveries = []store.DeliveryRecord{delivery}
			repository.deliveries[delivery.ID] = delivery
			replicator := &fakeResultReplicator{calls: make(chan resultReplicationCall, 1), mutateReceipt: func(receipt *ResultReplicationReceipt) { test.mutate(receipt, fixture) }}
			gate := admission.NewGate()
			if err := gate.Open(fixture.epoch); err != nil {
				t.Fatal(err)
			}
			options := testEngineOptions(repository, gate, &fakeSender{})
			options.Replicator = replicator
			engine, err := NewEngine(options)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := runEngine(t, ctx, engine)
			<-engine.Ready()
			call := <-replicator.calls
			replicator.ack(call)
			if err := <-done; err == nil {
				t.Fatal("wrong durable replication receipt did not stop owner")
			}
			cancel()
			repository.mu.Lock()
			defer repository.mu.Unlock()
			if repository.deliveries[delivery.ID].State == store.Completed {
				t.Fatal("delivery completed without exact secondary receipt")
			}
		})
	}
}

func TestResultRecoveryNeverUsesNormalReplicatorForHistoricalProvenance(t *testing.T) {
	fixture, sink, replica := workerFixtureWithLocalPrimarySink(t)
	repository := newFakeRepository(fixture)
	record, provenance := fixture.result(t, sink, replica, 1, model.PrimaryReplica)
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
	repository.assignments[replacement.JobID] = installed
	repository.work.Assignments = []store.InstalledAssignment{installed}
	repository.work.Results = []store.StoredResult{{Record: record, Provenance: provenance}}
	repository.results = append([]store.StoredResult(nil), repository.work.Results...)
	replicator := &fakeResultReplicator{calls: make(chan resultReplicationCall, 1)}
	gate := admission.NewGate()
	if err := gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	options := testEngineOptions(repository, gate, &fakeSender{})
	options.Replicator = replicator
	engine, err := NewEngine(options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	for i := 0; i < 10_000; i++ {
		select {
		case <-replicator.calls:
			t.Fatal("historical provenance entered normal result replicator")
		default:
		}
	}
	cancel()
	<-done
}

func TestResultRecoveryResumesExactDurablePrimaryWithoutRegeneration(t *testing.T) {
	fixture, sink, replica := workerFixtureWithLocalPrimarySink(t)
	repository := newFakeRepository(fixture)
	delivery := fixture.sinkDelivery(t, sink, 1)
	delivery.State = store.Processed
	delivery.Outputs = []model.Tuple{cloneTuple(delivery.Tuple)}
	record, provenance := fixture.result(t, sink, replica, 1, model.PrimaryReplica)
	repository.work.Deliveries = []store.DeliveryRecord{delivery}
	repository.work.Results = []store.StoredResult{{Record: record, Provenance: provenance}}
	repository.deliveries[delivery.ID] = delivery
	repository.results = append([]store.StoredResult(nil), repository.work.Results...)
	replicator := &fakeResultReplicator{calls: make(chan resultReplicationCall, 1)}
	gate := admission.NewGate()
	if err := gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	options := testEngineOptions(repository, gate, &fakeSender{})
	options.Replicator = replicator
	engine, err := NewEngine(options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	call := <-replicator.calls
	if call.record.Checksum != record.Checksum || string(call.record.Value) != string(record.Value) {
		t.Fatalf("recovery changed durable result: got=%+v want=%+v", call.record, record)
	}
	replicator.ack(call)
	waitFor(t, func() bool {
		repository.mu.Lock()
		defer repository.mu.Unlock()
		return repository.deliveries[delivery.ID].State == store.Completed
	})
	repository.mu.Lock()
	if indexOf(repository.log, "result") >= 0 {
		t.Fatalf("recovery redundantly regenerated/upserted result: %v", repository.log)
	}
	repository.mu.Unlock()
	cancel()
	<-done
}

func TestResultRecoveryRetainsSecondaryWithoutNormalReplication(t *testing.T) {
	fixture, sink, replica := workerFixtureWithLocalPrimarySink(t)
	fixture.localNode, fixture.localEpoch = replica.SecondaryNodeID, replica.SecondaryEpoch
	repository := newFakeRepository(fixture)
	record, provenance := fixture.result(t, sink, replica, 1, model.SecondaryReplica)
	repository.work.Results = []store.StoredResult{{Record: record, Provenance: provenance}}
	repository.results = append([]store.StoredResult(nil), repository.work.Results...)
	replicator := &fakeResultReplicator{calls: make(chan resultReplicationCall, 1)}
	gate := admission.NewGate()
	if err := gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	options := testEngineOptions(repository, gate, &fakeSender{})
	options.Replicator = replicator
	engine, err := NewEngine(options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	for i := 0; i < 10_000; i++ {
		select {
		case <-replicator.calls:
			t.Fatal("durable secondary copy entered normal primary replication")
		default:
		}
	}
	cancel()
	<-done
}

func TestResultDiscardsExecutionWhoseSinkAuthorityChangedBeforePersistence(t *testing.T) {
	fixture, sink, _ := workerFixtureWithLocalPrimarySink(t)
	repository := newFakeRepository(fixture)
	delivery := fixture.sinkDelivery(t, sink, 1)
	repository.work.Deliveries = []store.DeliveryRecord{delivery}
	repository.deliveries[delivery.ID] = delivery
	started := make(chan struct{})
	release := make(chan struct{})
	gate := admission.NewGate()
	if err := gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	options := testEngineOptions(repository, gate, &fakeSender{})
	options.Execute = func(ctx context.Context, operator model.OperatorSpec, tuple model.Tuple) ([]model.Tuple, error) {
		close(started)
		select {
		case <-release:
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
	<-started
	repository.mu.Lock()
	newFence := fixture.epoch
	newFence.Term++
	newFence.BeginIndex++
	newFence.Nonce[0]++
	repository.work.Fence = newFence
	repository.mu.Unlock()
	close(release)
	for index := 0; index < 100; index++ {
		if err := engine.ReconcileAssignment(ctx, fixture.assignment.Assignment.JobID); err != nil {
			t.Fatal(err)
		}
	}
	repository.mu.Lock()
	state := repository.deliveries[delivery.ID].State
	log := append([]string(nil), repository.log...)
	repository.mu.Unlock()
	if state != store.Received || indexOf(log, "processed") >= 0 || indexOf(log, "result") >= 0 {
		t.Fatalf("stale sink execution mutated durable state: state=%v log=%v", state, log)
	}
	cancel()
	<-done
}

func TestResultRecoveryFailsClosedUnderClosedAssignment(t *testing.T) {
	fixture, sink, replica := workerFixtureWithLocalPrimarySink(t)
	fixture.assignment.SchedulingState = model.Closed
	repository := newFakeRepository(fixture)
	delivery := fixture.sinkDelivery(t, sink, 1)
	delivery.State = store.Processed
	delivery.Outputs = []model.Tuple{cloneTuple(delivery.Tuple)}
	record, provenance := fixture.result(t, sink, replica, 1, model.PrimaryReplica)
	repository.work.Assignments = []store.InstalledAssignment{fixture.assignment}
	repository.work.Deliveries = []store.DeliveryRecord{delivery}
	repository.work.Results = []store.StoredResult{{Record: record, Provenance: provenance}}
	repository.assignments[fixture.assignment.Assignment.JobID] = fixture.assignment
	repository.deliveries[delivery.ID] = delivery
	repository.results = append([]store.StoredResult(nil), repository.work.Results...)
	replicator := &fakeResultReplicator{calls: make(chan resultReplicationCall, 1)}
	options := testEngineOptions(repository, admission.NewGate(), &fakeSender{})
	options.Replicator = replicator
	engine, err := NewEngine(options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	for index := 0; index < 10_000; index++ {
		select {
		case <-replicator.calls:
			t.Fatal("closed assignment entered normal result replication")
		default:
		}
	}
	cancel()
	<-done
}

func TestResultInFlightReceiptAfterAuthorityClosureIsBenignAndRetained(t *testing.T) {
	fixture, sink, replica := workerFixtureWithLocalPrimarySink(t)
	repository := newFakeRepository(fixture)
	delivery := fixture.sinkDelivery(t, sink, 1)
	delivery.State = store.Processed
	delivery.Outputs = []model.Tuple{cloneTuple(delivery.Tuple)}
	record, provenance := fixture.result(t, sink, replica, 1, model.PrimaryReplica)
	repository.work.Deliveries = []store.DeliveryRecord{delivery}
	repository.work.Results = []store.StoredResult{{Record: record, Provenance: provenance}}
	repository.deliveries[delivery.ID] = delivery
	repository.results = append([]store.StoredResult(nil), repository.work.Results...)
	replicator := &fakeResultReplicator{calls: make(chan resultReplicationCall, 1)}
	gate := admission.NewGate()
	if err := gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	options := testEngineOptions(repository, gate, &fakeSender{})
	options.Replicator = replicator
	engine, err := NewEngine(options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	call := <-replicator.calls
	repository.mu.Lock()
	closed := fixture.assignment
	closed.SchedulingState = model.Closed
	repository.assignments[closed.Assignment.JobID] = closed
	repository.mu.Unlock()
	replicator.ack(call)
	for index := 0; index < 100; index++ {
		if err := engine.ReconcileAssignment(ctx, closed.Assignment.JobID); err != nil {
			t.Fatalf("authority race killed owner: %v", err)
		}
	}
	repository.mu.Lock()
	state := repository.deliveries[delivery.ID].State
	repository.mu.Unlock()
	if state != store.Processed {
		t.Fatalf("late receipt completed stale sink: %v", state)
	}
	cancel()
	<-done
}

type resultReplicationCall struct {
	record     model.ResultRecord
	provenance model.ResultCopyProvenance
	response   chan resultReplicationResponse
}

type resultReplicationResponse struct {
	receipt ResultReplicationReceipt
	err     error
}

type fakeResultReplicator struct {
	mu            sync.Mutex
	calls         chan resultReplicationCall
	mutateReceipt func(*ResultReplicationReceipt)
}

func (replicator *fakeResultReplicator) ReplicateRecord(ctx context.Context, record model.ResultRecord, provenance model.ResultCopyProvenance) (ResultReplicationReceipt, error) {
	response := make(chan resultReplicationResponse, 1)
	call := resultReplicationCall{record: record, provenance: provenance, response: response}
	select {
	case replicator.calls <- call:
	case <-ctx.Done():
		return ResultReplicationReceipt{}, ctx.Err()
	}
	select {
	case result := <-response:
		return result.receipt, result.err
	case <-ctx.Done():
		return ResultReplicationReceipt{}, ctx.Err()
	}
}

func (replicator *fakeResultReplicator) ack(call resultReplicationCall) {
	encoded, _ := model.MarshalResultRecord(call.record)
	receipt := ResultReplicationReceipt{DestinationNodeID: call.provenance.ReplicaSet.SecondaryNodeID, DestinationWorkerEpoch: call.provenance.ReplicaSet.SecondaryEpoch, StreamChecksum: model.ResultRecordStreamChecksum(call.record), StreamLength: uint64(len(encoded)), CoordinatorEpoch: call.provenance.CoordinatorEpoch}
	if replicator.mutateReceipt != nil {
		replicator.mutateReceipt(&receipt)
	}
	call.response <- resultReplicationResponse{receipt: receipt}
}

var _ ResultReplicator = (*fakeResultReplicator)(nil)

func workerFixtureWithLocalPrimarySink(t *testing.T) (workerTestFixture, model.AssignmentToken, model.ResultReplicaSet) {
	t.Helper()
	fixture := workerFixture(t)
	for _, replica := range fixture.assignment.Assignment.ResultReplicas {
		for _, token := range fixture.assignment.Assignment.Tasks {
			if token.Task == replica.SinkTask && token.WorkerID == replica.PrimaryNodeID && token.WorkerEpoch == replica.PrimaryEpoch {
				fixture.localNode = replica.PrimaryNodeID
				fixture.localEpoch = replica.PrimaryEpoch
				return fixture, token, replica
			}
		}
	}
	t.Fatal("fixture has no exact local primary sink")
	return workerTestFixture{}, model.AssignmentToken{}, model.ResultReplicaSet{}
}

func (fixture workerTestFixture) sinkDelivery(t *testing.T, sink model.AssignmentToken, sequence uint64) store.DeliveryRecord {
	t.Helper()
	parent := fixture.delivery(t, sequence)
	outputs, err := model.ExecuteOperator(model.OperatorSpec{Name: "multiply", Version: 1, Settings: []model.Setting{{Key: "factor", Value: "2"}}}, parent.Tuple)
	if err != nil {
		t.Fatal(err)
	}
	outboxes, err := deriveOutboxes(fixture.assignment, parent, outputs)
	if err != nil {
		t.Fatal(err)
	}
	for _, outbox := range outboxes {
		if outbox.Destination == sink {
			reservation, err := fixture.topology.WorstCaseCustodyBytes(sink.Task)
			if err != nil {
				t.Fatal(err)
			}
			return store.DeliveryRecord{ID: outbox.ID, Tuple: outbox.Tuple, Producer: outbox.Producer, Destination: outbox.Destination, AssignmentRevision: outbox.AssignmentRevision, AssignmentDigest: outbox.AssignmentDigest, CoordinatorEpoch: outbox.CoordinatorEpoch, State: store.Received, Reservation: reservation}
		}
	}
	t.Fatal("fixture route did not target local sink")
	return store.DeliveryRecord{}
}

func (fixture workerTestFixture) result(t *testing.T, sink model.AssignmentToken, replica model.ResultReplicaSet, sequence uint64, role model.ResultReplicaRole) (model.ResultRecord, model.ResultCopyProvenance) {
	t.Helper()
	delivery := fixture.sinkDelivery(t, sink, sequence)
	encoded, err := model.MarshalTuple(delivery.Tuple)
	if err != nil {
		t.Fatal(err)
	}
	record, err := model.NewResultRecord(delivery.ID.Tuple, sink.Task, sink.SpecificationHash, encoded)
	if err != nil {
		t.Fatal(err)
	}
	return record, model.ResultCopyProvenance{AssignmentRevision: fixture.assignment.Assignment.Revision, AssignmentDigest: fixture.assignment.Assignment.Digest, ReplicaSet: replica, DestinationRole: role, CoordinatorEpoch: fixture.epoch}
}

func indexOf(values []string, value string) int {
	for index, candidate := range values {
		if candidate == value {
			return index
		}
	}
	return -1
}
