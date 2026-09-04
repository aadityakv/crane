package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"

	"crane/internal/crane/admission"
	"crane/internal/crane/model"
	"crane/internal/crane/protocol"
	"crane/internal/crane/store"
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

func TestResultParallelDirectSinkParentsCompleteLiveAndAfterRestart(t *testing.T) {
	for _, test := range []struct {
		name      string
		recovered bool
	}{
		{name: "live"},
		{name: "recovered durable result", recovered: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture, parents, sourceOutboxes, record, provenance := parallelDirectSinkResultFixture(t)
			repository := newFakeRepository(fixture)
			for _, parent := range parents {
				owned := parent.Clone()
				if test.recovered {
					owned.State = store.Processed
					owned.Outputs = []model.Tuple{cloneTuple(owned.Tuple)}
				}
				repository.work.Deliveries = append(repository.work.Deliveries, owned)
				repository.deliveries[owned.ID] = owned
			}
			if test.recovered {
				repository.work.Deliveries[0], repository.work.Deliveries[1] = repository.work.Deliveries[1], repository.work.Deliveries[0]
				repository.work.Results = []store.StoredResult{{Record: record, Provenance: provenance}}
				repository.results = append([]store.StoredResult(nil), repository.work.Results...)
			}
			for _, outbox := range sourceOutboxes {
				outbox.Accepted = true
				outbox.RetryDeadlineUnixNano = int64(^uint64(0) >> 1)
				repository.work.Outboxes = append(repository.work.Outboxes, outbox)
				repository.outboxes[outbox.ID] = outbox
			}
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
			waitFor(t, func() bool {
				repository.mu.Lock()
				defer repository.mu.Unlock()
				return repository.deliveries[parents[0].ID].State == store.Processed && repository.deliveries[parents[1].ID].State == store.Processed
			})
			replicator.ack(call)
			waitFor(t, func() bool {
				repository.mu.Lock()
				defer repository.mu.Unlock()
				return repository.deliveries[parents[0].ID].State == store.Completed && repository.deliveries[parents[1].ID].State == store.Completed
			})
			for _, outbox := range sourceOutboxes {
				ack := protocol.TupleACK{DeliveryID: outbox.ID, Destination: outbox.Destination, Assignment: protocol.AssignmentSetIdentity{JobID: fixture.assignment.Assignment.JobID, Revision: fixture.assignment.Assignment.Revision, Digest: fixture.assignment.Assignment.Digest}, Coordinator: fixture.epoch, Status: protocol.TupleCompleted}
				if err := engine.HandleACK(ctx, ack); err != nil {
					t.Fatal(err)
				}
			}
			waitFor(t, func() bool {
				repository.mu.Lock()
				defer repository.mu.Unlock()
				return repository.persistEventCalls == 1 && repository.work.PendingEvents[0].Completion.New == 1
			})
			cancel()
			<-done
		})
	}
}

func TestResultParallelDirectSinkParentsRejectConflictingBytes(t *testing.T) {
	fixture, parents, _, record, provenance := parallelDirectSinkResultFixture(t)
	for index := range parents {
		parents[index].State = store.Processed
		parents[index].Outputs = []model.Tuple{cloneTuple(parents[index].Tuple)}
	}
	parents[1].Outputs[0].Fields[0].Value.Int64++
	repository := newFakeRepository(fixture)
	repository.work.Deliveries = append([]store.DeliveryRecord(nil), parents...)
	for _, parent := range parents {
		repository.deliveries[parent.ID] = parent
	}
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
	defer cancel()
	done := runEngine(t, ctx, engine)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("conflicting parallel result stopped without error")
		}
	case <-replicator.calls:
		t.Fatal("conflicting parallel result entered replication")
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	for _, parent := range parents {
		if repository.deliveries[parent.ID].State == store.Completed {
			t.Fatalf("conflicting parent completed: %v", parent.ID)
		}
	}
}

func TestResultReplicatorCannotMutateOwnedResultBytes(t *testing.T) {
	fixture, sink, _ := workerFixtureWithLocalPrimarySink(t)
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
	exact := call.record
	exact.Value = append([]byte(nil), call.record.Value...)
	call.record.Value[0]++
	replicator.ackRecord(call, exact)
	completed, finished := false, false
	var runErr error
	for index := 0; index < 2_000_000 && !completed && !finished; index++ {
		repository.mu.Lock()
		completed = repository.deliveries[delivery.ID].State == store.Completed
		repository.mu.Unlock()
		select {
		case runErr = <-done:
			finished = true
		default:
		}
	}
	if !completed {
		t.Fatalf("replicator mutation prevented exact completion: runErr=%v", runErr)
	}
	call.record.Value[0]++
	repository.mu.Lock()
	stored := repository.results[0].Record
	repository.mu.Unlock()
	if !bytes.Equal(stored.Value, exact.Value) || stored.Checksum != exact.Checksum {
		t.Fatalf("replicator mutated durable result: got=%x want=%x", stored.Value, exact.Value)
	}
	cancel()
	if !finished {
		<-done
	}
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

// TestResultRecoveryReReplicatesHistoricalProvenanceAsCurrentMember pins the
// Task 24 defect #4 ruling over the former fail-closed rule: a copy retained
// under a superseded assignment revision by a worker that is STILL a current
// replica member re-enters ordinary replication under the current envelope
// (and re-binds on receipt); a worker that is no longer a member never does
// (TestRetainedResultNeverMovesWhenNotCurrentMember).
func TestResultRecoveryReReplicatesHistoricalProvenanceAsCurrentMember(t *testing.T) {
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
	call := <-replicator.calls
	want := model.ResultCopyProvenance{AssignmentRevision: replacement.Revision, AssignmentDigest: replacement.Digest, ReplicaSet: replica, DestinationRole: model.SecondaryReplica, CoordinatorEpoch: fixture.epoch}
	if call.provenance != want || call.record.Checksum != record.Checksum {
		t.Fatalf("re-replication provenance=%+v want=%+v", call.provenance, want)
	}
	replicator.ack(call)
	want.DestinationRole = model.PrimaryReplica
	waitFor(t, func() bool {
		repository.mu.Lock()
		defer repository.mu.Unlock()
		return len(repository.results) == 1 && repository.results[0].Provenance == want
	})
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
	replicator.ackRecord(call, call.record)
}

func (replicator *fakeResultReplicator) ackRecord(call resultReplicationCall, record model.ResultRecord) {
	encoded, _ := model.MarshalResultRecord(record)
	receipt := ResultReplicationReceipt{DestinationNodeID: call.provenance.ReplicaSet.SecondaryNodeID, DestinationWorkerEpoch: call.provenance.ReplicaSet.SecondaryEpoch, StreamChecksum: model.ResultRecordStreamChecksum(record), StreamLength: uint64(len(encoded)), CoordinatorEpoch: call.provenance.CoordinatorEpoch}
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

func parallelDirectSinkResultFixture(t *testing.T) (workerTestFixture, []store.DeliveryRecord, []store.OutboxRecord, model.ResultRecord, model.ResultCopyProvenance) {
	t.Helper()
	base := workerFixture(t)
	spec := base.topology.Spec()
	spec.Edges = append(spec.Edges,
		model.EdgeSpec{EdgeID: 3, SourceStageID: 1, DestinationStageID: 3, Routing: model.RoutingFieldHash, Field: "value"},
		model.EdgeSpec{EdgeID: 4, SourceStageID: 1, DestinationStageID: 3, Routing: model.RoutingFieldHash, Field: "value"},
	)
	topology, err := model.ValidateTopology(spec)
	if err != nil {
		t.Fatal(err)
	}
	localEpoch := model.WorkerEpoch{1}
	workers := []model.WorkerPlacement{{NodeID: 1, WorkerEpoch: localEpoch, SlotCapacity: 8}, {NodeID: 2, WorkerEpoch: model.WorkerEpoch{2}, SlotCapacity: 8}}
	var fixture workerTestFixture
	var sink model.AssignmentToken
	var replica model.ResultReplicaSet
	for candidate := uint16(1); candidate < 2048; candidate++ {
		job := model.JobID{byte(candidate), byte(candidate >> 8)}
		assignment, buildErr := model.BuildAssignmentSet(job, topology.Digest(), 1, topology, workers)
		if buildErr != nil {
			t.Fatal(buildErr)
		}
		var source, transform model.AssignmentToken
		for _, token := range assignment.Tasks {
			switch token.Task.StageID {
			case 1:
				source = token
			case 2:
				transform = token
			case 3:
				sink = token
			}
		}
		for _, candidateReplica := range assignment.ResultReplicas {
			if candidateReplica.SinkTask == sink.Task {
				replica = candidateReplica
			}
		}
		if source.WorkerID != 1 || sink.WorkerID != 1 || replica.PrimaryNodeID != 1 || source.WorkerEpoch != localEpoch || sink.WorkerEpoch != localEpoch || replica.PrimaryEpoch != localEpoch {
			continue
		}
		epoch := model.CoordinatorEpoch{Term: 4, BeginIndex: 9, Coordinator: 2, Nonce: [16]byte{7}}
		fixture = workerTestFixture{topology: topology, assignment: store.InstalledAssignment{Assignment: assignment, SpecificationBytes: topology.CanonicalBytes(), Topology: topology, JobControlRevision: 1, SchedulingState: model.Running, CoordinatorEpoch: epoch}, epoch: epoch, source: source, transform: transform, localNode: 1, localEpoch: localEpoch}
		break
	}
	if fixture.assignment.Assignment.JobID == (model.JobID{}) {
		t.Fatal("could not place source and sink primary on the local worker")
	}
	assignment := fixture.assignment.Assignment
	outboxes, err := deriveSourceOutboxes(fixture.assignment, fixture.source, 1, func() model.Tuple {
		tuple, exists, tupleErr := model.SourceTuple(topology, fixture.source.Task, 1)
		if tupleErr != nil || !exists {
			t.Fatalf("source tuple: exists=%v err=%v", exists, tupleErr)
		}
		return tuple
	}())
	if err != nil {
		t.Fatal(err)
	}
	parents := make([]store.DeliveryRecord, 0, 2)
	for _, outbox := range outboxes {
		if outbox.Destination.Task != sink.Task || outbox.ID.EdgeID < 3 {
			continue
		}
		reservation, reserveErr := topology.WorstCaseCustodyBytes(sink.Task)
		if reserveErr != nil {
			t.Fatal(reserveErr)
		}
		parents = append(parents, store.DeliveryRecord{ID: outbox.ID, Tuple: cloneTuple(outbox.Tuple), Producer: outbox.Producer, Destination: outbox.Destination, AssignmentRevision: outbox.AssignmentRevision, AssignmentDigest: outbox.AssignmentDigest, CoordinatorEpoch: outbox.CoordinatorEpoch, State: store.Received, Reservation: reservation})
	}
	if len(parents) != 2 || parents[0].ID.Tuple != parents[1].ID.Tuple || parents[0].Destination.Task != parents[1].Destination.Task {
		t.Fatalf("parallel parents=%+v", parents)
	}
	encoded, err := model.MarshalTuple(parents[0].Tuple)
	if err != nil {
		t.Fatal(err)
	}
	record, err := model.NewResultRecord(parents[0].ID.Tuple, sink.Task, sink.SpecificationHash, encoded)
	if err != nil {
		t.Fatal(err)
	}
	provenance := model.ResultCopyProvenance{AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest, ReplicaSet: replica, DestinationRole: model.PrimaryReplica, CoordinatorEpoch: fixture.epoch}
	return fixture, parents, outboxes, record, provenance
}

func indexOf(values []string, value string) int {
	for index, candidate := range values {
		if candidate == value {
			return index
		}
	}
	return -1
}

func TestSealResultPartitionBuildsCanonicalSortedArtifact(t *testing.T) {
	fixture := newTransferFixture(t)
	reversed := []model.ResultRecord{fixture.records[1], fixture.records[0]}
	artifact, stream, err := SealResultPartition(fixture.assignment.Assignment.JobID, fixture.replica.SinkTask, fixture.assignment.Topology.Digest(), reversed)
	if err != nil {
		t.Fatal(err)
	}
	if artifact.JobID != fixture.assignment.Assignment.JobID || artifact.SinkTask != fixture.replica.SinkTask ||
		artifact.SpecificationHash != fixture.assignment.Topology.Digest() || artifact.RecordCount != 2 {
		t.Fatalf("artifact identity=%+v", artifact)
	}
	want := make([]byte, 0)
	for _, record := range fixture.records {
		entry, err := protocol.EncodedResultPageRecordBytes(record)
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, entry...)
	}
	if !bytes.Equal(stream, want) {
		t.Fatalf("stream=%x want=%x", stream, want)
	}
	if artifact.TotalLength != uint64(len(want)) || artifact.Checksum != sha256.Sum256(want) {
		t.Fatalf("artifact length/checksum=%d/%x want=%d/%x", artifact.TotalLength, artifact.Checksum, len(want), sha256.Sum256(want))
	}
	emptyIdentity, _, err := SealResultPartition(fixture.assignment.Assignment.JobID, fixture.replica.SinkTask, fixture.assignment.Topology.Digest(), nil)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := protocol.MarshalWorkerMessage(protocol.ResultArtifactChunk{Transfer: protocol.TransferChunk{TransferID: [16]byte{1}, JobID: artifact.JobID, TotalLength: artifact.TotalLength, Checksum: artifact.Checksum, Data: append([]byte(nil), stream...), Final: true}, Artifact: artifact, DestinationNodeID: fixture.replica.SecondaryNodeID, DestinationWorkerEpoch: fixture.replica.SecondaryEpoch, CoordinatorEpoch: fixture.epoch})
	if err != nil {
		t.Fatalf("sealed artifact violates the protocol schema: %v", err)
	}
	if len(wire) == 0 {
		t.Fatal("empty artifact encoding")
	}
	emptyWire, err := protocol.MarshalWorkerMessage(protocol.ResultArtifactChunk{Transfer: protocol.TransferChunk{TransferID: [16]byte{2}, JobID: emptyIdentity.JobID, Checksum: emptyIdentity.Checksum, Final: true}, Artifact: emptyIdentity, DestinationNodeID: fixture.replica.SecondaryNodeID, DestinationWorkerEpoch: fixture.replica.SecondaryEpoch, CoordinatorEpoch: fixture.epoch})
	if err != nil {
		t.Fatalf("empty sealed artifact violates the protocol schema: %v", err)
	}
	if len(emptyWire) == 0 {
		t.Fatal("empty artifact encoding")
	}
}

func TestSealResultPartitionRejectsDuplicatesForeignSinksAndBuildsEmptyArtifact(t *testing.T) {
	fixture := newTransferFixture(t)
	duplicate := fixture.records[0]
	if _, _, err := SealResultPartition(fixture.assignment.Assignment.JobID, fixture.replica.SinkTask, fixture.assignment.Topology.Digest(), []model.ResultRecord{fixture.records[0], duplicate}); err == nil {
		t.Fatal("duplicate tuple sealed")
	}
	foreign := fixture.records[0]
	foreign.SinkTask.Partition++
	if _, _, err := SealResultPartition(fixture.assignment.Assignment.JobID, fixture.replica.SinkTask, fixture.assignment.Topology.Digest(), []model.ResultRecord{foreign}); err == nil {
		t.Fatal("foreign sink sealed")
	}
	empty, stream, err := SealResultPartition(fixture.assignment.Assignment.JobID, fixture.replica.SinkTask, fixture.assignment.Topology.Digest(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if empty.RecordCount != 0 || empty.TotalLength != 0 || len(stream) != 0 || empty.Checksum != sha256.Sum256(nil) {
		t.Fatalf("empty artifact=%+v stream=%x", empty, stream)
	}
}

func TestArtifactStoreSealIsDurableIdempotentAndRejectsMismatchedBytes(t *testing.T) {
	fixture := newTransferFixture(t)
	artifact, stream, err := SealResultPartition(fixture.assignment.Assignment.JobID, fixture.replica.SinkTask, fixture.assignment.Topology.Digest(), fixture.records)
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := NewArtifactStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := artifacts.Sealed(artifact)
	if err != nil || sealed {
		t.Fatalf("unknown artifact reported sealed=%t err=%v", sealed, err)
	}
	if err := artifacts.Seal(artifact, stream); err != nil {
		t.Fatalf("seal: %v", err)
	}
	if sealed, err = artifacts.Sealed(artifact); err != nil || !sealed {
		t.Fatalf("sealed artifact not durable=%t err=%v", sealed, err)
	}
	if err := artifacts.Seal(artifact, stream); err != nil {
		t.Fatalf("idempotent re-seal: %v", err)
	}
	changed := append([]byte(nil), stream...)
	changed[0] ^= 0xff
	if err := artifacts.Seal(artifact, changed); err == nil {
		t.Fatal("mismatched re-seal accepted")
	}
	if sealed, err = artifacts.Sealed(artifact); err != nil || !sealed {
		t.Fatalf("sealed artifact lost=%t err=%v", sealed, err)
	}
	if data, _, readErr := artifacts.Read(artifact, 0, artifact.TotalLength); readErr != nil || !bytes.Equal(data, stream) {
		t.Fatalf("sealed bytes changed=%x err=%v", data, readErr)
	}
	data, more, err := artifacts.Read(artifact, 0, uint64(len(stream)))
	if err != nil || more || !bytes.Equal(data, stream) {
		t.Fatalf("read=%x more=%t err=%v", data, more, err)
	}
	partial, more, err := artifacts.Read(artifact, 0, 3)
	if err != nil || !more || len(partial) != 3 || !bytes.Equal(partial, stream[:3]) {
		t.Fatalf("partial read=%x more=%t err=%v", partial, more, err)
	}
	unknown := artifact
	unknown.Checksum[0] ^= 1
	if _, _, err := artifacts.Read(unknown, 0, 4); err == nil {
		t.Fatal("unknown artifact read")
	}
	sealed, err = artifacts.Sealed(unknown)
	if err != nil || sealed {
		t.Fatalf("unknown sealed=%t err=%v", sealed, err)
	}
}

func TestArtifactStoreInstallResumesAcrossRestartAndVerifiesFinalChecksum(t *testing.T) {
	fixture := newTransferFixture(t)
	artifact, stream, err := SealResultPartition(fixture.assignment.Assignment.JobID, fixture.replica.SinkTask, fixture.assignment.Topology.Digest(), fixture.records)
	if err != nil {
		t.Fatal(err)
	}
	split := len(stream) / 2
	first := protocol.TransferChunk{TransferID: [16]byte{1}, JobID: artifact.JobID, TotalLength: artifact.TotalLength, Checksum: artifact.Checksum, Offset: 0, Data: append([]byte(nil), stream[:split]...)}
	second := protocol.TransferChunk{TransferID: [16]byte{2}, JobID: artifact.JobID, TotalLength: artifact.TotalLength, Checksum: artifact.Checksum, Offset: uint64(split), Data: append([]byte(nil), stream[split:]...), Final: true}

	dir := t.TempDir()
	artifacts, err := NewArtifactStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	next, complete, err := artifacts.Install(artifact, first)
	if err != nil || complete || next != uint64(split) {
		t.Fatalf("first install next=%d complete=%t err=%v", next, complete, err)
	}
	if nextAgain, _, err := artifacts.Install(artifact, first); err != nil || nextAgain != uint64(split) {
		t.Fatalf("duplicate install next=%d err=%v", nextAgain, err)
	}
	changed := protocol.TransferChunk{TransferID: first.TransferID, JobID: first.JobID, TotalLength: first.TotalLength, Checksum: first.Checksum, Offset: first.Offset, Data: append([]byte(nil), first.Data...)}
	changed.Data[0] ^= 0xff
	if _, _, err := artifacts.Install(artifact, changed); err == nil {
		t.Fatal("changed overlap accepted")
	}
	gap := protocol.TransferChunk{TransferID: [16]byte{3}, JobID: artifact.JobID, TotalLength: artifact.TotalLength, Checksum: artifact.Checksum, Offset: uint64(split + 7), Data: []byte{1}}
	if _, _, err := artifacts.Install(artifact, gap); err == nil {
		t.Fatal("gap accepted")
	}

	restarted, err := NewArtifactStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	next, complete, err = restarted.Install(artifact, second)
	if err != nil || !complete || next != artifact.TotalLength {
		t.Fatalf("final install next=%d complete=%t err=%v", next, complete, err)
	}
	if sealed, err := restarted.Sealed(artifact); err != nil || !sealed {
		t.Fatalf("completed install not sealed=%t err=%v", sealed, err)
	}
	data, _, err := restarted.Read(artifact, 0, artifact.TotalLength)
	if err != nil || !bytes.Equal(data, stream) {
		t.Fatalf("installed bytes=%x err=%v", data, err)
	}
	if nextAgain, completeAgain, err := restarted.Install(artifact, second); err != nil || !completeAgain || nextAgain != artifact.TotalLength {
		t.Fatalf("duplicate final install next=%d complete=%t err=%v", nextAgain, completeAgain, err)
	}

	corrupt := artifact
	corrupt.Checksum[0] ^= 1
	fresh, err := NewArtifactStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	badFirst := protocol.TransferChunk{TransferID: [16]byte{4}, JobID: corrupt.JobID, TotalLength: corrupt.TotalLength, Checksum: corrupt.Checksum, Offset: 0, Data: append([]byte(nil), stream[:split]...)}
	badSecond := protocol.TransferChunk{TransferID: [16]byte{5}, JobID: corrupt.JobID, TotalLength: corrupt.TotalLength, Checksum: corrupt.Checksum, Offset: uint64(split), Data: append([]byte(nil), stream[split:]...), Final: true}
	if _, _, err := fresh.Install(corrupt, badFirst); err != nil {
		t.Fatalf("corrupt artifact prefix: %v", err)
	}
	if _, _, err := fresh.Install(corrupt, badSecond); err == nil {
		t.Fatal("checksum mismatch completed")
	}
	if sealed, err := fresh.Sealed(corrupt); err != nil || sealed {
		t.Fatalf("corrupt artifact sealed=%t err=%v", sealed, err)
	}

	empty, emptyStream, err := SealResultPartition(artifact.JobID, fixture.replica.SinkTask, artifact.SpecificationHash, nil)
	if err != nil {
		t.Fatal(err)
	}
	emptyChunk := protocol.TransferChunk{TransferID: [16]byte{9}, JobID: empty.JobID, TotalLength: 0, Checksum: empty.Checksum, Offset: 0, Final: true}
	next, complete, err = fresh.Install(empty, emptyChunk)
	if err != nil || !complete || next != 0 || len(emptyStream) != 0 {
		t.Fatalf("empty install next=%d complete=%t err=%v", next, complete, err)
	}
}

// TestFinalCheckpointVectorAcceptsHistoricalCheckpointProof pins the defect
// #2 ruling seal-path half for terminal result sealing: a durable retained
// checkpoint proof under a historical coordinator epoch, JobControlRevision,
// and assignment binding still proves its committed watermark, so
// FinalCheckpointVector must accept it instead of demanding current-authority
// equality and blocking sealing forever after any authority advance.
func TestFinalCheckpointVectorAcceptsHistoricalCheckpointProof(t *testing.T) {
	fixture := workerFixture(t)
	eof, err := model.SourceEOF(fixture.topology, fixture.source.Task)
	if err != nil {
		t.Fatal(err)
	}
	if eof == 0 {
		t.Fatal("fixture requires a nonzero source EOF")
	}
	olderEpoch := fixture.epoch
	olderEpoch.Term--
	olderEpoch.BeginIndex--
	olderEpoch.Nonce[0]++
	// Any retained proof whose authority differs from the current
	// installation is historical once the coordinator advances.
	historical := store.CheckpointAuthority{JobControlRevision: fixture.assignment.JobControlRevision + 7, AssignmentRevision: fixture.assignment.Assignment.Revision + 3, AssignmentDigest: [32]byte{9}, SourceToken: fixture.source, CoordinatorEpoch: olderEpoch}
	work := store.RecoveredWork{Fence: fixture.epoch, Assignments: []store.InstalledAssignment{fixture.assignment}, Sources: []store.SourceCursor{{
		Source: fixture.source.Task, NextSequence: eof + 1, EOF: eof,
		Watermark: eof, RaftIndex: 21, CheckpointRevision: 4, CheckpointAuthority: historical,
	}}}
	vector, ok := FinalCheckpointVector(work, fixture.assignment)
	if !ok {
		t.Fatal("final checkpoint vector refused a historical durable proof")
	}
	if len(vector) != 1 || vector[0].Source != fixture.source.Task || vector[0].Watermark != eof {
		t.Fatalf("final vector = %+v", vector)
	}
}
