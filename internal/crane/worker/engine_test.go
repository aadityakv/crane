package worker

import (
	"context"
	"crypto/sha256"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/crane/admission"
	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
	"github.com/aaditya/cs425mp3/internal/crane/store"
)

func TestEngineRequiresAndPreservesCallerGate(t *testing.T) {
	repository := newFakeRepository(workerFixture(t))
	if _, err := NewEngine(EngineOptions{Repository: repository, Sender: &fakeSender{}, Replicator: automaticResultReplicator{}, Clock: clock.NewManual(time.Unix(0, 0))}); err == nil {
		t.Fatal("NewEngine accepted a nil shared gate")
	}
	gate := admission.NewGate()
	if _, err := NewEngine(EngineOptions{Repository: repository, Sender: &fakeSender{}, Gate: gate, Clock: clock.NewManual(time.Unix(0, 0))}); err == nil {
		t.Fatal("NewEngine accepted a nil result replicator")
	}
	minimal, err := NewEngine(EngineOptions{Repository: repository, Sender: &fakeSender{}, Replicator: automaticResultReplicator{}, Gate: gate, Clock: clock.NewManual(time.Unix(0, 0))})
	if err != nil || minimal.gate != gate {
		t.Fatalf("NewEngine defaults = %v, gate preserved=%t", err, minimal != nil && minimal.gate == gate)
	}
	engine, err := NewEngine(testEngineOptions(repository, gate, &fakeSender{}))
	if err != nil {
		t.Fatal(err)
	}
	if engine.gate != gate {
		t.Fatal("NewEngine replaced caller-owned gate")
	}
	equalRetries := testEngineOptions(repository, gate, &fakeSender{})
	equalRetries.CompletedRetryInterval = equalRetries.AcceptedRetryInterval
	if _, err = NewEngine(equalRetries); err == nil {
		t.Fatal("NewEngine accepted identical custody and completion retry intervals")
	}
}

func TestEngineReadyClosesOnlyAfterSingleOwnedRecoveryAndCancellationJoins(t *testing.T) {
	fixture := workerFixture(t)
	repository := newFakeRepository(fixture)
	repository.recoverStarted = make(chan struct{})
	repository.recoverRelease = make(chan struct{})
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
	<-repository.recoverStarted
	select {
	case <-engine.Ready():
		t.Fatal("Ready closed before RecoverWork returned")
	default:
	}
	close(repository.recoverRelease)
	<-engine.Ready()
	if repository.recoverCalls != 1 {
		t.Fatalf("RecoverWork calls = %d, want 1", repository.recoverCalls)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run cancellation = %v", err)
	}
}

func TestEngineRejectsInvalidDurableLocalIdentityBeforeReady(t *testing.T) {
	fixture := workerFixture(t)
	for _, test := range []struct {
		name  string
		node  uint16
		epoch model.WorkerEpoch
	}{
		{name: "zero node", epoch: fixture.localEpoch},
		{name: "invalid epoch", node: fixture.localNode},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository := newFakeRepository(fixture)
			repository.localNode, repository.localEpoch = test.node, test.epoch
			gate := admission.NewGate()
			if err := gate.Open(fixture.epoch); err != nil {
				t.Fatal(err)
			}
			engine, err := NewEngine(testEngineOptions(repository, gate, &fakeSender{}))
			if err != nil {
				t.Fatal(err)
			}
			if err = engine.Run(context.Background()); err == nil {
				t.Fatal("Run accepted an invalid durable local identity")
			}
			select {
			case <-engine.Ready():
				t.Fatal("Ready closed after invalid durable local identity")
			default:
			}
		})
	}
}

func TestEngineRecoveredPendingEventRetainsTransactionIdentity(t *testing.T) {
	fixture := workerFixture(t)
	event := fixture.failureEvent(7)
	repository := newFakeRepository(fixture)
	repository.work.PendingEvents = []model.WorkerEvent{event}
	repository.work.NextTransactionID = 8
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
	waitFor(t, func() bool { return len(engine.Events()) > 0 })
	got := <-engine.Events()
	if got.TransactionID != 7 || got.Failure == nil || got.Failure.TransactionID != 7 {
		t.Fatalf("recovered event identity changed: %+v", got)
	}
	if repository.persistEventCalls != 0 {
		t.Fatal("recovery re-persisted an existing durable event")
	}
	cancel()
	<-done
}

func TestEngineRecoveryDeepOwnsFailureEventBody(t *testing.T) {
	fixture := workerFixture(t)
	repository := newFakeRepository(fixture)
	event := fixture.failureEvent(1)
	work := repository.work.Clone()
	work.PendingEvents = []model.WorkerEvent{event}
	work.NextTransactionID = 2
	engine, err := NewEngine(testEngineOptions(repository, admission.NewGate(), &fakeSender{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.consumeRecovery(work); err != nil {
		t.Fatal(err)
	}
	want := *event.Failure
	work.PendingEvents[0].Failure.Code++
	work.PendingEvents[0].Failure.DetailDigest[0]++
	got := engine.eventQueue[0].Failure
	if got == nil || *got != want {
		t.Fatalf("recovery caller mutated owned failure event: got=%+v want=%+v", got, want)
	}
}

func TestEngineReconcileAssignmentObservesPostReadyDurableInstall(t *testing.T) {
	fixture := workerFixture(t)
	repository := newFakeRepository(fixture)
	repository.work.Assignments = nil
	repository.work.Sources = nil
	repository.assignments = make(map[model.JobID]store.InstalledAssignment)
	sender := &fakeSender{}
	gate := admission.NewGate()
	if err := gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(testEngineOptions(repository, gate, sender))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	runtimeYield()
	if sender.count() != 0 {
		t.Fatal("source emitted before durable post-Ready install")
	}
	repository.mu.Lock()
	repository.assignments[fixture.assignment.Assignment.JobID] = fixture.assignment
	repository.mu.Unlock()
	if err = engine.ReconcileAssignment(ctx, fixture.assignment.Assignment.JobID); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 1_000_000 && sender.count() == 0; index++ {
		runtime.Gosched()
	}
	if sender.count() == 0 {
		repository.mu.Lock()
		log, sources, outboxes := append([]string(nil), repository.log...), append([]store.SourceCursor(nil), repository.work.Sources...), len(repository.outboxes)
		repository.mu.Unlock()
		t.Fatalf("post-Ready source was not sent: log=%v sources=%+v outboxes=%d jobs=%d", log, sources, outboxes, len(engine.jobs))
	}
	cancel()
	<-done
}

func TestEngineReconcileAssignmentRejectsClosedOrStalePostReadyInstall(t *testing.T) {
	fixture := workerFixture(t)
	for _, test := range []struct {
		name      string
		installed store.InstalledAssignment
		fence     model.CoordinatorEpoch
	}{
		{name: "closed", installed: func() store.InstalledAssignment {
			value := fixture.assignment
			value.SchedulingState = model.Closed
			return value
		}(), fence: fixture.epoch},
		{name: "stale", installed: fixture.assignment, fence: func() model.CoordinatorEpoch {
			value := fixture.epoch
			value.Term++
			value.BeginIndex++
			value.Nonce[0]++
			return value
		}()},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository := newFakeRepository(fixture)
			repository.work.Assignments = nil
			repository.work.Sources = nil
			repository.work.Fence = test.fence
			repository.assignments = map[model.JobID]store.InstalledAssignment{fixture.assignment.Assignment.JobID: test.installed}
			sender := &fakeSender{}
			gate := admission.NewGate()
			if err := gate.Open(test.fence); err != nil {
				t.Fatal(err)
			}
			engine, err := NewEngine(testEngineOptions(repository, gate, sender))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := runEngine(t, ctx, engine)
			<-engine.Ready()
			if err = engine.ReconcileAssignment(ctx, fixture.assignment.Assignment.JobID); err != nil {
				t.Fatal(err)
			}
			runtimeYield()
			if sender.count() != 0 {
				t.Fatal("closed/stale install emitted source")
			}
			cancel()
			<-done
		})
	}
}

func TestEngineCancellationJoinsBoundedExecutors(t *testing.T) {
	fixture := workerFixture(t)
	repository := newFakeRepository(fixture)
	for sequence := uint64(1); sequence <= 8; sequence++ {
		delivery := fixture.delivery(t, sequence)
		repository.work.Deliveries = append(repository.work.Deliveries, delivery)
		repository.deliveries[delivery.ID] = delivery
	}
	gate := admission.NewGate()
	if err := gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	active, maximum := 0, 0
	options := testEngineOptions(repository, gate, &fakeSender{})
	options.MaxExecutors = 2
	options.Execute = func(ctx context.Context, _ model.OperatorSpec, _ model.Tuple) ([]model.Tuple, error) {
		mu.Lock()
		active++
		if active > maximum {
			maximum = active
		}
		mu.Unlock()
		<-ctx.Done()
		mu.Lock()
		active--
		mu.Unlock()
		return nil, ctx.Err()
	}
	engine, err := NewEngine(options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return active == 2
	})
	cancel()
	<-done
	mu.Lock()
	defer mu.Unlock()
	if active != 0 || maximum != 2 {
		t.Fatalf("executor active/max after cancellation = %d/%d", active, maximum)
	}
}

func TestEngineCloseAndWaitDrainsExecutionThroughDurableProcessed(t *testing.T) {
	fixture := workerFixture(t)
	repository := newFakeRepository(fixture)
	record := fixture.delivery(t, 13)
	repository.work.Deliveries = []store.DeliveryRecord{record}
	repository.deliveries[record.ID] = record
	repository.processedStarted = make(chan struct{})
	repository.processedRelease = make(chan struct{})
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
	<-repository.processedStarted
	closed := make(chan error, 1)
	go func() { closed <- gate.CloseAndWait(context.Background()) }()
	for {
		release, enterErr := gate.Enter()
		if errors.Is(enterErr, admission.ErrClosed) {
			break
		}
		if enterErr != nil {
			t.Fatal(enterErr)
		}
		release()
		runtime.Gosched()
	}
	for index := 0; index < 10_000; index++ {
		select {
		case err = <-closed:
			t.Fatalf("CloseAndWait returned before MarkProcessed committed: %v", err)
		default:
			runtime.Gosched()
		}
	}
	close(repository.processedRelease)
	if err = <-closed; err != nil {
		t.Fatal(err)
	}
	cancel()
	<-done
}

func TestEngineFatalExitReleasesQueuedRunningAndBufferedExecutionPermits(t *testing.T) {
	fixture := workerFixture(t)
	for _, test := range []struct {
		name      string
		configure func(*testing.T, *fakeRepository, *EngineOptions, **Engine)
	}{
		{name: "queued and running", configure: func(t *testing.T, repository *fakeRepository, options *EngineOptions, _ **Engine) {
			started := make(chan struct{})
			options.MaxExecutors = 1
			options.Execute = func(ctx context.Context, _ model.OperatorSpec, _ model.Tuple) ([]model.Tuple, error) {
				select {
				case <-started:
				default:
					close(started)
				}
				<-ctx.Done()
				return nil, ctx.Err()
			}
			repository.advanceSourceBefore = func() { <-started }
			repository.advanceSourceErr = errors.New("injected fatal source persistence")
		}},
		{name: "buffered results", configure: func(t *testing.T, repository *fakeRepository, options *EngineOptions, engine **Engine) {
			var executed atomic.Int32
			bothExecuting := make(chan struct{})
			options.MaxExecutors = 2
			options.Execute = func(ctx context.Context, operator model.OperatorSpec, tuple model.Tuple) ([]model.Tuple, error) {
				if executed.Add(1) == 2 {
					close(bothExecuting)
				}
				select {
				case <-bothExecuting:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				return model.ExecuteOperator(operator, tuple)
			}
			repository.advanceSourceBefore = func() {
				for index := 0; index < 1_000_000; index++ {
					if *engine != nil && len((*engine).executorResults) >= 2 {
						return
					}
					runtime.Gosched()
				}
				jobs, results := -1, -1
				if *engine != nil {
					jobs, results = len((*engine).executorJobs), len((*engine).executorResults)
				}
				t.Errorf("executor results did not buffer before injected fatal exit: executed=%d jobs=%d results=%d", executed.Load(), jobs, results)
			}
			repository.advanceSourceErr = errors.New("injected fatal with buffered results")
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository := newFakeRepository(fixture)
			repository.work.Sources = nil
			for sequence := uint64(10); sequence < 13; sequence++ {
				record := fixture.delivery(t, sequence)
				repository.work.Deliveries = append(repository.work.Deliveries, record)
				repository.deliveries[record.ID] = record
			}
			gate := admission.NewGate()
			if err := gate.Open(fixture.epoch); err != nil {
				t.Fatal(err)
			}
			options := testEngineOptions(repository, gate, &fakeSender{})
			var engine *Engine
			test.configure(t, repository, &options, &engine)
			var err error
			engine, err = NewEngine(options)
			if err != nil {
				t.Fatal(err)
			}
			done := runEngine(t, context.Background(), engine)
			if err = <-done; err == nil {
				t.Fatal("injected fatal owner error was lost")
			}
			assertGateClosesWithoutTimers(t, gate)
			select {
			case _, ok := <-engine.Events():
				if ok {
					t.Fatal("Events published after terminal pre-Ready exit")
				}
			default:
				t.Fatal("Events remained open after terminal pre-Ready exit")
			}
		})
	}
}

func assertGateClosesWithoutTimers(t *testing.T, gate *admission.Gate) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	closed := make(chan error, 1)
	go func() { closed <- gate.CloseAndWait(ctx) }()
	for index := 0; index < 1_000_000; index++ {
		select {
		case err := <-closed:
			if err != nil {
				t.Fatal(err)
			}
			cancel()
			return
		default:
			runtime.Gosched()
		}
	}
	cancel()
	<-closed
	t.Fatal("CloseAndWait did not finish after engine terminal cleanup")
}

func testEngineOptions(repository Repository, gate *admission.Gate, sender Sender) EngineOptions {
	return EngineOptions{
		Repository:             repository,
		Sender:                 sender,
		Replicator:             automaticResultReplicator{},
		Gate:                   gate,
		Clock:                  clock.NewManual(time.Unix(100, 0)),
		MaxExecutors:           2,
		MaxPendingWork:         8,
		MaxPendingOutboxes:     8,
		AcceptedRetryInterval:  10 * time.Millisecond,
		CompletedRetryInterval: 50 * time.Millisecond,
	}
}

func runEngine(t *testing.T, ctx context.Context, engine *Engine) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- engine.Run(ctx) }()
	return done
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	for i := 0; i < 2_000_000; i++ {
		if condition() {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("condition was not reached")
}

type workerTestFixture struct {
	topology   model.ValidatedTopology
	assignment store.InstalledAssignment
	epoch      model.CoordinatorEpoch
	source     model.AssignmentToken
	transform  model.AssignmentToken
	localNode  uint16
	localEpoch model.WorkerEpoch
}

func workerFixture(t *testing.T) workerTestFixture {
	return workerFixtureWithRange(t, "1", "16")
}

func workerFixtureWithRange(t *testing.T, start, end string) workerTestFixture {
	t.Helper()
	spec := model.TopologySpec{SchemaVersion: 1, Name: "worker-test", RegistryFingerprint: model.RegistryFingerprint(), Stages: []model.StageSpec{
		{StageID: 1, Name: "source", Role: model.StageSource, Parallelism: 1, Operator: model.OperatorSpec{Name: "range", Version: 1, Settings: []model.Setting{{Key: "end_exclusive", Value: end}, {Key: "start", Value: start}}}},
		{StageID: 2, Name: "transform", Role: model.StageTransform, Parallelism: 1, Operator: model.OperatorSpec{Name: "multiply", Version: 1, Settings: []model.Setting{{Key: "factor", Value: "2"}}}},
		{StageID: 3, Name: "sink", Role: model.StageSink, Parallelism: 1, Operator: model.OperatorSpec{Name: "collect", Version: 1}},
	}, Edges: []model.EdgeSpec{
		{EdgeID: 1, SourceStageID: 1, DestinationStageID: 2, Routing: model.RoutingShuffle},
		{EdgeID: 2, SourceStageID: 2, DestinationStageID: 3, Routing: model.RoutingFieldHash, Field: "value"},
	}}
	topology, err := model.ValidateTopology(spec)
	if err != nil {
		t.Fatal(err)
	}
	localEpoch := model.WorkerEpoch{1}
	workers := []model.WorkerPlacement{{NodeID: 1, WorkerEpoch: localEpoch, SlotCapacity: 8}, {NodeID: 2, WorkerEpoch: model.WorkerEpoch{2}, SlotCapacity: 8}}
	for candidate := uint16(1); candidate < 256; candidate++ {
		job := model.JobID{byte(candidate), byte(candidate >> 8)}
		set, err := model.BuildAssignmentSet(job, topology.Digest(), 1, topology, workers)
		if err != nil {
			t.Fatal(err)
		}
		var source, transform model.AssignmentToken
		for _, token := range set.Tasks {
			if token.WorkerID == 1 && token.Task.StageID == 1 {
				source = token
			}
			if token.WorkerID == 1 && token.Task.StageID == 2 {
				transform = token
			}
		}
		if source != (model.AssignmentToken{}) && transform != (model.AssignmentToken{}) {
			epoch := model.CoordinatorEpoch{Term: 4, BeginIndex: 9, Coordinator: 2, Nonce: [16]byte{7}}
			return workerTestFixture{topology: topology, assignment: store.InstalledAssignment{Assignment: set, SpecificationBytes: topology.CanonicalBytes(), Topology: topology, JobControlRevision: 1, SchedulingState: model.Running, CoordinatorEpoch: epoch}, epoch: epoch, source: source, transform: transform, localNode: 1, localEpoch: localEpoch}
		}
	}
	t.Fatal("could not find local source/transform assignment")
	return workerTestFixture{}
}

func (fixture workerTestFixture) delivery(t *testing.T, sequence uint64) store.DeliveryRecord {
	t.Helper()
	tuple, exists, err := model.SourceTuple(fixture.topology, fixture.source.Task, sequence)
	if err != nil || !exists {
		t.Fatalf("SourceTuple = %+v,%v,%v", tuple, exists, err)
	}
	id := model.DeliveryID{Tuple: model.DeriveSourceTupleID(fixture.assignment.Assignment.JobID, fixture.source.Task, sequence), EdgeID: 1, DestinationTask: fixture.transform.Task}
	reservation, err := fixture.topology.WorstCaseCustodyBytes(fixture.transform.Task)
	if err != nil {
		t.Fatal(err)
	}
	return store.DeliveryRecord{ID: id, Tuple: tuple, Producer: fixture.source, Destination: fixture.transform, AssignmentRevision: fixture.assignment.Assignment.Revision, AssignmentDigest: fixture.assignment.Assignment.Digest, CoordinatorEpoch: fixture.epoch, State: store.Received, Reservation: reservation}
}

func (fixture workerTestFixture) message(t *testing.T, sequence uint64) protocol.TupleDelivery {
	record := fixture.delivery(t, sequence)
	return deliveryMessage(record)
}

func (fixture workerTestFixture) failureEvent(transaction uint64) model.WorkerEvent {
	report := &model.JobFailureReport{JobID: fixture.assignment.Assignment.JobID, JobControlRevision: fixture.assignment.JobControlRevision, AssignmentRevision: fixture.assignment.Assignment.Revision, Task: fixture.transform, Epoch: fixture.epoch, TransactionID: transaction, Code: model.FailureOperator, DetailDigest: sha256.Sum256([]byte("failure"))}
	return model.WorkerEvent{WorkerID: fixture.localNode, WorkerEpoch: fixture.localEpoch, TransactionID: transaction, Kind: model.WorkerEventFailure, Failure: report}
}

type fakeRepository struct {
	mu                    sync.Mutex
	work                  store.RecoveredWork
	assignments           map[model.JobID]store.InstalledAssignment
	deliveries            map[model.DeliveryID]store.DeliveryRecord
	outboxes              map[model.DeliveryID]store.OutboxRecord
	results               []store.StoredResult
	sources               map[model.TaskID]store.SourceCursor
	log                   []string
	recoverCalls          int
	recoverStarted        chan struct{}
	recoverRelease        chan struct{}
	processedStarted      chan struct{}
	processedRelease      chan struct{}
	receiveCalls          int
	probeCalls            int
	persistEventCalls     int
	applyCheckpointCalls  int
	localNode             uint16
	localEpoch            model.WorkerEpoch
	receiveStarted        chan struct{}
	receiveRelease        chan struct{}
	receiveErr            error
	processedErr          error
	advanceSourceErr      error
	advanceSourceBefore   func()
	outboxCompleteStarted chan struct{}
	outboxCompleteRelease chan struct{}
	eventAckStarted       chan struct{}
	eventAckRelease       chan struct{}
}

func newFakeRepository(fixture workerTestFixture) *fakeRepository {
	eof, _ := model.SourceEOF(fixture.topology, fixture.source.Task)
	work := store.RecoveredWork{Fence: fixture.epoch, Assignments: []store.InstalledAssignment{fixture.assignment}, Sources: []store.SourceCursor{{Source: fixture.source.Task, NextSequence: eof + 1, EOF: eof}}, NextTransactionID: 1}
	return &fakeRepository{work: work, assignments: map[model.JobID]store.InstalledAssignment{fixture.assignment.Assignment.JobID: fixture.assignment}, deliveries: make(map[model.DeliveryID]store.DeliveryRecord), outboxes: make(map[model.DeliveryID]store.OutboxRecord), results: nil, sources: map[model.TaskID]store.SourceCursor{fixture.source.Task: work.Sources[0]}, localNode: fixture.localNode, localEpoch: fixture.localEpoch}
}

func (repository *fakeRepository) LocalIdentity() (uint16, model.WorkerEpoch) {
	return repository.localNode, repository.localEpoch
}

func (repository *fakeRepository) RecoverWork() (store.RecoveredWork, error) {
	repository.mu.Lock()
	repository.recoverCalls++
	started, release := repository.recoverStarted, repository.recoverRelease
	work := repository.work.Clone()
	repository.mu.Unlock()
	if started != nil {
		select {
		case <-started:
		default:
			close(started)
		}
	}
	if release != nil {
		<-release
	}
	return work, nil
}
func (repository *fakeRepository) CurrentFence() model.CoordinatorEpoch {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	return repository.work.Fence
}
func (repository *fakeRepository) InstalledAssignment(job model.JobID) (store.InstalledAssignment, bool) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	value, ok := repository.assignments[job]
	return value, ok
}
func (repository *fakeRepository) Receive(record store.DeliveryRecord) (store.DeliveryState, error) {
	if repository.receiveStarted != nil {
		select {
		case <-repository.receiveStarted:
		default:
			close(repository.receiveStarted)
		}
	}
	if repository.receiveRelease != nil {
		<-repository.receiveRelease
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	repository.receiveCalls++
	repository.log = append(repository.log, "receive")
	if repository.receiveErr != nil {
		return 0, repository.receiveErr
	}
	if prior, ok := repository.deliveries[record.ID]; ok {
		return prior.State, nil
	}
	record.State = store.Received
	repository.deliveries[record.ID] = record
	return store.Received, nil
}
func (repository *fakeRepository) ProbeDelivery(record store.DeliveryRecord) (store.DeliveryState, bool, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	repository.probeCalls++
	prior, ok := repository.deliveries[record.ID]
	if !ok {
		return 0, false, nil
	}
	if !sameTestDeliveryDefinition(prior, record) {
		return 0, true, model.ErrIdentityReuse
	}
	return prior.State, true, nil
}
func (repository *fakeRepository) MarkProcessed(id model.DeliveryID, outputs []model.Tuple, outboxes []store.OutboxRecord) error {
	if repository.processedStarted != nil {
		select {
		case <-repository.processedStarted:
		default:
			close(repository.processedStarted)
		}
	}
	if repository.processedRelease != nil {
		<-repository.processedRelease
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if repository.processedErr != nil {
		return repository.processedErr
	}
	repository.log = append(repository.log, "processed")
	record := repository.deliveries[id]
	record.State = store.Processed
	record.Outputs = cloneTestTuples(outputs)
	record.OutboxIDs = nil
	for _, outbox := range outboxes {
		record.OutboxIDs = append(record.OutboxIDs, outbox.ID)
		repository.outboxes[outbox.ID] = outbox
	}
	repository.deliveries[id] = record
	return nil
}
func (repository *fakeRepository) MarkCompleted(id model.DeliveryID) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	repository.log = append(repository.log, "completed")
	record := repository.deliveries[id]
	record.State = store.Completed
	repository.deliveries[id] = record
	return nil
}
func (repository *fakeRepository) AdvanceSource(cursor store.SourceCursor, outboxes []store.OutboxRecord) error {
	if repository.advanceSourceBefore != nil {
		repository.advanceSourceBefore()
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if repository.advanceSourceErr != nil {
		return repository.advanceSourceErr
	}
	repository.log = append(repository.log, "source")
	repository.work.Sources = upsertTestSource(repository.work.Sources, cursor)
	repository.sources[cursor.Source] = cursor
	for _, outbox := range outboxes {
		repository.outboxes[outbox.ID] = outbox
	}
	return nil
}
func (repository *fakeRepository) MarkOutboxDispatched(id model.DeliveryID, deadlineUnixNano int64) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	record, ok := repository.outboxes[id]
	if !ok {
		return errors.New("unknown outbox")
	}
	record.RetryDeadlineUnixNano = deadlineUnixNano
	repository.outboxes[id] = record
	repository.log = append(repository.log, "outbox-dispatched")
	return nil
}
func (repository *fakeRepository) MarkOutboxAccepted(id model.DeliveryID, deadlineUnixNano int64) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	record, ok := repository.outboxes[id]
	if !ok {
		return errors.New("unknown outbox")
	}
	if record.Accepted {
		if record.RetryDeadlineUnixNano != deadlineUnixNano {
			return model.ErrIdentityReuse
		}
		return nil
	}
	record.Accepted = true
	record.RetryDeadlineUnixNano = deadlineUnixNano
	repository.outboxes[id] = record
	repository.log = append(repository.log, "outbox-accepted")
	return nil
}
func (repository *fakeRepository) MarkOutboxCompleted(id model.DeliveryID) error {
	if repository.outboxCompleteStarted != nil {
		select {
		case <-repository.outboxCompleteStarted:
		default:
			close(repository.outboxCompleteStarted)
		}
	}
	if repository.outboxCompleteRelease != nil {
		<-repository.outboxCompleteRelease
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	repository.log = append(repository.log, "outbox-completed")
	outbox := repository.outboxes[id]
	outbox.Completed = true
	repository.outboxes[id] = outbox
	return nil
}
func (repository *fakeRepository) PersistEvent(event model.WorkerEvent) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	repository.persistEventCalls++
	repository.log = append(repository.log, "event")
	repository.work.PendingEvents = append(repository.work.PendingEvents, event)
	repository.work.NextTransactionID = event.TransactionID + 1
	return nil
}
func (repository *fakeRepository) UpsertResult(record model.ResultRecord, provenance model.ResultCopyProvenance) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	for index, stored := range repository.results {
		if stored.Record.SinkTask == record.SinkTask && stored.Record.TupleID == record.TupleID {
			if stored.Record.Checksum != record.Checksum {
				return model.ErrIdentityReuse
			}
			if stored.Provenance == provenance {
				return nil
			}
			if !store.ResultProvenanceOrderedBefore(stored.Provenance, provenance) {
				return model.ErrIdentityReuse
			}
			// Copy-provenance rebind of the identical logical record (the
			// store's Task 24 defect #4 rule).
			repository.log = append(repository.log, "result-rebind")
			repository.results[index].Provenance = provenance
			for workIndex := range repository.work.Results {
				if repository.work.Results[workIndex].Record.SinkTask == record.SinkTask && repository.work.Results[workIndex].Record.TupleID == record.TupleID {
					repository.work.Results[workIndex].Provenance = provenance
				}
			}
			return nil
		}
	}
	repository.log = append(repository.log, "result")
	stored := store.StoredResult{Record: record, Provenance: provenance}
	repository.results = append(repository.results, stored)
	repository.work.Results = append(repository.work.Results, stored)
	return nil
}
func (repository *fakeRepository) ApplyCheckpoint(notice model.CheckpointNotice) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	repository.applyCheckpointCalls++
	cursor, ok := repository.sources[notice.Source]
	if ok && notice.Watermark == cursor.Watermark && notice.RaftIndex == cursor.RaftIndex {
		return nil
	}
	assignment, assignmentOK := repository.assignments[notice.JobID]
	if !assignmentOK || assignment.CoordinatorEpoch != notice.Epoch || notice.Epoch != repository.work.Fence {
		return errors.New("checkpoint coordinator fence mismatch")
	}
	var token model.AssignmentToken
	tokenOK := false
	for _, candidate := range assignment.Assignment.Tasks {
		if candidate.Task == notice.Source {
			token = candidate
			tokenOK = true
			break
		}
	}
	if !tokenOK {
		return errors.New("checkpoint source has no installed token")
	}
	eof, eofErr := model.SourceEOF(assignment.Topology, notice.Source)
	if eofErr != nil || notice.Watermark > eof || notice.Watermark == 0 {
		return errors.New("checkpoint source or watermark is outside installed topology")
	}
	if !ok {
		cursor = store.SourceCursor{Source: notice.Source, NextSequence: 1, EOF: eof}
	}
	if notice.Watermark <= cursor.Watermark || notice.RaftIndex <= cursor.RaftIndex || cursor.CheckpointRevision == ^uint64(0) {
		return model.ErrIdentityReuse
	}
	var report *model.CompletionReport
	for i := range repository.work.PendingEvents {
		candidate := repository.work.PendingEvents[i].Completion
		if candidate != nil && candidate.Source == notice.Source && candidate.New == notice.Watermark {
			report = candidate
			break
		}
	}
	if report != nil && report.ExpectedCheckpointRevision == cursor.CheckpointRevision && report.Prior == cursor.Watermark && report.Epoch == notice.Epoch {
		cursor.CheckpointRevision = report.ExpectedCheckpointRevision + 1
		cursor.CheckpointAuthority = store.CheckpointAuthority{JobControlRevision: report.JobControlRevision, AssignmentRevision: report.AssignmentRevision, AssignmentDigest: assignment.Assignment.Digest, SourceToken: report.Token, CoordinatorEpoch: report.Epoch}
	} else {
		// Committed-watermark adoption mirrors the durable store contract
		// (Task 24 defect #2 ruling): current-fence notice, no report needed.
		cursor.CheckpointRevision++
		cursor.CheckpointAuthority = store.CheckpointAuthority{JobControlRevision: assignment.JobControlRevision, AssignmentRevision: assignment.Assignment.Revision, AssignmentDigest: assignment.Assignment.Digest, SourceToken: token, CoordinatorEpoch: notice.Epoch}
	}
	cursor.Watermark = notice.Watermark
	cursor.RaftIndex = notice.RaftIndex
	if cursor.NextSequence <= notice.Watermark {
		cursor.NextSequence = notice.Watermark + 1
	}
	repository.sources[notice.Source] = cursor
	repository.work.Sources = upsertTestSource(repository.work.Sources, cursor)
	repository.log = append(repository.log, "checkpoint")
	return nil
}
func (repository *fakeRepository) AcknowledgeEvents(through uint64) error {
	if repository.eventAckStarted != nil {
		select {
		case <-repository.eventAckStarted:
		default:
			close(repository.eventAckStarted)
		}
	}
	if repository.eventAckRelease != nil {
		<-repository.eventAckRelease
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	retained := repository.work.PendingEvents[:0]
	for _, event := range repository.work.PendingEvents {
		if event.TransactionID > through {
			retained = append(retained, event)
			continue
		}
		if event.Completion != nil {
			report := event.Completion
			cursor, ok := repository.sources[report.Source]
			applied := ok && report.ExpectedCheckpointRevision != ^uint64(0) && cursor.Watermark == report.New && cursor.CheckpointRevision == report.ExpectedCheckpointRevision+1 && cursor.CheckpointAuthority.SourceToken == report.Token && cursor.CheckpointAuthority.CoordinatorEpoch == report.Epoch
			assignment, assignmentOK := repository.assignments[report.JobID]
			current, tokenOK := findAssignmentToken(assignment.Assignment, report.Source)
			superseded := assignmentOK && tokenOK && assignment.JobControlRevision >= report.JobControlRevision && assignment.Assignment.Revision >= report.AssignmentRevision && (assignment.JobControlRevision > report.JobControlRevision || assignment.Assignment.Revision > report.AssignmentRevision) && (assignment.Assignment.Revision > report.AssignmentRevision && current.AssignmentRevision == assignment.Assignment.Revision || assignment.Assignment.Revision == report.AssignmentRevision && current == report.Token)
			if !applied && !superseded {
				return errors.New("completion event acknowledged before checkpoint")
			}
		} else {
			assignment, ok := repository.assignments[event.Failure.JobID]
			if !ok || assignment.SchedulingState != model.Closed {
				return errors.New("failure event acknowledged before closure")
			}
		}
	}
	repository.work.PendingEvents = retained
	repository.log = append(repository.log, "event-ack")
	return nil
}

func cloneTestTuples(input []model.Tuple) []model.Tuple {
	result := make([]model.Tuple, len(input))
	for i, tuple := range input {
		encoded, _ := model.MarshalTuple(tuple)
		result[i], _ = model.UnmarshalTuple(encoded)
	}
	return result
}
func sameTestDeliveryDefinition(left, right store.DeliveryRecord) bool {
	if left.ID != right.ID || left.Producer != right.Producer || left.Destination != right.Destination || left.AssignmentRevision != right.AssignmentRevision || left.AssignmentDigest != right.AssignmentDigest || left.CoordinatorEpoch != right.CoordinatorEpoch || left.Reservation != right.Reservation {
		return false
	}
	leftBytes, leftErr := model.MarshalTuple(left.Tuple)
	rightBytes, rightErr := model.MarshalTuple(right.Tuple)
	return leftErr == nil && rightErr == nil && string(leftBytes) == string(rightBytes)
}
func upsertTestSource(input []store.SourceCursor, cursor store.SourceCursor) []store.SourceCursor {
	for i := range input {
		if input[i].Source == cursor.Source {
			input[i] = cursor
			return input
		}
	}
	return append(input, cursor)
}

type fakeSender struct {
	mu         sync.Mutex
	deliveries []protocol.TupleDelivery
	times      []time.Time
	now        func() time.Time
	before     func()
	notify     chan protocol.TupleDelivery
	err        error
}

type automaticResultReplicator struct{}

func (automaticResultReplicator) ReplicateRecord(_ context.Context, record model.ResultRecord, provenance model.ResultCopyProvenance) (ResultReplicationReceipt, error) {
	encoded, err := model.MarshalResultRecord(record)
	if err != nil {
		return ResultReplicationReceipt{}, err
	}
	return ResultReplicationReceipt{DestinationNodeID: provenance.ReplicaSet.SecondaryNodeID, DestinationWorkerEpoch: provenance.ReplicaSet.SecondaryEpoch, StreamChecksum: model.ResultRecordStreamChecksum(record), StreamLength: uint64(len(encoded)), CoordinatorEpoch: provenance.CoordinatorEpoch}, nil
}

func (sender *fakeSender) Send(_ context.Context, delivery protocol.TupleDelivery) error {
	if sender.before != nil {
		sender.before()
	}
	sender.mu.Lock()
	sender.deliveries = append(sender.deliveries, delivery)
	if sender.now != nil {
		sender.times = append(sender.times, sender.now())
	}
	notify, err := sender.notify, sender.err
	sender.mu.Unlock()
	if notify != nil {
		select {
		case notify <- delivery:
		default:
		}
	}
	return err
}
func (sender *fakeSender) count() int {
	sender.mu.Lock()
	defer sender.mu.Unlock()
	return len(sender.deliveries)
}

func (sender *fakeSender) sendTimes() []time.Time {
	sender.mu.Lock()
	defer sender.mu.Unlock()
	return append([]time.Time(nil), sender.times...)
}

func deliveryMessage(record store.DeliveryRecord) protocol.TupleDelivery {
	return protocol.TupleDelivery{DeliveryID: record.ID, Tuple: record.Tuple, Producer: record.Producer, Destination: record.Destination, Assignment: protocol.AssignmentSetIdentity{JobID: record.ID.Tuple.JobID, Revision: record.AssignmentRevision, Digest: record.AssignmentDigest}, Coordinator: record.CoordinatorEpoch}
}
