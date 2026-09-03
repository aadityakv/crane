package worker

import (
	"context"
	"errors"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/config"
	"github.com/aaditya/cs425mp3/internal/crane/admission"
	"github.com/aaditya/cs425mp3/internal/crane/membership"
	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
	"github.com/aaditya/cs425mp3/internal/crane/store"
	"github.com/aaditya/cs425mp3/internal/swim"
	"github.com/aaditya/cs425mp3/internal/wire"
)

// The repair driver owns the destination side of every bilateral
// RepairResultPartition grant this worker durably holds: on grant install and
// on recovery it schedules the grant, then one serialized loop pulls the
// covered record range from the named source over an authenticated +5
// worker-to-worker session — NextRepairRecord at the source, durable local
// install through the existing ReceiveResultRecord path, progress acked by
// the next pull's verified digest — until the source reports completion.

type repairDriverRepository struct {
	*transferRepository
	transaction uint64
}

func (repository *repairDriverRepository) DurableTransactionID() (uint64, error) {
	return repository.transaction, nil
}

func newRepairDriverRepository(repository *transferRepository) *repairDriverRepository {
	return &repairDriverRepository{transferRepository: repository, transaction: 7}
}

// routingRepairClient routes pulls to a real in-process source owner while
// observing every pulled destination status, and can inject source failures
// or covered-vector violations.
type routingRepairClient struct {
	mu     sync.Mutex
	clock  *clock.Manual
	owner  *TransferOwner
	peer   TransferPeer
	source *transferRepository
	calls  []protocol.ResultRepairStatus
	times  []time.Time
	inject func(call int, status protocol.ResultRepairStatus) (protocol.WorkerMessage, bool, error)
}

func (client *routingRepairClient) pulled() []protocol.ResultRepairStatus {
	client.mu.Lock()
	defer client.mu.Unlock()
	return append([]protocol.ResultRepairStatus(nil), client.calls...)
}

func (client *routingRepairClient) pulledAt() []time.Time {
	client.mu.Lock()
	defer client.mu.Unlock()
	return append([]time.Time(nil), client.times...)
}

func (client *routingRepairClient) PullRepair(ctx context.Context, sourceNode uint16, sourceEpoch model.WorkerEpoch, status protocol.ResultRepairStatus) (protocol.WorkerMessage, error) {
	client.mu.Lock()
	client.calls = append(client.calls, status)
	client.times = append(client.times, client.clock.Now())
	inject := client.inject
	client.mu.Unlock()
	if sourceNode != status.Instruction.SourceNodeID || sourceEpoch != status.Instruction.SourceWorkerEpoch {
		return nil, errors.New("pull routed to a non-grant source")
	}
	if inject != nil {
		response, handled, err := inject(len(client.pulled()), status)
		if handled {
			return response, err
		}
	}
	chunk, complete, err := client.owner.ServeRepairPull(ctx, client.peer, status)
	if err != nil {
		return nil, err
	}
	if complete {
		return repairPullCompleteResponse(client.source, sourceNode, sourceEpoch, status), nil
	}
	return chunk, nil
}

// repairPullCompleteResponse mirrors the +5 dispatch conversion from the
// serving owner's complete signal to the terminal WorkerStatus response.
func repairPullCompleteResponse(source *transferRepository, sourceNode uint16, sourceEpoch model.WorkerEpoch, pulled protocol.ResultRepairStatus) protocol.WorkerMessage {
	work, err := source.RecoverWork()
	if err != nil {
		return protocol.WorkerStatus{}
	}
	for _, repair := range work.Repairs {
		if repair.Instruction.RepairID == pulled.RepairID && repair.Role == store.RepairSource {
			status := repairStatus(repair)
			return protocol.WorkerStatus{NodeID: sourceNode, WorkerEpoch: sourceEpoch, CoordinatorEpoch: work.Fence, StoreTransactionID: 9, Repair: &status}
		}
	}
	return protocol.WorkerStatus{}
}

type repairDriverFixture struct {
	transferFixture
	sourceRepository      *repairDriverRepository
	destinationRepository *repairDriverRepository
	sourceOwner           *TransferOwner
	destinationOwner      *TransferOwner
	gate                  *admission.Gate
	clock                 *clock.Manual
	client                *routingRepairClient
	driver                *RepairDriver
	cancel                context.CancelFunc
	done                  chan error
	stopped               *bool
}

func newRepairDriverFixture(t *testing.T) *repairDriverFixture {
	t.Helper()
	fixture := &repairDriverFixture{transferFixture: newTransferFixture(t), gate: admission.NewGate(), clock: clock.NewManual(time.Unix(1000, 0))}
	if err := fixture.gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	fixture.installSourceRepair(t)
	fixture.installRepair(t)
	// The pull request marshals the whole instruction, and the wire contract
	// requires a nonempty checkpoint vector, exactly as production grants
	// always carry one; rebind the fixture grants under a fully covering
	// vector so the durable fixture matches the wire-serializable form.
	rebindFixtureCheckpoints(t, fixture, fixture.records[len(fixture.records)-1].TupleID.SourceSequence)
	fixture.sourceRepository = newRepairDriverRepository(fixture.transferFixture.source)
	fixture.destinationRepository = newRepairDriverRepository(fixture.transferFixture.destination)
	var err error
	fixture.sourceOwner, err = NewTransferOwner(TransferOptions{Repository: fixture.sourceRepository})
	if err != nil {
		t.Fatal(err)
	}
	fixture.destinationOwner, err = NewTransferOwner(TransferOptions{Repository: fixture.destinationRepository})
	if err != nil {
		t.Fatal(err)
	}
	fixture.client = &routingRepairClient{clock: fixture.clock, owner: fixture.sourceOwner, peer: fixture.destinationPeer(), source: fixture.transferFixture.source}
	fixture.driver, err = NewRepairDriver(RepairDriverOptions{Repository: fixture.destinationRepository, Transfer: fixture.destinationOwner, Client: fixture.client, Clock: fixture.clock, RetryInterval: 200 * time.Millisecond, MaxRetryInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return fixture
}

func (fixture *repairDriverFixture) start(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	stopped := false
	fixture.stopped = &stopped
	fixture.cancel = cancel
	fixture.done = make(chan error, 1)
	go func() { fixture.done <- fixture.driver.Run(ctx) }()
	t.Cleanup(func() {
		if stopped {
			return
		}
		cancel()
		select {
		case <-fixture.done:
		case <-time.After(5 * time.Second):
			t.Fatal("repair driver did not join on cancel")
		}
	})
}

func (fixture *repairDriverFixture) stop() {
	*fixture.stopped = true
	fixture.cancel()
	<-fixture.done
}

func (fixture *repairDriverFixture) destinationRepair(t *testing.T) store.ResultRepairRecord {
	t.Helper()
	work, err := fixture.destinationRepository.RecoverWork()
	if err != nil {
		t.Fatal(err)
	}
	for _, repair := range work.Repairs {
		if repair.Instruction.RepairID == fixture.repair.Instruction.RepairID {
			return repair
		}
	}
	t.Fatal("destination repair grant missing")
	return store.ResultRepairRecord{}
}

func (fixture *repairDriverFixture) sourceRepair(t *testing.T) store.ResultRepairRecord {
	t.Helper()
	work, err := fixture.sourceRepository.RecoverWork()
	if err != nil {
		t.Fatal(err)
	}
	for _, repair := range work.Repairs {
		if repair.Instruction.RepairID == fixture.repair.Instruction.RepairID {
			return repair
		}
	}
	t.Fatal("source repair grant missing")
	return store.ResultRepairRecord{}
}

// advanceUntil fires every pending manual-clock timer deadline by advancing
// the simulated clock in small quanta until condition holds.
func (fixture *repairDriverFixture) advanceUntil(condition func() bool) bool {
	deadline := fixture.clock.Now().Add(2 * time.Minute)
	for !condition() {
		if fixture.clock.Now().After(deadline) {
			return false
		}
		fixture.clock.Advance(10 * time.Millisecond)
		runtime.Gosched()
	}
	return true
}

func TestNewRepairDriverRejectsInvalidDependencies(t *testing.T) {
	fixture := newRepairDriverFixture(t)
	base := RepairDriverOptions{Repository: fixture.destinationRepository, Transfer: fixture.destinationOwner, Client: fixture.client, Clock: fixture.clock, RetryInterval: time.Millisecond, MaxRetryInterval: 10 * time.Millisecond}
	tests := map[string]func(*RepairDriverOptions){
		"no repository":   func(options *RepairDriverOptions) { options.Repository = nil },
		"no transfer":     func(options *RepairDriverOptions) { options.Transfer = nil },
		"no client":       func(options *RepairDriverOptions) { options.Client = nil },
		"no clock":        func(options *RepairDriverOptions) { options.Clock = nil },
		"zero retry":      func(options *RepairDriverOptions) { options.RetryInterval = 0 },
		"zero max retry":  func(options *RepairDriverOptions) { options.MaxRetryInterval = 0 },
		"max below retry": func(options *RepairDriverOptions) { options.MaxRetryInterval = options.RetryInterval - 1 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			options := base
			mutate(&options)
			if _, err := NewRepairDriver(options); err == nil {
				t.Fatal("invalid repair driver options accepted")
			}
		})
	}
	if _, err := NewRepairDriver(base); err != nil {
		t.Fatalf("valid options rejected: %v", err)
	}
}

// TestRepairDriverStreamsPendingGrantToCompletion proves the happy path
// record-by-record: every pull carries the destination's durable progress,
// the source advances only its durably acked prefix (install before ack), and
// both grants durably reach RepairComplete with the records installed.
func TestRepairDriverStreamsPendingGrantToCompletion(t *testing.T) {
	fixture := newRepairDriverFixture(t)
	fixture.driver.Schedule(fixture.destinationRepair(t))
	fixture.start(t)
	// The source reaches RepairComplete only when the final pull's verified
	// progress acknowledges the destination's complete install, so this is
	// the deterministic end-of-drive observation.
	if !fixture.advanceUntil(func() bool { return fixture.sourceRepair(t).State == store.RepairComplete }) {
		t.Fatalf("source grant never completed: dest=%+v", fixture.destinationRepair(t))
	}
	source := fixture.sourceRepair(t)
	if source.RecordCount != fixture.repair.Instruction.ExpectedRecordCount {
		t.Fatalf("source grant did not complete: %+v", source)
	}
	if !fixture.advanceUntil(func() bool { return fixture.destinationRepair(t).State == store.RepairComplete }) {
		t.Fatalf("destination grant never completed: %+v", fixture.destinationRepair(t))
	}
	destination := fixture.destinationRepair(t)
	if destination.RecordCount != fixture.repair.Instruction.ExpectedRecordCount || destination.ContentDigest != fixture.repair.Instruction.ExpectedContentDigest {
		t.Fatalf("destination summary mismatch: %+v", destination)
	}
	work, err := fixture.destinationRepository.RecoverWork()
	if err != nil {
		t.Fatal(err)
	}
	if len(work.Results) != len(fixture.records) {
		t.Fatalf("installed results=%d want=%d", len(work.Results), len(fixture.records))
	}
	pulled := fixture.client.pulled()
	for index, status := range pulled {
		if status.Role != protocol.RepairDestination || status.RepairID != fixture.repair.Instruction.RepairID {
			t.Fatalf("pull %d carried a foreign grant: %+v", index, status)
		}
	}
	// Pull 0 asks from zero progress; pull 1 proves record 0 was durably
	// installed before the source advanced past it; the final pull reports
	// the complete summary under which the source marks itself complete.
	if len(pulled) != 3 || pulled[0].RecordCount != 0 || pulled[1].RecordCount != 1 || pulled[2].RecordCount != fixture.repair.Instruction.ExpectedRecordCount {
		for _, status := range pulled {
			t.Logf("pulled status count=%d state=%d", status.RecordCount, status.State)
		}
		t.Fatalf("pull progression did not install durably before acking: %d pulls", len(pulled))
	}
}

// TestRepairDriverResumesFromDurableProgress proves crash-safe resume: a
// driver restarted after a mid-stream crash resumes from the durable
// destination progress, never re-pulls covered records as new work, and both
// sides still complete.
func TestRepairDriverResumesFromDurableProgress(t *testing.T) {
	fixture := newRepairDriverFixture(t)
	var mu sync.Mutex
	served := 0
	fixture.client.inject = func(call int, status protocol.ResultRepairStatus) (protocol.WorkerMessage, bool, error) {
		mu.Lock()
		defer mu.Unlock()
		if served == 1 {
			served++
			return nil, true, errors.New("source unavailable")
		}
		served++
		return nil, false, nil
	}
	fixture.driver.Schedule(fixture.destinationRepair(t))
	fixture.start(t)
	if !fixture.advanceUntil(func() bool { return fixture.destinationRepair(t).NextRecord == 1 }) {
		t.Fatalf("first record never installed: %+v", fixture.destinationRepair(t))
	}
	// Crash: stop the first driver incarnation mid-stream while the source
	// is unreachable (the next exchange fails and backs off).
	fixture.stop()

	// A fresh incarnation recovers the retained grant and resumes strictly
	// from durable progress.
	restart, err := NewRepairDriver(RepairDriverOptions{Repository: fixture.destinationRepository, Transfer: fixture.destinationOwner, Client: fixture.client, Clock: fixture.clock, RetryInterval: 200 * time.Millisecond, MaxRetryInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	fixture.driver = restart
	before := len(fixture.client.pulled())
	fixture.start(t)
	if !fixture.advanceUntil(func() bool { return fixture.destinationRepair(t).State == store.RepairComplete }) {
		t.Fatalf("resumed grant never completed: %+v", fixture.destinationRepair(t))
	}
	if !fixture.advanceUntil(func() bool { return fixture.sourceRepair(t).State == store.RepairComplete }) {
		t.Fatalf("source grant never completed: %+v", fixture.sourceRepair(t))
	}
	resume := fixture.client.pulled()[before]
	if resume.RecordCount != 1 || resume.State != protocol.RepairStreaming {
		t.Fatalf("resume did not continue from durable progress: %+v", resume)
	}
	work, err := fixture.destinationRepository.RecoverWork()
	if err != nil {
		t.Fatal(err)
	}
	if len(work.Results) != len(fixture.records) {
		t.Fatalf("duplicate installs after resume: results=%d", len(work.Results))
	}
}

// TestRepairDriverRetriesSourceFailureWithBoundedBackoff proves transient
// source failures retry with doubling backoff capped at the configured
// maximum, then complete once the source recovers.
func TestRepairDriverRetriesSourceFailureWithBoundedBackoff(t *testing.T) {
	fixture := newRepairDriverFixture(t)
	var mu sync.Mutex
	failures := 0
	fixture.client.inject = func(call int, status protocol.ResultRepairStatus) (protocol.WorkerMessage, bool, error) {
		mu.Lock()
		defer mu.Unlock()
		if failures < 4 {
			failures++
			return nil, true, errors.New("source unreachable")
		}
		return nil, false, nil
	}
	fixture.driver.Schedule(fixture.destinationRepair(t))
	fixture.start(t)
	if !fixture.advanceUntil(func() bool { return fixture.sourceRepair(t).State == store.RepairComplete }) {
		t.Fatalf("grant never completed after source recovery: %+v", fixture.destinationRepair(t))
	}
	times := fixture.client.pulledAt()
	// Four failed pulls, then the two completing record pulls plus the final
	// completion pull.
	if len(times) != 7 {
		t.Fatalf("pull schedule unexpected: %d pulls", len(times))
	}
	// The manual clock only fires while the harness advances it, so observed
	// gaps carry bounded scheduler overshoot; the doubling schedule and its
	// cap remain exact lower bounds.
	tolerance := 100 * time.Millisecond
	intervals := []time.Duration{200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, time.Second}
	for index, want := range intervals {
		got := times[index+1].Sub(times[index])
		if got < want || got > want+tolerance {
			t.Fatalf("backoff %d = %v want %v (capped doubling)", index, got, want)
		}
	}
	// A successful exchange resets backoff: the next pull follows within one
	// advance quantum, far below the initial retry interval.
	if next := times[5].Sub(times[4]); next >= intervals[0] {
		t.Fatalf("success did not reset backoff: %v", next)
	}
}

// rebindFixtureCheckpoints rewrites both durable grants so the instruction's
// checkpoint vector covers source sequences through watermark, recomputing
// every derived identity and expected summary. A watermark below a record's
// sequence makes that record a covered-vector violation when served.
func rebindFixtureCheckpoints(t *testing.T, fixture *repairDriverFixture, watermark uint64) {
	t.Helper()
	checkpoint := model.SourceCheckpoint{Source: fixture.records[0].TupleID.SourceTask, Watermark: watermark}
	covered := make([]model.ResultRecord, 0, len(fixture.records))
	for _, record := range fixture.records {
		if record.TupleID.SourceSequence <= checkpoint.Watermark {
			covered = append(covered, record)
		}
	}
	queryDigest := model.ResultInventoryQueryDigest(model.ResultInventoryQueryDefinition{JobID: fixture.repair.Instruction.JobID, SinkTask: fixture.repair.Instruction.SinkTask, SpecificationHash: fixture.repair.Instruction.SpecificationHash, AssignmentRevision: fixture.repair.Instruction.AssignmentRevision, AssignmentDigest: fixture.repair.Instruction.AssignmentDigest, Checkpoints: []model.SourceCheckpoint{checkpoint}, CheckpointDigest: model.CheckpointVectorDigest([]model.SourceCheckpoint{checkpoint})})
	count, total, digest, err := ResultInventoryAggregate(queryDigest, covered)
	if err != nil {
		t.Fatal(err)
	}
	rebind := func(repair store.ResultRepairRecord) store.ResultRepairRecord {
		repair.Instruction.Checkpoints = []model.SourceCheckpoint{checkpoint}
		repair.Instruction.CheckpointDigest = model.CheckpointVectorDigest(repair.Instruction.Checkpoints)
		repair.Instruction.InventoryQueryDigest = queryDigest
		repair.Instruction.ExpectedRecordCount = count
		repair.Instruction.ExpectedTotalBytes = total
		repair.Instruction.ExpectedContentDigest = digest
		rebindTransferRepair(&repair)
		repair.State = store.RepairPending
		repair.NextRecord, repair.NextOffset, repair.RecordCount, repair.TotalBytes = 0, 0, 0, 0
		repair.ContentDigest = model.EmptyResultInventoryDigest(repair.InstructionDigest)
		return repair
	}
	for _, repository := range []*transferRepository{fixture.transferFixture.source, fixture.transferFixture.destination} {
		repository.mu.Lock()
		for index := range repository.work.Repairs {
			repository.work.Repairs[index] = rebind(repository.work.Repairs[index])
		}
		repository.repairs = append([]store.ResultRepairRecord(nil), repository.work.Repairs...)
		repository.mu.Unlock()
	}
	fixture.transferFixture.repair = rebind(fixture.transferFixture.repair)
}

// TestRepairDriverFailsClosedOnCoveredVectorViolation proves a source that
// serves a record outside the grant's covered checkpoint vector is rejected
// by the existing validation, the loop stops (no unbounded retries), and no
// uncovered state is mutated.
func TestRepairDriverFailsClosedOnCoveredVectorViolation(t *testing.T) {
	fixture := newRepairDriverFixture(t)
	rebindFixtureCheckpoints(t, fixture, fixture.records[0].TupleID.SourceSequence)
	fixture.client.inject = func(call int, status protocol.ResultRepairStatus) (protocol.WorkerMessage, bool, error) {
		if call != 1 {
			return nil, false, nil
		}
		// A lying source serves the record beyond the committed checkpoint
		// watermark: the existing receive validation must reject it.
		chunk := fixture.repairChunk(t, fixture.records[1])
		return chunk, true, nil
	}
	fixture.driver.Schedule(fixture.destinationRepair(t))
	fixture.start(t)
	// The violating pull itself must be observed.
	if !fixture.advanceUntil(func() bool { return len(fixture.client.pulled()) >= 1 }) {
		t.Fatal("violating pull never issued")
	}
	grant := fixture.destinationRepair(t)
	if grant.NextRecord != 0 {
		t.Fatalf("covered-vector violation mutated durable progress: %+v", grant)
	}
	work, err := fixture.destinationRepository.RecoverWork()
	if err != nil {
		t.Fatal(err)
	}
	if len(work.Results) != 0 {
		t.Fatalf("violation installed uncovered results: %d", len(work.Results))
	}
	if fixture.advanceUntil(func() bool { return len(fixture.client.pulled()) >= 2 }) {
		t.Fatalf("driver kept retrying a deterministic validation rejection: %d pulls", len(fixture.client.pulled()))
	}
}

// TestRepairDriverStopsOnSupersededOrRevokedGrant proves the loop never
// drives a grant whose durable authority disappeared before streaming began.
func TestRepairDriverStopsOnSupersededOrRevokedGrant(t *testing.T) {
	tests := map[string]func(*testing.T, *repairDriverFixture){
		"stale fence": func(t *testing.T, fixture *repairDriverFixture) {
			fixture.destination.mu.Lock()
			newer := fixture.epoch
			newer.Term++
			fixture.destination.work.Fence = newer
			fixture.destination.mu.Unlock()
		},
		"revoked grant": func(t *testing.T, fixture *repairDriverFixture) {
			revoked := fixture.destinationRepair(t)
			revoked.State = store.RepairFailed
			revoked.ErrorCode = protocol.WorkerErrorCorrupt
			if err := fixture.destination.UpsertRepair(revoked); err != nil {
				t.Fatal(err)
			}
		},
		"missing grant": func(t *testing.T, fixture *repairDriverFixture) {
			fixture.destination.mu.Lock()
			fixture.destination.work.Repairs = nil
			fixture.destination.repairs = nil
			fixture.destination.mu.Unlock()
		},
	}
	for name, sabotage := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newRepairDriverFixture(t)
			fixture.driver.Schedule(fixture.destinationRepair(t))
			sabotage(t, fixture)
			fixture.start(t)
			if fixture.advanceUntil(func() bool { return len(fixture.client.pulled()) > 0 }) {
				t.Fatalf("sabotaged grant was driven: %d pulls", len(fixture.client.pulled()))
			}
		})
	}
}

// TestRepairDriverStopsMidStreamWhenAuthorityAdvances proves a grant being
// driven stops as soon as its durable authority is superseded between
// exchanges.
func TestRepairDriverStopsMidStreamWhenAuthorityAdvances(t *testing.T) {
	fixture := newRepairDriverFixture(t)
	var once sync.Once
	fixture.client.inject = func(call int, status protocol.ResultRepairStatus) (protocol.WorkerMessage, bool, error) {
		once.Do(func() {
			fixture.destination.mu.Lock()
			newer := fixture.epoch
			newer.Term++
			fixture.destination.work.Fence = newer
			fixture.destination.mu.Unlock()
		})
		return nil, false, nil
	}
	fixture.driver.Schedule(fixture.destinationRepair(t))
	fixture.start(t)
	if !fixture.advanceUntil(func() bool { return len(fixture.client.pulled()) >= 1 }) {
		t.Fatal("first pull never issued")
	}
	if fixture.advanceUntil(func() bool { return len(fixture.client.pulled()) >= 2 }) {
		t.Fatalf("stale-authority grant kept being driven: %d pulls", len(fixture.client.pulled()))
	}
}

// TestRepairDriverRecoverySchedulesRetainedGrants proves Run recovers every
// retained destination grant and drives it without any explicit Schedule.
func TestRepairDriverRecoverySchedulesRetainedGrants(t *testing.T) {
	fixture := newRepairDriverFixture(t)
	fixture.start(t)
	if !fixture.advanceUntil(func() bool { return fixture.sourceRepair(t).State == store.RepairComplete }) {
		t.Fatalf("recovered grant never driven: dest=%+v", fixture.destinationRepair(t))
	}
	if fixture.destinationRepair(t).State != store.RepairComplete {
		t.Fatalf("recovered destination grant incomplete: %+v", fixture.destinationRepair(t))
	}
}

// TestRepairDriverDoesNotDriveSourceRoleGrants proves grants this worker
// holds only as the source endpoint are never driven: the destination owns
// the loop.
func TestRepairDriverDoesNotDriveSourceRoleGrants(t *testing.T) {
	fixture := newRepairDriverFixture(t)
	sourceGrant := fixture.destinationRepair(t)
	sourceGrant.Role = store.RepairSource
	fixture.driver.Schedule(sourceGrant)
	fixture.destination.mu.Lock()
	fixture.destination.work.Repairs = nil
	fixture.destination.repairs = nil
	fixture.destination.mu.Unlock()
	fixture.start(t)
	if fixture.advanceUntil(func() bool { return len(fixture.client.pulled()) > 0 }) {
		t.Fatalf("source-role grant was driven: %d pulls", len(fixture.client.pulled()))
	}
	// A subsequently scheduled destination grant still drives, proving the
	// driver stayed live rather than crashing.
	destination := fixture.sourceRepair(t)
	destination.Role = store.RepairDestination
	destination.State = store.RepairPending
	destination.NextRecord, destination.NextOffset, destination.RecordCount, destination.TotalBytes = 0, 0, 0, 0
	destination.ContentDigest = model.EmptyResultInventoryDigest(destination.InstructionDigest)
	if err := fixture.destination.UpsertRepair(destination); err != nil {
		t.Fatal(err)
	}
	fixture.driver.Schedule(destination)
	if !fixture.advanceUntil(func() bool { return fixture.destinationRepair(t).State == store.RepairComplete }) {
		t.Fatal("destination grant scheduled after a source grant was never driven")
	}
}

// TestRepairDriverRunJoinsPromptlyDuringBackoff proves a driver waiting out a
// retry backoff still joins promptly on cancellation.
func TestRepairDriverRunJoinsPromptlyDuringBackoff(t *testing.T) {
	fixture := newRepairDriverFixture(t)
	fixture.client.inject = func(call int, status protocol.ResultRepairStatus) (protocol.WorkerMessage, bool, error) {
		return nil, true, errors.New("source unreachable")
	}
	fixture.driver.Schedule(fixture.destinationRepair(t))
	fixture.start(t)
	if !fixture.advanceUntil(func() bool { return len(fixture.client.pulled()) >= 1 }) {
		t.Fatal("first pull never issued")
	}
	*fixture.stopped = true
	joined := make(chan error, 1)
	go func() { joined <- <-fixture.done }()
	fixture.cancel()
	select {
	case err := <-joined:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("run error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("driver did not join during backoff")
	}
}

// TestRepairDriverPullsOverAuthenticatedWorkerControlSession proves the full
// production wire path: the destination driver dials the source's real +5
// control listener, handshakes, pulls with its durable status, installs each
// response chunk durably, and observes the terminal source status — with the
// historical source serving under a closed process admission gate.
func TestRepairDriverPullsOverAuthenticatedWorkerControlSession(t *testing.T) {
	fixture := newRepairDriverFixture(t)
	specification, ok := config.LookupService(config.ServiceCraneWorker)
	if !ok {
		t.Fatal("crane worker service specification missing")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if port <= int(specification.Offset) {
		t.Fatalf("ephemeral port %d below service offset %d", port, specification.Offset)
	}
	authenticator := wire.NewHMACAuthenticator([]byte("repair-driver-wire-test-secret"))
	members := &controlTestMembership{view: membership.View{Revision: 1, Members: []swim.Member{
		{NodeID: fixture.replica.PrimaryNodeID, Host: "127.0.0.1", BasePort: uint16(port) - specification.Offset, Incarnation: 1, Status: swim.Alive},
		{NodeID: fixture.replica.SecondaryNodeID, Host: "127.0.0.1", BasePort: 9200, Incarnation: 1, Status: swim.Alive},
	}}}
	controlRepository := &repairControlRepository{repository: fixture.transferFixture.source, node: fixture.replica.PrimaryNodeID, epoch: fixture.replica.PrimaryEpoch}
	sourceOwner, err := NewControlOwner(ControlOptions{Config: config.NodeConfig{NodeID: fixture.replica.PrimaryNodeID, Crane: config.DefaultCraneConfig(), Timing: config.DefaultTimingConfig()}, ClusterID: [16]byte{7}, Repository: controlRepository, Engine: &controlNoopEngine{}, Transfer: fixture.sourceOwner, Gate: admission.NewGate(), Membership: members, Clock: fixture.clock})
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- serveRepairControlSessions(context.Background(), listener, sourceOwner, authenticator, [16]byte{7}, fixture.replica.PrimaryNodeID, fixture.replica.PrimaryEpoch, fixture.epoch, fixture.clock)
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-serveDone
	})

	client := newDialRepairSourceClient(dialRepairSourceClientOptions{ClusterID: [16]byte{7}, Authenticator: authenticator, Clock: fixture.clock, Membership: members, Repository: fixture.destinationRepository, Timeout: 2 * time.Second, Dial: (&net.Dialer{}).DialContext})
	driver, err := NewRepairDriver(RepairDriverOptions{Repository: fixture.destinationRepository, Transfer: fixture.destinationOwner, Client: client, Clock: fixture.clock, RetryInterval: 200 * time.Millisecond, MaxRetryInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	fixture.driver = driver
	fixture.driver.Schedule(fixture.destinationRepair(t))
	fixture.start(t)
	if !fixture.advanceUntil(func() bool { return fixture.destinationRepair(t).State == store.RepairComplete }) {
		t.Fatalf("wire-driven grant never completed: %+v", fixture.destinationRepair(t))
	}
	if !fixture.advanceUntil(func() bool { return fixture.sourceRepair(t).State == store.RepairComplete }) {
		t.Fatalf("wire source grant never completed: %+v", fixture.sourceRepair(t))
	}
}

// serveRepairControlSessions mirrors the production +5 connection loop for
// the wire-level driver test.
func serveRepairControlSessions(ctx context.Context, listener net.Listener, owner *ControlOwner, authenticator wire.Authenticator, clusterID [16]byte, node uint16, workerEpoch model.WorkerEpoch, epoch model.CoordinatorEpoch, source clock.Clock) error {
	var handlers sync.WaitGroup
	defer handlers.Wait()
	stopAccept := context.AfterFunc(ctx, func() { _ = listener.Close() })
	defer stopAccept()
	for {
		connection, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		session, err := owner.NewSession(connection.RemoteAddr(), connection.Close)
		if err != nil {
			_ = connection.Close()
			continue
		}
		handlers.Add(1)
		go func() {
			defer handlers.Done()
			defer session.Close()
			limits := wire.DefaultLimits()
			limits.MaxFrameSize = int(model.WorkerControlMaxFrameBytesV1)
			limits.ExpectedClusterID = &clusterID
			stream := wire.NewTCPFrameStream(connection, authenticator, limits, 2*time.Second)
			for {
				frame, err := stream.ReadFrame(ctx)
				if err != nil {
					return
				}
				response, handleErr := session.Handle(ctx, frame)
				if handleErr != nil {
					response = protocol.WorkerError{NodeID: node, WorkerEpoch: workerEpoch, CoordinatorEpoch: epoch, RelatedMessage: frame.Header.Message, Code: protocol.WorkerErrorUnauthorized, Detail: []byte(handleErr.Error())}
				}
				payload, err := protocol.MarshalWorkerMessage(response)
				if err != nil {
					return
				}
				outbound := wire.Frame{Header: wire.Header{Version: wire.Version1, Message: response.MessageType(), ClusterID: clusterID, SenderID: node, RequestID: frame.Header.RequestID, TimestampMillis: source.Now().UnixMilli(), Codec: wire.CodecBinary}, Payload: payload}
				if err := stream.WriteFrame(ctx, outbound); err != nil {
					return
				}
			}
		}()
	}
}

// repairControlRepository adapts a transfer repository to the +5 command
// owner's durable surface for the wire-level driver test.
type repairControlRepository struct {
	repository *transferRepository
	node       uint16
	epoch      model.WorkerEpoch
}

func (repository *repairControlRepository) LocalIdentity() (uint16, model.WorkerEpoch) {
	return repository.node, repository.epoch
}
func (repository *repairControlRepository) RecoverWorkBounded() (store.RecoveredWork, error) {
	return repository.RecoverWork()
}

func (repository *repairControlRepository) RecoverWork() (store.RecoveredWork, error) {
	return repository.repository.RecoverWork()
}
func (repository *repairControlRepository) DurableTransactionID() (uint64, error) { return 11, nil }
func (repository *repairControlRepository) Fence(model.CoordinatorEpoch) error    { return nil }
func (repository *repairControlRepository) InstallAssignment(model.AssignmentSet, model.TopologySpec, uint64, model.SchedulingState, model.CoordinatorEpoch) error {
	return nil
}
func (repository *repairControlRepository) PendingEvents(uint64, uint16) ([]model.WorkerEvent, uint64, bool, error) {
	return nil, 0, false, nil
}
func (repository *repairControlRepository) UpsertRepair(repair store.ResultRepairRecord) error {
	return repository.repository.UpsertRepair(repair)
}
func (repository *repairControlRepository) ObserveCheckpoint(protocol.CheckpointNotice) error {
	return nil
}

// TestRepairDriverInstallsWhileProcessAdmissionClosed pins the Task 24
// defect #6 ruling: the coordinator repairs a sink partition while the job is
// fenced Closed (activateJob: Closed install, notices, repair, Running
// install), and after any leadership fence the destination process's
// admission gate stays closed until that final Running install. The
// destination-driven transfer therefore must not require the process
// admission gate: the durable grant under the current fence and the
// transfer's own authority/coverage validation are the authority. A driver
// that waited for the gate would deadlock with the coordinator waiting for
// the repair.
func TestRepairDriverInstallsWhileProcessAdmissionClosed(t *testing.T) {
	fixture := newRepairDriverFixture(t)
	if err := fixture.gate.CloseAndWait(context.Background()); err != nil {
		t.Fatal(err)
	}
	fixture.driver.Schedule(fixture.destinationRepair(t))
	fixture.start(t)
	if !fixture.advanceUntil(func() bool { return fixture.sourceRepair(t).State == store.RepairComplete }) {
		t.Fatalf("repair never completed under a closed process admission gate: dest=%+v", fixture.destinationRepair(t))
	}
	if !fixture.advanceUntil(func() bool { return fixture.destinationRepair(t).State == store.RepairComplete }) {
		t.Fatalf("destination grant never completed: %+v", fixture.destinationRepair(t))
	}
	work, err := fixture.destinationRepository.RecoverWork()
	if err != nil {
		t.Fatal(err)
	}
	if len(work.Results) != len(fixture.records) {
		t.Fatalf("installed results=%d want=%d", len(work.Results), len(fixture.records))
	}
}

// installSupersedingAssignment installs revision+1 of the fixture assignment
// with an unchanged placement on the given repositories: the reassignment
// that supersedes a grant need not touch the repaired sink.
func installSupersedingAssignment(t *testing.T, fixture *repairDriverFixture, repositories ...*transferRepository) store.InstalledAssignment {
	t.Helper()
	prior := fixture.assignment.Assignment
	tasks := append([]model.AssignmentToken(nil), prior.Tasks...)
	for index := range tasks {
		tasks[index].AssignmentRevision = prior.Revision + 1
	}
	set, err := model.NewAssignmentSet(prior.JobID, prior.Revision+1, tasks, append([]model.ResultReplicaSet(nil), prior.ResultReplicas...), fixture.assignment.Topology)
	if err != nil {
		t.Fatal(err)
	}
	installed := fixture.assignment
	installed.Assignment = set
	installed.JobControlRevision++
	for _, repository := range repositories {
		repository.mu.Lock()
		repository.assignments[set.JobID] = installed
		repository.work.Assignments = []store.InstalledAssignment{installed}
		repository.mu.Unlock()
	}
	return installed
}

// supersedingGrant derives the coordinator's replacement grant for the fixture
// sink under the superseding assignment: same vector, new revision/digest and
// therefore a new RepairID.
func supersedingGrant(t *testing.T, fixture *repairDriverFixture, installed store.InstalledAssignment) store.ResultRepairRecord {
	t.Helper()
	grant := fixture.repair
	grant.Instruction.Checkpoints = append([]model.SourceCheckpoint(nil), fixture.repair.Instruction.Checkpoints...)
	grant.Instruction.AssignmentRevision = installed.Assignment.Revision
	grant.Instruction.AssignmentDigest = installed.Assignment.Digest
	rebindTransferRepair(&grant)
	count, total, digest, err := ResultInventoryAggregate(grant.Instruction.InventoryQueryDigest, fixture.records)
	if err != nil {
		t.Fatal(err)
	}
	grant.Instruction.ExpectedRecordCount, grant.Instruction.ExpectedTotalBytes, grant.Instruction.ExpectedContentDigest = count, total, digest
	rebindTransferRepair(&grant)
	grant.State = store.RepairPending
	grant.NextRecord, grant.NextOffset, grant.RecordCount, grant.TotalBytes = 0, 0, 0, 0
	if grant.Instruction.RepairID == fixture.repair.Instruction.RepairID {
		t.Fatal("superseding grant derived the same repair identity")
	}
	return grant
}

func (fixture *repairDriverFixture) repairByID(t *testing.T, repository *repairDriverRepository, id [16]byte) store.ResultRepairRecord {
	t.Helper()
	work, err := repository.RecoverWork()
	if err != nil {
		t.Fatal(err)
	}
	for _, repair := range work.Repairs {
		if repair.Instruction.RepairID == id {
			return repair
		}
	}
	t.Fatalf("repair %x missing", id[:4])
	return store.ResultRepairRecord{}
}

// repairState reads one durable grant's state, reporting the zero state when
// the grant is absent.
func (fixture *repairDriverFixture) repairState(repository *repairDriverRepository, id [16]byte) store.RepairState {
	work, err := repository.RecoverWork()
	if err != nil {
		return 0
	}
	for _, repair := range work.Repairs {
		if repair.Instruction.RepairID == id {
			return repair.State
		}
	}
	return 0
}

func (fixture *repairDriverFixture) pullsFor(id [16]byte) int {
	count := 0
	for _, status := range fixture.client.pulled() {
		if status.RepairID == id {
			count++
		}
	}
	return count
}

// TestRepairDriverStopsOnSupersededRevision pins the superseded-grant ruling's
// first rule: a durable grant bound to an assignment revision the installed
// assignment has advanced past is dead and is never driven.
func TestRepairDriverStopsOnSupersededRevision(t *testing.T) {
	fixture := newRepairDriverFixture(t)
	fixture.driver.Schedule(fixture.destinationRepair(t))
	installSupersedingAssignment(t, fixture, fixture.transferFixture.destination)
	fixture.start(t)
	if fixture.advanceUntil(func() bool { return len(fixture.client.pulled()) > 0 }) {
		t.Fatalf("superseded grant was driven: %d pulls", len(fixture.client.pulled()))
	}
}

// TestRepairDriverRetiresSupersededGrantAndDrivesReplacementAcrossRestart
// pins the review's I1 wedge scenario end to end: a grant at revision r1 is
// mid-stream when a reassignment installs r2 and the coordinator's
// replacement grant arrives behind it. The dead grant stops (no further
// pulls, no unbounded retry), the replacement streams to completion, and a
// restarted driver that recovers both grants still never head-of-line blocks
// on the dead one.
func TestRepairDriverRetiresSupersededGrantAndDrivesReplacementAcrossRestart(t *testing.T) {
	fixture := newRepairDriverFixture(t)
	dead := fixture.repair.Instruction.RepairID
	var mu sync.Mutex
	var replacementID [16]byte
	replacement := func() [16]byte {
		mu.Lock()
		defer mu.Unlock()
		return replacementID
	}
	fixture.client.inject = func(call int, status protocol.ResultRepairStatus) (protocol.WorkerMessage, bool, error) {
		if status.RepairID != dead {
			return nil, false, nil
		}
		mu.Lock()
		defer mu.Unlock()
		if replacementID == ([16]byte{}) {
			installed := installSupersedingAssignment(t, fixture, fixture.transferFixture.source, fixture.transferFixture.destination)
			grant := supersedingGrant(t, fixture, installed)
			sourceGrant := grant
			sourceGrant.Role = store.RepairSource
			if err := fixture.transferFixture.source.UpsertRepair(sourceGrant); err != nil {
				t.Error(err)
			}
			if err := fixture.transferFixture.destination.UpsertRepair(grant); err != nil {
				t.Error(err)
			}
			replacementID = grant.Instruction.RepairID
			fixture.driver.Schedule(grant)
		}
		// The source is momentarily unreachable, so the loop re-reads its
		// durable authority before the next exchange.
		return nil, true, errors.New("source unreachable")
	}
	fixture.driver.Schedule(fixture.destinationRepair(t))
	fixture.start(t)
	if !fixture.advanceUntil(func() bool {
		id := replacement()
		return id != ([16]byte{}) && fixture.repairState(fixture.destinationRepository, id) == store.RepairComplete
	}) {
		t.Fatalf("replacement grant never completed behind the superseded grant: pulls=%d", len(fixture.client.pulled()))
	}
	if !fixture.advanceUntil(func() bool {
		return fixture.repairState(fixture.sourceRepository, replacement()) == store.RepairComplete
	}) {
		t.Fatal("source replacement grant never completed")
	}
	if got := fixture.pullsFor(dead); got != 1 {
		t.Fatalf("superseded grant kept being driven: %d pulls", got)
	}
	if fixture.repairByID(t, fixture.destinationRepository, dead).State != store.RepairPending {
		t.Fatalf("driver mutated the superseded grant: %+v", fixture.repairByID(t, fixture.destinationRepository, dead))
	}
	work, err := fixture.destinationRepository.RecoverWork()
	if err != nil {
		t.Fatal(err)
	}
	if len(work.Results) != len(fixture.records) {
		t.Fatalf("installed results=%d want=%d", len(work.Results), len(fixture.records))
	}

	// Restart: recovery schedules the dead grant first and the completed
	// replacement second; the dead grant must not block the loop.
	fixture.stop()
	pullsBefore := len(fixture.client.pulled())
	restarted, err := NewRepairDriver(RepairDriverOptions{Repository: fixture.destinationRepository, Transfer: fixture.destinationOwner, Client: fixture.client, Clock: fixture.clock, RetryInterval: 200 * time.Millisecond, MaxRetryInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	fixture.driver = restarted
	fixture.start(t)
	if !fixture.advanceUntil(func() bool { return len(fixture.client.pulled()) > pullsBefore }) {
		t.Fatal("restarted driver blocked behind the superseded grant")
	}
	if got := fixture.pullsFor(dead); got != 1 {
		t.Fatalf("restarted driver re-drove the superseded grant: %d pulls", got)
	}
	if fixture.pullsFor(replacement()) <= 3 {
		t.Fatalf("restarted driver did not reach the replacement grant: pulls=%d", fixture.pullsFor(replacement()))
	}
}

// TestRepairDriverStopsOnTypedSourceRejectionUntilRegrant pins the ruling's
// second rule: a deterministic source rejection (typed, not a transport
// failure) stops the drive loop instead of retrying forever, so a dead grant
// never head-of-line blocks the serialized driver; the coordinator's explicit
// idempotent re-grant re-arms exactly one fresh attempt, which lets the
// destination-first grant order (destination pulls before the source holds
// its grant) converge once the source grant lands.
func TestRepairDriverStopsOnTypedSourceRejectionUntilRegrant(t *testing.T) {
	fixture := newRepairDriverFixture(t)
	var mu sync.Mutex
	reject := true
	fixture.client.inject = func(call int, status protocol.ResultRepairStatus) (protocol.WorkerMessage, bool, error) {
		mu.Lock()
		defer mu.Unlock()
		if reject {
			return nil, true, ErrTransferUnauthorized
		}
		return nil, false, nil
	}
	fixture.driver.Schedule(fixture.destinationRepair(t))
	fixture.start(t)
	if !fixture.advanceUntil(func() bool { return len(fixture.client.pulled()) >= 1 }) {
		t.Fatal("first pull never issued")
	}
	if fixture.advanceUntil(func() bool { return len(fixture.client.pulled()) >= 2 }) {
		t.Fatalf("typed source rejection was retried: %d pulls", len(fixture.client.pulled()))
	}
	if fixture.destinationRepair(t).State != store.RepairPending {
		t.Fatalf("rejected grant mutated: %+v", fixture.destinationRepair(t))
	}
	// The coordinator re-delivers the identical grant: one fresh attempt.
	fixture.driver.Schedule(fixture.destinationRepair(t))
	if !fixture.advanceUntil(func() bool { return len(fixture.client.pulled()) >= 2 }) {
		t.Fatal("re-grant did not re-arm the rejected grant")
	}
	if fixture.advanceUntil(func() bool { return len(fixture.client.pulled()) >= 3 }) {
		t.Fatalf("re-armed attempt retried the typed rejection: %d pulls", len(fixture.client.pulled()))
	}
	mu.Lock()
	reject = false
	mu.Unlock()
	fixture.driver.Schedule(fixture.destinationRepair(t))
	if !fixture.advanceUntil(func() bool { return fixture.destinationRepair(t).State == store.RepairComplete }) {
		t.Fatalf("grant never completed once the source accepted: %+v", fixture.destinationRepair(t))
	}
}

// acceptRecorder records every accepted +5 connection so a test can count
// sessions and sever them.
type acceptRecorder struct {
	net.Listener
	mu    sync.Mutex
	conns []net.Conn
}

func (recorder *acceptRecorder) Accept() (net.Conn, error) {
	connection, err := recorder.Listener.Accept()
	if err != nil {
		return nil, err
	}
	recorder.mu.Lock()
	recorder.conns = append(recorder.conns, connection)
	recorder.mu.Unlock()
	return connection, nil
}

func (recorder *acceptRecorder) count() int {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return len(recorder.conns)
}

func (recorder *acceptRecorder) severAll() {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	for _, connection := range recorder.conns {
		_ = connection.Close()
	}
}

// TestRepairSourceClientReusesSessionAcrossPullsAndRedialsAfterFailure pins
// the repair-session-reuse ruling: every pull of one grant travels over one
// authenticated +5 session to the source incarnation (one dial, one
// handshake, one replay identity per record — the defect-#9 budget), a later
// pull reuses it, and a failed exchange drops the session so the next pull
// re-establishes exactly one.
func TestRepairSourceClientReusesSessionAcrossPullsAndRedialsAfterFailure(t *testing.T) {
	fixture := newRepairDriverFixture(t)
	specification, ok := config.LookupService(config.ServiceCraneWorker)
	if !ok {
		t.Fatal("crane worker service specification missing")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if port <= int(specification.Offset) {
		t.Fatalf("ephemeral port %d below service offset %d", port, specification.Offset)
	}
	accepted := &acceptRecorder{Listener: listener}
	authenticator := wire.NewHMACAuthenticator([]byte("repair-session-reuse-test-secret"))
	members := &controlTestMembership{view: membership.View{Revision: 1, Members: []swim.Member{
		{NodeID: fixture.replica.PrimaryNodeID, Host: "127.0.0.1", BasePort: uint16(port) - specification.Offset, Incarnation: 1, Status: swim.Alive},
		{NodeID: fixture.replica.SecondaryNodeID, Host: "127.0.0.1", BasePort: 9200, Incarnation: 1, Status: swim.Alive},
	}}}
	controlRepository := &repairControlRepository{repository: fixture.transferFixture.source, node: fixture.replica.PrimaryNodeID, epoch: fixture.replica.PrimaryEpoch}
	sourceOwner, err := NewControlOwner(ControlOptions{Config: config.NodeConfig{NodeID: fixture.replica.PrimaryNodeID, Crane: config.DefaultCraneConfig(), Timing: config.DefaultTimingConfig()}, ClusterID: [16]byte{7}, Repository: controlRepository, Engine: &controlNoopEngine{}, Transfer: fixture.sourceOwner, Gate: admission.NewGate(), Membership: members, Clock: fixture.clock})
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- serveRepairControlSessions(context.Background(), accepted, sourceOwner, authenticator, [16]byte{7}, fixture.replica.PrimaryNodeID, fixture.replica.PrimaryEpoch, fixture.epoch, fixture.clock)
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-serveDone
	})
	var dialMu sync.Mutex
	dials := 0
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		dialMu.Lock()
		dials++
		dialMu.Unlock()
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}
	dialed := func() int {
		dialMu.Lock()
		defer dialMu.Unlock()
		return dials
	}
	client := newDialRepairSourceClient(dialRepairSourceClientOptions{ClusterID: [16]byte{7}, Authenticator: authenticator, Clock: fixture.clock, Membership: members, Repository: fixture.destinationRepository, Timeout: 2 * time.Second, Dial: dial})
	t.Cleanup(func() {
		if closer, ok := any(client).(interface{ Close() }); ok {
			closer.Close()
		}
	})
	driver, err := NewRepairDriver(RepairDriverOptions{Repository: fixture.destinationRepository, Transfer: fixture.destinationOwner, Client: client, Clock: fixture.clock, RetryInterval: 200 * time.Millisecond, MaxRetryInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	fixture.driver = driver
	fixture.driver.Schedule(fixture.destinationRepair(t))
	fixture.start(t)
	if !fixture.advanceUntil(func() bool { return fixture.destinationRepair(t).State == store.RepairComplete }) {
		t.Fatalf("wire-driven grant never completed: %+v", fixture.destinationRepair(t))
	}
	if !fixture.advanceUntil(func() bool { return fixture.sourceRepair(t).State == store.RepairComplete }) {
		t.Fatalf("wire source grant never completed: %+v", fixture.sourceRepair(t))
	}
	if got := dialed(); got != 1 || accepted.count() != 1 {
		t.Fatalf("three pulls used %d dials and %d sessions, want one authenticated session", got, accepted.count())
	}
	source, sourceEpoch := fixture.repair.Instruction.SourceNodeID, fixture.repair.Instruction.SourceWorkerEpoch
	status := repairStatus(fixture.destinationRepair(t))
	if _, err := client.PullRepair(context.Background(), source, sourceEpoch, status); err != nil {
		t.Fatalf("pull over the cached session: %v", err)
	}
	if got := dialed(); got != 1 {
		t.Fatalf("later pull dialed again: %d dials", got)
	}
	accepted.severAll()
	if _, err := client.PullRepair(context.Background(), source, sourceEpoch, status); !errors.Is(err, ErrRepairSourceUnavailable) {
		t.Fatalf("severed session pull err=%v want %v", err, ErrRepairSourceUnavailable)
	}
	if _, err := client.PullRepair(context.Background(), source, sourceEpoch, status); err != nil {
		t.Fatalf("pull after the dropped session: %v", err)
	}
	if got := dialed(); got != 2 || accepted.count() != 2 {
		t.Fatalf("recovery used %d dials and %d sessions, want exactly one new session", got, accepted.count())
	}
}
