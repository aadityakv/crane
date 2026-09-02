package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/aaditya/cs425mp3/internal/crane/admission"
	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
	"github.com/aaditya/cs425mp3/internal/crane/store"
)

// partnerChangedAssignment derives revision+1 of the fixture's installed
// assignment in which the local worker's replica partner for the sink is
// replaced by node 9 at a fresh epoch. When the replaced endpoint is the
// primary, the sink task token moves with it (attempt bumped).
func partnerChangedAssignment(t *testing.T, fixture workerTestFixture, sink model.TaskID, scheduling model.SchedulingState) store.InstalledAssignment {
	t.Helper()
	prior := fixture.assignment.Assignment
	replicas := append([]model.ResultReplicaSet(nil), prior.ResultReplicas...)
	tasks := append([]model.AssignmentToken(nil), prior.Tasks...)
	replaced, replacedEpoch := uint16(9), model.WorkerEpoch{9}
	for index := range replicas {
		if replicas[index].SinkTask != sink {
			continue
		}
		if replicas[index].PrimaryNodeID == fixture.localNode && replicas[index].PrimaryEpoch == fixture.localEpoch {
			replicas[index].SecondaryNodeID, replicas[index].SecondaryEpoch = replaced, replacedEpoch
		} else {
			old := replicas[index].PrimaryNodeID
			replicas[index].PrimaryNodeID, replicas[index].PrimaryEpoch = replaced, replacedEpoch
			for taskIndex := range tasks {
				if tasks[taskIndex].Task == sink && tasks[taskIndex].WorkerID == old {
					tasks[taskIndex].WorkerID, tasks[taskIndex].WorkerEpoch, tasks[taskIndex].Attempt = replaced, replacedEpoch, tasks[taskIndex].Attempt+1
				}
			}
		}
	}
	for index := range tasks {
		tasks[index].AssignmentRevision = prior.Revision + 1
	}
	set, err := model.NewAssignmentSet(prior.JobID, prior.Revision+1, tasks, replicas, fixture.topology)
	if err != nil {
		t.Fatal(err)
	}
	installed := fixture.assignment
	installed.Assignment = set
	installed.SchedulingState = scheduling
	installed.JobControlRevision++
	return installed
}

func installAssignment(repository *fakeRepository, installed store.InstalledAssignment) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	repository.assignments[installed.Assignment.JobID] = installed
	repository.work.Assignments = []store.InstalledAssignment{installed}
	repository.work.Fence = installed.CoordinatorEpoch
}

func storedResults(repository *fakeRepository) []store.StoredResult {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	return append([]store.StoredResult(nil), repository.results...)
}

func expectedEnvelope(installed store.InstalledAssignment, sink model.TaskID, role model.ResultReplicaRole) model.ResultCopyProvenance {
	replica, _ := findResultReplica(installed.Assignment, sink)
	return model.ResultCopyProvenance{AssignmentRevision: installed.Assignment.Revision, AssignmentDigest: installed.Assignment.Digest, ReplicaSet: replica, DestinationRole: role, CoordinatorEpoch: installed.CoordinatorEpoch}
}

func startResultEngine(t *testing.T, repository *fakeRepository, epoch model.CoordinatorEpoch, replicator *fakeResultReplicator) (*Engine, context.CancelFunc, <-chan error) {
	t.Helper()
	gate := admission.NewGate()
	if err := gate.Open(epoch); err != nil {
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
	return engine, cancel, done
}

// TestRetainedResultReReplicatesToReplacementPartner pins the Task 24 defect
// #4 ruling's holder half. A Completed result retained by the current primary
// under the superseded replica pair is re-replicated to the replacement
// partner under the current envelope — driven by the partner-changing install
// (live) and by recovery — under Running and under Draining; on the durable
// partner receipt the local copy re-binds to the current pair. Nothing moves
// while the retained envelope is current.
func TestRetainedResultReReplicatesToReplacementPartner(t *testing.T) {
	for _, test := range []struct {
		name       string
		recovered  bool
		scheduling model.SchedulingState
	}{
		{name: "install running", scheduling: model.Running},
		{name: "recovery running", recovered: true, scheduling: model.Running},
		{name: "install draining", scheduling: model.Draining},
		{name: "recovery draining", recovered: true, scheduling: model.Draining},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture, sink, replica := workerFixtureWithLocalPrimarySink(t)
			repository := newFakeRepository(fixture)
			record, retained := fixture.result(t, sink, replica, 1, model.PrimaryReplica)
			repository.results = []store.StoredResult{{Record: record, Provenance: retained}}
			repository.work.Results = append([]store.StoredResult(nil), repository.results...)
			current := partnerChangedAssignment(t, fixture, sink.Task, test.scheduling)
			if test.recovered {
				installAssignment(repository, current)
			}
			replicator := &fakeResultReplicator{calls: make(chan resultReplicationCall, 1)}
			engine, cancel, done := startResultEngine(t, repository, fixture.epoch, replicator)
			defer func() { cancel(); <-done }()
			if !test.recovered {
				select {
				case call := <-replicator.calls:
					t.Fatalf("current-envelope result replicated: %+v", call.provenance)
				default:
				}
				installAssignment(repository, current)
				if err := engine.ReconcileAssignment(context.Background(), current.Assignment.JobID); err != nil {
					t.Fatal(err)
				}
			}
			call := <-replicator.calls
			want := expectedEnvelope(current, sink.Task, model.SecondaryReplica)
			if call.provenance != want || call.record.Checksum != record.Checksum {
				t.Fatalf("re-replication provenance=%+v want=%+v", call.provenance, want)
			}
			replicator.ack(call)
			local := expectedEnvelope(current, sink.Task, model.PrimaryReplica)
			waitFor(t, func() bool {
				results := storedResults(repository)
				return len(results) == 1 && results[0].Provenance == local
			})
			if err := engine.ReconcileAssignment(context.Background(), current.Assignment.JobID); err != nil {
				t.Fatal(err)
			}
			select {
			case call := <-replicator.calls:
				t.Fatalf("rebound result replicated again: %+v", call.provenance)
			default:
			}
		})
	}
}

// TestRetainedResultSecondaryHolderReReplicatesToReplacementPrimary pins the
// symmetric direction: the surviving secondary re-replicates its retained
// copies to the replacement primary under the current envelope and re-binds
// its own copy under the secondary role — both when the copies are recovered
// at start and when they were received live through the transfer owner (so
// the engine never owned them) before the partner-changing install.
func TestRetainedResultSecondaryHolderReReplicatesToReplacementPrimary(t *testing.T) {
	for _, live := range []bool{false, true} {
		t.Run(map[bool]string{false: "recovered", true: "received live"}[live], func(t *testing.T) {
			testSecondaryHolderReReplicates(t, live)
		})
	}
}

func testSecondaryHolderReReplicates(t *testing.T, live bool) {
	fixture, sink, replica := workerFixtureWithLocalPrimarySink(t)
	fixture.localNode, fixture.localEpoch = replica.SecondaryNodeID, replica.SecondaryEpoch
	repository := newFakeRepository(fixture)
	record, retained := fixture.result(t, sink, replica, 1, model.SecondaryReplica)
	seed := func() {
		repository.mu.Lock()
		defer repository.mu.Unlock()
		repository.results = []store.StoredResult{{Record: record, Provenance: retained}}
		repository.work.Results = append([]store.StoredResult(nil), repository.results...)
	}
	current := partnerChangedAssignment(t, fixture, sink.Task, model.Running)
	if !live {
		seed()
		installAssignment(repository, current)
	}
	replicator := &fakeResultReplicator{calls: make(chan resultReplicationCall, 1)}
	replicator.mutateReceipt = func(receipt *ResultReplicationReceipt) {
		replica, _ := findResultReplica(current.Assignment, sink.Task)
		receipt.DestinationNodeID, receipt.DestinationWorkerEpoch = replica.PrimaryNodeID, replica.PrimaryEpoch
	}
	engine, cancel, done := startResultEngine(t, repository, fixture.epoch, replicator)
	defer func() { cancel(); <-done }()
	if live {
		seed()
		installAssignment(repository, current)
		if err := engine.ReconcileAssignment(context.Background(), current.Assignment.JobID); err != nil {
			t.Fatal(err)
		}
	}
	call := <-replicator.calls
	if want := expectedEnvelope(current, sink.Task, model.PrimaryReplica); call.provenance != want {
		t.Fatalf("re-replication provenance=%+v want=%+v", call.provenance, want)
	}
	replicator.ack(call)
	local := expectedEnvelope(current, sink.Task, model.SecondaryReplica)
	waitFor(t, func() bool {
		results := storedResults(repository)
		return len(results) == 1 && results[0].Provenance == local
	})
}

// TestRetainedResultCoveredByGrantStaysWithGrant pins the ruling's split:
// records a durable current-epoch bilateral repair grant naming this worker
// covers are re-established by the grant, never re-replicated by the holder;
// records above the grant's committed vector are.
func TestRetainedResultCoveredByGrantStaysWithGrant(t *testing.T) {
	fixture, sink, replica := workerFixtureWithLocalPrimarySink(t)
	repository := newFakeRepository(fixture)
	covered, retained := fixture.result(t, sink, replica, 1, model.PrimaryReplica)
	above, _ := fixture.result(t, sink, replica, 2, model.PrimaryReplica)
	repository.results = []store.StoredResult{{Record: covered, Provenance: retained}, {Record: above, Provenance: retained}}
	repository.work.Results = append([]store.StoredResult(nil), repository.results...)
	current := partnerChangedAssignment(t, fixture, sink.Task, model.Running)
	installAssignment(repository, current)
	currentReplica, _ := findResultReplica(current.Assignment, sink.Task)
	vector := []model.SourceCheckpoint{{Source: covered.TupleID.SourceTask, Watermark: covered.TupleID.SourceSequence}}
	instruction := model.RepairResultPartitionDefinition{CoordinatorEpoch: fixture.epoch, JobID: current.Assignment.JobID, AssignmentRevision: current.Assignment.Revision, AssignmentDigest: current.Assignment.Digest, SourceNodeID: fixture.localNode, SourceWorkerEpoch: fixture.localEpoch, DestinationNodeID: currentReplica.SecondaryNodeID, DestinationWorkerEpoch: currentReplica.SecondaryEpoch, SinkTask: sink.Task, SpecificationHash: fixture.topology.Digest(), Checkpoints: vector, CheckpointDigest: model.CheckpointVectorDigest(vector), ExpectedRecordCount: 1}
	repository.mu.Lock()
	repository.work.Repairs = []store.ResultRepairRecord{{Instruction: instruction, Role: store.RepairSource, State: store.RepairPending}}
	repository.mu.Unlock()
	replicator := &fakeResultReplicator{calls: make(chan resultReplicationCall, 2)}
	_, cancel, done := startResultEngine(t, repository, fixture.epoch, replicator)
	defer func() { cancel(); <-done }()
	call := <-replicator.calls
	if call.record.TupleID != above.TupleID {
		t.Fatalf("re-replicated %v, want only the record above the grant vector %v", call.record.TupleID, above.TupleID)
	}
	replicator.ack(call)
	local := expectedEnvelope(current, sink.Task, model.PrimaryReplica)
	waitFor(t, func() bool {
		for _, stored := range storedResults(repository) {
			if stored.Record.TupleID == above.TupleID && stored.Provenance == local {
				return true
			}
		}
		return false
	})
	select {
	case call := <-replicator.calls:
		t.Fatalf("grant-covered record re-replicated: %v", call.record.TupleID)
	default:
	}
	for _, stored := range storedResults(repository) {
		if stored.Record.TupleID == covered.TupleID && stored.Provenance != retained {
			t.Fatalf("grant-covered copy re-bound by the holder: %+v", stored.Provenance)
		}
	}
}

// TestRetainedResultNeverMovesWhenNotCurrentMember pins the fail-closed side:
// a worker that is no longer an endpoint of the current replica set, or holds
// the job Closed, never re-replicates its retained copies.
func TestRetainedResultNeverMovesWhenNotCurrentMember(t *testing.T) {
	for _, test := range []struct {
		name  string
		shape func(*testing.T, workerTestFixture, model.AssignmentToken) store.InstalledAssignment
	}{
		{name: "replaced holder", shape: func(t *testing.T, fixture workerTestFixture, sink model.AssignmentToken) store.InstalledAssignment {
			installed := partnerChangedAssignment(t, fixture, sink.Task, model.Running)
			set := installed.Assignment
			replicas := append([]model.ResultReplicaSet(nil), set.ResultReplicas...)
			tasks := append([]model.AssignmentToken(nil), set.Tasks...)
			for index := range replicas {
				if replicas[index].SinkTask == sink.Task {
					replicas[index].PrimaryNodeID, replicas[index].PrimaryEpoch = 8, model.WorkerEpoch{8}
				}
			}
			for index := range tasks {
				if tasks[index].Task == sink.Task {
					tasks[index].WorkerID, tasks[index].WorkerEpoch, tasks[index].Attempt = 8, model.WorkerEpoch{8}, tasks[index].Attempt+1
				}
			}
			replaced, err := model.NewAssignmentSet(set.JobID, set.Revision, tasks, replicas, fixture.topology)
			if err != nil {
				t.Fatal(err)
			}
			installed.Assignment = replaced
			return installed
		}},
		{name: "closed re-fence", shape: func(t *testing.T, fixture workerTestFixture, sink model.AssignmentToken) store.InstalledAssignment {
			return partnerChangedAssignment(t, fixture, sink.Task, model.Closed)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture, sink, replica := workerFixtureWithLocalPrimarySink(t)
			repository := newFakeRepository(fixture)
			record, retained := fixture.result(t, sink, replica, 1, model.PrimaryReplica)
			repository.results = []store.StoredResult{{Record: record, Provenance: retained}}
			repository.work.Results = append([]store.StoredResult(nil), repository.results...)
			installAssignment(repository, test.shape(t, fixture, sink))
			replicator := &fakeResultReplicator{calls: make(chan resultReplicationCall, 1)}
			engine, cancel, done := startResultEngine(t, repository, fixture.epoch, replicator)
			defer func() { cancel(); <-done }()
			if err := engine.ReconcileAssignment(context.Background(), fixture.assignment.Assignment.JobID); err != nil {
				t.Fatal(err)
			}
			select {
			case call := <-replicator.calls:
				t.Fatalf("non-member re-replicated: %+v", call.provenance)
			default:
			}
			if results := storedResults(repository); len(results) != 1 || results[0].Provenance != retained {
				t.Fatalf("retained copy mutated: %+v", results)
			}
		})
	}
}

// TestProcessedParentUnderSupersededFenceCompletesAfterReplication pins the
// defect #5 ruling applied to sink custody: a Processed sink delivery and its
// result retained under a superseded fence (identical assignment) replicate
// under the current fence, the local copy re-binds, and the parent completes.
func TestProcessedParentUnderSupersededFenceCompletesAfterReplication(t *testing.T) {
	fixture, sink, replica := workerFixtureWithLocalPrimarySink(t)
	repository := newFakeRepository(fixture)
	delivery := fixture.sinkDelivery(t, sink, 1)
	delivery.State = store.Processed
	delivery.Outputs = []model.Tuple{cloneTuple(delivery.Tuple)}
	record, retained := fixture.result(t, sink, replica, 1, model.PrimaryReplica)
	repository.work.Deliveries = []store.DeliveryRecord{delivery}
	repository.deliveries[delivery.ID] = delivery
	repository.results = []store.StoredResult{{Record: record, Provenance: retained}}
	repository.work.Results = append([]store.StoredResult(nil), repository.results...)
	newer := readoptFixture(t, repository, fixture)
	replicator := &fakeResultReplicator{calls: make(chan resultReplicationCall, 1)}
	_, cancel, done := startResultEngine(t, repository, newer, replicator)
	defer func() { cancel(); <-done }()
	call := <-replicator.calls
	want := retained
	want.DestinationRole, want.CoordinatorEpoch = model.SecondaryReplica, newer
	if call.provenance != want {
		t.Fatalf("replication provenance=%+v want=%+v", call.provenance, want)
	}
	replicator.ack(call)
	local := retained
	local.CoordinatorEpoch = newer
	waitFor(t, func() bool {
		repository.mu.Lock()
		defer repository.mu.Unlock()
		return repository.deliveries[delivery.ID].State == store.Completed && len(repository.results) == 1 && repository.results[0].Provenance == local
	})
}

// TestReceiveRetainedReReplicationUnderDrainingIsIdempotentAndFailClosed pins
// the receiver half: the replacement partner accepts the re-replicated record
// under the current envelope while the job drains, a duplicate is idempotent,
// and a non-current sender, a stale envelope and a checksum mismatch are
// rejected without mutation.
func TestReceiveRetainedReReplicationUnderDrainingIsIdempotentAndFailClosed(t *testing.T) {
	fixture := newTransferFixture(t)
	draining := fixture.assignment
	draining.SchedulingState = model.Draining
	fixture.destination.assignments[draining.Assignment.JobID] = draining
	fixture.destination.work.Assignments = []store.InstalledAssignment{draining}
	owner, err := NewTransferOwner(TransferOptions{Repository: fixture.destination})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	chunk := fixture.normalChunk(t)
	ack, err := owner.ReceiveResultRecord(ctx, fixture.sourcePeer(), chunk)
	if err != nil || ack.Checksum != model.ResultRecordStreamChecksum(chunk.Record) {
		t.Fatalf("draining receive: ack=%+v err=%v", ack, err)
	}
	duplicate, err := owner.ReceiveResultRecord(ctx, fixture.sourcePeer(), chunk)
	if err != nil || duplicate != ack {
		t.Fatalf("duplicate receive: ack=%+v err=%v", duplicate, err)
	}
	if work, _ := fixture.destination.RecoverWork(); len(work.Results) != 1 || work.Results[0].Provenance != chunk.Provenance {
		t.Fatalf("destination results=%+v", work.Results)
	}

	replaced := fixture.sourcePeer()
	replaced.WorkerEpoch = model.WorkerEpoch{0xEE}
	if _, err := owner.ReceiveResultRecord(ctx, replaced, chunk); !errors.Is(err, ErrTransferUnauthorized) {
		t.Fatalf("non-current sender: %v", err)
	}
	stale := chunk
	stale.Provenance.AssignmentDigest[0] ^= 0xFF
	stale.Transfer.TransferID, _ = DeriveResultRecordTransferID(TransferNormalReplication, stale.Record, stale.Provenance, stale.DestinationNodeID, stale.DestinationWorkerEpoch, [16]byte{}, [32]byte{})
	if _, err := owner.ReceiveResultRecord(ctx, fixture.sourcePeer(), stale); !errors.Is(err, ErrTransferUnauthorized) {
		t.Fatalf("superseded envelope: %v", err)
	}
	corrupt := fixture.normalChunk(t)
	corrupt.Record.Checksum[0] ^= 0xFF
	corrupt.Transfer.Checksum = model.ResultRecordStreamChecksum(corrupt.Record)
	corrupt.Transfer.TransferID, _ = DeriveResultRecordTransferID(TransferNormalReplication, corrupt.Record, corrupt.Provenance, corrupt.DestinationNodeID, corrupt.DestinationWorkerEpoch, [16]byte{}, [32]byte{})
	if _, err := owner.ReceiveResultRecord(ctx, fixture.sourcePeer(), corrupt); err == nil {
		t.Fatal("checksum mismatch was accepted")
	}
	closed := draining
	closed.SchedulingState = model.Closed
	fixture.destination.assignments[closed.Assignment.JobID] = closed
	fixture.destination.work.Assignments = []store.InstalledAssignment{closed}
	if _, err := owner.ReceiveResultRecord(ctx, fixture.sourcePeer(), chunk); !errors.Is(err, ErrTransferUnauthorized) {
		t.Fatalf("closed re-fence: %v", err)
	}
	if work, _ := fixture.destination.RecoverWork(); len(work.Results) != 1 {
		t.Fatalf("rejected receipts mutated the destination: %+v", work.Results)
	}
	_ = protocol.ResultRecordAck{}
}
