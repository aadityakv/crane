package worker

import (
	"context"
	"crypto/sha256"
	"errors"
	"runtime"
	"sync"
	"testing"

	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
	"github.com/aaditya/cs425mp3/internal/crane/store"
)

func TestTransferNormalReplicationPersistsBeforeExactAck(t *testing.T) {
	fixture := newTransferFixture(t)
	owner := mustTransferOwner(t, fixture.destination)
	chunk := fixture.normalChunk(t)

	ack, err := owner.ReceiveResultRecord(context.Background(), fixture.sourcePeer(), chunk)
	if err != nil {
		t.Fatal(err)
	}
	if len(fixture.destination.results) != 1 || fixture.destination.log[0] != "result" {
		t.Fatalf("result was not durable before ACK: results=%d log=%v", len(fixture.destination.results), fixture.destination.log)
	}
	if err := protocol.ValidateResultRecordAckCorrelation(chunk, ack); err != nil {
		t.Fatalf("ACK does not bind exact durable record: %v", err)
	}
	chunk.Record.Value[0] ^= 0xff
	if fixture.destination.results[0].Record.Value[0] == chunk.Record.Value[0] {
		t.Fatal("caller mutated the durable result buffer")
	}
}

func TestTransferRejectsPartialWrongIdentityAndAuthorityBeforeMutation(t *testing.T) {
	fixture := newTransferFixture(t)
	tests := map[string]func(*protocol.ResultRecordChunk, *TransferPeer){
		"partial record": func(chunk *protocol.ResultRecordChunk, _ *TransferPeer) {
			chunk.Transfer.Data = append([]byte(nil), chunk.Transfer.Data[:len(chunk.Transfer.Data)-1]...)
			chunk.Transfer.Final = false
		},
		"wrong transfer ID":  func(chunk *protocol.ResultRecordChunk, _ *TransferPeer) { chunk.Transfer.TransferID[0] ^= 1 },
		"wrong source epoch": func(_ *protocol.ResultRecordChunk, peer *TransferPeer) { peer.WorkerEpoch[0] ^= 1 },
		"coordinator role":   func(_ *protocol.ResultRecordChunk, peer *TransferPeer) { peer.Role = TransferCoordinatorCommand },
		"unbound repair": func(chunk *protocol.ResultRecordChunk, _ *TransferPeer) {
			chunk.RepairID = [16]byte{9}
			chunk.RepairInstructionDigest = [32]byte{9}
		},
		"stale fence": func(chunk *protocol.ResultRecordChunk, _ *TransferPeer) { chunk.Provenance.CoordinatorEpoch.Term++ },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			repository := fixture.destination.clone()
			owner := mustTransferOwner(t, repository)
			chunk, peer := fixture.normalChunk(t), fixture.sourcePeer()
			mutate(&chunk, &peer)
			if _, err := owner.ReceiveResultRecord(context.Background(), peer, chunk); err == nil {
				t.Fatal("invalid transfer accepted")
			}
			if len(repository.results) != 0 || len(repository.log) != 0 {
				t.Fatalf("invalid transfer mutated repository: %+v", repository.log)
			}
		})
	}
}

func TestTransferIDBindsRoleRecordProvenanceDestinationAndFence(t *testing.T) {
	fixture := newTransferFixture(t)
	chunk := fixture.normalChunk(t)
	base, err := DeriveResultRecordTransferID(TransferNormalReplication, chunk.Record, chunk.Provenance, chunk.DestinationNodeID, chunk.DestinationWorkerEpoch, [16]byte{}, [32]byte{})
	if err != nil || base == ([16]byte{}) {
		t.Fatalf("derive base ID: id=%x err=%v", base, err)
	}
	mutated := chunk
	mutated.Provenance.CoordinatorEpoch.Term++
	changedFence, _ := DeriveResultRecordTransferID(TransferNormalReplication, mutated.Record, mutated.Provenance, mutated.DestinationNodeID, mutated.DestinationWorkerEpoch, [16]byte{}, [32]byte{})
	changedRole, _ := DeriveResultRecordTransferID(TransferHistoricalRepair, chunk.Record, chunk.Provenance, chunk.DestinationNodeID, chunk.DestinationWorkerEpoch, fixture.repair.Instruction.RepairID, fixture.repair.InstructionDigest)
	changedDestination, _ := DeriveResultRecordTransferID(TransferNormalReplication, chunk.Record, chunk.Provenance, chunk.DestinationNodeID+1, chunk.DestinationWorkerEpoch, [16]byte{}, [32]byte{})
	if base == changedFence || base == changedRole || base == changedDestination {
		t.Fatal("transfer ID omitted an authority field")
	}
}

func TestTransferHistoricalRepairPersistsRecordThenProgressAndResumesWholeRecord(t *testing.T) {
	fixture := newTransferFixture(t)
	fixture.installRepair(t)
	owner := mustTransferOwner(t, fixture.destination)
	chunk := fixture.repairChunk(t, fixture.records[0])

	ack, err := owner.ReceiveResultRecord(context.Background(), fixture.repairSourcePeer(), chunk)
	if err != nil {
		t.Fatal(err)
	}
	if err := protocol.ValidateResultRecordAckCorrelation(chunk, ack); err != nil {
		t.Fatal(err)
	}
	if want := []string{"result", "repair"}; !equalStrings(fixture.destination.log, want) {
		t.Fatalf("durability order=%v want=%v", fixture.destination.log, want)
	}
	progress := fixture.destination.repairs[0]
	stream, _ := model.MarshalResultRecord(fixture.records[0])
	if progress.State != store.RepairStreaming || progress.NextRecord != 1 || progress.NextOffset != uint64(len(stream)) {
		t.Fatalf("durable progress=%+v", progress)
	}

	// An ACK lost after both writes causes an exact whole-record retransmit. A
	// reconstructed owner must ACK it idempotently without advancing twice.
	restarted := mustTransferOwner(t, fixture.destination)
	if _, err = restarted.ReceiveResultRecord(context.Background(), fixture.repairSourcePeer(), chunk); err != nil {
		t.Fatalf("exact recovered duplicate: %v", err)
	}
	if got := fixture.destination.repairs[0]; got.NextRecord != 1 || got.NextOffset != uint64(len(stream)) {
		t.Fatalf("duplicate advanced repair twice: %+v", got)
	}
	changed := chunk
	changed.Transfer.Checksum[0] ^= 1
	if _, err = restarted.ReceiveResultRecord(context.Background(), fixture.repairSourcePeer(), changed); err == nil {
		t.Fatal("changed overlap accepted")
	}
}

func TestTransferRepairRecoversResultDurableBeforeFirstProgress(t *testing.T) {
	fixture := newTransferFixture(t)
	fixture.installRepair(t)
	owner := mustTransferOwner(t, fixture.destination)
	chunk := fixture.repairChunk(t, fixture.records[0])
	fixture.destination.repairErrOnce = errors.New("injected repair sync failure")
	if _, err := owner.ReceiveResultRecord(context.Background(), fixture.repairSourcePeer(), chunk); err == nil {
		t.Fatal("injected progress failure was hidden")
	}
	if len(fixture.destination.results) != 1 || fixture.destination.repairs[0].NextRecord != 0 {
		t.Fatalf("crash window not reproduced: results=%d repair=%+v", len(fixture.destination.results), fixture.destination.repairs[0])
	}
	restarted := mustTransferOwner(t, fixture.destination)
	if _, err := restarted.ReceiveResultRecord(context.Background(), fixture.repairSourcePeer(), chunk); err != nil {
		t.Fatalf("recovered first record did not advance progress: %v", err)
	}
	if fixture.destination.repairs[0].NextRecord != 1 {
		t.Fatalf("recovered progress=%+v", fixture.destination.repairs[0])
	}
}

func TestTransferRepairRecoversResultDurableBeforeLaterProgress(t *testing.T) {
	fixture := newTransferFixture(t)
	fixture.installRepair(t)
	owner := mustTransferOwner(t, fixture.destination)
	first := fixture.repairChunk(t, fixture.records[0])
	if _, err := owner.ReceiveResultRecord(context.Background(), fixture.repairSourcePeer(), first); err != nil {
		t.Fatal(err)
	}
	second := fixture.repairChunk(t, fixture.records[1])
	fixture.destination.repairErrOnce = errors.New("injected later progress failure")
	if _, err := owner.ReceiveResultRecord(context.Background(), fixture.repairSourcePeer(), second); err == nil {
		t.Fatal("injected later progress failure was hidden")
	}
	if len(fixture.destination.results) != 2 || fixture.destination.repairs[0].NextRecord != 1 {
		t.Fatalf("later crash window not reproduced: results=%d repair=%+v", len(fixture.destination.results), fixture.destination.repairs[0])
	}
	restarted := mustTransferOwner(t, fixture.destination)
	if _, err := restarted.ReceiveResultRecord(context.Background(), fixture.repairSourcePeer(), second); err != nil {
		t.Fatalf("recovered later record did not advance progress: %v", err)
	}
	if fixture.destination.repairs[0].State != store.RepairComplete || fixture.destination.repairs[0].NextRecord != 2 {
		t.Fatalf("recovered terminal progress=%+v", fixture.destination.repairs[0])
	}
}

func TestTransferRepairSourceSortsInventoryAndAdvancesOnlyAfterExactAck(t *testing.T) {
	fixture := newTransferFixture(t)
	fixture.installSourceRepair(t)
	owner := mustTransferOwner(t, fixture.source)
	destination := fixture.destinationRepairStatus()

	chunk, complete, err := owner.NextRepairRecord(context.Background(), fixture.destinationPeer(), fixture.repair.Instruction.RepairID, destination)
	if err != nil || complete {
		t.Fatalf("next repair record: complete=%t err=%v", complete, err)
	}
	if chunk.Record.TupleID != fixture.records[0].TupleID {
		t.Fatalf("source did not choose canonical first record: got=%+v want=%+v", chunk.Record.TupleID, fixture.records[0].TupleID)
	}
	if fixture.source.repairs[0].NextRecord != 0 {
		t.Fatal("source advanced before durable destination ACK")
	}
	ack := fixture.ack(chunk)
	if err = owner.AcknowledgeRepairRecord(context.Background(), fixture.destinationPeer(), chunk, ack); err != nil {
		t.Fatal(err)
	}
	if fixture.source.repairs[0].NextRecord != 1 || fixture.source.log[len(fixture.source.log)-1] != "repair" {
		t.Fatalf("source progress not durably advanced: %+v log=%v", fixture.source.repairs[0], fixture.source.log)
	}
	restarted := mustTransferOwner(t, fixture.source)
	next, complete, err := restarted.NextRepairRecord(context.Background(), fixture.destinationPeer(), fixture.repair.Instruction.RepairID, destination)
	if err != nil || complete || next.Record.TupleID != fixture.records[1].TupleID {
		t.Fatalf("resume next whole record: got=%+v complete=%t err=%v", next.Record.TupleID, complete, err)
	}
}

func TestTransferRepairSourceAllowsRetainedOldEpochProvenanceButNotWrongNode(t *testing.T) {
	fixture := newTransferFixture(t)
	fixture.installSourceRepair(t)
	old := model.WorkerEpoch{99}
	for index := range fixture.source.results {
		fixture.source.results[index].Provenance.ReplicaSet.PrimaryEpoch = old
		fixture.source.work.Results[index].Provenance.ReplicaSet.PrimaryEpoch = old
	}
	owner := mustTransferOwner(t, fixture.source)
	if _, _, err := owner.NextRepairRecord(context.Background(), fixture.destinationPeer(), fixture.repair.Instruction.RepairID, fixture.destinationRepairStatus()); err != nil {
		t.Fatalf("retained old provenance rejected under current source grant: %v", err)
	}
	fixture.source.results[0].Provenance.ReplicaSet.PrimaryNodeID = 99
	fixture.source.work.Results[0].Provenance.ReplicaSet.PrimaryNodeID = 99
	if _, _, err := owner.NextRepairRecord(context.Background(), fixture.destinationPeer(), fixture.repair.Instruction.RepairID, fixture.destinationRepairStatus()); !errors.Is(err, ErrTransferUnauthorized) {
		t.Fatalf("wrong retained source node err=%v", err)
	}
}

func TestTransferRecoveredRepairExposedButOldFenceNeverResumes(t *testing.T) {
	fixture := newTransferFixture(t)
	fixture.installSourceRepair(t)
	owner := mustTransferOwner(t, fixture.source)
	got, err := owner.RecoveredRepairs()
	if err != nil || len(got) != 1 || got[0].Instruction.RepairID != fixture.repair.Instruction.RepairID {
		t.Fatalf("recovered repairs=%+v", got)
	}
	got[0].Instruction.Checkpoints = append(got[0].Instruction.Checkpoints, model.SourceCheckpoint{Source: fixture.records[0].TupleID.SourceTask, Watermark: 1})
	again, err := owner.RecoveredRepairs()
	if err != nil || len(again[0].Instruction.Checkpoints) != 0 {
		t.Fatal("caller mutated recovered repair ownership")
	}
	fixture.source.recoverErr = errors.New("injected recovery failure")
	if _, err = owner.RecoveredRepairs(); !errors.Is(err, fixture.source.recoverErr) {
		t.Fatalf("recovery error=%v", err)
	}
	fixture.source.recoverErr = nil
	fixture.source.work.Fence.Term++
	if _, _, err := owner.NextRepairRecord(context.Background(), fixture.destinationPeer(), fixture.repair.Instruction.RepairID, fixture.destinationRepairStatus()); !errors.Is(err, ErrTransferStaleAuthority) {
		t.Fatalf("old-fence resume err=%v", err)
	}
}

func TestTransferArtifactAndFetchStayFailClosedUntilDurableArtifactStore(t *testing.T) {
	fixture := newTransferFixture(t)
	owner := mustTransferOwner(t, fixture.destination)
	if _, err := owner.ReceiveResultArtifact(context.Background(), fixture.sourcePeer(), protocol.ResultArtifactChunk{}); !errors.Is(err, ErrResultArtifactUnavailable) {
		t.Fatalf("artifact err=%v", err)
	}
	if _, err := owner.OpenResultFetch(context.Background(), TransferPeer{NodeID: fixture.epoch.Coordinator, Role: TransferLeaderFetch}, protocol.ResultFetchRequest{}); !errors.Is(err, ErrResultFetchUnavailable) {
		t.Fatalf("fetch err=%v", err)
	}
}

func TestTransferEnforcesPerPeerLimitAndCanceledWorkNeverEnters(t *testing.T) {
	fixture := newTransferFixture(t)
	fixture.destination.resultStarted = make(chan struct{})
	fixture.destination.resultRelease = make(chan struct{})
	owner, err := NewTransferOwner(TransferOptions{Repository: fixture.destination, MaxPerPeer: 1, MaxActive: 2, MaxQueuedWork: 1})
	if err != nil {
		t.Fatal(err)
	}
	chunk := fixture.normalChunk(t)
	firstDone := make(chan error, 1)
	go func() {
		_, receiveErr := owner.ReceiveResultRecord(context.Background(), fixture.sourcePeer(), chunk)
		firstDone <- receiveErr
	}()
	<-fixture.destination.resultStarted
	waiting, stopWaiting := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		_, receiveErr := owner.ReceiveResultRecord(waiting, fixture.sourcePeer(), chunk)
		secondDone <- receiveErr
	}()
	for {
		owner.mu.Lock()
		queued := owner.queued
		owner.mu.Unlock()
		if queued == 1 {
			break
		}
		runtime.Gosched()
	}
	if _, err = owner.ReceiveResultRecord(context.Background(), fixture.sourcePeer(), chunk); !errors.Is(err, ErrTransferCapacity) {
		t.Fatalf("bounded queue overflow err=%v", err)
	}
	stopWaiting()
	if err = <-secondDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("queued cancellation err=%v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = owner.ReceiveResultRecord(canceled, TransferPeer{NodeID: 77, WorkerEpoch: model.WorkerEpoch{7}, Role: TransferNormalReplication}, chunk); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled work err=%v", err)
	}
	close(fixture.destination.resultRelease)
	if err = <-firstDone; err != nil {
		t.Fatal(err)
	}
}

type transferRepository struct {
	mu            sync.Mutex
	work          store.RecoveredWork
	localNode     uint16
	localEpoch    model.WorkerEpoch
	assignments   map[model.JobID]store.InstalledAssignment
	results       []store.StoredResult
	repairs       []store.ResultRepairRecord
	log           []string
	resultStarted chan struct{}
	resultRelease chan struct{}
	recoverErr    error
	repairErrOnce error
}

func (r *transferRepository) clone() *transferRepository {
	r.mu.Lock()
	defer r.mu.Unlock()
	return &transferRepository{work: r.work.Clone(), localNode: r.localNode, localEpoch: r.localEpoch, assignments: cloneTransferAssignments(r.assignments)}
}

func (r *transferRepository) RecoverWork() (store.RecoveredWork, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.recoverErr != nil {
		return store.RecoveredWork{}, r.recoverErr
	}
	return r.work.Clone(), nil
}
func (r *transferRepository) LocalIdentity() (uint16, model.WorkerEpoch) {
	return r.localNode, r.localEpoch
}
func (r *transferRepository) CurrentFence() model.CoordinatorEpoch {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.work.Fence
}
func (r *transferRepository) InstalledAssignment(job model.JobID) (store.InstalledAssignment, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.assignments[job]
	return v, ok
}
func (r *transferRepository) UpsertResult(record model.ResultRecord, provenance model.ResultCopyProvenance) error {
	if r.resultStarted != nil {
		select {
		case <-r.resultStarted:
		default:
			close(r.resultStarted)
		}
	}
	if r.resultRelease != nil {
		<-r.resultRelease
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, prior := range r.results {
		if prior.Record.SinkTask == record.SinkTask && prior.Record.TupleID == record.TupleID {
			if prior.Record.Checksum != record.Checksum || prior.Provenance != provenance {
				return model.ErrIdentityReuse
			}
			return nil
		}
	}
	record.Value = append([]byte(nil), record.Value...)
	r.results = append(r.results, store.StoredResult{Record: record, Provenance: provenance})
	r.work.Results = append(r.work.Results, store.StoredResult{Record: record, Provenance: provenance})
	r.log = append(r.log, "result")
	return nil
}
func (r *transferRepository) UpsertRepair(repair store.ResultRepairRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.repairErrOnce != nil {
		err := r.repairErrOnce
		r.repairErrOnce = nil
		return err
	}
	for i := range r.repairs {
		if r.repairs[i].Instruction.RepairID == repair.Instruction.RepairID {
			if repair.NextRecord < r.repairs[i].NextRecord || repair.NextOffset < r.repairs[i].NextOffset {
				return errors.New("repair regression")
			}
			r.repairs[i] = repair
			r.work.Repairs[i] = repair
			r.log = append(r.log, "repair")
			return nil
		}
	}
	r.repairs = append(r.repairs, repair)
	r.work.Repairs = append(r.work.Repairs, repair)
	r.log = append(r.log, "repair")
	return nil
}

type transferFixture struct {
	assignment          store.InstalledAssignment
	epoch               model.CoordinatorEpoch
	source, destination *transferRepository
	replica             model.ResultReplicaSet
	records             []model.ResultRecord
	repair              store.ResultRepairRecord
}

func newTransferFixture(t *testing.T) transferFixture {
	t.Helper()
	base, _, replica := workerFixtureWithLocalPrimarySink(t)
	assignment := base.assignment
	source := &transferRepository{work: store.RecoveredWork{Fence: base.epoch, Assignments: []store.InstalledAssignment{assignment}, NextTransactionID: 1}, localNode: replica.PrimaryNodeID, localEpoch: replica.PrimaryEpoch, assignments: map[model.JobID]store.InstalledAssignment{assignment.Assignment.JobID: assignment}}
	destination := &transferRepository{work: store.RecoveredWork{Fence: base.epoch, Assignments: []store.InstalledAssignment{assignment}, NextTransactionID: 1}, localNode: replica.SecondaryNodeID, localEpoch: replica.SecondaryEpoch, assignments: map[model.JobID]store.InstalledAssignment{assignment.Assignment.JobID: assignment}}
	records := []model.ResultRecord{transferResult(t, base, replica, 1), transferResult(t, base, replica, 2)}
	if records[1].TupleID.SourceSequence < records[0].TupleID.SourceSequence {
		records[0], records[1] = records[1], records[0]
	}
	queryDigest := model.ResultInventoryQueryDigest(model.ResultInventoryQueryDefinition{JobID: assignment.Assignment.JobID, SinkTask: replica.SinkTask, SpecificationHash: base.topology.Digest(), AssignmentRevision: assignment.Assignment.Revision, AssignmentDigest: assignment.Assignment.Digest, CheckpointDigest: model.CheckpointVectorDigest(nil)})
	count, total, digest, err := ResultInventoryAggregate(queryDigest, records)
	if err != nil {
		t.Fatal(err)
	}
	definition := model.RepairResultPartitionDefinition{CoordinatorEpoch: base.epoch, JobID: assignment.Assignment.JobID, AssignmentRevision: assignment.Assignment.Revision, AssignmentDigest: assignment.Assignment.Digest, SourceNodeID: replica.PrimaryNodeID, SourceWorkerEpoch: replica.PrimaryEpoch, DestinationNodeID: replica.SecondaryNodeID, DestinationWorkerEpoch: replica.SecondaryEpoch, SinkTask: replica.SinkTask, SpecificationHash: base.topology.Digest(), CheckpointDigest: model.CheckpointVectorDigest(nil), InventoryQueryDigest: queryDigest, ExpectedRecordCount: count, ExpectedTotalBytes: total, ExpectedContentDigest: digest}
	definition.RepairID = model.DeriveRepairID(definition)
	repair := store.ResultRepairRecord{Instruction: definition, InstructionDigest: model.RepairInstructionDigest(definition), Role: store.RepairDestination, State: store.RepairPending}
	repair.ContentDigest = model.EmptyResultInventoryDigest(repair.InstructionDigest)
	return transferFixture{assignment: assignment, epoch: base.epoch, source: source, destination: destination, replica: replica, records: records, repair: repair}
}

func transferResult(t *testing.T, fixture workerTestFixture, replica model.ResultReplicaSet, sequence uint64) model.ResultRecord {
	t.Helper()
	tuple, exists, err := model.SourceTuple(fixture.topology, fixture.source.Task, sequence)
	if err != nil || !exists {
		t.Fatalf("source tuple: exists=%t err=%v", exists, err)
	}
	encoded, err := model.MarshalTuple(tuple)
	if err != nil {
		t.Fatal(err)
	}
	record, err := model.NewResultRecord(model.DeriveSourceTupleID(fixture.assignment.Assignment.JobID, fixture.source.Task, sequence), replica.SinkTask, fixture.topology.Digest(), encoded)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func (f transferFixture) provenance() model.ResultCopyProvenance {
	return model.ResultCopyProvenance{AssignmentRevision: f.assignment.Assignment.Revision, AssignmentDigest: f.assignment.Assignment.Digest, ReplicaSet: f.replica, DestinationRole: model.SecondaryReplica, CoordinatorEpoch: f.epoch}
}
func (f transferFixture) sourcePeer() TransferPeer {
	return TransferPeer{NodeID: f.replica.PrimaryNodeID, WorkerEpoch: f.replica.PrimaryEpoch, Role: TransferNormalReplication}
}
func (f transferFixture) repairSourcePeer() TransferPeer {
	p := f.sourcePeer()
	p.Role = TransferHistoricalRepair
	return p
}
func (f transferFixture) destinationPeer() TransferPeer {
	return TransferPeer{NodeID: f.replica.SecondaryNodeID, WorkerEpoch: f.replica.SecondaryEpoch, Role: TransferHistoricalRepair}
}
func (f transferFixture) normalChunk(t *testing.T) protocol.ResultRecordChunk {
	return resultTransferChunk(t, TransferNormalReplication, f.records[0], f.provenance(), f.replica.SecondaryNodeID, f.replica.SecondaryEpoch, [16]byte{}, [32]byte{})
}
func (f transferFixture) repairChunk(t *testing.T, record model.ResultRecord) protocol.ResultRecordChunk {
	return resultTransferChunk(t, TransferHistoricalRepair, record, f.provenance(), f.replica.SecondaryNodeID, f.replica.SecondaryEpoch, f.repair.Instruction.RepairID, f.repair.InstructionDigest)
}
func resultTransferChunk(t *testing.T, role TransferRole, record model.ResultRecord, provenance model.ResultCopyProvenance, destination uint16, epoch model.WorkerEpoch, repairID [16]byte, instruction [32]byte) protocol.ResultRecordChunk {
	t.Helper()
	stream, err := model.MarshalResultRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	id, err := DeriveResultRecordTransferID(role, record, provenance, destination, epoch, repairID, instruction)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.ResultRecordChunk{Transfer: protocol.TransferChunk{TransferID: id, JobID: record.TupleID.JobID, TotalLength: uint64(len(stream)), Checksum: sha256.Sum256(stream), Data: append([]byte(nil), stream...), Final: true}, Record: record, Provenance: provenance, DestinationNodeID: destination, DestinationWorkerEpoch: epoch, RepairID: repairID, RepairInstructionDigest: instruction}
}
func (f transferFixture) installRepair(t *testing.T) {
	t.Helper()
	if err := f.destination.UpsertRepair(f.repair); err != nil {
		t.Fatal(err)
	}
	f.destination.log = nil
}
func (f transferFixture) installSourceRepair(t *testing.T) {
	t.Helper()
	sourceRepair := f.repair
	sourceRepair.Role = store.RepairSource
	provenance := f.provenance()
	provenance.DestinationRole = model.PrimaryReplica
	f.source.results = []store.StoredResult{{Record: f.records[1], Provenance: provenance}, {Record: f.records[0], Provenance: provenance}}
	f.source.work.Results = append([]store.StoredResult(nil), f.source.results...)
	if err := f.source.UpsertRepair(sourceRepair); err != nil {
		t.Fatal(err)
	}
	f.source.log = nil
}
func (f transferFixture) destinationRepairStatus() protocol.ResultRepairStatus {
	return protocol.ResultRepairStatus{Instruction: repairProtocolDefinition(f.repair.Instruction), RepairID: f.repair.Instruction.RepairID, InstructionDigest: f.repair.InstructionDigest, Role: protocol.RepairDestination, State: protocol.RepairPending, ContentDigest: model.EmptyResultInventoryDigest(f.repair.InstructionDigest)}
}
func (f transferFixture) ack(chunk protocol.ResultRecordChunk) protocol.ResultRecordAck {
	return protocol.ResultRecordAck{TransferID: chunk.Transfer.TransferID, NodeID: chunk.DestinationNodeID, WorkerEpoch: chunk.DestinationWorkerEpoch, RepairID: chunk.RepairID, RepairInstructionDigest: chunk.RepairInstructionDigest, NextOffset: chunk.Transfer.TotalLength, TotalLength: chunk.Transfer.TotalLength, Checksum: chunk.Transfer.Checksum, Complete: true, CoordinatorEpoch: chunk.Provenance.CoordinatorEpoch}
}

func mustTransferOwner(t *testing.T, repository TransferRepository) *TransferOwner {
	t.Helper()
	owner, err := NewTransferOwner(TransferOptions{Repository: repository})
	if err != nil {
		t.Fatal(err)
	}
	return owner
}
func cloneTransferAssignments(input map[model.JobID]store.InstalledAssignment) map[model.JobID]store.InstalledAssignment {
	output := make(map[model.JobID]store.InstalledAssignment, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
