package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"runtime"
	"sync"
	"testing"

	"crane/internal/crane/model"
	"crane/internal/crane/protocol"
	"crane/internal/crane/store"
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

func TestTransferInventoryUsesCanonicalLengthPrefixedEntries(t *testing.T) {
	fixture := newTransferFixture(t)
	logical, err := model.MarshalResultRecord(fixture.records[0])
	if err != nil {
		t.Fatal(err)
	}
	count, total, _, err := ResultInventoryAggregate(fixture.repair.Instruction.InventoryQueryDigest, fixture.records[:1])
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || total != uint64(len(logical)+4) {
		t.Fatalf("inventory count/bytes=%d/%d want=1/%d", count, total, len(logical)+4)
	}
}

func TestTransferRealStoreUsesEntryBoundsAndRecoversProgress(t *testing.T) {
	for _, test := range []struct {
		name  string
		value func(*testing.T) []byte
		want  uint64
	}{
		{name: "minimum", value: func(t *testing.T) []byte {
			encoded, err := model.MarshalTuple(model.Tuple{Fields: []model.Field{}})
			if err != nil {
				t.Fatal(err)
			}
			return encoded
		}, want: model.ResultArtifactMinRecordBytesV1},
		{name: "maximum", value: func(t *testing.T) []byte {
			encoded, err := model.MarshalTuple(model.Tuple{Fields: []model.Field{{Name: "x", Value: model.Value{Type: model.ValueBytes, Bytes: make([]byte, 504)}}}})
			if err != nil {
				t.Fatal(err)
			}
			return encoded
		}, want: model.ResultArtifactMaxRecordBytesV1},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newTransferFixture(t)
			record, err := model.NewResultRecord(fixture.records[0].TupleID, fixture.replica.SinkTask, fixture.records[0].SpecificationHash, test.value(t))
			if err != nil {
				t.Fatal(err)
			}
			fixture.records = []model.ResultRecord{record}
			count, total, digest, err := ResultInventoryAggregate(fixture.repair.Instruction.InventoryQueryDigest, fixture.records)
			if err != nil {
				t.Fatal(err)
			}
			if total != test.want {
				t.Fatalf("entry bytes=%d want=%d", total, test.want)
			}
			fixture.repair.Instruction.ExpectedRecordCount, fixture.repair.Instruction.ExpectedTotalBytes, fixture.repair.Instruction.ExpectedContentDigest = count, total, digest
			rebindTransferRepair(&fixture.repair)
			path := t.TempDir() + "/worker"
			repository := openRealTransferRepository(t, path, fixture)
			if err := repository.UpsertRepair(fixture.repair); err != nil {
				t.Fatalf("real Store grant: %v", err)
			}
			owner := mustTransferOwner(t, repository)
			if _, err := owner.ReceiveResultRecord(context.Background(), fixture.repairSourcePeer(), fixture.repairChunk(t, record)); err != nil {
				t.Fatalf("real Store receive: %v", err)
			}
			if err := repository.workerStore.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := store.Open(path, repository.identity, repository.options)
			if err != nil {
				t.Fatalf("reopen real Store: %v", err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			work, err := reopened.RecoverWork()
			if err != nil || len(work.Repairs) != 1 || work.Repairs[0].State != store.RepairComplete || work.Repairs[0].NextOffset != test.want || work.Repairs[0].TotalBytes != test.want {
				t.Fatalf("recovered real progress=%+v err=%v", work.Repairs, err)
			}
		})
	}
}

func TestTransferRealStoreAcceptsExactPerJobRepairBoundary(t *testing.T) {
	fixture := newTransferFixture(t)
	const exact = uint64(64 << 20)
	fixture.repair.Instruction.ExpectedRecordCount = 100_000
	fixture.repair.Instruction.ExpectedTotalBytes = exact
	fixture.repair.Instruction.ExpectedContentDigest = sha256.Sum256([]byte("exact-64-mib-inventory"))
	rebindTransferRepair(&fixture.repair)
	path := t.TempDir() + "/worker"
	repository := openRealTransferRepository(t, path, fixture)
	if err := repository.UpsertRepair(fixture.repair); err != nil {
		t.Fatalf("exact boundary grant: %v", err)
	}
	if err := repository.workerStore.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(path, repository.identity, repository.options)
	if err != nil {
		t.Fatalf("reopen exact boundary grant: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	work, err := reopened.RecoverWork()
	if err != nil || len(work.Repairs) != 1 || work.Repairs[0].Instruction.ExpectedTotalBytes != exact {
		t.Fatalf("recovered exact boundary=%+v err=%v", work.Repairs, err)
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
	if progress.State != store.RepairStreaming || progress.NextRecord != 1 || progress.NextOffset != uint64(len(stream)+4) {
		t.Fatalf("durable progress=%+v", progress)
	}

	// An ACK lost after both writes causes an exact whole-record retransmit. A
	// reconstructed owner must ACK it idempotently without advancing twice.
	restarted := mustTransferOwner(t, fixture.destination)
	if _, err = restarted.ReceiveResultRecord(context.Background(), fixture.repairSourcePeer(), chunk); err != nil {
		t.Fatalf("exact recovered duplicate: %v", err)
	}
	if got := fixture.destination.repairs[0]; got.NextRecord != 1 || got.NextOffset != uint64(len(stream)+4) {
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

func TestTransferSerializesWholeDestinationRepairTransition(t *testing.T) {
	fixture := newTransferFixture(t)
	fixture.installRepair(t)
	owner := mustTransferOwner(t, fixture.destination)
	firstBlocked := make(chan struct{})
	releaseFirst := make(chan struct{})
	fixture.destination.beforeResult = func(record model.ResultRecord) {
		if record.TupleID == fixture.records[0].TupleID {
			select {
			case <-firstBlocked:
			default:
				close(firstBlocked)
			}
			<-releaseFirst
		}
	}
	firstChunk := fixture.repairChunk(t, fixture.records[0])
	secondChunk := fixture.repairChunk(t, fixture.records[1])
	firstDone := make(chan error, 1)
	go func() {
		_, err := owner.ReceiveResultRecord(context.Background(), fixture.repairSourcePeer(), firstChunk)
		firstDone <- err
	}()
	<-firstBlocked
	secondDone := make(chan error, 1)
	go func() {
		_, err := owner.ReceiveResultRecord(context.Background(), fixture.repairSourcePeer(), secondChunk)
		secondDone <- err
	}()
	for index := 0; index < 1000; index++ {
		runtime.Gosched()
	}
	fixture.destination.mu.Lock()
	beforeReleaseResults := len(fixture.destination.results)
	fixture.destination.mu.Unlock()
	if beforeReleaseResults != 0 {
		t.Fatalf("later repair record mutated while prior transition blocked: results=%d", beforeReleaseResults)
	}
	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	if got := fixture.destination.repairs[0]; got.State != store.RepairComplete || got.NextRecord != 2 || got.NextOffset != got.Instruction.ExpectedTotalBytes {
		t.Fatalf("serialized repair outcome=%+v", got)
	}
}

func TestTransferSerializesDuplicateSourceProgressACKs(t *testing.T) {
	fixture := newTransferFixture(t)
	fixture.installSourceRepair(t)
	owner := mustTransferOwner(t, fixture.source)
	chunk, complete, err := owner.NextRepairRecord(context.Background(), fixture.destinationPeer(), fixture.repair.Instruction.RepairID, fixture.destinationRepairStatus())
	if err != nil || complete {
		t.Fatalf("next record complete=%t err=%v", complete, err)
	}
	ack := fixture.ack(chunk)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	fixture.source.beforeRepair = func(repair store.ResultRepairRecord) {
		if repair.NextRecord == 1 {
			entered <- struct{}{}
			<-release
		}
	}
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- owner.AcknowledgeRepairRecord(context.Background(), fixture.destinationPeer(), chunk, ack)
	}()
	<-entered
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- owner.AcknowledgeRepairRecord(context.Background(), fixture.destinationPeer(), chunk, ack)
	}()
	for index := 0; index < 1000; index++ {
		runtime.Gosched()
	}
	select {
	case <-entered:
		t.Fatal("duplicate source progress entered durable transition concurrently")
	default:
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	if got := fixture.source.repairs[0]; got.NextRecord != 1 || got.NextOffset == 0 {
		t.Fatalf("duplicate ACK progress=%+v", got)
	}
}

func TestTransferRepairRejectsRecordsOutsideCheckpointVectorBeforeMutation(t *testing.T) {
	for _, test := range []struct {
		name   string
		record func(transferFixture) model.ResultRecord
	}{
		{name: "above watermark", record: func(f transferFixture) model.ResultRecord {
			tuple := f.records[1].TupleID
			tuple.SourceSequence++
			tuple.PathDigest = sha256.Sum256([]byte("above-repair-watermark"))
			record, err := model.NewResultRecord(tuple, f.records[1].SinkTask, f.records[1].SpecificationHash, f.records[1].Value)
			if err != nil {
				t.Fatal(err)
			}
			return record
		}},
		{name: "absent source", record: func(f transferFixture) model.ResultRecord {
			tuple := f.records[1].TupleID
			tuple.SourceTask.StageID = 2
			record, err := model.NewResultRecord(tuple, f.records[1].SinkTask, f.records[1].SpecificationHash, f.records[1].Value)
			if err != nil {
				t.Fatal(err)
			}
			return record
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, repositoryKind := range []string{"fake", "real"} {
				t.Run(repositoryKind, func(t *testing.T) {
					fixture := newTransferFixture(t)
					fixture.repair.Instruction.Checkpoints = []model.SourceCheckpoint{{Source: fixture.records[1].TupleID.SourceTask, Watermark: fixture.records[1].TupleID.SourceSequence}}
					fixture.repair.Instruction.CheckpointDigest = model.CheckpointVectorDigest(fixture.repair.Instruction.Checkpoints)
					fixture.repair.Instruction.InventoryQueryDigest = model.ResultInventoryQueryDigest(model.ResultInventoryQueryDefinition{JobID: fixture.repair.Instruction.JobID, SinkTask: fixture.repair.Instruction.SinkTask, SpecificationHash: fixture.repair.Instruction.SpecificationHash, AssignmentRevision: fixture.repair.Instruction.AssignmentRevision, AssignmentDigest: fixture.repair.Instruction.AssignmentDigest, Checkpoints: fixture.repair.Instruction.Checkpoints, CheckpointDigest: fixture.repair.Instruction.CheckpointDigest})
					count, total, digest, err := ResultInventoryAggregate(fixture.repair.Instruction.InventoryQueryDigest, fixture.records)
					if err != nil {
						t.Fatal(err)
					}
					fixture.repair.Instruction.ExpectedRecordCount, fixture.repair.Instruction.ExpectedTotalBytes, fixture.repair.Instruction.ExpectedContentDigest = count, total, digest
					rebindTransferRepair(&fixture.repair)
					var repository TransferRepository
					if repositoryKind == "fake" {
						fixture.installRepair(t)
						repository = fixture.destination
					} else {
						realRepository := openRealTransferRepository(t, t.TempDir()+"/worker", fixture)
						if err := realRepository.UpsertRepair(fixture.repair); err != nil {
							t.Fatal(err)
						}
						repository = realRepository
					}
					owner := mustTransferOwner(t, repository)
					if _, err = owner.ReceiveResultRecord(context.Background(), fixture.repairSourcePeer(), fixture.repairChunk(t, test.record(fixture))); err == nil {
						t.Fatal("out-of-vector record accepted")
					}
					work, recoverErr := repository.RecoverWork()
					if recoverErr != nil || len(work.Results) != 0 || len(work.Repairs) != 1 || work.Repairs[0].NextRecord != 0 {
						t.Fatalf("out-of-vector record mutated state: results=%d repairs=%+v err=%v", len(work.Results), work.Repairs, recoverErr)
					}
					if _, err = owner.ReceiveResultRecord(context.Background(), fixture.repairSourcePeer(), fixture.repairChunk(t, fixture.records[0])); err != nil {
						t.Fatalf("valid covered prefix failed after rejection: %v", err)
					}
				})
			}
		})
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
	beforeResult  func(model.ResultRecord)
	beforeRepair  func(store.ResultRepairRecord)
}

type realTransferRepository struct {
	workerStore *store.Store
	identity    store.Identity
	options     store.Options
	node        uint16
	epoch       model.WorkerEpoch
}

func openRealTransferRepository(t *testing.T, path string, fixture transferFixture) *realTransferRepository {
	t.Helper()
	identity := store.Identity{ClusterID: [16]byte{51}, NodeID: fixture.destination.localNode}
	options := store.Options{MaxBytes: 16 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return fixture.destination.localEpoch, nil }}
	workerStore, err := store.Open(path, identity, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = workerStore.Close() })
	if err := workerStore.Fence(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	if err := workerStore.InstallAssignment(fixture.assignment.Assignment, fixture.assignment.Topology.Spec(), fixture.assignment.JobControlRevision, fixture.assignment.SchedulingState, fixture.epoch); err != nil {
		t.Fatal(err)
	}
	return &realTransferRepository{workerStore: workerStore, identity: identity, options: options, node: identity.NodeID, epoch: fixture.destination.localEpoch}
}

func (r *realTransferRepository) RecoverWork() (store.RecoveredWork, error) {
	return r.workerStore.RecoverWork()
}
func (r *realTransferRepository) LocalIdentity() (uint16, model.WorkerEpoch) { return r.node, r.epoch }
func (r *realTransferRepository) CurrentFence() model.CoordinatorEpoch {
	work, _ := r.workerStore.RecoverWork()
	return work.Fence
}
func (r *realTransferRepository) InstalledAssignment(job model.JobID) (store.InstalledAssignment, bool) {
	work, err := r.workerStore.RecoverWork()
	if err != nil {
		return store.InstalledAssignment{}, false
	}
	for _, assignment := range work.Assignments {
		if assignment.Assignment.JobID == job {
			return assignment, true
		}
	}
	return store.InstalledAssignment{}, false
}
func (r *realTransferRepository) UpsertResult(record model.ResultRecord, provenance model.ResultCopyProvenance) error {
	return r.workerStore.UpsertResult(record, provenance)
}
func (r *realTransferRepository) UpsertRepair(repair store.ResultRepairRecord) error {
	return r.workerStore.UpsertRepair(repair)
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
	if r.beforeResult != nil {
		r.beforeResult(record)
	}
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
	for index, prior := range r.results {
		if prior.Record.SinkTask == record.SinkTask && prior.Record.TupleID == record.TupleID {
			if prior.Record.Checksum != record.Checksum {
				return model.ErrIdentityReuse
			}
			if prior.Provenance == provenance {
				return nil
			}
			// Mirror the store's rebind contract: the identical logical
			// record re-binds only from a strictly superseded envelope.
			if !store.ResultProvenanceOrderedBefore(prior.Provenance, provenance) {
				return model.ErrIdentityReuse
			}
			r.results[index].Provenance = provenance
			for workIndex := range r.work.Results {
				if r.work.Results[workIndex].Record.SinkTask == record.SinkTask && r.work.Results[workIndex].Record.TupleID == record.TupleID {
					r.work.Results[workIndex].Provenance = provenance
				}
			}
			r.log = append(r.log, "result")
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
	if r.beforeRepair != nil {
		r.beforeRepair(repair)
	}
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

func rebindTransferRepair(repair *store.ResultRepairRecord) {
	repair.Instruction.InventoryQueryDigest = model.ResultInventoryQueryDigest(model.ResultInventoryQueryDefinition{JobID: repair.Instruction.JobID, SinkTask: repair.Instruction.SinkTask, SpecificationHash: repair.Instruction.SpecificationHash, AssignmentRevision: repair.Instruction.AssignmentRevision, AssignmentDigest: repair.Instruction.AssignmentDigest, Checkpoints: repair.Instruction.Checkpoints, CheckpointDigest: repair.Instruction.CheckpointDigest})
	repair.Instruction.RepairID = model.DeriveRepairID(repair.Instruction)
	repair.InstructionDigest = model.RepairInstructionDigest(repair.Instruction)
	repair.ContentDigest = model.EmptyResultInventoryDigest(repair.InstructionDigest)
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

// setTransferScheduling re-installs the fixture assignment at one scheduling
// state on both fake repositories.
func (f *transferFixture) setTransferScheduling(t *testing.T, state model.SchedulingState) {
	t.Helper()
	f.assignment.SchedulingState = state
	for _, repository := range []*transferRepository{f.source, f.destination} {
		repository.mu.Lock()
		repository.assignments[f.assignment.Assignment.JobID] = f.assignment
		repository.work.Assignments = []store.InstalledAssignment{f.assignment}
		repository.mu.Unlock()
	}
}

// seedFinalObservations records durable final committed-checkpoint
// observations for every source task of the fixture assignment.
func (f *transferFixture) seedFinalObservations(t *testing.T, raftIndex uint64) {
	t.Helper()
	observations := make([]store.CommittedCheckpoint, 0)
	for _, token := range f.assignment.Assignment.Tasks {
		stage, ok := findStage(f.assignment.Topology, token.Task.StageID)
		if !ok || stage.Role != model.StageSource {
			continue
		}
		eof, err := model.SourceEOF(f.assignment.Topology, token.Task)
		if err != nil {
			t.Fatal(err)
		}
		observations = append(observations, store.CommittedCheckpoint{
			Notice:             model.CheckpointNotice{JobID: f.assignment.Assignment.JobID, Source: token.Task, Watermark: eof, RaftIndex: raftIndex, Epoch: f.epoch},
			JobControlRevision: f.assignment.JobControlRevision, AssignmentRevision: f.assignment.Assignment.Revision, AssignmentDigest: f.assignment.Assignment.Digest,
		})
	}
	if len(observations) == 0 {
		t.Fatal("fixture has no source tasks")
	}
	for _, repository := range []*transferRepository{f.source, f.destination} {
		repository.mu.Lock()
		repository.work.Checkpoints = append([]store.CommittedCheckpoint(nil), observations...)
		repository.mu.Unlock()
	}
}

// installPrimaryRecords retains the fixture records on the primary under the
// current primary-copy provenance.
func (f *transferFixture) installPrimaryRecords(t *testing.T) {
	t.Helper()
	provenance := f.provenance()
	provenance.DestinationRole = model.PrimaryReplica
	f.source.mu.Lock()
	f.source.results = []store.StoredResult{{Record: f.records[1], Provenance: provenance}, {Record: f.records[0], Provenance: provenance}}
	f.source.work.Results = append([]store.StoredResult(nil), f.source.results...)
	f.source.mu.Unlock()
}

func (f transferFixture) leaderPeer() TransferPeer {
	return TransferPeer{NodeID: f.epoch.Coordinator, Role: TransferLeaderFetch}
}

func (f transferFixture) coordinatorArtifactPeer() TransferPeer {
	return TransferPeer{NodeID: f.epoch.Coordinator, WorkerEpoch: model.WorkerEpoch{5}, Role: TransferNormalReplication}
}

// sealedFixtureArtifact derives the canonical sealed artifact of the fixture's
// covered record set.
func sealedFixtureArtifact(t *testing.T, f transferFixture) (protocol.ResultArtifact, []byte) {
	t.Helper()
	artifact, stream, err := SealResultPartition(f.assignment.Assignment.JobID, f.replica.SinkTask, f.assignment.Topology.Digest(), f.records)
	if err != nil {
		t.Fatal(err)
	}
	return artifact, stream
}

// artifactChunkFor builds one authenticated artifact chunk over the stream slice.
func artifactChunkFor(t *testing.T, artifact protocol.ResultArtifact, stream []byte, offset, length uint64, destination uint16, destinationEpoch model.WorkerEpoch, peer TransferPeer, epoch model.CoordinatorEpoch) protocol.ResultArtifactChunk {
	t.Helper()
	end := offset + length
	chunk := protocol.ResultArtifactChunk{
		Transfer: protocol.TransferChunk{JobID: artifact.JobID, TotalLength: artifact.TotalLength, Checksum: artifact.Checksum, Offset: offset, Data: append([]byte(nil), stream[offset:end]...), Final: end == artifact.TotalLength},
		Artifact: artifact, DestinationNodeID: destination, DestinationWorkerEpoch: destinationEpoch, CoordinatorEpoch: epoch,
	}
	id, err := DeriveResultArtifactTransferID(TransferNormalReplication, artifact, destination, destinationEpoch, offset, length, epoch)
	if err != nil {
		t.Fatal(err)
	}
	chunk.Transfer.TransferID = id
	if peer.Role != TransferNormalReplication {
		t.Fatalf("artifact chunk peer role %d", peer.Role)
	}
	return chunk
}

func TestOpenResultFetchSealsOnDemandFromRetainedRecordsAndStreamsExactSlices(t *testing.T) {
	fixture := newTransferFixture(t)
	fixture.setTransferScheduling(t, model.Draining)
	fixture.seedFinalObservations(t, 11)
	fixture.installPrimaryRecords(t)
	artifacts, err := NewArtifactStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	owner := mustArtifactTransferOwner(t, fixture.source, artifacts)
	artifact, stream := sealedFixtureArtifact(t, fixture)

	request := protocol.ResultFetchRequest{Artifact: artifact, ReplicaNodeID: fixture.replica.PrimaryNodeID, ReplicaWorkerEpoch: fixture.replica.PrimaryEpoch, Offset: 0, CoordinatorEpoch: fixture.epoch}
	chunk, err := owner.OpenResultFetch(context.Background(), fixture.leaderPeer(), request)
	if err != nil {
		t.Fatal(err)
	}
	wantEnd := uint64(protocol.MaxTransferChunkBytes)
	if wantEnd > artifact.TotalLength {
		wantEnd = artifact.TotalLength
	}
	if !bytes.Equal(chunk.Transfer.Data, stream[:wantEnd]) || chunk.Transfer.Offset != 0 || chunk.Transfer.Final != (wantEnd == artifact.TotalLength) ||
		chunk.Transfer.TotalLength != artifact.TotalLength || chunk.Transfer.Checksum != artifact.Checksum || chunk.Artifact != artifact ||
		chunk.SourceNodeID != fixture.replica.PrimaryNodeID || chunk.SourceWorkerEpoch != fixture.replica.PrimaryEpoch || chunk.CoordinatorEpoch != fixture.epoch {
		t.Fatalf("first fetch chunk=%+v", chunk)
	}
	if sealed, sealErr := artifacts.Sealed(artifact); sealErr != nil || !sealed {
		t.Fatalf("fetch did not durably seal=%t err=%v", sealed, sealErr)
	}

	middle := protocol.ResultFetchRequest{Artifact: artifact, ReplicaNodeID: fixture.replica.PrimaryNodeID, ReplicaWorkerEpoch: fixture.replica.PrimaryEpoch, Offset: 1, CoordinatorEpoch: fixture.epoch}
	rest, err := owner.OpenResultFetch(context.Background(), fixture.leaderPeer(), middle)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rest.Transfer.Data, stream[1:]) || rest.Transfer.Offset != 1 || !rest.Transfer.Final {
		t.Fatalf("tail fetch chunk offset=%d final=%t bytes=%x", rest.Transfer.Offset, rest.Transfer.Final, rest.Transfer.Data)
	}

	// A restarted owner serves the already sealed artifact even after every
	// retained record copy vanished (sealed bytes are the authority).
	fixture.source.mu.Lock()
	fixture.source.results = nil
	fixture.source.work.Results = nil
	fixture.source.mu.Unlock()
	restarted := mustArtifactTransferOwner(t, fixture.source, artifacts)
	tail, err := restarted.OpenResultFetch(context.Background(), fixture.leaderPeer(), middle)
	if err != nil || !bytes.Equal(tail.Transfer.Data, stream[1:]) {
		t.Fatalf("restart fetch err=%v bytes=%x", err, tail.Transfer.Data)
	}
}

func TestOpenResultFetchSealsRetainedOldProvenanceRecordsUnderCurrentEnvelope(t *testing.T) {
	fixture := newTransferFixture(t)
	fixture.setTransferScheduling(t, model.Draining)
	fixture.seedFinalObservations(t, 12)
	fixture.installPrimaryRecords(t)
	old := model.WorkerEpoch{99}
	fixture.source.mu.Lock()
	for index := range fixture.source.results {
		fixture.source.results[index].Provenance.ReplicaSet.PrimaryEpoch = old
		fixture.source.work.Results[index].Provenance.ReplicaSet.PrimaryEpoch = old
	}
	fixture.source.mu.Unlock()
	artifacts, err := NewArtifactStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	owner := mustArtifactTransferOwner(t, fixture.source, artifacts)
	artifact, stream := sealedFixtureArtifact(t, fixture)

	request := protocol.ResultFetchRequest{Artifact: artifact, ReplicaNodeID: fixture.replica.PrimaryNodeID, ReplicaWorkerEpoch: fixture.replica.PrimaryEpoch, Offset: 0, CoordinatorEpoch: fixture.epoch}
	chunk, err := owner.OpenResultFetch(context.Background(), fixture.leaderPeer(), request)
	if err != nil {
		t.Fatalf("retained old provenance record rejected instead of re-enveloped: %v", err)
	}
	if !bytes.Equal(chunk.Transfer.Data, stream[:uint64(len(chunk.Transfer.Data))]) || chunk.Artifact != artifact {
		t.Fatalf("re-enveloped chunk=%+v", chunk)
	}
	// The logical checkpoint-covered record itself stays retained untouched.
	fixture.source.mu.Lock()
	records := append([]model.ResultRecord(nil), fixture.source.results[0].Record, fixture.source.results[1].Record)
	fixture.source.mu.Unlock()
	for _, record := range records {
		if record.Validate() != nil {
			t.Fatalf("logical record rejected: %+v", record)
		}
	}
}

func TestOpenResultFetchFailsClosedWithoutTerminalAuthority(t *testing.T) {
	fixture := newTransferFixture(t)
	fixture.installPrimaryRecords(t)
	artifacts, err := NewArtifactStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	artifact, _ := sealedFixtureArtifact(t, fixture)
	request := protocol.ResultFetchRequest{Artifact: artifact, ReplicaNodeID: fixture.replica.PrimaryNodeID, ReplicaWorkerEpoch: fixture.replica.PrimaryEpoch, CoordinatorEpoch: fixture.epoch}

	running := mustArtifactTransferOwner(t, fixture.source, artifacts)
	if _, err := running.OpenResultFetch(context.Background(), fixture.leaderPeer(), request); err == nil {
		t.Fatal("running assignment served result fetch")
	}

	fixture.setTransferScheduling(t, model.Draining)
	noStore := mustTransferOwner(t, fixture.source)
	if _, err := noStore.OpenResultFetch(context.Background(), fixture.leaderPeer(), request); !errors.Is(err, ErrResultFetchUnavailable) {
		t.Fatalf("missing artifact store err=%v", err)
	}

	unproven := mustArtifactTransferOwner(t, fixture.source, artifacts)
	if _, err := unproven.OpenResultFetch(context.Background(), fixture.leaderPeer(), request); !errors.Is(err, ErrResultFetchUnavailable) {
		t.Fatalf("unprovable final vector err=%v", err)
	}

	fixture.seedFinalObservations(t, 14)
	owner := mustArtifactTransferOwner(t, fixture.source, artifacts)
	if _, err := owner.OpenResultFetch(context.Background(), fixture.sourcePeer(), request); err == nil {
		t.Fatal("non-leader peer served result fetch")
	}
	foreign := request
	foreign.ReplicaNodeID = fixture.replica.SecondaryNodeID
	foreign.ReplicaWorkerEpoch = fixture.replica.SecondaryEpoch
	if _, err := owner.OpenResultFetch(context.Background(), fixture.leaderPeer(), foreign); err == nil {
		t.Fatal("foreign replica destination served")
	}
	stale := request
	stale.CoordinatorEpoch.Term++
	if _, err := owner.OpenResultFetch(context.Background(), TransferPeer{NodeID: fixture.epoch.Coordinator, Role: TransferLeaderFetch}, stale); err == nil {
		t.Fatal("stale fence served result fetch")
	}
	notLeader := request
	if _, err := owner.OpenResultFetch(context.Background(), TransferPeer{NodeID: fixture.replica.SecondaryNodeID, Role: TransferLeaderFetch}, notLeader); err == nil {
		t.Fatal("non-coordinator leader-fetch peer served")
	}
	if sealed, sealErr := artifacts.Sealed(artifact); sealErr != nil || sealed {
		t.Fatalf("rejected fetches sealed nothing sealed=%t err=%v", sealed, sealErr)
	}
}

func TestReceiveResultArtifactInstallsResumablyWithExactDurableAcks(t *testing.T) {
	fixture := newTransferFixture(t)
	fixture.setTransferScheduling(t, model.Draining)
	artifacts, err := NewArtifactStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	owner := mustArtifactTransferOwner(t, fixture.destination, artifacts)
	artifact, stream := sealedFixtureArtifact(t, fixture)
	destination := fixture.coordinatorArtifactPeer()

	split := uint64(len(stream) / 2)
	first := artifactChunkFor(t, artifact, stream, 0, split, fixture.replica.SecondaryNodeID, fixture.replica.SecondaryEpoch, destination, fixture.epoch)
	ack, err := owner.ReceiveResultArtifact(context.Background(), destination, first)
	if err != nil {
		t.Fatal(err)
	}
	if ack.TransferID != first.Transfer.TransferID || ack.NodeID != fixture.replica.SecondaryNodeID || ack.WorkerEpoch != fixture.replica.SecondaryEpoch ||
		ack.Artifact != artifact || ack.NextOffset != split || ack.Complete || ack.CoordinatorEpoch != fixture.epoch {
		t.Fatalf("partial install ack=%+v", ack)
	}
	duplicate, err := owner.ReceiveResultArtifact(context.Background(), destination, first)
	if err != nil || duplicate.NextOffset != split || duplicate.Complete {
		t.Fatalf("duplicate install ack=%+v err=%v", duplicate, err)
	}

	second := artifactChunkFor(t, artifact, stream, split, artifact.TotalLength-split, fixture.replica.SecondaryNodeID, fixture.replica.SecondaryEpoch, destination, fixture.epoch)
	finalAck, err := owner.ReceiveResultArtifact(context.Background(), destination, second)
	if err != nil || !finalAck.Complete || finalAck.NextOffset != artifact.TotalLength {
		t.Fatalf("final install ack=%+v err=%v", finalAck, err)
	}
	if sealed, sealErr := artifacts.Sealed(artifact); sealErr != nil || !sealed {
		t.Fatalf("artifact not durable=%t err=%v", sealed, sealErr)
	}
	again, err := owner.ReceiveResultArtifact(context.Background(), destination, second)
	if err != nil || !again.Complete || again.NextOffset != artifact.TotalLength {
		t.Fatalf("idempotent final ack=%+v err=%v", again, err)
	}

	// A worker-to-worker sender (the other current replica endpoint) is an
	// equally authorized source for a fresh destination copy.
	otherDir := t.TempDir()
	otherArtifacts, err := NewArtifactStore(otherDir)
	if err != nil {
		t.Fatal(err)
	}
	peerOwner := mustArtifactTransferOwner(t, fixture.source, otherArtifacts)
	peerFirst := artifactChunkFor(t, artifact, stream, 0, split, fixture.replica.PrimaryNodeID, fixture.replica.PrimaryEpoch, fixture.sourcePeer(), fixture.epoch)
	peerAck, err := peerOwner.ReceiveResultArtifact(context.Background(), fixture.sourcePeer(), peerFirst)
	if err != nil || peerAck.NodeID != fixture.replica.PrimaryNodeID || peerAck.NextOffset != split {
		t.Fatalf("worker-to-worker artifact ack=%+v err=%v", peerAck, err)
	}
}

func TestReceiveResultArtifactRejectsUnauthorizedSendersTargetsAndStaleAuthority(t *testing.T) {
	fixture := newTransferFixture(t)
	artifacts, err := NewArtifactStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	owner := mustArtifactTransferOwner(t, fixture.destination, artifacts)
	artifact, stream := sealedFixtureArtifact(t, fixture)
	destination := fixture.coordinatorArtifactPeer()

	mutators := map[string]func(*transferFixture, *protocol.ResultArtifactChunk, *TransferPeer){
		"running scheduling": func(f *transferFixture, _ *protocol.ResultArtifactChunk, _ *TransferPeer) {
			f.setTransferScheduling(t, model.Running)
		},
		"stale fence": func(_ *transferFixture, chunk *protocol.ResultArtifactChunk, _ *TransferPeer) {
			chunk.CoordinatorEpoch.Term++
		},
		"foreign destination": func(_ *transferFixture, chunk *protocol.ResultArtifactChunk, _ *TransferPeer) {
			chunk.DestinationNodeID = fixture.replica.PrimaryNodeID
			chunk.DestinationWorkerEpoch = fixture.replica.PrimaryEpoch
		},
		"unknown sender": func(_ *transferFixture, _ *protocol.ResultArtifactChunk, peer *TransferPeer) {
			peer.NodeID = 77
			peer.WorkerEpoch = model.WorkerEpoch{7}
		},
		"corrupt transfer ID": func(_ *transferFixture, chunk *protocol.ResultArtifactChunk, _ *TransferPeer) {
			chunk.Transfer.TransferID[0] ^= 1
		},
		"mutated payload": func(_ *transferFixture, chunk *protocol.ResultArtifactChunk, _ *TransferPeer) {
			chunk.Transfer.Data[0] ^= 0xff
		},
		"stale worker epoch": func(_ *transferFixture, chunk *protocol.ResultArtifactChunk, _ *TransferPeer) {
			chunk.DestinationWorkerEpoch[0]++
		},
		"coordinator role": func(_ *transferFixture, _ *protocol.ResultArtifactChunk, peer *TransferPeer) {
			peer.Role = TransferCoordinatorCommand
		},
		"historical repair role": func(_ *transferFixture, _ *protocol.ResultArtifactChunk, peer *TransferPeer) {
			peer.Role = TransferHistoricalRepair
		},
	}
	fixture.setTransferScheduling(t, model.Draining)
	for name, mutate := range mutators {
		t.Run(name, func(t *testing.T) {
			fixture.setTransferScheduling(t, model.Draining)
			chunk := artifactChunkFor(t, artifact, stream, 0, uint64(len(stream)), fixture.replica.SecondaryNodeID, fixture.replica.SecondaryEpoch, destination, fixture.epoch)
			peer := destination
			mutate(&fixture, &chunk, &peer)
			if _, err := owner.ReceiveResultArtifact(context.Background(), peer, chunk); err == nil {
				t.Fatal("unauthorized artifact accepted")
			}
		})
	}
	if sealed, sealErr := artifacts.Sealed(artifact); sealErr != nil || sealed {
		t.Fatalf("rejected transfers sealed nothing sealed=%t err=%v", sealed, sealErr)
	}
	// After every rejection the exact authorized chunk still installs.
	fixture.setTransferScheduling(t, model.Draining)
	good := artifactChunkFor(t, artifact, stream, 0, uint64(len(stream)), fixture.replica.SecondaryNodeID, fixture.replica.SecondaryEpoch, fixture.coordinatorArtifactPeer(), fixture.epoch)
	ack, err := owner.ReceiveResultArtifact(context.Background(), fixture.coordinatorArtifactPeer(), good)
	if err != nil || !ack.Complete || ack.NextOffset != artifact.TotalLength {
		t.Fatalf("authorized install after rejections ack=%+v err=%v", ack, err)
	}
}

func mustArtifactTransferOwner(t *testing.T, repository TransferRepository, artifacts *ArtifactStore) *TransferOwner {
	t.Helper()
	owner, err := NewTransferOwner(TransferOptions{Repository: repository, Artifacts: artifacts})
	if err != nil {
		t.Fatal(err)
	}
	return owner
}

// TestTransferRepairRebindsHeldSupersededCopy pins the review's M1 fix: a
// destination that already holds the byte-identical covered record under a
// strictly superseded provenance re-binds it to the grant's current pair
// through the store (defect #4 ruling) and advances, instead of failing the
// repair as identity reuse; a held copy under an envelope that is not
// ordered before the grant's stays refused.
func TestTransferRepairRebindsHeldSupersededCopy(t *testing.T) {
	fixture := newTransferFixture(t)
	fixture.installRepair(t)
	held := fixture.provenance()
	held.AssignmentRevision--
	fixture.destination.results = []store.StoredResult{{Record: fixture.records[0], Provenance: held}}
	fixture.destination.work.Results = append([]store.StoredResult(nil), fixture.destination.results...)
	owner := mustTransferOwner(t, fixture.destination)
	chunk := fixture.repairChunk(t, fixture.records[0])
	ack, err := owner.ReceiveResultRecord(context.Background(), fixture.repairSourcePeer(), chunk)
	if err != nil {
		t.Fatalf("held superseded copy refused: %v", err)
	}
	if err := protocol.ValidateResultRecordAckCorrelation(chunk, ack); err != nil {
		t.Fatal(err)
	}
	if got := fixture.destination.results[0].Provenance; got != chunk.Provenance {
		t.Fatalf("held copy provenance=%+v want rebound to %+v", got, chunk.Provenance)
	}
	if progress := fixture.destination.repairs[0]; progress.State != store.RepairStreaming || progress.NextRecord != 1 {
		t.Fatalf("durable progress=%+v", progress)
	}
	if want := []string{"result", "repair"}; !equalStrings(fixture.destination.log, want) {
		t.Fatalf("durability order=%v want=%v", fixture.destination.log, want)
	}

	unordered := newTransferFixture(t)
	unordered.installRepair(t)
	foreign := unordered.provenance()
	foreign.DestinationRole = model.PrimaryReplica
	unordered.destination.results = []store.StoredResult{{Record: unordered.records[0], Provenance: foreign}}
	unordered.destination.work.Results = append([]store.StoredResult(nil), unordered.destination.results...)
	if _, err := mustTransferOwner(t, unordered.destination).ReceiveResultRecord(context.Background(), unordered.repairSourcePeer(), unordered.repairChunk(t, unordered.records[0])); !errors.Is(err, ErrTransferIdentityReuse) {
		t.Fatalf("held copy under an unordered envelope err=%v want %v", err, ErrTransferIdentityReuse)
	}
	if got := unordered.destination.repairs[0]; got.NextRecord != 0 {
		t.Fatalf("refused chunk advanced the repair: %+v", got)
	}
}
