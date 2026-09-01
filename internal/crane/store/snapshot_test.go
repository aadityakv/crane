package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/aaditya/cs425mp3/internal/crane/model"
)

func TestSnapshotPersistsCompleteStateReclaimsWALAndContinuesSequence(t *testing.T) {
	path, identity, options, store, fixture := populatedSnapshotStore(t)
	beforeState, beforeWork := store.Recovered(), mustRecoverWork(t, store)
	if len(beforeWork.Assignments) != 1 || len(beforeWork.Sources) != 1 || len(beforeWork.Deliveries) != 1 || len(beforeWork.Results) != 1 || len(beforeWork.Repairs) != 2 || len(beforeWork.PendingEvents) != 2 {
		t.Fatalf("fixture is incomplete: %+v", beforeWork)
	}

	snapshot, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	afterState := store.Recovered()
	if snapshot.Generation != 1 || snapshot.BaseSequence != beforeState.LastSequence || snapshot.TransactionCount != beforeState.TransactionCount {
		t.Fatalf("snapshot=%+v before=%+v", snapshot, beforeState)
	}
	if afterState.SnapshotGeneration != 1 || afterState.SnapshotBytes != snapshot.Bytes || afterState.LastSequence != beforeState.LastSequence || afterState.TransactionCount != beforeState.TransactionCount {
		t.Fatalf("after snapshot metadata=%+v", afterState)
	}
	if afterState.WALBytes >= beforeState.WALBytes {
		t.Fatalf("WAL was not physically reclaimed: before=%d after=%d", beforeState.WALBytes, afterState.WALBytes)
	}
	if got := mustRecoverWork(t, store); !reflect.DeepEqual(got, beforeWork) {
		t.Fatalf("snapshot changed logical state\nbefore=%+v\nafter=%+v", beforeWork, got)
	}

	if err := store.Fence(fixture.epoch); err != nil {
		t.Fatalf("post-snapshot append: %v", err)
	}
	appended := store.Recovered()
	if appended.LastSequence != beforeState.LastSequence+3 || appended.TransactionCount != beforeState.TransactionCount+1 {
		t.Fatalf("global identity did not continue: %+v", appended)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, identity, Options{MaxBytes: options.MaxBytes})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.WorkerEpoch() != (model.WorkerEpoch{7}) || reopened.Recovered() != appended || !reflect.DeepEqual(mustRecoverWork(t, reopened), beforeWork) {
		t.Fatalf("reopen changed snapshot/WAL state: metadata=%+v work=%+v", reopened.Recovered(), mustRecoverWork(t, reopened))
	}
	if state, err := reopened.Receive(fixture.compacted); err != nil || state != Compacted {
		t.Fatalf("late exact compacted retry=%v,%v", state, err)
	}
	changed := fixture.compacted.Clone()
	changed.Tuple.Fields[0].Value.Int64++
	if _, err := reopened.Receive(changed); !errors.Is(err, model.ErrIdentityReuse) {
		t.Fatalf("late changed compacted retry=%v", err)
	}
}

func TestSnapshotSchemaIsDeterministicChecksummedAndOwned(t *testing.T) {
	path, identity, options, store, _ := populatedSnapshotStore(t)
	first, firstSnapshot, firstDigest := snapshotImageForTest(t, store.state, store.work, 1)
	second, secondSnapshot, secondDigest := snapshotImageForTest(t, store.state, store.work, 1)
	if firstSnapshot != secondSnapshot || firstDigest != secondDigest || !bytes.Equal(first, second) {
		t.Fatal("same state/generation produced non-deterministic snapshot bytes")
	}
	if firstSnapshot.Bytes != uint64(len(first)) || firstSnapshot.Generation != 1 {
		t.Fatalf("snapshot metadata=%+v bytes=%d", firstSnapshot, len(first))
	}
	if string(first[:4]) != "CWSS" || binary.BigEndian.Uint16(first[4:6]) != 1 || binary.BigEndian.Uint16(first[6:8]) != snapshotHeaderBytes || binary.BigEndian.Uint64(first[8:16]) != uint64(len(first)) {
		t.Fatalf("snapshot header=%x", first[:16])
	}
	wantDigest := sha256.Sum256(first[:len(first)-snapshotFooterBytes])
	if !bytes.Equal(first[len(first)-snapshotFooterBytes:], wantDigest[:]) {
		t.Fatal("snapshot footer does not bind the exact image")
	}
	if _, err := store.Snapshot(); err != nil {
		t.Fatal(err)
	}
	work := mustRecoverWork(t, store)
	work.Assignments[0].SpecificationBytes[0] ^= 1
	work.Deliveries[0].Tuple.Fields[0].Value.Int64++
	work.Results[0].Record.Value[0] ^= 1
	if reflect.DeepEqual(work, mustRecoverWork(t, store)) {
		t.Fatal("snapshot-published work aliases caller-owned view")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{currentFilename, snapshotFilename(1), generationWALFilename(1)} {
		info, err := os.Lstat(filepath.Join(path, name))
		if err != nil {
			t.Fatal(err)
		}
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
			t.Fatalf("%s mode=%v", name, info.Mode())
		}
	}
	markerBytes, err := os.ReadFile(filepath.Join(path, currentFilename))
	if err != nil {
		t.Fatal(err)
	}
	if len(markerBytes) != currentFileBytes || string(markerBytes[:4]) != "CWCM" || binary.BigEndian.Uint16(markerBytes[4:6]) != 1 {
		t.Fatalf("current marker=%x", markerBytes)
	}
	walBytes, err := os.ReadFile(filepath.Join(path, generationWALFilename(1)))
	if err != nil {
		t.Fatal(err)
	}
	anchor, end, partial, err := decodeRecord(walBytes, 0)
	if err != nil || partial || end != len(walBytes) || anchor.kind != recordSnapshotIdentity || anchor.sequence != firstSnapshot.BaseSequence {
		t.Fatalf("replacement WAL anchor=%+v end=%d partial=%v err=%v", anchor, end, partial, err)
	}
	data, err := os.ReadFile(filepath.Join(path, snapshotFilename(1)))
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)/2] ^= 1
	if err := os.WriteFile(filepath.Join(path, snapshotFilename(1)), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, identity, Options{MaxBytes: options.MaxBytes}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt snapshot open=%v", err)
	}
}

func TestSnapshotProspectiveCapacityReclaimsAFullLegacyWAL(t *testing.T) {
	_, _, _, store, fixture := populatedSnapshotStore(t)
	for i := 0; i < 64; i++ {
		if err := store.Fence(fixture.epoch); err != nil {
			t.Fatal(err)
		}
	}
	metadata, _, err := snapshotMetadata(store.state, store.work, 1)
	if err != nil {
		t.Fatal(err)
	}
	anchor, err := encodeSnapshotAnchor(walSnapshotAnchor{Identity: store.state.Identity, WorkerEpoch: store.state.WorkerEpoch, Generation: 1, BaseSequence: store.state.LastSequence, SnapshotDigest: [32]byte{1}})
	if err != nil {
		t.Fatal(err)
	}
	reserved, err := reservedBytes(store.work)
	if err != nil {
		t.Fatal(err)
	}
	store.options.MaxBytes = metadata.Bytes + uint64(len(anchor)) + reserved
	if store.state.WALBytes+reserved <= store.options.MaxBytes {
		t.Fatalf("fixture WAL is not over prospective capacity: wal=%d reserved=%d final=%d", store.state.WALBytes, reserved, store.options.MaxBytes)
	}
	if _, err := store.Snapshot(); err != nil {
		t.Fatalf("prospective compaction capacity=%v", err)
	}
}

func TestSnapshotRejectsImpossibleCheckedTransactionMetadata(t *testing.T) {
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 1}
	epoch := model.WorkerEpoch{7}
	state := RecoveredState{Identity: identity, WorkerEpoch: epoch, LastSequence: 1, TransactionCount: 1}
	if _, _, err := snapshotMetadata(state, newRecoveredWork(), 1); err == nil {
		t.Fatal("snapshot accepted more transactions than its global sequence can contain")
	}
	current := currentGeneration{Identity: identity, WorkerEpoch: epoch, Generation: 1, BaseSequence: 1, TransactionCount: math.MaxUint64, SnapshotBytes: snapshotHeaderBytes + snapshotFooterBytes, SnapshotDigest: [32]byte{1}}
	if _, err := encodeCurrentGeneration(current); err == nil {
		t.Fatal("current marker accepted overflowing/impossible transaction count")
	}
}

func TestSnapshotConcurrentLifecycleIsSerializedAndReopenable(t *testing.T) {
	path, identity, options, store, fixture := populatedSnapshotStore(t)
	start := make(chan struct{})
	var wait sync.WaitGroup
	for i := 0; i < 8; i++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			for iteration := 0; iteration < 8; iteration++ {
				_, _ = store.RecoverWork()
				if index%2 == 0 {
					_ = store.Fence(fixture.epoch)
				} else {
					_, _ = store.Snapshot()
				}
			}
		}(i)
	}
	wait.Add(1)
	go func() {
		defer wait.Done()
		<-start
		_ = store.Close()
	}()
	close(start)
	wait.Wait()
	reopened, err := Open(path, identity, Options{MaxBytes: options.MaxBytes})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.RecoverWork(); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotPreservesFencedHistoricalAssignmentsResultsRepairsAndEvents(t *testing.T) {
	path, identity, options, store, fixture := populatedSnapshotStore(t)
	newFence := fixture.epoch
	newFence.Term++
	newFence.BeginIndex++
	newFence.Nonce[0]++
	if err := store.Fence(newFence); err != nil {
		t.Fatal(err)
	}
	tokens := append([]model.AssignmentToken(nil), fixture.assignment.Tasks...)
	for index := range tokens {
		tokens[index].AssignmentRevision++
	}
	replacement, err := model.NewAssignmentSet(fixture.assignment.JobID, fixture.assignment.Revision+1, tokens, fixture.assignment.ResultReplicas, fixture.topology)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.InstallAssignment(replacement, fixture.topology.Spec(), 2, model.Running, newFence); err != nil {
		t.Fatal(err)
	}
	before := mustRecoverWork(t, store)
	if before.Fence != newFence || before.Assignments[0].CoordinatorEpoch != newFence || before.Assignments[0].Assignment.Revision != replacement.Revision || before.Deliveries[0].AssignmentRevision != fixture.assignment.Revision || before.Results[0].Provenance.AssignmentRevision != fixture.assignment.Revision || before.Results[0].Provenance.CoordinatorEpoch != fixture.epoch || before.Repairs[0].Instruction.CoordinatorEpoch != fixture.epoch || before.PendingEvents[0].Failure.Epoch != fixture.epoch {
		t.Fatal("fixture does not contain separately fenced historical state")
	}
	if _, err := store.Snapshot(); err != nil {
		t.Fatalf("snapshot rejected valid fenced historical state: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, identity, Options{MaxBytes: options.MaxBytes})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := mustRecoverWork(t, reopened); !reflect.DeepEqual(got, before) {
		t.Fatalf("historical state changed\nbefore=%+v\nafter=%+v", before, got)
	}
}

func TestSnapshotRejectsIdentityEpochMixedGenerationAndUnsafeFiles(t *testing.T) {
	t.Run("identity", func(t *testing.T) {
		path, _, options, store, _ := populatedSnapshotStore(t)
		if _, err := store.Snapshot(); err != nil {
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		wrong := Identity{ClusterID: [16]byte{9}, NodeID: 1}
		if _, err := Open(path, wrong, Options{MaxBytes: options.MaxBytes}); !errors.Is(err, ErrIdentityMismatch) {
			t.Fatalf("foreign identity=%v", err)
		}
	})

	t.Run("mixed generation", func(t *testing.T) {
		path, identity, options, store, _ := populatedSnapshotStore(t)
		if _, err := store.Snapshot(); err != nil {
			t.Fatal(err)
		}
		oldWAL, err := os.ReadFile(filepath.Join(path, generationWALFilename(1)))
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Fence(store.work.Fence); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Snapshot(); err != nil {
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, generationWALFilename(2)), oldWAL, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(path, identity, Options{MaxBytes: options.MaxBytes}); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("mixed snapshot/WAL generation=%v", err)
		}
	})

	for _, kind := range []string{"unknown", "snapshot hardlink", "snapshot symlink"} {
		t.Run(kind, func(t *testing.T) {
			path, identity, options, store, _ := populatedSnapshotStore(t)
			if _, err := store.Snapshot(); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			snapshotPath := filepath.Join(path, snapshotFilename(1))
			switch kind {
			case "unknown":
				if err := os.WriteFile(filepath.Join(path, "foreign"), []byte("x"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "snapshot hardlink":
				if err := os.Link(snapshotPath, filepath.Join(t.TempDir(), "copy")); err != nil {
					t.Fatal(err)
				}
			case "snapshot symlink":
				target := filepath.Join(t.TempDir(), "snapshot")
				data, _ := os.ReadFile(snapshotPath)
				if err := os.WriteFile(target, data, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(snapshotPath); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, snapshotPath); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := Open(path, identity, Options{MaxBytes: options.MaxBytes}); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("unsafe %s open=%v", kind, err)
			}
		})
	}
}

func TestSnapshotCapacityAccountsForSnapshotWALAndReservations(t *testing.T) {
	_, _, _, store, fixture := populatedSnapshotStore(t)
	if _, err := store.Snapshot(); err != nil {
		t.Fatal(err)
	}
	beforeState, beforeWork := store.Recovered(), mustRecoverWork(t, store)
	payload, err := encodeDeliveryRecord(fixture.next, nil)
	if err != nil {
		t.Fatal(err)
	}
	txBytes, err := transactionEncodedSize(Transaction{Records: []Record{{Type: recordDelivery, Payload: payload}}})
	if err != nil {
		t.Fatal(err)
	}
	store.options.MaxBytes = beforeState.SnapshotBytes + beforeState.WALBytes + txBytes + fixture.next.Reservation - 1
	if _, err := store.Receive(fixture.next); !errors.Is(err, ErrCapacity) {
		t.Fatalf("snapshot-aware capacity=%v", err)
	}
	if store.Recovered() != beforeState || !reflect.DeepEqual(mustRecoverWork(t, store), beforeWork) {
		t.Fatal("snapshot-aware capacity rejection mutated state")
	}
}

func TestSnapshotRecoveryRejectsCommittedCorruptionForEveryDomainRecordKind(t *testing.T) {
	for _, kind := range []RecordType{recordFence, recordAssignment, recordDelivery, recordDeliveryProcessed, recordDeliveryCompleted, recordCheckpoint, recordResult, recordEvent, recordEventAck, recordRepair, recordSource, recordOutboxAck} {
		t.Run(domainRecordName(kind), func(t *testing.T) {
			path, identity, options, store, fixture := populatedSnapshotStore(t)
			if _, err := store.Snapshot(); err != nil {
				t.Fatal(err)
			}
			record := corruptPostSnapshotRecord(t, store, fixture, kind)
			if err := commitRawForTest(store, Transaction{Records: []Record{record}}); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(path, identity, Options{MaxBytes: options.MaxBytes}); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("committed %s cross-reference corruption=%v", domainRecordName(kind), err)
			}
		})
	}
}

func TestSnapshotRejectsOversizedSparseFileBeforeWholeFileAllocation(t *testing.T) {
	path, identity, options, store, _ := populatedSnapshotStore(t)
	if _, err := store.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(filepath.Join(path, snapshotFilename(1)), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(int64(options.MaxBytes) + 1); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	var openErr error
	allocated := allocatedBytesForTest(func() {
		opened, err := Open(path, identity, Options{MaxBytes: options.MaxBytes})
		if opened != nil {
			_ = opened.Close()
		}
		openErr = err
	}, 1)
	if !errors.Is(openErr, ErrCorrupt) {
		t.Fatalf("oversized snapshot=%v", openErr)
	}
	if allocated > 2<<20 {
		t.Fatalf("oversized snapshot allocated %d bytes", allocated)
	}
}

type snapshotFixture struct {
	topology   model.ValidatedTopology
	assignment model.AssignmentSet
	epoch      model.CoordinatorEpoch
	compacted  DeliveryRecord
	next       DeliveryRecord
}

func snapshotImageForTest(t *testing.T, state RecoveredState, work RecoveredWork, generation uint64) ([]byte, Snapshot, [32]byte) {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "snapshot")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	snapshot, digest, err := writeSnapshotFile(file, state, work, generation, func(file *os.File, data []byte) (int, error) { return file.Write(data) })
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	return data, snapshot, digest
}

func domainRecordName(kind RecordType) string {
	names := map[RecordType]string{
		recordFence: "fence", recordAssignment: "assignment", recordDelivery: "delivery",
		recordDeliveryProcessed: "processed", recordDeliveryCompleted: "completed", recordCheckpoint: "checkpoint",
		recordResult: "result", recordEvent: "event", recordEventAck: "event-ack", recordRepair: "repair",
		recordSource: "source", recordOutboxAck: "outbox-ack",
	}
	return names[kind]
}

func corruptPostSnapshotRecord(t *testing.T, store *Store, fixture snapshotFixture, kind RecordType) Record {
	t.Helper()
	work := mustRecoverWork(t, store)
	encode := func(payload []byte, err error) Record {
		if err != nil {
			t.Fatal(err)
		}
		return Record{Type: kind, Payload: payload}
	}
	older := fixture.epoch
	older.Term--
	switch kind {
	case recordFence:
		return encode(encodeFence(older))
	case recordAssignment:
		assignment := work.Assignments[0]
		assignment.CoordinatorEpoch = older
		return encode(encodeAssignment(assignment))
	case recordDelivery:
		delivery := fixture.next.Clone()
		delivery.Destination.WorkerID = 2
		return encode(encodeDeliveryRecord(delivery, nil))
	case recordDeliveryProcessed:
		delivery := fixture.next.Clone()
		delivery.State = Processed
		delivery.Outputs, _ = model.ExecuteOperator(fixture.topology.Spec().Stages[1].Operator, delivery.Tuple)
		outboxes := domainOutboxesForOutputs(t, delivery, delivery.Outputs, fixture.assignment, fixture.topology)
		return encode(encodeDeliveryRecord(delivery, outboxes))
	case recordDeliveryCompleted:
		return encode(encodeDeliveryIDPayload(fixture.next.ID))
	case recordCheckpoint:
		cursor := work.Sources[0]
		return encode(encodeCheckpoint(model.CheckpointNotice{JobID: fixture.assignment.JobID, Source: cursor.Source, Watermark: 0, RaftIndex: 10, Epoch: fixture.epoch}))
	case recordResult:
		result, provenance := domainResultSequence(t, fixture.topology, fixture.assignment, fixture.epoch, 0, 7)
		provenance.CoordinatorEpoch = older
		return encode(encodeStoredResult(StoredResult{Record: result, Provenance: provenance}))
	case recordEvent:
		event := domainFailureEvent(store, fixture.assignment, fixture.epoch, work.NextTransactionID)
		event.Failure.JobControlRevision++
		return encode(encodeEvent(event))
	case recordEventAck:
		return Record{Type: kind, Payload: encodeUint64Payload(work.NextTransactionID)}
	case recordRepair:
		repair := domainRepair(t, fixture.topology, fixture.assignment, older, store.state.Identity.NodeID, store.state.WorkerEpoch)
		return encode(encodeRepair(repair))
	case recordSource:
		cursor := work.Sources[0]
		cursor.NextSequence = 1
		return encode(encodeSource(cursor, nil))
	case recordOutboxAck:
		return encode(encodeDeliveryIDPayload(fixture.next.ID))
	default:
		t.Fatalf("unknown kind %d", kind)
		return Record{}
	}
}

func populatedSnapshotStore(t *testing.T) (string, Identity, Options, *Store, snapshotFixture) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "worker")
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 1}
	options := Options{MaxBytes: 32 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return model.WorkerEpoch{7}, nil }}
	store, err := Open(path, identity, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	topology, assignment, epoch := domainAssignmentWithRange(t, store.WorkerEpoch(), identity.NodeID, 8)
	if err := store.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	var source model.AssignmentToken
	for _, token := range assignment.Tasks {
		if token.Task.StageID == 1 && token.WorkerID == identity.NodeID && token.WorkerEpoch == store.WorkerEpoch() {
			source = token
			break
		}
	}
	if source == (model.AssignmentToken{}) {
		t.Fatal("fixture has no local source")
	}
	sourceOutbox := domainSourceOutbox(t, topology, assignment, epoch, source, 1)
	if err := store.AdvanceSource(SourceCursor{Source: source.Task, NextSequence: 2, EOF: 8, Watermark: 0, RaftIndex: 0}, []OutboxRecord{sourceOutbox}); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkOutboxCompleted(sourceOutbox.ID); err != nil {
		t.Fatal(err)
	}
	compacted := domainDeliverySequence(t, topology, assignment, epoch, 1)
	if _, err := store.Receive(compacted); err != nil {
		t.Fatal(err)
	}
	outputs, outboxes := exactProcessedRecords(t, topology, assignment, compacted)
	if err := store.MarkProcessed(compacted.ID, outputs, outboxes); err != nil {
		t.Fatal(err)
	}
	for _, outbox := range outboxes {
		if err := store.MarkOutboxCompleted(outbox.ID); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.MarkCompleted(compacted.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyCheckpoint(model.CheckpointNotice{JobID: assignment.JobID, Source: source.Task, Watermark: 1, RaftIndex: 11, Epoch: epoch}); err != nil {
		t.Fatal(err)
	}
	next := domainDeliverySequence(t, topology, assignment, epoch, 2)
	if _, err := store.Receive(next); err != nil {
		t.Fatal(err)
	}
	record, provenance := domainResult(t, topology, assignment, epoch, 0)
	if err := store.UpsertResult(record, provenance); err != nil {
		t.Fatal(err)
	}
	pending := domainRepair(t, topology, assignment, epoch, identity.NodeID, store.WorkerEpoch())
	if err := store.UpsertRepair(pending); err != nil {
		t.Fatal(err)
	}
	if err := store.PersistEvent(domainFailureEvent(store, assignment, epoch, 1)); err != nil {
		t.Fatal(err)
	}
	completed := pending
	replica := assignment.ResultReplicas[1]
	completed.Instruction.SinkTask = replica.SinkTask
	if replica.PrimaryNodeID == identity.NodeID && replica.PrimaryEpoch == store.WorkerEpoch() {
		completed.Instruction.SourceNodeID, completed.Instruction.SourceWorkerEpoch = replica.PrimaryNodeID, replica.PrimaryEpoch
		completed.Instruction.DestinationNodeID, completed.Instruction.DestinationWorkerEpoch = replica.SecondaryNodeID, replica.SecondaryEpoch
		completed.Role = RepairSource
	} else {
		completed.Instruction.SourceNodeID, completed.Instruction.SourceWorkerEpoch = replica.SecondaryNodeID, replica.SecondaryEpoch
		completed.Instruction.DestinationNodeID, completed.Instruction.DestinationWorkerEpoch = replica.PrimaryNodeID, replica.PrimaryEpoch
		completed.Role = RepairSource
	}
	completed.State = RepairComplete
	completed.NextRecord = completed.Instruction.ExpectedRecordCount
	completed.NextOffset = completed.Instruction.ExpectedTotalBytes
	completed.RecordCount = completed.Instruction.ExpectedRecordCount
	completed.TotalBytes = completed.Instruction.ExpectedTotalBytes
	completed.ContentDigest = completed.Instruction.ExpectedContentDigest
	rebindRepair(&completed)
	if err := store.UpsertRepair(completed); err != nil {
		t.Fatal(err)
	}
	report := model.CompletionReport{JobID: assignment.JobID, JobControlRevision: 1, AssignmentRevision: assignment.Revision, Source: source.Task, Token: source, Epoch: epoch, Prior: 1, New: 2, EOF: 8, WorkerTransactionID: 2}
	report.Digest = model.CompletionReportDigest(report)
	completion := model.WorkerEvent{WorkerID: identity.NodeID, WorkerEpoch: store.WorkerEpoch(), TransactionID: 2, Kind: model.WorkerEventCompletion, Completion: &report}
	if err := store.PersistEvent(completion); err != nil {
		t.Fatal(err)
	}
	return path, identity, options, store, snapshotFixture{topology: topology, assignment: assignment, epoch: epoch, compacted: compacted, next: domainDeliverySequence(t, topology, assignment, epoch, 3)}
}
