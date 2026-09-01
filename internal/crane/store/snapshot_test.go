package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
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
	anchor, err := encodeSnapshotAnchor(walSnapshotAnchor{Identity: store.state.Identity, WorkerEpoch: store.state.WorkerEpoch, Generation: 1, BaseSequence: store.state.LastSequence, TransactionCount: store.state.TransactionCount, SnapshotDigest: [32]byte{1}})
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
	for _, test := range []struct {
		name               string
		base, transactions uint64
		valid              bool
	}{
		{name: "empty exact", base: 1, valid: true},
		{name: "empty too large", base: 2},
		{name: "one below minimum", base: 3, transactions: 1},
		{name: "one minimum", base: 4, transactions: 1, valid: true},
		{name: "one mixed", base: 17, transactions: 1, valid: true},
		{name: "one maximum", base: MaxTransactionRecords + 3, transactions: 1, valid: true},
		{name: "one above maximum", base: MaxTransactionRecords + 4, transactions: 1},
		{name: "two minimum", base: 7, transactions: 2, valid: true},
		{name: "two maximum", base: 1 + 2*(MaxTransactionRecords+2), transactions: 2, valid: true},
		{name: "lower overflow", base: math.MaxUint64, transactions: math.MaxUint64/3 + 1},
		{name: "upper overflow cannot excuse low base", base: 2, transactions: math.MaxUint64},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := validSnapshotTransactionMetadata(test.base, test.transactions); got != test.valid {
				t.Fatalf("validSnapshotTransactionMetadata(%d,%d)=%v want=%v", test.base, test.transactions, got, test.valid)
			}
			state := RecoveredState{Identity: identity, WorkerEpoch: epoch, LastSequence: test.base, TransactionCount: test.transactions}
			_, _, snapshotErr := snapshotMetadata(state, newRecoveredWork(), 1)
			current := currentGeneration{Identity: identity, WorkerEpoch: epoch, Generation: 1, BaseSequence: test.base, TransactionCount: test.transactions, SnapshotBytes: snapshotHeaderBytes + snapshotFooterBytes, SnapshotDigest: [32]byte{1}}
			_, currentErr := encodeCurrentGeneration(current)
			if (snapshotErr == nil) != test.valid || (currentErr == nil) != test.valid {
				t.Fatalf("artifact validation snapshot=%v current=%v valid=%v", snapshotErr, currentErr, test.valid)
			}
		})
	}

	for _, test := range []struct {
		name               string
		base, transactions uint64
		valid              bool
	}{
		{name: "anchor minimum", base: 4, transactions: 1, valid: true},
		{name: "anchor maximum", base: MaxTransactionRecords + 3, transactions: 1, valid: true},
		{name: "anchor below", base: 3, transactions: 1},
		{name: "anchor above", base: MaxTransactionRecords + 4, transactions: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload := rawSnapshotAnchorPayloadForTest(identity, epoch, 1, test.base, test.transactions, [32]byte{1})
			decoded, err := decodeSnapshotAnchor(payload)
			if (err == nil) != test.valid {
				t.Fatalf("decodeSnapshotAnchor=%v valid=%v", err, test.valid)
			}
			if test.valid {
				field := reflect.ValueOf(decoded).FieldByName("TransactionCount")
				if !field.IsValid() || field.Uint() != test.transactions {
					t.Fatalf("anchor transaction count field=%v want=%d", field.IsValid(), test.transactions)
				}
			}
		})
	}
}

func TestSnapshotEventIdentityIsTransactionBoundedAndCannotWrap(t *testing.T) {
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 1}
	epoch := model.WorkerEpoch{7}
	for _, test := range []struct {
		name, next   string
		transactions uint64
		valid        bool
	}{
		{name: "initial", next: "1", valid: true},
		{name: "exact", next: "2", transactions: 1, valid: true},
		{name: "ahead", next: "3", transactions: 1},
		{name: "zero", next: "0"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var next uint64
			for _, digit := range []byte(test.next) {
				next = next*10 + uint64(digit-'0')
			}
			work := newRecoveredWork()
			work.NextTransactionID = next
			state := RecoveredState{Identity: identity, WorkerEpoch: epoch, LastSequence: 1 + 3*test.transactions, TransactionCount: test.transactions}
			_, _, err := snapshotMetadata(state, work, 1)
			if (err == nil) != test.valid {
				t.Fatalf("snapshotMetadata=%v valid=%v", err, test.valid)
			}
		})
	}

	for _, raw := range []bool{false, true} {
		t.Run(map[bool]string{false: "public", true: "raw"}[raw], func(t *testing.T) {
			_, _, _, store, fixture := populatedSnapshotStore(t)
			store.work.NextTransactionID = math.MaxUint64
			event := domainFailureEvent(store, fixture.assignment, fixture.epoch, math.MaxUint64)
			beforeState, beforeWork := store.Recovered(), mustRecoverWork(t, store)
			fault := &oneShotFault{point: FaultBeforeAppend, err: errors.New("must not reach WAL")}
			store.options.Faults = fault
			var err error
			if raw {
				payload, encodeErr := encodeEvent(event)
				if encodeErr != nil {
					t.Fatal(encodeErr)
				}
				err = store.Commit(Transaction{Records: []Record{{Type: recordEvent, Payload: payload}}})
			} else {
				err = store.PersistEvent(event)
			}
			if err == nil || fault.fired || store.Recovered() != beforeState || !reflect.DeepEqual(mustRecoverWork(t, store), beforeWork) {
				t.Fatalf("wrap admission err=%v fault=%v metadata=%+v", err, fault.fired, store.Recovered())
			}
		})
	}
}

func TestSnapshotRejectsAckedEmptyEventIdentityAheadOfTransactionCount(t *testing.T) {
	_, _, _, store, _ := populatedSnapshotStore(t)
	assignment := store.work.Assignments[0]
	delivery := store.work.Deliveries[0]
	outputs, outboxes := exactProcessedRecords(t, assignment.Topology, assignment.Assignment, delivery)
	if err := store.MarkProcessed(delivery.ID, outputs, outboxes); err != nil {
		t.Fatal(err)
	}
	for _, outbox := range outboxes {
		if err := store.MarkOutboxCompleted(outbox.ID); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.MarkCompleted(delivery.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyCheckpoint(model.CheckpointNotice{JobID: assignment.Assignment.JobID, Source: store.work.Sources[0].Source, Watermark: 2, RaftIndex: 12, Epoch: assignment.CoordinatorEpoch}); err != nil {
		t.Fatal(err)
	}
	replaceAssignmentStateForTest(t, store, assignment.Assignment, assignment.Topology, assignment.JobControlRevision+1, model.Closed, assignment.CoordinatorEpoch)
	if err := store.AcknowledgeEvents(store.work.NextTransactionID - 1); err != nil {
		t.Fatal(err)
	}
	if len(store.work.PendingEvents) != 0 {
		t.Fatal("event fixture was not fully acknowledged")
	}
	work := store.work
	domainRecords := store.state.LastSequence - 1 - 2*store.state.TransactionCount
	work.NextTransactionID = domainRecords + 1
	if err := recoverSnapshotImageWithNextForTest(t, store.state, work, domainRecords+2); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("acked-empty oversized next transaction recovered: %v", err)
	}
	if err := recoverSnapshotImageForTest(t, store.state, work); err != nil {
		t.Fatalf("exact acked-empty event boundary rejected: %v", err)
	}
}

func TestSnapshotEventAccountingUsesDurableDomainRecordCount(t *testing.T) {
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 1}
	epoch := model.WorkerEpoch{7}
	for _, test := range []struct {
		name               string
		base, transactions uint64
		next               uint64
		valid              bool
	}{
		{name: "empty", base: 1, next: 1, valid: true},
		{name: "minimum one-record transaction exact", base: 4, transactions: 1, next: 2, valid: true},
		{name: "minimum one-record transaction plus one", base: 4, transactions: 1, next: 3},
		{name: "two events in one transaction exact", base: 5, transactions: 1, next: 3, valid: true},
		{name: "two events in one transaction plus one", base: 5, transactions: 1, next: 4},
		{name: "maximum record transaction exact", base: MaxTransactionRecords + 3, transactions: 1, next: MaxTransactionRecords + 1, valid: true},
		{name: "maximum record transaction plus one", base: MaxTransactionRecords + 3, transactions: 1, next: MaxTransactionRecords + 2},
		{name: "zero next", base: 4, transactions: 1},
		{name: "subtraction edge", base: 3, transactions: 1, next: 1},
		{name: "multiplication edge", base: math.MaxUint64, transactions: math.MaxUint64/3 + 1, next: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			work := newRecoveredWork()
			work.NextTransactionID = test.next
			state := RecoveredState{Identity: identity, WorkerEpoch: epoch, LastSequence: test.base, TransactionCount: test.transactions}
			_, _, snapshotErr := snapshotMetadata(state, work, 1)
			current := currentGeneration{Identity: identity, WorkerEpoch: epoch, Generation: 1, BaseSequence: test.base, TransactionCount: test.transactions, SnapshotBytes: snapshotHeaderBytes + snapshotFooterBytes, SnapshotDigest: [32]byte{1}}
			_, currentErr := encodeCurrentGeneration(current)
			_, anchorErr := encodeSnapshotAnchor(walSnapshotAnchor{Identity: identity, WorkerEpoch: epoch, Generation: 1, BaseSequence: test.base, TransactionCount: test.transactions, SnapshotDigest: [32]byte{1}})
			if (snapshotErr == nil) != test.valid {
				t.Fatalf("snapshot metadata=%v valid=%v", snapshotErr, test.valid)
			}
			metadataValid := validSnapshotTransactionMetadata(test.base, test.transactions)
			if (currentErr == nil) != metadataValid || (anchorErr == nil) != metadataValid {
				t.Fatalf("marker=%v anchor=%v metadataValid=%v", currentErr, anchorErr, metadataValid)
			}
		})
	}

	for _, count := range []int{2, MaxTransactionRecords} {
		t.Run(fmt.Sprintf("raw batch %d", count), func(t *testing.T) {
			path, identity, options, store, assignment, epoch := eventBatchSnapshotStore(t)
			records := make([]Record, count)
			for index := range records {
				event := domainFailureEvent(store, assignment, epoch, uint64(index+1))
				payload, err := encodeEvent(event)
				if err != nil {
					t.Fatal(err)
				}
				records[index] = Record{Type: recordEvent, Payload: payload}
			}
			if err := store.Commit(Transaction{Records: records}); err != nil {
				t.Fatalf("sequential event batch=%v", err)
			}
			if got := store.work.NextTransactionID; got != uint64(count)+1 {
				t.Fatalf("next event ID=%d want=%d", got, count+1)
			}
			if _, err := store.Snapshot(); err != nil {
				t.Fatalf("snapshot batched events=%v", err)
			}
			installed := store.work.Assignments[0]
			replaceAssignmentStateForTest(t, store, assignment, installed.Topology, installed.JobControlRevision+1, model.Closed, epoch)
			if err := store.AcknowledgeEvents(uint64(count)); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Snapshot(); err != nil {
				t.Fatalf("snapshot acknowledged-empty events=%v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(path, identity, Options{MaxBytes: options.MaxBytes})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			work, err := reopened.RecoverWork()
			if err != nil || len(work.PendingEvents) != 0 || work.NextTransactionID != uint64(count)+1 {
				t.Fatalf("recovered acknowledged events=%d next=%d err=%v", len(work.PendingEvents), work.NextTransactionID, err)
			}
		})
	}
}

func TestSnapshotResultPreflightUsesArtifactEntryBounds(t *testing.T) {
	path, identity, options, store, fixture := populatedSnapshotStore(t)
	makeResult := func(sequence uint64, tuple []byte) (model.ResultRecord, model.ResultCopyProvenance) {
		record, provenance := domainResultSequence(t, fixture.topology, fixture.assignment, fixture.epoch, 0, sequence)
		var err error
		record, err = model.NewResultRecord(record.TupleID, record.SinkTask, record.SpecificationHash, tuple)
		if err != nil {
			t.Fatal(err)
		}
		return record, provenance
	}
	emptyTuple, err := model.MarshalTuple(model.Tuple{})
	if err != nil {
		t.Fatal(err)
	}
	maxTuple, err := model.MarshalTuple(model.Tuple{Fields: []model.Field{{Name: "x", Value: model.Value{Type: model.ValueBytes, Bytes: make([]byte, 504)}}}})
	if err != nil {
		t.Fatal(err)
	}
	minimum, minProvenance := makeResult(50, emptyTuple)
	maximum, maxProvenance := makeResult(51, maxTuple)
	minLogical, err := model.MarshalResultRecord(minimum)
	if err != nil {
		t.Fatal(err)
	}
	maxLogical, err := model.MarshalResultRecord(maximum)
	if err != nil {
		t.Fatal(err)
	}
	if len(minLogical) != 166 || len(maxLogical) != 676 || uint64(len(minLogical)+4) != model.ResultArtifactMinRecordBytesV1 || uint64(len(maxLogical)+4) != model.ResultArtifactMaxRecordBytesV1 {
		t.Fatalf("logical/artifact boundaries min=%d/%d max=%d/%d", len(minLogical), len(minLogical)+4, len(maxLogical), len(maxLogical)+4)
	}
	minPayload, err := encodeStoredResult(StoredResult{Record: minimum, Provenance: minProvenance})
	if err != nil {
		t.Fatal(err)
	}
	maxPayload, err := encodeStoredResult(StoredResult{Record: maximum, Provenance: maxProvenance})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		payload []byte
		valid   bool
	}{
		{name: "minimum", payload: minPayload, valid: true},
		{name: "maximum", payload: maxPayload, valid: true},
		{name: "below minimum", payload: resultPayloadWithDeclaredLogicalLength(t, minPayload, 165)},
		{name: "above maximum", payload: resultPayloadWithDeclaredLogicalLength(t, maxPayload, 677)},
	} {
		t.Run(test.name, func(t *testing.T) {
			work := newRecoveredWork()
			work.Fence = fixture.epoch
			work.Assignments = append(work.Assignments, store.work.Assignments[0])
			decoder := snapshotDecoder{work: work}
			err := decoder.consume(snapshotResult, test.payload)
			if (err == nil) != test.valid {
				t.Fatalf("result preflight=%v valid=%v", err, test.valid)
			}
		})
	}

	entryBytes := uint64(len(minLogical) + 4)
	for _, test := range []struct {
		name  string
		prior uint64
		valid bool
	}{
		{name: "aggregate exact", prior: model.LimitsV1().MaxResultRecordsBytesPerJob - entryBytes, valid: true},
		{name: "aggregate over", prior: model.LimitsV1().MaxResultRecordsBytesPerJob - entryBytes + 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			work := newRecoveredWork()
			work.Fence = fixture.epoch
			work.Assignments = append(work.Assignments, store.work.Assignments[0])
			work.indexes.resultBytesByJob[minimum.TupleID.JobID] = test.prior
			decoder := snapshotDecoder{work: work}
			err := decoder.consume(snapshotResult, minPayload)
			if (err == nil) != test.valid {
				t.Fatalf("aggregate preflight=%v valid=%v", err, test.valid)
			}
			if test.valid && decoder.work.indexes.resultBytesByJob[minimum.TupleID.JobID] != model.LimitsV1().MaxResultRecordsBytesPerJob {
				t.Fatalf("aggregate bytes=%d", decoder.work.indexes.resultBytesByJob[minimum.TupleID.JobID])
			}
		})
	}

	if err := store.UpsertResult(minimum, minProvenance); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertResult(maximum, maxProvenance); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Snapshot(); err != nil {
		t.Fatalf("snapshot legal result boundaries=%v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, identity, Options{MaxBytes: options.MaxBytes})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	work, err := reopened.RecoverWork()
	if err != nil {
		t.Fatal(err)
	}
	found := map[int]bool{}
	for _, result := range work.Results {
		logical, marshalErr := model.MarshalResultRecord(result.Record)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		found[len(logical)] = true
	}
	if !found[166] || !found[676] {
		t.Fatalf("recovered result logical sizes=%v", found)
	}
}

func TestSnapshotCheckpointEvidenceRequiresRaftIndexAndPinsZeroWatermark(t *testing.T) {
	path, identity, options, store, fixture := populatedSnapshotStore(t)
	work := store.work.Clone()
	work.Sources[0].RaftIndex = 0
	if err := recoverSnapshotImageForTest(t, store.state, work); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("watermark without committed Raft index recovered: %v", err)
	}
	work.Sources[0].Watermark = 0
	work.Sources[0].RaftIndex = 0
	work.Sources[0].CheckpointRevision = 0
	work.Sources[0].CheckpointAuthority = CheckpointAuthority{}
	if err := recoverSnapshotImageForTest(t, store.state, work); err != nil {
		t.Fatalf("initial zero watermark/index rejected: %v", err)
	}
	work.Sources[0].RaftIndex = 12
	if err := recoverSnapshotImageForTest(t, store.state, work); err != nil {
		t.Fatalf("committed zero-watermark checkpoint rejected: %v", err)
	}
	if _, err := store.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, identity, Options{MaxBytes: options.MaxBytes})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if state, err := reopened.Receive(fixture.compacted); err != nil || state != Compacted {
		t.Fatalf("late retry after checkpoint snapshot=%v,%v", state, err)
	}
}

func TestSnapshotRejectsNoncanonicalNestedOutboxOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker")
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 1}
	store, err := Open(path, identity, Options{MaxBytes: 8 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return model.WorkerEpoch{7}, nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	spec := domainTopologySpec(8)
	spec.Stages[2] = model.StageSpec{StageID: 3, Name: "fanout", Role: model.StageTransform, Parallelism: 2, Operator: model.OperatorSpec{Name: "multiply", Version: 1, Settings: []model.Setting{{Key: "factor", Value: "1"}}}}
	spec.Stages = append(spec.Stages, model.StageSpec{StageID: 4, Name: "sink", Role: model.StageSink, Parallelism: 2, Operator: model.OperatorSpec{Name: "collect", Version: 1}})
	spec.Edges[1].Routing = model.RoutingBroadcast
	spec.Edges[1].Field = ""
	spec.Edges = append(spec.Edges, model.EdgeSpec{EdgeID: 3, SourceStageID: 3, DestinationStageID: 4, Routing: model.RoutingFieldHash, Field: "value"})
	topology, assignment, epoch := domainAssignmentFromSpec(t, store.WorkerEpoch(), identity.NodeID, spec)
	if err := store.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	delivery := domainDeliverySequence(t, topology, assignment, epoch, 1)
	if _, err := store.Receive(delivery); err != nil {
		t.Fatal(err)
	}
	outputs, outboxes := exactProcessedRecords(t, topology, assignment, delivery)
	if len(outboxes) < 2 {
		t.Fatalf("fixture needs at least two derived outboxes, got %d", len(outboxes))
	}
	if err := store.MarkProcessed(delivery.ID, outputs, outboxes); err != nil {
		t.Fatal(err)
	}
	work := store.work.Clone()
	index := deliveryIndex(work.Deliveries, delivery.ID)
	work.Deliveries[index].OutboxIDs[0], work.Deliveries[index].OutboxIDs[1] = work.Deliveries[index].OutboxIDs[1], work.Deliveries[index].OutboxIDs[0]
	if err := recoverSnapshotImageForTest(t, store.state, work); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("checksummed swapped nested outboxes recovered: %v", err)
	}
}

func TestSnapshotDecoderRejectsEveryCollectionOverflowBeforeDecodeAllocation(t *testing.T) {
	_, _, _, store, fixture := populatedSnapshotStore(t)
	base := store.work.Clone()
	assignmentPayload, err := encodeAssignment(base.Assignments[0])
	if err != nil {
		t.Fatal(err)
	}
	sourcePayload, err := encodeSource(base.Sources[0], nil)
	if err != nil {
		t.Fatal(err)
	}
	deliveryPayload, err := encodeDeliveryRecord(fixture.next, nil)
	if err != nil {
		t.Fatal(err)
	}
	resultPayload, err := encodeStoredResult(base.Results[0])
	if err != nil {
		t.Fatal(err)
	}
	repairPayload, err := encodeRepair(base.Repairs[0])
	if err != nil {
		t.Fatal(err)
	}
	eventPayload, err := encodeEvent(base.PendingEvents[0])
	if err != nil {
		t.Fatal(err)
	}
	fencePayload, err := encodeFence(base.Fence)
	if err != nil {
		t.Fatal(err)
	}
	maxSources := model.LimitsV1().MaxRetainedJobs * model.LimitsV1().MaxTasksPerJob
	if maxSources > uint64(math.MaxInt) {
		t.Fatal("source cap exceeds test address space")
	}

	tests := []struct {
		name    string
		kind    snapshotRecordKind
		payload []byte
		seed    func() RecoveredWork
		budget  uint64
	}{
		{name: "fence", kind: snapshotFence, payload: fencePayload, seed: func() RecoveredWork { return base.Clone() }, budget: 1024},
		{name: "assignments", kind: snapshotAssignment, payload: assignmentPayload, seed: func() RecoveredWork {
			work := newRecoveredWork()
			work.Fence = base.Fence
			work.Assignments = make([]InstalledAssignment, int(model.LimitsV1().MaxRetainedJobs))
			return work
		}, budget: 1024},
		{name: "sources", kind: snapshotSource, payload: sourcePayload, seed: func() RecoveredWork {
			work := newRecoveredWork()
			work.Sources = make([]SourceCursor, int(maxSources))
			return work
		}, budget: 1024},
		{name: "deliveries", kind: snapshotDelivery, payload: deliveryPayload, seed: func() RecoveredWork {
			work := newRecoveredWork()
			work.Deliveries = make([]DeliveryRecord, MaxTransactionRecords)
			return work
		}, budget: 1024},
		{name: "results", kind: snapshotResult, payload: resultPayload, seed: func() RecoveredWork {
			work := newRecoveredWork()
			work.Fence = base.Fence
			work.Assignments = append([]InstalledAssignment(nil), base.Assignments...)
			work.indexes.resultCount = maxStoredResultCount()
			work.indexes.resultBytesByJob[base.Results[0].Record.TupleID.JobID] = model.LimitsV1().MaxResultRecordsBytesPerJob
			return work
		}, budget: 1024},
		{name: "repairs", kind: snapshotRepair, payload: repairPayload, seed: func() RecoveredWork {
			work := newRecoveredWork()
			work.Fence = base.Fence
			work.Assignments = append([]InstalledAssignment(nil), base.Assignments...)
			work.Repairs = make([]ResultRepairRecord, 64)
			return work
		}, budget: 1024},
		{name: "events", kind: snapshotEvent, payload: eventPayload, seed: func() RecoveredWork {
			work := newRecoveredWork()
			work.PendingEvents = make([]model.WorkerEvent, MaxTransactionRecords)
			return work
		}, budget: 1024},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decoder := snapshotDecoder{work: test.seed()}
			var consumeErr error
			allocated := allocatedBytesForTest(func() { consumeErr = decoder.consume(test.kind, test.payload) }, 1)
			if consumeErr == nil {
				t.Fatal("collection overflow was accepted")
			}
			if allocated > test.budget {
				t.Fatalf("overflow allocated %d bytes before rejection; budget=%d", allocated, test.budget)
			}
		})
	}

	t.Run("nested delivery outboxes", func(t *testing.T) {
		payload := append([]byte(nil), deliveryPayload...)
		binary.BigEndian.PutUint16(payload[len(payload)-2:], uint16(model.LimitsV1().MaxDerivedDeliveries+1))
		decoder := snapshotDecoder{work: newRecoveredWork()}
		var consumeErr error
		allocated := allocatedBytesForTest(func() { consumeErr = decoder.consume(snapshotDelivery, payload) }, 1)
		if consumeErr == nil {
			t.Fatal("nested outbox overflow was accepted")
		}
		if allocated > 1024 {
			t.Fatalf("nested overflow allocated %d bytes before rejection", allocated)
		}
	})
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
	for _, kind := range []RecordType{recordFence, recordAssignment, recordDelivery, recordDeliveryProcessed, recordDeliveryCompleted, recordCheckpoint, recordResult, recordEvent, recordEventAck, recordRepair, recordSource, recordOutboxAck, recordOutboxRetry} {
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

func recoverSnapshotImageForTest(t *testing.T, state RecoveredState, work RecoveredWork) error {
	t.Helper()
	data, snapshot, digest := snapshotImageForTest(t, state, work, 1)
	current := currentGeneration{Identity: state.Identity, WorkerEpoch: state.WorkerEpoch, Generation: 1, BaseSequence: state.LastSequence, TransactionCount: state.TransactionCount, SnapshotBytes: snapshot.Bytes, SnapshotDigest: digest}
	_, _, err := recoverSnapshotReader(bytes.NewReader(data), int64(len(data)), state.Identity, current, uint64(len(data)))
	return err
}

func recoverSnapshotImageWithNextForTest(t *testing.T, state RecoveredState, work RecoveredWork, next uint64) error {
	t.Helper()
	data, snapshot, _ := snapshotImageForTest(t, state, work, 1)
	binary.BigEndian.PutUint64(data[74:82], next)
	binary.BigEndian.PutUint32(data[100:104], crc32.Checksum(data[:100], walCRC))
	digest := sha256.Sum256(data[:len(data)-snapshotFooterBytes])
	copy(data[len(data)-snapshotFooterBytes:], digest[:])
	current := currentGeneration{Identity: state.Identity, WorkerEpoch: state.WorkerEpoch, Generation: 1, BaseSequence: state.LastSequence, TransactionCount: state.TransactionCount, SnapshotBytes: snapshot.Bytes, SnapshotDigest: digest}
	_, _, err := recoverSnapshotReader(bytes.NewReader(data), int64(len(data)), state.Identity, current, uint64(len(data)))
	return err
}

func rawSnapshotAnchorPayloadForTest(identity Identity, epoch model.WorkerEpoch, generation, baseSequence, transactionCount uint64, digest [32]byte) []byte {
	payload := make([]byte, 92)
	binary.BigEndian.PutUint16(payload[:2], 1)
	copy(payload[2:18], identity.ClusterID[:])
	binary.BigEndian.PutUint16(payload[18:20], identity.NodeID)
	copy(payload[20:36], epoch[:])
	binary.BigEndian.PutUint64(payload[36:44], generation)
	binary.BigEndian.PutUint64(payload[44:52], baseSequence)
	binary.BigEndian.PutUint64(payload[52:60], transactionCount)
	copy(payload[60:92], digest[:])
	return payload
}

func domainRecordName(kind RecordType) string {
	names := map[RecordType]string{
		recordFence: "fence", recordAssignment: "assignment", recordDelivery: "delivery",
		recordDeliveryProcessed: "processed", recordDeliveryCompleted: "completed", recordCheckpoint: "checkpoint",
		recordResult: "result", recordEvent: "event", recordEventAck: "event-ack", recordRepair: "repair",
		recordSource: "source", recordOutboxAck: "outbox-ack", recordOutboxRetry: "outbox-retry",
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
	case recordOutboxRetry:
		return encode(encodeOutboxRetry(outboxRetryUpdate{ID: fixture.next.ID, DeadlineUnixNano: 1}))
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
	notice := model.CheckpointNotice{JobID: assignment.JobID, Source: source.Task, Watermark: 1, RaftIndex: 11, Epoch: epoch}
	persistCompletionForCheckpoint(t, store, notice)
	if err := store.ApplyCheckpoint(notice); err != nil {
		t.Fatal(err)
	}
	if err := store.AcknowledgeEvents(1); err != nil {
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
	if err := store.PersistEvent(domainFailureEvent(store, assignment, epoch, 2)); err != nil {
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
	report := model.CompletionReport{JobID: assignment.JobID, JobControlRevision: 1, AssignmentRevision: assignment.Revision, Source: source.Task, Token: source, Epoch: epoch, Prior: 1, New: 2, EOF: 8, ExpectedCheckpointRevision: 1, WorkerTransactionID: 3}
	report.Digest = model.CompletionReportDigest(report)
	completion := model.WorkerEvent{WorkerID: identity.NodeID, WorkerEpoch: store.WorkerEpoch(), TransactionID: 3, Kind: model.WorkerEventCompletion, Completion: &report}
	if err := store.PersistEvent(completion); err != nil {
		t.Fatal(err)
	}
	return path, identity, options, store, snapshotFixture{topology: topology, assignment: assignment, epoch: epoch, compacted: compacted, next: domainDeliverySequence(t, topology, assignment, epoch, 3)}
}

func eventBatchSnapshotStore(t *testing.T) (string, Identity, Options, *Store, model.AssignmentSet, model.CoordinatorEpoch) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "event-batch-worker")
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
	return path, identity, options, store, assignment, epoch
}

func resultPayloadWithDeclaredLogicalLength(t *testing.T, payload []byte, declared uint32) []byte {
	t.Helper()
	if len(payload) < 6 {
		t.Fatal("result payload is too short")
	}
	actual := binary.BigEndian.Uint32(payload[2:6])
	result := append([]byte(nil), payload...)
	if declared > actual {
		result = append(result[:6+actual], append(make([]byte, int(declared-actual)), result[6+actual:]...)...)
	}
	binary.BigEndian.PutUint32(result[2:6], declared)
	return result
}
