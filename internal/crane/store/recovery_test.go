package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"crane/internal/crane/model"
)

func TestStoreInitializesIdentityBound0700Directory0600FilesAndPersistsEpoch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker")
	epoch := model.WorkerEpoch{9}
	calls := 0
	store, err := Open(path, Identity{ClusterID: [16]byte{1}, NodeID: 2}, Options{MaxBytes: 1 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { calls++; return epoch, nil }})
	if err != nil {
		t.Fatal(err)
	}
	if store.WorkerEpoch() != epoch || calls != 1 {
		t.Fatalf("epoch=%x calls=%d", store.WorkerEpoch(), calls)
	}
	for name, mode := range map[string]os.FileMode{".": 0o700, WorkerWALFilename: 0o600, WorkerLockFilename: 0o600} {
		info, err := os.Stat(filepath.Join(path, name))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != mode {
			t.Fatalf("%s mode=%o want=%o", name, info.Mode().Perm(), mode)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, Identity{ClusterID: [16]byte{1}, NodeID: 2}, Options{MaxBytes: 1 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { calls++; return model.WorkerEpoch{8}, nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.WorkerEpoch() != epoch || calls != 1 {
		t.Fatalf("reopen epoch=%x calls=%d", reopened.WorkerEpoch(), calls)
	}
}

func TestIdentityMismatchCopiedStoreAndCorruptionNeverRegenerateEpoch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker")
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 2}
	store := mustOpen(t, path, identity, 1<<20, model.WorkerEpoch{3})
	store.Close()
	for _, wrong := range []Identity{{ClusterID: [16]byte{2}, NodeID: 2}, {ClusterID: [16]byte{1}, NodeID: 3}} {
		calls := 0
		_, err := Open(path, wrong, Options{MaxBytes: 1 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { calls++; return model.WorkerEpoch{4}, nil }})
		if !errors.Is(err, ErrIdentityMismatch) || calls != 0 {
			t.Fatalf("wrong=%#v err=%v calls=%d", wrong, err, calls)
		}
	}
	wal := filepath.Join(path, WorkerWALFilename)
	bytes, _ := os.ReadFile(wal)
	bytes[20] ^= 1
	if err := os.WriteFile(wal, bytes, 0o600); err != nil {
		t.Fatal(err)
	}
	calls := 0
	_, err := Open(path, identity, Options{MaxBytes: 1 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { calls++; return model.WorkerEpoch{5}, nil }})
	if !errors.Is(err, ErrCorrupt) || calls != 0 {
		t.Fatalf("corrupt err=%v calls=%d", err, calls)
	}
}

func TestWorkerEpochRegeneratesOnlyAfterExplicitEmptyStoreRecreation(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "worker")
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 2}
	store := mustOpen(t, path, identity, 1<<20, model.WorkerEpoch{3})
	store.Close()
	if err := os.Remove(filepath.Join(path, WorkerWALFilename)); err != nil {
		t.Fatal(err)
	}
	calls := 0
	if _, err := Open(path, identity, Options{MaxBytes: 1 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { calls++; return model.WorkerEpoch{4}, nil }}); !errors.Is(err, ErrCorrupt) || calls != 0 {
		t.Fatalf("missing WAL err=%v calls=%d", err, calls)
	}
	if err := os.RemoveAll(path); err != nil {
		t.Fatal(err)
	}
	recreated, err := Open(path, identity, Options{MaxBytes: 1 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { calls++; return model.WorkerEpoch{4}, nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer recreated.Close()
	if recreated.WorkerEpoch() != (model.WorkerEpoch{4}) || calls != 1 {
		t.Fatalf("epoch=%x calls=%d", recreated.WorkerEpoch(), calls)
	}
}

func TestRecoveryTruncatesEveryIncompleteFinalTransactionButRejectsCommittedCorruption(t *testing.T) {
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 2}
	tail, err := encodeTransaction(5, Transaction{Records: []Record{{Type: 101, Payload: []byte("two")}}})
	if err != nil {
		t.Fatal(err)
	}
	beginEnd := walRecordEndForTest(t, tail, 0)
	dataEnd := walRecordEndForTest(t, tail, beginEnd)
	cuts := []int{1, walHeaderBytes - 1, walHeaderBytes + 1, beginEnd - 1, beginEnd + 1, dataEnd - 1, dataEnd + 1, len(tail) - 1}
	for _, cut := range cuts {
		t.Run(fmt.Sprintf("cut-%d", cut), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "worker")
			store := mustOpen(t, path, identity, 1<<20, model.WorkerEpoch{3})
			if err := store.Fence(model.CoordinatorEpoch{Term: 1, BeginIndex: 1, Coordinator: 1, Nonce: [16]byte{1}}); err != nil {
				t.Fatal(err)
			}
			store.Close()
			walPath := filepath.Join(path, WorkerWALFilename)
			before, _ := os.ReadFile(walPath)
			file, _ := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0)
			_, _ = file.Write(tail[:cut])
			file.Close()
			reopened, err := Open(path, identity, Options{MaxBytes: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if reopened.Recovered().TransactionCount != 1 {
				t.Fatalf("transactions=%#v", reopened.Recovered())
			}
			after, _ := os.ReadFile(walPath)
			if len(after) != len(before) {
				t.Fatalf("length=%d want=%d", len(after), len(before))
			}
		})
	}
	path := filepath.Join(t.TempDir(), "worker")
	store := mustOpen(t, path, identity, 1<<20, model.WorkerEpoch{3})
	_ = store.Fence(model.CoordinatorEpoch{Term: 1, BeginIndex: 1, Coordinator: 1, Nonce: [16]byte{1}})
	store.Close()
	walPath := filepath.Join(path, WorkerWALFilename)
	data, _ := os.ReadFile(walPath)
	data[len(data)-20] ^= 1
	_ = os.WriteFile(walPath, data, 0o600)
	if _, err := Open(path, identity, Options{MaxBytes: 1 << 20}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("committed corruption err=%v", err)
	}
}

func TestCommitAndRecoveryConsumerOwnTransactionPayloads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker")
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 2}
	store := mustOpen(t, path, identity, 1<<20, model.WorkerEpoch{3})
	payload := []byte("owned")
	if err := commitRawForTest(store, Transaction{Records: []Record{{Type: 100, Payload: payload}}}); err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	wal, err := os.Open(filepath.Join(path, WorkerWALFilename))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	info, err := wal.Stat()
	if err != nil {
		t.Fatal(err)
	}
	consumer := &collectingRecoveryConsumer{}
	if _, _, err := recoverWALReader(wal, info.Size(), identity, consumer); err != nil {
		t.Fatal(err)
	}
	if got := string(consumer.records[0].Payload); got != "owned" {
		t.Fatalf("payload=%q", got)
	}
}

func TestTask15RecoveryAcceptsLegacyCheckpointWithoutCompletionProof(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker")
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 1}
	options := Options{MaxBytes: 16 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return model.WorkerEpoch{7}, nil }}
	workerStore, err := Open(path, identity, options)
	if err != nil {
		t.Fatal(err)
	}
	topology, assignment, epoch := domainAssignmentWithRange(t, workerStore.WorkerEpoch(), identity.NodeID, 3)
	if err := workerStore.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := workerStore.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	delivery := domainDeliverySequence(t, topology, assignment, epoch, 1)
	if _, err := workerStore.Receive(delivery); err != nil {
		t.Fatal(err)
	}
	outputs, outboxes := exactProcessedRecords(t, topology, assignment, delivery)
	if err := workerStore.MarkProcessed(delivery.ID, outputs, outboxes); err != nil {
		t.Fatal(err)
	}
	for _, outbox := range outboxes {
		if err := workerStore.MarkOutboxCompleted(outbox.ID); err != nil {
			t.Fatal(err)
		}
	}
	if err := workerStore.MarkCompleted(delivery.ID); err != nil {
		t.Fatal(err)
	}
	firstSequence := workerStore.Recovered().LastSequence + 1
	if err := workerStore.Close(); err != nil {
		t.Fatal(err)
	}
	notice := model.CheckpointNotice{JobID: assignment.JobID, Source: delivery.ID.Tuple.SourceTask, Watermark: 1, RaftIndex: 9, Epoch: epoch}
	payload, err := encodeCheckpoint(notice)
	if err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint16(payload[:2], domainRecordSchema)
	appendLegacyTransactionForTest(t, path, firstSequence, Transaction{Records: []Record{{Type: recordCheckpoint, Payload: payload}}})
	reopened, err := Open(path, identity, Options{MaxBytes: options.MaxBytes})
	if err != nil {
		t.Fatalf("legacy checkpoint WAL did not reopen: %v", err)
	}
	defer reopened.Close()
	work := mustRecoverWork(t, reopened)
	if len(work.Sources) != 1 || work.Sources[0].Watermark != 1 || work.Sources[0].CheckpointRevision != 0 || work.Sources[0].CheckpointAuthority != (CheckpointAuthority{}) || len(work.Deliveries) != 0 {
		t.Fatalf("legacy checkpoint recovery=%+v", work)
	}
}

func TestTask15RecoveryAcceptsLegacyEventAckBeforeCheckpointProof(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker")
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 1}
	options := Options{MaxBytes: 16 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return model.WorkerEpoch{7}, nil }}
	workerStore, err := Open(path, identity, options)
	if err != nil {
		t.Fatal(err)
	}
	topology, assignment, epoch := domainAssignmentWithRange(t, workerStore.WorkerEpoch(), identity.NodeID, 3)
	if err := workerStore.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := workerStore.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	work := mustRecoverWork(t, workerStore)
	source := assignment.Tasks[0].Task
	for _, token := range assignment.Tasks {
		if token.Task.StageID == 1 {
			source = token.Task
			break
		}
	}
	notice := model.CheckpointNotice{JobID: assignment.JobID, Source: source, Watermark: 1, RaftIndex: 9, Epoch: epoch}
	event := completionEventForCheckpoint(t, work, notice)
	if err := workerStore.PersistEvent(event); err != nil {
		t.Fatal(err)
	}
	firstSequence := workerStore.Recovered().LastSequence + 1
	if err := workerStore.Close(); err != nil {
		t.Fatal(err)
	}
	payload := encodeUint64Payload(event.TransactionID)
	binary.BigEndian.PutUint16(payload[:2], domainRecordSchema)
	appendLegacyTransactionForTest(t, path, firstSequence, Transaction{Records: []Record{{Type: recordEventAck, Payload: payload}}})
	reopened, err := Open(path, identity, Options{MaxBytes: options.MaxBytes})
	if err != nil {
		t.Fatalf("legacy event-ack WAL did not reopen: %v", err)
	}
	defer reopened.Close()
	if pending := mustRecoverWork(t, reopened).PendingEvents; len(pending) != 0 {
		t.Fatalf("legacy acknowledged events=%+v", pending)
	}
}

func TestTask15LiveCommitCannotInvokeLegacyCheckpointOrEventAckSemantics(t *testing.T) {
	t.Run("checkpoint", func(t *testing.T) {
		workerStore, identity, _ := openDomainStore(t, 16<<20)
		topology, assignment, epoch := domainAssignmentWithRange(t, workerStore.WorkerEpoch(), identity.NodeID, 3)
		if err := workerStore.Fence(epoch); err != nil {
			t.Fatal(err)
		}
		if err := workerStore.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
			t.Fatal(err)
		}
		source := sourceTaskForTest(t, assignment)
		notice := model.CheckpointNotice{JobID: assignment.JobID, Source: source, Watermark: 1, RaftIndex: 9, Epoch: epoch}
		payload, err := encodeCheckpoint(notice)
		if err != nil {
			t.Fatal(err)
		}
		binary.BigEndian.PutUint16(payload[:2], domainRecordSchema)
		before := mustRecoverWork(t, workerStore)
		if err := workerStore.Commit(Transaction{Records: []Record{{Type: recordCheckpoint, Payload: payload}}}); err == nil {
			t.Fatal("live Commit invoked legacy checkpoint semantics")
		}
		if after := mustRecoverWork(t, workerStore); !reflect.DeepEqual(after, before) {
			t.Fatal("rejected legacy checkpoint mutated live store")
		}
	})

	t.Run("event ack", func(t *testing.T) {
		workerStore, identity, _ := openDomainStore(t, 16<<20)
		topology, assignment, epoch := domainAssignmentWithRange(t, workerStore.WorkerEpoch(), identity.NodeID, 3)
		if err := workerStore.Fence(epoch); err != nil {
			t.Fatal(err)
		}
		if err := workerStore.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
			t.Fatal(err)
		}
		notice := model.CheckpointNotice{JobID: assignment.JobID, Source: sourceTaskForTest(t, assignment), Watermark: 1, RaftIndex: 9, Epoch: epoch}
		event := completionEventForCheckpoint(t, mustRecoverWork(t, workerStore), notice)
		if err := workerStore.PersistEvent(event); err != nil {
			t.Fatal(err)
		}
		payload := encodeUint64Payload(event.TransactionID)
		binary.BigEndian.PutUint16(payload[:2], domainRecordSchema)
		before := mustRecoverWork(t, workerStore)
		if err := workerStore.Commit(Transaction{Records: []Record{{Type: recordEventAck, Payload: payload}}}); err == nil {
			t.Fatal("live Commit invoked legacy event-ack semantics")
		}
		if after := mustRecoverWork(t, workerStore); !reflect.DeepEqual(after, before) {
			t.Fatal("rejected legacy event ack mutated live store")
		}
	})
}

func TestTask15LegacyCheckpointRecoveryDistinguishesHistoricalAndExactProof(t *testing.T) {
	for _, test := range []struct {
		name      string
		replace   bool
		wantProof bool
	}{
		{name: "historical mismatched pending completion", replace: true},
		{name: "exact 6d proof", wantProof: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "worker")
			identity := Identity{ClusterID: [16]byte{1}, NodeID: 1}
			options := Options{MaxBytes: 16 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return model.WorkerEpoch{7}, nil }}
			workerStore, err := Open(path, identity, options)
			if err != nil {
				t.Fatal(err)
			}
			topology, assignment, epoch := domainAssignmentWithRange(t, workerStore.WorkerEpoch(), identity.NodeID, 3)
			if err := workerStore.Fence(epoch); err != nil {
				t.Fatal(err)
			}
			if err := workerStore.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
				t.Fatal(err)
			}
			notice := model.CheckpointNotice{JobID: assignment.JobID, Source: sourceTaskForTest(t, assignment), Watermark: 1, RaftIndex: 9, Epoch: epoch}
			event := completionEventForCheckpoint(t, mustRecoverWork(t, workerStore), notice)
			if err := workerStore.PersistEvent(event); err != nil {
				t.Fatal(err)
			}
			if test.replace {
				tokens := append([]model.AssignmentToken(nil), assignment.Tasks...)
				for index := range tokens {
					tokens[index].AssignmentRevision++
				}
				replacement, replaceErr := model.NewAssignmentSet(assignment.JobID, assignment.Revision+1, tokens, assignment.ResultReplicas, topology)
				if replaceErr != nil {
					t.Fatal(replaceErr)
				}
				if err := workerStore.InstallAssignment(replacement, topology.Spec(), 2, model.Running, epoch); err != nil {
					t.Fatal(err)
				}
			}
			firstSequence := workerStore.Recovered().LastSequence + 1
			if err := workerStore.Close(); err != nil {
				t.Fatal(err)
			}
			payload, err := encodeCheckpoint(notice)
			if err != nil {
				t.Fatal(err)
			}
			binary.BigEndian.PutUint16(payload[:2], domainRecordSchema)
			appendLegacyTransactionForTest(t, path, firstSequence, Transaction{Records: []Record{{Type: recordCheckpoint, Payload: payload}}})
			reopened, err := Open(path, identity, Options{MaxBytes: options.MaxBytes})
			if err != nil {
				t.Fatalf("legacy checkpoint recovery: %v", err)
			}
			defer reopened.Close()
			work := mustRecoverWork(t, reopened)
			if len(work.Sources) != 1 || work.Sources[0].Watermark != 1 {
				t.Fatalf("recovered cursor=%+v", work.Sources)
			}
			proof := work.Sources[0].CheckpointAuthority
			if test.wantProof {
				if work.Sources[0].CheckpointRevision != 1 || proof.AssignmentRevision != assignment.Revision || proof.AssignmentDigest != assignment.Digest || proof.SourceToken != event.Completion.Token {
					t.Fatalf("exact 6d proof not reconstructed: cursor=%+v", work.Sources[0])
				}
			} else if work.Sources[0].CheckpointRevision != 0 || proof != (CheckpointAuthority{}) {
				t.Fatalf("historical Task14 checkpoint invented proof: cursor=%+v", work.Sources[0])
			}
		})
	}
}

func sourceTaskForTest(t *testing.T, assignment model.AssignmentSet) model.TaskID {
	t.Helper()
	for _, token := range assignment.Tasks {
		if token.Task.StageID == 1 {
			return token.Task
		}
	}
	t.Fatal("assignment has no source task")
	return model.TaskID{}
}

func appendLegacyTransactionForTest(t *testing.T, path string, firstSequence uint64, transaction Transaction) {
	t.Helper()
	encoded, err := encodeTransaction(firstSequence, transaction)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(filepath.Join(path, WorkerWALFilename), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := file.Write(encoded)
	closeErr := file.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryRejectsCommittedInnerLengthInflatedPastEOF(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker")
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 2}
	store := mustOpen(t, path, identity, 1<<20, model.WorkerEpoch{3})
	if err := commitRawForTest(store, Transaction{Records: []Record{{Type: 100, Payload: []byte("abc")}}}); err != nil {
		t.Fatal(err)
	}
	store.Close()
	walPath := filepath.Join(path, WorkerWALFilename)
	data, _ := os.ReadFile(walPath)
	identityEnd := walRecordEndForTest(t, data, 0)
	beginEnd := walRecordEndForTest(t, data, identityEnd)
	binary.BigEndian.PutUint32(data[beginEnd+8:beginEnd+12], uint32(MaxRecordPayloadBytes))
	if err := os.WriteFile(walPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, identity, Options{MaxBytes: 1 << 20}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("inflated committed length err=%v", err)
	}
}

func TestRecoveryRejectsCRCValidSameSequenceDataFrameSplice(t *testing.T) {
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 2}
	txA, err := encodeTransaction(2, Transaction{Records: []Record{{Type: 100, Payload: []byte("aaa")}}})
	if err != nil {
		t.Fatal(err)
	}
	txB, err := encodeTransaction(2, Transaction{Records: []Record{{Type: 100, Payload: []byte("bbb")}}})
	if err != nil {
		t.Fatal(err)
	}
	beginEnd := walRecordEndForTest(t, txA, 0)
	dataEnd := walRecordEndForTest(t, txA, beginEnd)
	otherBeginEnd := walRecordEndForTest(t, txB, 0)
	otherDataEnd := walRecordEndForTest(t, txB, otherBeginEnd)
	copy(txA[beginEnd:dataEnd], txB[otherBeginEnd:otherDataEnd])
	path := filepath.Join(t.TempDir(), "worker")
	store := mustOpen(t, path, identity, 1<<20, model.WorkerEpoch{3})
	store.Close()
	walPath := filepath.Join(path, WorkerWALFilename)
	file, _ := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0)
	_, _ = file.Write(txA)
	file.Close()
	if _, err := Open(path, identity, Options{MaxBytes: 1 << 20}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("spliced transaction err=%v", err)
	}
}

func TestRecoveryRejectsSchemaSequenceLengthAndCrossReferenceCorruption(t *testing.T) {
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 2}
	identityFrame, err := encodeIdentity(identity, model.WorkerEpoch{3})
	if err != nil {
		t.Fatal(err)
	}
	transaction, err := encodeTransaction(2, Transaction{Records: []Record{{Type: 100, Payload: []byte("abc")}}})
	if err != nil {
		t.Fatal(err)
	}
	base := append(append([]byte(nil), identityFrame...), transaction...)
	transactionAt := len(identityFrame)
	beginEnd := walRecordEndForTest(t, base, transactionAt)
	dataEnd := walRecordEndForTest(t, base, beginEnd)
	tests := []struct {
		name   string
		mutate func([]byte)
	}{
		{name: "schema", mutate: func(data []byte) { data[transactionAt+5]++; recomputeWALFrameCRCForTest(t, data, transactionAt) }},
		{name: "checksum", mutate: func(data []byte) { data[beginEnd-1]++ }},
		{name: "zero sequence", mutate: func(data []byte) {
			binary.BigEndian.PutUint64(data[transactionAt+12:transactionAt+20], 0)
			recomputeWALFrameCRCForTest(t, data, transactionAt)
		}},
		{name: "record count", mutate: func(data []byte) {
			binary.BigEndian.PutUint32(data[transactionAt+walHeaderBytes:transactionAt+walHeaderBytes+4], 2)
			recomputeWALFrameCRCForTest(t, data, transactionAt)
		}},
		{name: "inner payload length", mutate: func(data []byte) {
			binary.BigEndian.PutUint32(data[beginEnd+walHeaderBytes+2:beginEnd+walHeaderBytes+6], 4)
			recomputeWALFrameCRCForTest(t, data, beginEnd)
		}},
		{name: "commit sequence", mutate: func(data []byte) {
			binary.BigEndian.PutUint64(data[dataEnd+12:dataEnd+20], 9)
			recomputeWALFrameCRCForTest(t, data, dataEnd)
		}},
		{name: "commit boundary", mutate: func(data []byte) { data[dataEnd+walHeaderBytes+12]++; recomputeWALFrameCRCForTest(t, data, dataEnd) }},
		{name: "declared total", mutate: func(data []byte) {
			binary.BigEndian.PutUint64(data[transactionAt+walHeaderBytes+4:transactionAt+walHeaderBytes+12], uint64(len(transaction)-1))
			recomputeWALFrameCRCForTest(t, data, transactionAt)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			corrupt := append([]byte(nil), base...)
			test.mutate(corrupt)
			if _, _, err := recoverWAL(corrupt, identity); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("recover error=%v, want ErrCorrupt", err)
			}
		})
	}
}

func TestRecoveryRejectsCountImpossibleSpanBeforePartialTailClassification(t *testing.T) {
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 2}
	identityFrame, err := encodeIdentity(identity, model.WorkerEpoch{3})
	if err != nil {
		t.Fatal(err)
	}
	minimum := uint64(2*(walHeaderBytes+boundaryPayloadBytes+walChecksumBytes) + walHeaderBytes + dataPrefixBytes + walChecksumBytes)
	maximum := minimum + MaxRecordPayloadBytes
	for _, test := range []struct {
		name      string
		span      uint64
		wantError bool
	}{
		{name: "minimum minus one", span: minimum - 1, wantError: true},
		{name: "minimum", span: minimum},
		{name: "maximum", span: maximum},
		{name: "maximum plus one", span: maximum + 1, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			boundary := make([]byte, boundaryPayloadBytes)
			binary.BigEndian.PutUint32(boundary[:4], 1)
			binary.BigEndian.PutUint64(boundary[4:12], test.span)
			boundary[12] = 1
			begin, encodeErr := encodeRecord(recordTransactionBegin, 2, boundary)
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			wal := append(append([]byte(nil), identityFrame...), begin...)
			state, truncateAt, recoverErr := recoverWAL(wal, identity)
			if test.wantError {
				if !errors.Is(recoverErr, ErrCorrupt) {
					t.Fatalf("span=%d error=%v, want ErrCorrupt", test.span, recoverErr)
				}
				return
			}
			if recoverErr != nil || truncateAt != len(identityFrame) || state.LastSequence != 1 {
				t.Fatalf("span=%d state=%#v truncate=%d error=%v", test.span, state, truncateAt, recoverErr)
			}
		})
	}
	for _, payloadBytes := range []int{0, MaxRecordPayloadBytes} {
		transaction, err := encodeTransaction(2, Transaction{Records: []Record{{Type: 100, Payload: make([]byte, payloadBytes)}}})
		if err != nil {
			t.Fatal(err)
		}
		wal := append(append([]byte(nil), identityFrame...), transaction...)
		if _, truncateAt, err := recoverWAL(wal, identity); err != nil || truncateAt != len(wal) {
			t.Fatalf("canonical payload=%d truncate=%d error=%v", payloadBytes, truncateAt, err)
		}
	}
}

func TestRecoveryTruncatesEveryCanonicalFinalTransactionPrefix(t *testing.T) {
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 2}
	identityFrame, _ := encodeIdentity(identity, model.WorkerEpoch{3})
	committed, _ := encodeTransaction(2, Transaction{Records: []Record{{Type: 100, Payload: []byte("committed")}}})
	tail, _ := encodeTransaction(5, Transaction{Records: []Record{{Type: 101, Payload: []byte("tail")}}})
	base := append(append([]byte(nil), identityFrame...), committed...)
	for cut := 1; cut < len(tail); cut++ {
		wal := append(append([]byte(nil), base...), tail[:cut]...)
		state, truncateAt, err := recoverWAL(wal, identity)
		if err != nil || truncateAt != len(base) || state.TransactionCount != 1 || state.LastSequence != 4 {
			t.Fatalf("cut=%d state=%#v truncate=%d want=%d error=%v", cut, state, truncateAt, len(base), err)
		}
	}
}

func TestOpenRejectsLargeEarlyCorruptSparseWALWithoutWholeFileAllocation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker")
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 2}
	store := mustOpen(t, path, identity, 1<<20, model.WorkerEpoch{3})
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	const sparseBytes = int64(64 << 20)
	wal, err := os.OpenFile(filepath.Join(path, WorkerWALFilename), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.Truncate(sparseBytes); err != nil {
		wal.Close()
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	opened, openErr := Open(path, identity, Options{MaxBytes: uint64(sparseBytes)})
	runtime.ReadMemStats(&after)
	if opened != nil {
		opened.Close()
	}
	if !errors.Is(openErr, ErrCorrupt) {
		t.Fatalf("Open error=%v, want ErrCorrupt", openErr)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 8<<20 {
		t.Fatalf("early corrupt sparse WAL allocated %d bytes, want <=%d", allocated, 8<<20)
	}
}

func TestRecoveryConsumerRunsOnlyAfterFullValidationAndReceivesOwnedRecords(t *testing.T) {
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 2}
	identityFrame, _ := encodeIdentity(identity, model.WorkerEpoch{3})
	transaction, _ := encodeTransaction(2, Transaction{Records: []Record{{Type: 100, Payload: []byte("owned")}}})
	valid := append(append([]byte(nil), identityFrame...), transaction...)
	corrupt := append(append([]byte(nil), valid...), make([]byte, walHeaderBytes)...)
	consumer := &collectingRecoveryConsumer{}
	if _, _, err := recoverWALReader(bytes.NewReader(corrupt), int64(len(corrupt)), identity, consumer); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt replay error=%v, want ErrCorrupt", err)
	}
	if consumer.beginCalls != 0 || len(consumer.records) != 0 || consumer.commitCalls != 0 {
		t.Fatalf("consumer ran before full validation: %#v", consumer)
	}
	consumer = &collectingRecoveryConsumer{}
	state, truncateAt, err := recoverWALReader(bytes.NewReader(valid), int64(len(valid)), identity, consumer)
	if err != nil || truncateAt != int64(len(valid)) || state.TransactionCount != 1 {
		t.Fatalf("state=%#v truncate=%d error=%v", state, truncateAt, err)
	}
	for i := range valid {
		valid[i] = 0
	}
	if consumer.beginCalls != 1 || consumer.commitCalls != 1 || len(consumer.records) != 1 || string(consumer.records[0].Payload) != "owned" {
		t.Fatalf("consumer output=%#v", consumer)
	}
}

type collectingRecoveryConsumer struct {
	beginCalls  int
	commitCalls int
	records     []Record
}

func (consumer *collectingRecoveryConsumer) BeginTransaction(uint32) error {
	consumer.beginCalls++
	return nil
}

func (consumer *collectingRecoveryConsumer) ConsumeRecord(record Record) error {
	consumer.records = append(consumer.records, record)
	return nil
}

func (consumer *collectingRecoveryConsumer) CommitTransaction() error {
	consumer.commitCalls++
	return nil
}

func FuzzRecoverWAL(f *testing.F) {
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 2}
	identityFrame, _ := encodeIdentity(identity, model.WorkerEpoch{3})
	transaction, _ := encodeTransaction(2, Transaction{Records: []Record{{Type: 100, Payload: []byte("seed")}}})
	f.Add([]byte(nil))
	f.Add(identityFrame)
	f.Add(append(append([]byte(nil), identityFrame...), transaction...))
	f.Fuzz(func(t *testing.T, data []byte) {
		state, truncateAt, err := recoverWAL(data, identity)
		if err != nil {
			return
		}
		if truncateAt < 0 || truncateAt > len(data) || state.Identity != identity || state.WorkerEpoch == (model.WorkerEpoch{}) {
			t.Fatalf("invalid successful recovery: truncate=%d bytes=%d state=%#v", truncateAt, len(data), state)
		}
	})
}

func walRecordEndForTest(t *testing.T, data []byte, offset int) int {
	t.Helper()
	if len(data)-offset < walHeaderBytes {
		t.Fatal("short test record")
	}
	return offset + walHeaderBytes + int(binary.BigEndian.Uint32(data[offset+8:offset+12])) + walChecksumBytes
}

func recomputeWALFrameCRCForTest(t *testing.T, data []byte, offset int) {
	t.Helper()
	end := walRecordEndForTest(t, data, offset)
	binary.BigEndian.PutUint32(data[end-walChecksumBytes:end], crc32.Checksum(data[offset:end-walChecksumBytes], walCRC))
}

func mustOpen(t *testing.T, path string, identity Identity, max uint64, epoch model.WorkerEpoch) *Store {
	t.Helper()
	store, err := Open(path, identity, Options{MaxBytes: max, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return epoch, nil }})
	if err != nil {
		t.Fatal(err)
	}
	return store
}
