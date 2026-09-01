package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/aaditya/cs425mp3/internal/crane/model"
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
			if err := store.Commit(Transaction{Records: []Record{{Type: 100, Payload: []byte("one")}}}); err != nil {
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
			if len(reopened.Recovered().Transactions) != 1 {
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
	_ = store.Commit(Transaction{Records: []Record{{Type: 100, Payload: []byte("one")}}})
	store.Close()
	walPath := filepath.Join(path, WorkerWALFilename)
	data, _ := os.ReadFile(walPath)
	data[len(data)-20] ^= 1
	_ = os.WriteFile(walPath, data, 0o600)
	if _, err := Open(path, identity, Options{MaxBytes: 1 << 20}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("committed corruption err=%v", err)
	}
}

func TestRecoveryReturnsOwnedTransactions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker")
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 2}
	store := mustOpen(t, path, identity, 1<<20, model.WorkerEpoch{3})
	defer store.Close()
	payload := []byte("owned")
	if err := store.Commit(Transaction{Records: []Record{{Type: 100, Payload: payload}}}); err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	first := store.Recovered()
	first.Transactions[0].Records[0].Payload[0] = 'Y'
	if got := string(store.Recovered().Transactions[0].Records[0].Payload); got != "owned" {
		t.Fatalf("payload=%q", got)
	}
}

func TestRecoveryRejectsCommittedInnerLengthInflatedPastEOF(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker")
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 2}
	store := mustOpen(t, path, identity, 1<<20, model.WorkerEpoch{3})
	if err := store.Commit(Transaction{Records: []Record{{Type: 100, Payload: []byte("abc")}}}); err != nil {
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
		for _, transaction := range state.Transactions {
			if err := transaction.Validate(); err != nil {
				t.Fatalf("published invalid transaction: %v", err)
			}
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
