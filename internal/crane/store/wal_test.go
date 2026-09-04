package store

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"crane/internal/crane/model"
)

func TestWALPinsMagicSchemaTypesBigEndianLengthSequenceAndCRC32C(t *testing.T) {
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 2}
	epoch := model.WorkerEpoch{3}
	encoded, err := encodeIdentity(identity, epoch)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded[:4]) != "CWWL" || binary.BigEndian.Uint16(encoded[4:6]) != 1 || binary.BigEndian.Uint16(encoded[6:8]) != uint16(recordIdentity) || binary.BigEndian.Uint32(encoded[8:12]) != 34 || binary.BigEndian.Uint64(encoded[12:20]) != 1 {
		t.Fatalf("header=%x", encoded[:20])
	}
	wantCRC := crc32.Checksum(encoded[:len(encoded)-4], crc32.MakeTable(crc32.Castagnoli))
	if got := binary.BigEndian.Uint32(encoded[len(encoded)-4:]); got != wantCRC {
		t.Fatalf("crc=%08x want=%08x", got, wantCRC)
	}
	if encoded[20] != 1 || binary.BigEndian.Uint16(encoded[36:38]) != 2 || encoded[38] != 3 {
		t.Fatalf("identity payload=%x", encoded[20:])
	}
	const identityGolden = "4357574c0001000100000022000000000000000101000000000000000000000000000000000203000000000000000000000000000000f61a010d"
	if hex.EncodeToString(encoded) != identityGolden {
		t.Fatalf("identity encoding=%x", encoded)
	}
}

func TestWALPinsTransactionBoundarySizeDigestAndSequence(t *testing.T) {
	transaction := Transaction{Records: []Record{{Type: 100, Payload: []byte("abc")}}}
	encoded, err := encodeTransaction(2, transaction)
	if err != nil {
		t.Fatal(err)
	}
	wantBytes, err := transactionEncodedSize(transaction)
	if err != nil {
		t.Fatal(err)
	}
	if uint64(len(encoded)) != wantBytes {
		t.Fatalf("encoded bytes=%d want=%d", len(encoded), wantBytes)
	}
	begin, beginEnd, partial, err := decodeRecord(encoded, 0)
	if err != nil || partial {
		t.Fatalf("begin err=%v partial=%v", err, partial)
	}
	data, dataEnd, partial, err := decodeRecord(encoded, beginEnd)
	if err != nil || partial {
		t.Fatalf("data err=%v partial=%v", err, partial)
	}
	commit, commitEnd, partial, err := decodeRecord(encoded, dataEnd)
	if err != nil || partial {
		t.Fatalf("commit err=%v partial=%v", err, partial)
	}
	if begin.kind != recordTransactionBegin || begin.sequence != 2 || data.kind != recordTransactionData || data.sequence != 3 || commit.kind != recordTransactionCommit || commit.sequence != 4 {
		t.Fatalf("record identities: begin=%#v data=%#v commit=%#v", begin, data, commit)
	}
	if binary.BigEndian.Uint32(begin.payload[:4]) != 1 || binary.BigEndian.Uint64(begin.payload[4:12]) != uint64(len(encoded)) || !bytes.Equal(begin.payload, commit.payload) || commitEnd != len(encoded) {
		t.Fatalf("boundary begin=%x commit=%x end=%d", begin.payload, commit.payload, commitEnd)
	}
	if bytes.Equal(begin.payload[12:], make([]byte, 32)) {
		t.Fatal("zero transaction digest")
	}
	const transactionGolden = "4357574c000100020000002c00000000000000020000000100000000000000a9a3d141f729fd3c76a6a0a140d65fd48a25d3d009ff35fe3432e64af6be429777218f7af04357574c000100030000000900000000000000030064000000036162634e4e3ff64357574c000100040000002c00000000000000040000000100000000000000a9a3d141f729fd3c76a6a0a140d65fd48a25d3d009ff35fe3432e64af6be4297775018ae04"
	if hex.EncodeToString(encoded) != transactionGolden {
		t.Fatalf("transaction encoding=%x", encoded)
	}
}

func TestWALCommitChecksMaxBytesBeforeWriteAndSyncsBeforeSuccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker")
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 2}
	base := mustOpen(t, path, identity, 1<<20, model.WorkerEpoch{3})
	base.Close()
	info, _ := os.Stat(filepath.Join(path, WorkerWALFilename))
	store, err := Open(path, identity, Options{MaxBytes: uint64(info.Size())})
	if err != nil {
		t.Fatal(err)
	}
	before := store.Recovered()
	if err := commitRawForTest(store, Transaction{Records: []Record{{Type: 100, Payload: []byte{1}}}}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("capacity err=%v", err)
	}
	if store.Recovered().LastSequence != before.LastSequence {
		t.Fatal("capacity rejection mutated state")
	}
	store.Close()
	fault := &testFault{point: FaultBeforeSync, err: errors.New("sync fault")}
	store, err = Open(path, identity, Options{MaxBytes: 1 << 20, Faults: fault})
	if err != nil {
		t.Fatal(err)
	}
	if err := commitRawForTest(store, Transaction{Records: []Record{{Type: 100, Payload: []byte{1}}}}); !errors.Is(err, fault.err) {
		t.Fatalf("fault err=%v", err)
	}
	if store.Recovered().LastSequence != before.LastSequence {
		t.Fatal("failed sync published state")
	}
	store.Close()
}

func TestCommitPreflightsExactBytesBeforeTransactionSizedAllocation(t *testing.T) {
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 2}
	path := filepath.Join(t.TempDir(), "worker")
	fault := &testFault{point: FaultBeforeAppend, err: errors.New("must not run")}
	store, err := Open(path, identity, Options{MaxBytes: uint64(walHeaderBytes + identityPayloadBytes + walChecksumBytes), Faults: fault, NewWorkerEpoch: func() (model.WorkerEpoch, error) {
		return model.WorkerEpoch{3}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	payload := make([]byte, MaxRecordPayloadBytes)
	records := make([]Record, MaxTransactionRecords)
	for i := range records {
		records[i] = Record{Type: 100, Payload: payload}
	}
	transaction := Transaction{Records: records}
	allocations := testing.AllocsPerRun(5, func() {
		if err := commitRawForTest(store, transaction); !errors.Is(err, ErrCapacity) {
			t.Fatalf("capacity error=%v", err)
		}
	})
	if allocations != 0 {
		t.Fatalf("capacity preflight allocations=%f, want 0", allocations)
	}
	if fault.calls != 0 || store.Recovered().LastSequence != 1 {
		t.Fatalf("fault calls=%d recovered=%#v", fault.calls, store.Recovered())
	}
}

func TestTransactionAccountingExactCapacityAndSequenceBoundaries(t *testing.T) {
	transaction := Transaction{Records: []Record{{Type: 100, Payload: []byte{1}}}}
	transactionBytes, err := transactionEncodedSize(transaction)
	if err != nil {
		t.Fatal(err)
	}
	want := uint64(2*(walHeaderBytes+boundaryPayloadBytes+walChecksumBytes) + walHeaderBytes + dataPrefixBytes + 1 + walChecksumBytes)
	if transactionBytes != want {
		t.Fatalf("transaction bytes=%d want=%d", transactionBytes, want)
	}
	identityBytes := uint64(walHeaderBytes + identityPayloadBytes + walChecksumBytes)
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 2}
	for _, test := range []struct {
		name    string
		limit   uint64
		wantErr error
	}{
		{name: "exact", limit: identityBytes + transactionBytes},
		{name: "one short", limit: identityBytes + transactionBytes - 1, wantErr: ErrCapacity},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := mustOpen(t, filepath.Join(t.TempDir(), "worker"), identity, test.limit, model.WorkerEpoch{3})
			defer store.Close()
			if err := commitRawForTest(store, transaction); !errors.Is(err, test.wantErr) {
				t.Fatalf("Commit error=%v want=%v", err, test.wantErr)
			}
		})
	}
	if _, err := encodeTransaction(math.MaxUint64-2, transaction); err != nil {
		t.Fatalf("last valid sequence range: %v", err)
	}
	if _, err := encodeTransaction(math.MaxUint64-1, transaction); !errors.Is(err, ErrInvalidTransaction) {
		t.Fatalf("overflow sequence error=%v", err)
	}
}

func TestStoreCommitValidatesBeforeAppendAndIsConcurrencyBounded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker")
	store := mustOpen(t, path, Identity{ClusterID: [16]byte{1}, NodeID: 2}, 1<<20, model.WorkerEpoch{3})
	defer store.Close()
	for _, tx := range []Transaction{{}, {Records: []Record{{}}}, {Records: []Record{{Type: 100, Payload: make([]byte, MaxRecordPayloadBytes+1)}}}} {
		if err := store.Commit(tx); !errors.Is(err, ErrInvalidTransaction) {
			t.Fatalf("invalid tx err=%v", err)
		}
	}
	var wait sync.WaitGroup
	for i := 0; i < 20; i++ {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			if err := commitRawForTest(store, Transaction{Records: []Record{{Type: RecordType(100 + i), Payload: []byte{byte(i)}}}}); err != nil {
				t.Errorf("commit %d: %v", i, err)
			}
		}(i)
	}
	wait.Wait()
	recovered := store.Recovered()
	if recovered.TransactionCount != 20 || recovered.LastSequence == 0 {
		t.Fatalf("recovered=%#v", recovered)
	}
}

func TestStoreConcurrentAccessAndCloseAreRaceSafeAndBounded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker")
	store := mustOpen(t, path, Identity{ClusterID: [16]byte{1}, NodeID: 2}, 1<<20, model.WorkerEpoch{3})
	start := make(chan struct{})
	var wait sync.WaitGroup
	for i := 0; i < 32; i++ {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			<-start
			_ = store.WorkerEpoch()
			_ = store.Recovered()
			err := commitRawForTest(store, Transaction{Records: []Record{{Type: RecordType(100 + i), Payload: []byte{byte(i)}}}})
			if err != nil && !errors.Is(err, ErrClosed) {
				t.Errorf("concurrent Commit error=%v", err)
			}
		}(i)
	}
	wait.Add(1)
	go func() {
		defer wait.Done()
		<-start
		if err := store.Close(); err != nil {
			t.Errorf("Close error=%v", err)
		}
	}()
	close(start)
	wait.Wait()
	if err := store.Close(); err != nil {
		t.Fatalf("idempotent Close error=%v", err)
	}
	if err := store.Commit(Transaction{Records: []Record{{Type: 100}}}); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-close Commit error=%v", err)
	}
}

func TestStoreExportedAPIsHaveUtilityDocumentation(t *testing.T) {
	files := []string{"errors.go", "records.go", "types.go", "store.go", "store_lock_unix.go"}
	for _, filename := range files {
		parsed, err := parser.ParseFile(token.NewFileSet(), filename, nil, parser.ParseComments)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range parsed.Decls {
			switch declaration := declaration.(type) {
			case *ast.FuncDecl:
				if declaration.Name.IsExported() && declaration.Doc == nil {
					t.Errorf("%s: exported function %s lacks utility documentation", filename, declaration.Name.Name)
				}
			case *ast.GenDecl:
				for _, specification := range declaration.Specs {
					value, ok := specification.(*ast.ValueSpec)
					if !ok {
						continue
					}
					for _, name := range value.Names {
						if name.IsExported() && declaration.Doc == nil && value.Doc == nil {
							t.Errorf("%s: exported value %s lacks utility documentation", filename, name.Name)
						}
					}
				}
			}
		}
	}
}

type testFault struct {
	point FaultPoint
	err   error
	calls int
}

func (f *testFault) Inject(point FaultPoint) error {
	f.calls++
	if point == f.point {
		return f.err
	}
	return nil
}
