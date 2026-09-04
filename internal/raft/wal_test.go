package raft

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/aadityakv/crane/internal/config"
)

func TestWALRecordExactCanonicalBytes(t *testing.T) {
	payload, err := encodeTransactionBoundaryPayload(1, transactionFlagHardState|transactionFlagApplied, 2)
	if err != nil {
		t.Fatal(err)
	}
	got, err := encodeWALRecord(walRecordTransactionBegin, payload)
	if err != nil {
		t.Fatal(err)
	}
	want, err := hex.DecodeString("5246574c00010100000000000000000c000000000000000111020000bc30c9b3")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("record bytes = %x, want %x", got, want)
	}
}

func TestIdentityExactCanonicalBytes(t *testing.T) {
	identity := StorageIdentity{
		FormatVersion:    StorageFormatVersion1,
		ClusterID:        [16]byte{1, 2, 3, 4},
		LocalVoterID:     1,
		VoterFingerprint: VoterFingerprint{0xa1, 0xa2},
	}
	got := encodeStorageIdentity(identity)
	want, err := hex.DecodeString("524944310001010203040000000000000000000000000001a1a2000000000000000000000000000000000000000000000000000000000000491bcaf7")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("identity bytes = %x, want %x", got, want)
	}
}

func TestWALPayloadFieldsAreExactBigEndianAndTransactionOrderIsClosed(t *testing.T) {
	entry := mustStorageEntry(t, 1, 2, "abc")
	entries, err := encodeEntriesPayload(7, []Entry{entry})
	if err != nil {
		t.Fatal(err)
	}
	wantEntries, _ := hex.DecodeString("00000000000000070001000000000000000100000000000000020100000003616263")
	if string(entries) != string(wantEntries) {
		t.Fatalf("entries payload = %x, want %x", entries, wantEntries)
	}
	hard := encodeHardStatePayload(7, HardState{Term: 9, VotedFor: 2, CommitIndex: 5})
	wantHard, _ := hex.DecodeString("0000000000000007000000000000000900020000000000000005")
	if string(hard) != string(wantHard) {
		t.Fatalf("hard-state payload = %x, want %x", hard, wantHard)
	}
	snapshot := encodeSnapshotPayload(7, SnapshotMetadata{LastIncludedIndex: 5, LastIncludedTerm: 4, StateMachineSchemaVersion: 3})
	wantSnapshot, _ := hex.DecodeString("00000000000000070000000000000005000000000000000400000003")
	if string(snapshot) != string(wantSnapshot) {
		t.Fatalf("snapshot payload = %x, want %x", snapshot, wantSnapshot)
	}

	applied := uint64(1)
	transaction, err := encodeWALTransaction(7, PersistenceBatch{
		HardState:    hardStatePointer(HardState{Term: 2, CommitIndex: 1}),
		ReplaceFrom:  1,
		Entries:      []Entry{entry},
		AppliedIndex: &applied,
	})
	if err != nil {
		t.Fatal(err)
	}
	var types []walRecordType
	for offset := int64(0); offset < int64(len(transaction)); {
		recordType, _, next, partial, err := readWALRecordAtBytes(transaction, offset)
		if err != nil || partial {
			t.Fatalf("decode transaction record = type %d partial=%v err=%v", recordType, partial, err)
		}
		types = append(types, recordType)
		offset = next
	}
	wantTypes := []walRecordType{walRecordTransactionBegin, walRecordTruncate, walRecordEntries, walRecordAppliedIndex, walRecordHardState, walRecordTransactionCommit}
	if !reflect.DeepEqual(types, wantTypes) {
		t.Fatalf("record order = %v, want %v", types, wantTypes)
	}
}

func TestWALRestartAfterSuffixReplacementRecoversExactState(t *testing.T) {
	directory := t.TempDir()
	identity, voters := testStorageIdentity(t, 1)
	store, err := OpenFileStore(directory, identity, voters)
	if err != nil {
		t.Fatalf("OpenFileStore: %v", err)
	}
	if err := store.Persist(PersistenceBatch{
		HardState:   hardStatePointer(HardState{Term: 2, CommitIndex: 1}),
		ReplaceFrom: 1,
		Entries: []Entry{
			mustStorageEntry(t, 1, 1, "one"),
			mustStorageEntry(t, 2, 2, "old-two"),
			mustStorageEntry(t, 3, 2, "old-three"),
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(PersistenceBatch{
		HardState:   hardStatePointer(HardState{Term: 3, CommitIndex: 1}),
		ReplaceFrom: 2,
		Entries:     []Entry{mustStorageEntry(t, 2, 3, "new-two")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenFileStore(directory, identity, voters)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	state, err := reopened.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if state.HardState.Term != 3 || len(state.Entries) != 2 || string(state.Entries[1].CommandBytes()) != "new-two" {
		t.Fatalf("recovered wrong replacement state: %+v", state)
	}
}

func TestPartialFinalTransactionIsTruncatedAndSyncedAtEveryByte(t *testing.T) {
	identity, voters := testStorageIdentity(t, 1)
	second := PersistenceBatch{
		HardState:   hardStatePointer(HardState{Term: 2, CommitIndex: 1}),
		ReplaceFrom: 2,
		Entries:     []Entry{mustStorageEntry(t, 2, 2, "two")},
	}
	transaction, err := encodeWALTransaction(2, second)
	if err != nil {
		t.Fatal(err)
	}
	for cut := 1; cut < len(transaction); cut++ {
		t.Run(stringCutName(cut), func(t *testing.T) {
			directory := t.TempDir()
			baseline := initializeOneTransaction(t, directory, identity, voters)
			walPath := filepath.Join(directory, RaftStorageDirectoryName, RaftWALFilename)
			appendBytes(t, walPath, transaction[:cut])

			store, err := OpenFileStore(directory, identity, voters)
			if err != nil {
				t.Fatalf("recover partial cut %d: %v", cut, err)
			}
			state, recoverErr := store.Recover()
			if recoverErr != nil {
				t.Fatal(recoverErr)
			}
			if len(state.Entries) != 1 || state.HardState.Term != 1 {
				t.Fatalf("partial cut %d became visible: %+v", cut, state)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(walPath)
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() != int64(len(baseline)) {
				t.Fatalf("WAL size after cut %d recovery = %d, want %d", cut, info.Size(), len(baseline))
			}
		})
	}
}

func TestCorruptCompleteWALRecordIsFatalWithoutTruncation(t *testing.T) {
	directory := t.TempDir()
	identity, voters := testStorageIdentity(t, 1)
	baseline := initializeOneTransaction(t, directory, identity, voters)
	transaction, err := encodeWALTransaction(2, PersistenceBatch{HardState: hardStatePointer(HardState{Term: 2, CommitIndex: 1})})
	if err != nil {
		t.Fatal(err)
	}
	transaction[len(transaction)-1] ^= 0xff
	walPath := filepath.Join(directory, RaftStorageDirectoryName, RaftWALFilename)
	appendBytes(t, walPath, transaction)
	wantSize := int64(len(baseline) + len(transaction))

	if _, err := OpenFileStore(directory, identity, voters); !errors.Is(err, ErrStorageCorrupt) {
		t.Fatalf("OpenFileStore error = %v, want ErrStorageCorrupt", err)
	}
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != wantSize {
		t.Fatalf("complete corrupt WAL was truncated to %d, want untouched %d", info.Size(), wantSize)
	}
}

func TestCorruptWALRejectsUnknownTypeOversizeAndInvalidSequence(t *testing.T) {
	identity, voters := testStorageIdentity(t, 1)
	tests := []struct {
		name   string
		record func(t *testing.T) []byte
		text   string
	}{
		{
			name: "unknown type",
			record: func(t *testing.T) []byte {
				record, err := encodeWALRecord(walRecordTransactionBegin, mustBoundaryPayload(t, 1, transactionFlagHardState, 1))
				if err != nil {
					t.Fatal(err)
				}
				record[6] = 0xff
				rewriteRecordCRC(record)
				return record
			},
			text: "type",
		},
		{
			name: "oversized declaration",
			record: func(t *testing.T) []byte {
				header := make([]byte, walRecordHeaderBytes)
				copy(header, walMagic[:])
				binary.BigEndian.PutUint16(header[4:6], uint16(StorageFormatVersion1))
				header[6] = byte(walRecordTransactionBegin)
				binary.BigEndian.PutUint64(header[8:16], MaxWALRecordPayloadBytes+1)
				return header
			},
			text: "maximum",
		},
		{
			name: "record outside transaction",
			record: func(t *testing.T) []byte {
				record, err := encodeWALRecord(walRecordHardState, make([]byte, hardStatePayloadBytes))
				if err != nil {
					t.Fatal(err)
				}
				return record
			},
			text: "sequence",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			store, err := OpenFileStore(directory, identity, voters)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			walPath := filepath.Join(directory, RaftStorageDirectoryName, RaftWALFilename)
			appendBytes(t, walPath, test.record(t))
			_, err = OpenFileStore(directory, identity, voters)
			if !errors.Is(err, ErrStorageCorrupt) || !strings.Contains(err.Error(), test.text) {
				t.Fatalf("OpenFileStore error = %v, want ErrStorageCorrupt containing %q", err, test.text)
			}
		})
	}
}

func TestCorruptCompleteSemanticTransactionIsFatal(t *testing.T) {
	directory := t.TempDir()
	identity, voters := testStorageIdentity(t, 1)
	store, err := OpenFileStore(directory, identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	transaction, err := encodeWALTransaction(1, PersistenceBatch{HardState: hardStatePointer(HardState{Term: 1, CommitIndex: 9})})
	if err != nil {
		t.Fatal(err)
	}
	appendBytes(t, filepath.Join(directory, RaftStorageDirectoryName, RaftWALFilename), transaction)
	if _, err := OpenFileStore(directory, identity, voters); !errors.Is(err, ErrStorageCorrupt) {
		t.Fatalf("OpenFileStore error = %v, want fatal semantic ErrStorageCorrupt", err)
	}
}

func TestWALHostileCountsLengthsFlagsAndTrailingBytesFailClosed(t *testing.T) {
	boundary := mustBoundaryPayload(t, 1, transactionFlagHardState, 1)
	badCount := append([]byte(nil), boundary...)
	badCount[9] = 2
	badFlags := append([]byte(nil), boundary...)
	badFlags[8] = 0x80
	badReserved := append([]byte(nil), boundary...)
	badReserved[11] = 1
	for name, payload := range map[string][]byte{
		"count":    badCount,
		"flags":    badFlags,
		"reserved": badReserved,
		"trailing": append(append([]byte(nil), boundary...), 0),
	} {
		t.Run("boundary_"+name, func(t *testing.T) {
			if _, _, _, err := decodeTransactionBoundaryPayload(payload); !errors.Is(err, ErrStorageCorrupt) {
				t.Fatalf("decode boundary error = %v, want ErrStorageCorrupt", err)
			}
		})
	}

	impossibleCount := make([]byte, 10)
	binary.BigEndian.PutUint64(impossibleCount[:8], 1)
	binary.BigEndian.PutUint16(impossibleCount[8:10], ^uint16(0))
	if _, err := decodeEntriesPayload(impossibleCount, 1); !errors.Is(err, ErrStorageCorrupt) {
		t.Fatalf("impossible count error = %v, want ErrStorageCorrupt", err)
	}
	badCommandLength := make([]byte, 10+minimumWALEntryBytes)
	binary.BigEndian.PutUint64(badCommandLength[:8], 1)
	binary.BigEndian.PutUint16(badCommandLength[8:10], 1)
	binary.BigEndian.PutUint64(badCommandLength[10:18], 1)
	binary.BigEndian.PutUint64(badCommandLength[18:26], 1)
	badCommandLength[26] = byte(EntryCommand)
	binary.BigEndian.PutUint32(badCommandLength[27:31], uint32(config.MaxRaftCommandBytes+1))
	if _, err := decodeEntriesPayload(badCommandLength, 1); !errors.Is(err, ErrStorageCorrupt) {
		t.Fatalf("impossible command length error = %v, want ErrStorageCorrupt", err)
	}
	validEntries, err := encodeEntriesPayload(1, []Entry{mustStorageEntry(t, 1, 1, "one")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeEntriesPayload(append(validEntries, 0), 1); !errors.Is(err, ErrStorageCorrupt) {
		t.Fatalf("trailing entries error = %v, want ErrStorageCorrupt", err)
	}
}

func TestWALRestartAfterEveryEffectRecordKind(t *testing.T) {
	directory := t.TempDir()
	identity, voters := testStorageIdentity(t, 1)
	store, err := OpenFileStore(directory, identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(PersistenceBatch{
		HardState:   hardStatePointer(HardState{Term: 3, CommitIndex: 2}),
		ReplaceFrom: 1,
		Entries: []Entry{
			mustStorageEntry(t, 1, 1, "one"),
			mustStorageEntry(t, 2, 2, "two"),
			mustStorageEntry(t, 3, 3, "three"),
		},
	}); err != nil {
		t.Fatal(err)
	}
	applied := uint64(2)
	if err := store.Persist(PersistenceBatch{AppliedIndex: &applied}); err != nil {
		t.Fatal(err)
	}
	base := SnapshotMetadata{LastIncludedIndex: 2, LastIncludedTerm: 2, StateMachineSchemaVersion: 1}
	snapshot, err := NewSnapshot(identity, base, []byte("state-at-two"), 64)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PersistSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(PersistenceBatch{ReplaceFrom: 3}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenFileStore(directory, identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	state, err := reopened.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if state.SnapshotBase != base || state.AppliedIndex != 2 || state.HardState.CommitIndex != 2 || len(state.Entries) != 0 {
		t.Fatalf("recovered effect records = %+v", state)
	}
}

func initializeOneTransaction(t *testing.T, directory string, identity StorageIdentity, voters VoterSet) []byte {
	t.Helper()
	store, err := OpenFileStore(directory, identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(PersistenceBatch{
		HardState:   hardStatePointer(HardState{Term: 1, CommitIndex: 1}),
		ReplaceFrom: 1,
		Entries:     []Entry{mustStorageEntry(t, 1, 1, "one")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(filepath.Join(directory, RaftStorageDirectoryName, RaftWALFilename))
	if err != nil {
		t.Fatal(err)
	}
	return content
}

func appendBytes(t *testing.T, path string, content []byte) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(content); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func rewriteRecordCRC(record []byte) {
	payloadLength := binary.BigEndian.Uint64(record[8:16])
	checksumAt := uint64(walRecordHeaderBytes) + payloadLength
	binary.BigEndian.PutUint32(record[checksumAt:checksumAt+walRecordChecksumBytes], crc32.Checksum(record[:checksumAt], crc32.MakeTable(crc32.Castagnoli)))
}

func readWALRecordAtBytes(content []byte, offset int64) (walRecordType, []byte, int64, bool, error) {
	directory, err := os.MkdirTemp("", "raft-record-test-")
	if err != nil {
		return 0, nil, 0, false, err
	}
	defer os.RemoveAll(directory)
	path := filepath.Join(directory, "record")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		return 0, nil, 0, false, err
	}
	file, err := os.Open(path)
	if err != nil {
		return 0, nil, 0, false, err
	}
	defer file.Close()
	return readWALRecordAt(file, offset, int64(len(content)))
}

func mustBoundaryPayload(t *testing.T, transactionID uint64, flags transactionFlags, count uint8) []byte {
	t.Helper()
	payload, err := encodeTransactionBoundaryPayload(transactionID, flags, count)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func stringCutName(cut int) string {
	const digits = "0123456789"
	if cut == 0 {
		return "cut_0"
	}
	buffer := make([]byte, 0, 24)
	for value := cut; value > 0; value /= 10 {
		buffer = append(buffer, digits[value%10])
	}
	for left, right := 0, len(buffer)-1; left < right; left, right = left+1, right-1 {
		buffer[left], buffer[right] = buffer[right], buffer[left]
	}
	return "cut_" + string(buffer)
}
