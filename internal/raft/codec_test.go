package raft

import (
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"crane/internal/config"
	"crane/internal/wire"
)

func TestCodecRejectsEveryTruncatedPrefix(t *testing.T) {
	for _, rpc := range validRPCFixtures(t) {
		message, encoded, err := EncodeRPC(rpc, DefaultCodecLimits())
		if err != nil {
			t.Fatalf("EncodeRPC(%T): %v", rpc, err)
		}
		for length := 0; length < len(encoded); length++ {
			if _, err := DecodeRPC(message, encoded[:length], DefaultCodecLimits()); err == nil {
				t.Fatalf("DecodeRPC(%T) accepted truncated prefix %d/%d", rpc, length, len(encoded))
			}
		}
	}
}

func TestCodecRejectsHostileCountsLengthsOffsetsAndTrailingData(t *testing.T) {
	appendRPC := AppendEntriesRequest{LeaderID: 1, Term: 1, Generation: 1}
	_, appendBytes, err := EncodeRPC(appendRPC, DefaultCodecLimits())
	if err != nil {
		t.Fatalf("Encode append: %v", err)
	}
	impossibleCount := append([]byte(nil), appendBytes...)
	binary.BigEndian.PutUint16(impossibleCount[44:46], math.MaxUint16)
	if _, err := DecodeRPC(wire.MessageRaftAppendEntriesRequest, impossibleCount, DefaultCodecLimits()); !errors.Is(err, ErrRPCTooLarge) {
		t.Fatalf("impossible count error = %v, want ErrRPCTooLarge", err)
	}

	commandLength := rawAppendWithDeclaredCommandLength(5)
	if _, err := DecodeRPC(wire.MessageRaftAppendEntriesRequest, commandLength, CodecLimits{MaxCommandBytes: 4}); !errors.Is(err, ErrRPCTooLarge) {
		t.Fatalf("oversized command error = %v, want ErrRPCTooLarge", err)
	}

	snapshotLength := rawSnapshotWithDeclaredChunkLength(5)
	if _, err := DecodeRPC(wire.MessageRaftInstallSnapshotRequest, snapshotLength, CodecLimits{MaxSnapshotChunkBytes: 4}); !errors.Is(err, ErrRPCTooLarge) {
		t.Fatalf("oversized chunk error = %v, want ErrRPCTooLarge", err)
	}

	overflow := rawSnapshotWithOffset(math.MaxUint64, math.MaxUint64, []byte{1})
	if _, err := DecodeRPC(wire.MessageRaftInstallSnapshotRequest, overflow, DefaultCodecLimits()); !errors.Is(err, ErrRPCTooLarge) {
		t.Fatalf("offset overflow error = %v, want ErrRPCTooLarge", err)
	}

	message, valid, err := EncodeRPC(task5Handshake(1, VoterFingerprint{1}), DefaultCodecLimits())
	if err != nil {
		t.Fatalf("Encode handshake: %v", err)
	}
	for _, trailing := range [][]byte{{0}, {1, 2, 3}, make([]byte, 257)} {
		candidate := append(append([]byte(nil), valid...), trailing...)
		if _, err := DecodeRPC(message, candidate, DefaultCodecLimits()); err == nil {
			t.Fatalf("DecodeRPC accepted %d trailing bytes", len(trailing))
		}
	}
}

func TestCodecRejectsDeclaredCollectionsBeforeAllocation(t *testing.T) {
	impossibleCount := rawAppendWithCount(math.MaxUint16)
	allocations := testing.AllocsPerRun(100, func() {
		_, _ = DecodeRPC(wire.MessageRaftAppendEntriesRequest, impossibleCount, DefaultCodecLimits())
	})
	if allocations > 12 {
		t.Fatalf("impossible count used %.1f allocations, want at most 12 bounded error allocations", allocations)
	}
}

func TestCodecRejectsHostileAppendSemanticsBeforeVariableAllocation(t *testing.T) {
	largeCommand := make([]byte, 128<<10)
	oneEntry := encodedRPCPayload(t, AppendEntriesRequest{LeaderID: 1, Term: 3, Generation: 1, Entries: []Entry{mustEntry(t, 1, 3, EntryCommand, largeCommand)}})
	twoEntries := encodedRPCPayload(t, AppendEntriesRequest{LeaderID: 1, Term: 3, Generation: 1, Entries: []Entry{mustEntry(t, 1, 3, EntryCommand, largeCommand), mustEntry(t, 2, 3, EntryNoOp, nil)}})
	manyEntries := make([]Entry, 64)
	manyEntries[0] = mustEntry(t, 1, 3, EntryCommand, largeCommand)
	for index := 1; index < len(manyEntries); index++ {
		manyEntries[index] = mustEntry(t, uint64(index+1), 3, EntryNoOp, nil)
	}
	manyEntryPayload := encodedRPCPayload(t, AppendEntriesRequest{LeaderID: 1, Term: 3, Generation: 1, Entries: manyEntries})

	tests := []struct {
		name    string
		payload []byte
	}{
		{name: "zero_generation", payload: mutateUint64(manyEntryPayload, 12, 0)},
		{name: "zero_leader_correlation", payload: mutateUint16(oneEntry, 2, 0)},
		{name: "invalid_previous_position", payload: mutateUint64(oneEntry, 20, 1)},
		{name: "entry_ordering", payload: mutateUint64(twoEntries, 46, 2)},
		{name: "entry_kind", payload: mutateByte(oneEntry, 62, 99)},
		{name: "entry_term_above_leader", payload: mutateUint64(oneEntry, 54, 4)},
		{name: "trailing_data", payload: append(append([]byte(nil), oneEntry...), 0)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertRejectedWithoutVariableAllocation(t, wire.MessageRaftAppendEntriesRequest, test.payload)
		})
	}
}

func TestCodecRejectsHostileSnapshotSemanticsBeforeVariableAllocation(t *testing.T) {
	chunk := make([]byte, 128<<10)
	valid := encodedRPCPayload(t, InstallSnapshotRequest{LeaderID: 1, Term: 3, TransferID: TransferID{1}, SnapshotID: SnapshotID{1}, LastIncludedIndex: 2, LastIncludedTerm: 2, StateMachineSchemaVersion: 1, TotalLength: uint64(len(chunk)), Checksum: SnapshotChecksum{1}, Chunk: chunk, Done: true})

	tests := []struct {
		name    string
		payload []byte
	}{
		{name: "zero_leader_correlation", payload: mutateUint16(valid, 2, 0)},
		{name: "zero_transfer", payload: mutateZeroRange(valid, 12, 28)},
		{name: "zero_snapshot_identity", payload: mutateZeroRange(valid, 28, 44)},
		{name: "included_term_above_leader", payload: mutateUint64(valid, 52, 4)},
		{name: "offset_plus_length_range", payload: mutateUint64(valid, 104, 1)},
		{name: "done_mismatch", payload: mutateByte(valid, len(valid)-1, 0)},
		{name: "trailing_data", payload: append(append([]byte(nil), valid...), 0)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertRejectedWithoutVariableAllocation(t, wire.MessageRaftInstallSnapshotRequest, test.payload)
		})
	}
}

func TestCodecAbsoluteCeilingsCannotBeRaised(t *testing.T) {
	rpc := task5Handshake(1, VoterFingerprint{1})
	tests := []struct {
		name   string
		limits CodecLimits
	}{
		{name: "command", limits: CodecLimits{MaxCommandBytes: config.MaxRaftCommandBytes + 1}},
		{name: "snapshot_chunk", limits: CodecLimits{MaxSnapshotChunkBytes: config.MaxRaftSnapshotChunkBytes + 1}},
		{name: "snapshot_total", limits: CodecLimits{MaxSnapshotBytes: config.MaxRaftSnapshotBytes + 1}},
		{name: "encoded_payload", limits: CodecLimits{MaxEncodedBytes: MaxRPCPayloadBytes + 1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := EncodeRPC(rpc, test.limits); !errors.Is(err, ErrRPCTooLarge) {
				t.Fatalf("EncodeRPC error = %v, want ErrRPCTooLarge", err)
			}
		})
	}
}

func TestCodecAbsoluteCeilingBoundariesAreAccepted(t *testing.T) {
	command := make([]byte, config.MaxRaftCommandBytes)
	appendRPC := AppendEntriesRequest{LeaderID: 1, Term: 1, Generation: 1, Entries: []Entry{mustEntry(t, 1, 1, EntryCommand, command)}}
	if _, _, err := EncodeRPC(appendRPC, DefaultCodecLimits()); err != nil {
		t.Fatalf("EncodeRPC command boundary: %v", err)
	}

	chunk := make([]byte, config.MaxRaftSnapshotChunkBytes)
	snapshotRPC := InstallSnapshotRequest{LeaderID: 1, Term: 1, TransferID: TransferID{1}, SnapshotID: SnapshotID{1}, LastIncludedIndex: 1, LastIncludedTerm: 1, StateMachineSchemaVersion: 1, TotalLength: uint64(len(chunk)), Checksum: SnapshotChecksum{1}, Chunk: chunk, Done: true}
	if _, _, err := EncodeRPC(snapshotRPC, DefaultCodecLimits()); err != nil {
		t.Fatalf("EncodeRPC chunk boundary: %v", err)
	}

	snapshotRPC.TotalLength = config.MaxRaftSnapshotBytes
	snapshotRPC.Chunk = []byte{1}
	snapshotRPC.Done = false
	if _, _, err := EncodeRPC(snapshotRPC, DefaultCodecLimits()); err != nil {
		t.Fatalf("EncodeRPC snapshot boundary: %v", err)
	}

	limits := DefaultCodecLimits()
	limits.MaxEncodedBytes = MaxRPCPayloadBytes
	if _, _, err := EncodeRPC(task5Handshake(1, VoterFingerprint{1}), limits); err != nil {
		t.Fatalf("EncodeRPC encoded boundary: %v", err)
	}
}

func TestCodecAbsoluteDataCeilingsRejectOneAbove(t *testing.T) {
	command := make([]byte, config.MaxRaftCommandBytes+1)
	appendRPC := AppendEntriesRequest{LeaderID: 1, Term: 1, Generation: 1, Entries: []Entry{mustEntry(t, 1, 1, EntryCommand, command)}}
	if _, _, err := EncodeRPC(appendRPC, CodecLimits{MaxCommandBytes: config.MaxRaftCommandBytes + 1}); !errors.Is(err, ErrRPCTooLarge) {
		t.Fatalf("command one-above error = %v, want ErrRPCTooLarge", err)
	}

	chunk := make([]byte, config.MaxRaftSnapshotChunkBytes+1)
	snapshotRPC := InstallSnapshotRequest{LeaderID: 1, Term: 1, TransferID: TransferID{1}, SnapshotID: SnapshotID{1}, LastIncludedIndex: 1, LastIncludedTerm: 1, StateMachineSchemaVersion: 1, TotalLength: uint64(len(chunk)), Checksum: SnapshotChecksum{1}, Chunk: chunk, Done: true}
	if _, _, err := EncodeRPC(snapshotRPC, CodecLimits{MaxSnapshotChunkBytes: config.MaxRaftSnapshotChunkBytes + 1}); !errors.Is(err, ErrRPCTooLarge) {
		t.Fatalf("chunk one-above error = %v, want ErrRPCTooLarge", err)
	}

	snapshotRPC.TotalLength = config.MaxRaftSnapshotBytes + 1
	snapshotRPC.Chunk = []byte{1}
	snapshotRPC.Done = false
	if _, _, err := EncodeRPC(snapshotRPC, CodecLimits{MaxSnapshotBytes: config.MaxRaftSnapshotBytes + 1}); !errors.Is(err, ErrRPCTooLarge) {
		t.Fatalf("snapshot one-above error = %v, want ErrRPCTooLarge", err)
	}
}

func FuzzCodec(f *testing.F) {
	for _, rpc := range validRPCFixturesForFuzz() {
		message, encoded, err := EncodeRPC(rpc, DefaultCodecLimits())
		if err != nil {
			f.Fatalf("EncodeRPC(%T): %v", rpc, err)
		}
		f.Add(uint16(message), encoded)
		for length := 0; length < len(encoded); length++ {
			f.Add(uint16(message), append([]byte(nil), encoded[:length]...))
		}
	}
	f.Add(uint16(wire.MessageRaftAppendEntriesRequest), rawAppendWithCount(math.MaxUint16))
	f.Add(uint16(wire.MessageRaftAppendEntriesRequest), rawAppendWithDeclaredCommandLength(math.MaxUint32))
	f.Add(uint16(wire.MessageRaftInstallSnapshotRequest), rawSnapshotWithDeclaredChunkLength(math.MaxUint32))
	f.Add(uint16(wire.MessageRaftInstallSnapshotRequest), rawSnapshotWithOffset(math.MaxUint64, math.MaxUint64, []byte{1}))

	f.Fuzz(func(t *testing.T, message uint16, payload []byte) {
		if len(payload) > MaxRPCPayloadBytes+1 {
			t.Skip()
		}
		_, _ = DecodeRPC(wire.MessageType(message), payload, DefaultCodecLimits())
	})
}

func validRPCFixtures(t *testing.T) []RPC {
	t.Helper()
	return validRPCFixturesForFuzz()
}

func validRPCFixturesForFuzz() []RPC {
	return []RPC{
		task5Handshake(1, VoterFingerprint{1}),
		task5HandshakeAck(2, VoterFingerprint{1}),
		PreVoteRequest{CandidateID: 1, CurrentTerm: 1, ProspectiveTerm: 2},
		PreVoteResponse{ResponderID: 2, CandidateID: 1, Term: 3, RequestCurrentTerm: 1, ProspectiveTerm: 2},
		RequestVoteRequest{CandidateID: 1, Term: 2},
		RequestVoteResponse{ResponderID: 2, CandidateID: 1, Term: 2, RequestTerm: 2},
		AppendEntriesRequest{LeaderID: 1, Term: 2, Generation: 1},
		AppendEntriesResponse{ResponderID: 2, LeaderID: 1, Term: 2, RequestTerm: 2, Generation: 1, Success: true},
		InstallSnapshotRequest{LeaderID: 1, Term: 2, TransferID: TransferID{1}, SnapshotID: SnapshotID{1}, LastIncludedIndex: 1, LastIncludedTerm: 1, StateMachineSchemaVersion: 1, Checksum: SnapshotChecksum{1}, Done: true},
		InstallSnapshotResponse{ResponderID: 2, LeaderID: 1, Term: 2, RequestTerm: 2, TransferID: TransferID{1}, SnapshotID: SnapshotID{1}, Success: true, Done: true},
		ErrorResponse{Code: ProtocolErrorMalformed, ResponderID: 2, Term: 2},
	}
}

func rawAppendWithCount(count uint16) []byte {
	payload := make([]byte, 46)
	binary.BigEndian.PutUint16(payload[0:2], RPCSchemaVersion)
	binary.BigEndian.PutUint16(payload[2:4], 1)
	binary.BigEndian.PutUint64(payload[4:12], 1)
	binary.BigEndian.PutUint64(payload[12:20], 1)
	binary.BigEndian.PutUint16(payload[44:46], count)
	return payload
}

func rawAppendWithDeclaredCommandLength(length uint32) []byte {
	payload := rawAppendWithCount(1)
	entry := make([]byte, 21)
	binary.BigEndian.PutUint64(entry[0:8], 1)
	binary.BigEndian.PutUint64(entry[8:16], 1)
	entry[16] = byte(EntryCommand)
	binary.BigEndian.PutUint32(entry[17:21], length)
	return append(payload, entry...)
}

func rawSnapshotWithDeclaredChunkLength(length uint32) []byte {
	return rawSnapshotWithOffsetAndDeclaredLength(0, uint64(length), length, nil)
}

func rawSnapshotWithOffset(offset, total uint64, chunk []byte) []byte {
	return rawSnapshotWithOffsetAndDeclaredLength(offset, total, uint32(len(chunk)), chunk)
}

func rawSnapshotWithOffsetAndDeclaredLength(offset, total uint64, length uint32, chunk []byte) []byte {
	payload := make([]byte, 116+len(chunk)+1)
	binary.BigEndian.PutUint16(payload[0:2], RPCSchemaVersion)
	binary.BigEndian.PutUint16(payload[2:4], 1)
	binary.BigEndian.PutUint64(payload[4:12], 1)
	payload[12] = 1
	payload[28] = 1
	binary.BigEndian.PutUint64(payload[44:52], 1)
	binary.BigEndian.PutUint64(payload[52:60], 1)
	binary.BigEndian.PutUint32(payload[60:64], 1)
	binary.BigEndian.PutUint64(payload[64:72], total)
	payload[72] = 1
	binary.BigEndian.PutUint64(payload[104:112], offset)
	binary.BigEndian.PutUint32(payload[112:116], length)
	copy(payload[116:], chunk)
	payload[len(payload)-1] = 1
	return payload
}

func encodedRPCPayload(t *testing.T, rpc RPC) []byte {
	t.Helper()
	_, payload, err := EncodeRPC(rpc, DefaultCodecLimits())
	if err != nil {
		t.Fatalf("EncodeRPC(%T): %v", rpc, err)
	}
	return payload
}

func assertRejectedWithoutVariableAllocation(t *testing.T, message wire.MessageType, payload []byte) {
	t.Helper()
	if _, err := DecodeRPC(message, payload, DefaultCodecLimits()); err == nil {
		t.Fatal("DecodeRPC accepted hostile payload")
	}
	result := testing.Benchmark(func(b *testing.B) {
		for range b.N {
			_, _ = DecodeRPC(message, payload, DefaultCodecLimits())
		}
	})
	if allocated := result.AllocedBytesPerOp(); allocated > 512 {
		t.Fatalf("rejected decode allocated %d bytes/op, want at most 512 fixed error bytes", allocated)
	}
}

func mutateUint16(payload []byte, offset int, value uint16) []byte {
	mutated := append([]byte(nil), payload...)
	binary.BigEndian.PutUint16(mutated[offset:offset+2], value)
	return mutated
}

func mutateUint64(payload []byte, offset int, value uint64) []byte {
	mutated := append([]byte(nil), payload...)
	binary.BigEndian.PutUint64(mutated[offset:offset+8], value)
	return mutated
}

func mutateByte(payload []byte, offset int, value byte) []byte {
	mutated := append([]byte(nil), payload...)
	mutated[offset] = value
	return mutated
}

func mutateZeroRange(payload []byte, start, end int) []byte {
	mutated := append([]byte(nil), payload...)
	clear(mutated[start:end])
	return mutated
}
