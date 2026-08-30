package raft

import (
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"github.com/aaditya/cs425mp3/internal/wire"
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
	if _, err := DecodeRPC(wire.MessageRaftInstallSnapshotRequest, overflow, CodecLimits{MaxSnapshotBytes: math.MaxUint64}); !errors.Is(err, ErrInvalidRPC) {
		t.Fatalf("offset overflow error = %v, want ErrInvalidRPC", err)
	}

	message, valid, err := EncodeRPC(Handshake{SenderID: 1, VoterFingerprint: VoterFingerprint{1}}, DefaultCodecLimits())
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
		Handshake{SenderID: 1, VoterFingerprint: VoterFingerprint{1}},
		HandshakeAck{ResponderID: 2, VoterFingerprint: VoterFingerprint{1}},
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
	payload := make([]byte, 114+len(chunk)+1)
	binary.BigEndian.PutUint16(payload[0:2], RPCSchemaVersion)
	binary.BigEndian.PutUint16(payload[2:4], 1)
	binary.BigEndian.PutUint64(payload[4:12], 1)
	payload[12] = 1
	payload[28] = 1
	binary.BigEndian.PutUint64(payload[44:52], 1)
	binary.BigEndian.PutUint64(payload[52:60], 1)
	binary.BigEndian.PutUint16(payload[60:62], 1)
	binary.BigEndian.PutUint64(payload[62:70], total)
	payload[70] = 1
	binary.BigEndian.PutUint64(payload[102:110], offset)
	binary.BigEndian.PutUint32(payload[110:114], length)
	copy(payload[114:], chunk)
	payload[len(payload)-1] = 1
	return payload
}
