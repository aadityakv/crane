package raft

import (
	"encoding/hex"
	"errors"
	"math"
	"reflect"
	"testing"

	"crane/internal/wire"
)

func TestMessageExactCanonicalLayouts(t *testing.T) {
	fingerprint := VoterFingerprint{1}
	transferID := TransferID{1}
	snapshotID := SnapshotID{2}
	checksum := SnapshotChecksum{3}
	entries := []Entry{
		mustEntry(t, 5, 3, EntryNoOp, nil),
		mustEntry(t, 6, 3, EntryCommand, []byte("x")),
	}
	tests := []struct {
		name    string
		rpc     RPC
		message wire.MessageType
		wantHex string
	}{
		{name: "handshake", rpc: task5Handshake(1, fingerprint), message: wire.MessageRaftHandshake, wantHex: "00020001010000000000000000000000000000000000000000000000000000000000000010a44b7bf119aec85037e343680323c220ee02b09a627298dc8965fba4ae021b"},
		{name: "handshake_ack", rpc: task5HandshakeAck(2, fingerprint), message: wire.MessageRaftHandshakeAck, wantHex: "00020002010000000000000000000000000000000000000000000000000000000000000010a44b7bf119aec85037e343680323c220ee02b09a627298dc8965fba4ae021b"},
		{name: "pre_vote_request", rpc: PreVoteRequest{CandidateID: 1, CurrentTerm: 2, ProspectiveTerm: 3, LastLogIndex: 4, LastLogTerm: 2}, message: wire.MessageRaftPreVoteRequest, wantHex: "000100010000000000000002000000000000000300000000000000040000000000000002"},
		{name: "pre_vote_response", rpc: PreVoteResponse{ResponderID: 2, CandidateID: 1, Term: 1, RequestCurrentTerm: 2, ProspectiveTerm: 3, Granted: true}, message: wire.MessageRaftPreVoteResponse, wantHex: "00010002000100000000000000010000000000000002000000000000000301"},
		{name: "vote_request", rpc: RequestVoteRequest{CandidateID: 1, Term: 3, LastLogIndex: 4, LastLogTerm: 2}, message: wire.MessageRaftRequestVoteRequest, wantHex: "00010001000000000000000300000000000000040000000000000002"},
		{name: "vote_response", rpc: RequestVoteResponse{ResponderID: 2, CandidateID: 1, Term: 3, RequestTerm: 3, Granted: true}, message: wire.MessageRaftRequestVoteResponse, wantHex: "0001000200010000000000000003000000000000000301"},
		{name: "append_request", rpc: AppendEntriesRequest{LeaderID: 1, Term: 3, Generation: 9, PrevLogIndex: 4, PrevLogTerm: 2, LeaderCommit: 4, Entries: entries}, message: wire.MessageRaftAppendEntriesRequest, wantHex: "0001000100000000000000030000000000000009000000000000000400000000000000020000000000000004000200000000000000050000000000000003020000000000000000000000060000000000000003010000000178"},
		{name: "append_response", rpc: AppendEntriesResponse{ResponderID: 2, LeaderID: 1, Term: 3, RequestTerm: 3, Generation: 9, Success: true, MatchIndex: 6}, message: wire.MessageRaftAppendEntriesResponse, wantHex: "00010002000100000000000000030000000000000003000000000000000901000000000000000600000000000000000000000000000000"},
		{name: "snapshot_request", rpc: InstallSnapshotRequest{LeaderID: 1, Term: 4, TransferID: transferID, SnapshotID: snapshotID, LastIncludedIndex: 10, LastIncludedTerm: 3, StateMachineSchemaVersion: 1, TotalLength: 3, Checksum: checksum, Offset: 0, Chunk: []byte("abc"), Done: true}, message: wire.MessageRaftInstallSnapshotRequest, wantHex: "0001000100000000000000040100000000000000000000000000000002000000000000000000000000000000000000000000000a0000000000000003000000010000000000000003030000000000000000000000000000000000000000000000000000000000000000000000000000000000000361626301"},
		{name: "snapshot_response", rpc: InstallSnapshotResponse{ResponderID: 2, LeaderID: 1, Term: 4, RequestTerm: 4, TransferID: transferID, SnapshotID: snapshotID, NextOffset: 3, Success: true, Done: true}, message: wire.MessageRaftInstallSnapshotResponse, wantHex: "00010002000100000000000000040000000000000004010000000000000000000000000000000200000000000000000000000000000000000000000000030101"},
		{name: "protocol_error", rpc: ErrorResponse{Code: ProtocolErrorMalformed, ResponderID: 2, Term: 4}, message: wire.MessageRaftError, wantHex: "0001000100020000000000000004"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			message, encoded, err := EncodeRPC(test.rpc, DefaultCodecLimits())
			if err != nil {
				t.Fatalf("EncodeRPC: %v", err)
			}
			if message != test.message {
				t.Fatalf("message = %d, want %d", message, test.message)
			}
			if got := hex.EncodeToString(encoded); got != test.wantHex {
				t.Fatalf("canonical bytes = %s, want %s", got, test.wantHex)
			}
			decoded, err := DecodeRPC(message, encoded, DefaultCodecLimits())
			if err != nil {
				t.Fatalf("DecodeRPC: %v", err)
			}
			if !reflect.DeepEqual(decoded, test.rpc) {
				t.Fatalf("round trip = %#v, want %#v", decoded, test.rpc)
			}
		})
	}
}

func TestMessageValidAndInvalidDomains(t *testing.T) {
	validSet := mustVoterSet(t, 3)
	fingerprint := validSet.Fingerprint()
	transferID := TransferID{1}
	snapshotID := SnapshotID{2}
	checksum := SnapshotChecksum{3}
	valid := []RPC{
		task5Handshake(1, fingerprint),
		task5HandshakeAck(2, fingerprint),
		PreVoteRequest{CandidateID: 1, CurrentTerm: 0, ProspectiveTerm: 1, LastLogIndex: 0, LastLogTerm: 0},
		PreVoteResponse{ResponderID: 2, CandidateID: 1, Term: 1, RequestCurrentTerm: 2, ProspectiveTerm: 3, Granted: true},
		PreVoteResponse{ResponderID: 2, CandidateID: 1, Term: 4, RequestCurrentTerm: 2, ProspectiveTerm: 3},
		RequestVoteRequest{CandidateID: 1, Term: 3, LastLogIndex: 4, LastLogTerm: 2},
		RequestVoteResponse{ResponderID: 2, CandidateID: 1, Term: 4, RequestTerm: 3},
		AppendEntriesRequest{LeaderID: 1, Term: 5, Generation: 8, PrevLogIndex: 1, PrevLogTerm: 2, Entries: []Entry{mustEntry(t, 2, 2, EntryNoOp, nil), mustEntry(t, 3, 4, EntryNoOp, nil), mustEntry(t, 4, 5, EntryNoOp, nil)}},
		AppendEntriesRequest{LeaderID: 1, Term: 3, Generation: 7, PrevLogIndex: 0, PrevLogTerm: 0},
		AppendEntriesResponse{ResponderID: 2, LeaderID: 1, Term: 3, RequestTerm: 3, Generation: 7, Success: true},
		AppendEntriesResponse{ResponderID: 2, LeaderID: 1, Term: 3, RequestTerm: 3, Generation: 7, ConflictIndex: 1},
		AppendEntriesResponse{ResponderID: 2, LeaderID: 1, Term: 4, RequestTerm: 3, Generation: 7, ConflictIndex: 1},
		InstallSnapshotRequest{LeaderID: 1, Term: 3, TransferID: transferID, SnapshotID: snapshotID, LastIncludedIndex: 4, LastIncludedTerm: 2, StateMachineSchemaVersion: 1, TotalLength: 0, Checksum: checksum, Done: true},
		InstallSnapshotResponse{ResponderID: 2, LeaderID: 1, Term: 3, RequestTerm: 3, TransferID: transferID, SnapshotID: snapshotID, Success: true, Done: true},
		InstallSnapshotResponse{ResponderID: 2, LeaderID: 1, Term: 4, RequestTerm: 3, TransferID: transferID, SnapshotID: snapshotID},
		InstallSnapshotResponse{ResponderID: 2, LeaderID: 1, Term: 4, RequestTerm: 3, TransferID: transferID, SnapshotID: snapshotID, Success: true},
		ErrorResponse{Code: ProtocolErrorMalformed, ResponderID: 2, Term: 3},
	}
	for _, rpc := range valid {
		message, encoded, err := EncodeRPC(rpc, DefaultCodecLimits())
		if err != nil {
			t.Fatalf("EncodeRPC(%T): %v", rpc, err)
		}
		if _, err := DecodeRPC(message, encoded, DefaultCodecLimits()); err != nil {
			t.Fatalf("DecodeRPC(%T): %v", rpc, err)
		}
	}

	oversizedCommand := make([]byte, 5)
	oversizedChunk := make([]byte, 5)
	invalid := []struct {
		name   string
		rpc    RPC
		limits CodecLimits
	}{
		{name: "zero_handshake_id", rpc: Handshake{VoterFingerprint: fingerprint, ApplicationFingerprint: task5ApplicationFingerprint}},
		{name: "zero_fingerprint", rpc: Handshake{SenderID: 1, ApplicationFingerprint: task5ApplicationFingerprint}},
		{name: "zero_application_fingerprint", rpc: Handshake{SenderID: 1, VoterFingerprint: fingerprint}},
		{name: "same_handshake_ack_actor", rpc: HandshakeAck{ResponderID: 0, VoterFingerprint: fingerprint, ApplicationFingerprint: task5ApplicationFingerprint}},
		{name: "pre_vote_prospective_not_next", rpc: PreVoteRequest{CandidateID: 1, CurrentTerm: 2, ProspectiveTerm: 4}},
		{name: "pre_vote_term_overflow", rpc: PreVoteRequest{CandidateID: 1, CurrentTerm: ^uint64(0), ProspectiveTerm: 0}},
		{name: "pre_vote_bad_log_pair", rpc: PreVoteRequest{CandidateID: 1, CurrentTerm: 2, ProspectiveTerm: 3, LastLogTerm: 1}},
		{name: "pre_vote_response_zero_candidate", rpc: PreVoteResponse{ResponderID: 2, Term: 2, RequestCurrentTerm: 2, ProspectiveTerm: 3}},
		{name: "pre_vote_response_higher_term_grant", rpc: PreVoteResponse{ResponderID: 2, CandidateID: 1, Term: 4, RequestCurrentTerm: 2, ProspectiveTerm: 3, Granted: true}},
		{name: "pre_vote_last_log_term_above_current", rpc: PreVoteRequest{CandidateID: 1, CurrentTerm: 2, ProspectiveTerm: 3, LastLogIndex: 1, LastLogTerm: 3}},
		{name: "vote_zero_term", rpc: RequestVoteRequest{CandidateID: 1}},
		{name: "vote_response_current_before_request", rpc: RequestVoteResponse{ResponderID: 2, CandidateID: 1, Term: 2, RequestTerm: 3}},
		{name: "vote_response_higher_term_grant", rpc: RequestVoteResponse{ResponderID: 2, CandidateID: 1, Term: 4, RequestTerm: 3, Granted: true}},
		{name: "vote_last_log_term_above_candidate", rpc: RequestVoteRequest{CandidateID: 1, Term: 2, LastLogIndex: 1, LastLogTerm: 3}},
		{name: "append_zero_generation", rpc: AppendEntriesRequest{LeaderID: 1, Term: 3}},
		{name: "append_bad_previous_pair", rpc: AppendEntriesRequest{LeaderID: 1, Term: 3, Generation: 1, PrevLogTerm: 1}},
		{name: "append_previous_term_above_leader", rpc: AppendEntriesRequest{LeaderID: 1, Term: 2, Generation: 1, PrevLogIndex: 1, PrevLogTerm: 3}},
		{name: "append_entry_gap", rpc: AppendEntriesRequest{LeaderID: 1, Term: 3, Generation: 1, Entries: []Entry{mustEntry(t, 2, 3, EntryNoOp, nil)}}},
		{name: "append_entry_term_above_leader", rpc: AppendEntriesRequest{LeaderID: 1, Term: 2, Generation: 1, Entries: []Entry{mustEntry(t, 1, 3, EntryNoOp, nil)}}},
		{name: "append_entry_term_below_previous", rpc: AppendEntriesRequest{LeaderID: 1, Term: 3, Generation: 1, PrevLogIndex: 1, PrevLogTerm: 2, Entries: []Entry{mustEntry(t, 2, 1, EntryNoOp, nil)}}},
		{name: "append_entry_terms_decrease", rpc: AppendEntriesRequest{LeaderID: 1, Term: 3, Generation: 1, Entries: []Entry{mustEntry(t, 1, 3, EntryNoOp, nil), mustEntry(t, 2, 2, EntryNoOp, nil)}}},
		{name: "append_entry_count_cap", rpc: AppendEntriesRequest{LeaderID: 1, Term: 3, Generation: 1, Entries: []Entry{mustEntry(t, 1, 3, EntryNoOp, nil), mustEntry(t, 2, 3, EntryNoOp, nil)}}, limits: CodecLimits{MaxAppendEntries: 1}},
		{name: "append_command_cap", rpc: AppendEntriesRequest{LeaderID: 1, Term: 3, Generation: 1, Entries: []Entry{mustEntry(t, 1, 3, EntryCommand, oversizedCommand)}}, limits: CodecLimits{MaxCommandBytes: 4}},
		{name: "append_encoded_byte_cap", rpc: AppendEntriesRequest{LeaderID: 1, Term: 3, Generation: 1}, limits: CodecLimits{MaxEncodedBytes: 10}},
		{name: "append_success_with_conflict", rpc: AppendEntriesResponse{ResponderID: 2, LeaderID: 1, Term: 3, RequestTerm: 3, Generation: 1, Success: true, ConflictIndex: 1}},
		{name: "append_failure_without_hint", rpc: AppendEntriesResponse{ResponderID: 2, LeaderID: 1, Term: 3, RequestTerm: 3, Generation: 1}},
		{name: "append_higher_term_success", rpc: AppendEntriesResponse{ResponderID: 2, LeaderID: 1, Term: 4, RequestTerm: 3, Generation: 1, Success: true}},
		{name: "snapshot_zero_transfer", rpc: InstallSnapshotRequest{LeaderID: 1, Term: 3, SnapshotID: snapshotID, LastIncludedIndex: 1, LastIncludedTerm: 1, StateMachineSchemaVersion: 1, Checksum: checksum, Done: true}},
		{name: "snapshot_zero_identity", rpc: InstallSnapshotRequest{LeaderID: 1, Term: 3, TransferID: transferID, LastIncludedIndex: 1, LastIncludedTerm: 1, StateMachineSchemaVersion: 1, Checksum: checksum, Done: true}},
		{name: "snapshot_offset_gap", rpc: InstallSnapshotRequest{LeaderID: 1, Term: 3, TransferID: transferID, SnapshotID: snapshotID, LastIncludedIndex: 1, LastIncludedTerm: 1, StateMachineSchemaVersion: 1, TotalLength: 3, Checksum: checksum, Offset: 2, Chunk: []byte("ab"), Done: true}},
		{name: "snapshot_done_before_end", rpc: InstallSnapshotRequest{LeaderID: 1, Term: 3, TransferID: transferID, SnapshotID: snapshotID, LastIncludedIndex: 1, LastIncludedTerm: 1, StateMachineSchemaVersion: 1, TotalLength: 3, Checksum: checksum, Chunk: []byte("ab"), Done: true}},
		{name: "snapshot_included_term_above_leader", rpc: InstallSnapshotRequest{LeaderID: 1, Term: 2, TransferID: transferID, SnapshotID: snapshotID, LastIncludedIndex: 1, LastIncludedTerm: 3, StateMachineSchemaVersion: 1, Checksum: checksum, Done: true}},
		{name: "snapshot_chunk_cap", rpc: InstallSnapshotRequest{LeaderID: 1, Term: 3, TransferID: transferID, SnapshotID: snapshotID, LastIncludedIndex: 1, LastIncludedTerm: 1, StateMachineSchemaVersion: 1, TotalLength: 5, Checksum: checksum, Chunk: oversizedChunk, Done: true}, limits: CodecLimits{MaxSnapshotChunkBytes: 4}},
		{name: "snapshot_response_missing_request_term", rpc: InstallSnapshotResponse{ResponderID: 2, LeaderID: 1, Term: 3, TransferID: transferID, SnapshotID: snapshotID}},
		{name: "snapshot_response_done_rejection", rpc: InstallSnapshotResponse{ResponderID: 2, LeaderID: 1, Term: 3, RequestTerm: 3, TransferID: transferID, SnapshotID: snapshotID, Done: true}},
		{name: "unknown_protocol_error", rpc: ErrorResponse{Code: 99, ResponderID: 2, Term: 3}},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := EncodeRPC(test.rpc, test.limits); !errors.Is(err, ErrInvalidRPC) && !errors.Is(err, ErrRPCTooLarge) {
				t.Fatalf("EncodeRPC error = %v, want ErrInvalidRPC or ErrRPCTooLarge", err)
			}
		})
	}

	if err := ValidateRPCSender(task5Handshake(2, fingerprint), 1, validSet); !errors.Is(err, ErrInvalidRPC) {
		t.Fatalf("mismatched sender error = %v, want ErrInvalidRPC", err)
	}
	wrongFingerprint := fingerprint
	wrongFingerprint[0] ^= 0xff
	if err := ValidateRPCSender(task5Handshake(1, wrongFingerprint), 1, validSet); !errors.Is(err, ErrVoterFingerprint) {
		t.Fatalf("fingerprint error = %v, want ErrVoterFingerprint", err)
	}
	if err := ValidateRPCSender(RequestVoteRequest{CandidateID: 1, Term: 1}, 9, validSet); !errors.Is(err, ErrNotVoter) {
		t.Fatalf("nonvoter sender error = %v, want ErrNotVoter", err)
	}
}

func TestMessageDecodeRejectsTrailingBytesAndUnknownType(t *testing.T) {
	message, encoded, err := EncodeRPC(RequestVoteRequest{CandidateID: 1, Term: 1}, DefaultCodecLimits())
	if err != nil {
		t.Fatalf("EncodeRPC: %v", err)
	}
	withTrailing := append(append([]byte(nil), encoded...), 0)
	if _, err := DecodeRPC(message, withTrailing, DefaultCodecLimits()); !errors.Is(err, ErrMalformedRPC) {
		t.Fatalf("trailing-byte error = %v, want ErrMalformedRPC", err)
	}
	if _, err := DecodeRPC(wire.MessageType(65535), []byte{0, 1}, DefaultCodecLimits()); !errors.Is(err, ErrUnknownRPC) {
		t.Fatalf("unknown-message error = %v, want ErrUnknownRPC", err)
	}
}

func TestMessageDecodedBytesAndClonesAreOwned(t *testing.T) {
	rpc := AppendEntriesRequest{LeaderID: 1, Term: 2, Generation: 3, Entries: []Entry{mustEntry(t, 1, 2, EntryCommand, []byte("command"))}}
	message, encoded, err := EncodeRPC(rpc, DefaultCodecLimits())
	if err != nil {
		t.Fatalf("EncodeRPC: %v", err)
	}
	decodedRPC, err := DecodeRPC(message, encoded, DefaultCodecLimits())
	if err != nil {
		t.Fatalf("DecodeRPC: %v", err)
	}
	decoded := decodedRPC.(AppendEntriesRequest)
	encoded[len(encoded)-1] ^= 0xff
	if string(decoded.Entries[0].CommandBytes()) != "command" {
		t.Fatalf("decoded command aliased encoded payload: %q", decoded.Entries[0].CommandBytes())
	}
	cloned := CloneRPC(decoded).(AppendEntriesRequest)
	cloned.Entries[0].command[0] = 'X'
	if string(decoded.Entries[0].CommandBytes()) != "command" {
		t.Fatalf("CloneRPC aliased entry command: %q", decoded.Entries[0].CommandBytes())
	}

	snapshot := InstallSnapshotRequest{LeaderID: 1, Term: 2, TransferID: TransferID{1}, SnapshotID: SnapshotID{1}, LastIncludedIndex: 1, LastIncludedTerm: 1, StateMachineSchemaVersion: 1, TotalLength: 3, Checksum: SnapshotChecksum{1}, Chunk: []byte("abc"), Done: true}
	clonedSnapshot := CloneRPC(snapshot).(InstallSnapshotRequest)
	clonedSnapshot.Chunk[0] = 'X'
	if string(snapshot.Chunk) != "abc" {
		t.Fatalf("CloneRPC aliased snapshot chunk: %q", snapshot.Chunk)
	}
}

func TestRPCMessageTypesUseStableWireIDsAndBinaryCodec(t *testing.T) {
	if RPCSchemaVersion != 1 {
		t.Fatalf("RPCSchemaVersion = %d, want 1", RPCSchemaVersion)
	}
	if HandshakeSchemaVersion != 2 {
		t.Fatalf("HandshakeSchemaVersion = %d, want 2", HandshakeSchemaVersion)
	}
	if wire.CodecBinary != 2 {
		t.Fatalf("CodecBinary = %d, want 2", wire.CodecBinary)
	}
}

func TestMessageStateMachineSchemaUsesUint32Domain(t *testing.T) {
	rpc := InstallSnapshotRequest{
		LeaderID:                  1,
		Term:                      2,
		TransferID:                TransferID{1},
		SnapshotID:                SnapshotID{1},
		LastIncludedIndex:         1,
		LastIncludedTerm:          1,
		StateMachineSchemaVersion: math.MaxUint32,
		Checksum:                  SnapshotChecksum{1},
		Done:                      true,
	}
	message, encoded, err := EncodeRPC(rpc, DefaultCodecLimits())
	if err != nil {
		t.Fatalf("EncodeRPC: %v", err)
	}
	decoded, err := DecodeRPC(message, encoded, DefaultCodecLimits())
	if err != nil {
		t.Fatalf("DecodeRPC: %v", err)
	}
	if got := decoded.(InstallSnapshotRequest).StateMachineSchemaVersion; got != math.MaxUint32 {
		t.Fatalf("StateMachineSchemaVersion = %d, want %d", got, uint64(math.MaxUint32))
	}
}

func mustEntry(t *testing.T, index, term uint64, kind EntryKind, command []byte) Entry {
	t.Helper()
	entry, err := NewEntry(index, term, kind, command)
	if err != nil {
		t.Fatalf("NewEntry: %v", err)
	}
	return entry
}
