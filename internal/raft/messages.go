package raft

import (
	"fmt"

	"github.com/aaditya/cs425mp3/internal/wire"
)

// RPCSchemaVersion is the initial canonical binary schema for non-handshake Raft payloads.
const RPCSchemaVersion uint16 = 1

// HandshakeSchemaVersion is the canonical application-fenced peer handshake schema.
const HandshakeSchemaVersion uint16 = 2

// SnapshotID identifies immutable snapshot content independently of transfer retries.
type SnapshotID [16]byte

// SnapshotChecksum is the SHA-256 checksum of complete snapshot state bytes.
type SnapshotChecksum [32]byte

// RPC is one concrete validated Raft wire payload.
type RPC interface {
	// MessageType returns the stable authenticated wire registry identifier.
	MessageType() wire.MessageType
}

// Handshake binds a new authenticated stream to one configured voter set and sender.
type Handshake struct {
	// SenderID is the configured voter opening the stream.
	SenderID uint16
	// VoterFingerprint is the canonical fixed-voter trust boundary.
	VoterFingerprint VoterFingerprint
	// ApplicationFingerprint binds the complete deterministic application protocol.
	ApplicationFingerprint [32]byte
}

// MessageType returns wire.MessageRaftHandshake.
func (Handshake) MessageType() wire.MessageType { return wire.MessageRaftHandshake }

// HandshakeAck acknowledges a voter stream binding.
type HandshakeAck struct {
	// ResponderID is the configured voter accepting the stream.
	ResponderID uint16
	// VoterFingerprint is the responder's canonical fixed-voter trust boundary.
	VoterFingerprint VoterFingerprint
	// ApplicationFingerprint binds the complete deterministic application protocol.
	ApplicationFingerprint [32]byte
}

// MessageType returns wire.MessageRaftHandshakeAck.
func (HandshakeAck) MessageType() wire.MessageType { return wire.MessageRaftHandshakeAck }

// PreVoteRequest asks whether a prospective election could succeed without changing durable term state.
type PreVoteRequest struct {
	// CandidateID is the configured prospective candidate.
	CandidateID uint16
	// CurrentTerm is the sender's current durable term.
	CurrentTerm uint64
	// ProspectiveTerm is exactly CurrentTerm plus one.
	ProspectiveTerm uint64
	// LastLogIndex is the candidate's final available log index, or zero for an empty log.
	LastLogIndex uint64
	// LastLogTerm is the term at LastLogIndex, or zero for an empty log.
	LastLogTerm uint64
}

// MessageType returns wire.MessageRaftPreVoteRequest.
func (PreVoteRequest) MessageType() wire.MessageType { return wire.MessageRaftPreVoteRequest }

// PreVoteResponse reports one configured voter's decision for an exact prospective election.
type PreVoteResponse struct {
	// ResponderID is the configured voter making the decision.
	ResponderID uint16
	// CandidateID correlates the response to the prospective candidate.
	CandidateID uint16
	// Term is the responder's current durable term.
	Term uint64
	// RequestCurrentTerm correlates the response to the candidate's current term.
	RequestCurrentTerm uint64
	// ProspectiveTerm correlates the response to the requested prospective term.
	ProspectiveTerm uint64
	// Granted reports whether the candidate's log could receive this vote.
	Granted bool
}

// MessageType returns wire.MessageRaftPreVoteResponse.
func (PreVoteResponse) MessageType() wire.MessageType { return wire.MessageRaftPreVoteResponse }

// RequestVoteRequest asks for a durable vote in one active election term.
type RequestVoteRequest struct {
	// CandidateID is the configured candidate requesting the vote.
	CandidateID uint16
	// Term is the nonzero active election term.
	Term uint64
	// LastLogIndex is the candidate's final available log index, or zero for an empty log.
	LastLogIndex uint64
	// LastLogTerm is the term at LastLogIndex, or zero for an empty log.
	LastLogTerm uint64
}

// MessageType returns wire.MessageRaftRequestVoteRequest.
func (RequestVoteRequest) MessageType() wire.MessageType { return wire.MessageRaftRequestVoteRequest }

// RequestVoteResponse reports one configured voter's decision for an exact election request.
type RequestVoteResponse struct {
	// ResponderID is the configured voter making the decision.
	ResponderID uint16
	// CandidateID correlates the response to the requesting candidate.
	CandidateID uint16
	// Term is the responder's current term and is never below RequestTerm.
	Term uint64
	// RequestTerm is the exact election term from the request.
	RequestTerm uint64
	// Granted reports whether the durable vote was granted.
	Granted bool
}

// MessageType returns wire.MessageRaftRequestVoteResponse.
func (RequestVoteResponse) MessageType() wire.MessageType { return wire.MessageRaftRequestVoteResponse }

// AppendEntriesRequest replicates a bounded contiguous log batch or carries an empty heartbeat.
type AppendEntriesRequest struct {
	// LeaderID is the configured leader sending the request.
	LeaderID uint16
	// Term is the leader's nonzero current term.
	Term uint64
	// Generation identifies this leader-local replication attempt.
	Generation RequestGeneration
	// PrevLogIndex is the index immediately preceding Entries, or zero at the log base.
	PrevLogIndex uint64
	// PrevLogTerm is the term at PrevLogIndex, or zero at the log base.
	PrevLogTerm uint64
	// LeaderCommit is the greatest index the leader knows committed.
	LeaderCommit uint64
	// Entries is an owned contiguous ascending log batch.
	Entries []Entry
}

// MessageType returns wire.MessageRaftAppendEntriesRequest.
func (AppendEntriesRequest) MessageType() wire.MessageType {
	return wire.MessageRaftAppendEntriesRequest
}

// AppendEntriesResponse reports the result of one exact replication generation.
type AppendEntriesResponse struct {
	// ResponderID is the configured follower sending the response.
	ResponderID uint16
	// LeaderID correlates the response to the requesting leader.
	LeaderID uint16
	// Term is the responder's current term and is never below RequestTerm.
	Term uint64
	// RequestTerm is the exact leader term from the request.
	RequestTerm uint64
	// Generation correlates the response to one leader-local replication attempt.
	Generation RequestGeneration
	// Success reports whether the preceding entry matched and the batch was accepted.
	Success bool
	// MatchIndex is the greatest verified index on success and zero on rejection.
	MatchIndex uint64
	// ConflictTerm is the rejected conflicting term, or zero when the follower lacks the entry.
	ConflictTerm uint64
	// ConflictIndex is the first useful repair index on rejection and zero on success.
	ConflictIndex uint64
}

// MessageType returns wire.MessageRaftAppendEntriesResponse.
func (AppendEntriesResponse) MessageType() wire.MessageType {
	return wire.MessageRaftAppendEntriesResponse
}

// InstallSnapshotRequest carries one bounded, exactly positioned snapshot chunk.
type InstallSnapshotRequest struct {
	// LeaderID is the configured leader sending the chunk.
	LeaderID uint16
	// Term is the leader's nonzero current term.
	Term uint64
	// TransferID identifies one snapshot transfer attempt.
	TransferID TransferID
	// SnapshotID identifies the immutable snapshot content.
	SnapshotID SnapshotID
	// LastIncludedIndex is the final log index represented by the snapshot.
	LastIncludedIndex uint64
	// LastIncludedTerm is the term at LastIncludedIndex.
	LastIncludedTerm uint64
	// StateMachineSchemaVersion identifies the application snapshot encoding.
	StateMachineSchemaVersion uint32
	// TotalLength is the complete bounded snapshot state length.
	TotalLength uint64
	// Checksum authenticates the complete snapshot state after transfer.
	Checksum SnapshotChecksum
	// Offset is the exact zero-based position of Chunk.
	Offset uint64
	// Chunk is an owned bounded slice of snapshot state bytes.
	Chunk []byte
	// Done is true exactly when this chunk reaches TotalLength.
	Done bool
}

// MessageType returns wire.MessageRaftInstallSnapshotRequest.
func (InstallSnapshotRequest) MessageType() wire.MessageType {
	return wire.MessageRaftInstallSnapshotRequest
}

// InstallSnapshotResponse reports durable progress for one exact snapshot transfer.
type InstallSnapshotResponse struct {
	// ResponderID is the configured follower sending the response.
	ResponderID uint16
	// LeaderID correlates the response to the requesting leader.
	LeaderID uint16
	// Term is the responder's current term and is never below RequestTerm.
	Term uint64
	// RequestTerm is the exact leader term from the request.
	RequestTerm uint64
	// TransferID correlates the response to one transfer attempt.
	TransferID TransferID
	// SnapshotID correlates the response to immutable snapshot content.
	SnapshotID SnapshotID
	// NextOffset is the next byte position the follower requires.
	NextOffset uint64
	// Success reports whether the exact chunk was accepted or already durable.
	Success bool
	// Done reports that the complete snapshot was durably installed.
	Done bool
}

// MessageType returns wire.MessageRaftInstallSnapshotResponse.
func (InstallSnapshotResponse) MessageType() wire.MessageType {
	return wire.MessageRaftInstallSnapshotResponse
}

// ProtocolErrorCode is a closed authentication-safe protocol failure enumeration.
type ProtocolErrorCode uint16

const (
	// ProtocolErrorMalformed reports a structurally malformed canonical payload.
	ProtocolErrorMalformed ProtocolErrorCode = iota + 1
	// ProtocolErrorUnsupportedSchema reports an unknown payload schema version.
	ProtocolErrorUnsupportedSchema
	// ProtocolErrorUnauthorizedVoter reports a sender outside the fixed voter set.
	ProtocolErrorUnauthorizedVoter
	// ProtocolErrorFingerprintMismatch reports another fixed voter configuration.
	ProtocolErrorFingerprintMismatch
	// ProtocolErrorUnexpectedMessage reports an RPC invalid for the stream state.
	ProtocolErrorUnexpectedMessage
	// ProtocolErrorOverloaded reports a bounded peer queue refusal.
	ProtocolErrorOverloaded
)

// ErrorResponse carries only a closed error code and safe responder metadata.
type ErrorResponse struct {
	// Code is a closed protocol error classification.
	Code ProtocolErrorCode
	// ResponderID is the configured voter reporting the error.
	ResponderID uint16
	// Term is the responder's current term, which may be zero before initialization.
	Term uint64
}

// MessageType returns wire.MessageRaftError.
func (ErrorResponse) MessageType() wire.MessageType { return wire.MessageRaftError }

// ValidateRPCSender binds decoded payload identity to an authenticated configured frame sender.
func ValidateRPCSender(rpc RPC, senderID uint16, voters VoterSet) error {
	if !voters.Contains(senderID) {
		return fmt.Errorf("%w: sender ID %d", ErrNotVoter, senderID)
	}
	rpc = normalizeRPC(rpc)
	if rpc == nil {
		return fmt.Errorf("%w: nil payload", ErrInvalidRPC)
	}
	var actorID uint16
	var correlatedID uint16
	switch message := rpc.(type) {
	case Handshake:
		actorID = message.SenderID
		if message.VoterFingerprint != voters.Fingerprint() {
			return ErrVoterFingerprint
		}
	case HandshakeAck:
		actorID = message.ResponderID
		if message.VoterFingerprint != voters.Fingerprint() {
			return ErrVoterFingerprint
		}
	case PreVoteRequest:
		actorID = message.CandidateID
	case PreVoteResponse:
		actorID, correlatedID = message.ResponderID, message.CandidateID
	case RequestVoteRequest:
		actorID = message.CandidateID
	case RequestVoteResponse:
		actorID, correlatedID = message.ResponderID, message.CandidateID
	case AppendEntriesRequest:
		actorID = message.LeaderID
	case AppendEntriesResponse:
		actorID, correlatedID = message.ResponderID, message.LeaderID
	case InstallSnapshotRequest:
		actorID = message.LeaderID
	case InstallSnapshotResponse:
		actorID, correlatedID = message.ResponderID, message.LeaderID
	case ErrorResponse:
		actorID = message.ResponderID
	default:
		return fmt.Errorf("%w: %T", ErrUnknownRPC, rpc)
	}
	if actorID != senderID {
		return fmt.Errorf("%w: payload actor %d does not match authenticated sender %d", ErrInvalidRPC, actorID, senderID)
	}
	if correlatedID != 0 && !voters.Contains(correlatedID) {
		return fmt.Errorf("%w: correlated voter ID %d", ErrNotVoter, correlatedID)
	}
	return nil
}

// CloneRPC returns an independently owned copy of rpc and all nested byte slices.
func CloneRPC(rpc RPC) RPC {
	rpc = normalizeRPC(rpc)
	switch message := rpc.(type) {
	case AppendEntriesRequest:
		message.Entries = cloneEntries(message.Entries)
		return message
	case InstallSnapshotRequest:
		message.Chunk = cloneBytes(message.Chunk)
		return message
	default:
		return message
	}
}

func normalizeRPC(rpc RPC) RPC {
	switch message := rpc.(type) {
	case *Handshake:
		if message != nil {
			return *message
		}
	case *HandshakeAck:
		if message != nil {
			return *message
		}
	case *PreVoteRequest:
		if message != nil {
			return *message
		}
	case *PreVoteResponse:
		if message != nil {
			return *message
		}
	case *RequestVoteRequest:
		if message != nil {
			return *message
		}
	case *RequestVoteResponse:
		if message != nil {
			return *message
		}
	case *AppendEntriesRequest:
		if message != nil {
			return *message
		}
	case *AppendEntriesResponse:
		if message != nil {
			return *message
		}
	case *InstallSnapshotRequest:
		if message != nil {
			return *message
		}
	case *InstallSnapshotResponse:
		if message != nil {
			return *message
		}
	case *ErrorResponse:
		if message != nil {
			return *message
		}
	default:
		return rpc
	}
	return nil
}

func cloneEntries(entries []Entry) []Entry {
	if entries == nil {
		return nil
	}
	owned := make([]Entry, len(entries))
	for index := range entries {
		owned[index] = entries[index].Clone()
	}
	return owned
}
