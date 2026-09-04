package raft

import (
	"encoding/binary"
	"fmt"
	"math"

	"crane/internal/config"
	"crane/internal/wire"
)

const (
	// MaxRPCPayloadBytes is the largest payload that fits the default authenticated 8 MiB frame.
	MaxRPCPayloadBytes = (8 << 20) - wire.FixedHeaderSize - wire.MACSize
	minimumEntryBytes  = 8 + 8 + 1 + 4
)

// CodecLimits bounds every peer-controlled collection before allocation.
type CodecLimits struct {
	// MaxAppendEntries bounds the entry count in one AppendEntries request.
	MaxAppendEntries uint16
	// MaxCommandBytes bounds one opaque application command.
	MaxCommandBytes uint64
	// MaxSnapshotChunkBytes bounds one InstallSnapshot chunk.
	MaxSnapshotChunkBytes uint64
	// MaxSnapshotBytes bounds the complete snapshot length and response offsets.
	MaxSnapshotBytes uint64
	// MaxEncodedBytes bounds one complete canonical Raft payload.
	MaxEncodedBytes uint64
}

// DefaultCodecLimits returns the documented v1 network and allocation bounds.
func DefaultCodecLimits() CodecLimits {
	return CodecLimits{
		MaxAppendEntries:      config.DefaultRaftConfig().MaxAppendEntries,
		MaxCommandBytes:       config.MaxRaftCommandBytes,
		MaxSnapshotChunkBytes: config.MaxRaftSnapshotChunkBytes,
		MaxSnapshotBytes:      config.MaxRaftSnapshotBytes,
		MaxEncodedBytes:       MaxRPCPayloadBytes,
	}
}

// EncodeRPC validates rpc and returns its stable wire type plus canonical
// big-endian payload. Consensus RPCs carry the v1 schema; Handshake messages
// carry HandshakeSchemaVersion.
func EncodeRPC(rpc RPC, limits CodecLimits) (wire.MessageType, []byte, error) {
	resolved, err := resolveCodecLimits(limits)
	if err != nil {
		return 0, nil, err
	}
	rpc = normalizeRPC(rpc)
	if rpc == nil {
		return 0, nil, fmt.Errorf("%w: nil payload", ErrInvalidRPC)
	}
	if err := validateRPC(rpc, resolved); err != nil {
		return 0, nil, err
	}

	encoder := payloadEncoder{maximum: resolved.MaxEncodedBytes}
	if err := encoder.uint16(schemaVersionForRPC(rpc)); err != nil {
		return 0, nil, err
	}
	switch message := rpc.(type) {
	case Handshake:
		err = encodeHandshake(&encoder, message)
	case HandshakeAck:
		err = encodeHandshakeAck(&encoder, message)
	case PreVoteRequest:
		err = encodePreVoteRequest(&encoder, message)
	case PreVoteResponse:
		err = encodePreVoteResponse(&encoder, message)
	case RequestVoteRequest:
		err = encodeRequestVoteRequest(&encoder, message)
	case RequestVoteResponse:
		err = encodeRequestVoteResponse(&encoder, message)
	case AppendEntriesRequest:
		err = encodeAppendEntriesRequest(&encoder, message)
	case AppendEntriesResponse:
		err = encodeAppendEntriesResponse(&encoder, message)
	case InstallSnapshotRequest:
		err = encodeInstallSnapshotRequest(&encoder, message)
	case InstallSnapshotResponse:
		err = encodeInstallSnapshotResponse(&encoder, message)
	case ErrorResponse:
		err = encodeErrorResponse(&encoder, message)
	default:
		return 0, nil, fmt.Errorf("%w: %T", ErrUnknownRPC, rpc)
	}
	if err != nil {
		return 0, nil, err
	}
	return rpc.MessageType(), encoder.bytes(), nil
}

// DecodeRPC parses exactly one bounded canonical payload and rejects trailing bytes.
func DecodeRPC(messageType wire.MessageType, payload []byte, limits CodecLimits) (RPC, error) {
	resolved, err := resolveCodecLimits(limits)
	if err != nil {
		return nil, err
	}
	if uint64(len(payload)) > resolved.MaxEncodedBytes {
		return nil, fmt.Errorf("%w: payload is %d bytes, maximum is %d", ErrRPCTooLarge, len(payload), resolved.MaxEncodedBytes)
	}
	decoder := payloadDecoder{payload: payload}
	version, err := decoder.uint16()
	if err != nil {
		return nil, err
	}
	if version != schemaVersionForMessage(messageType) {
		return nil, fmt.Errorf("%w: version %d", ErrUnsupportedSchema, version)
	}

	var rpc RPC
	switch messageType {
	case wire.MessageRaftHandshake:
		rpc, err = decodeHandshake(&decoder)
	case wire.MessageRaftHandshakeAck:
		rpc, err = decodeHandshakeAck(&decoder)
	case wire.MessageRaftPreVoteRequest:
		rpc, err = decodePreVoteRequest(&decoder)
	case wire.MessageRaftPreVoteResponse:
		rpc, err = decodePreVoteResponse(&decoder)
	case wire.MessageRaftRequestVoteRequest:
		rpc, err = decodeRequestVoteRequest(&decoder)
	case wire.MessageRaftRequestVoteResponse:
		rpc, err = decodeRequestVoteResponse(&decoder)
	case wire.MessageRaftAppendEntriesRequest:
		rpc, err = decodeAppendEntriesRequest(&decoder, resolved)
	case wire.MessageRaftAppendEntriesResponse:
		rpc, err = decodeAppendEntriesResponse(&decoder)
	case wire.MessageRaftInstallSnapshotRequest:
		rpc, err = decodeInstallSnapshotRequest(&decoder, resolved)
	case wire.MessageRaftInstallSnapshotResponse:
		rpc, err = decodeInstallSnapshotResponse(&decoder)
	case wire.MessageRaftError:
		rpc, err = decodeErrorResponse(&decoder)
	default:
		return nil, fmt.Errorf("%w: message type %d", ErrUnknownRPC, messageType)
	}
	if err != nil {
		return nil, err
	}
	if decoder.remaining() != 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes", ErrMalformedRPC, decoder.remaining())
	}
	if err := validateRPC(rpc, resolved); err != nil {
		return nil, err
	}
	return rpc, nil
}

func resolveCodecLimits(limits CodecLimits) (CodecLimits, error) {
	defaults := DefaultCodecLimits()
	if limits.MaxAppendEntries == 0 {
		limits.MaxAppendEntries = defaults.MaxAppendEntries
	}
	if limits.MaxCommandBytes == 0 {
		limits.MaxCommandBytes = defaults.MaxCommandBytes
	}
	if limits.MaxSnapshotChunkBytes == 0 {
		limits.MaxSnapshotChunkBytes = defaults.MaxSnapshotChunkBytes
	}
	if limits.MaxSnapshotBytes == 0 {
		limits.MaxSnapshotBytes = defaults.MaxSnapshotBytes
	}
	if limits.MaxEncodedBytes == 0 {
		limits.MaxEncodedBytes = defaults.MaxEncodedBytes
	}
	if limits.MaxCommandBytes > defaults.MaxCommandBytes {
		return CodecLimits{}, fmt.Errorf("%w: command limit %d exceeds absolute maximum %d", ErrRPCTooLarge, limits.MaxCommandBytes, defaults.MaxCommandBytes)
	}
	if limits.MaxSnapshotChunkBytes > defaults.MaxSnapshotChunkBytes {
		return CodecLimits{}, fmt.Errorf("%w: snapshot chunk limit %d exceeds absolute maximum %d", ErrRPCTooLarge, limits.MaxSnapshotChunkBytes, defaults.MaxSnapshotChunkBytes)
	}
	if limits.MaxSnapshotBytes > defaults.MaxSnapshotBytes {
		return CodecLimits{}, fmt.Errorf("%w: snapshot limit %d exceeds absolute maximum %d", ErrRPCTooLarge, limits.MaxSnapshotBytes, defaults.MaxSnapshotBytes)
	}
	if limits.MaxEncodedBytes > defaults.MaxEncodedBytes {
		return CodecLimits{}, fmt.Errorf("%w: encoded limit %d exceeds absolute maximum %d", ErrRPCTooLarge, limits.MaxEncodedBytes, defaults.MaxEncodedBytes)
	}
	if limits.MaxCommandBytes > math.MaxUint32 || limits.MaxSnapshotChunkBytes > math.MaxUint32 {
		return CodecLimits{}, fmt.Errorf("%w: byte-string limit exceeds uint32 length domain", ErrRPCTooLarge)
	}
	if limits.MaxEncodedBytes > math.MaxInt {
		return CodecLimits{}, fmt.Errorf("%w: encoded limit exceeds local integer domain", ErrRPCTooLarge)
	}
	return limits, nil
}

func validateRPC(rpc RPC, limits CodecLimits) error {
	switch message := rpc.(type) {
	case Handshake:
		if message.SenderID == 0 || message.VoterFingerprint == (VoterFingerprint{}) || message.ApplicationFingerprint == ([32]byte{}) {
			return fmt.Errorf("%w: handshake requires sender, voter fingerprint, and application fingerprint", ErrInvalidRPC)
		}
	case HandshakeAck:
		if message.ResponderID == 0 || message.VoterFingerprint == (VoterFingerprint{}) || message.ApplicationFingerprint == ([32]byte{}) {
			return fmt.Errorf("%w: handshake acknowledgement requires responder, voter fingerprint, and application fingerprint", ErrInvalidRPC)
		}
	case PreVoteRequest:
		if message.CandidateID == 0 || !isNextTerm(message.CurrentTerm, message.ProspectiveTerm) || !validLogPosition(message.LastLogIndex, message.LastLogTerm) || message.LastLogTerm > message.CurrentTerm {
			return fmt.Errorf("%w: invalid pre-vote request", ErrInvalidRPC)
		}
	case PreVoteResponse:
		if !validDistinctIDs(message.ResponderID, message.CandidateID) || !isNextTerm(message.RequestCurrentTerm, message.ProspectiveTerm) || (message.Term > message.ProspectiveTerm && message.Granted) {
			return fmt.Errorf("%w: invalid pre-vote response", ErrInvalidRPC)
		}
	case RequestVoteRequest:
		if message.CandidateID == 0 || message.Term == 0 || !validLogPosition(message.LastLogIndex, message.LastLogTerm) || message.LastLogTerm > message.Term {
			return fmt.Errorf("%w: invalid vote request", ErrInvalidRPC)
		}
	case RequestVoteResponse:
		if !validDistinctIDs(message.ResponderID, message.CandidateID) || message.RequestTerm == 0 || message.Term < message.RequestTerm || (message.Term > message.RequestTerm && message.Granted) {
			return fmt.Errorf("%w: invalid vote response", ErrInvalidRPC)
		}
	case AppendEntriesRequest:
		if err := validateAppendRequest(message, limits); err != nil {
			return err
		}
	case AppendEntriesResponse:
		if err := validateAppendResponse(message); err != nil {
			return err
		}
	case InstallSnapshotRequest:
		if err := validateSnapshotRequest(message, limits); err != nil {
			return err
		}
	case InstallSnapshotResponse:
		if err := validateSnapshotResponse(message, limits); err != nil {
			return err
		}
	case ErrorResponse:
		if message.ResponderID == 0 || message.Code < ProtocolErrorMalformed || message.Code > ProtocolErrorOverloaded {
			return fmt.Errorf("%w: invalid protocol error metadata", ErrInvalidRPC)
		}
	default:
		return fmt.Errorf("%w: %T", ErrUnknownRPC, rpc)
	}
	return nil
}

func validateAppendRequest(message AppendEntriesRequest, limits CodecLimits) error {
	if message.LeaderID == 0 || message.Term == 0 || message.Generation == 0 || !validLogPosition(message.PrevLogIndex, message.PrevLogTerm) || message.PrevLogTerm > message.Term {
		return fmt.Errorf("%w: invalid append request metadata", ErrInvalidRPC)
	}
	if len(message.Entries) > int(limits.MaxAppendEntries) {
		return fmt.Errorf("%w: append count %d exceeds %d", ErrRPCTooLarge, len(message.Entries), limits.MaxAppendEntries)
	}
	expected := message.PrevLogIndex
	previousTerm := message.PrevLogTerm
	for index, entry := range message.Entries {
		if expected == math.MaxUint64 {
			return fmt.Errorf("%w: append index overflow", ErrInvalidRPC)
		}
		expected++
		if entry.Index != expected || entry.Term == 0 || entry.Term < previousTerm || entry.Term > message.Term || (entry.Kind != EntryCommand && entry.Kind != EntryNoOp) {
			return fmt.Errorf("%w: invalid entry %d ordering or metadata", ErrInvalidRPC, index)
		}
		previousTerm = entry.Term
		commandLength := uint64(len(entry.command))
		if commandLength > limits.MaxCommandBytes {
			return fmt.Errorf("%w: command is %d bytes, maximum is %d", ErrRPCTooLarge, commandLength, limits.MaxCommandBytes)
		}
		if entry.Kind == EntryNoOp && commandLength != 0 {
			return fmt.Errorf("%w: no-op entry carries command bytes", ErrInvalidRPC)
		}
	}
	return nil
}

func validateAppendResponse(message AppendEntriesResponse) error {
	if !validDistinctIDs(message.ResponderID, message.LeaderID) || message.RequestTerm == 0 || message.Term < message.RequestTerm || message.Generation == 0 {
		return fmt.Errorf("%w: invalid append response correlation", ErrInvalidRPC)
	}
	if message.Success {
		if message.Term > message.RequestTerm {
			return fmt.Errorf("%w: higher-term append response cannot succeed", ErrInvalidRPC)
		}
		if message.ConflictTerm != 0 || message.ConflictIndex != 0 {
			return fmt.Errorf("%w: successful append carries conflict hint", ErrInvalidRPC)
		}
	} else if message.MatchIndex != 0 || message.ConflictIndex == 0 {
		return fmt.Errorf("%w: rejected append requires a conflict index and zero match index", ErrInvalidRPC)
	}
	return nil
}

func validateSnapshotRequest(message InstallSnapshotRequest, limits CodecLimits) error {
	if message.LeaderID == 0 || message.Term == 0 || message.TransferID.IsZero() || message.SnapshotID == (SnapshotID{}) || message.Checksum == (SnapshotChecksum{}) {
		return fmt.Errorf("%w: invalid snapshot identity", ErrInvalidRPC)
	}
	if message.LastIncludedIndex == 0 || message.LastIncludedTerm == 0 || message.LastIncludedTerm > message.Term || message.StateMachineSchemaVersion == 0 {
		return fmt.Errorf("%w: invalid snapshot metadata", ErrInvalidRPC)
	}
	if message.TotalLength > limits.MaxSnapshotBytes {
		return fmt.Errorf("%w: snapshot total is %d bytes, maximum is %d", ErrRPCTooLarge, message.TotalLength, limits.MaxSnapshotBytes)
	}
	chunkLength := uint64(len(message.Chunk))
	if chunkLength > limits.MaxSnapshotChunkBytes {
		return fmt.Errorf("%w: snapshot chunk is %d bytes, maximum is %d", ErrRPCTooLarge, chunkLength, limits.MaxSnapshotChunkBytes)
	}
	if message.Offset > message.TotalLength || chunkLength > message.TotalLength-message.Offset {
		return fmt.Errorf("%w: snapshot offset plus chunk exceeds total", ErrInvalidRPC)
	}
	end := message.Offset + chunkLength
	if message.Done != (end == message.TotalLength) {
		return fmt.Errorf("%w: snapshot Done does not match final offset", ErrInvalidRPC)
	}
	if message.TotalLength != 0 && chunkLength == 0 {
		return fmt.Errorf("%w: nonempty snapshot carries an empty chunk", ErrInvalidRPC)
	}
	return nil
}

func validateSnapshotResponse(message InstallSnapshotResponse, limits CodecLimits) error {
	if !validDistinctIDs(message.ResponderID, message.LeaderID) || message.RequestTerm == 0 || message.Term < message.RequestTerm || message.TransferID.IsZero() || message.SnapshotID == (SnapshotID{}) {
		return fmt.Errorf("%w: invalid snapshot response correlation", ErrInvalidRPC)
	}
	if message.NextOffset > limits.MaxSnapshotBytes {
		return fmt.Errorf("%w: next snapshot offset exceeds maximum", ErrRPCTooLarge)
	}
	if message.Done && !message.Success {
		return fmt.Errorf("%w: rejected snapshot response cannot be done", ErrInvalidRPC)
	}
	return nil
}

func validDistinctIDs(left, right uint16) bool {
	return left != 0 && right != 0 && left != right
}

func validLogPosition(index, term uint64) bool {
	return (index == 0) == (term == 0)
}

func isNextTerm(current, prospective uint64) bool {
	return current != math.MaxUint64 && prospective == current+1
}

type payloadEncoder struct {
	payload []byte
	maximum uint64
}

func (e *payloadEncoder) bytes() []byte { return e.payload }

func (e *payloadEncoder) add(value []byte) error {
	if uint64(len(value)) > e.maximum-uint64(len(e.payload)) {
		return fmt.Errorf("%w: encoded payload exceeds %d bytes", ErrRPCTooLarge, e.maximum)
	}
	e.payload = append(e.payload, value...)
	return nil
}

func (e *payloadEncoder) uint8(value uint8) error { return e.add([]byte{value}) }

func (e *payloadEncoder) uint16(value uint16) error {
	var bytes [2]byte
	binary.BigEndian.PutUint16(bytes[:], value)
	return e.add(bytes[:])
}

func (e *payloadEncoder) uint32(value uint32) error {
	var bytes [4]byte
	binary.BigEndian.PutUint32(bytes[:], value)
	return e.add(bytes[:])
}

func (e *payloadEncoder) uint64(value uint64) error {
	var bytes [8]byte
	binary.BigEndian.PutUint64(bytes[:], value)
	return e.add(bytes[:])
}

func (e *payloadEncoder) boolean(value bool) error {
	if value {
		return e.uint8(1)
	}
	return e.uint8(0)
}

func (e *payloadEncoder) bytes32(value []byte) error {
	if len(value) > math.MaxUint32 {
		return fmt.Errorf("%w: byte string exceeds uint32 length domain", ErrRPCTooLarge)
	}
	if err := e.uint32(uint32(len(value))); err != nil {
		return err
	}
	return e.add(value)
}

type payloadDecoder struct {
	payload []byte
	offset  int
}

func (d *payloadDecoder) remaining() int { return len(d.payload) - d.offset }

func (d *payloadDecoder) take(length int) ([]byte, error) {
	if length < 0 || length > d.remaining() {
		return nil, fmt.Errorf("%w: need %d bytes with %d remaining", ErrMalformedRPC, length, d.remaining())
	}
	value := d.payload[d.offset : d.offset+length]
	d.offset += length
	return value, nil
}

func (d *payloadDecoder) uint8() (uint8, error) {
	bytes, err := d.take(1)
	if err != nil {
		return 0, err
	}
	return bytes[0], nil
}

func (d *payloadDecoder) uint16() (uint16, error) {
	bytes, err := d.take(2)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(bytes), nil
}

func (d *payloadDecoder) uint32() (uint32, error) {
	bytes, err := d.take(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(bytes), nil
}

func (d *payloadDecoder) uint64() (uint64, error) {
	bytes, err := d.take(8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(bytes), nil
}

func (d *payloadDecoder) boolean() (bool, error) {
	value, err := d.uint8()
	if err != nil {
		return false, err
	}
	if value > 1 {
		return false, fmt.Errorf("%w: boolean byte is %d", ErrMalformedRPC, value)
	}
	return value == 1, nil
}

func (d *payloadDecoder) fixed(destination []byte) error {
	bytes, err := d.take(len(destination))
	if err != nil {
		return err
	}
	copy(destination, bytes)
	return nil
}

func (d *payloadDecoder) bytes32Borrowed(maximum uint64) ([]byte, error) {
	length, err := d.uint32()
	if err != nil {
		return nil, err
	}
	if uint64(length) > maximum {
		return nil, fmt.Errorf("%w: declared byte string is %d bytes, maximum is %d", ErrRPCTooLarge, length, maximum)
	}
	if uint64(length) > uint64(d.remaining()) {
		return nil, fmt.Errorf("%w: declared byte string is %d bytes with %d remaining", ErrMalformedRPC, length, d.remaining())
	}
	if length == 0 {
		return nil, nil
	}
	return d.take(int(length))
}

func encodeHandshake(e *payloadEncoder, m Handshake) error {
	if err := e.uint16(m.SenderID); err != nil {
		return err
	}
	if err := e.add(m.VoterFingerprint[:]); err != nil {
		return err
	}
	return e.add(m.ApplicationFingerprint[:])
}

func encodeHandshakeAck(e *payloadEncoder, m HandshakeAck) error {
	if err := e.uint16(m.ResponderID); err != nil {
		return err
	}
	if err := e.add(m.VoterFingerprint[:]); err != nil {
		return err
	}
	return e.add(m.ApplicationFingerprint[:])
}

func encodePreVoteRequest(e *payloadEncoder, m PreVoteRequest) error {
	if err := e.uint16(m.CandidateID); err != nil {
		return err
	}
	return encodeUint64s(e, m.CurrentTerm, m.ProspectiveTerm, m.LastLogIndex, m.LastLogTerm)
}

func encodePreVoteResponse(e *payloadEncoder, m PreVoteResponse) error {
	if err := encodeUint16s(e, m.ResponderID, m.CandidateID); err != nil {
		return err
	}
	if err := encodeUint64s(e, m.Term, m.RequestCurrentTerm, m.ProspectiveTerm); err != nil {
		return err
	}
	return e.boolean(m.Granted)
}

func encodeRequestVoteRequest(e *payloadEncoder, m RequestVoteRequest) error {
	if err := e.uint16(m.CandidateID); err != nil {
		return err
	}
	return encodeUint64s(e, m.Term, m.LastLogIndex, m.LastLogTerm)
}

func encodeRequestVoteResponse(e *payloadEncoder, m RequestVoteResponse) error {
	if err := encodeUint16s(e, m.ResponderID, m.CandidateID); err != nil {
		return err
	}
	if err := encodeUint64s(e, m.Term, m.RequestTerm); err != nil {
		return err
	}
	return e.boolean(m.Granted)
}

func encodeAppendEntriesRequest(e *payloadEncoder, m AppendEntriesRequest) error {
	if err := e.uint16(m.LeaderID); err != nil {
		return err
	}
	if err := encodeUint64s(e, m.Term, uint64(m.Generation), m.PrevLogIndex, m.PrevLogTerm, m.LeaderCommit); err != nil {
		return err
	}
	if err := e.uint16(uint16(len(m.Entries))); err != nil {
		return err
	}
	for _, entry := range m.Entries {
		if err := encodeUint64s(e, entry.Index, entry.Term); err != nil {
			return err
		}
		if err := e.uint8(uint8(entry.Kind)); err != nil {
			return err
		}
		if err := e.bytes32(entry.command); err != nil {
			return err
		}
	}
	return nil
}

func encodeAppendEntriesResponse(e *payloadEncoder, m AppendEntriesResponse) error {
	if err := encodeUint16s(e, m.ResponderID, m.LeaderID); err != nil {
		return err
	}
	if err := encodeUint64s(e, m.Term, m.RequestTerm, uint64(m.Generation)); err != nil {
		return err
	}
	if err := e.boolean(m.Success); err != nil {
		return err
	}
	return encodeUint64s(e, m.MatchIndex, m.ConflictTerm, m.ConflictIndex)
}

func encodeInstallSnapshotRequest(e *payloadEncoder, m InstallSnapshotRequest) error {
	if err := e.uint16(m.LeaderID); err != nil {
		return err
	}
	if err := e.uint64(m.Term); err != nil {
		return err
	}
	if err := e.add(m.TransferID[:]); err != nil {
		return err
	}
	if err := e.add(m.SnapshotID[:]); err != nil {
		return err
	}
	if err := encodeUint64s(e, m.LastIncludedIndex, m.LastIncludedTerm); err != nil {
		return err
	}
	if err := e.uint32(m.StateMachineSchemaVersion); err != nil {
		return err
	}
	if err := e.uint64(m.TotalLength); err != nil {
		return err
	}
	if err := e.add(m.Checksum[:]); err != nil {
		return err
	}
	if err := e.uint64(m.Offset); err != nil {
		return err
	}
	if err := e.bytes32(m.Chunk); err != nil {
		return err
	}
	return e.boolean(m.Done)
}

func encodeInstallSnapshotResponse(e *payloadEncoder, m InstallSnapshotResponse) error {
	if err := encodeUint16s(e, m.ResponderID, m.LeaderID); err != nil {
		return err
	}
	if err := encodeUint64s(e, m.Term, m.RequestTerm); err != nil {
		return err
	}
	if err := e.add(m.TransferID[:]); err != nil {
		return err
	}
	if err := e.add(m.SnapshotID[:]); err != nil {
		return err
	}
	if err := e.uint64(m.NextOffset); err != nil {
		return err
	}
	if err := e.boolean(m.Success); err != nil {
		return err
	}
	return e.boolean(m.Done)
}

func encodeErrorResponse(e *payloadEncoder, m ErrorResponse) error {
	if err := e.uint16(uint16(m.Code)); err != nil {
		return err
	}
	if err := e.uint16(m.ResponderID); err != nil {
		return err
	}
	return e.uint64(m.Term)
}

func encodeUint16s(e *payloadEncoder, values ...uint16) error {
	for _, value := range values {
		if err := e.uint16(value); err != nil {
			return err
		}
	}
	return nil
}

func encodeUint64s(e *payloadEncoder, values ...uint64) error {
	for _, value := range values {
		if err := e.uint64(value); err != nil {
			return err
		}
	}
	return nil
}

func decodeHandshake(d *payloadDecoder) (RPC, error) {
	id, err := d.uint16()
	if err != nil {
		return nil, err
	}
	message := Handshake{SenderID: id}
	if err := d.fixed(message.VoterFingerprint[:]); err != nil {
		return nil, err
	}
	if err := d.fixed(message.ApplicationFingerprint[:]); err != nil {
		return nil, err
	}
	return message, nil
}

func decodeHandshakeAck(d *payloadDecoder) (RPC, error) {
	id, err := d.uint16()
	if err != nil {
		return nil, err
	}
	message := HandshakeAck{ResponderID: id}
	if err := d.fixed(message.VoterFingerprint[:]); err != nil {
		return nil, err
	}
	if err := d.fixed(message.ApplicationFingerprint[:]); err != nil {
		return nil, err
	}
	return message, nil
}

func schemaVersionForRPC(rpc RPC) uint16 {
	switch rpc.(type) {
	case Handshake, HandshakeAck:
		return HandshakeSchemaVersion
	default:
		return RPCSchemaVersion
	}
}

func schemaVersionForMessage(messageType wire.MessageType) uint16 {
	switch messageType {
	case wire.MessageRaftHandshake, wire.MessageRaftHandshakeAck:
		return HandshakeSchemaVersion
	default:
		return RPCSchemaVersion
	}
}

func decodePreVoteRequest(d *payloadDecoder) (RPC, error) {
	id, err := d.uint16()
	if err != nil {
		return nil, err
	}
	fields, err := decodeUint64s(d, 4)
	if err != nil {
		return nil, err
	}
	return PreVoteRequest{CandidateID: id, CurrentTerm: fields[0], ProspectiveTerm: fields[1], LastLogIndex: fields[2], LastLogTerm: fields[3]}, nil
}

func decodePreVoteResponse(d *payloadDecoder) (RPC, error) {
	ids, err := decodeUint16s(d, 2)
	if err != nil {
		return nil, err
	}
	fields, err := decodeUint64s(d, 3)
	if err != nil {
		return nil, err
	}
	granted, err := d.boolean()
	if err != nil {
		return nil, err
	}
	return PreVoteResponse{ResponderID: ids[0], CandidateID: ids[1], Term: fields[0], RequestCurrentTerm: fields[1], ProspectiveTerm: fields[2], Granted: granted}, nil
}

func decodeRequestVoteRequest(d *payloadDecoder) (RPC, error) {
	id, err := d.uint16()
	if err != nil {
		return nil, err
	}
	fields, err := decodeUint64s(d, 3)
	if err != nil {
		return nil, err
	}
	return RequestVoteRequest{CandidateID: id, Term: fields[0], LastLogIndex: fields[1], LastLogTerm: fields[2]}, nil
}

func decodeRequestVoteResponse(d *payloadDecoder) (RPC, error) {
	ids, err := decodeUint16s(d, 2)
	if err != nil {
		return nil, err
	}
	fields, err := decodeUint64s(d, 2)
	if err != nil {
		return nil, err
	}
	granted, err := d.boolean()
	if err != nil {
		return nil, err
	}
	return RequestVoteResponse{ResponderID: ids[0], CandidateID: ids[1], Term: fields[0], RequestTerm: fields[1], Granted: granted}, nil
}

func decodeAppendEntriesRequest(d *payloadDecoder, limits CodecLimits) (RPC, error) {
	leaderID, err := d.uint16()
	if err != nil {
		return nil, err
	}
	fields, err := decodeUint64s(d, 5)
	if err != nil {
		return nil, err
	}
	count, err := d.uint16()
	if err != nil {
		return nil, err
	}
	if count > limits.MaxAppendEntries {
		return nil, fmt.Errorf("%w: append count %d exceeds %d", ErrRPCTooLarge, count, limits.MaxAppendEntries)
	}
	if uint64(count)*minimumEntryBytes > uint64(d.remaining()) {
		return nil, fmt.Errorf("%w: append count cannot fit remaining payload", ErrMalformedRPC)
	}
	message := AppendEntriesRequest{LeaderID: leaderID, Term: fields[0], Generation: RequestGeneration(fields[1]), PrevLogIndex: fields[2], PrevLogTerm: fields[3], LeaderCommit: fields[4]}
	if err := validateAppendRequest(message, limits); err != nil {
		return nil, err
	}

	entriesOffset := d.offset
	scan := *d
	expectedIndex := message.PrevLogIndex
	previousTerm := message.PrevLogTerm
	for index := 0; index < int(count); index++ {
		entry, err := decodeBorrowedEntry(&scan, limits.MaxCommandBytes)
		if err != nil {
			return nil, err
		}
		if expectedIndex == math.MaxUint64 {
			return nil, fmt.Errorf("%w: append index overflow", ErrInvalidRPC)
		}
		expectedIndex++
		if entry.Index != expectedIndex || entry.Term == 0 || entry.Term < previousTerm || entry.Term > message.Term || (entry.Kind != EntryCommand && entry.Kind != EntryNoOp) {
			return nil, fmt.Errorf("%w: invalid entry %d ordering or metadata", ErrInvalidRPC, index)
		}
		if entry.Kind == EntryNoOp && len(entry.command) != 0 {
			return nil, fmt.Errorf("%w: no-op entry carries command bytes", ErrInvalidRPC)
		}
		previousTerm = entry.Term
	}
	if scan.remaining() != 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes", ErrMalformedRPC, scan.remaining())
	}

	d.offset = entriesOffset
	message.Entries = make([]Entry, int(count))
	for index := range message.Entries {
		entry, err := decodeBorrowedEntry(d, limits.MaxCommandBytes)
		if err != nil {
			return nil, err
		}
		entry.command = cloneBytes(entry.command)
		message.Entries[index] = entry
	}
	return message, nil
}

func decodeBorrowedEntry(d *payloadDecoder, maximumCommandBytes uint64) (Entry, error) {
	index, err := d.uint64()
	if err != nil {
		return Entry{}, err
	}
	term, err := d.uint64()
	if err != nil {
		return Entry{}, err
	}
	kind, err := d.uint8()
	if err != nil {
		return Entry{}, err
	}
	command, err := d.bytes32Borrowed(maximumCommandBytes)
	if err != nil {
		return Entry{}, err
	}
	return Entry{Index: index, Term: term, Kind: EntryKind(kind), command: command}, nil
}

func decodeAppendEntriesResponse(d *payloadDecoder) (RPC, error) {
	ids, err := decodeUint16s(d, 2)
	if err != nil {
		return nil, err
	}
	fields, err := decodeUint64s(d, 3)
	if err != nil {
		return nil, err
	}
	success, err := d.boolean()
	if err != nil {
		return nil, err
	}
	hints, err := decodeUint64s(d, 3)
	if err != nil {
		return nil, err
	}
	return AppendEntriesResponse{ResponderID: ids[0], LeaderID: ids[1], Term: fields[0], RequestTerm: fields[1], Generation: RequestGeneration(fields[2]), Success: success, MatchIndex: hints[0], ConflictTerm: hints[1], ConflictIndex: hints[2]}, nil
}

func decodeInstallSnapshotRequest(d *payloadDecoder, limits CodecLimits) (RPC, error) {
	leaderID, err := d.uint16()
	if err != nil {
		return nil, err
	}
	term, err := d.uint64()
	if err != nil {
		return nil, err
	}
	message := InstallSnapshotRequest{LeaderID: leaderID, Term: term}
	if err := d.fixed(message.TransferID[:]); err != nil {
		return nil, err
	}
	if err := d.fixed(message.SnapshotID[:]); err != nil {
		return nil, err
	}
	metadata, err := decodeUint64s(d, 2)
	if err != nil {
		return nil, err
	}
	message.LastIncludedIndex, message.LastIncludedTerm = metadata[0], metadata[1]
	message.StateMachineSchemaVersion, err = d.uint32()
	if err != nil {
		return nil, err
	}
	message.TotalLength, err = d.uint64()
	if err != nil {
		return nil, err
	}
	if message.TotalLength > limits.MaxSnapshotBytes {
		return nil, fmt.Errorf("%w: snapshot total is %d bytes, maximum is %d", ErrRPCTooLarge, message.TotalLength, limits.MaxSnapshotBytes)
	}
	if err := d.fixed(message.Checksum[:]); err != nil {
		return nil, err
	}
	message.Offset, err = d.uint64()
	if err != nil {
		return nil, err
	}
	message.Chunk, err = d.bytes32Borrowed(limits.MaxSnapshotChunkBytes)
	if err != nil {
		return nil, err
	}
	message.Done, err = d.boolean()
	if err != nil {
		return nil, err
	}
	if d.remaining() != 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes", ErrMalformedRPC, d.remaining())
	}
	if err := validateSnapshotRequest(message, limits); err != nil {
		return nil, err
	}
	message.Chunk = cloneBytes(message.Chunk)
	return message, nil
}

func decodeInstallSnapshotResponse(d *payloadDecoder) (RPC, error) {
	ids, err := decodeUint16s(d, 2)
	if err != nil {
		return nil, err
	}
	fields, err := decodeUint64s(d, 2)
	if err != nil {
		return nil, err
	}
	message := InstallSnapshotResponse{ResponderID: ids[0], LeaderID: ids[1], Term: fields[0], RequestTerm: fields[1]}
	if err := d.fixed(message.TransferID[:]); err != nil {
		return nil, err
	}
	if err := d.fixed(message.SnapshotID[:]); err != nil {
		return nil, err
	}
	message.NextOffset, err = d.uint64()
	if err != nil {
		return nil, err
	}
	message.Success, err = d.boolean()
	if err != nil {
		return nil, err
	}
	message.Done, err = d.boolean()
	if err != nil {
		return nil, err
	}
	return message, nil
}

func decodeErrorResponse(d *payloadDecoder) (RPC, error) {
	code, err := d.uint16()
	if err != nil {
		return nil, err
	}
	responderID, err := d.uint16()
	if err != nil {
		return nil, err
	}
	term, err := d.uint64()
	if err != nil {
		return nil, err
	}
	return ErrorResponse{Code: ProtocolErrorCode(code), ResponderID: responderID, Term: term}, nil
}

func decodeUint16s(d *payloadDecoder, count int) ([]uint16, error) {
	values := make([]uint16, count)
	for index := range values {
		value, err := d.uint16()
		if err != nil {
			return nil, err
		}
		values[index] = value
	}
	return values, nil
}

func decodeUint64s(d *payloadDecoder, count int) ([]uint64, error) {
	values := make([]uint64, count)
	for index := range values {
		value, err := d.uint64()
		if err != nil {
			return nil, err
		}
		values[index] = value
	}
	return values, nil
}
