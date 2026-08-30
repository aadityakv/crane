package raft

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"math/bits"

	"github.com/aaditya/cs425mp3/internal/config"
)

const (
	// MaxWALRecordPayloadBytes bounds every record before allocation.
	MaxWALRecordPayloadBytes uint64 = MaxRPCPayloadBytes
	// MaxWALTransactionBytes bounds one complete atomic persistence batch.
	MaxWALTransactionBytes uint64 = MaxRPCPayloadBytes

	walRecordHeaderBytes   = 16
	walRecordChecksumBytes = 4
	boundaryPayloadBytes   = 12
	hardStatePayloadBytes  = 26
	truncatePayloadBytes   = 16
	snapshotPayloadBytes   = 28
	appliedPayloadBytes    = 16
	minimumWALEntryBytes   = 8 + 8 + 1 + 4
)

var (
	walMagic       = [4]byte{'R', 'F', 'W', 'L'}
	walCRC32CTable = crc32.MakeTable(crc32.Castagnoli)
)

type walRecordType uint8

const (
	walRecordTransactionBegin walRecordType = iota + 1
	walRecordTruncate
	walRecordEntries
	walRecordSnapshotBase
	walRecordAppliedIndex
	walRecordHardState
	walRecordTransactionCommit
)

type transactionFlags uint8

const (
	transactionFlagHardState transactionFlags = 1 << iota
	transactionFlagTruncate
	transactionFlagEntries
	transactionFlagSnapshot
	transactionFlagApplied
	transactionFlagsAll = transactionFlagHardState | transactionFlagTruncate | transactionFlagEntries | transactionFlagSnapshot | transactionFlagApplied
)

type walRecord struct {
	typeCode walRecordType
	payload  []byte
}

func encodeWALRecord(recordType walRecordType, payload []byte) ([]byte, error) {
	if !validWALRecordType(recordType) {
		return nil, fmt.Errorf("%w: unknown WAL record type %d", ErrStorageCorrupt, recordType)
	}
	if uint64(len(payload)) > MaxWALRecordPayloadBytes {
		return nil, fmt.Errorf("%w: WAL record payload is %d bytes, maximum is %d", ErrInvalidStorageState, len(payload), MaxWALRecordPayloadBytes)
	}
	total64 := uint64(walRecordHeaderBytes) + uint64(len(payload)) + walRecordChecksumBytes
	if total64 > uint64(math.MaxInt) {
		return nil, fmt.Errorf("%w: WAL record exceeds local integer domain", ErrInvalidStorageState)
	}
	encoded := make([]byte, int(total64))
	copy(encoded[:4], walMagic[:])
	binary.BigEndian.PutUint16(encoded[4:6], uint16(StorageFormatVersion1))
	encoded[6] = byte(recordType)
	encoded[7] = 0
	binary.BigEndian.PutUint64(encoded[8:16], uint64(len(payload)))
	copy(encoded[walRecordHeaderBytes:], payload)
	checksumAt := walRecordHeaderBytes + len(payload)
	binary.BigEndian.PutUint32(encoded[checksumAt:], crc32.Checksum(encoded[:checksumAt], walCRC32CTable))
	return encoded, nil
}

func encodeWALTransaction(transactionID uint64, batch PersistenceBatch) ([]byte, error) {
	if transactionID == 0 {
		return nil, fmt.Errorf("%w: zero WAL transaction ID", ErrInvalidStorageState)
	}
	batch = batch.Clone()
	flags := flagsForBatch(batch)
	count := uint8(bits.OnesCount8(uint8(flags)))
	if flags == 0 || count == 0 {
		return nil, fmt.Errorf("%w: empty WAL transaction", ErrInvalidStorageState)
	}
	boundary, err := encodeTransactionBoundaryPayload(transactionID, flags, count)
	if err != nil {
		return nil, err
	}
	records := []walRecord{{typeCode: walRecordTransactionBegin, payload: boundary}}
	if flags&transactionFlagSnapshot != 0 {
		records = append(records, walRecord{typeCode: walRecordSnapshotBase, payload: encodeSnapshotPayload(transactionID, *batch.SnapshotBase)})
	}
	if flags&transactionFlagTruncate != 0 {
		records = append(records, walRecord{typeCode: walRecordTruncate, payload: encodeTwoUint64Payload(transactionID, batch.ReplaceFrom)})
	}
	if flags&transactionFlagEntries != 0 {
		payload, err := encodeEntriesPayload(transactionID, batch.Entries)
		if err != nil {
			return nil, err
		}
		records = append(records, walRecord{typeCode: walRecordEntries, payload: payload})
	}
	if flags&transactionFlagApplied != 0 {
		records = append(records, walRecord{typeCode: walRecordAppliedIndex, payload: encodeTwoUint64Payload(transactionID, *batch.AppliedIndex)})
	}
	if flags&transactionFlagHardState != 0 {
		records = append(records, walRecord{typeCode: walRecordHardState, payload: encodeHardStatePayload(transactionID, *batch.HardState)})
	}
	records = append(records, walRecord{typeCode: walRecordTransactionCommit, payload: boundary})

	var encoded []byte
	var total uint64
	for _, record := range records {
		bytes, err := encodeWALRecord(record.typeCode, record.payload)
		if err != nil {
			return nil, err
		}
		if uint64(len(bytes)) > MaxWALTransactionBytes-total {
			return nil, fmt.Errorf("%w: WAL transaction exceeds %d bytes", ErrInvalidStorageState, MaxWALTransactionBytes)
		}
		total += uint64(len(bytes))
		encoded = append(encoded, bytes...)
	}
	return encoded, nil
}

func encodeTransactionBoundaryPayload(transactionID uint64, flags transactionFlags, count uint8) ([]byte, error) {
	if transactionID == 0 || flags == 0 || flags&^transactionFlagsAll != 0 || count != uint8(bits.OnesCount8(uint8(flags))) {
		return nil, fmt.Errorf("%w: invalid transaction boundary id=%d flags=%02x count=%d", ErrInvalidStorageState, transactionID, flags, count)
	}
	payload := make([]byte, boundaryPayloadBytes)
	binary.BigEndian.PutUint64(payload[0:8], transactionID)
	payload[8] = byte(flags)
	payload[9] = count
	return payload, nil
}

func decodeTransactionBoundaryPayload(payload []byte) (uint64, transactionFlags, uint8, error) {
	if len(payload) != boundaryPayloadBytes {
		return 0, 0, 0, fmt.Errorf("%w: transaction boundary length=%d want=%d", ErrStorageCorrupt, len(payload), boundaryPayloadBytes)
	}
	id := binary.BigEndian.Uint64(payload[0:8])
	flags := transactionFlags(payload[8])
	count := payload[9]
	if id == 0 || flags == 0 || flags&^transactionFlagsAll != 0 || count != uint8(bits.OnesCount8(uint8(flags))) || payload[10] != 0 || payload[11] != 0 {
		return 0, 0, 0, fmt.Errorf("%w: impossible transaction boundary", ErrStorageCorrupt)
	}
	return id, flags, count, nil
}

func flagsForBatch(batch PersistenceBatch) transactionFlags {
	var flags transactionFlags
	if batch.HardState != nil {
		flags |= transactionFlagHardState
	}
	if batch.ReplaceFrom != 0 {
		flags |= transactionFlagTruncate
	}
	if len(batch.Entries) != 0 {
		flags |= transactionFlagEntries
	}
	if batch.SnapshotBase != nil {
		flags |= transactionFlagSnapshot
	}
	if batch.AppliedIndex != nil {
		flags |= transactionFlagApplied
	}
	return flags
}

func expectedRecordTypes(flags transactionFlags) []walRecordType {
	records := make([]walRecordType, 0, bits.OnesCount8(uint8(flags)))
	if flags&transactionFlagSnapshot != 0 {
		records = append(records, walRecordSnapshotBase)
	}
	if flags&transactionFlagTruncate != 0 {
		records = append(records, walRecordTruncate)
	}
	if flags&transactionFlagEntries != 0 {
		records = append(records, walRecordEntries)
	}
	if flags&transactionFlagApplied != 0 {
		records = append(records, walRecordAppliedIndex)
	}
	if flags&transactionFlagHardState != 0 {
		records = append(records, walRecordHardState)
	}
	return records
}

func encodeHardStatePayload(transactionID uint64, state HardState) []byte {
	payload := make([]byte, hardStatePayloadBytes)
	binary.BigEndian.PutUint64(payload[0:8], transactionID)
	binary.BigEndian.PutUint64(payload[8:16], state.Term)
	binary.BigEndian.PutUint16(payload[16:18], state.VotedFor)
	binary.BigEndian.PutUint64(payload[18:26], state.CommitIndex)
	return payload
}

func decodeHardStatePayload(payload []byte, transactionID uint64) (HardState, error) {
	if len(payload) != hardStatePayloadBytes || binary.BigEndian.Uint64(payload[:8]) != transactionID {
		return HardState{}, fmt.Errorf("%w: invalid hard-state record", ErrStorageCorrupt)
	}
	return HardState{
		Term:        binary.BigEndian.Uint64(payload[8:16]),
		VotedFor:    binary.BigEndian.Uint16(payload[16:18]),
		CommitIndex: binary.BigEndian.Uint64(payload[18:26]),
	}, nil
}

func encodeTwoUint64Payload(transactionID, value uint64) []byte {
	payload := make([]byte, truncatePayloadBytes)
	binary.BigEndian.PutUint64(payload[0:8], transactionID)
	binary.BigEndian.PutUint64(payload[8:16], value)
	return payload
}

func decodeTwoUint64Payload(payload []byte, transactionID uint64, description string) (uint64, error) {
	if len(payload) != truncatePayloadBytes || binary.BigEndian.Uint64(payload[:8]) != transactionID {
		return 0, fmt.Errorf("%w: invalid %s record", ErrStorageCorrupt, description)
	}
	return binary.BigEndian.Uint64(payload[8:16]), nil
}

func encodeSnapshotPayload(transactionID uint64, metadata SnapshotMetadata) []byte {
	payload := make([]byte, snapshotPayloadBytes)
	binary.BigEndian.PutUint64(payload[0:8], transactionID)
	binary.BigEndian.PutUint64(payload[8:16], metadata.LastIncludedIndex)
	binary.BigEndian.PutUint64(payload[16:24], metadata.LastIncludedTerm)
	binary.BigEndian.PutUint32(payload[24:28], metadata.StateMachineSchemaVersion)
	return payload
}

func decodeSnapshotPayload(payload []byte, transactionID uint64) (SnapshotMetadata, error) {
	if len(payload) != snapshotPayloadBytes || binary.BigEndian.Uint64(payload[:8]) != transactionID {
		return SnapshotMetadata{}, fmt.Errorf("%w: invalid snapshot-base record", ErrStorageCorrupt)
	}
	return SnapshotMetadata{
		LastIncludedIndex:         binary.BigEndian.Uint64(payload[8:16]),
		LastIncludedTerm:          binary.BigEndian.Uint64(payload[16:24]),
		StateMachineSchemaVersion: binary.BigEndian.Uint32(payload[24:28]),
	}, nil
}

func encodeEntriesPayload(transactionID uint64, entries []Entry) ([]byte, error) {
	if len(entries) == 0 || len(entries) > math.MaxUint16 {
		return nil, fmt.Errorf("%w: WAL entry count %d is invalid", ErrInvalidStorageState, len(entries))
	}
	payload := make([]byte, 10)
	binary.BigEndian.PutUint64(payload[:8], transactionID)
	binary.BigEndian.PutUint16(payload[8:10], uint16(len(entries)))
	for _, entry := range entries {
		if err := validateLogEntry(entry); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidStorageState, err)
		}
		command := entry.CommandBytes()
		if uint64(len(command)) > config.MaxRaftCommandBytes || uint64(len(command)) > math.MaxUint32 {
			return nil, fmt.Errorf("%w: command length %d exceeds %d", ErrInvalidStorageState, len(command), config.MaxRaftCommandBytes)
		}
		addition := uint64(minimumWALEntryBytes) + uint64(len(command))
		if addition > MaxWALRecordPayloadBytes-uint64(len(payload)) {
			return nil, fmt.Errorf("%w: entries record exceeds %d bytes", ErrInvalidStorageState, MaxWALRecordPayloadBytes)
		}
		start := len(payload)
		payload = append(payload, make([]byte, int(addition))...)
		binary.BigEndian.PutUint64(payload[start:start+8], entry.Index)
		binary.BigEndian.PutUint64(payload[start+8:start+16], entry.Term)
		payload[start+16] = byte(entry.Kind)
		binary.BigEndian.PutUint32(payload[start+17:start+21], uint32(len(command)))
		copy(payload[start+21:], command)
	}
	return payload, nil
}

func decodeEntriesPayload(payload []byte, transactionID uint64) ([]Entry, error) {
	if len(payload) < 10 || binary.BigEndian.Uint64(payload[:8]) != transactionID {
		return nil, fmt.Errorf("%w: invalid entries record prefix", ErrStorageCorrupt)
	}
	count := uint64(binary.BigEndian.Uint16(payload[8:10]))
	remaining := uint64(len(payload) - 10)
	if count == 0 || count > remaining/minimumWALEntryBytes || count > uint64(math.MaxInt) {
		return nil, fmt.Errorf("%w: impossible WAL entry count %d", ErrStorageCorrupt, count)
	}
	entries := make([]Entry, 0, int(count))
	offset := uint64(10)
	for item := uint64(0); item < count; item++ {
		if uint64(len(payload))-offset < minimumWALEntryBytes {
			return nil, fmt.Errorf("%w: truncated entry metadata", ErrStorageCorrupt)
		}
		index := binary.BigEndian.Uint64(payload[offset : offset+8])
		term := binary.BigEndian.Uint64(payload[offset+8 : offset+16])
		kind := EntryKind(payload[offset+16])
		commandLength := uint64(binary.BigEndian.Uint32(payload[offset+17 : offset+21]))
		offset += minimumWALEntryBytes
		if commandLength > config.MaxRaftCommandBytes || commandLength > uint64(len(payload))-offset || commandLength > uint64(math.MaxInt) {
			return nil, fmt.Errorf("%w: impossible command length %d", ErrStorageCorrupt, commandLength)
		}
		entry, err := NewEntry(index, term, kind, payload[offset:offset+commandLength])
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrStorageCorrupt, err)
		}
		entries = append(entries, entry)
		offset += commandLength
	}
	if offset != uint64(len(payload)) {
		return nil, fmt.Errorf("%w: %d trailing entry payload bytes", ErrStorageCorrupt, uint64(len(payload))-offset)
	}
	return entries, nil
}

func validWALRecordType(recordType walRecordType) bool {
	return recordType >= walRecordTransactionBegin && recordType <= walRecordTransactionCommit
}
