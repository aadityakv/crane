package store

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"

	"github.com/aaditya/cs425mp3/internal/crane/model"
)

const (
	walSchemaVersion     uint16 = 1
	walHeaderBytes              = 20
	walChecksumBytes            = 4
	identityPayloadBytes        = 34
	boundaryPayloadBytes        = 44
	dataPrefixBytes             = 6
)

var walMagic = [4]byte{'C', 'W', 'W', 'L'}
var walCRC = crc32.MakeTable(crc32.Castagnoli)

const transactionDigestDomain = "cs425/crane/worker-wal-transaction/v1\x00"

type walRecordType uint16

const (
	recordIdentity          walRecordType = 1
	recordTransactionBegin  walRecordType = 2
	recordTransactionData   walRecordType = 3
	recordTransactionCommit walRecordType = 4
)

type walRecord struct {
	kind     walRecordType
	sequence uint64
	payload  []byte
}

func encodeRecord(kind walRecordType, sequence uint64, payload []byte) ([]byte, error) {
	if kind < recordIdentity || kind > recordTransactionCommit || sequence == 0 || uint64(len(payload)) > math.MaxUint32 {
		return nil, fmt.Errorf("%w: invalid WAL record", ErrInvalidTransaction)
	}
	total := uint64(walHeaderBytes+walChecksumBytes) + uint64(len(payload))
	if total > uint64(math.MaxInt) {
		return nil, fmt.Errorf("%w: record size overflow", ErrInvalidTransaction)
	}
	encoded := make([]byte, int(total))
	writeRecordHeader(encoded, kind, sequence, uint32(len(payload)))
	copy(encoded[20:], payload)
	binary.BigEndian.PutUint32(encoded[len(encoded)-4:], crc32.Checksum(encoded[:len(encoded)-4], walCRC))
	return encoded, nil
}

func encodeIdentity(identity Identity, epoch model.WorkerEpoch) ([]byte, error) {
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	if err := validateEpoch(epoch); err != nil {
		return nil, err
	}
	payload := make([]byte, identityPayloadBytes)
	copy(payload[:16], identity.ClusterID[:])
	binary.BigEndian.PutUint16(payload[16:18], identity.NodeID)
	copy(payload[18:], epoch[:])
	return encodeRecord(recordIdentity, 1, payload)
}

func encodeTransaction(firstSequence uint64, transaction Transaction) ([]byte, error) {
	if err := transaction.Validate(); err != nil {
		return nil, err
	}
	total, err := transactionEncodedSize(transaction)
	if err != nil {
		return nil, err
	}
	needed := uint64(len(transaction.Records)) + 2
	if firstSequence == 0 || firstSequence > math.MaxUint64-needed+1 {
		return nil, fmt.Errorf("%w: sequence overflow", ErrInvalidTransaction)
	}
	if total > uint64(math.MaxInt) {
		return nil, fmt.Errorf("%w: encoded transaction exceeds address space", ErrInvalidTransaction)
	}
	result := make([]byte, int(total))
	boundaryRecordBytes := walHeaderBytes + boundaryPayloadBytes + walChecksumBytes
	offset := boundaryRecordBytes
	sequence := firstSequence + 1
	for _, record := range transaction.Records {
		payloadLength := dataPrefixBytes + len(record.Payload)
		frameLength := walHeaderBytes + payloadLength + walChecksumBytes
		frame := result[offset : offset+frameLength]
		writeRecordHeader(frame, recordTransactionData, sequence, uint32(payloadLength))
		binary.BigEndian.PutUint16(frame[walHeaderBytes:walHeaderBytes+2], uint16(record.Type))
		binary.BigEndian.PutUint32(frame[walHeaderBytes+2:walHeaderBytes+6], uint32(len(record.Payload)))
		copy(frame[walHeaderBytes+dataPrefixBytes:], record.Payload)
		binary.BigEndian.PutUint32(frame[len(frame)-walChecksumBytes:], crc32.Checksum(frame[:len(frame)-walChecksumBytes], walCRC))
		offset += frameLength
		sequence++
	}
	hasher := sha256.New()
	_, _ = hasher.Write([]byte(transactionDigestDomain))
	_, _ = hasher.Write(result[boundaryRecordBytes:offset])
	var digest [sha256.Size]byte
	copy(digest[:], hasher.Sum(nil))
	boundary := make([]byte, boundaryPayloadBytes)
	binary.BigEndian.PutUint32(boundary[:4], uint32(len(transaction.Records)))
	binary.BigEndian.PutUint64(boundary[4:12], total)
	copy(boundary[12:], digest[:])
	begin, err := encodeRecord(recordTransactionBegin, firstSequence, boundary)
	if err != nil {
		return nil, err
	}
	commit, err := encodeRecord(recordTransactionCommit, sequence, boundary)
	if err != nil {
		return nil, err
	}
	copy(result[:boundaryRecordBytes], begin)
	copy(result[offset:], commit)
	if offset+len(commit) != len(result) {
		return nil, fmt.Errorf("%w: encoded transaction size drift", ErrInvalidTransaction)
	}
	return result, nil
}

func transactionEncodedSize(transaction Transaction) (uint64, error) {
	if err := transaction.Validate(); err != nil {
		return 0, err
	}
	total := uint64(2 * (walHeaderBytes + boundaryPayloadBytes + walChecksumBytes))
	for _, record := range transaction.Records {
		frame := uint64(walHeaderBytes+dataPrefixBytes+walChecksumBytes) + uint64(len(record.Payload))
		if total > math.MaxUint64-frame {
			return 0, fmt.Errorf("%w: encoded transaction size overflow", ErrInvalidTransaction)
		}
		total += frame
	}
	return total, nil
}

func writeRecordHeader(destination []byte, kind walRecordType, sequence uint64, payloadLength uint32) {
	copy(destination[:4], walMagic[:])
	binary.BigEndian.PutUint16(destination[4:6], walSchemaVersion)
	binary.BigEndian.PutUint16(destination[6:8], uint16(kind))
	binary.BigEndian.PutUint32(destination[8:12], payloadLength)
	binary.BigEndian.PutUint64(destination[12:20], sequence)
}

func decodeRecord(data []byte, offset int) (walRecord, int, bool, error) {
	remaining := len(data) - offset
	if remaining < walHeaderBytes {
		return walRecord{}, offset, true, nil
	}
	header := data[offset : offset+walHeaderBytes]
	if string(header[:4]) != string(walMagic[:]) || binary.BigEndian.Uint16(header[4:6]) != walSchemaVersion {
		return walRecord{}, offset, false, fmt.Errorf("%w: WAL magic/schema", ErrCorrupt)
	}
	kind := walRecordType(binary.BigEndian.Uint16(header[6:8]))
	if kind < recordIdentity || kind > recordTransactionCommit {
		return walRecord{}, offset, false, fmt.Errorf("%w: record type %d", ErrCorrupt, kind)
	}
	sequence := binary.BigEndian.Uint64(header[12:20])
	if sequence == 0 {
		return walRecord{}, offset, false, fmt.Errorf("%w: zero record sequence", ErrCorrupt)
	}
	length := uint64(binary.BigEndian.Uint32(header[8:12]))
	switch kind {
	case recordIdentity:
		if length != identityPayloadBytes {
			return walRecord{}, offset, false, fmt.Errorf("%w: identity length %d", ErrCorrupt, length)
		}
	case recordTransactionBegin, recordTransactionCommit:
		if length != boundaryPayloadBytes {
			return walRecord{}, offset, false, fmt.Errorf("%w: boundary length %d", ErrCorrupt, length)
		}
	case recordTransactionData:
		if length < dataPrefixBytes || length > dataPrefixBytes+MaxRecordPayloadBytes {
			return walRecord{}, offset, false, fmt.Errorf("%w: data length %d", ErrCorrupt, length)
		}
	}
	total := uint64(walHeaderBytes+walChecksumBytes) + length
	if total > uint64(math.MaxInt) || total > uint64(remaining) {
		return walRecord{}, offset, true, nil
	}
	end := offset + int(total)
	want := binary.BigEndian.Uint32(data[end-4 : end])
	if crc32.Checksum(data[offset:end-4], walCRC) != want {
		return walRecord{}, offset, false, fmt.Errorf("%w: checksum", ErrCorrupt)
	}
	payload := append([]byte(nil), data[offset+walHeaderBytes:end-4]...)
	return walRecord{kind: kind, sequence: sequence, payload: payload}, end, false, nil
}
