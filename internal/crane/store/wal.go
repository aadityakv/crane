package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// recoveryConsumer is the bounded Task14 seam. Implementations reduce one
// record at a time into prospective high-level state; they must not retain raw
// history merely to replay it later.
type recoveryConsumer interface {
	BeginTransaction(recordCount uint32) error
	ConsumeRecord(Record) error
	CommitTransaction() error
}

func recoverWAL(data []byte, expected Identity) (RecoveredState, int, error) {
	state, truncateAt, err := recoverWALReader(bytes.NewReader(data), int64(len(data)), expected, nil)
	return state, int(truncateAt), err
}

// recoverWALReader validates the complete WAL before invoking a consumer in a
// second bounded streaming pass. The consumer therefore never observes a WAL
// whose existing bytes were already known to be corrupt.
func recoverWALReader(reader io.ReaderAt, size int64, expected Identity, consumer recoveryConsumer) (RecoveredState, int64, error) {
	state, truncateAt, err := scanWAL(reader, size, expected, nil)
	if err != nil || consumer == nil {
		return state, truncateAt, err
	}
	replayed, replayEnd, err := scanWAL(reader, truncateAt, expected, consumer)
	if err != nil {
		return RecoveredState{}, 0, err
	}
	if replayEnd != truncateAt || replayed != state {
		return RecoveredState{}, 0, fmt.Errorf("%w: WAL changed between validation and replay", ErrCorrupt)
	}
	return state, truncateAt, nil
}

func scanWAL(reader io.ReaderAt, size int64, expected Identity, consumer recoveryConsumer) (RecoveredState, int64, error) {
	state := RecoveredState{Identity: expected}
	record, offset, incomplete, err := decodeRecordAt(reader, size, 0)
	if err != nil || incomplete || record.kind != recordIdentity || record.sequence != 1 || len(record.payload) != identityPayloadBytes {
		return RecoveredState{}, 0, fmt.Errorf("%w: invalid identity record: %v", ErrCorrupt, err)
	}
	var identity Identity
	copy(identity.ClusterID[:], record.payload[:16])
	identity.NodeID = binary.BigEndian.Uint16(record.payload[16:18])
	copy(state.WorkerEpoch[:], record.payload[18:34])
	if identity != expected {
		return RecoveredState{}, 0, fmt.Errorf("%w: disk cluster=%x node=%d", ErrIdentityMismatch, identity.ClusterID, identity.NodeID)
	}
	if err := validateEpoch(state.WorkerEpoch); err != nil {
		return RecoveredState{}, 0, fmt.Errorf("%w: zero epoch", ErrCorrupt)
	}
	state.LastSequence = 1
	for offset < size {
		beginAt := offset
		begin, next, partial, err := decodeRecordAt(reader, size, offset)
		if partial {
			state.WALBytes = uint64(beginAt)
			return state, beginAt, nil
		}
		if err != nil || begin.kind != recordTransactionBegin || begin.sequence != state.LastSequence+1 {
			return RecoveredState{}, 0, fmt.Errorf("%w: invalid transaction begin: %v", ErrCorrupt, err)
		}
		count, transactionBytes, digest, boundaryErr := decodeBoundary(begin.payload)
		if boundaryErr != nil {
			return RecoveredState{}, 0, boundaryErr
		}
		if count == 0 || count > MaxTransactionRecords {
			return RecoveredState{}, 0, fmt.Errorf("%w: record count %d", ErrCorrupt, count)
		}
		if begin.sequence > math.MaxUint64-count-1 {
			return RecoveredState{}, 0, fmt.Errorf("%w: transaction sequence overflow", ErrCorrupt)
		}
		minimum, maximum, boundsErr := transactionSpanBounds(count)
		if boundsErr != nil || transactionBytes < minimum || transactionBytes > maximum {
			return RecoveredState{}, 0, fmt.Errorf("%w: transaction length %d outside count=%d bounds [%d,%d]", ErrCorrupt, transactionBytes, count, minimum, maximum)
		}
		if transactionBytes > uint64(size-beginAt) {
			state.WALBytes = uint64(beginAt)
			return state, beginAt, nil
		}
		transactionEnd := beginAt + int64(transactionBytes)
		offset = next
		expectedSequence := begin.sequence + 1
		hasher := sha256.New()
		_, _ = hasher.Write([]byte(transactionDigestDomain))
		if consumer != nil {
			if err := consumer.BeginTransaction(uint32(count)); err != nil {
				return RecoveredState{}, 0, fmt.Errorf("recovery consumer begin: %w", err)
			}
		}
		for i := uint64(0); i < count; i++ {
			item, itemNext, itemPartial, itemErr := decodeRecordAt(reader, size, offset)
			if itemPartial {
				return RecoveredState{}, 0, fmt.Errorf("%w: incomplete record inside complete declared transaction", ErrCorrupt)
			}
			if itemErr != nil || item.kind != recordTransactionData || item.sequence != expectedSequence || len(item.payload) < dataPrefixBytes {
				return RecoveredState{}, 0, fmt.Errorf("%w: invalid transaction data: %v", ErrCorrupt, itemErr)
			}
			payloadLength := uint64(binary.BigEndian.Uint32(item.payload[2:6]))
			recordType := RecordType(binary.BigEndian.Uint16(item.payload[:2]))
			if recordType == 0 || payloadLength != uint64(len(item.payload)-dataPrefixBytes) || payloadLength > MaxRecordPayloadBytes {
				return RecoveredState{}, 0, fmt.Errorf("%w: invalid application record", ErrCorrupt)
			}
			_, _ = hasher.Write(item.frame)
			if consumer != nil {
				if err := consumer.ConsumeRecord(Record{Type: recordType, Payload: item.payload[dataPrefixBytes:]}); err != nil {
					return RecoveredState{}, 0, fmt.Errorf("recovery consumer record: %w", err)
				}
			}
			offset = itemNext
			expectedSequence++
		}
		commit, commitNext, commitPartial, commitErr := decodeRecordAt(reader, size, offset)
		if commitPartial {
			return RecoveredState{}, 0, fmt.Errorf("%w: incomplete commit inside complete declared transaction", ErrCorrupt)
		}
		if commitErr != nil || commit.kind != recordTransactionCommit || commit.sequence != expectedSequence || !bytes.Equal(commit.payload, begin.payload) || commitNext != transactionEnd {
			return RecoveredState{}, 0, fmt.Errorf("%w: invalid transaction commit: %v", ErrCorrupt, commitErr)
		}
		var computed [sha256.Size]byte
		copy(computed[:], hasher.Sum(nil))
		if computed != digest {
			return RecoveredState{}, 0, fmt.Errorf("%w: transaction digest mismatch", ErrCorrupt)
		}
		if consumer != nil {
			if err := consumer.CommitTransaction(); err != nil {
				return RecoveredState{}, 0, fmt.Errorf("recovery consumer commit: %w", err)
			}
		}
		state.TransactionCount++
		state.LastSequence = commit.sequence
		offset = commitNext
	}
	state.WALBytes = uint64(offset)
	return state, offset, nil
}

func transactionSpanBounds(count uint64) (uint64, uint64, error) {
	if count == 0 || count > MaxTransactionRecords {
		return 0, 0, fmt.Errorf("%w: record count %d", ErrInvalidTransaction, count)
	}
	boundaryBytes := uint64(2 * (walHeaderBytes + boundaryPayloadBytes + walChecksumBytes))
	minimumData := uint64(walHeaderBytes + dataPrefixBytes + walChecksumBytes)
	maximumData := minimumData + MaxRecordPayloadBytes
	if count > (math.MaxUint64-boundaryBytes)/maximumData {
		return 0, 0, fmt.Errorf("%w: transaction span overflow", ErrInvalidTransaction)
	}
	return boundaryBytes + count*minimumData, boundaryBytes + count*maximumData, nil
}

func decodeBoundary(payload []byte) (uint64, uint64, [32]byte, error) {
	if len(payload) != boundaryPayloadBytes {
		return 0, 0, [32]byte{}, fmt.Errorf("%w: boundary length", ErrCorrupt)
	}
	count := uint64(binary.BigEndian.Uint32(payload[:4]))
	total := binary.BigEndian.Uint64(payload[4:12])
	var digest [32]byte
	copy(digest[:], payload[12:44])
	if digest == ([32]byte{}) {
		return 0, 0, [32]byte{}, fmt.Errorf("%w: zero transaction digest", ErrCorrupt)
	}
	return count, total, digest, nil
}
