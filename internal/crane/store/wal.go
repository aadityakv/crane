package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
)

func recoverWAL(data []byte, expected Identity) (RecoveredState, int, error) {
	state := RecoveredState{Identity: expected}
	record, offset, incomplete, err := decodeRecord(data, 0)
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
	for offset < len(data) {
		beginAt := offset
		begin, next, partial, err := decodeRecord(data, offset)
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
		if transactionBytes < uint64(2*(walHeaderBytes+boundaryPayloadBytes+walChecksumBytes)) || transactionBytes > uint64(len(data)-beginAt) {
			if transactionBytes > uint64(len(data)-beginAt) {
				state.WALBytes = uint64(beginAt)
				return state, beginAt, nil
			}
			return RecoveredState{}, 0, fmt.Errorf("%w: impossible transaction length %d", ErrCorrupt, transactionBytes)
		}
		transactionEnd := beginAt + int(transactionBytes)
		offset = next
		expectedSequence := begin.sequence + 1
		transaction := Transaction{Records: make([]Record, 0, int(count))}
		hasher := sha256.New()
		_, _ = hasher.Write([]byte(transactionDigestDomain))
		for i := uint64(0); i < count; i++ {
			itemAt := offset
			item, itemNext, itemPartial, itemErr := decodeRecord(data, offset)
			if itemPartial {
				return RecoveredState{}, 0, fmt.Errorf("%w: incomplete record inside complete declared transaction", ErrCorrupt)
			}
			if itemErr != nil || item.kind != recordTransactionData || item.sequence != expectedSequence || len(item.payload) < dataPrefixBytes {
				return RecoveredState{}, 0, fmt.Errorf("%w: invalid transaction data: %v", ErrCorrupt, itemErr)
			}
			payloadLength := uint64(binary.BigEndian.Uint32(item.payload[2:6]))
			if RecordType(binary.BigEndian.Uint16(item.payload[:2])) == 0 || payloadLength != uint64(len(item.payload)-dataPrefixBytes) || payloadLength > MaxRecordPayloadBytes {
				return RecoveredState{}, 0, fmt.Errorf("%w: invalid application record", ErrCorrupt)
			}
			transaction.Records = append(transaction.Records, Record{Type: RecordType(binary.BigEndian.Uint16(item.payload[:2])), Payload: append([]byte(nil), item.payload[6:]...)})
			_, _ = hasher.Write(data[itemAt:itemNext])
			offset = itemNext
			expectedSequence++
		}
		commit, commitNext, commitPartial, commitErr := decodeRecord(data, offset)
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
		state.Transactions = append(state.Transactions, transaction)
		state.LastSequence = commit.sequence
		offset = commitNext
	}
	state.WALBytes = uint64(offset)
	return state, offset, nil
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
