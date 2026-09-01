package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/aaditya/cs425mp3/internal/crane/model"
)

const (
	currentFilename          = "worker.current"
	currentTempFilename      = ".worker.current.tmp"
	snapshotSchemaVersion    = uint16(1)
	snapshotHeaderBytes      = 104
	snapshotFooterBytes      = sha256.Size
	snapshotFrameHeaderBytes = 6
	snapshotFrameCRCBytes    = 4
	snapshotFrameOverhead    = snapshotFrameHeaderBytes + snapshotFrameCRCBytes
	currentSchemaVersion     = uint16(1)
	currentFileBytes         = 110
)

var snapshotMagic = [4]byte{'C', 'W', 'S', 'S'}
var currentMagic = [4]byte{'C', 'W', 'C', 'M'}

type snapshotRecordKind uint16

const (
	snapshotFence snapshotRecordKind = iota + 1
	snapshotAssignment
	snapshotSource
	snapshotDelivery
	snapshotOutbox
	snapshotResult
	snapshotRepair
	snapshotEvent
	snapshotCheckpointObservation
)

// Snapshot identifies one committed complete high-level worker-state image.
type Snapshot struct {
	// Generation is the monotonic durable generation selected by worker.current.
	Generation uint64
	// BaseSequence is the last global WAL sequence included in the image.
	BaseSequence uint64
	// TransactionCount is the total committed transaction count included in the image.
	TransactionCount uint64
	// Bytes is the exact checksummed snapshot file length.
	Bytes uint64
}

type currentGeneration struct {
	Identity         Identity
	WorkerEpoch      model.WorkerEpoch
	Generation       uint64
	BaseSequence     uint64
	TransactionCount uint64
	SnapshotBytes    uint64
	SnapshotDigest   [32]byte
}

func snapshotFilename(generation uint64) string {
	return fmt.Sprintf("worker.snapshot.%020d", generation)
}

func snapshotTempFilename(generation uint64) string {
	return fmt.Sprintf(".worker.snapshot.%020d.tmp", generation)
}

func generationWALFilename(generation uint64) string {
	return fmt.Sprintf("worker.wal.%020d", generation)
}

func generationWALTempFilename(generation uint64) string {
	return fmt.Sprintf(".worker.wal.%020d.tmp", generation)
}

func recognizedStoreFilename(name string) bool {
	if name == WorkerWALFilename || name == WorkerLockFilename || name == currentFilename || name == currentTempFilename {
		return true
	}
	_, _, ok := parseGenerationFilename(name)
	return ok
}

func parseGenerationFilename(name string) (generation uint64, temporary bool, ok bool) {
	for _, prefix := range []string{"worker.snapshot.", "worker.wal."} {
		candidate := name
		if strings.HasPrefix(candidate, ".") && strings.HasSuffix(candidate, ".tmp") {
			temporary = true
			candidate = strings.TrimSuffix(strings.TrimPrefix(candidate, "."), ".tmp")
		}
		if !strings.HasPrefix(candidate, prefix) {
			continue
		}
		digits := strings.TrimPrefix(candidate, prefix)
		if len(digits) != 20 || digits[0] == '-' {
			return 0, false, false
		}
		value, err := strconv.ParseUint(digits, 10, 64)
		if err != nil || value == 0 || fmt.Sprintf("%020d", value) != digits {
			return 0, false, false
		}
		return value, temporary, true
	}
	return 0, false, false
}

func writeSnapshotFile(file *os.File, state RecoveredState, work RecoveredWork, generation uint64, write func(*os.File, []byte) (int, error)) (Snapshot, [32]byte, error) {
	metadata, header, err := snapshotMetadata(state, work, generation)
	if err != nil {
		return Snapshot{}, [32]byte{}, err
	}
	hasher := sha256.New()
	if err := writeSnapshotChunk(file, header, write, hasher); err != nil {
		return Snapshot{}, [32]byte{}, err
	}
	err = visitSnapshotRecords(work, func(kind snapshotRecordKind, payload []byte) error {
		frame, frameErr := encodeSnapshotFrame(kind, payload)
		if frameErr != nil {
			return frameErr
		}
		return writeSnapshotChunk(file, frame, write, hasher)
	})
	if err != nil {
		return Snapshot{}, [32]byte{}, err
	}
	var digest [32]byte
	copy(digest[:], hasher.Sum(nil))
	if digest == ([32]byte{}) {
		return Snapshot{}, [32]byte{}, fmt.Errorf("%w: zero snapshot digest", ErrUnavailable)
	}
	if err := writeFullWith(file, digest[:], write); err != nil {
		return Snapshot{}, [32]byte{}, err
	}
	return metadata, digest, nil
}

func writeSnapshotChunk(file *os.File, data []byte, write func(*os.File, []byte) (int, error), hasher hash.Hash) error {
	if err := writeFullWith(file, data, write); err != nil {
		return err
	}
	_, _ = hasher.Write(data)
	return nil
}

func snapshotMetadata(state RecoveredState, work RecoveredWork, generation uint64) (Snapshot, []byte, error) {
	return snapshotMetadataWithSourceSchemas(state, work, generation, nil)
}

func snapshotMetadataWithSourceSchemas(state RecoveredState, work RecoveredWork, generation uint64, sourceSchemas map[model.TaskID]uint16) (Snapshot, []byte, error) {
	if generation == 0 || !validSnapshotTransactionMetadata(state.LastSequence, state.TransactionCount) || !validSnapshotEventMetadata(work.NextTransactionID, state.LastSequence, state.TransactionCount) || state.WorkerEpoch.Validate() != nil || state.Identity.Validate() != nil {
		return Snapshot{}, nil, fmt.Errorf("%w: invalid snapshot metadata", ErrInvalidTransaction)
	}
	var count, body uint64
	err := visitSnapshotRecordsWithSourceSchemas(work, sourceSchemas, func(_ snapshotRecordKind, payload []byte) error {
		if len(payload) > MaxRecordPayloadBytes {
			return fmt.Errorf("%w: snapshot record exceeds bound", ErrCapacity)
		}
		if count == math.MaxUint64 {
			return fmt.Errorf("%w: snapshot record count", ErrCapacity)
		}
		frameBytes := uint64(snapshotFrameOverhead) + uint64(len(payload))
		if body > math.MaxUint64-frameBytes {
			return fmt.Errorf("%w: snapshot byte overflow", ErrCapacity)
		}
		count++
		body += frameBytes
		return nil
	})
	if err != nil {
		return Snapshot{}, nil, err
	}
	base := uint64(snapshotHeaderBytes + snapshotFooterBytes)
	if body > math.MaxUint64-base {
		return Snapshot{}, nil, fmt.Errorf("%w: snapshot length overflow", ErrCapacity)
	}
	total := base + body
	header := make([]byte, snapshotHeaderBytes)
	copy(header[:4], snapshotMagic[:])
	binary.BigEndian.PutUint16(header[4:6], snapshotSchemaVersion)
	binary.BigEndian.PutUint16(header[6:8], snapshotHeaderBytes)
	binary.BigEndian.PutUint64(header[8:16], total)
	copy(header[16:32], state.Identity.ClusterID[:])
	binary.BigEndian.PutUint16(header[32:34], state.Identity.NodeID)
	copy(header[34:50], state.WorkerEpoch[:])
	binary.BigEndian.PutUint64(header[50:58], generation)
	binary.BigEndian.PutUint64(header[58:66], state.LastSequence)
	binary.BigEndian.PutUint64(header[66:74], state.TransactionCount)
	binary.BigEndian.PutUint64(header[74:82], work.NextTransactionID)
	binary.BigEndian.PutUint64(header[82:90], count)
	binary.BigEndian.PutUint64(header[90:98], body)
	// Bytes 98:100 are reserved and canonically zero.
	binary.BigEndian.PutUint32(header[100:104], crc32.Checksum(header[:100], walCRC))
	return Snapshot{Generation: generation, BaseSequence: state.LastSequence, TransactionCount: state.TransactionCount, Bytes: total}, header, nil
}

func encodeSnapshotFrame(kind snapshotRecordKind, payload []byte) ([]byte, error) {
	if kind < snapshotFence || kind > snapshotCheckpointObservation || len(payload) > MaxRecordPayloadBytes {
		return nil, fmt.Errorf("%w: invalid snapshot frame", ErrInvalidTransaction)
	}
	frame := make([]byte, snapshotFrameOverhead+len(payload))
	binary.BigEndian.PutUint16(frame[:2], uint16(kind))
	binary.BigEndian.PutUint32(frame[2:6], uint32(len(payload)))
	copy(frame[6:], payload)
	binary.BigEndian.PutUint32(frame[len(frame)-4:], crc32.Checksum(frame[:len(frame)-4], walCRC))
	return frame, nil
}

func visitSnapshotRecords(work RecoveredWork, visit func(snapshotRecordKind, []byte) error) error {
	return visitSnapshotRecordsWithSourceSchemas(work, nil, visit)
}

func visitSnapshotRecordsWithSourceSchemas(work RecoveredWork, sourceSchemas map[model.TaskID]uint16, visit func(snapshotRecordKind, []byte) error) error {
	if work.Fence != (model.CoordinatorEpoch{}) {
		payload, err := encodeFence(work.Fence)
		if err != nil {
			return err
		}
		if err := visit(snapshotFence, payload); err != nil {
			return err
		}
	}
	assignments := append([]InstalledAssignment(nil), work.Assignments...)
	sort.Slice(assignments, func(i, j int) bool {
		return bytes.Compare(assignments[i].Assignment.JobID[:], assignments[j].Assignment.JobID[:]) < 0
	})
	for _, assignment := range assignments {
		payload, err := encodeAssignment(assignment)
		if err != nil {
			return err
		}
		if err := visit(snapshotAssignment, payload); err != nil {
			return err
		}
	}
	sources := append([]SourceCursor(nil), work.Sources...)
	sort.Slice(sources, func(i, j int) bool { return taskLess(sources[i].Source, sources[j].Source) })
	for _, source := range sources {
		schema := sourceRecordSchema
		if sourceSchemas != nil && sourceSchemas[source.Source] != 0 {
			schema = sourceSchemas[source.Source]
		}
		payload, err := encodeSourceSchema(source, nil, schema)
		if err != nil {
			return err
		}
		if err := visit(snapshotSource, payload); err != nil {
			return err
		}
	}
	checkpoints := append([]CommittedCheckpoint(nil), work.Checkpoints...)
	sort.Slice(checkpoints, func(i, j int) bool { return taskLess(checkpoints[i].Notice.Source, checkpoints[j].Notice.Source) })
	for _, checkpoint := range checkpoints {
		payload, err := encodeCheckpointObservation(checkpoint)
		if err != nil {
			return err
		}
		if err := visit(snapshotCheckpointObservation, payload); err != nil {
			return err
		}
	}
	outboxByID := make(map[model.DeliveryID]OutboxRecord, len(work.Outboxes))
	for _, outbox := range work.Outboxes {
		if _, duplicate := outboxByID[outbox.ID]; duplicate {
			return errors.New("duplicate outbox identity in snapshot state")
		}
		outboxByID[outbox.ID] = outbox
	}
	referenced := make(map[model.DeliveryID]struct{})
	deliveries := append([]DeliveryRecord(nil), work.Deliveries...)
	sort.Slice(deliveries, func(i, j int) bool { return deliveryIDLess(deliveries[i].ID, deliveries[j].ID) })
	for _, delivery := range deliveries {
		outboxes := make([]OutboxRecord, len(delivery.OutboxIDs))
		for i, id := range delivery.OutboxIDs {
			outbox, ok := outboxByID[id]
			if !ok {
				return errors.New("snapshot delivery references missing outbox")
			}
			outboxes[i] = outbox
			referenced[id] = struct{}{}
		}
		payload, err := encodeDeliveryRecord(delivery, outboxes)
		if err != nil {
			return err
		}
		if err := visit(snapshotDelivery, payload); err != nil {
			return err
		}
	}
	standalone := make([]OutboxRecord, 0, len(work.Outboxes)-len(referenced))
	for _, outbox := range work.Outboxes {
		if _, ok := referenced[outbox.ID]; !ok {
			standalone = append(standalone, outbox)
		}
	}
	sort.Slice(standalone, func(i, j int) bool { return deliveryIDLess(standalone[i].ID, standalone[j].ID) })
	for _, outbox := range standalone {
		payload, err := encodeOutbox(outbox)
		if err != nil {
			return err
		}
		if err := visit(snapshotOutbox, payload); err != nil {
			return err
		}
	}
	var resultErr error
	visitResults(work, func(result StoredResult) bool {
		payload, err := encodeStoredResult(result)
		if err == nil {
			err = visit(snapshotResult, payload)
		}
		resultErr = err
		return err == nil
	})
	if resultErr != nil {
		return resultErr
	}
	repairs := append([]ResultRepairRecord(nil), work.Repairs...)
	sort.Slice(repairs, func(i, j int) bool {
		return bytes.Compare(repairs[i].Instruction.RepairID[:], repairs[j].Instruction.RepairID[:]) < 0
	})
	for _, repair := range repairs {
		payload, err := encodeRepair(repair)
		if err != nil {
			return err
		}
		if err := visit(snapshotRepair, payload); err != nil {
			return err
		}
	}
	events := append([]model.WorkerEvent(nil), work.PendingEvents...)
	sort.Slice(events, func(i, j int) bool { return events[i].TransactionID < events[j].TransactionID })
	for _, event := range events {
		payload, err := encodeEvent(event)
		if err != nil {
			return err
		}
		if err := visit(snapshotEvent, payload); err != nil {
			return err
		}
	}
	return nil
}

func recoverSnapshotReader(reader io.ReaderAt, size int64, expected Identity, current currentGeneration, maxBytes uint64) (RecoveredWork, Snapshot, error) {
	if size < snapshotHeaderBytes+snapshotFooterBytes || uint64(size) > maxBytes {
		return RecoveredWork{}, Snapshot{}, fmt.Errorf("%w: snapshot size %d", ErrCorrupt, size)
	}
	header := make([]byte, snapshotHeaderBytes)
	if err := readAtFull(reader, header, 0); err != nil {
		return RecoveredWork{}, Snapshot{}, fmt.Errorf("%w: snapshot header: %v", ErrCorrupt, err)
	}
	metadata, nextTransactionID, count, body, err := decodeSnapshotHeader(header, size, expected, current)
	if err != nil {
		return RecoveredWork{}, Snapshot{}, err
	}
	wantDigest := make([]byte, snapshotFooterBytes)
	if err := readAtFull(reader, wantDigest, size-snapshotFooterBytes); err != nil {
		return RecoveredWork{}, Snapshot{}, fmt.Errorf("%w: snapshot footer", ErrCorrupt)
	}
	hasher := sha256.New()
	buffer := make([]byte, 64<<10)
	for offset := int64(0); offset < size-snapshotFooterBytes; {
		chunk := int64(len(buffer))
		if remaining := size - snapshotFooterBytes - offset; chunk > remaining {
			chunk = remaining
		}
		if err := readAtFull(reader, buffer[:chunk], offset); err != nil {
			return RecoveredWork{}, Snapshot{}, fmt.Errorf("%w: snapshot digest read", ErrCorrupt)
		}
		_, _ = hasher.Write(buffer[:chunk])
		offset += chunk
	}
	var digest [32]byte
	copy(digest[:], hasher.Sum(nil))
	if digest == ([32]byte{}) || !bytes.Equal(digest[:], wantDigest) || digest != current.SnapshotDigest {
		return RecoveredWork{}, Snapshot{}, fmt.Errorf("%w: snapshot digest mismatch", ErrCorrupt)
	}
	if err := scanSnapshotFrames(reader, snapshotHeaderBytes, body, count, nil); err != nil {
		return RecoveredWork{}, Snapshot{}, err
	}
	decoder := newSnapshotDecoder(newRecoveredWork(), body)
	decoder.work.NextTransactionID = nextTransactionID
	if err := scanSnapshotFrames(reader, snapshotHeaderBytes, body, count, decoder); err != nil {
		return RecoveredWork{}, Snapshot{}, err
	}
	if err := validateSnapshotWork(decoder.work, expected.NodeID, current.WorkerEpoch); err != nil {
		return RecoveredWork{}, Snapshot{}, fmt.Errorf("%w: snapshot state: %v", ErrCorrupt, err)
	}
	canonicalState := RecoveredState{Identity: expected, WorkerEpoch: current.WorkerEpoch, LastSequence: metadata.BaseSequence, TransactionCount: metadata.TransactionCount}
	canonical, canonicalHeader, err := snapshotMetadataWithSourceSchemas(canonicalState, decoder.work, metadata.Generation, decoder.sourceSchemas)
	if err != nil || canonical != metadata {
		return RecoveredWork{}, Snapshot{}, fmt.Errorf("%w: noncanonical snapshot metadata", ErrCorrupt)
	}
	canonicalHasher := sha256.New()
	_, _ = canonicalHasher.Write(canonicalHeader)
	err = visitSnapshotRecordsWithSourceSchemas(decoder.work, decoder.sourceSchemas, func(kind snapshotRecordKind, payload []byte) error {
		frame, frameErr := encodeSnapshotFrame(kind, payload)
		if frameErr == nil {
			_, _ = canonicalHasher.Write(frame)
		}
		return frameErr
	})
	if err != nil || !bytes.Equal(canonicalHasher.Sum(nil), digest[:]) {
		return RecoveredWork{}, Snapshot{}, fmt.Errorf("%w: noncanonical snapshot bytes", ErrCorrupt)
	}
	return decoder.work, metadata, nil
}

func decodeSnapshotHeader(header []byte, size int64, expected Identity, current currentGeneration) (Snapshot, uint64, uint64, uint64, error) {
	if len(header) != snapshotHeaderBytes || !bytes.Equal(header[:4], snapshotMagic[:]) || binary.BigEndian.Uint16(header[4:6]) != snapshotSchemaVersion || binary.BigEndian.Uint16(header[6:8]) != snapshotHeaderBytes || header[98] != 0 || header[99] != 0 || crc32.Checksum(header[:100], walCRC) != binary.BigEndian.Uint32(header[100:104]) {
		return Snapshot{}, 0, 0, 0, fmt.Errorf("%w: snapshot header schema/checksum", ErrCorrupt)
	}
	var identity Identity
	copy(identity.ClusterID[:], header[16:32])
	identity.NodeID = binary.BigEndian.Uint16(header[32:34])
	var epoch model.WorkerEpoch
	copy(epoch[:], header[34:50])
	if identity != expected {
		return Snapshot{}, 0, 0, 0, fmt.Errorf("%w: snapshot identity", ErrIdentityMismatch)
	}
	total := binary.BigEndian.Uint64(header[8:16])
	generation := binary.BigEndian.Uint64(header[50:58])
	baseSequence := binary.BigEndian.Uint64(header[58:66])
	transactions := binary.BigEndian.Uint64(header[66:74])
	nextTransactionID := binary.BigEndian.Uint64(header[74:82])
	count := binary.BigEndian.Uint64(header[82:90])
	body := binary.BigEndian.Uint64(header[90:98])
	if count > math.MaxUint64/snapshotFrameOverhead {
		return Snapshot{}, 0, 0, 0, fmt.Errorf("%w: snapshot header count", ErrCorrupt)
	}
	minimumBody := count * snapshotFrameOverhead
	if body < minimumBody || body > uint64(size)-snapshotHeaderBytes-snapshotFooterBytes || total != uint64(size) || total != uint64(snapshotHeaderBytes+snapshotFooterBytes)+body || generation == 0 || !validSnapshotEventMetadata(nextTransactionID, baseSequence, transactions) {
		return Snapshot{}, 0, 0, 0, fmt.Errorf("%w: snapshot header bounds", ErrCorrupt)
	}
	if current.Identity != identity || current.WorkerEpoch != epoch || current.Generation != generation || current.BaseSequence != baseSequence || current.TransactionCount != transactions || current.SnapshotBytes != total || !validSnapshotTransactionMetadata(baseSequence, transactions) {
		return Snapshot{}, 0, 0, 0, fmt.Errorf("%w: snapshot/current marker mismatch", ErrCorrupt)
	}
	return Snapshot{Generation: generation, BaseSequence: baseSequence, TransactionCount: transactions, Bytes: total}, nextTransactionID, count, body, nil
}

func scanSnapshotFrames(reader io.ReaderAt, start int64, bodyBytes, count uint64, decoder *snapshotDecoder) error {
	offset, end := start, start+int64(bodyBytes)
	for index := uint64(0); index < count; index++ {
		if end-offset < snapshotFrameOverhead {
			return fmt.Errorf("%w: truncated snapshot frame header", ErrCorrupt)
		}
		var header [snapshotFrameHeaderBytes]byte
		if err := readAtFull(reader, header[:], offset); err != nil {
			return fmt.Errorf("%w: snapshot frame header", ErrCorrupt)
		}
		kind := snapshotRecordKind(binary.BigEndian.Uint16(header[:2]))
		length := uint64(binary.BigEndian.Uint32(header[2:6]))
		frameBytes := uint64(snapshotFrameOverhead) + length
		if kind < snapshotFence || kind > snapshotCheckpointObservation || length < snapshotMinimumPayload(kind) || length > MaxRecordPayloadBytes || frameBytes > uint64(end-offset) {
			return fmt.Errorf("%w: snapshot frame bounds", ErrCorrupt)
		}
		if decoder != nil {
			if err := decoder.reserveFrame(kind, length); err != nil {
				return fmt.Errorf("%w: snapshot frame %d: %v", ErrCorrupt, index, err)
			}
			payload := make([]byte, int(length))
			if err := readAtFull(reader, payload, offset+snapshotFrameHeaderBytes); err != nil {
				return fmt.Errorf("%w: snapshot frame", ErrCorrupt)
			}
			var checksum [snapshotFrameCRCBytes]byte
			if err := readAtFull(reader, checksum[:], offset+snapshotFrameHeaderBytes+int64(length)); err != nil || snapshotFrameChecksum(header[:], payload) != binary.BigEndian.Uint32(checksum[:]) {
				return fmt.Errorf("%w: snapshot frame checksum", ErrCorrupt)
			}
			if err := decoder.consumeReserved(kind, payload); err != nil {
				return fmt.Errorf("%w: snapshot frame %d: %v", ErrCorrupt, index, err)
			}
		} else if err := verifySnapshotFrame(reader, offset, header[:], length); err != nil {
			return err
		}
		offset += int64(frameBytes)
	}
	if offset != end {
		return fmt.Errorf("%w: snapshot body length mismatch", ErrCorrupt)
	}
	return nil
}

func snapshotMinimumPayload(kind snapshotRecordKind) uint64 {
	switch kind {
	case snapshotFence:
		return 36
	case snapshotAssignment:
		return 6 // schema plus a length-delimited canonical value
	case snapshotResult:
		return 303 // wrapper, minimum canonical result and fixed provenance
	case snapshotSource:
		return 56 // schema, task, four cursors, nested count
	case snapshotDelivery:
		return 19 // schema, state, reservation, delivery blob, two nested counts
	case snapshotOutbox:
		return 7 // schema, completion bit, delivery blob
	case snapshotRepair:
		return 410 // zero-checkpoint repair definition and fixed progress
	case snapshotEvent:
		return 223 // fixed event prefix plus the smaller failure body
	case snapshotCheckpointObservation:
		return 136 // schema, notice, revisions and assignment digest
	default:
		return math.MaxUint64
	}
}

func snapshotFrameChecksum(header, payload []byte) uint32 {
	digest := crc32.New(walCRC)
	_, _ = digest.Write(header)
	_, _ = digest.Write(payload)
	return digest.Sum32()
}

func verifySnapshotFrame(reader io.ReaderAt, offset int64, header []byte, length uint64) error {
	digest := crc32.New(walCRC)
	_, _ = digest.Write(header)
	var buffer [64 << 10]byte
	payloadOffset := offset + snapshotFrameHeaderBytes
	for remaining := length; remaining != 0; {
		chunk := uint64(len(buffer))
		if chunk > remaining {
			chunk = remaining
		}
		if err := readAtFull(reader, buffer[:int(chunk)], payloadOffset); err != nil {
			return fmt.Errorf("%w: snapshot frame", ErrCorrupt)
		}
		_, _ = digest.Write(buffer[:int(chunk)])
		payloadOffset += int64(chunk)
		remaining -= chunk
	}
	var checksum [snapshotFrameCRCBytes]byte
	if err := readAtFull(reader, checksum[:], payloadOffset); err != nil || digest.Sum32() != binary.BigEndian.Uint32(checksum[:]) {
		return fmt.Errorf("%w: snapshot frame checksum", ErrCorrupt)
	}
	return nil
}

type snapshotDecoder struct {
	work          RecoveredWork
	initialized   bool
	fences        uint64
	assignments   uint64
	sources       uint64
	checkpoints   uint64
	deliveries    uint64
	outboxes      uint64
	results       uint64
	repairs       uint64
	events        uint64
	resultBytes   map[model.JobID]uint64
	maxOutboxes   uint64
	sourceSchemas map[model.TaskID]uint16
}

func (decoder *snapshotDecoder) consume(kind snapshotRecordKind, payload []byte) error {
	if err := decoder.reserveFrame(kind, uint64(len(payload))); err != nil {
		return err
	}
	return decoder.consumeReserved(kind, payload)
}

func newSnapshotDecoder(work RecoveredWork, bodyBytes uint64) *snapshotDecoder {
	decoder := &snapshotDecoder{work: work, sourceSchemas: make(map[model.TaskID]uint16)}
	if snapshotFrameOverhead != 0 {
		decoder.maxOutboxes = bodyBytes / snapshotFrameOverhead
	}
	return decoder
}

func (decoder *snapshotDecoder) initializeCounts() error {
	if decoder.initialized {
		return nil
	}
	decoder.initialized = true
	if decoder.work.Fence != (model.CoordinatorEpoch{}) {
		decoder.fences = 1
	}
	decoder.assignments = uint64(len(decoder.work.Assignments))
	decoder.sources = uint64(len(decoder.work.Sources))
	decoder.checkpoints = uint64(len(decoder.work.Checkpoints))
	decoder.deliveries = uint64(len(decoder.work.Deliveries))
	decoder.outboxes = uint64(len(decoder.work.Outboxes))
	decoder.repairs = uint64(len(decoder.work.Repairs))
	decoder.events = uint64(len(decoder.work.PendingEvents))
	decoder.resultBytes = make(map[model.JobID]uint64)
	if decoder.work.indexes != nil {
		decoder.results = decoder.work.indexes.resultCount
		for job, total := range decoder.work.indexes.resultBytesByJob {
			decoder.resultBytes[job] = total
		}
		return nil
	}
	decoder.results = uint64(len(decoder.work.Results))
	for _, result := range decoder.work.Results {
		if result.canonical == nil {
			return errors.New("snapshot decoder seed has unindexed result")
		}
		prior := decoder.resultBytes[result.Record.TupleID.JobID]
		if uint64(len(result.canonical)) > math.MaxUint64-prior {
			return ErrCapacity
		}
		decoder.resultBytes[result.Record.TupleID.JobID] = prior + uint64(len(result.canonical))
	}
	return nil
}

func reserveSnapshotCount(count *uint64, maximum uint64) error {
	if *count >= maximum {
		return ErrCapacity
	}
	*count++
	return nil
}

func (decoder *snapshotDecoder) reserveFrame(kind snapshotRecordKind, length uint64) error {
	if length < snapshotMinimumPayload(kind) || length > MaxRecordPayloadBytes {
		return ErrCapacity
	}
	if err := decoder.initializeCounts(); err != nil {
		return err
	}
	limits := model.LimitsV1()
	if limits.MaxRetainedJobs != 0 && limits.MaxTasksPerJob > math.MaxUint64/limits.MaxRetainedJobs {
		return ErrCapacity
	}
	maxSources := limits.MaxRetainedJobs * limits.MaxTasksPerJob
	switch kind {
	case snapshotFence:
		return reserveSnapshotCount(&decoder.fences, 1)
	case snapshotAssignment:
		return reserveSnapshotCount(&decoder.assignments, limits.MaxRetainedJobs)
	case snapshotSource:
		return reserveSnapshotCount(&decoder.sources, maxSources)
	case snapshotCheckpointObservation:
		return reserveSnapshotCount(&decoder.checkpoints, maxSources)
	case snapshotDelivery:
		return reserveSnapshotCount(&decoder.deliveries, MaxTransactionRecords)
	case snapshotOutbox:
		if decoder.maxOutboxes != 0 {
			return reserveSnapshotCount(&decoder.outboxes, decoder.maxOutboxes)
		}
		if decoder.outboxes == math.MaxUint64 {
			return ErrCapacity
		}
		decoder.outboxes++
		return nil
	case snapshotResult:
		return reserveSnapshotCount(&decoder.results, maxStoredResultCount())
	case snapshotRepair:
		return reserveSnapshotCount(&decoder.repairs, 64)
	case snapshotEvent:
		return reserveSnapshotCount(&decoder.events, MaxTransactionRecords)
	default:
		return errors.New("unknown snapshot record kind")
	}
}

func (decoder *snapshotDecoder) consumeReserved(kind snapshotRecordKind, payload []byte) error {
	if err := decoder.preflightNested(kind, payload); err != nil {
		return err
	}
	switch kind {
	case snapshotFence:
		epoch, err := decodeFence(payload)
		if err != nil {
			return err
		}
		return applyFence(&decoder.work, epoch)
	case snapshotAssignment:
		assignment, err := decodeAssignment(payload)
		if err != nil {
			return err
		}
		return applySnapshotAssignment(&decoder.work, assignment)
	case snapshotSource:
		cursor, outboxes, err := decodeSource(payload)
		if err != nil || len(outboxes) != 0 {
			return errors.New("invalid snapshot source")
		}
		decoder.sourceSchemas[cursor.Source] = binary.BigEndian.Uint16(payload[:2])
		decoder.work.Sources = append(decoder.work.Sources, cursor)
		return nil
	case snapshotCheckpointObservation:
		checkpoint, err := decodeCheckpointObservation(payload)
		if err != nil {
			return err
		}
		return applySnapshotCheckpointObservation(&decoder.work, checkpoint)
	case snapshotDelivery:
		delivery, outboxes, err := decodeDeliveryRecord(payload)
		if err != nil || !outboxesCanonical(outboxes) {
			if err == nil {
				err = errors.New("snapshot delivery outboxes are not canonical")
			}
			return err
		}
		delivery.definitionDigest, err = deliveryDefinitionDigest(delivery)
		if err != nil {
			return err
		}
		decoder.work.Deliveries = append(decoder.work.Deliveries, delivery)
		decoder.work.Outboxes = append(decoder.work.Outboxes, outboxes...)
		return nil
	case snapshotOutbox:
		outbox, err := decodeOutbox(payload)
		if err != nil {
			return err
		}
		decoder.work.Outboxes = append(decoder.work.Outboxes, outbox)
		return nil
	case snapshotResult:
		result, err := decodeStoredResult(payload)
		if err != nil {
			return err
		}
		return applySnapshotResult(&decoder.work, result)
	case snapshotRepair:
		repair, err := decodeRepair(payload)
		if err != nil {
			return err
		}
		return applySnapshotRepair(&decoder.work, repair)
	case snapshotEvent:
		event, err := decodeEvent(payload)
		if err != nil {
			return err
		}
		decoder.work.PendingEvents = append(decoder.work.PendingEvents, event)
		return nil
	default:
		return errors.New("unknown snapshot record kind")
	}
}

func (decoder *snapshotDecoder) reserveOutboxes(count uint64) error {
	if count > math.MaxUint64-decoder.outboxes {
		return ErrCapacity
	}
	next := decoder.outboxes + count
	if decoder.maxOutboxes != 0 && next > decoder.maxOutboxes {
		return ErrCapacity
	}
	decoder.outboxes = next
	return nil
}

// preflightNested walks only fixed-width lengths and counters. It deliberately
// does not decode or clone peer bytes; semantic decoding happens after every
// collection and byte reservation is known to fit.
func (decoder *snapshotDecoder) preflightNested(kind snapshotRecordKind, payload []byte) error {
	switch kind {
	case snapshotDelivery:
		reader := snapshotPayloadReader{data: payload}
		if err := reader.schema(); err != nil || reader.skip(1+8) != nil || reader.blob(MaxRecordPayloadBytes) != nil {
			return errors.New("invalid snapshot delivery prefix")
		}
		outputs, err := reader.u16()
		if err != nil || uint64(outputs) > model.LimitsV1().MaxOperatorOutputs || reader.remaining() < int(outputs)*4+2 {
			return errors.New("snapshot delivery outputs exceed bounds")
		}
		for index := uint16(0); index < outputs; index++ {
			if err := reader.blob(model.LimitsV1().MaxTuplePayloadBytes); err != nil {
				return err
			}
		}
		outboxes, err := reader.u16()
		if err != nil || uint64(outboxes) > model.LimitsV1().MaxDerivedDeliveries || reader.remaining() < int(outboxes)*4 {
			return errors.New("snapshot delivery outboxes exceed bounds")
		}
		if err := decoder.reserveOutboxes(uint64(outboxes)); err != nil {
			return err
		}
		for index := uint16(0); index < outboxes; index++ {
			if err := reader.blob(MaxRecordPayloadBytes); err != nil {
				return err
			}
		}
		if !reader.done() {
			return errors.New("trailing snapshot delivery bytes")
		}
	case snapshotSource:
		reader := snapshotPayloadReader{data: payload}
		schema, err := reader.u16()
		if err != nil || schema != domainRecordSchema && schema != sourceRecordSchema {
			return errors.New("invalid snapshot source schema")
		}
		fixed := 20 + 4*8
		if schema == sourceRecordSchema {
			// checkpoint revision + job/assignment revisions + digest + token + epoch
			fixed += 8 + 8 + 8 + 32 + 86 + 34
		}
		if reader.skip(fixed) != nil {
			return errors.New("invalid snapshot source prefix")
		}
		outboxes, err := reader.u16()
		if err != nil || uint64(outboxes) > model.LimitsV1().MaxDerivedDeliveries || reader.remaining() < int(outboxes)*4 {
			return errors.New("snapshot source outboxes exceed bounds")
		}
		// Snapshot source cursors never embed outboxes: those are emitted as
		// independently bounded canonical outbox frames.
		if outboxes != 0 {
			return errors.New("snapshot source embeds outboxes")
		}
		if !reader.done() {
			return errors.New("trailing snapshot source bytes")
		}
	case snapshotResult:
		reader := snapshotPayloadReader{data: payload}
		if err := reader.schema(); err != nil {
			return err
		}
		logical, err := reader.blobBytes(MaxRecordPayloadBytes)
		entryBytes, sizeErr := resultArtifactEntryBytes(uint64(len(logical)))
		if err != nil || sizeErr != nil {
			return errors.New("snapshot result logical bytes exceed bounds")
		}
		var job model.JobID
		copy(job[:], logical[2:18])
		prior := decoder.resultBytes[job]
		if prior > model.LimitsV1().MaxResultRecordsBytesPerJob || entryBytes > model.LimitsV1().MaxResultRecordsBytesPerJob-prior {
			return ErrCapacity
		}
		decoder.resultBytes[job] = prior + entryBytes
	case snapshotRepair:
		// Through SpecificationHash in RepairResultPartitionDefinition.
		const checkpointCountOffset = 2 + 16 + 34 + 16 + 8 + 32 + 2 + 16 + 2 + 16 + 20 + 32
		reader := snapshotPayloadReader{data: payload}
		if err := reader.skip(checkpointCountOffset); err != nil {
			return errors.New("truncated snapshot repair definition")
		}
		checkpoints, err := reader.u16()
		if err != nil || checkpoints > model.WorkerControlMaxCheckpointsV1 || reader.remaining() < int(checkpoints)*28+212 {
			return errors.New("snapshot repair checkpoints exceed bounds")
		}
	}
	return nil
}

type snapshotPayloadReader struct {
	data   []byte
	offset int
}

func (reader *snapshotPayloadReader) remaining() int { return len(reader.data) - reader.offset }
func (reader *snapshotPayloadReader) done() bool     { return reader.remaining() == 0 }
func (reader *snapshotPayloadReader) skip(count int) error {
	if count < 0 || reader.remaining() < count {
		return errors.New("truncated snapshot payload")
	}
	reader.offset += count
	return nil
}
func (reader *snapshotPayloadReader) u16() (uint16, error) {
	if reader.remaining() < 2 {
		return 0, errors.New("truncated snapshot uint16")
	}
	value := binary.BigEndian.Uint16(reader.data[reader.offset : reader.offset+2])
	reader.offset += 2
	return value, nil
}
func (reader *snapshotPayloadReader) u32() (uint32, error) {
	if reader.remaining() < 4 {
		return 0, errors.New("truncated snapshot uint32")
	}
	value := binary.BigEndian.Uint32(reader.data[reader.offset : reader.offset+4])
	reader.offset += 4
	return value, nil
}
func (reader *snapshotPayloadReader) schema() error {
	value, err := reader.u16()
	if err != nil || value != domainRecordSchema {
		return errors.New("unsupported snapshot domain schema")
	}
	return nil
}
func (reader *snapshotPayloadReader) blob(maximum uint64) error {
	_, err := reader.blobBytes(maximum)
	return err
}
func (reader *snapshotPayloadReader) blobBytes(maximum uint64) ([]byte, error) {
	count, err := reader.u32()
	if err != nil || uint64(count) > maximum || reader.remaining() < int(count) {
		return nil, errors.New("snapshot blob exceeds bounds or remaining bytes")
	}
	value := reader.data[reader.offset : reader.offset+int(count)]
	reader.offset += int(count)
	return value, nil
}

// Snapshot records are already-accepted durable facts. A later fence or
// assignment may make their provenance historical without invalidating the
// retained fact. These reducers therefore validate historical provenance at
// or before the current authority, while the WAL reducers remain deliberately
// stricter and admit new mutations only under the exact current authority.
func applySnapshotAssignment(work *RecoveredWork, installed InstalledAssignment) error {
	decoded, err := validateSnapshotAssignment(installed, work.Fence)
	if err != nil {
		return err
	}
	if assignmentIndex(work.Assignments, installed.Assignment.JobID) >= 0 {
		return errors.New("duplicate snapshot assignment")
	}
	if uint64(len(work.Assignments)) >= model.LimitsV1().MaxRetainedJobs {
		return ErrCapacity
	}
	installed.Topology = decoded
	work.Assignments = append(work.Assignments, cloneInstalled(installed))
	return nil
}

func applySnapshotCheckpointObservation(work *RecoveredWork, observation CommittedCheckpoint) error {
	if err := validateCheckpointObservation(observation); err != nil {
		return err
	}
	assignment, ok := findAssignment(work, observation.Notice.JobID)
	if !ok || observation.AssignmentRevision > assignment.Assignment.Revision || observation.JobControlRevision > assignment.JobControlRevision || !snapshotEpochAtOrBefore(observation.Notice.Epoch, work.Fence) {
		return errors.New("snapshot checkpoint observation authority mismatch")
	}
	if observation.AssignmentRevision == assignment.Assignment.Revision && observation.AssignmentDigest != assignment.Assignment.Digest {
		return errors.New("snapshot checkpoint observation assignment digest mismatch")
	}
	stage, ok := findStage(assignment.Topology, observation.Notice.Source.StageID)
	if !ok || stage.Role != model.StageSource {
		return errors.New("snapshot checkpoint observation source is not a source stage")
	}
	if checkpointObservationIndex(work.Checkpoints, observation.Notice.Source) >= 0 {
		return errors.New("duplicate snapshot checkpoint observation")
	}
	work.Checkpoints = append(work.Checkpoints, observation)
	return nil
}

func validateSnapshotAssignment(installed InstalledAssignment, fence model.CoordinatorEpoch) (model.ValidatedTopology, error) {
	decoded, err := model.DecodeTopology(installed.SpecificationBytes)
	if err != nil {
		return model.ValidatedTopology{}, err
	}
	if err := installed.Assignment.Validate(decoded); err != nil {
		return model.ValidatedTopology{}, err
	}
	if installed.JobControlRevision == 0 || installed.SchedulingState < model.SchedulingClosed || installed.SchedulingState > model.SchedulingDraining || !snapshotEpochAtOrBefore(installed.CoordinatorEpoch, fence) {
		return model.ValidatedTopology{}, errors.New("invalid snapshot assignment metadata")
	}
	return decoded, nil
}

func applySnapshotResult(work *RecoveredWork, result StoredResult) error {
	assignment, ok := findAssignment(work, result.Record.TupleID.JobID)
	if !ok {
		return errors.New("snapshot result references unknown job")
	}
	if err := validateSnapshotResult(result, assignment, work.Fence); err != nil {
		return err
	}
	if result.canonical == nil {
		result.canonical, _ = model.MarshalResultRecord(result.Record)
	}
	if result.canonical == nil {
		return errors.New("snapshot result is not canonical")
	}
	if err := ensureWorkIndexes(work); err != nil {
		return err
	}
	key := resultKey{SinkTask: result.Record.SinkTask, TupleID: result.Record.TupleID}
	if findResultNode(work.indexes.results, key) != nil {
		return model.ErrIdentityReuse
	}
	entryBytes, err := resultArtifactEntryBytes(uint64(len(result.canonical)))
	if err != nil {
		return err
	}
	jobBytes := work.indexes.resultBytesByJob[result.Record.TupleID.JobID]
	if jobBytes > model.LimitsV1().MaxResultRecordsBytesPerJob || entryBytes > model.LimitsV1().MaxResultRecordsBytesPerJob-jobBytes || work.indexes.resultCount >= maxStoredResultCount() {
		return ErrCapacity
	}
	result.Record.Value = append([]byte(nil), result.Record.Value...)
	inserted, err := insertResultNode(work.indexes.results, &resultNode{key: key, value: result, height: 1})
	if err != nil {
		return err
	}
	work.indexes.results = inserted
	work.indexes.resultBytesByJob[result.Record.TupleID.JobID] = jobBytes + entryBytes
	work.indexes.resultCount++
	return nil
}

func validateSnapshotResult(result StoredResult, assignment InstalledAssignment, fence model.CoordinatorEpoch) error {
	if result.Record.SpecificationHash != assignment.Topology.Digest() {
		return errors.New("snapshot result references unknown topology")
	}
	stage, exists := findStage(assignment.Topology, result.Record.SinkTask.StageID)
	if !exists || stage.Role != model.StageSink || result.Record.SinkTask.Partition >= stage.Parallelism {
		return errors.New("snapshot result is not an installed sink partition")
	}
	if err := result.Provenance.Validate(result.Record); err != nil {
		return err
	}
	provenance := result.Provenance
	if provenance.AssignmentRevision > assignment.Assignment.Revision || !snapshotEpochAtOrBefore(provenance.CoordinatorEpoch, fence) {
		return errors.New("snapshot result provenance is newer than installed authority")
	}
	if provenance.AssignmentRevision == assignment.Assignment.Revision {
		want, exists := findReplica(assignment.Assignment, result.Record.SinkTask)
		if !exists || provenance.AssignmentDigest != assignment.Assignment.Digest || provenance.ReplicaSet != want {
			return errors.New("snapshot result current assignment cross-reference mismatch")
		}
	}
	return nil
}

func applySnapshotRepair(work *RecoveredWork, repair ResultRepairRecord) error {
	definition := repair.Instruction
	assignment, ok := findAssignment(work, definition.JobID)
	if !ok {
		return errors.New("snapshot repair references unknown job")
	}
	if err := validateSnapshotRepair(repair, assignment, work.Fence); err != nil {
		return err
	}
	if repairIndex(work.Repairs, definition.RepairID) >= 0 {
		return model.ErrIdentityReuse
	}
	if len(work.Repairs) >= 64 {
		return ErrCapacity
	}
	work.Repairs = append(work.Repairs, cloneRepair(repair))
	return nil
}

func validateSnapshotRepair(repair ResultRepairRecord, assignment InstalledAssignment, fence model.CoordinatorEpoch) error {
	if err := validateRepair(repair); err != nil {
		return err
	}
	definition := repair.Instruction
	if definition.SpecificationHash != assignment.Topology.Digest() || definition.AssignmentRevision > assignment.Assignment.Revision || !snapshotEpochAtOrBefore(definition.CoordinatorEpoch, fence) {
		return errors.New("snapshot repair references unknown or newer authority")
	}
	stage, exists := findStage(assignment.Topology, definition.SinkTask.StageID)
	if !exists || stage.Role != model.StageSink || definition.SinkTask.Partition >= stage.Parallelism {
		return errors.New("snapshot repair is not for an installed sink partition")
	}
	for index, checkpoint := range definition.Checkpoints {
		if checkpoint.Source.JobID != definition.JobID || index > 0 && !taskLess(definition.Checkpoints[index-1].Source, checkpoint.Source) {
			return errors.New("snapshot repair checkpoint vector is not canonical")
		}
		eof, err := model.SourceEOF(assignment.Topology, checkpoint.Source)
		if err != nil || checkpoint.Watermark > eof {
			return errors.New("snapshot repair checkpoint is outside installed topology")
		}
	}
	if definition.AssignmentRevision == assignment.Assignment.Revision {
		replica, exists := findReplica(assignment.Assignment, definition.SinkTask)
		if definition.AssignmentDigest != assignment.Assignment.Digest || !exists || !repairDestinationMatchesReplica(definition, replica) {
			return errors.New("snapshot repair current assignment cross-reference mismatch")
		}
	}
	return nil
}

func snapshotEpochAtOrBefore(candidate, current model.CoordinatorEpoch) bool {
	if candidate.Validate() != nil || current.Validate() != nil {
		return false
	}
	comparison := compareEpochOrder(candidate, current)
	return comparison < 0 || comparison == 0 && candidate == current
}

func validateSnapshotWork(work RecoveredWork, nodeID uint16, workerEpoch model.WorkerEpoch) error {
	if work.NextTransactionID == 0 || len(work.Deliveries) > MaxTransactionRecords || len(work.Repairs) > 64 || len(work.PendingEvents) > MaxTransactionRecords || uint64(len(work.Assignments)) > model.LimitsV1().MaxRetainedJobs {
		return errors.New("snapshot collection exceeds domain bounds")
	}
	for index, assignment := range work.Assignments {
		decoded, err := validateSnapshotAssignment(assignment, work.Fence)
		if err != nil || decoded.Digest() != assignment.Topology.Digest() || index > 0 && !jobIDLess(work.Assignments[index-1].Assignment.JobID, assignment.Assignment.JobID) {
			return errors.New("invalid or noncanonical snapshot assignment")
		}
	}
	maxSources := model.LimitsV1().MaxRetainedJobs * model.LimitsV1().MaxTasksPerJob
	if uint64(len(work.Sources)) > maxSources {
		return errors.New("snapshot source collection exceeds domain bounds")
	}
	seenSources := make(map[model.TaskID]struct{}, len(work.Sources))
	for _, cursor := range work.Sources {
		if _, duplicate := seenSources[cursor.Source]; duplicate {
			return errors.New("duplicate snapshot source")
		}
		seenSources[cursor.Source] = struct{}{}
		assignment, ok := findAssignment(&work, cursor.Source.JobID)
		token, tokenOK := findToken(assignment.Assignment, cursor.Source)
		eof, eofErr := model.SourceEOF(assignment.Topology, cursor.Source)
		if !ok || !tokenOK || token.WorkerID != nodeID || token.WorkerEpoch != workerEpoch || eofErr != nil || eof != cursor.EOF || cursor.NextSequence == 0 || cursor.NextSequence > model.LimitsV1().MaxSourceSequences+1 || cursor.Watermark >= cursor.NextSequence || cursor.Watermark > cursor.EOF || cursor.EOF != 0 && cursor.NextSequence > cursor.EOF+1 || cursor.Watermark != 0 && cursor.RaftIndex == 0 || cursor.Watermark == 0 && cursor.CheckpointRevision != 0 || !validCheckpointAuthority(cursor) {
			return errors.New("invalid snapshot source cursor")
		}
	}
	if uint64(len(work.Checkpoints)) > maxSources {
		return errors.New("snapshot checkpoint observation collection exceeds domain bounds")
	}
	for index, observation := range work.Checkpoints {
		if index > 0 && !taskLess(work.Checkpoints[index-1].Notice.Source, observation.Notice.Source) {
			return errors.New("snapshot checkpoint observations are not canonical")
		}
		assignment, ok := findAssignment(&work, observation.Notice.JobID)
		if !ok || observation.AssignmentRevision > assignment.Assignment.Revision || observation.JobControlRevision > assignment.JobControlRevision || !snapshotEpochAtOrBefore(observation.Notice.Epoch, work.Fence) {
			return errors.New("invalid snapshot checkpoint observation authority")
		}
		if observation.AssignmentRevision == assignment.Assignment.Revision && observation.AssignmentDigest != assignment.Assignment.Digest {
			return errors.New("invalid snapshot checkpoint observation digest")
		}
		stage, exists := findStage(assignment.Topology, observation.Notice.Source.StageID)
		if validateCheckpointObservation(observation) != nil || !exists || stage.Role != model.StageSource {
			return errors.New("invalid snapshot checkpoint observation")
		}
	}
	seenDeliveries := make(map[model.DeliveryID]struct{}, len(work.Deliveries))
	referencedOutboxes := make(map[model.DeliveryID]struct{})
	referencedExpected := make(map[model.DeliveryID]OutboxRecord)
	for _, delivery := range work.Deliveries {
		if _, duplicate := seenDeliveries[delivery.ID]; duplicate {
			return errors.New("duplicate snapshot delivery")
		}
		seenDeliveries[delivery.ID] = struct{}{}
		assignment, ok := findAssignment(&work, delivery.ID.Tuple.JobID)
		if !ok || validateSnapshotDelivery(delivery, assignment, work.Fence) != nil || delivery.State < Received || delivery.State > Completed {
			return errors.New("invalid snapshot delivery")
		}
		if cursor := sourceIndex(work.Sources, delivery.ID.Tuple.SourceTask); cursor >= 0 && delivery.ID.Tuple.SourceSequence <= work.Sources[cursor].Watermark {
			return errors.New("snapshot retained checkpoint-covered delivery")
		}
		if delivery.State == Received {
			if len(delivery.Outputs) != 0 || len(delivery.OutboxIDs) != 0 {
				return errors.New("received snapshot delivery has processed content")
			}
			continue
		}
		if !outboxIDsCanonical(delivery.OutboxIDs) {
			return errors.New("snapshot delivery outbox identities are not canonical")
		}
		if err := validateProcessedOutputs(delivery, delivery.Outputs, assignment); err != nil {
			return err
		}
		expected, err := expectedSnapshotProcessedOutboxes(delivery, assignment)
		if err != nil || len(expected) != len(delivery.OutboxIDs) {
			return errors.New("snapshot delivery outbox set mismatch")
		}
		for _, id := range delivery.OutboxIDs {
			if _, duplicate := referencedOutboxes[id]; duplicate {
				return errors.New("snapshot outbox has multiple parents")
			}
			if _, ok := expected[id]; !ok {
				return errors.New("snapshot delivery references non-derived outbox")
			}
			referencedOutboxes[id] = struct{}{}
			referencedExpected[id] = expected[id]
		}
	}
	seenOutboxes := make(map[model.DeliveryID]OutboxRecord, len(work.Outboxes))
	for _, outbox := range work.Outboxes {
		if _, duplicate := seenOutboxes[outbox.ID]; duplicate {
			return errors.New("duplicate snapshot outbox")
		}
		seenOutboxes[outbox.ID] = outbox
		assignment, ok := findAssignment(&work, outbox.ID.Tuple.JobID)
		if !ok || validateSnapshotOutbox(outbox, assignment, work.Fence) != nil {
			return errors.New("invalid snapshot outbox")
		}
		if _, owned := referencedOutboxes[outbox.ID]; owned {
			if !equalSnapshotDerivedOutbox(referencedExpected[outbox.ID], outbox) {
				return errors.New("snapshot delivery outbox definition mismatch")
			}
			continue
		}
		cursorIndex := sourceIndex(work.Sources, outbox.ID.Tuple.SourceTask)
		if cursorIndex < 0 {
			return errors.New("orphan snapshot outbox")
		}
		cursor := work.Sources[cursorIndex]
		sequence := outbox.ID.Tuple.SourceSequence
		if outbox.Producer.Task != cursor.Source || sequence <= cursor.Watermark || sequence >= cursor.NextSequence {
			return errors.New("snapshot source outbox outside retained cursor range")
		}
		cursorForSequence := cursor
		cursorForSequence.NextSequence = sequence + 1
		expected, err := expectedSourceOutboxes(cursorForSequence, outbox.Producer, assignment)
		want, exists := expected[outbox.ID]
		if err != nil || !exists || !equalOutboxDefinition(want, outbox) {
			return errors.New("snapshot source outbox route mismatch")
		}
	}
	for id := range referencedOutboxes {
		outbox, ok := seenOutboxes[id]
		if !ok {
			return errors.New("snapshot delivery references missing outbox")
		}
		if deliveryIndexForOutboxCompleted(work.Deliveries, id) && !outbox.Completed {
			return errors.New("completed snapshot delivery has incomplete outbox")
		}
	}
	var resultErr error
	visitResults(work, func(result StoredResult) bool {
		assignment, ok := findAssignment(&work, result.Record.TupleID.JobID)
		if !ok {
			resultErr = errors.New("snapshot result references unknown assignment")
			return false
		}
		resultErr = validateSnapshotResult(result, assignment, work.Fence)
		return resultErr == nil
	})
	if resultErr != nil {
		return resultErr
	}
	seenRepairs := make(map[[16]byte]struct{}, len(work.Repairs))
	for _, repair := range work.Repairs {
		if _, duplicate := seenRepairs[repair.Instruction.RepairID]; duplicate {
			return errors.New("duplicate snapshot repair")
		}
		seenRepairs[repair.Instruction.RepairID] = struct{}{}
		assignment, ok := findAssignment(&work, repair.Instruction.JobID)
		if !ok || validateSnapshotRepair(repair, assignment, work.Fence) != nil {
			return errors.New("invalid snapshot repair")
		}
	}
	if err := validateSnapshotEvents(work); err != nil {
		return err
	}
	return validateSnapshotWorkLocal(work, nodeID, workerEpoch)
}

func validateSnapshotEvents(work RecoveredWork) error {
	if len(work.PendingEvents) == 0 {
		return nil
	}
	next := work.PendingEvents[0].TransactionID
	for _, event := range work.PendingEvents {
		if err := event.Validate(); err != nil || event.TransactionID != next {
			return errors.New("invalid snapshot event sequence")
		}
		var job model.JobID
		var revision, jobRevision uint64
		var token model.AssignmentToken
		var epoch model.CoordinatorEpoch
		if event.Completion != nil {
			job = event.Completion.JobID
			revision = event.Completion.AssignmentRevision
			jobRevision = event.Completion.JobControlRevision
			token = event.Completion.Token
			epoch = event.Completion.Epoch
		} else {
			job = event.Failure.JobID
			revision = event.Failure.AssignmentRevision
			jobRevision = event.Failure.JobControlRevision
			token = event.Failure.Task
			epoch = event.Failure.Epoch
		}
		assignment, ok := findAssignment(&work, job)
		if !ok || revision > assignment.Assignment.Revision || jobRevision > assignment.JobControlRevision || token.AssignmentRevision != revision || token.SpecificationHash != assignment.Topology.Digest() || !snapshotEpochAtOrBefore(epoch, work.Fence) {
			return errors.New("snapshot event authority cross-reference mismatch")
		}
		if revision == assignment.Assignment.Revision && !containsToken(assignment.Assignment, token) {
			return errors.New("snapshot event current assignment token mismatch")
		}
		if event.Completion != nil {
			stage, exists := findStage(assignment.Topology, event.Completion.Source.StageID)
			if !exists || stage.Role != model.StageSource {
				return errors.New("snapshot completion source is not a source stage")
			}
		}
		if next == math.MaxUint64 {
			return errors.New("snapshot event transaction sequence overflows")
		}
		next++
	}
	if next != work.NextTransactionID {
		return errors.New("snapshot event sequence does not reach next transaction")
	}
	return nil
}

func validateSnapshotWorkLocal(work RecoveredWork, nodeID uint16, workerEpoch model.WorkerEpoch) error {
	for _, cursor := range work.Sources {
		assignment, ok := findAssignment(&work, cursor.Source.JobID)
		token, tokenOK := findToken(assignment.Assignment, cursor.Source)
		if !ok || !tokenOK || token.WorkerID != nodeID || token.WorkerEpoch != workerEpoch {
			return errors.New("snapshot source cursor is not current local custody")
		}
	}
	for _, observation := range work.Checkpoints {
		assignment, ok := findAssignment(&work, observation.Notice.JobID)
		if !ok || !assignmentTargetsWorker(assignment.Assignment, nodeID, workerEpoch) {
			return errors.New("snapshot checkpoint observation is not local assignment participation")
		}
	}
	for _, delivery := range work.Deliveries {
		if delivery.Destination.WorkerID != nodeID || delivery.Destination.WorkerEpoch != workerEpoch {
			return errors.New("snapshot delivery targets another worker incarnation")
		}
	}
	for _, outbox := range work.Outboxes {
		if outbox.Producer.WorkerID != nodeID || outbox.Producer.WorkerEpoch != workerEpoch {
			return errors.New("snapshot outbox producer is not this worker incarnation")
		}
	}
	var resultTargetErr error
	visitResults(work, func(result StoredResult) bool {
		if !provenanceTargets(result.Provenance, nodeID, workerEpoch) {
			resultTargetErr = errors.New("snapshot result provenance targets another worker incarnation")
			return false
		}
		return true
	})
	if resultTargetErr != nil {
		return resultTargetErr
	}
	for _, event := range work.PendingEvents {
		if event.WorkerID != nodeID || event.WorkerEpoch != workerEpoch {
			return errors.New("snapshot event targets another worker incarnation")
		}
	}
	for _, repair := range work.Repairs {
		if !repairTargets(repair, nodeID, workerEpoch) {
			return errors.New("snapshot repair role targets another worker incarnation")
		}
	}
	return nil
}

func jobIDLess(left, right model.JobID) bool { return bytes.Compare(left[:], right[:]) < 0 }

func validateSnapshotDelivery(record DeliveryRecord, assignment InstalledAssignment, fence model.CoordinatorEpoch) error {
	if record.State < Received || record.State > Compacted || record.AssignmentRevision == 0 || record.AssignmentDigest == ([32]byte{}) || record.AssignmentRevision > assignment.Assignment.Revision || !snapshotEpochAtOrBefore(record.CoordinatorEpoch, fence) {
		return errors.New("invalid snapshot delivery metadata")
	}
	if record.Producer.Validate() != nil || record.Destination.Validate() != nil || record.Producer.Task.JobID != assignment.Assignment.JobID || record.Destination.Task != record.ID.DestinationTask || record.Producer.AssignmentRevision != record.AssignmentRevision || record.Destination.AssignmentRevision != record.AssignmentRevision || record.Producer.SpecificationHash != assignment.Topology.Digest() || record.Destination.SpecificationHash != assignment.Topology.Digest() {
		return errors.New("invalid snapshot delivery tokens")
	}
	if _, err := deliveryDefinitionDigest(record); err != nil {
		return err
	}
	wantReservation, err := assignment.Topology.WorstCaseCustodyBytes(record.Destination.Task)
	if err != nil || record.Reservation != wantReservation {
		return errors.New("snapshot delivery reservation mismatch")
	}
	if record.AssignmentRevision == assignment.Assignment.Revision {
		if record.AssignmentDigest != assignment.Assignment.Digest {
			return errors.New("snapshot delivery current assignment digest mismatch")
		}
		return validateDelivery(record, assignment, record.CoordinatorEpoch)
	}
	// The assignment history is intentionally not retained. Re-derive the
	// complete logical path using the immutable topology/current task universe,
	// then compare the historical tokens by task and self-fenced revision.
	derived, exists, err := deriveDeliveryDefinition(assignment, assignment.CoordinatorEpoch, record.ID)
	if err != nil || !exists || derived.ID != record.ID || !equalTuples([]model.Tuple{derived.Tuple}, []model.Tuple{record.Tuple}) || derived.Producer.Task != record.Producer.Task || derived.Destination.Task != record.Destination.Task || derived.Reservation != record.Reservation {
		return errors.New("snapshot delivery historical logical path mismatch")
	}
	return nil
}

func validateSnapshotOutbox(record OutboxRecord, assignment InstalledAssignment, fence model.CoordinatorEpoch) error {
	if record.AssignmentRevision == 0 || record.AssignmentRevision > assignment.Assignment.Revision || record.AssignmentDigest == ([32]byte{}) || !snapshotEpochAtOrBefore(record.CoordinatorEpoch, fence) || record.Accepted && record.RetryDeadlineUnixNano == 0 {
		return errors.New("invalid snapshot outbox authority")
	}
	if record.AssignmentRevision == assignment.Assignment.Revision {
		return validateOutbox(record, assignment, record.CoordinatorEpoch)
	}
	if record.Producer.Validate() != nil || record.Destination.Validate() != nil || record.Producer.AssignmentRevision != record.AssignmentRevision || record.Destination.AssignmentRevision != record.AssignmentRevision || record.Producer.SpecificationHash != assignment.Topology.Digest() || record.Destination.SpecificationHash != assignment.Topology.Digest() || record.Destination.Task != record.ID.DestinationTask {
		return errors.New("invalid historical snapshot outbox")
	}
	delivery := DeliveryRecord{ID: record.ID, Tuple: record.Tuple, Producer: record.Producer, Destination: record.Destination, AssignmentRevision: record.AssignmentRevision, AssignmentDigest: record.AssignmentDigest, CoordinatorEpoch: record.CoordinatorEpoch, State: Received}
	if _, err := deliveryDefinitionDigest(delivery); err != nil {
		return err
	}
	return validateRoute(assignment.Topology, record.ID, record.Tuple, record.Producer.Task)
}

func expectedSnapshotProcessedOutboxes(delivery DeliveryRecord, assignment InstalledAssignment) (map[model.DeliveryID]OutboxRecord, error) {
	result := make(map[model.DeliveryID]OutboxRecord)
	for outputIndex, tuple := range delivery.Outputs {
		if outputIndex > math.MaxUint16 {
			return nil, errors.New("snapshot output ordinal exceeds v1 identity")
		}
		for _, edge := range assignment.Topology.Spec().Edges {
			if edge.SourceStageID != delivery.Destination.Task.StageID {
				continue
			}
			child := model.DeriveChildTupleID(delivery.ID.Tuple, delivery.Destination.Task, edge.EdgeID, uint16(outputIndex))
			partitions, err := model.Route(assignment.Topology, edge, child, tuple)
			if err != nil {
				return nil, err
			}
			for _, partition := range partitions {
				task := model.TaskID{JobID: delivery.ID.Tuple.JobID, StageID: edge.DestinationStageID, Partition: partition}
				id := model.DeliveryID{Tuple: child, EdgeID: edge.EdgeID, DestinationTask: task}
				if _, duplicate := result[id]; duplicate {
					return nil, errors.New("snapshot topology derived duplicate outbox")
				}
				result[id] = OutboxRecord{ID: id, Tuple: tuple, Producer: delivery.Destination, AssignmentRevision: delivery.AssignmentRevision, AssignmentDigest: delivery.AssignmentDigest, CoordinatorEpoch: delivery.CoordinatorEpoch}
			}
		}
	}
	if uint64(len(result)) > model.LimitsV1().MaxDerivedDeliveries {
		return nil, errors.New("snapshot topology-derived outboxes exceed v1 bound")
	}
	return result, nil
}

func equalSnapshotDerivedOutbox(expected, actual OutboxRecord) bool {
	return expected.ID == actual.ID && expected.Producer == actual.Producer && expected.AssignmentRevision == actual.AssignmentRevision && expected.AssignmentDigest == actual.AssignmentDigest && expected.CoordinatorEpoch == actual.CoordinatorEpoch && actual.Destination.Task == actual.ID.DestinationTask && equalTuples([]model.Tuple{expected.Tuple}, []model.Tuple{actual.Tuple})
}

func deliveryIndexForOutboxCompleted(deliveries []DeliveryRecord, id model.DeliveryID) bool {
	for _, delivery := range deliveries {
		if delivery.State != Completed {
			continue
		}
		for _, outboxID := range delivery.OutboxIDs {
			if outboxID == id {
				return true
			}
		}
	}
	return false
}

func encodeCurrentGeneration(current currentGeneration) ([]byte, error) {
	if current.Identity.Validate() != nil || current.WorkerEpoch.Validate() != nil || current.Generation == 0 || !validSnapshotTransactionMetadata(current.BaseSequence, current.TransactionCount) || current.SnapshotBytes < snapshotHeaderBytes+snapshotFooterBytes || current.SnapshotDigest == ([32]byte{}) {
		return nil, fmt.Errorf("%w: invalid current generation", ErrInvalidTransaction)
	}
	data := make([]byte, currentFileBytes)
	copy(data[:4], currentMagic[:])
	binary.BigEndian.PutUint16(data[4:6], currentSchemaVersion)
	binary.BigEndian.PutUint16(data[6:8], currentFileBytes)
	copy(data[8:24], current.Identity.ClusterID[:])
	binary.BigEndian.PutUint16(data[24:26], current.Identity.NodeID)
	copy(data[26:42], current.WorkerEpoch[:])
	binary.BigEndian.PutUint64(data[42:50], current.Generation)
	binary.BigEndian.PutUint64(data[50:58], current.BaseSequence)
	binary.BigEndian.PutUint64(data[58:66], current.TransactionCount)
	binary.BigEndian.PutUint64(data[66:74], current.SnapshotBytes)
	copy(data[74:106], current.SnapshotDigest[:])
	binary.BigEndian.PutUint32(data[106:110], crc32.Checksum(data[:106], walCRC))
	return data, nil
}

func decodeCurrentGeneration(data []byte, expected Identity) (currentGeneration, error) {
	if len(data) != currentFileBytes || !bytes.Equal(data[:4], currentMagic[:]) || binary.BigEndian.Uint16(data[4:6]) != currentSchemaVersion || binary.BigEndian.Uint16(data[6:8]) != currentFileBytes || crc32.Checksum(data[:106], walCRC) != binary.BigEndian.Uint32(data[106:110]) {
		return currentGeneration{}, fmt.Errorf("%w: current marker schema/checksum", ErrCorrupt)
	}
	var current currentGeneration
	copy(current.Identity.ClusterID[:], data[8:24])
	current.Identity.NodeID = binary.BigEndian.Uint16(data[24:26])
	copy(current.WorkerEpoch[:], data[26:42])
	current.Generation = binary.BigEndian.Uint64(data[42:50])
	current.BaseSequence = binary.BigEndian.Uint64(data[50:58])
	current.TransactionCount = binary.BigEndian.Uint64(data[58:66])
	current.SnapshotBytes = binary.BigEndian.Uint64(data[66:74])
	copy(current.SnapshotDigest[:], data[74:106])
	if current.Identity != expected {
		return currentGeneration{}, fmt.Errorf("%w: current marker identity", ErrIdentityMismatch)
	}
	if current.WorkerEpoch.Validate() != nil || current.Generation == 0 || !validSnapshotTransactionMetadata(current.BaseSequence, current.TransactionCount) || current.SnapshotBytes < snapshotHeaderBytes+snapshotFooterBytes || current.SnapshotDigest == ([32]byte{}) {
		return currentGeneration{}, fmt.Errorf("%w: invalid current marker", ErrCorrupt)
	}
	return current, nil
}

func validSnapshotTransactionMetadata(baseSequence, transactionCount uint64) bool {
	if transactionCount == 0 {
		return baseSequence == 1
	}
	if transactionCount > (math.MaxUint64-1)/3 {
		return false
	}
	minimum := 1 + 3*transactionCount
	if baseSequence < minimum {
		return false
	}
	maximumRecords := uint64(MaxTransactionRecords + 2)
	if transactionCount <= (math.MaxUint64-1)/maximumRecords && baseSequence > 1+maximumRecords*transactionCount {
		return false
	}
	return true
}

func snapshotDomainRecordCount(baseSequence, transactionCount uint64) (uint64, bool) {
	if !validSnapshotTransactionMetadata(baseSequence, transactionCount) || transactionCount > math.MaxUint64/2 {
		return 0, false
	}
	overhead := 2 * transactionCount
	if baseSequence == 0 || baseSequence-1 < overhead {
		return 0, false
	}
	return baseSequence - 1 - overhead, true
}

func validSnapshotEventMetadata(nextTransactionID, baseSequence, transactionCount uint64) bool {
	domainRecords, ok := snapshotDomainRecordCount(baseSequence, transactionCount)
	return ok && nextTransactionID != 0 && nextTransactionID-1 <= domainRecords
}

func (store *Store) recoverExisting(identity Identity) error {
	_, markerErr := store.root.Lstat(currentFilename)
	if errors.Is(markerErr, os.ErrNotExist) {
		return store.recoverLegacy(identity)
	}
	if markerErr != nil {
		return fmt.Errorf("%w: stat current marker: %v", ErrCorrupt, markerErr)
	}
	marker, err := openWAL(store.root, currentFilename, false)
	if err != nil {
		return fmt.Errorf("%w: open current marker: %v", ErrCorrupt, err)
	}
	markerInfo, statErr := marker.Stat()
	if statErr != nil || markerInfo.Size() != currentFileBytes {
		_ = marker.Close()
		return fmt.Errorf("%w: current marker size", ErrCorrupt)
	}
	markerBytes := make([]byte, currentFileBytes)
	readErr := readAtFull(marker, markerBytes, 0)
	closeErr := marker.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return fmt.Errorf("%w: read current marker: %v", ErrCorrupt, err)
	}
	current, err := decodeCurrentGeneration(markerBytes, identity)
	if err != nil {
		return err
	}
	snapshotFile, err := openWAL(store.root, snapshotFilename(current.Generation), false)
	if err != nil {
		return fmt.Errorf("%w: open active snapshot: %v", ErrCorrupt, err)
	}
	snapshotInfo, err := snapshotFile.Stat()
	if err != nil || snapshotInfo.Size() <= 0 || uint64(snapshotInfo.Size()) != current.SnapshotBytes || uint64(snapshotInfo.Size()) > store.options.MaxBytes {
		_ = snapshotFile.Close()
		return fmt.Errorf("%w: active snapshot size", ErrCorrupt)
	}
	work, snapshot, recoverErr := recoverSnapshotReader(snapshotFile, snapshotInfo.Size(), identity, current, store.options.MaxBytes)
	closeErr = snapshotFile.Close()
	if err := errors.Join(recoverErr, closeErr); err != nil {
		if errors.Is(err, ErrIdentityMismatch) {
			return err
		}
		return fmt.Errorf("%w: recover snapshot: %v", ErrCorrupt, err)
	}
	wal, err := openWAL(store.root, generationWALFilename(current.Generation), false)
	if err != nil {
		return fmt.Errorf("%w: open active generation WAL: %v", ErrCorrupt, err)
	}
	store.wal = wal
	walInfo, err := wal.Stat()
	if err != nil || walInfo.Size() <= 0 || uint64(walInfo.Size()) > store.options.MaxBytes || uint64(walInfo.Size()) > uint64(math.MaxInt) {
		return fmt.Errorf("%w: active generation WAL size", ErrCorrupt)
	}
	anchor := walSnapshotAnchor{Identity: identity, WorkerEpoch: current.WorkerEpoch, Generation: current.Generation, BaseSequence: current.BaseSequence, TransactionCount: current.TransactionCount, SnapshotDigest: current.SnapshotDigest}
	reducer := &workReducer{current: work, allowLegacy: true}
	state, truncateAt, err := recoverSnapshotWALReader(wal, walInfo.Size(), identity, anchor, reducer)
	if err != nil {
		if errors.Is(err, ErrIdentityMismatch) {
			return err
		}
		return fmt.Errorf("%w: recover generation WAL: %v", ErrCorrupt, err)
	}
	if truncateAt != walInfo.Size() {
		if err := wal.Truncate(truncateAt); err != nil {
			return err
		}
		if err := store.operations.syncFile(wal); err != nil {
			return err
		}
	}
	if _, err := wal.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	state.SnapshotGeneration = snapshot.Generation
	state.SnapshotBytes = snapshot.Bytes
	if err := validateSnapshotWork(reducer.current, state.Identity.NodeID, state.WorkerEpoch); err != nil {
		return fmt.Errorf("%w: recovered snapshot/WAL state: %v", ErrCorrupt, err)
	}
	if err := validateRecoveredCapacity(state, reducer.current, store.options.MaxBytes); err != nil {
		return err
	}
	store.state, store.work = state, reducer.current
	removed, err := store.cleanupObsolete(current.Generation)
	if err == nil && removed {
		err = store.operations.syncFile(store.directory)
	}
	return err
}

func (store *Store) recoverLegacy(identity Identity) error {
	wal, err := openWAL(store.root, WorkerWALFilename, false)
	if err != nil {
		return fmt.Errorf("%w: open existing WAL: %v", ErrCorrupt, err)
	}
	store.wal = wal
	info, err := wal.Stat()
	if err != nil {
		return err
	}
	if info.Size() <= 0 || uint64(info.Size()) > store.options.MaxBytes || uint64(info.Size()) > uint64(math.MaxInt) {
		return fmt.Errorf("%w: WAL size %d", ErrCorrupt, info.Size())
	}
	reducer := newRecoveryWorkReducer()
	state, truncateAt, err := recoverWALReader(wal, info.Size(), identity, reducer)
	if err != nil {
		if !errors.Is(err, ErrCorrupt) && !errors.Is(err, ErrIdentityMismatch) {
			return fmt.Errorf("%w: %v", ErrCorrupt, err)
		}
		return err
	}
	if truncateAt != info.Size() {
		if err := wal.Truncate(truncateAt); err != nil {
			return err
		}
		if err := store.operations.syncFile(wal); err != nil {
			return err
		}
	}
	if _, err := wal.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	if err := validateRecoveredWorkLocal(reducer.current, state.Identity.NodeID, state.WorkerEpoch); err != nil {
		return fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	if err := validateRecoveredCapacity(state, reducer.current, store.options.MaxBytes); err != nil {
		return err
	}
	store.state, store.work = state, reducer.current
	removed, err := store.cleanupObsolete(0)
	if err == nil && removed {
		err = store.operations.syncFile(store.directory)
	}
	return err
}

func validateRecoveredCapacity(state RecoveredState, work RecoveredWork, maxBytes uint64) error {
	used, ok := checkedAdd(state.SnapshotBytes, state.WALBytes)
	if !ok || used > maxBytes {
		return fmt.Errorf("%w: recovered durable bytes exceed configured capacity", ErrCorrupt)
	}
	reserved, err := reservedBytes(work)
	if err != nil || reserved > maxBytes-used {
		return fmt.Errorf("%w: recovered reservation exceeds configured capacity", ErrCorrupt)
	}
	return nil
}

// Snapshot crash-atomically replaces WAL history with one complete state image.
func (store *Store) Snapshot() (Snapshot, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return Snapshot{}, ErrClosed
	}
	if store.failed {
		return Snapshot{}, ErrUnavailable
	}
	if err := validateSnapshotWork(store.work, store.state.Identity.NodeID, store.state.WorkerEpoch); err != nil {
		return Snapshot{}, err
	}
	if store.state.SnapshotGeneration == math.MaxUint64 {
		return Snapshot{}, fmt.Errorf("%w: snapshot generation overflow", ErrCapacity)
	}
	generation := store.state.SnapshotGeneration + 1
	metadata, _, err := snapshotMetadata(store.state, store.work, generation)
	if err != nil {
		return Snapshot{}, err
	}
	placeholder := walSnapshotAnchor{Identity: store.state.Identity, WorkerEpoch: store.state.WorkerEpoch, Generation: generation, BaseSequence: store.state.LastSequence, TransactionCount: store.state.TransactionCount, SnapshotDigest: [32]byte{1}}
	anchorBytes, err := encodeSnapshotAnchor(placeholder)
	if err != nil {
		return Snapshot{}, err
	}
	used, ok := checkedAdd(metadata.Bytes, uint64(len(anchorBytes)))
	reserved, reserveErr := reservedBytes(store.work)
	if !ok || reserveErr != nil || used > store.options.MaxBytes || reserved > store.options.MaxBytes-used {
		return Snapshot{}, ErrCapacity
	}
	snapshot, err := store.replaceWithSnapshot(generation)
	if err != nil {
		store.failed = true
		return Snapshot{}, err
	}
	return snapshot, nil
}

func (store *Store) replaceWithSnapshot(generation uint64) (Snapshot, error) {
	if err := store.inject(FaultSnapshotTempCreate); err != nil {
		return Snapshot{}, err
	}
	snapshotTemp := snapshotTempFilename(generation)
	snapshotFile, err := openWAL(store.root, snapshotTemp, true)
	if err != nil {
		return Snapshot{}, err
	}
	if err := store.inject(FaultSnapshotTempWrite); err != nil {
		_ = snapshotFile.Close()
		return Snapshot{}, err
	}
	metadata, digest, writeErr := writeSnapshotFile(snapshotFile, store.state, store.work, generation, store.operations.writeFile)
	if writeErr == nil {
		writeErr = store.inject(FaultSnapshotTempSync)
	}
	if writeErr == nil {
		writeErr = store.operations.syncFile(snapshotFile)
	}
	closeErr := snapshotFile.Close()
	closeErr = errors.Join(closeErr, store.inject(FaultSnapshotTempClose))
	if err := errors.Join(writeErr, closeErr); err != nil {
		return Snapshot{}, err
	}
	if err := store.inject(FaultSnapshotRename); err != nil {
		return Snapshot{}, err
	}
	if err := store.root.Rename(snapshotTemp, snapshotFilename(generation)); err != nil {
		return Snapshot{}, err
	}
	if err := store.inject(FaultSnapshotDirectorySync); err != nil {
		return Snapshot{}, err
	}
	if err := store.operations.syncFile(store.directory); err != nil {
		return Snapshot{}, err
	}

	anchor := walSnapshotAnchor{Identity: store.state.Identity, WorkerEpoch: store.state.WorkerEpoch, Generation: generation, BaseSequence: store.state.LastSequence, TransactionCount: store.state.TransactionCount, SnapshotDigest: digest}
	anchorBytes, err := encodeSnapshotAnchor(anchor)
	if err != nil {
		return Snapshot{}, err
	}
	if err := store.inject(FaultReplacementWALCreate); err != nil {
		return Snapshot{}, err
	}
	walTemp := generationWALTempFilename(generation)
	replacement, err := openWAL(store.root, walTemp, true)
	if err != nil {
		return Snapshot{}, err
	}
	err = store.inject(FaultReplacementWALWrite)
	if err == nil {
		err = writeFullWith(replacement, anchorBytes, store.operations.writeFile)
	}
	if err == nil {
		err = store.inject(FaultReplacementWALSync)
	}
	if err == nil {
		err = store.operations.syncFile(replacement)
	}
	closeErr = replacement.Close()
	closeErr = errors.Join(closeErr, store.inject(FaultReplacementWALClose))
	if err = errors.Join(err, closeErr); err != nil {
		return Snapshot{}, err
	}
	if err := store.inject(FaultReplacementWALRename); err != nil {
		return Snapshot{}, err
	}
	if err := store.root.Rename(walTemp, generationWALFilename(generation)); err != nil {
		return Snapshot{}, err
	}
	if err := store.inject(FaultReplacementWALDirectorySync); err != nil {
		return Snapshot{}, err
	}
	if err := store.operations.syncFile(store.directory); err != nil {
		return Snapshot{}, err
	}
	newWAL, err := openWAL(store.root, generationWALFilename(generation), false)
	if err != nil {
		return Snapshot{}, err
	}
	if _, err := newWAL.Seek(0, io.SeekEnd); err != nil {
		_ = newWAL.Close()
		return Snapshot{}, err
	}

	current := currentGeneration{Identity: store.state.Identity, WorkerEpoch: store.state.WorkerEpoch, Generation: generation, BaseSequence: store.state.LastSequence, TransactionCount: store.state.TransactionCount, SnapshotBytes: metadata.Bytes, SnapshotDigest: digest}
	currentBytes, err := encodeCurrentGeneration(current)
	if err != nil {
		_ = newWAL.Close()
		return Snapshot{}, err
	}
	if err := store.inject(FaultCurrentTempCreate); err != nil {
		_ = newWAL.Close()
		return Snapshot{}, err
	}
	currentTemp, err := openWAL(store.root, currentTempFilename, true)
	if err != nil {
		_ = newWAL.Close()
		return Snapshot{}, err
	}
	err = store.inject(FaultCurrentTempWrite)
	if err == nil {
		err = writeFullWith(currentTemp, currentBytes, store.operations.writeFile)
	}
	if err == nil {
		err = store.inject(FaultCurrentTempSync)
	}
	if err == nil {
		err = store.operations.syncFile(currentTemp)
	}
	closeErr = currentTemp.Close()
	closeErr = errors.Join(closeErr, store.inject(FaultCurrentTempClose))
	if err = errors.Join(err, closeErr); err != nil {
		_ = newWAL.Close()
		return Snapshot{}, err
	}
	if err := store.inject(FaultCurrentRename); err != nil {
		_ = newWAL.Close()
		return Snapshot{}, err
	}
	if err := store.root.Rename(currentTempFilename, currentFilename); err != nil {
		_ = newWAL.Close()
		return Snapshot{}, err
	}
	if err := store.inject(FaultCurrentDirectorySync); err != nil {
		_ = newWAL.Close()
		return Snapshot{}, err
	}
	if err := store.operations.syncFile(store.directory); err != nil {
		_ = newWAL.Close()
		return Snapshot{}, err
	}

	previous := store.wal
	store.wal = newWAL
	store.state.SnapshotGeneration = generation
	store.state.SnapshotBytes = metadata.Bytes
	store.state.WALBytes = uint64(len(anchorBytes))
	closeErr = previous.Close()
	closeErr = errors.Join(closeErr, store.inject(FaultPreviousWALClose))
	if closeErr != nil {
		return Snapshot{}, closeErr
	}
	if err := store.inject(FaultObsoleteCleanup); err != nil {
		return Snapshot{}, err
	}
	if _, err := store.cleanupObsolete(generation); err != nil {
		return Snapshot{}, err
	}
	if err := store.inject(FaultObsoleteDirectorySync); err != nil {
		return Snapshot{}, err
	}
	if err := store.operations.syncFile(store.directory); err != nil {
		return Snapshot{}, err
	}
	return metadata, nil
}

func (store *Store) cleanupObsolete(activeGeneration uint64) (bool, error) {
	directory, err := store.root.Open(".")
	if err != nil {
		return false, err
	}
	names, readErr := directory.Readdirnames(-1)
	closeErr := directory.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return false, err
	}
	removed := false
	for _, name := range names {
		keep := name == WorkerLockFilename
		if activeGeneration == 0 {
			keep = keep || name == WorkerWALFilename
		} else {
			keep = keep || name == currentFilename || name == snapshotFilename(activeGeneration) || name == generationWALFilename(activeGeneration)
		}
		if keep {
			continue
		}
		if !recognizedStoreFilename(name) {
			return false, fmt.Errorf("%w: unexpected worker file %q", ErrCorrupt, name)
		}
		info, err := store.root.Lstat(name)
		if err != nil || validateOwnedRegular(info) != nil {
			return false, fmt.Errorf("%w: unsafe obsolete worker file %q", ErrCorrupt, name)
		}
		if err := store.root.Remove(name); err != nil {
			return false, err
		}
		removed = true
	}
	return removed, nil
}
