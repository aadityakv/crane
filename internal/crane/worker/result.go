package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
	"github.com/aaditya/cs425mp3/internal/crane/store"
)

type ownedResult struct {
	record     model.ResultRecord
	provenance model.ResultCopyProvenance
	parents    []model.DeliveryID
	sending    bool
}

type resultJob struct {
	key        resultIdentity
	record     model.ResultRecord
	provenance model.ResultCopyProvenance
}

type resultResponse struct {
	key     resultIdentity
	receipt ResultReplicationReceipt
	err     error
}

type resultIdentity struct {
	sink  model.TaskID
	tuple model.TupleID
}

func resultID(record model.ResultRecord) resultIdentity {
	return resultIdentity{sink: record.SinkTask, tuple: record.TupleID}
}

func (engine *Engine) resultWorker(ctx context.Context) {
	defer engine.workers.Done()
	for job := range engine.resultJobs {
		receipt, err := engine.replicator.ReplicateRecord(ctx, cloneResultRecord(job.record), job.provenance)
		select {
		case engine.resultResponses <- resultResponse{key: job.key, receipt: receipt, err: err}:
		case <-ctx.Done():
		}
	}
}

func (engine *Engine) reconcileResults(ctx context.Context) error {
	for _, result := range engine.results {
		result.parents = result.parents[:0]
	}
	deliveryIDs := make([]model.DeliveryID, 0, len(engine.deliveries))
	for id := range engine.deliveries {
		deliveryIDs = append(deliveryIDs, id)
	}
	sort.Slice(deliveryIDs, func(i, j int) bool { return deliveryIDLess(deliveryIDs[i], deliveryIDs[j]) })
	for _, id := range deliveryIDs {
		delivery := engine.deliveries[id]
		if delivery.State != store.Processed {
			continue
		}
		assignment, ok := engine.repository.InstalledAssignment(id.Tuple.JobID)
		if !ok {
			return errors.New("processed sink references missing assignment")
		}
		stage, ok := findStage(assignment.Topology, delivery.Destination.Task.StageID)
		if !ok || stage.Role != model.StageSink {
			continue
		}
		if !engine.currentSinkAuthority(assignment, delivery) {
			// A retained old-revision delivery/result may only move under the
			// bilateral repair grant owned by Tasks 16/17/19.
			continue
		}
		if len(delivery.Outputs) != 1 {
			return errors.New("processed sink does not contain one canonical output")
		}
		encoded, err := model.MarshalTuple(delivery.Outputs[0])
		if err != nil {
			return err
		}
		record, err := model.NewResultRecord(delivery.ID.Tuple, delivery.Destination.Task, delivery.Destination.SpecificationHash, encoded)
		if err != nil {
			return err
		}
		replica, ok := findResultReplica(assignment.Assignment, record.SinkTask)
		if !ok || replica.PrimaryNodeID != engine.localNode || replica.PrimaryEpoch != engine.localEpoch || replica.SecondaryNodeID == engine.localNode {
			return errors.New("sink task is not the exact local primary result replica")
		}
		provenance := model.ResultCopyProvenance{AssignmentRevision: assignment.Assignment.Revision, AssignmentDigest: assignment.Assignment.Digest, ReplicaSet: replica, DestinationRole: model.PrimaryReplica, CoordinatorEpoch: assignment.CoordinatorEpoch}
		key := resultIdentity{sink: delivery.Destination.Task, tuple: delivery.ID.Tuple}
		owned, exists := engine.results[key]
		if !exists {
			if err := engine.repository.UpsertResult(record, provenance); err != nil {
				return engine.ownerError("persist primary result", err)
			}
			owned = &ownedResult{record: cloneResultRecord(record), provenance: provenance}
			engine.results[key] = owned
		} else if !equalOwnedResult(owned, record, provenance) {
			return model.ErrIdentityReuse
		}
		owned.parents = append(owned.parents, id)
	}

	type scheduledResult struct {
		key    resultIdentity
		parent model.DeliveryID
	}
	ordered := make([]scheduledResult, 0, len(engine.results))
	for key, result := range engine.results {
		if len(result.parents) > 0 {
			ordered = append(ordered, scheduledResult{key: key, parent: result.parents[0]})
		}
	}
	sort.Slice(ordered, func(i, j int) bool { return deliveryIDLess(ordered[i].parent, ordered[j].parent) })
	for _, candidate := range ordered {
		key, result := candidate.key, engine.results[candidate.key]
		if result.sending || len(result.parents) == 0 {
			continue
		}
		parent, ok := engine.deliveries[result.parents[0]]
		if !ok || parent.State != store.Processed {
			continue
		}
		assignment, ok := engine.repository.InstalledAssignment(result.record.TupleID.JobID)
		if !ok || !engine.currentResultProvenance(assignment, result) {
			// Historical bytes are permanent, but normal replication is
			// deliberately fail-closed without a bilateral repair grant.
			continue
		}
		target := result.provenance
		target.DestinationRole = model.SecondaryReplica
		select {
		case engine.resultJobs <- resultJob{key: key, record: cloneResultRecord(result.record), provenance: target}:
			result.sending = true
		default:
			return nil
		}
	}
	return nil
}

func (engine *Engine) handleResultResponse(response resultResponse) error {
	result, ok := engine.results[response.key]
	if !ok {
		return errors.New("result replication completed for unknown result")
	}
	result.sending = false
	if errors.Is(response.err, context.Canceled) {
		return nil
	}
	if response.err != nil {
		return engine.ownerError("replicate result", response.err)
	}
	assignment, ok := engine.repository.InstalledAssignment(result.record.TupleID.JobID)
	if !ok || !engine.currentResultProvenance(assignment, result) {
		// Assignment/fence closure may race an in-flight durable ACK. Retain the
		// primary bytes and Processed parent; a later authorized reconcile or
		// bilateral repair decides the next action without killing the owner.
		return nil
	}
	encoded, err := model.MarshalResultRecord(result.record)
	if err != nil {
		return err
	}
	replica := result.provenance.ReplicaSet
	if response.receipt.DestinationNodeID != replica.SecondaryNodeID || response.receipt.DestinationWorkerEpoch != replica.SecondaryEpoch || response.receipt.StreamChecksum != model.ResultRecordStreamChecksum(result.record) || response.receipt.StreamLength != uint64(len(encoded)) || response.receipt.CoordinatorEpoch != assignment.CoordinatorEpoch {
		return errors.New("result replication receipt does not bind exact durable secondary copy")
	}
	parents := append([]model.DeliveryID(nil), result.parents...)
	for _, parentID := range parents {
		parent, exists := engine.deliveries[parentID]
		if !exists || parent.State != store.Processed || !engine.currentSinkAuthority(assignment, parent) {
			// An authority change between dispatch and receipt retains every
			// parent for later authorized reconciliation.
			return nil
		}
	}
	for _, parentID := range parents {
		if err := engine.repository.MarkCompleted(parentID); err != nil {
			return engine.ownerError("complete replicated sink delivery", err)
		}
		parent := engine.deliveries[parentID]
		parent.State = store.Completed
		engine.deliveries[parentID] = parent
	}
	return nil
}

func (engine *Engine) currentSinkAuthority(assignment store.InstalledAssignment, delivery store.DeliveryRecord) bool {
	return assignment.SchedulingState == model.Running && assignment.CoordinatorEpoch == engine.repository.CurrentFence() && delivery.CoordinatorEpoch == assignment.CoordinatorEpoch && delivery.AssignmentRevision == assignment.Assignment.Revision && delivery.AssignmentDigest == assignment.Assignment.Digest && delivery.Destination.WorkerID == engine.localNode && delivery.Destination.WorkerEpoch == engine.localEpoch && containsAssignmentToken(assignment.Assignment, delivery.Destination)
}

func (engine *Engine) currentResultProvenance(assignment store.InstalledAssignment, result *ownedResult) bool {
	replica, ok := findResultReplica(assignment.Assignment, result.record.SinkTask)
	return ok && assignment.SchedulingState == model.Running && assignment.CoordinatorEpoch == engine.repository.CurrentFence() && result.provenance.AssignmentRevision == assignment.Assignment.Revision && result.provenance.AssignmentDigest == assignment.Assignment.Digest && result.provenance.CoordinatorEpoch == assignment.CoordinatorEpoch && result.provenance.ReplicaSet == replica && result.provenance.DestinationRole == model.PrimaryReplica && replica.PrimaryNodeID == engine.localNode && replica.PrimaryEpoch == engine.localEpoch && replica.SecondaryNodeID != engine.localNode
}

func findResultReplica(set model.AssignmentSet, task model.TaskID) (model.ResultReplicaSet, bool) {
	for _, replica := range set.ResultReplicas {
		if replica.SinkTask == task {
			return replica, true
		}
	}
	return model.ResultReplicaSet{}, false
}

func cloneResultRecord(record model.ResultRecord) model.ResultRecord {
	record.Value = append([]byte(nil), record.Value...)
	return record
}

func equalOwnedResult(owned *ownedResult, record model.ResultRecord, provenance model.ResultCopyProvenance) bool {
	return owned != nil && owned.record.TupleID == record.TupleID && owned.record.SinkTask == record.SinkTask && owned.record.SpecificationHash == record.SpecificationHash && owned.record.Checksum == record.Checksum && bytes.Equal(owned.record.Value, record.Value) && owned.provenance == provenance
}

// SealResultPartition derives the canonical sealed artifact identity and
// complete canonical stream for one sink partition's covered records. The
// stream is the TupleID-sorted concatenation of u32-length-prefixed canonical
// result records, so its SHA-256 authenticates the exact logical record set
// independent of any copy provenance.
func SealResultPartition(job model.JobID, sink model.TaskID, specificationHash [32]byte, records []model.ResultRecord) (protocol.ResultArtifact, []byte, error) {
	sorted := append([]model.ResultRecord(nil), records...)
	sort.Slice(sorted, func(i, j int) bool { return tupleTransferLess(sorted[i].TupleID, sorted[j].TupleID) })
	stream := make([]byte, 0)
	for index, record := range sorted {
		if index > 0 && sorted[index-1].TupleID == record.TupleID {
			return protocol.ResultArtifact{}, nil, model.ErrIdentityReuse
		}
		if record.TupleID.JobID != job || record.SinkTask != sink {
			return protocol.ResultArtifact{}, nil, fmt.Errorf("record %d does not belong to the sealed partition", index)
		}
		if err := record.Validate(); err != nil {
			return protocol.ResultArtifact{}, nil, err
		}
		if record.SpecificationHash != specificationHash {
			return protocol.ResultArtifact{}, nil, fmt.Errorf("record %d carries a foreign specification hash", index)
		}
		entry, err := protocol.EncodedResultPageRecordBytes(record)
		if err != nil {
			return protocol.ResultArtifact{}, nil, err
		}
		if uint64(len(stream)) > ^uint64(0)-uint64(len(entry)) || uint64(len(stream))+uint64(len(entry)) > protocol.MaxTransferTotalBytes {
			return protocol.ResultArtifact{}, nil, errors.New("sealed artifact exceeds the protocol bound")
		}
		stream = append(stream, entry...)
	}
	artifact := protocol.ResultArtifact{
		JobID: job, SinkTask: sink, SpecificationHash: specificationHash,
		RecordCount: uint64(len(sorted)), TotalLength: uint64(len(stream)), Checksum: sha256.Sum256(stream),
	}
	if err := validateSealedArtifact(artifact); err != nil {
		return protocol.ResultArtifact{}, nil, err
	}
	return artifact, stream, nil
}

// validateSealedArtifact enforces the complete sealed-artifact identity and
// count/length bounds before any durable mutation or transfer.
func validateSealedArtifact(artifact protocol.ResultArtifact) error {
	if artifact.JobID.Validate() != nil || artifact.SinkTask.Validate() != nil || artifact.SinkTask.JobID != artifact.JobID ||
		artifact.SpecificationHash == ([32]byte{}) || artifact.TotalLength > protocol.MaxTransferTotalBytes || artifact.Checksum == ([32]byte{}) {
		return errors.New("invalid sealed artifact identity")
	}
	if artifact.TotalLength == 0 && (artifact.RecordCount != 0 || artifact.Checksum != sha256.Sum256(nil)) ||
		artifact.TotalLength > 0 && artifact.RecordCount == 0 {
		return errors.New("sealed artifact count/length mismatch")
	}
	if artifact.RecordCount > model.ResultArtifactMaxRecordCountV1 {
		return errors.New("sealed artifact record count exceeds the bound")
	}
	if artifact.RecordCount > 0 {
		if artifact.TotalLength < artifact.RecordCount*model.ResultArtifactMinRecordBytesV1 ||
			artifact.TotalLength > artifact.RecordCount*model.ResultArtifactMaxRecordBytesV1 {
			return errors.New("sealed artifact bytes cannot contain the declared records")
		}
	}
	return nil
}

// FinalCheckpointVector computes the complete committed checkpoint vector of
// one installed assignment when every source task has a durable final
// watermark proof (a local authoritative cursor or a committed checkpoint
// observation). It reports false whenever any source's finality cannot be
// proven, so sealing never proceeds on unproven coverage.
func FinalCheckpointVector(work store.RecoveredWork, assignment store.InstalledAssignment) ([]model.SourceCheckpoint, bool) {
	vector := make([]model.SourceCheckpoint, 0)
	for _, stage := range assignment.Topology.Spec().Stages {
		if stage.Role != model.StageSource {
			continue
		}
		for partition := uint16(0); partition < stage.Parallelism; partition++ {
			task := model.TaskID{JobID: assignment.Assignment.JobID, StageID: stage.StageID, Partition: partition}
			eof, err := model.SourceEOF(assignment.Topology, task)
			if err != nil {
				return nil, false
			}
			watermark, proven := finalSourceWatermark(work, assignment, task)
			if !proven || watermark != eof {
				return nil, false
			}
			vector = append(vector, model.SourceCheckpoint{Source: task, Watermark: watermark})
		}
	}
	if len(vector) == 0 {
		return nil, false
	}
	sort.Slice(vector, func(i, j int) bool { return compareTask(vector[i].Source, vector[j].Source) < 0 })
	return vector, true
}

// finalSourceWatermark proves one source task's committed watermark from
// durable local state. A local source cursor with exact assignment authority
// is authoritative; otherwise a matching committed-checkpoint observation
// proves the watermark. Without either, only the trivially committed zero
// watermark of an untouched source is provable; an unproven existing cursor
// proves nothing.
func finalSourceWatermark(work store.RecoveredWork, assignment store.InstalledAssignment, task model.TaskID) (uint64, bool) {
	cursorExists := false
	for _, cursor := range work.Sources {
		if cursor.Source != task {
			continue
		}
		cursorExists = true
		if cursor.Watermark == 0 && cursor.CheckpointRevision == 0 {
			return 0, true
		}
		authority := cursor.CheckpointAuthority
		if cursor.RaftIndex != 0 && authority.JobControlRevision == assignment.JobControlRevision &&
			authority.AssignmentRevision == assignment.Assignment.Revision && authority.AssignmentDigest == assignment.Assignment.Digest &&
			authority.CoordinatorEpoch == work.Fence {
			return cursor.Watermark, true
		}
	}
	for _, observation := range work.Checkpoints {
		if observation.Notice.Source == task && observation.Notice.JobID == assignment.Assignment.JobID && observation.Notice.Epoch == work.Fence &&
			observation.JobControlRevision == assignment.JobControlRevision && observation.AssignmentRevision == assignment.Assignment.Revision &&
			observation.AssignmentDigest == assignment.Assignment.Digest && observation.Notice.RaftIndex != 0 {
			return observation.Notice.Watermark, true
		}
	}
	if cursorExists {
		return 0, false
	}
	// Without any durable observation only the trivially committed zero
	// watermark of an untouched source can be proven; finality then demands a
	// zero EOF, which the caller checks.
	return 0, true
}

// SealingRecords selects the checkpoint-covered canonical record set of one
// sink partition from retained durable results. Retained historical copy
// provenance is accepted: the immutable logical record, not its envelope,
// defines the sealed artifact, which itself becomes the new authenticated
// envelope for any destination copy.
func SealingRecords(work store.RecoveredWork, assignment store.InstalledAssignment, sink model.TaskID, vector []model.SourceCheckpoint) ([]model.ResultRecord, error) {
	watermarks := make(map[model.TaskID]uint64, len(vector))
	for _, entry := range vector {
		watermarks[entry.Source] = entry.Watermark
	}
	records := make([]model.ResultRecord, 0)
	for _, stored := range work.Results {
		record := stored.Record
		if record.TupleID.JobID != assignment.Assignment.JobID || record.SinkTask != sink || record.SpecificationHash != assignment.Topology.Digest() {
			continue
		}
		watermark, covered := watermarks[record.TupleID.SourceTask]
		if !covered || record.TupleID.SourceSequence > watermark {
			continue
		}
		if err := record.Validate(); err != nil {
			return nil, err
		}
		records = append(records, cloneResultRecord(record))
	}
	sort.Slice(records, func(i, j int) bool { return tupleTransferLess(records[i].TupleID, records[j].TupleID) })
	for index := 1; index < len(records); index++ {
		if records[index-1].TupleID == records[index].TupleID {
			return nil, model.ErrIdentityReuse
		}
	}
	return records, nil
}

// ArtifactStore durably retains sealed result artifacts under one directory
// with temporary-file, fsync, and atomic-rename semantics. Every mutation is
// serialized under one mutex and only complete checksum-verified artifacts
// become visible.
type ArtifactStore struct {
	directory string
	mu        sync.Mutex
}

// NewArtifactStore validates the artifact directory without creating or
// moving any file.
func NewArtifactStore(directory string) (*ArtifactStore, error) {
	if directory == "" {
		return nil, errors.New("empty artifact directory")
	}
	return &ArtifactStore{directory: directory}, nil
}

// artifactStorePath derives the canonical durable name of one exact artifact
// identity.
func artifactStorePath(directory string, artifact protocol.ResultArtifact) string {
	encoded := []byte("crane-result-artifact-name-v1\x00")
	encoded = append(encoded, artifact.JobID[:]...)
	encoded = appendTaskTransfer(encoded, artifact.SinkTask)
	encoded = append(encoded, artifact.SpecificationHash[:]...)
	encoded = appendUint64Transfer(encoded, artifact.RecordCount)
	encoded = appendUint64Transfer(encoded, artifact.TotalLength)
	encoded = append(encoded, artifact.Checksum[:]...)
	sum := sha256.Sum256(encoded)
	return directory + string(os.PathSeparator) + hex.EncodeToString(sum[:]) + ".artifact"
}

// Sealed reports whether one exact artifact identity is durably installed.
func (store *ArtifactStore) Sealed(artifact protocol.ResultArtifact) (bool, error) {
	if err := validateSealedArtifact(artifact); err != nil {
		return false, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return artifactFileSealed(store.directory, artifact)
}

// Read returns up to max sealed artifact bytes at offset and reports whether
// more bytes remain after the returned slice.
func (store *ArtifactStore) Read(artifact protocol.ResultArtifact, offset, max uint64) ([]byte, bool, error) {
	if err := validateSealedArtifact(artifact); err != nil {
		return nil, false, err
	}
	if offset > artifact.TotalLength {
		return nil, false, errors.New("artifact read offset outside bounds")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	sealed, err := artifactFileSealed(store.directory, artifact)
	if err != nil {
		return nil, false, err
	}
	if !sealed {
		return nil, false, errors.New("sealed artifact is not installed")
	}
	file, err := os.Open(artifactStorePath(store.directory, artifact))
	if err != nil {
		return nil, false, err
	}
	defer file.Close()
	length := artifact.TotalLength - offset
	if length > max {
		length = max
	}
	if length == 0 {
		return nil, offset < artifact.TotalLength, nil
	}
	data := make([]byte, length)
	if _, err := file.ReadAt(data, int64(offset)); err != nil {
		return nil, false, err
	}
	return data, offset+length < artifact.TotalLength, nil
}

// Seal durably installs one complete artifact stream. Re-sealing the exact
// bytes is idempotent; any different bytes under the same identity are
// rejected without mutation.
func (store *ArtifactStore) Seal(artifact protocol.ResultArtifact, stream []byte) error {
	if err := validateSealedArtifact(artifact); err != nil {
		return err
	}
	if uint64(len(stream)) != artifact.TotalLength || sha256.Sum256(stream) != artifact.Checksum {
		return errors.New("sealed stream does not match the artifact identity")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	sealed, err := artifactFileSealed(store.directory, artifact)
	if err != nil {
		return err
	}
	if sealed {
		existing, readErr := os.ReadFile(artifactStorePath(store.directory, artifact))
		if readErr != nil {
			return readErr
		}
		if bytes.Equal(existing, stream) {
			return nil
		}
		return model.ErrIdentityReuse
	}
	if err := os.MkdirAll(store.directory, 0o700); err != nil {
		return err
	}
	return writeArtifactDurable(store.directory, artifactStorePath(store.directory, artifact), stream)
}

// Install advances one resumable durable artifact receive by an exact
// transfer chunk. Chunks must continue the durable next offset, exact
// duplicates are idempotent, and the complete artifact becomes visible only
// after its whole-stream checksum verifies.
func (store *ArtifactStore) Install(artifact protocol.ResultArtifact, chunk protocol.TransferChunk) (uint64, bool, error) {
	if err := validateSealedArtifact(artifact); err != nil {
		return 0, false, err
	}
	if chunk.TransferID == ([16]byte{}) || chunk.JobID != artifact.JobID || chunk.TotalLength != artifact.TotalLength || chunk.Checksum != artifact.Checksum {
		return 0, false, errors.New("artifact chunk does not bind the artifact identity")
	}
	if chunk.Offset > artifact.TotalLength || uint64(len(chunk.Data)) > artifact.TotalLength-chunk.Offset {
		return 0, false, errors.New("artifact chunk outside total bounds")
	}
	end := chunk.Offset + uint64(len(chunk.Data))
	if chunk.Final != (end == artifact.TotalLength) {
		return 0, false, errors.New("artifact chunk final flag mismatch")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	sealed, err := artifactFileSealed(store.directory, artifact)
	if err != nil {
		return 0, false, err
	}
	if sealed {
		return artifact.TotalLength, true, nil
	}
	if err := os.MkdirAll(store.directory, 0o700); err != nil {
		return 0, false, err
	}
	finalPath := artifactStorePath(store.directory, artifact)
	partialPath := finalPath + ".part"
	next := uint64(0)
	if info, statErr := os.Stat(partialPath); statErr == nil {
		next = uint64(info.Size())
	} else if !os.IsNotExist(statErr) {
		return 0, false, statErr
	}
	if chunk.Offset > next {
		return next, false, errors.New("artifact chunk leaves a gap")
	}
	if chunk.Offset < next && end > chunk.Offset {
		overlapEnd := next
		if end < next {
			overlapEnd = end
		}
		file, openErr := os.Open(partialPath)
		if openErr != nil {
			return next, false, openErr
		}
		existing := make([]byte, overlapEnd-chunk.Offset)
		_, readErr := file.ReadAt(existing, int64(chunk.Offset))
		closeErr := file.Close()
		if readErr != nil {
			return next, false, readErr
		}
		if closeErr != nil {
			return next, false, closeErr
		}
		if !bytes.Equal(existing, chunk.Data[:overlapEnd-chunk.Offset]) {
			return next, false, model.ErrIdentityReuse
		}
	}
	if end > next {
		file, openErr := os.OpenFile(partialPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if openErr != nil {
			return next, false, openErr
		}
		if _, writeErr := file.Write(chunk.Data[next-chunk.Offset:]); writeErr != nil {
			file.Close()
			return next, false, writeErr
		}
		if syncErr := file.Sync(); syncErr != nil {
			file.Close()
			return next, false, syncErr
		}
		if closeErr := file.Close(); closeErr != nil {
			return next, false, closeErr
		}
		next = end
	}
	if next != artifact.TotalLength {
		return next, false, nil
	}
	var stream []byte
	if artifact.TotalLength > 0 {
		var readErr error
		stream, readErr = os.ReadFile(partialPath)
		if readErr != nil {
			return next, false, readErr
		}
	}
	if uint64(len(stream)) != artifact.TotalLength || sha256.Sum256(stream) != artifact.Checksum {
		_ = os.Remove(partialPath)
		return 0, false, errors.New("installed artifact bytes fail the checksum")
	}
	if err := writeArtifactDurable(store.directory, finalPath, stream); err != nil {
		return next, false, err
	}
	if artifact.TotalLength > 0 {
		_ = os.Remove(partialPath)
	}
	return next, true, nil
}

// artifactFileSealed reports whether the exact artifact file exists.
func artifactFileSealed(directory string, artifact protocol.ResultArtifact) (bool, error) {
	_, err := os.Stat(artifactStorePath(directory, artifact))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// writeArtifactDurable publishes exact bytes through a temporary file, fsync,
// atomic rename, and a directory sync barrier.
func writeArtifactDurable(directory, finalPath string, stream []byte) error {
	temporary, err := os.CreateTemp(directory, ".artifact-temp-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	remove := true
	defer func() {
		if remove {
			_ = os.Remove(temporaryName)
		}
	}()
	if _, err := temporary.Write(stream); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryName, finalPath); err != nil {
		return err
	}
	remove = false
	directoryFile, err := os.Open(directory)
	if err != nil {
		return err
	}
	syncErr := directoryFile.Sync()
	closeErr := directoryFile.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}
