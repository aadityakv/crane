package coordinator

// This file owns the leader's terminal-results workflow: sealing sink
// partitions into canonical two-copy artifacts after final checkpoints,
// committing idempotent manifests, and driving the job to Succeeded with its
// terminal Closed installation. Result bytes never enter Raft; every decision
// is re-derived from the replicated view plus durable worker status so any
// interrupted pass converges under a later leadership session.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sort"

	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
	"github.com/aadityakv/crane/internal/crane/state"
	"github.com/aadityakv/crane/internal/crane/worker"
)

// maxSealFetchChunks bounds one artifact fetch even against a misbehaving
// worker that never sets its final flag.
const maxSealFetchChunks = 1 << 22

var (
	// ErrResultSealIncomplete reports that one partition's two current
	// artifact copies could not yet be established; the job stays Draining.
	ErrResultSealIncomplete = errors.New("crane result seal incomplete")
)

// ResultTransferClient is the authenticated +5 result-artifact surface the
// terminal workflow requires: the leader fetches sealed artifacts from a
// current replica and installs durable artifact chunks on both current
// replicas. Production dial wiring belongs to the node-composition task.
type ResultTransferClient interface {
	// Fetch streams one exact bounded artifact slice from one worker.
	Fetch(context.Context, uint16, protocol.ResultFetchRequest) (protocol.ResultFetchChunk, error)
	// ReceiveArtifact delivers one authenticated artifact chunk and returns
	// its exact durable acknowledgment.
	ReceiveArtifact(context.Context, uint16, protocol.ResultArtifactChunk) (protocol.ResultArtifactAck, error)
}

// driveTerminalResults performs one full-view terminal-results pass: every
// Running job whose committed checkpoints are final transitions to Draining
// with that exact revision installed on all task/result workers, every
// expected sink partition is sealed or reconstructed onto two current
// distinct worker incarnations, each idempotent ResultManifest commits, and
// only then Succeeded and the terminal Closed installation follow. It reports
// whether the pass converged; failures leave the job Draining for a later
// pass or leadership session.
func (actor *Actor) driveTerminalResults(ctx context.Context, epoch model.CoordinatorEpoch) bool {
	if actor.options.Results == nil {
		return true
	}
	if ctx.Err() != nil {
		return false
	}
	view := actor.options.Machine.View()
	jobs := make([]model.JobID, 0, len(view.Jobs))
	for _, job := range view.Jobs {
		jobs = append(jobs, job.JobID)
	}
	sort.Slice(jobs, func(i, j int) bool { return bytes.Compare(jobs[i][:], jobs[j][:]) < 0 })
	for _, jobID := range jobs {
		if ctx.Err() != nil {
			return false
		}
		record, ok := findJob(actor.options.Machine.View(), jobID)
		if !ok || terminalLifecycle(record.Lifecycle) || record.Assignment == nil || len(record.NeedsReassignment) > 0 {
			continue
		}
		if record.Lifecycle == state.JobRunning {
			if !jobCheckpointsFinal(record) {
				continue
			}
			if !actor.transitionJob(ctx, epoch, record, state.JobRunning, state.JobDraining) {
				return false
			}
			drained, ok := findJob(actor.options.Machine.View(), jobID)
			if !ok || drained.Assignment == nil {
				return false
			}
			if !actor.installAssignment(ctx, epoch, drained, model.Draining, true, 0) {
				return false
			}
			record = drained
		}
		if record.Lifecycle != state.JobDraining {
			continue
		}
		if !actor.sealJobResults(ctx, epoch, jobID) {
			return false
		}
	}
	return ctx.Err() == nil
}

// jobCheckpointsFinal mirrors the replicated final-checkpoint classification:
// every committed source checkpoint sits exactly at its immutable EOF, and a
// source whose committed EOF is zero is empty and trivially finally
// checkpointed without any materialized record.
func jobCheckpointsFinal(record state.JobRecord) bool {
	if len(record.SourceEOFs) == 0 {
		return false
	}
	for source, eof := range record.SourceEOFs {
		checkpoint, ok := record.Checkpoints[source]
		if !ok {
			if eof.EOF == 0 {
				continue
			}
			return false
		}
		if checkpoint.Revision == 0 || checkpoint.Watermark != eof.EOF {
			return false
		}
	}
	return true
}

// manifestPartitionBound reports whether one job's sink-partition count
// exceeds the replicated manifest bound and can therefore never seal.
func manifestPartitionBound(partitions int) bool {
	return uint64(partitions) > model.LimitsV1().MaxResultManifestsPerJob
}

// sealJobResults seals every expected sink partition of one Draining job,
// commits each idempotent manifest, and — only once every expected partition
// carries a committed current manifest — transitions to Succeeded and installs
// the terminal Closed state.
func (actor *Actor) sealJobResults(ctx context.Context, epoch model.CoordinatorEpoch, jobID model.JobID) bool {
	record, ok := findJob(actor.options.Machine.View(), jobID)
	if !ok || record.Assignment == nil || record.Lifecycle != state.JobDraining || len(record.NeedsReassignment) > 0 {
		return false
	}
	topology, err := model.DecodeTopology(record.TopologyBytes)
	if err != nil {
		return false
	}
	if manifestPartitionBound(len(record.Assignment.ResultReplicas)) {
		return false
	}
	vector := checkpointVector(topology, record)
	for _, entry := range vector {
		eof, exists := record.SourceEOFs[entry.Source]
		if !exists || eof.EOF != entry.Watermark {
			return false
		}
	}
	replicas := append([]model.ResultReplicaSet(nil), record.Assignment.ResultReplicas...)
	sort.Slice(replicas, func(i, j int) bool { return taskIDLessView(replicas[i].SinkTask, replicas[j].SinkTask) })
	for _, replica := range replicas {
		if manifestSatisfied(record, replica, record.TopologyDigest) {
			continue
		}
		if !actor.sealPartition(ctx, epoch, jobID, topology, replica, vector) {
			return false
		}
		if updated, ok := findJob(actor.options.Machine.View(), jobID); ok {
			record = updated
		}
	}
	for _, replica := range replicas {
		if !manifestSatisfied(record, replica, record.TopologyDigest) {
			return false
		}
	}
	if !actor.transitionJob(ctx, epoch, record, state.JobDraining, state.JobSucceeded) {
		return false
	}
	succeeded, ok := findJob(actor.options.Machine.View(), jobID)
	if !ok {
		return false
	}
	return actor.installAssignment(ctx, epoch, succeeded, model.Closed, false, 0)
}

// manifestSatisfied reports whether one sink already holds a committed
// current manifest binding the exact replica placement.
func manifestSatisfied(record state.JobRecord, replica model.ResultReplicaSet, specification [32]byte) bool {
	manifest, ok := record.Manifests[replica.SinkTask]
	return ok && manifest.ManifestRevision != 0 && manifest.SpecificationHash == specification && manifest.Replicas == replica
}

// taskIDLessView orders two task IDs canonically.
func taskIDLessView(left, right model.TaskID) bool {
	if comparison := bytes.Compare(left.JobID[:], right.JobID[:]); comparison != 0 {
		return comparison < 0
	}
	if left.StageID != right.StageID {
		return left.StageID < right.StageID
	}
	return left.Partition < right.Partition
}

// sealPartition establishes exactly two current durable copies of one sink
// partition's canonical artifact and commits its idempotent manifest. The
// artifact bytes are fetched from a current replica (either one; the survivor
// when the other is unreachable), verified against both replicas' agreeing
// inventory summaries, and durably installed on both current replica
// endpoints before the manifest proposes.
func (actor *Actor) sealPartition(ctx context.Context, epoch model.CoordinatorEpoch, jobID model.JobID, topology model.ValidatedTopology, replica model.ResultReplicaSet, vector []protocol.SourceCheckpoint) bool {
	view := actor.options.Machine.View()
	record, ok := findJob(view, jobID)
	if !ok || record.Assignment == nil {
		return false
	}
	if !currentDistinctReplicaEndpoints(view, replica) {
		return false
	}
	set := *record.Assignment
	query := buildInventoryQuery(record, set, replica.SinkTask, vector)
	primarySummary, primaryErr := actor.queryInventory(ctx, epoch, replica.PrimaryNodeID, query)
	secondarySummary, secondaryErr := actor.queryInventory(ctx, epoch, replica.SecondaryNodeID, query)
	if primaryErr != nil || secondaryErr != nil || !summariesEqual(primarySummary, secondarySummary) {
		return false
	}
	artifact, stream, ok := actor.fetchPartitionArtifact(ctx, epoch, replica, replica.SinkTask, record.TopologyDigest, primarySummary)
	if !ok {
		return false
	}
	records, err := decodeArtifactRecords(stream, jobID, replica.SinkTask, record.TopologyDigest, vector)
	if err != nil {
		return false
	}
	count, total, digest, err := worker.ResultInventoryAggregate(query.QueryDigest, records)
	if err != nil || count != primarySummary.RecordCount || total != primarySummary.TotalBytes || digest != primarySummary.ContentDigest {
		// The fetched bytes must be exactly the record set both replicas
		// reported under the bound inventory query.
		return false
	}
	endpoints := []repairEndpoint{
		{node: replica.PrimaryNodeID, epoch: replica.PrimaryEpoch},
		{node: replica.SecondaryNodeID, epoch: replica.SecondaryEpoch},
	}
	for _, endpoint := range endpoints {
		if !actor.installPartitionArtifact(ctx, epoch, endpoint, artifact, stream) {
			return false
		}
	}
	return actor.commitResultManifest(ctx, epoch, jobID, replica, artifact)
}

// workerCurrent reports whether one node's replicated worker record is a
// current non-Offline incarnation at exactly the demanded epoch.
func workerCurrent(view state.View, node uint16, epoch model.WorkerEpoch) bool {
	record, ok := findWorker(view, node)
	return ok && record.State != state.WorkerOffline && record.Epoch == epoch
}

// currentDistinctReplicaEndpoints reports whether one replica set names two
// current distinct worker incarnations. Two epochs of one NodeID never count
// as two durable copies.
func currentDistinctReplicaEndpoints(view state.View, replica model.ResultReplicaSet) bool {
	if replica.PrimaryNodeID == replica.SecondaryNodeID || replica.PrimaryNodeID == 0 || replica.SecondaryNodeID == 0 {
		return false
	}
	return workerCurrent(view, replica.PrimaryNodeID, replica.PrimaryEpoch) && workerCurrent(view, replica.SecondaryNodeID, replica.SecondaryEpoch)
}

// fetchPartitionArtifact streams one complete sealed artifact from either
// current replica, falling back to the second endpoint when the first is
// unreachable, and verifies the assembled stream end to end. The initial
// request carries the strongest identity the leader can derive without the
// bytes — the agreed inventory count/total bound with the inventory content
// digest as a deterministic discovery checksum — and every response must
// repeat one consistent true artifact identity whose stream SHA-256 matches.
func (actor *Actor) fetchPartitionArtifact(ctx context.Context, epoch model.CoordinatorEpoch, replica model.ResultReplicaSet, sink model.TaskID, specification [32]byte, summary protocol.ResultInventorySummary) (protocol.ResultArtifact, []byte, bool) {
	endpoints := []repairEndpoint{
		{node: replica.PrimaryNodeID, epoch: replica.PrimaryEpoch},
		{node: replica.SecondaryNodeID, epoch: replica.SecondaryEpoch},
	}
	for _, endpoint := range endpoints {
		artifact, stream, ok := actor.fetchFromReplica(ctx, epoch, endpoint, sink, specification, summary)
		if ok {
			return artifact, stream, true
		}
		if ctx.Err() != nil {
			return protocol.ResultArtifact{}, nil, false
		}
	}
	return protocol.ResultArtifact{}, nil, false
}

// fetchFromReplica assembles one complete artifact stream from one replica's
// bounded fetch chunks with exact contiguity, identity, and checksum
// verification.
func (actor *Actor) fetchFromReplica(ctx context.Context, epoch model.CoordinatorEpoch, endpoint repairEndpoint, sink model.TaskID, specification [32]byte, summary protocol.ResultInventorySummary) (protocol.ResultArtifact, []byte, bool) {
	discovery := protocol.ResultArtifact{
		JobID: sink.JobID, SinkTask: sink, SpecificationHash: specification,
		RecordCount: summary.RecordCount, TotalLength: summary.TotalBytes, Checksum: summary.ContentDigest,
	}
	if summary.TotalBytes == 0 {
		discovery.Checksum = sha256.Sum256(nil)
	}
	request := discovery
	stream := make([]byte, 0)
	var artifact protocol.ResultArtifact
	offset := uint64(0)
	for chunkIndex := 0; chunkIndex < maxSealFetchChunks; chunkIndex++ {
		chunk, err := actor.options.Results.Fetch(ctx, endpoint.node, protocol.ResultFetchRequest{
			Artifact: request, ReplicaNodeID: endpoint.node, ReplicaWorkerEpoch: endpoint.epoch,
			Offset: offset, CoordinatorEpoch: epoch,
		})
		if err != nil {
			return protocol.ResultArtifact{}, nil, false
		}
		if chunk.SourceNodeID != endpoint.node || chunk.SourceWorkerEpoch != endpoint.epoch || chunk.CoordinatorEpoch != epoch {
			return protocol.ResultArtifact{}, nil, false
		}
		if chunk.Artifact.JobID != sink.JobID || chunk.Artifact.SinkTask != sink || chunk.Artifact.SpecificationHash != specification ||
			chunk.Artifact.RecordCount != summary.RecordCount || chunk.Artifact.TotalLength != summary.TotalBytes {
			return protocol.ResultArtifact{}, nil, false
		}
		if chunk.Transfer.JobID != sink.JobID || chunk.Transfer.Offset != offset || chunk.Transfer.TotalLength != chunk.Artifact.TotalLength ||
			chunk.Transfer.Checksum != chunk.Artifact.Checksum || uint64(len(chunk.Transfer.Data)) > protocol.MaxTransferChunkBytes {
			return protocol.ResultArtifact{}, nil, false
		}
		end := chunk.Transfer.Offset + uint64(len(chunk.Transfer.Data))
		if end > chunk.Artifact.TotalLength || chunk.Transfer.Final != (end == chunk.Artifact.TotalLength) {
			return protocol.ResultArtifact{}, nil, false
		}
		if chunkIndex == 0 {
			artifact = chunk.Artifact
			if artifact.Checksum == ([32]byte{}) || artifact.TotalLength > model.LimitsV1().MaxResultRecordsBytesPerJob {
				return protocol.ResultArtifact{}, nil, false
			}
			stream = make([]byte, 0, artifact.TotalLength)
			request = artifact
		} else if chunk.Artifact != artifact {
			return protocol.ResultArtifact{}, nil, false
		}
		stream = append(stream, chunk.Transfer.Data...)
		offset = end
		if chunk.Transfer.Final {
			if uint64(len(stream)) != artifact.TotalLength || sha256.Sum256(stream) != artifact.Checksum {
				return protocol.ResultArtifact{}, nil, false
			}
			return artifact, stream, true
		}
	}
	return protocol.ResultArtifact{}, nil, false
}

// decodeArtifactRecords decodes one sealed artifact stream into its complete
// canonical record list, enforcing strict global TupleID order, exact sink
// and specification binding, and checkpoint coverage.
func decodeArtifactRecords(stream []byte, job model.JobID, sink model.TaskID, specification [32]byte, vector []protocol.SourceCheckpoint) ([]model.ResultRecord, error) {
	watermarks := make(map[model.TaskID]uint64, len(vector))
	for _, entry := range vector {
		watermarks[entry.Source] = entry.Watermark
	}
	records := make([]model.ResultRecord, 0)
	for offset := 0; offset < len(stream); {
		if len(stream)-offset < 4 {
			return nil, errors.New("truncated artifact entry prefix")
		}
		length := uint64(stream[offset])<<24 | uint64(stream[offset+1])<<16 | uint64(stream[offset+2])<<8 | uint64(stream[offset+3])
		if length == 0 || length > uint64(len(stream)-offset-4) {
			return nil, errors.New("artifact entry length outside bounds")
		}
		record, err := model.UnmarshalResultRecord(stream[offset+4 : offset+4+int(length)])
		if err != nil {
			return nil, err
		}
		if record.TupleID.JobID != job || record.SinkTask != sink || record.SpecificationHash != specification {
			return nil, errors.New("artifact record outside the sealed partition")
		}
		watermark, covered := watermarks[record.TupleID.SourceTask]
		if !covered || record.TupleID.SourceSequence > watermark {
			return nil, errors.New("artifact record outside the final checkpoint vector")
		}
		if len(records) > 0 && !tupleLessView(records[len(records)-1].TupleID, record.TupleID) {
			return nil, errors.New("artifact records are not strictly globally ordered")
		}
		records = append(records, record)
		offset += 4 + int(length)
	}
	return records, nil
}

// tupleLessView orders two tuple IDs canonically by job, source task,
// sequence, and path digest.
func tupleLessView(left, right model.TupleID) bool {
	if comparison := bytes.Compare(left.JobID[:], right.JobID[:]); comparison != 0 {
		return comparison < 0
	}
	if comparison := taskIDBytesCompare(left.SourceTask, right.SourceTask); comparison != 0 {
		return comparison < 0
	}
	if left.SourceSequence != right.SourceSequence {
		return left.SourceSequence < right.SourceSequence
	}
	return bytes.Compare(left.PathDigest[:], right.PathDigest[:]) < 0
}

func taskIDBytesCompare(left, right model.TaskID) int {
	if comparison := bytes.Compare(left.JobID[:], right.JobID[:]); comparison != 0 {
		return comparison
	}
	if left.StageID != right.StageID {
		if left.StageID < right.StageID {
			return -1
		}
		return 1
	}
	if left.Partition != right.Partition {
		if left.Partition < right.Partition {
			return -1
		}
		return 1
	}
	return 0
}

// installPartitionArtifact durably installs one complete artifact on one
// current replica endpoint through bounded authenticated chunks, resuming
// from the receiver's acknowledged durable offset after any interruption.
func (actor *Actor) installPartitionArtifact(ctx context.Context, epoch model.CoordinatorEpoch, endpoint repairEndpoint, artifact protocol.ResultArtifact, stream []byte) bool {
	offset := uint64(0)
	for attempts := uint64(0); attempts <= artifact.TotalLength+1; attempts++ {
		length := uint64(protocol.MaxTransferChunkBytes)
		if artifact.TotalLength-offset < length {
			length = artifact.TotalLength - offset
		}
		end := offset + length
		id, err := worker.DeriveResultArtifactTransferID(worker.TransferNormalReplication, artifact, endpoint.node, endpoint.epoch, offset, length, epoch)
		if err != nil {
			return false
		}
		chunk := protocol.ResultArtifactChunk{
			Transfer: protocol.TransferChunk{
				TransferID: id, JobID: artifact.JobID, TotalLength: artifact.TotalLength, Checksum: artifact.Checksum,
				Offset: offset, Data: append([]byte(nil), stream[offset:end]...), Final: end == artifact.TotalLength,
			},
			Artifact: artifact, DestinationNodeID: endpoint.node, DestinationWorkerEpoch: endpoint.epoch, CoordinatorEpoch: epoch,
		}
		ack, err := actor.options.Results.ReceiveArtifact(ctx, endpoint.node, chunk)
		if err != nil {
			return false
		}
		if ack.NodeID != endpoint.node || ack.WorkerEpoch != endpoint.epoch || ack.Artifact != artifact ||
			ack.TransferID != chunk.Transfer.TransferID || ack.CoordinatorEpoch != epoch ||
			ack.NextOffset < end || ack.NextOffset > artifact.TotalLength || ack.Complete != (ack.NextOffset == artifact.TotalLength) {
			return false
		}
		if ack.Complete {
			return true
		}
		if ack.NextOffset <= offset {
			return false
		}
		offset = ack.NextOffset
	}
	return false
}

// commitResultManifest proposes one idempotent manifest seal under a stable
// command identity and verifies the committed record. A manifest that already
// satisfies the requirement resolves without any new proposal, so repeated or
// leader-changed passes never create duplicate or conflicting manifests.
func (actor *Actor) commitResultManifest(ctx context.Context, epoch model.CoordinatorEpoch, jobID model.JobID, replica model.ResultReplicaSet, artifact protocol.ResultArtifact) bool {
	view := actor.options.Machine.View()
	record, ok := findJob(view, jobID)
	if !ok || record.Assignment == nil {
		return false
	}
	if manifestSatisfied(record, replica, record.TopologyDigest) {
		if existing := record.Manifests[replica.SinkTask]; existing.RecordCount == artifact.RecordCount && existing.TotalBytes == artifact.TotalLength && existing.Checksum == artifact.Checksum {
			return true
		}
		return false
	}
	currentRevision := record.Manifests[replica.SinkTask].ManifestRevision
	manifest := state.ResultManifest{
		JobID: jobID, SinkTask: replica.SinkTask, ManifestRevision: currentRevision + 1,
		SpecificationHash: record.TopologyDigest, RecordCount: artifact.RecordCount,
		TotalBytes: artifact.TotalLength, Checksum: artifact.Checksum, Replicas: replica,
	}
	subject := state.SubjectKey{Kind: state.SubjectResultManifest, JobID: jobID, TaskID: replica.SinkTask}
	id := internalCommandID(state.CommandSealManifest, epoch, subject, currentRevision,
		uint64Bytes(manifest.RecordCount), uint64Bytes(manifest.TotalBytes), manifest.Checksum[:],
		replica.PrimaryEpoch[:], replica.SecondaryEpoch[:], uint16Bytes(replica.PrimaryNodeID), uint16Bytes(replica.SecondaryNodeID))
	command, err := state.NewSealManifest(id, currentRevision, manifest, epoch)
	if err != nil {
		return false
	}
	result, err := actor.proposeResolved(ctx, id, subject, command)
	if err != nil {
		return false
	}
	switch result.Code {
	case state.ResultStaleEpoch:
		return false
	case state.ResultSuccess, state.ResultRevisionMismatch:
	default:
		return false
	}
	updated, ok := findJob(actor.options.Machine.View(), jobID)
	if !ok {
		return false
	}
	if !manifestSatisfied(updated, replica, record.TopologyDigest) {
		return false
	}
	committed := updated.Manifests[replica.SinkTask]
	return committed.RecordCount == manifest.RecordCount && committed.TotalBytes == manifest.TotalBytes && committed.Checksum == manifest.Checksum
}
