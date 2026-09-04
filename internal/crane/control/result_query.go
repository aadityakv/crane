// Package control owns Crane's public +4 control-plane logic: the
// memory-bounded global result-page query engine over committed manifests.
// The +4 TCP service itself is built by a later task; this package provides
// its verified, transport-independent core.
package control

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"sort"

	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
	"github.com/aadityakv/crane/internal/crane/state"
)

var (
	// ErrInvalidResultPage reports a malformed or self-contradictory page
	// request binding.
	ErrInvalidResultPage = errors.New("crane result page request is invalid")
	// ErrResultQueryUnavailable reports that no complete current manifest set
	// satisfies the request, or that both copies of a required partition
	// could not be opened.
	ErrResultQueryUnavailable = errors.New("crane result query unavailable")
	// ErrCorruptResultSet reports a cross-partition duplicate TupleID or any
	// other violation of the global canonical result order.
	ErrCorruptResultSet = errors.New("crane result set is corrupt")
)

// PageLimitTooSmallError reports that the first eligible complete record
// entry does not fit the requested page budget. The global cursor never
// advanced, so the caller may retry unchanged with a budget of at least
// RequiredBytes.
type PageLimitTooSmallError struct {
	// RequiredBytes is the complete encoded entry size that must fit.
	RequiredBytes uint32
}

// Error returns the stable rejection description.
func (e PageLimitTooSmallError) Error() string {
	return "crane result page limit too small"
}

// RecordStream yields one validated sealed record at a time from exactly one
// partition copy. Implementations retain at most one bounded record.
type RecordStream interface {
	// Next returns the next record in canonical TupleID order or io.EOF at
	// the end of the partition.
	Next(context.Context) (model.ResultRecord, error)
	// Close releases the underlying transfer.
	Close() error
}

// ResultFetcher opens one sealed partition copy for streaming. Production
// wiring speaks the authenticated +3 leader fetch; Task 21 binds it to the
// committed manifest identity and barrier semantics.
type ResultFetcher interface {
	// OpenPartition opens the requested replica copy of one sealed artifact.
	OpenPartition(context.Context, protocol.ResultFetchRequest) (RecordStream, error)
}

// QueryEngine serves stateless global result pages from the replicated
// committed manifests. It performs no barrier of its own: the caller owns the
// View/manifest-set binding (Task 21); this engine verifies the request
// against the current machine view and streams one copy per partition with
// fallback, merging in global TupleID order with at most one bounded record
// per partition plus the response page in memory.
type QueryEngine struct {
	// Machine is the local replicated Crane state machine.
	Machine *state.Machine
	// Fetcher streams sealed partition copies.
	Fetcher ResultFetcher
}

// ResultManifestSetDigest binds every committed manifest of one job in
// canonical sink order. Two locally consistent voters with different sets
// derive different digests, and any cursor bound to one revision is rejected
// under another.
func ResultManifestSetDigest(manifests []state.ResultManifest) [32]byte {
	sorted := append([]state.ResultManifest(nil), manifests...)
	sort.Slice(sorted, func(i, j int) bool { return taskLessQuery(sorted[i].SinkTask, sorted[j].SinkTask) })
	encoded := []byte("crane-result-manifest-set-digest-v1\x00")
	for _, manifest := range sorted {
		encoded = append(encoded, manifest.JobID[:]...)
		encoded = appendTaskQuery(encoded, manifest.SinkTask)
		encoded = appendUint64Query(encoded, manifest.ManifestRevision)
		encoded = append(encoded, manifest.SpecificationHash[:]...)
		encoded = appendUint64Query(encoded, manifest.RecordCount)
		encoded = appendUint64Query(encoded, manifest.TotalBytes)
		encoded = append(encoded, manifest.Checksum[:]...)
		encoded = appendUint16Query(encoded, manifest.Replicas.PrimaryNodeID)
		encoded = appendUint16Query(encoded, manifest.Replicas.SecondaryNodeID)
		encoded = append(encoded, manifest.Replicas.PrimaryEpoch[:]...)
		encoded = append(encoded, manifest.Replicas.SecondaryEpoch[:]...)
	}
	return sha256.Sum256(encoded)
}

// Page returns one page of globally ordered records strictly after the
// stateless request cursor. Records are never split across pages, a page
// budget below the first complete entry rejects with PageLimitTooSmallError
// without advancement, and any cross-partition duplicate TupleID rejects the
// manifest set as corruption.
func (engine *QueryEngine) Page(ctx context.Context, request protocol.ResultPageRequest) (protocol.ResultPageResponse, error) {
	if err := validatePageRequest(request); err != nil {
		return protocol.ResultPageResponse{}, err
	}
	view := engine.Machine.View()
	record, ok := viewJob(view, request.JobID)
	if !ok || record.Assignment == nil {
		return protocol.ResultPageResponse{}, ErrResultQueryUnavailable
	}
	manifests, ok := completeCurrentManifests(record)
	if !ok {
		return protocol.ResultPageResponse{}, ErrResultQueryUnavailable
	}
	if ResultManifestSetDigest(manifests) != request.ManifestDigest {
		return protocol.ResultPageResponse{}, ErrResultQueryUnavailable
	}
	sort.Slice(manifests, func(i, j int) bool { return taskLessQuery(manifests[i].SinkTask, manifests[j].SinkTask) })

	partitions := make([]partitionHead, 0, len(manifests))
	defer func() {
		for _, partition := range partitions {
			_ = partition.stream.Close()
		}
	}()
	for _, manifest := range manifests {
		stream, err := engine.openPartition(ctx, view.CoordinatorEpoch, manifest)
		if err != nil {
			return protocol.ResultPageResponse{}, err
		}
		partition := partitionHead{stream: stream}
		head, err := stream.Next(ctx)
		if err != nil && !errors.Is(err, io.EOF) {
			return protocol.ResultPageResponse{}, err
		}
		if !errors.Is(err, io.EOF) {
			partition.head = head
			partition.live = true
			if request.HasLastTuple && !tupleLessQuery(request.Last, head.TupleID) {
				seeked := false
				for {
					next, nextErr := stream.Next(ctx)
					if errors.Is(nextErr, io.EOF) {
						seeked = true
						break
					}
					if nextErr != nil {
						return protocol.ResultPageResponse{}, nextErr
					}
					if tupleLessQuery(request.Last, next.TupleID) {
						partition.head = next
						break
					}
				}
				if seeked {
					partition.live = false
				}
			}
		}
		partitions = append(partitions, partition)
	}

	response := protocol.ResultPageResponse{
		JobID: request.JobID, ManifestDigest: request.ManifestDigest,
		RequestHasLastTuple: request.HasLastTuple, RequestLast: request.Last, PageBytes: request.PageBytes,
	}
	used := uint32(0)
	for {
		live := 0
		minimum := -1
		for index := range partitions {
			if !partitions[index].live {
				continue
			}
			live++
			if minimum < 0 || tupleLessQuery(partitions[index].head.TupleID, partitions[minimum].head.TupleID) {
				minimum = index
				continue
			}
			if partitions[index].head.TupleID == partitions[minimum].head.TupleID {
				// The deterministic route assigns each TupleID to exactly one
				// partition; a duplicate rejects the manifest set even when
				// the value bytes match.
				return protocol.ResultPageResponse{}, ErrCorruptResultSet
			}
		}
		if minimum < 0 {
			response.End = true
			break
		}
		_ = live
		entry, err := protocol.EncodedResultPageRecordBytes(partitions[minimum].head)
		if err != nil {
			return protocol.ResultPageResponse{}, err
		}
		if len(response.Records) == 0 && uint32(len(entry)) > request.PageBytes {
			return protocol.ResultPageResponse{}, PageLimitTooSmallError{RequiredBytes: uint32(len(entry))}
		}
		if used > request.PageBytes-uint32(len(entry)) {
			break
		}
		used += uint32(len(entry))
		response.Records = append(response.Records, partitions[minimum].head)
		next, nextErr := partitions[minimum].stream.Next(ctx)
		if errors.Is(nextErr, io.EOF) {
			partitions[minimum].live = false
			continue
		}
		if nextErr != nil {
			return protocol.ResultPageResponse{}, nextErr
		}
		partitions[minimum].head = next
	}
	if len(response.Records) == 0 {
		response.NextHasLastTuple = request.HasLastTuple
		response.NextLast = request.Last
	} else {
		response.NextHasLastTuple = true
		response.NextLast = response.Records[len(response.Records)-1].TupleID
	}
	if err := protocol.ValidateResultPageResponseCorrelation(request, response); err != nil {
		return protocol.ResultPageResponse{}, err
	}
	return response, nil
}

// partitionHead retains at most one bounded record of one open partition.
type partitionHead struct {
	stream RecordStream
	head   model.ResultRecord
	live   bool
}

// openPartition streams one committed manifest's artifact from its primary
// copy, falling back to the secondary copy when the primary fails.
func (engine *QueryEngine) openPartition(ctx context.Context, epoch model.CoordinatorEpoch, manifest state.ResultManifest) (RecordStream, error) {
	artifact := protocol.ResultArtifact{
		JobID: manifest.JobID, SinkTask: manifest.SinkTask, SpecificationHash: manifest.SpecificationHash,
		RecordCount: manifest.RecordCount, TotalLength: manifest.TotalBytes, Checksum: manifest.Checksum,
	}
	endpoints := []struct {
		node  uint16
		epoch model.WorkerEpoch
	}{
		{manifest.Replicas.PrimaryNodeID, manifest.Replicas.PrimaryEpoch},
		{manifest.Replicas.SecondaryNodeID, manifest.Replicas.SecondaryEpoch},
	}
	var lastErr error
	for _, endpoint := range endpoints {
		request := protocol.ResultFetchRequest{
			Artifact: artifact, ReplicaNodeID: endpoint.node, ReplicaWorkerEpoch: endpoint.epoch,
			Offset: 0, CoordinatorEpoch: epoch,
		}
		stream, err := engine.Fetcher.OpenPartition(ctx, request)
		if err == nil {
			return stream, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	if lastErr == nil {
		lastErr = ErrResultQueryUnavailable
	}
	return nil, lastErr
}

// validatePageRequest enforces the public request binding before any state
// read: a valid job, a nonzero manifest digest, a page budget within the
// protocol bound, and a cursor selector consistent with its value.
func validatePageRequest(request protocol.ResultPageRequest) error {
	if request.JobID.Validate() != nil || request.ManifestDigest == ([32]byte{}) ||
		request.PageBytes == 0 || request.PageBytes > protocol.MaxResultPageBytes {
		return ErrInvalidResultPage
	}
	if request.HasLastTuple {
		if request.Last.Validate() != nil || request.Last.JobID != request.JobID {
			return ErrInvalidResultPage
		}
	} else if request.Last != (model.TupleID{}) {
		return ErrInvalidResultPage
	}
	return nil
}

// viewJob locates one job record in an owned view.
func viewJob(view state.View, job model.JobID) (state.JobRecord, bool) {
	for _, record := range view.Jobs {
		if record.JobID == job {
			return record, true
		}
	}
	return state.JobRecord{}, false
}

// completeCurrentManifests reports the committed manifest list only when
// every expected partition carries a manifest binding the exact current
// placement and immutable specification.
func completeCurrentManifests(record state.JobRecord) ([]state.ResultManifest, bool) {
	if len(record.Manifests) != len(record.Assignment.ResultReplicas) {
		return nil, false
	}
	manifests := make([]state.ResultManifest, 0, len(record.Manifests))
	for _, replica := range record.Assignment.ResultReplicas {
		manifest, ok := record.Manifests[replica.SinkTask]
		if !ok || manifest.ManifestRevision == 0 || manifest.SpecificationHash != record.TopologyDigest || manifest.Replicas != replica {
			return nil, false
		}
		manifests = append(manifests, manifest)
	}
	return manifests, true
}

// tupleLessQuery orders two tuple IDs by the canonical global order: job,
// source task, sequence, then path digest.
func tupleLessQuery(a, b model.TupleID) bool {
	if comparison := bytes.Compare(a.JobID[:], b.JobID[:]); comparison != 0 {
		return comparison < 0
	}
	if comparison := taskCompareQuery(a.SourceTask, b.SourceTask); comparison != 0 {
		return comparison < 0
	}
	if a.SourceSequence != b.SourceSequence {
		return a.SourceSequence < b.SourceSequence
	}
	return bytes.Compare(a.PathDigest[:], b.PathDigest[:]) < 0
}

// taskCompareQuery orders two task IDs canonically.
func taskCompareQuery(a, b model.TaskID) int {
	if comparison := bytes.Compare(a.JobID[:], b.JobID[:]); comparison != 0 {
		return comparison
	}
	if a.StageID != b.StageID {
		if a.StageID < b.StageID {
			return -1
		}
		return 1
	}
	if a.Partition != b.Partition {
		if a.Partition < b.Partition {
			return -1
		}
		return 1
	}
	return 0
}

// taskLessQuery reports the canonical order of two sink task IDs.
func taskLessQuery(a, b model.TaskID) bool { return taskCompareQuery(a, b) < 0 }

// appendTaskQuery appends the canonical task ID bytes.
func appendTaskQuery(destination []byte, task model.TaskID) []byte {
	destination = append(destination, task.JobID[:]...)
	destination = appendUint16Query(destination, task.StageID)
	return appendUint16Query(destination, task.Partition)
}

// appendUint16Query appends one big-endian uint16.
func appendUint16Query(destination []byte, value uint16) []byte {
	return append(destination, byte(value>>8), byte(value))
}

// appendUint64Query appends one big-endian uint64.
func appendUint64Query(destination []byte, value uint64) []byte {
	return append(destination,
		byte(value>>56), byte(value>>48), byte(value>>40), byte(value>>32),
		byte(value>>24), byte(value>>16), byte(value>>8), byte(value))
}
