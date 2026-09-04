package store

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
)

func TestCommittedCheckpointObservationIsAtomicOwnedAndSurvivesSnapshotReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker")
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 1}
	options := Options{MaxBytes: 8 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return model.WorkerEpoch{7}, nil }}
	workerStore, err := Open(path, identity, options)
	if err != nil {
		t.Fatal(err)
	}
	topology, assignment, epoch := replicaCheckpointAssignment(t, workerStore.WorkerEpoch(), identity.NodeID)
	if err := workerStore.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := workerStore.InstallAssignment(assignment, topology.Spec(), 3, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	var remoteSource model.TaskID
	for _, token := range assignment.Tasks {
		if token.Task.StageID == 1 && (token.WorkerID != identity.NodeID || token.WorkerEpoch != workerStore.WorkerEpoch()) {
			remoteSource = token.Task
			break
		}
	}
	if remoteSource == (model.TaskID{}) {
		t.Fatal("fixture has no remote source")
	}
	notice := protocol.CheckpointNotice{Notice: model.CheckpointNotice{JobID: assignment.JobID, Source: remoteSource, Watermark: 2, RaftIndex: 11, Epoch: epoch}, JobControlRevision: 3, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest}
	eof, err := model.SourceEOF(topology, remoteSource)
	if err != nil {
		t.Fatal(err)
	}
	notice.Notice.Watermark = eof
	outOfRange := notice
	outOfRange.Notice.Watermark = eof + 1
	beforeInvalid := workerStore.Recovered()
	if err := workerStore.ObserveCheckpoint(outOfRange); err == nil || workerStore.Recovered() != beforeInvalid {
		t.Fatalf("out-of-range observation error/state = %v/%+v", err, workerStore.Recovered())
	}
	if err := workerStore.ObserveCheckpoint(notice); err != nil {
		t.Fatal(err)
	}
	payload, err := encodeCheckpointObservation(CommittedCheckpoint{Notice: notice.Notice, JobControlRevision: notice.JobControlRevision, AssignmentRevision: notice.AssignmentRevision, AssignmentDigest: notice.AssignmentDigest})
	if err != nil || binary.BigEndian.Uint16(payload[:2]) != model.WorkerStoreContractV1().WriteDomainRegistryVersion {
		t.Fatalf("checkpoint observation payload registry = %d, %v", binary.BigEndian.Uint16(payload[:2]), err)
	}
	legacyRegistry := append([]byte(nil), payload...)
	binary.BigEndian.PutUint16(legacyRegistry[:2], 1)
	beforeLegacy := workerStore.Recovered()
	if err := workerStore.Commit(Transaction{Records: []Record{{Type: recordCheckpointObservation, Payload: legacyRegistry}}}); err == nil || workerStore.Recovered() != beforeLegacy {
		t.Fatalf("v1 domain registry admitted v2 record: %v", err)
	}
	before, err := workerStore.RecoverWork()
	beforeState := workerStore.Recovered()
	if err != nil || len(before.Checkpoints) != 1 || before.Checkpoints[0].Notice != notice.Notice {
		t.Fatalf("observation = %#v, %v", before.Checkpoints, err)
	}
	if err := workerStore.ObserveCheckpoint(notice); err != nil {
		t.Fatalf("exact retry: %v", err)
	}
	if state := workerStore.Recovered(); state != beforeState {
		t.Fatalf("exact retry mutated durable metadata: before=%+v after=%+v", beforeState, state)
	}
	changed := notice
	changed.Notice.Watermark++
	if err := workerStore.ObserveCheckpoint(changed); !errors.Is(err, model.ErrIdentityReuse) {
		t.Fatalf("changed identity = %v", err)
	}
	after, _ := workerStore.RecoverWork()
	if !reflect.DeepEqual(after, before) {
		t.Fatal("rejected observation mutated state")
	}
	snapshot, err := workerStore.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	image, err := os.ReadFile(filepath.Join(path, snapshotFilename(snapshot.Generation)))
	if err != nil || binary.BigEndian.Uint16(image[4:6]) != model.WorkerStoreContractV1().WriteSnapshotVersion {
		t.Fatalf("snapshot write version = %d, %v", binary.BigEndian.Uint16(image[4:6]), err)
	}
	if err := workerStore.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, identity, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovered, err := reopened.RecoverWork()
	if err != nil || !reflect.DeepEqual(recovered.Checkpoints, before.Checkpoints) {
		t.Fatalf("reopened observations = %#v, %v", recovered.Checkpoints, err)
	}
}

func replicaCheckpointAssignment(t *testing.T, local model.WorkerEpoch, node uint16) (model.ValidatedTopology, model.AssignmentSet, model.CoordinatorEpoch) {
	t.Helper()
	spec := domainTopologySpec(4)
	spec.Stages[0].Parallelism = 4
	topology, err := model.ValidateTopology(spec)
	if err != nil {
		t.Fatal(err)
	}
	workers := []model.WorkerPlacement{{NodeID: node, WorkerEpoch: local, SlotCapacity: 8}, {NodeID: 2, WorkerEpoch: model.WorkerEpoch{8}, SlotCapacity: 8}, {NodeID: 3, WorkerEpoch: model.WorkerEpoch{9}, SlotCapacity: 8}}
	for candidate := uint16(1); candidate < 512; candidate++ {
		job := model.JobID{byte(candidate), byte(candidate >> 8)}
		assignment, buildErr := model.BuildAssignmentSet(job, topology.Digest(), 1, topology, workers)
		if buildErr != nil {
			t.Fatal(buildErr)
		}
		remoteSource, localReplica := false, false
		for _, token := range assignment.Tasks {
			remoteSource = remoteSource || token.Task.StageID == 1 && token.WorkerID != node
		}
		for _, replica := range assignment.ResultReplicas {
			localReplica = localReplica || replica.PrimaryNodeID == node && replica.PrimaryEpoch == local || replica.SecondaryNodeID == node && replica.SecondaryEpoch == local
		}
		if remoteSource && localReplica {
			return topology, assignment, model.CoordinatorEpoch{Term: 2, BeginIndex: 4, Coordinator: 2, Nonce: [16]byte{5}}
		}
	}
	t.Fatal("could not construct replica checkpoint assignment")
	return model.ValidatedTopology{}, model.AssignmentSet{}, model.CoordinatorEpoch{}
}
