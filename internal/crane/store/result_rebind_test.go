package store

import (
	"errors"
	"testing"

	"crane/internal/crane/model"
)

// rebindFixture opens a store whose node 1 holds one result under revision 1
// and returns everything needed to install a partner-changing revision 2.
func rebindFixture(t *testing.T) (*Store, string, Identity, Options, model.ValidatedTopology, model.AssignmentSet, model.CoordinatorEpoch, model.ResultRecord, model.ResultCopyProvenance) {
	t.Helper()
	path := t.TempDir() + "/rebind"
	identity := Identity{ClusterID: [16]byte{61}, NodeID: 1}
	options := Options{MaxBytes: 16 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return model.WorkerEpoch{7}, nil }}
	workerStore, err := Open(path, identity, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = workerStore.Close() })
	topology, prior, epoch := domainAssignment(t, workerStore.WorkerEpoch(), identity.NodeID)
	if err := workerStore.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := workerStore.InstallAssignment(prior, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	record, provenance := domainResult(t, topology, prior, epoch, 0)
	if err := workerStore.UpsertResult(record, provenance); err != nil {
		t.Fatal(err)
	}
	return workerStore, path, identity, options, topology, prior, epoch, record, provenance
}

// partnerChangedSet derives revision+1 of prior in which partition 0's
// partner of node 1 is replaced by node 4 at a fresh epoch. When node 1 was
// the secondary the new primary also takes over the sink task token.
func partnerChangedSet(t *testing.T, topology model.ValidatedTopology, prior model.AssignmentSet) model.AssignmentSet {
	t.Helper()
	replicas := append([]model.ResultReplicaSet(nil), prior.ResultReplicas...)
	tasks := append([]model.AssignmentToken(nil), prior.Tasks...)
	replaced := uint16(4)
	replacedEpoch := model.WorkerEpoch{10}
	if replicas[0].PrimaryNodeID == 1 {
		replicas[0].SecondaryNodeID, replicas[0].SecondaryEpoch = replaced, replacedEpoch
	} else {
		old := replicas[0].PrimaryNodeID
		replicas[0].PrimaryNodeID, replicas[0].PrimaryEpoch = replaced, replacedEpoch
		for index := range tasks {
			if tasks[index].Task == replicas[0].SinkTask && tasks[index].WorkerID == old {
				tasks[index].WorkerID, tasks[index].WorkerEpoch, tasks[index].Attempt = replaced, replacedEpoch, tasks[index].Attempt+1
			}
		}
	}
	for index := range tasks {
		tasks[index].AssignmentRevision = prior.Revision + 1
	}
	current, err := model.NewAssignmentSet(prior.JobID, prior.Revision+1, tasks, replicas, topology)
	if err != nil {
		t.Fatal(err)
	}
	return current
}

func currentProvenance(prior model.ResultCopyProvenance, current model.AssignmentSet, epoch model.CoordinatorEpoch) model.ResultCopyProvenance {
	return model.ResultCopyProvenance{AssignmentRevision: current.Revision, AssignmentDigest: current.Digest, ReplicaSet: current.ResultReplicas[0], DestinationRole: prior.DestinationRole, CoordinatorEpoch: epoch}
}

// TestUpsertResultRebindsRetainedCopyToCurrentPair pins the Task 24 defect #4
// ruling's store half: the identical logical record retained under a
// superseded envelope re-binds to the current pair (new revision with a
// changed partner, or the same revision under a newer fence), idempotently,
// durably across WAL replay, without touching the logical record.
func TestUpsertResultRebindsRetainedCopyToCurrentPair(t *testing.T) {
	workerStore, path, identity, options, topology, prior, epoch, record, provenance := rebindFixture(t)
	current := partnerChangedSet(t, topology, prior)
	if err := workerStore.InstallAssignment(current, topology.Spec(), 2, model.Running, epoch); err != nil {
		t.Fatalf("install partner-changed revision: %v", err)
	}
	rebound := currentProvenance(provenance, current, epoch)
	if err := workerStore.UpsertResult(record, rebound); err != nil {
		t.Fatalf("rebind retained copy: %v", err)
	}
	if err := workerStore.UpsertResult(record, rebound); err != nil {
		t.Fatalf("rebind must be idempotent: %v", err)
	}
	work, err := workerStore.RecoverWork()
	if err != nil || len(work.Results) != 1 || work.Results[0].Provenance != rebound || work.Results[0].Record.Checksum != record.Checksum {
		t.Fatalf("rebound result=%+v err=%v", work.Results, err)
	}

	// The same revision under a strictly newer committed fence re-binds too.
	newer := epoch
	newer.Term++
	newer.BeginIndex += 3
	if err := workerStore.Fence(newer); err != nil {
		t.Fatal(err)
	}
	if err := workerStore.InstallAssignment(current, topology.Spec(), 2, model.Running, newer); err != nil {
		t.Fatalf("rebind install under newer fence: %v", err)
	}
	fenced := rebound
	fenced.CoordinatorEpoch = newer
	if err := workerStore.UpsertResult(record, fenced); err != nil {
		t.Fatalf("rebind under newer fence: %v", err)
	}
	if err := workerStore.Close(); err != nil {
		t.Fatal(err)
	}
	workerStore, err = Open(path, identity, options)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = workerStore.Close() })
	work, err = workerStore.RecoverWork()
	if err != nil || len(work.Results) != 1 || work.Results[0].Provenance != fenced {
		t.Fatalf("replayed result=%+v err=%v", work.Results, err)
	}
}

// TestUpsertResultRebindRejectsChangedBytesAndUnorderedProvenance pins the
// fail-closed half: different logical bytes under the current envelope, a
// provenance that is not strictly superseded (same envelope, different role)
// and a regress to the superseded envelope are all refused without mutation.
func TestUpsertResultRebindRejectsChangedBytesAndUnorderedProvenance(t *testing.T) {
	workerStore, _, _, _, topology, prior, epoch, record, provenance := rebindFixture(t)
	current := partnerChangedSet(t, topology, prior)
	if err := workerStore.InstallAssignment(current, topology.Spec(), 2, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	rebound := currentProvenance(provenance, current, epoch)
	otherTuple, _ := model.MarshalTuple(model.Tuple{Fields: []model.Field{{Name: "value", Value: model.Value{Type: model.ValueInt64, Int64: 99}}}})
	changed, err := model.NewResultRecord(record.TupleID, record.SinkTask, record.SpecificationHash, otherTuple)
	if err != nil {
		t.Fatal(err)
	}
	if err := workerStore.UpsertResult(changed, rebound); !errors.Is(err, model.ErrIdentityReuse) {
		t.Fatalf("changed bytes under the current envelope: %v", err)
	}
	if err := workerStore.UpsertResult(record, rebound); err != nil {
		t.Fatal(err)
	}
	// Once current, the retained superseded envelope can never come back and
	// the same envelope under the other role is an identity reuse.
	if err := workerStore.UpsertResult(record, provenance); err == nil {
		t.Fatal("regress to the superseded envelope was accepted")
	}
	otherRole := rebound
	if otherRole.DestinationRole == model.PrimaryReplica {
		otherRole.DestinationRole = model.SecondaryReplica
	} else {
		otherRole.DestinationRole = model.PrimaryReplica
	}
	if err := workerStore.UpsertResult(record, otherRole); err == nil {
		t.Fatal("same envelope under the other role was accepted")
	}
	work, err := workerStore.RecoverWork()
	if err != nil || len(work.Results) != 1 || work.Results[0].Provenance != rebound || work.Results[0].Record.Checksum != record.Checksum {
		t.Fatalf("result after rejected rebinds=%+v err=%v", work.Results, err)
	}
}
