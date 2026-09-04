package model

import (
	"reflect"
	"testing"
)

func TestPlacementRendezvousFullDigestGolden(t *testing.T) {
	job := JobID{9}
	task := TaskID{JobID: job, StageID: 7, Partition: 3}
	tokens, err := PlaceTasks(job, [32]byte{7}, 4, []TaskID{task}, testWorkers())
	if err != nil {
		t.Fatal(err)
	}
	if got := tokens[0].WorkerID; got != 2 {
		t.Fatalf("rendezvous winner = %d, want 2", got)
	}
}

func TestAssignmentSetCompleteAndValidated(t *testing.T) {
	v := requireValidTopology(t, testTopology())
	job := JobID{9}
	set, err := BuildAssignmentSet(job, v.Digest(), 5, v, testWorkers())
	if err != nil {
		t.Fatal(err)
	}
	if len(set.Tasks) != 6 || len(set.ResultReplicas) != 2 {
		t.Fatalf("set counts = tasks %d replicas %d", len(set.Tasks), len(set.ResultReplicas))
	}
	if set.JobID != job || set.Revision != 5 || set.Digest == ([32]byte{}) {
		t.Fatalf("bad set header: %#v", set)
	}
	if err := set.Validate(v); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	for index, replica := range set.ResultReplicas {
		if replica.SinkTask.StageID != 3 || replica.SinkTask.Partition != uint16(index) {
			t.Fatalf("bad replica order: %#v", replica)
		}
		if replica.PrimaryNodeID == replica.SecondaryNodeID || replica.PrimaryEpoch == (WorkerEpoch{}) || replica.SecondaryEpoch == (WorkerEpoch{}) {
			t.Fatalf("bad replica: %#v", replica)
		}
		var primary uint16
		for _, token := range set.Tasks {
			if token.Task == replica.SinkTask {
				primary = token.WorkerID
			}
		}
		if primary != replica.PrimaryNodeID {
			t.Fatalf("primary %d != assigned sink %d", replica.PrimaryNodeID, primary)
		}
	}
	again, err := BuildAssignmentSet(job, v.Digest(), 5, v, []WorkerPlacement{testWorkers()[2], testWorkers()[0], testWorkers()[1]})
	if err != nil || !reflect.DeepEqual(set, again) {
		t.Fatalf("non-deterministic set: %v", err)
	}

	wrong := v.Digest()
	wrong[0] ^= 1
	if _, err := BuildAssignmentSet(job, wrong, 5, v, testWorkers()); err == nil {
		t.Fatal("wrong specification hash accepted")
	}
	corrupt := set
	corrupt.ResultReplicas = corrupt.ResultReplicas[:1]
	if err := corrupt.Validate(v); err == nil {
		t.Fatal("incomplete replica set accepted")
	}
	corrupt = set
	corrupt.Tasks = corrupt.Tasks[:5]
	if err := corrupt.Validate(v); err == nil {
		t.Fatal("incomplete task set accepted")
	}
	corrupt = set
	corrupt.ResultReplicas[0].SecondaryNodeID = corrupt.ResultReplicas[0].PrimaryNodeID
	if err := corrupt.Validate(v); err == nil {
		t.Fatal("same-node replicas accepted")
	}
}

func TestAssignmentSetAllowsEligibleIdleSecondary(t *testing.T) {
	v := requireValidTopology(t, testTopology())
	set, err := BuildAssignmentSet(JobID{5}, v.Digest(), 1, v, testWorkers())
	if err != nil {
		t.Fatal(err)
	}
	set.ResultReplicas[0].SecondaryNodeID = 99
	set.ResultReplicas[0].SecondaryEpoch = WorkerEpoch{99}
	set.Digest = assignmentDigest(set)
	if err := set.Validate(v); err != nil {
		t.Fatalf("structurally valid idle secondary rejected without a worker registry: %v", err)
	}
}

func TestAssignmentSetRejectsContradictoryEpochsAcrossEveryDuty(t *testing.T) {
	v := requireValidTopology(t, testTopology())
	set, err := BuildAssignmentSet(JobID{5}, v.Digest(), 1, v, testWorkers())
	if err != nil {
		t.Fatal(err)
	}

	t.Run("task to replica", func(t *testing.T) {
		corrupt := cloneAssignmentSetForTest(set)
		var taskDuty AssignmentToken
		for _, token := range corrupt.Tasks {
			if token.WorkerID != corrupt.ResultReplicas[0].PrimaryNodeID {
				taskDuty = token
				break
			}
		}
		if taskDuty.WorkerID == 0 {
			t.Fatal("fixture has no distinct task duty")
		}
		corrupt.ResultReplicas[0].SecondaryNodeID = taskDuty.WorkerID
		corrupt.ResultReplicas[0].SecondaryEpoch = taskDuty.WorkerEpoch
		corrupt.ResultReplicas[0].SecondaryEpoch[0] ^= 0xff
		corrupt.Digest = assignmentDigest(corrupt)
		if err := corrupt.Validate(v); err == nil {
			t.Fatal("task/replica epoch contradiction accepted")
		}
	})

	t.Run("replica to replica", func(t *testing.T) {
		corrupt := cloneAssignmentSetForTest(set)
		corrupt.ResultReplicas[0].SecondaryNodeID = 99
		corrupt.ResultReplicas[0].SecondaryEpoch = WorkerEpoch{1}
		corrupt.ResultReplicas[1].SecondaryNodeID = 99
		corrupt.ResultReplicas[1].SecondaryEpoch = WorkerEpoch{1}
		corrupt.ResultReplicas[1].SecondaryEpoch[0] ^= 0xff
		corrupt.Digest = assignmentDigest(corrupt)
		if err := corrupt.Validate(v); err == nil {
			t.Fatal("replica/replica epoch contradiction accepted")
		}
	})
}

func TestWorkerPlacementSlotBounds(t *testing.T) {
	job := JobID{1}
	task := TaskID{JobID: job, StageID: 1}
	hash := [32]byte{1}
	for _, slots := range []uint16{1, uint16(LimitsV1().MaxWorkerSlots)} {
		if _, err := PlaceTasks(job, hash, 1, []TaskID{task}, []WorkerPlacement{{NodeID: 1, WorkerEpoch: WorkerEpoch{1}, SlotCapacity: slots}}); err != nil {
			t.Fatalf("slots %d rejected: %v", slots, err)
		}
	}
	for _, slots := range []uint16{0, uint16(LimitsV1().MaxWorkerSlots + 1)} {
		if _, err := PlaceTasks(job, hash, 1, []TaskID{task}, []WorkerPlacement{{NodeID: 1, WorkerEpoch: WorkerEpoch{1}, SlotCapacity: slots}}); err == nil {
			t.Fatalf("slots %d accepted", slots)
		}
	}
}

func TestPlacementRejectsOversizedCollections(t *testing.T) {
	job := JobID{1}
	hash := [32]byte{1}
	tasks := make([]TaskID, LimitsV1().MaxTasksPerJob+1)
	if _, err := PlaceTasks(job, hash, 1, tasks, testWorkers()); err == nil {
		t.Fatal("oversized task input accepted")
	}
	workers := make([]WorkerPlacement, LimitsV1().MaxRegisteredWorkers+1)
	if _, err := PlaceTasks(job, hash, 1, []TaskID{{JobID: job, StageID: 1}}, workers); err == nil {
		t.Fatal("oversized worker input accepted")
	}
}

func cloneAssignmentSetForTest(set AssignmentSet) AssignmentSet {
	clone := set
	clone.Tasks = append([]AssignmentToken(nil), set.Tasks...)
	clone.ResultReplicas = append([]ResultReplicaSet(nil), set.ResultReplicas...)
	return clone
}
