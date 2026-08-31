package model

import (
	"reflect"
	"sort"
	"testing"
)

func TestRouteGoldensAndDeterminism(t *testing.T) {
	v := requireValidTopology(t, testTopology())
	job := JobID{1}
	tupleID := DeriveSourceTupleID(job, TaskID{JobID: job, StageID: 1}, 1)
	tuple := intTuple(42)
	shuffle, err := Route(v, v.Spec().Edges[0], tupleID, tuple)
	if err != nil || !reflect.DeepEqual(shuffle, []uint16{1}) {
		t.Fatalf("shuffle = %v,%v", shuffle, err)
	}
	field, err := Route(v, v.Spec().Edges[1], tupleID, tuple)
	if err != nil || !reflect.DeepEqual(field, []uint16{0}) {
		t.Fatalf("field hash = %v,%v", field, err)
	}

	spec := testTopology()
	spec.Edges[0].Routing = RoutingBroadcast
	v = requireValidTopology(t, spec)
	broadcast, err := Route(v, v.Spec().Edges[0], tupleID, tuple)
	if err != nil || !reflect.DeepEqual(broadcast, []uint16{0, 1}) {
		t.Fatalf("broadcast = %v,%v", broadcast, err)
	}
	if _, err := Route(v, EdgeSpec{EdgeID: 99, SourceStageID: 1, DestinationStageID: 2, Routing: RoutingShuffle}, tupleID, tuple); err == nil {
		t.Fatal("unowned edge accepted")
	}
}

func testWorkers() []WorkerPlacement {
	return []WorkerPlacement{
		{NodeID: 3, WorkerEpoch: WorkerEpoch{3}, SlotCapacity: 1},
		{NodeID: 1, WorkerEpoch: WorkerEpoch{1}, SlotCapacity: 2},
		{NodeID: 2, WorkerEpoch: WorkerEpoch{2}, SlotCapacity: 3},
	}
}

func TestPlacementConsumesSlotsAndIsPermutationDeterministic(t *testing.T) {
	job := JobID{9}
	tasks := []TaskID{{JobID: job, StageID: 1}, {JobID: job, StageID: 1, Partition: 1}, {JobID: job, StageID: 2}}
	hash := [32]byte{7}
	first, err := PlaceTasks(job, hash, 4, tasks, testWorkers())
	if err != nil {
		t.Fatal(err)
	}
	reversedTasks := append([]TaskID(nil), tasks...)
	sort.Slice(reversedTasks, func(i, j int) bool { return i > j })
	reversedWorkers := append([]WorkerPlacement(nil), testWorkers()...)
	sort.Slice(reversedWorkers, func(i, j int) bool { return i > j })
	second, err := PlaceTasks(job, hash, 4, reversedTasks, reversedWorkers)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("permutation changed placement: %#v %#v %v", first, second, err)
	}
	used := map[uint16]int{}
	for _, token := range first {
		if token.SpecificationHash != hash || token.AssignmentRevision != 4 || token.Attempt != 1 {
			t.Fatalf("bad token: %#v", token)
		}
		used[token.WorkerID]++
	}
	for _, worker := range testWorkers() {
		if used[worker.NodeID] > int(worker.SlotCapacity) {
			t.Fatal("slot capacity exceeded")
		}
	}
	if _, err := PlaceTasks(job, hash, 0, tasks, testWorkers()); err == nil {
		t.Fatal("zero revision accepted")
	}
	if _, err := PlaceTasks(job, hash, 1, append(tasks, TaskID{JobID: job, StageID: 3}), []WorkerPlacement{{NodeID: 1, WorkerEpoch: WorkerEpoch{1}, SlotCapacity: 1}}); err == nil {
		t.Fatal("insufficient slots accepted")
	}
}

func TestPlacementRendezvousTieBreaksOnLowerNodeID(t *testing.T) {
	score := [32]byte{0xaa}
	if !preferRendezvous(score, 2, score, 7) {
		t.Fatal("lower NodeID did not win an equal full-digest score")
	}
	if preferRendezvous(score, 7, score, 2) {
		t.Fatal("higher NodeID won an equal full-digest score")
	}
}
