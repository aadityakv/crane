package model

import (
	"bytes"
	"reflect"
	"testing"
)

func TestWorkerStoreContractPinsVersionsRegistriesAndCheckpointRules(t *testing.T) {
	want := WorkerStoreContract{
		SchemaVersion: 1, WALFrameVersion: 1,
		ReadableDomainRegistryVersions: []uint16{1, 2}, WriteDomainRegistryVersion: 2,
		ReadableSnapshotVersions: []uint16{1, 2}, WriteSnapshotVersion: 2,
		LegacyDomainRecordTypeMax: 13, CurrentDomainRecordTypeMax: 14,
		LegacySnapshotKindMax: 8, CurrentSnapshotKindMax: 9,
		DomainRecords: []WorkerStoreRecordDescriptor{
			{Name: "Fence", Value: 1, PayloadSchema: 1}, {Name: "Assignment", Value: 2, PayloadSchema: 1},
			{Name: "Delivery", Value: 3, PayloadSchema: 1}, {Name: "DeliveryProcessed", Value: 4, PayloadSchema: 1},
			{Name: "DeliveryCompleted", Value: 5, PayloadSchema: 1}, {Name: "Checkpoint", Value: 6, PayloadSchema: 2},
			{Name: "Result", Value: 7, PayloadSchema: 1}, {Name: "Event", Value: 8, PayloadSchema: 1},
			{Name: "EventAck", Value: 9, PayloadSchema: 2}, {Name: "Repair", Value: 10, PayloadSchema: 1},
			{Name: "Source", Value: 11, PayloadSchema: 2}, {Name: "OutboxAck", Value: 12, PayloadSchema: 1},
			{Name: "OutboxRetry", Value: 13, PayloadSchema: 1}, {Name: "CheckpointObservation", Value: 14, PayloadSchema: 2},
		},
		SnapshotKinds: []WorkerStoreRecordDescriptor{
			{Name: "Fence", Value: 1, PayloadSchema: 1}, {Name: "Assignment", Value: 2, PayloadSchema: 1},
			{Name: "Source", Value: 3, PayloadSchema: 2}, {Name: "Delivery", Value: 4, PayloadSchema: 1},
			{Name: "Outbox", Value: 5, PayloadSchema: 2}, {Name: "Result", Value: 6, PayloadSchema: 1},
			{Name: "Repair", Value: 7, PayloadSchema: 1}, {Name: "Event", Value: 8, PayloadSchema: 1},
			{Name: "CheckpointObservation", Value: 9, PayloadSchema: 2},
		},
		CheckpointObservationLayout:  []string{"SchemaVersion:u16(2)", "JobID:bytes16", "Source:TaskID", "Watermark:u64", "RaftIndex:u64", "CoordinatorEpoch:CoordinatorEpoch", "JobControlRevision:u64", "AssignmentRevision:u64", "AssignmentDigest:sha256"},
		MaxCheckpointObservations:    LimitsV1().MaxRetainedJobs * LimitsV1().MaxTasksPerJob,
		CheckpointObservationSortKey: "JobID+SourceTaskID",
		Rules: []string{
			"v1 snapshot admits kinds 1..8 only; v2 admits kinds 1..9",
			"domain registry v1 admits record types 1..13; v2 checkpoint observation is type 14 with payload schema 2",
			"checkpoint observation requires exact current fence, assignment revision/digest, job-control revision, source EOF and local assignment participation or Closed/Draining retained historical-result ownership",
			"checkpoint observations are sorted and keyed by exact JobID+Source TaskID; exact retry is metadata-inert; changed same RaftIndex and stale RaftIndex/watermark reject before mutation",
		},
	}
	got := WorkerStoreContractV1()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("WorkerStoreContractV1() = %#v, want %#v", got, want)
	}
	got.DomainRecords[0].Name = "changed"
	got.ReadableSnapshotVersions[0] = 9
	if again := WorkerStoreContractV1(); !reflect.DeepEqual(again, want) {
		t.Fatal("WorkerStoreContractV1 returned shared storage")
	}
}

func TestWorkerStoreContractCanonicalBytesAreMutationSensitive(t *testing.T) {
	baseline := canonicalWorkerStoreContractBytes(WorkerStoreContractV1())
	mutations := []func(*WorkerStoreContract){
		func(contract *WorkerStoreContract) { contract.WriteSnapshotVersion++ },
		func(contract *WorkerStoreContract) { contract.DomainRecords[13].PayloadSchema++ },
		func(contract *WorkerStoreContract) { contract.SnapshotKinds[8].Value++ },
		func(contract *WorkerStoreContract) { contract.CheckpointObservationLayout[0] += "!" },
		func(contract *WorkerStoreContract) { contract.MaxCheckpointObservations++ },
		func(contract *WorkerStoreContract) { contract.Rules[0] += "!" },
	}
	for index, mutate := range mutations {
		candidate := WorkerStoreContractV1()
		mutate(&candidate)
		if bytes.Equal(canonicalWorkerStoreContractBytes(candidate), baseline) {
			t.Fatalf("mutation %d did not change canonical bytes", index)
		}
	}
}
