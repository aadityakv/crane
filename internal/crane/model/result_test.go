package model

import "testing"

func TestResultRecordIdentityIndependentFromCopyProvenance(t *testing.T) {
	job := JobID{1}
	sink := TaskID{JobID: job, StageID: 3}
	source := TaskID{JobID: job, StageID: 1}
	value, err := MarshalTuple(intTuple(11))
	if err != nil {
		t.Fatal(err)
	}
	record, err := NewResultRecord(DeriveSourceTupleID(job, source, 1), sink, [32]byte{7}, value)
	if err != nil {
		t.Fatal(err)
	}
	want := [32]byte{0x01, 0x5e, 0x43, 0xbb, 0xe3, 0xd4, 0xcb, 0xcf, 0xdc, 0xd3, 0x30, 0xa5, 0x6b, 0x62, 0xa0, 0x70, 0x43, 0x33, 0xf2, 0x70, 0x38, 0xdf, 0x44, 0x99, 0x67, 0xa7, 0x3c, 0xb6, 0xb6, 0x56, 0xa0, 0x8e}
	if record.Checksum != want {
		t.Fatalf("checksum = %x, want %x", record.Checksum, want)
	}
	if err := record.Validate(); err != nil {
		t.Fatal(err)
	}
	copyValue := append([]byte(nil), record.Value...)
	copyValue[0] ^= 1
	if record.Value[0] == copyValue[0] {
		t.Fatal("result value not owned")
	}

	replicas := ResultReplicaSet{SinkTask: sink, PrimaryNodeID: 2, SecondaryNodeID: 3, PrimaryEpoch: WorkerEpoch{2}, SecondaryEpoch: WorkerEpoch{3}}
	first := ResultCopyProvenance{AssignmentRevision: 1, AssignmentDigest: [32]byte{1}, ReplicaSet: replicas, DestinationRole: PrimaryReplica, CoordinatorEpoch: CoordinatorEpoch{Term: 1, BeginIndex: 1, Coordinator: 2, Nonce: [16]byte{1}}}
	second := first
	second.AssignmentRevision = 2
	second.AssignmentDigest = [32]byte{2}
	second.DestinationRole = SecondaryReplica
	if err := first.Validate(record); err != nil {
		t.Fatal(err)
	}
	if err := second.Validate(record); err != nil {
		t.Fatal(err)
	}
	if record.Checksum != want {
		t.Fatal("copy provenance changed logical identity")
	}
	bad := first
	bad.ReplicaSet.SinkTask.Partition = 1
	if err := bad.Validate(record); err == nil {
		t.Fatal("wrong replica set accepted")
	}
	bad = first
	bad.DestinationRole = 0
	if err := bad.Validate(record); err == nil {
		t.Fatal("unknown destination role accepted")
	}
}

func TestResultRecordRejectsNonCanonicalTupleBytes(t *testing.T) {
	job := JobID{1}
	_, err := NewResultRecord(
		DeriveSourceTupleID(job, TaskID{JobID: job, StageID: 1}, 1),
		TaskID{JobID: job, StageID: 3},
		[32]byte{7},
		[]byte("not a canonical tuple"),
	)
	if err == nil {
		t.Fatal("non-canonical collect value accepted")
	}
}

func TestResultRecordRejectsOversizeBeforeCopy(t *testing.T) {
	job := JobID{1}
	value := make([]byte, LimitsV1().MaxTuplePayloadBytes+1)
	if _, err := NewResultRecord(DeriveSourceTupleID(job, TaskID{JobID: job, StageID: 1}, 1), TaskID{JobID: job, StageID: 3}, [32]byte{7}, value); err == nil {
		t.Fatal("oversize result value accepted")
	}
}
