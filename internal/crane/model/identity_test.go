package model

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func TestIdentityRejectsZeroAndMismatchedEmbeddedJobIDs(t *testing.T) {
	zeroJob := JobID{}
	job := jobIDFixture()
	task := TaskID{JobID: job, StageID: 1, Partition: 2}
	source := TupleID{JobID: job, SourceTask: task, SourceSequence: 1, PathDigest: digestFixture(0x80)}

	for name, identity := range map[string]interface{ Validate() error }{
		"client":            ClientID{},
		"client request":    ClientRequestID{},
		"job":               zeroJob,
		"task":              TaskID{},
		"worker epoch":      WorkerEpoch{},
		"coordinator epoch": CoordinatorEpoch{},
		"assignment token":  AssignmentToken{},
		"tuple":             TupleID{},
		"delivery":          DeliveryID{},
	} {
		if err := identity.Validate(); err == nil {
			t.Fatalf("%s accepted its zero value", name)
		}
	}

	if err := (TupleID{JobID: job, SourceTask: TaskID{JobID: JobID{1}, StageID: 1}, SourceSequence: 1, PathDigest: digestFixture(1)}).Validate(); err == nil {
		t.Fatal("TupleID accepted a source task for another job")
	}
	if err := (DeliveryID{Tuple: source, EdgeID: 1, DestinationTask: TaskID{JobID: JobID{1}, StageID: 1}}).Validate(); err == nil {
		t.Fatal("DeliveryID accepted a destination task for another job")
	}
}

func TestIdentityDerivationUsesExactDomainsBigEndianAndTruncation(t *testing.T) {
	client := ClientID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	topology := digestFixture(0)
	request := ClientRequestID{ClientID: client, Sequence: 0x0102030405060708}
	job := DeriveJobID(request, topology)
	if got := hex.EncodeToString(job[:]); got != "012654e89324a1834bdb4e9af0287a4d" {
		t.Fatalf("DeriveJobID() = %s", got)
	}
	if !bytes.Equal(job[:], []byte{0x01, 0x26, 0x54, 0xe8, 0x93, 0x24, 0xa1, 0x83, 0x4b, 0xdb, 0x4e, 0x9a, 0xf0, 0x28, 0x7a, 0x4d}) {
		t.Fatal("JobID was not the leading SHA-256 bytes")
	}

	sourceJob := JobID{0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f}
	sourceTask := TaskID{JobID: sourceJob, StageID: 0x1122, Partition: 0x3344}
	source := DeriveSourceTupleID(sourceJob, sourceTask, 0x0102030405060708)
	if got := hex.EncodeToString(source.PathDigest[:]); got != "03995ef357370299d49340e0f02f4070be9f34b70b57c248899efc3e50340d01" {
		t.Fatalf("source path digest = %s", got)
	}
	if source.JobID != sourceJob || source.SourceTask != sourceTask || source.SourceSequence != 0x0102030405060708 {
		t.Fatalf("source identity changed defining fields: %#v", source)
	}

	child0 := DeriveChildTupleID(source, TaskID{JobID: sourceJob, StageID: 0x5566, Partition: 0x7788}, 0x99aa, 0)
	child1 := DeriveChildTupleID(source, TaskID{JobID: sourceJob, StageID: 0x5566, Partition: 0x7788}, 0x99aa, 1)
	if got := hex.EncodeToString(child0.PathDigest[:]); got != "8e38b711fb047f8513365a63ce18747d650f9236384358f8ec537f3d93d22825" {
		t.Fatalf("child ordinal zero path digest = %s", got)
	}
	if got := hex.EncodeToString(child1.PathDigest[:]); got != "d00b1dcb579c3b557f6c34ec6d6620ac74685dc16aaa25a56eb778d7a9967099" {
		t.Fatalf("child ordinal one path digest = %s", got)
	}
	if child0 == child1 {
		t.Fatal("different child output ordinals collided")
	}
}

func TestIdentityValidationDistinguishesReuseFromExactRetry(t *testing.T) {
	job := jobIDFixture()
	task := TaskID{JobID: job, StageID: 7, Partition: 3}
	token := AssignmentToken{Task: task, WorkerID: 2, WorkerEpoch: WorkerEpoch{1}, Attempt: 4, SpecificationHash: digestFixture(1), AssignmentRevision: 9}
	if err := token.Validate(); err != nil {
		t.Fatalf("valid token rejected: %v", err)
	}
	if err := (AssignmentToken{Task: task, WorkerID: 2, WorkerEpoch: WorkerEpoch{1}, Attempt: 5, SpecificationHash: digestFixture(1), AssignmentRevision: 9}).Validate(); err != nil {
		t.Fatalf("a distinct assignment identity rejected: %v", err)
	}
	if err := (AssignmentToken{Task: task, WorkerID: 2, WorkerEpoch: WorkerEpoch{1}, Attempt: 4, SpecificationHash: digestFixture(1)}).Validate(); err == nil {
		t.Fatal("assignment token accepted zero assignment revision")
	}
}

func jobIDFixture() JobID {
	return JobID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
}

func digestFixture(start byte) [32]byte {
	var digest [32]byte
	for index := range digest {
		digest[index] = start + byte(index)
	}
	return digest
}
