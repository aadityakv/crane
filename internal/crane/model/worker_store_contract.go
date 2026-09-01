package model

const workerStoreContractFingerprintDomain = "cs425/crane/worker-store-contract/v1\x00"

// WorkerStoreRecordDescriptor pins one Store registry entry and payload schema.
type WorkerStoreRecordDescriptor struct {
	Name          string
	Value         uint16
	PayloadSchema uint16
}

// WorkerStoreContract contains every compatibility-sensitive worker Store rule.
type WorkerStoreContract struct {
	SchemaVersion                  uint16
	WALFrameVersion                uint16
	ReadableDomainRegistryVersions []uint16
	WriteDomainRegistryVersion     uint16
	ReadableSnapshotVersions       []uint16
	WriteSnapshotVersion           uint16
	LegacyDomainRecordTypeMax      uint16
	CurrentDomainRecordTypeMax     uint16
	LegacySnapshotKindMax          uint16
	CurrentSnapshotKindMax         uint16
	DomainRecords                  []WorkerStoreRecordDescriptor
	SnapshotKinds                  []WorkerStoreRecordDescriptor
	CheckpointObservationLayout    []string
	MaxCheckpointObservations      uint64
	CheckpointObservationSortKey   string
	Rules                          []string
}

// WorkerStoreContractV1 returns the immutable modeled worker Store contract.
func WorkerStoreContractV1() WorkerStoreContract {
	return WorkerStoreContract{
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
}

func canonicalWorkerStoreContractBytes(contract WorkerStoreContract) []byte {
	encoded := appendString([]byte(workerStoreContractFingerprintDomain), "crane-worker-store")
	for _, value := range []uint16{contract.SchemaVersion, contract.WALFrameVersion, contract.WriteDomainRegistryVersion, contract.WriteSnapshotVersion, contract.LegacyDomainRecordTypeMax, contract.CurrentDomainRecordTypeMax, contract.LegacySnapshotKindMax, contract.CurrentSnapshotKindMax} {
		encoded = appendUint16(encoded, value)
	}
	appendU16s := func(values []uint16) {
		encoded = appendUint16(encoded, uint16(len(values)))
		for _, value := range values {
			encoded = appendUint16(encoded, value)
		}
	}
	appendRecords := func(records []WorkerStoreRecordDescriptor) {
		encoded = appendUint16(encoded, uint16(len(records)))
		for _, record := range records {
			encoded = appendString(encoded, record.Name)
			encoded = appendUint16(encoded, record.Value)
			encoded = appendUint16(encoded, record.PayloadSchema)
		}
	}
	appendStrings := func(values []string) {
		encoded = appendUint16(encoded, uint16(len(values)))
		for _, value := range values {
			encoded = appendString(encoded, value)
		}
	}
	appendU16s(contract.ReadableDomainRegistryVersions)
	appendU16s(contract.ReadableSnapshotVersions)
	appendRecords(contract.DomainRecords)
	appendRecords(contract.SnapshotKinds)
	appendStrings(contract.CheckpointObservationLayout)
	encoded = appendUint64(encoded, contract.MaxCheckpointObservations)
	encoded = appendString(encoded, contract.CheckpointObservationSortKey)
	appendStrings(contract.Rules)
	return encoded
}
