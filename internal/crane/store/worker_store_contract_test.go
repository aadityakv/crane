package store

import (
	"reflect"
	"testing"

	"crane/internal/crane/model"
)

func TestWorkerStoreContractMatchesConcreteRegistries(t *testing.T) {
	contract := model.WorkerStoreContractV1()
	if walSchemaVersion != contract.WALFrameVersion {
		t.Fatalf("WAL frame version = %d, modeled %d", walSchemaVersion, contract.WALFrameVersion)
	}
	if snapshotSchemaVersionV1 != contract.ReadableSnapshotVersions[0] || snapshotSchemaVersion != contract.WriteSnapshotVersion {
		t.Fatalf("snapshot versions = read legacy %d/write %d, modeled %#v/write %d", snapshotSchemaVersionV1, snapshotSchemaVersion, contract.ReadableSnapshotVersions, contract.WriteSnapshotVersion)
	}

	domain := []model.WorkerStoreRecordDescriptor{
		{Name: "Fence", Value: uint16(recordFence), PayloadSchema: domainRecordSchema},
		{Name: "Assignment", Value: uint16(recordAssignment), PayloadSchema: domainRecordSchema},
		{Name: "Delivery", Value: uint16(recordDelivery), PayloadSchema: domainRecordSchema},
		{Name: "DeliveryProcessed", Value: uint16(recordDeliveryProcessed), PayloadSchema: domainRecordSchema},
		{Name: "DeliveryCompleted", Value: uint16(recordDeliveryCompleted), PayloadSchema: domainRecordSchema},
		{Name: "Checkpoint", Value: uint16(recordCheckpoint), PayloadSchema: checkpointRecordSchema},
		{Name: "Result", Value: uint16(recordResult), PayloadSchema: domainRecordSchema},
		{Name: "Event", Value: uint16(recordEvent), PayloadSchema: domainRecordSchema},
		{Name: "EventAck", Value: uint16(recordEventAck), PayloadSchema: eventAckRecordSchema},
		{Name: "Repair", Value: uint16(recordRepair), PayloadSchema: domainRecordSchema},
		{Name: "Source", Value: uint16(recordSource), PayloadSchema: sourceRecordSchema},
		{Name: "OutboxAck", Value: uint16(recordOutboxAck), PayloadSchema: domainRecordSchema},
		{Name: "OutboxRetry", Value: uint16(recordOutboxRetry), PayloadSchema: domainRecordSchema},
		{Name: "CheckpointObservation", Value: uint16(recordCheckpointObservation), PayloadSchema: checkpointObservationRecordSchema},
	}
	if !reflect.DeepEqual(domain, contract.DomainRecords) {
		t.Fatalf("domain registry drifted:\n got %#v\nwant %#v", domain, contract.DomainRecords)
	}

	snapshots := []model.WorkerStoreRecordDescriptor{
		{Name: "Fence", Value: uint16(snapshotFence), PayloadSchema: domainRecordSchema},
		{Name: "Assignment", Value: uint16(snapshotAssignment), PayloadSchema: domainRecordSchema},
		{Name: "Source", Value: uint16(snapshotSource), PayloadSchema: sourceRecordSchema},
		{Name: "Delivery", Value: uint16(snapshotDelivery), PayloadSchema: domainRecordSchema},
		{Name: "Outbox", Value: uint16(snapshotOutbox), PayloadSchema: outboxRecordSchema},
		{Name: "Result", Value: uint16(snapshotResult), PayloadSchema: domainRecordSchema},
		{Name: "Repair", Value: uint16(snapshotRepair), PayloadSchema: domainRecordSchema},
		{Name: "Event", Value: uint16(snapshotEvent), PayloadSchema: domainRecordSchema},
		{Name: "CheckpointObservation", Value: uint16(snapshotCheckpointObservation), PayloadSchema: checkpointObservationRecordSchema},
	}
	if !reflect.DeepEqual(snapshots, contract.SnapshotKinds) {
		t.Fatalf("snapshot registry drifted:\n got %#v\nwant %#v", snapshots, contract.SnapshotKinds)
	}
	if uint16(recordOutboxRetry) != contract.LegacyDomainRecordTypeMax || uint16(recordCheckpointObservation) != contract.CurrentDomainRecordTypeMax || uint16(snapshotEvent) != contract.LegacySnapshotKindMax || uint16(snapshotCheckpointObservation) != contract.CurrentSnapshotKindMax {
		t.Fatal("modeled legacy/current registry boundaries drifted")
	}
}
