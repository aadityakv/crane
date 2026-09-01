package worker

import (
	"context"

	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
	"github.com/aaditya/cs425mp3/internal/crane/store"
)

// Repository is the exact durable authority required by the slice-A worker
// owner. Every state transition represented here is crash-atomic in Task 14.
type Repository interface {
	RecoverWork() (store.RecoveredWork, error)
	LocalIdentity() (uint16, model.WorkerEpoch)
	CurrentFence() model.CoordinatorEpoch
	InstalledAssignment(model.JobID) (store.InstalledAssignment, bool)
	ProbeDelivery(store.DeliveryRecord) (store.DeliveryState, bool, error)
	Receive(store.DeliveryRecord) (store.DeliveryState, error)
	MarkProcessed(model.DeliveryID, []model.Tuple, []store.OutboxRecord) error
	MarkCompleted(model.DeliveryID) error
	AdvanceSource(store.SourceCursor, []store.OutboxRecord) error
	MarkOutboxCompleted(model.DeliveryID) error
	MarkOutboxDispatched(model.DeliveryID, int64) error
	MarkOutboxAccepted(model.DeliveryID, int64) error
	UpsertResult(model.ResultRecord, model.ResultCopyProvenance) error
	PersistEvent(model.WorkerEvent) error
	AcknowledgeEvents(uint64) error
	ApplyCheckpoint(model.CheckpointNotice) error
}

// Sender transmits one already-durable tuple outbox. Network framing and
// authentication belong to Task 16.
type Sender interface {
	Send(context.Context, protocol.TupleDelivery) error
}

// ResultReplicator durably copies one exact logical result to the provenance-
// designated replica. Task 16 validates transport chunks/ACKs and returns the
// transport-independent receipt consumed by this engine.
type ResultReplicator interface {
	ReplicateRecord(context.Context, model.ResultRecord, model.ResultCopyProvenance) (ResultReplicationReceipt, error)
}

// ResultReplicationReceipt is the transport-independent durable proof returned
// by Task 16 after it has authenticated and durably acknowledged the exact
// canonical result stream at the designated secondary incarnation.
type ResultReplicationReceipt struct {
	DestinationNodeID      uint16
	DestinationWorkerEpoch model.WorkerEpoch
	StreamChecksum         [32]byte
	StreamLength           uint64
	CoordinatorEpoch       model.CoordinatorEpoch
}
