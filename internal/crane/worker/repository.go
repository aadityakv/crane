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
	Receive(store.DeliveryRecord) (store.DeliveryState, error)
	MarkProcessed(model.DeliveryID, []model.Tuple, []store.OutboxRecord) error
	MarkCompleted(model.DeliveryID) error
	AdvanceSource(store.SourceCursor, []store.OutboxRecord) error
	MarkOutboxCompleted(model.DeliveryID) error
	PersistEvent(model.WorkerEvent) error
}

// Sender transmits one already-durable tuple outbox. Network framing and
// authentication belong to Task 16.
type Sender interface {
	Send(context.Context, protocol.TupleDelivery) error
}

// ResultReplicator is the slice boundary implemented by result completion in
// the independently reviewed Task 15 slice B. Slice A never calls it.
type ResultReplicator interface {
	ReplicateRecord(context.Context, model.ResultRecord, model.ResultCopyProvenance) error
}
