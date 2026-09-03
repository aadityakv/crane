package store

import (
	"errors"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/aaditya/cs425mp3/internal/crane/integrationhook"
	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/wire"
)

// orderLog records every WAL fsync and every durable boundary in the exact
// order the store performs them.
type orderLog struct {
	mu      sync.Mutex
	entries []string
}

func (log *orderLog) add(entry string) {
	log.mu.Lock()
	defer log.mu.Unlock()
	log.entries = append(log.entries, entry)
}

func (log *orderLog) tail(n int) []string {
	log.mu.Lock()
	defer log.mu.Unlock()
	if n > len(log.entries) {
		n = len(log.entries)
	}
	return append([]string(nil), log.entries[len(log.entries)-n:]...)
}

func (log *orderLog) boundaries() []string {
	log.mu.Lock()
	defer log.mu.Unlock()
	var result []string
	for _, entry := range log.entries {
		if strings.HasPrefix(entry, "boundary:") {
			result = append(result, strings.TrimPrefix(entry, "boundary:"))
		}
	}
	return result
}

type recordingHook struct{ log *orderLog }

func (hook recordingHook) DurableBoundary(name string) { hook.log.add("boundary:" + name) }
func (hook recordingHook) DatagramAction(integrationhook.Direction, wire.MessageType) integrationhook.Action {
	return integrationhook.Pass
}

func openOrderedStore(t *testing.T, log *orderLog, faults FaultInjector) (*Store, Identity) {
	t.Helper()
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 1}
	operations := defaultStoreOperations()
	realSync := operations.syncFile
	operations.syncFile = func(file *os.File) error {
		log.add("sync")
		return realSync(file)
	}
	options := Options{MaxBytes: 8 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return model.WorkerEpoch{7}, nil }, Hook: recordingHook{log: log}, Faults: faults}
	store, err := openWithOperations(t.TempDir()+"/worker", identity, options, operations)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, identity
}

// expectDurable asserts the last two ordered entries are exactly the WAL
// fsync followed by the named boundary: the boundary fires only after the
// transaction's sync succeeded and before the method returns to its caller.
func expectDurable(t *testing.T, log *orderLog, name string) {
	t.Helper()
	tail := log.tail(2)
	if len(tail) != 2 || tail[0] != "sync" || tail[1] != "boundary:"+name {
		t.Fatalf("ordered tail = %v, want [sync boundary:%s]", tail, name)
	}
}

func TestDurableBoundariesFireAfterSyncBeforeReturnWithExactNames(t *testing.T) {
	log := &orderLog{}
	store, identity := openOrderedStore(t, log, nil)
	if got := log.boundaries(); len(got) != 0 {
		t.Fatalf("open published boundaries %v", got)
	}
	topology, assignment, epoch := domainAssignment(t, store.WorkerEpoch(), identity.NodeID)

	if err := store.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	expectDurable(t, log, BoundaryFence)
	if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Closed, epoch); err != nil {
		t.Fatal(err)
	}
	expectDurable(t, log, BoundaryAssignmentClosed)
	if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	expectDurable(t, log, BoundaryAssignmentRunning)
	// An identical replay is still one committed (fsynced) transaction and
	// therefore publishes exactly once more, after its own sync.
	if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	expectDurable(t, log, BoundaryAssignmentRunning)

	delivery := domainDelivery(t, topology, assignment, epoch)
	if _, err := store.Receive(delivery); err != nil {
		t.Fatal(err)
	}
	expectDurable(t, log, BoundaryDeliveryReceived)
	before := len(log.boundaries())
	if state, err := store.Receive(delivery); err != nil || state != Received {
		t.Fatalf("duplicate Receive = %v,%v", state, err)
	}
	if got := log.boundaries(); len(got) != before {
		t.Fatalf("duplicate custody probe published %v", got[before:])
	}
	outputs, outboxes := exactProcessedRecords(t, topology, assignment, delivery)
	if err := store.MarkProcessed(delivery.ID, outputs, outboxes); err != nil {
		t.Fatal(err)
	}
	expectDurable(t, log, BoundaryDeliveryProcessed)
	if err := store.MarkOutboxDispatched(outboxes[0].ID, 5); err != nil {
		t.Fatal(err)
	}
	expectDurable(t, log, BoundaryOutboxDispatched)
	if err := store.MarkOutboxAccepted(outboxes[0].ID, 9); err != nil {
		t.Fatal(err)
	}
	expectDurable(t, log, BoundaryOutboxAccepted)
	for _, outbox := range outboxes {
		if err := store.MarkOutboxCompleted(outbox.ID); err != nil {
			t.Fatal(err)
		}
		expectDurable(t, log, BoundaryOutboxCompleted)
	}
	if err := store.MarkCompleted(delivery.ID); err != nil {
		t.Fatal(err)
	}
	expectDurable(t, log, BoundaryDeliveryCompleted)
	record, provenance := domainResult(t, topology, assignment, epoch, 0)
	if err := store.UpsertResult(record, provenance); err != nil {
		t.Fatal(err)
	}
	expectDurable(t, log, BoundaryResultUpserted)
	if err := store.PersistEvent(domainFailureEvent(store, assignment, epoch, 1)); err != nil {
		t.Fatal(err)
	}
	expectDurable(t, log, BoundaryEventPersisted)
	replaceAssignmentStateForTest(t, store, assignment, topology, 2, model.Closed, epoch)
	expectDurable(t, log, BoundaryAssignmentClosed)
	if err := store.AcknowledgeEvents(1); err != nil {
		t.Fatal(err)
	}
	expectDurable(t, log, BoundaryEventsAcknowledged)
	replaceAssignmentStateForTest(t, store, assignment, topology, 2, model.Running, epoch)
	notice := model.CheckpointNotice{JobID: assignment.JobID, Source: delivery.ID.Tuple.SourceTask, Watermark: delivery.ID.Tuple.SourceSequence, RaftIndex: 9, Epoch: epoch}
	persistCompletionForCheckpoint(t, store, notice)
	if err := store.ApplyCheckpoint(notice); err != nil {
		t.Fatal(err)
	}
	expectDurable(t, log, BoundaryCheckpointApplied)

	repair := domainRepair(t, topology, assignment, epoch, identity.NodeID, store.WorkerEpoch())
	if err := store.UpsertRepair(repair); err != nil {
		t.Fatal(err)
	}
	expectDurable(t, log, BoundaryRepairPending)
	progress := repair
	progress.State = RepairStreaming
	if err := store.UpsertRepair(progress); err != nil {
		t.Fatal(err)
	}
	expectDurable(t, log, BoundaryRepairStreaming)

	var source model.AssignmentToken
	for _, token := range assignment.Tasks {
		if token.Task.StageID == 1 && token.WorkerID == identity.NodeID {
			source = token
		}
	}
	sourceOutbox := domainSourceOutbox(t, topology, assignment, epoch, source, 2)
	before = len(log.boundaries())
	if err := store.AdvanceSource(SourceCursor{Source: source.Task, NextSequence: 3, EOF: 3}, []OutboxRecord{sourceOutbox}); err == nil {
		expectDurable(t, log, BoundarySourceAdvanced)
	} else if got := log.boundaries(); len(got) != before {
		t.Fatalf("rejected AdvanceSource published %v", got[before:])
	}
}

// rearmableFault fires once per arming at exactly one boundary.
type rearmableFault struct {
	point FaultPoint
	armed bool
	err   error
}

func (fault *rearmableFault) Inject(point FaultPoint) error {
	if fault.armed && point == fault.point {
		fault.armed = false
		return fault.err
	}
	return nil
}

func TestDurableBoundaryNeverFiresOnFailedOrRejectedDurability(t *testing.T) {
	log := &orderLog{}
	fault := &rearmableFault{err: errors.New("injected")}
	store, identity := openOrderedStore(t, log, fault)
	topology, assignment, epoch := domainAssignment(t, store.WorkerEpoch(), identity.NodeID)
	if err := store.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	published := len(log.boundaries())

	// Validation rejection before any write: no boundary.
	bad := domainDelivery(t, topology, assignment, epoch)
	bad.Destination.WorkerEpoch = model.WorkerEpoch{0xff}
	if _, err := store.Receive(bad); err == nil {
		t.Fatal("foreign destination accepted")
	}
	if got := log.boundaries(); len(got) != published {
		t.Fatalf("rejected Receive published %v", got[published:])
	}

	// Failure before the WAL append: no boundary, store still usable.
	fault.point, fault.armed = FaultBeforeAppend, true
	delivery := domainDelivery(t, topology, assignment, epoch)
	if _, err := store.Receive(delivery); !errors.Is(err, fault.err) {
		t.Fatalf("Receive with append fault = %v", err)
	}
	if got := log.boundaries(); len(got) != published {
		t.Fatalf("failed append published %v", got[published:])
	}

	// Failure after the append but before fsync poisons the store: no
	// boundary, and nothing later can fire either.
	fault.point, fault.armed = FaultBeforeSync, true
	if _, err := store.Receive(delivery); !errors.Is(err, fault.err) {
		t.Fatalf("Receive with sync fault = %v", err)
	}
	if got := log.boundaries(); len(got) != published {
		t.Fatalf("failed sync published %v", got[published:])
	}
	if _, err := store.Receive(delivery); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("poisoned store Receive = %v", err)
	}
	if got := log.boundaries(); len(got) != published {
		t.Fatalf("poisoned store published %v", got[published:])
	}
}

func TestNilHookDefaultsToProductionNoop(t *testing.T) {
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 1}
	store, err := Open(t.TempDir()+"/worker", identity, Options{MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, ok := store.options.Hook.(integrationhook.Noop); !ok {
		t.Fatalf("default hook = %T, want integrationhook.Noop", store.options.Hook)
	}
	if err := store.Fence(model.CoordinatorEpoch{Term: 1, BeginIndex: 1, Coordinator: 1, Nonce: [16]byte{1}}); err != nil {
		t.Fatal(err)
	}
}
