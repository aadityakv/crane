package worker

import (
	"context"
	"testing"

	"github.com/aadityakv/crane/internal/crane/admission"
	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
	"github.com/aadityakv/crane/internal/crane/store"
)

// readoptFixture rebinds the fixture installation under a strictly newer
// committed fence (identical assignment content) exactly as a leadership
// change plus coordinator re-install does, and returns the newer fence.
func readoptFixture(t *testing.T, repository *fakeRepository, fixture workerTestFixture) model.CoordinatorEpoch {
	t.Helper()
	newer := fixture.epoch
	newer.Term++
	newer.BeginIndex++
	newer.Nonce[0]++
	rebound := fixture.assignment
	rebound.CoordinatorEpoch = newer
	job := fixture.assignment.Assignment.JobID
	repository.mu.Lock()
	repository.assignments[job] = rebound
	repository.work.Assignments = []store.InstalledAssignment{rebound}
	repository.work.Fence = newer
	repository.mu.Unlock()
	return newer
}

// TestRecoveredReceivedReadoptsUnderCurrentFence pins the defect #5 ruling's
// execution half: a recovered Received delivery whose durable record carries a
// superseded fence but whose delivery definition re-validates byte-exactly
// against the CURRENT installed assignment re-enters execution, and every
// emission derived from it (outbox records and outbound messages) carries the
// CURRENT coordinator, never the superseded one. A record whose assignment was
// genuinely replaced never re-enters.
func TestRecoveredReceivedReadoptsUnderCurrentFence(t *testing.T) {
	fixture := workerFixture(t)
	repository := newFakeRepository(fixture)
	newer := readoptFixture(t, repository, fixture)
	retained := fixture.delivery(t, 4)
	replaced := fixture.delivery(t, 5)
	replaced.AssignmentRevision++
	replaced.AssignmentDigest[0] ^= 0xFF
	repository.mu.Lock()
	repository.work.Deliveries = []store.DeliveryRecord{retained, replaced}
	repository.deliveries[retained.ID] = retained
	repository.deliveries[replaced.ID] = replaced
	repository.mu.Unlock()

	gate := admission.NewGate()
	if err := gate.Open(newer); err != nil {
		t.Fatal(err)
	}
	sender := &fakeSender{}
	engine, err := NewEngine(testEngineOptions(repository, gate, sender))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	waitFor(t, func() bool {
		repository.mu.Lock()
		defer repository.mu.Unlock()
		record, ok := repository.deliveries[retained.ID]
		return ok && record.State == store.Processed
	})

	repository.mu.Lock()
	processed := repository.deliveries[retained.ID]
	untouched := repository.deliveries[replaced.ID]
	for _, outboxID := range processed.OutboxIDs {
		outbox := repository.outboxes[outboxID]
		if outbox.CoordinatorEpoch != newer {
			repository.mu.Unlock()
			t.Fatalf("derived outbox carries the superseded fence: %#v", outbox)
		}
	}
	repository.mu.Unlock()
	if len(processed.OutboxIDs) == 0 {
		t.Fatalf("readopted execution derived no outboxes: %#v", processed)
	}
	if untouched.State != store.Received {
		t.Fatalf("replaced-assignment record re-entered execution: %#v", untouched)
	}

	waitFor(t, func() bool { return sender.count() >= len(processed.OutboxIDs) })
	for _, message := range sender.deliveries {
		if message.Coordinator != newer {
			t.Fatalf("outbound emission carries the superseded fence: %#v", message)
		}
	}
	cancel()
	<-done
}

// TestRetainedOutboxReDerivesEmissionsUnderCurrentFence pins the sender-side
// half: a retained derived outbox whose durable record carries the superseded
// fence but whose assignment identity still matches the current installation
// re-sends under the CURRENT fence, and the receiver's current-fence ACK binds
// the durable outbox. The exact retained-brand ACK keeps binding it too.
func TestRetainedOutboxReDerivesEmissionsUnderCurrentFence(t *testing.T) {
	fixture := workerFixture(t)
	repository := newFakeRepository(fixture)
	newer := readoptFixture(t, repository, fixture)
	tuple, exists, err := model.SourceTuple(fixture.topology, fixture.source.Task, 1)
	if err != nil || !exists {
		t.Fatalf("SourceTuple = %t,%v", exists, err)
	}
	retained, err := deriveSourceOutboxes(fixture.assignment, fixture.source, 1, tuple)
	if err != nil {
		t.Fatal(err)
	}
	repository.mu.Lock()
	repository.work.Outboxes = retained
	for _, outbox := range retained {
		repository.outboxes[outbox.ID] = outbox
	}
	repository.mu.Unlock()

	gate := admission.NewGate()
	if err := gate.Open(newer); err != nil {
		t.Fatal(err)
	}
	sender := &fakeSender{}
	engine, err := NewEngine(testEngineOptions(repository, gate, sender))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	<-engine.Ready()
	waitFor(t, func() bool { return sender.count() >= 1 })
	sender.mu.Lock()
	messages := append([]protocol.TupleDelivery(nil), sender.deliveries...)
	sender.mu.Unlock()
	for _, message := range messages {
		if message.Coordinator != newer {
			t.Fatalf("re-sent emission carries the superseded fence: %#v", message)
		}
	}
	identity := protocol.AssignmentSetIdentity{JobID: retained[0].ID.Tuple.JobID, Revision: retained[0].AssignmentRevision, Digest: retained[0].AssignmentDigest}
	currentACK := protocol.TupleACK{DeliveryID: retained[0].ID, Destination: retained[0].Destination, Assignment: identity, Coordinator: newer, Status: protocol.TupleAccepted}
	if err := engine.HandleACK(ctx, currentACK); err != nil {
		t.Fatalf("current-fence ACK refused: %v", err)
	}
	repository.mu.Lock()
	accepted := repository.outboxes[retained[0].ID].Accepted
	repository.mu.Unlock()
	if !accepted {
		t.Fatal("current-fence ACK did not bind the durable outbox")
	}
	cancel()
	<-done
}
