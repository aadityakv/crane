package store

import (
	"errors"
	"testing"

	"github.com/aadityakv/crane/internal/crane/model"
)

// TestMarkProcessedReadoptsRetainedDeliveryUnderCurrentFence pins the defect
// #5 ruling's durable half: a retained Received delivery published under a
// superseded fence whose assignment identity still matches the current
// installation is re-derived under the CURRENT fence — MarkProcessed accepts
// the current-fence-stamped topology-derived outboxes, persists the delivery
// Processed with those outboxes, and replays idempotently. Genuinely replaced
// assignment identities and unrelated fences stay rejected.
func TestMarkProcessedReadoptsRetainedDeliveryUnderCurrentFence(t *testing.T) {
	store, _, identity, _ := rebindStoreForTest(t)
	topology, assignment, _, newer, delivery := seedRebindWorkForTest(t, store, identity, 1)
	if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, newer); err != nil {
		t.Fatalf("rebind under newer committed epoch: %v", err)
	}
	readopted := delivery
	readopted.CoordinatorEpoch = newer
	outputs, outboxes := exactProcessedRecords(t, topology, assignment, readopted)
	for _, outbox := range outboxes {
		if outbox.CoordinatorEpoch != newer || outbox.AssignmentRevision != assignment.Revision || outbox.AssignmentDigest != assignment.Digest {
			t.Fatalf("fixture outbox not current-fence derived: %#v", outbox)
		}
	}
	if err := store.MarkProcessed(delivery.ID, outputs, outboxes); err != nil {
		t.Fatalf("MarkProcessed of retained custody under current fence: %v", err)
	}
	if err := store.MarkProcessed(delivery.ID, outputs, outboxes); err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	work := mustRecoverWork(t, store)
	if len(work.Deliveries) != 1 || work.Deliveries[0].State != Processed || len(work.Deliveries[0].OutboxIDs) != len(outboxes) {
		t.Fatalf("processed deliveries=%+v", work.Deliveries)
	}
	if len(work.Outboxes) != len(outboxes) {
		t.Fatalf("durable outboxes=%d want %d", len(work.Outboxes), len(outboxes))
	}
	for _, outbox := range work.Outboxes {
		if outbox.CoordinatorEpoch != newer {
			t.Fatalf("durable outbox carries the superseded fence: %#v", outbox)
		}
	}

	// A genuinely replaced assignment identity never re-enters: outboxes
	// derived under a non-matching revision are refused.
	other := domainDeliverySequence(t, topology, assignment, newer, 2)
	if _, err := store.Receive(other); err != nil {
		t.Fatalf("seed replaced-identity delivery: %v", err)
	}
	replacedOutputs, replacedOutboxes := exactProcessedRecords(t, topology, assignment, other)
	for index := range replacedOutboxes {
		replacedOutboxes[index].AssignmentDigest[0] ^= 0xFF
	}
	if err := store.MarkProcessed(other.ID, replacedOutputs, replacedOutboxes); err == nil {
		t.Fatal("replaced-identity processing accepted")
	}

	// An unrelated fence (neither the retained nor the current one) is not a
	// re-adoption.
	stray := domainDeliverySequence(t, topology, assignment, newer, 3)
	if _, err := store.Receive(stray); err != nil {
		t.Fatalf("seed stray-fence delivery: %v", err)
	}
	strayFence := newer
	strayFence.Term += 5
	strayCurrent := stray
	strayCurrent.CoordinatorEpoch = strayFence
	strayOutputs, strayOutboxes := exactProcessedRecords(t, topology, assignment, strayCurrent)
	if err := store.MarkProcessed(stray.ID, strayOutputs, strayOutboxes); err == nil {
		t.Fatal("unrelated-fence outboxes accepted")
	}
}

// TestProbeDeliveryReadoptsCurrentFenceRebrandOfRetainedCustody pins the
// receiver-side idempotency oracle: a retained custody record answers a
// current-fence-rebranded replay of its exact logical definition, while any
// genuinely different definition and any epoch other than the retained or
// current one stays identity reuse.
func TestProbeDeliveryReadoptsCurrentFenceRebrandOfRetainedCustody(t *testing.T) {
	store, _, identity, _ := rebindStoreForTest(t)
	topology, assignment, _, newer, delivery := seedRebindWorkForTest(t, store, identity, 1)
	if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, newer); err != nil {
		t.Fatalf("rebind under newer committed epoch: %v", err)
	}
	rebrand := delivery
	rebrand.CoordinatorEpoch = newer
	state, found, err := store.ProbeDelivery(rebrand)
	if err != nil || !found || state != Received {
		t.Fatalf("current-fence rebrand probe = %v,%t,%v", state, found, err)
	}

	changed := rebrand
	changed.Tuple = cloneTuple(rebrand.Tuple)
	changed.Tuple.Fields[0].Value.Int64++
	if _, _, err := store.ProbeDelivery(changed); !errors.Is(err, model.ErrIdentityReuse) {
		t.Fatalf("changed definition probe = %v", err)
	}
	stray := delivery
	stray.CoordinatorEpoch = newer
	stray.CoordinatorEpoch.Term += 5
	if _, _, err := store.ProbeDelivery(stray); !errors.Is(err, model.ErrIdentityReuse) {
		t.Fatalf("unrelated-epoch probe = %v", err)
	}

	// The exact retained-brand replay keeps answering without the rebind.
	state, found, err = store.ProbeDelivery(delivery)
	if err != nil || !found || state != Received {
		t.Fatalf("retained-brand probe = %v,%t,%v", state, found, err)
	}
}
