package store

import (
	"errors"
	"reflect"
	"testing"

	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
)

func TestTask15ProbeDeliveryReturnsExactDurableStateAfterFenceAdvance(t *testing.T) {
	for _, test := range []struct {
		name  string
		state DeliveryState
	}{
		{name: "received", state: Received},
		{name: "processed", state: Processed},
		{name: "completed", state: Completed},
		{name: "compacted", state: Compacted},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, identity, _ := openDomainStore(t, 16<<20)
			topology, assignment, epoch := domainAssignment(t, store.WorkerEpoch(), identity.NodeID)
			if err := store.Fence(epoch); err != nil {
				t.Fatal(err)
			}
			if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
				t.Fatal(err)
			}
			delivery := domainDelivery(t, topology, assignment, epoch)
			if _, err := store.Receive(delivery); err != nil {
				t.Fatal(err)
			}
			if test.state >= Processed {
				outputs, outboxes := exactProcessedRecords(t, topology, assignment, delivery)
				if err := store.MarkProcessed(delivery.ID, outputs, outboxes); err != nil {
					t.Fatal(err)
				}
				if test.state >= Completed {
					for _, outbox := range outboxes {
						if err := store.MarkOutboxCompleted(outbox.ID); err != nil {
							t.Fatal(err)
						}
					}
					if err := store.MarkCompleted(delivery.ID); err != nil {
						t.Fatal(err)
					}
				}
			}
			if test.state == Compacted {
				if err := store.ApplyCheckpoint(model.CheckpointNotice{JobID: assignment.JobID, Source: delivery.ID.Tuple.SourceTask, Watermark: delivery.ID.Tuple.SourceSequence, RaftIndex: 11, Epoch: epoch}); err != nil {
					t.Fatal(err)
				}
			}
			newer := epoch
			newer.Term++
			newer.BeginIndex++
			newer.Nonce[0]++
			if err := store.Fence(newer); err != nil {
				t.Fatal(err)
			}

			before := mustRecoverWork(t, store)
			state, found, err := store.ProbeDelivery(delivery)
			if err != nil || !found || state != test.state {
				t.Fatalf("ProbeDelivery = %v,%v,%v, want %v,true,nil", state, found, err, test.state)
			}
			changed := delivery.Clone()
			changed.Tuple.Fields[0].Value.Int64++
			if _, _, err = store.ProbeDelivery(changed); !errors.Is(err, model.ErrIdentityReuse) {
				t.Fatalf("changed duplicate = %v", err)
			}
			if after := mustRecoverWork(t, store); !reflect.DeepEqual(after, before) {
				t.Fatal("delivery probe mutated durable work")
			}
		})
	}
}

func TestTask15CompactedProbeFailsClosedAfterAssignmentReplacement(t *testing.T) {
	store, identity, _ := openDomainStore(t, 16<<20)
	topology, assignment, epoch := domainAssignment(t, store.WorkerEpoch(), identity.NodeID)
	if err := store.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	delivery := domainDelivery(t, topology, assignment, epoch)
	if _, err := store.Receive(delivery); err != nil {
		t.Fatal(err)
	}
	outputs, outboxes := exactProcessedRecords(t, topology, assignment, delivery)
	if err := store.MarkProcessed(delivery.ID, outputs, outboxes); err != nil {
		t.Fatal(err)
	}
	for _, outbox := range outboxes {
		if err := store.MarkOutboxCompleted(outbox.ID); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.MarkCompleted(delivery.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyCheckpoint(model.CheckpointNotice{JobID: assignment.JobID, Source: delivery.ID.Tuple.SourceTask, Watermark: delivery.ID.Tuple.SourceSequence, RaftIndex: 11, Epoch: epoch}); err != nil {
		t.Fatal(err)
	}

	newer := epoch
	newer.Term++
	newer.BeginIndex++
	newer.Nonce[0]++
	if err := store.Fence(newer); err != nil {
		t.Fatal(err)
	}
	tokens := append([]model.AssignmentToken(nil), assignment.Tasks...)
	for index := range tokens {
		tokens[index].AssignmentRevision++
	}
	replacement, err := model.NewAssignmentSet(assignment.JobID, assignment.Revision+1, tokens, assignment.ResultReplicas, topology)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.InstallAssignment(replacement, topology.Spec(), 2, model.Running, newer); err != nil {
		t.Fatal(err)
	}

	before := mustRecoverWork(t, store)
	if state, found, probeErr := store.ProbeDelivery(delivery); state != 0 || !found || !errors.Is(probeErr, ErrHistoricalAuthorityUnavailable) {
		t.Fatalf("old exact compacted authority = %v,%v,%v", state, found, probeErr)
	}
	changed := delivery.Clone()
	changed.Tuple.Fields[0].Value.Int64++
	if _, found, probeErr := store.ProbeDelivery(changed); !found || !errors.Is(probeErr, model.ErrIdentityReuse) {
		t.Fatalf("changed compacted logical bytes = found %v, error %v", found, probeErr)
	}
	if after := mustRecoverWork(t, store); !reflect.DeepEqual(after, before) {
		t.Fatal("post-frontier compacted probes mutated durable work")
	}
}

func TestTask15ProcessedReplayRejectsMutableOutboxPhase(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(*OutboxRecord)
	}{
		{name: "accepted", mutate: func(outbox *OutboxRecord) {
			outbox.Accepted = true
			outbox.RetryDeadlineUnixNano = 17
		}},
		{name: "dispatch deadline", mutate: func(outbox *OutboxRecord) {
			outbox.RetryDeadlineUnixNano = 17
		}},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			for _, rawCompleted := range []bool{false, true} {
				name := "processed"
				if rawCompleted {
					name = "completed"
				}
				t.Run(name, func(t *testing.T) {
					store, identity, _ := openDomainStore(t, 16<<20)
					topology, assignment, epoch := domainAssignment(t, store.WorkerEpoch(), identity.NodeID)
					if err := store.Fence(epoch); err != nil {
						t.Fatal(err)
					}
					if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
						t.Fatal(err)
					}
					delivery := domainDelivery(t, topology, assignment, epoch)
					if _, err := store.Receive(delivery); err != nil {
						t.Fatal(err)
					}
					outputs, outboxes := exactProcessedRecords(t, topology, assignment, delivery)
					if err := store.MarkProcessed(delivery.ID, outputs, outboxes); err != nil {
						t.Fatal(err)
					}
					altered := make([]OutboxRecord, len(outboxes))
					for index := range outboxes {
						altered[index] = outboxes[index].Clone()
					}
					mutation.mutate(&altered[0])
					before := mustRecoverWork(t, store)
					if !rawCompleted {
						if err := store.MarkProcessed(delivery.ID, outputs, altered); !errors.Is(err, model.ErrIdentityReuse) {
							t.Fatalf("API replay with mutable phase = %v", err)
						}
					} else {
						for _, outbox := range outboxes {
							if err := store.MarkOutboxCompleted(outbox.ID); err != nil {
								t.Fatal(err)
							}
						}
						if err := store.MarkCompleted(delivery.ID); err != nil {
							t.Fatal(err)
						}
						before = mustRecoverWork(t, store)
					}
					replay := delivery.Clone()
					replay.State = Processed
					replay.Outputs = cloneTuples(outputs)
					payload, err := encodeDeliveryRecord(replay, altered)
					if err != nil {
						t.Fatal(err)
					}
					if err = store.Commit(Transaction{Records: []Record{{Type: recordDeliveryProcessed, Payload: payload}}}); !errors.Is(err, ErrInvalidTransaction) {
						t.Fatalf("raw replay with mutable phase = %v", err)
					}
					if after := mustRecoverWork(t, store); !reflect.DeepEqual(after, before) {
						t.Fatal("mutable processed replay changed durable work")
					}
				})
			}
		})
	}
}

func TestTask15MarkProcessedCompletedIsIdempotentAndExact(t *testing.T) {
	store, identity, _ := openDomainStore(t, 16<<20)
	topology, assignment, epoch := domainAssignment(t, store.WorkerEpoch(), identity.NodeID)
	if err := store.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	delivery := domainDelivery(t, topology, assignment, epoch)
	if _, err := store.Receive(delivery); err != nil {
		t.Fatal(err)
	}
	outputs, outboxes := exactProcessedRecords(t, topology, assignment, delivery)
	if err := store.MarkProcessed(delivery.ID, outputs, outboxes); err != nil {
		t.Fatal(err)
	}
	for _, outbox := range outboxes {
		if err := store.MarkOutboxCompleted(outbox.ID); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.MarkCompleted(delivery.ID); err != nil {
		t.Fatal(err)
	}
	baseline := mustRecoverWork(t, store)
	if err := store.MarkProcessed(delivery.ID, cloneTuples(outputs), cloneZeroPhaseOutboxes(outboxes)); err != nil {
		t.Fatalf("exact completed MarkProcessed retry = %v", err)
	}
	if after := mustRecoverWork(t, store); !reflect.DeepEqual(after, baseline) {
		t.Fatal("exact completed MarkProcessed retry mutated durable work")
	}

	mutations := []struct {
		name   string
		mutate func([]model.Tuple, []OutboxRecord)
	}{
		{name: "completed phase", mutate: func(_ []model.Tuple, outboxes []OutboxRecord) { outboxes[0].Completed = true }},
		{name: "accepted phase", mutate: func(_ []model.Tuple, outboxes []OutboxRecord) { outboxes[0].Accepted = true }},
		{name: "retry deadline", mutate: func(_ []model.Tuple, outboxes []OutboxRecord) { outboxes[0].RetryDeadlineUnixNano = 17 }},
		{name: "immutable outbox", mutate: func(_ []model.Tuple, outboxes []OutboxRecord) { outboxes[0].Tuple.Fields[0].Value.Int64++ }},
		{name: "output", mutate: func(outputs []model.Tuple, _ []OutboxRecord) { outputs[0].Fields[0].Value.Int64++ }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			candidateOutputs := cloneTuples(outputs)
			candidateOutboxes := cloneZeroPhaseOutboxes(outboxes)
			mutation.mutate(candidateOutputs, candidateOutboxes)
			if err := store.MarkProcessed(delivery.ID, candidateOutputs, candidateOutboxes); !errors.Is(err, model.ErrIdentityReuse) {
				t.Fatalf("completed MarkProcessed changed retry = %v", err)
			}
			if after := mustRecoverWork(t, store); !reflect.DeepEqual(after, baseline) {
				t.Fatal("changed completed MarkProcessed retry mutated durable work")
			}
		})
	}
}

func cloneZeroPhaseOutboxes(outboxes []OutboxRecord) []OutboxRecord {
	cloned := make([]OutboxRecord, len(outboxes))
	for index := range outboxes {
		cloned[index] = outboxes[index].Clone()
		cloned[index].Completed = false
		cloned[index].Accepted = false
		cloned[index].RetryDeadlineUnixNano = 0
	}
	return cloned
}

func TestTask15OutboxRetryPhaseAndDeadlineSurviveSnapshotReopen(t *testing.T) {
	path := t.TempDir() + "/worker"
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 1}
	options := Options{MaxBytes: 16 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return model.WorkerEpoch{7}, nil }}
	store, err := Open(path, identity, options)
	if err != nil {
		t.Fatal(err)
	}
	topology, assignment, epoch := domainAssignment(t, store.WorkerEpoch(), identity.NodeID)
	if err = store.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err = store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	var source model.AssignmentToken
	for _, token := range assignment.Tasks {
		if token.WorkerID == identity.NodeID && token.WorkerEpoch == store.WorkerEpoch() && token.Task.StageID == 1 {
			source = token
			break
		}
	}
	outbox := domainSourceOutbox(t, topology, assignment, epoch, source, 1)
	if err = store.AdvanceSource(SourceCursor{Source: source.Task, NextSequence: 2, EOF: 3}, []OutboxRecord{outbox}); err != nil {
		t.Fatal(err)
	}
	if err = store.MarkOutboxDispatched(outbox.ID, -990_000_000); err != nil {
		t.Fatal(err)
	}
	if err = store.MarkOutboxAccepted(outbox.ID, -950_000_000); err != nil {
		t.Fatal(err)
	}
	beforeInvalid := mustRecoverWork(t, store)
	if err = store.MarkOutboxAccepted(outbox.ID, -940_000_000); !errors.Is(err, model.ErrIdentityReuse) {
		t.Fatalf("changed Accepted retry = %v", err)
	}
	if afterInvalid := mustRecoverWork(t, store); !reflect.DeepEqual(afterInvalid, beforeInvalid) {
		t.Fatal("invalid retry transition mutated durable work")
	}
	if err = store.MarkOutboxCompleted(outbox.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, identity, Options{MaxBytes: options.MaxBytes})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	work := mustRecoverWork(t, reopened)
	if len(work.Outboxes) != 1 || !work.Outboxes[0].Accepted || work.Outboxes[0].RetryDeadlineUnixNano != -950_000_000 || !work.Outboxes[0].Completed {
		t.Fatalf("recovered retry metadata = %+v", work.Outboxes)
	}
}

func TestTask15OutboxCodecReadsV1AndRejectsUnsetAcceptedV2(t *testing.T) {
	store, identity, _ := openDomainStore(t, 16<<20)
	topology, assignment, epoch := domainAssignment(t, store.WorkerEpoch(), identity.NodeID)
	var source model.AssignmentToken
	for _, token := range assignment.Tasks {
		if token.WorkerID == identity.NodeID && token.WorkerEpoch == store.WorkerEpoch() && token.Task.StageID == 1 {
			source = token
			break
		}
	}
	outbox := domainSourceOutbox(t, topology, assignment, epoch, source, 1)
	message := protocol.TupleDelivery{DeliveryID: outbox.ID, Tuple: outbox.Tuple, Producer: outbox.Producer, Destination: outbox.Destination, Assignment: protocol.AssignmentSetIdentity{JobID: outbox.ID.Tuple.JobID, Revision: outbox.AssignmentRevision, Digest: outbox.AssignmentDigest}, Coordinator: outbox.CoordinatorEpoch}
	encodedMessage, err := protocol.MarshalTupleDelivery(message)
	if err != nil {
		t.Fatal(err)
	}
	v1 := newRecordWriter()
	v1.u16(domainRecordSchema)
	v1.u8(0)
	v1.blob(encodedMessage)
	decoded, err := decodeOutbox(v1.bytes())
	if err != nil {
		t.Fatal(err)
	}
	if !equalOutboxDefinition(decoded, outbox) || decoded.Accepted || decoded.RetryDeadlineUnixNano != 0 {
		t.Fatalf("decoded v1 outbox = %+v", decoded)
	}
	v2, err := encodeOutbox(outbox)
	if err != nil {
		t.Fatal(err)
	}
	v2[3] = 1
	if _, err = decodeOutbox(v2); err == nil {
		t.Fatal("accepted v2 outbox with zero retry deadline decoded")
	}
}
