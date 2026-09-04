package store

import (
	"errors"
	"reflect"
	"testing"

	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
)

func TestDeliveryProbeDeliveryReturnsExactDurableStateAfterFenceAdvance(t *testing.T) {
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
				notice := model.CheckpointNotice{JobID: assignment.JobID, Source: delivery.ID.Tuple.SourceTask, Watermark: delivery.ID.Tuple.SourceSequence, RaftIndex: 11, Epoch: epoch}
				persistCompletionForCheckpoint(t, store, notice)
				if err := store.ApplyCheckpoint(notice); err != nil {
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

func TestDeliveryCompactedProbeFailsClosedAfterAssignmentReplacement(t *testing.T) {
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
	notice := model.CheckpointNotice{JobID: assignment.JobID, Source: delivery.ID.Tuple.SourceTask, Watermark: delivery.ID.Tuple.SourceSequence, RaftIndex: 11, Epoch: epoch}
	persistCompletionForCheckpoint(t, store, notice)
	if err := store.ApplyCheckpoint(notice); err != nil {
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

func TestDeliveryProcessedReplayRejectsMutableOutboxPhase(t *testing.T) {
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

func TestDeliveryMarkProcessedCompletedIsIdempotentAndExact(t *testing.T) {
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

func TestDeliveryCheckpointRequiresDurableCompletionAndAdvancesRevisionOnce(t *testing.T) {
	newCompleted := func(t *testing.T) (*Store, model.AssignmentSet, model.CoordinatorEpoch, DeliveryRecord, model.AssignmentToken) {
		t.Helper()
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
		var source model.AssignmentToken
		for _, token := range assignment.Tasks {
			if token.Task == delivery.ID.Tuple.SourceTask {
				source = token
				break
			}
		}
		return store, assignment, epoch, delivery, source
	}
	noticeFor := func(assignment model.AssignmentSet, epoch model.CoordinatorEpoch, delivery DeliveryRecord) model.CheckpointNotice {
		return model.CheckpointNotice{JobID: assignment.JobID, Source: delivery.ID.Tuple.SourceTask, Watermark: delivery.ID.Tuple.SourceSequence, RaftIndex: 11, Epoch: epoch}
	}

	t.Run("adopted without completion event", func(t *testing.T) {
		store, assignment, epoch, delivery, source := newCompleted(t)
		// A current-fence notice whose watermark strictly exceeds the (absent)
		// durable cursor is the coordinator's authoritative committed-watermark
		// statement (Task 24 defect #2 ruling): the store adopts it under the
		// current authority proof with no local CompletionReport and compacts.
		if err := store.ApplyCheckpoint(noticeFor(assignment, epoch, delivery)); err != nil {
			t.Fatalf("committed watermark without a durable completion event was not adopted: %v", err)
		}
		work := mustRecoverWork(t, store)
		if len(work.Sources) != 1 || work.Sources[0].Watermark != delivery.ID.Tuple.SourceSequence || work.Sources[0].RaftIndex != 11 || work.Sources[0].CheckpointRevision != 1 {
			t.Fatalf("adopted cursor = %+v", work.Sources)
		}
		wantAuthority := CheckpointAuthority{JobControlRevision: 1, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest, SourceToken: source, CoordinatorEpoch: epoch}
		if work.Sources[0].CheckpointAuthority != wantAuthority {
			t.Fatalf("adopted authority=%+v want %+v", work.Sources[0].CheckpointAuthority, wantAuthority)
		}
		if len(work.Deliveries) != 0 {
			t.Fatalf("adoption must compact the covered delivery: %+v", work.Deliveries)
		}
	})

	t.Run("superseded event", func(t *testing.T) {
		store, assignment, epoch, delivery, source := newCompleted(t)
		report := model.CompletionReport{JobID: assignment.JobID, JobControlRevision: 1, AssignmentRevision: assignment.Revision, Source: source.Task, Token: source, Epoch: epoch, ExpectedCheckpointRevision: 0, Prior: 0, New: delivery.ID.Tuple.SourceSequence, EOF: 3, WorkerTransactionID: 1}
		report.Digest = model.CompletionReportDigest(report)
		event := model.WorkerEvent{WorkerID: source.WorkerID, WorkerEpoch: source.WorkerEpoch, TransactionID: 1, Kind: model.WorkerEventCompletion, Completion: &report}
		if err := store.PersistEvent(event); err != nil {
			t.Fatal(err)
		}
		before := mustRecoverWork(t, store)
		if err := store.AcknowledgeEvents(1); err == nil {
			t.Fatal("current unapplied completion event acknowledged")
		}
		if after := mustRecoverWork(t, store); !reflect.DeepEqual(after, before) {
			t.Fatal("rejected current completion ack mutated work")
		}
		tokens := append([]model.AssignmentToken(nil), assignment.Tasks...)
		for index := range tokens {
			tokens[index].AssignmentRevision++
		}
		replacement, err := model.NewAssignmentSet(assignment.JobID, assignment.Revision+1, tokens, assignment.ResultReplicas, before.Assignments[0].Topology)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.InstallAssignment(replacement, before.Assignments[0].Topology.Spec(), 2, model.Running, epoch); err != nil {
			t.Fatal(err)
		}
		if err := store.AcknowledgeEvents(1); err != nil {
			t.Fatalf("strict successor did not prove stale completion: %v", err)
		}
		if work := mustRecoverWork(t, store); len(work.PendingEvents) != 0 {
			t.Fatalf("superseded completion retained: %+v", work.PendingEvents)
		}
	})

	t.Run("correlated", func(t *testing.T) {
		store, assignment, epoch, delivery, source := newCompleted(t)
		report := model.CompletionReport{JobID: assignment.JobID, JobControlRevision: 1, AssignmentRevision: assignment.Revision, Source: source.Task, Token: source, Epoch: epoch, ExpectedCheckpointRevision: 0, Prior: 0, New: delivery.ID.Tuple.SourceSequence, EOF: 3, WorkerTransactionID: 1}
		report.Digest = model.CompletionReportDigest(report)
		event := model.WorkerEvent{WorkerID: source.WorkerID, WorkerEpoch: source.WorkerEpoch, TransactionID: 1, Kind: model.WorkerEventCompletion, Completion: &report}
		if err := store.PersistEvent(event); err != nil {
			t.Fatal(err)
		}
		notice := noticeFor(assignment, epoch, delivery)
		beforeEarlyAck := mustRecoverWork(t, store)
		if err := store.AcknowledgeEvents(1); err == nil {
			t.Fatal("completion event acknowledged before checkpoint")
		}
		if after := mustRecoverWork(t, store); !reflect.DeepEqual(after, beforeEarlyAck) {
			t.Fatal("early completion acknowledgment mutated durable work")
		}
		if err := store.ApplyCheckpoint(notice); err != nil {
			t.Fatal(err)
		}
		work := mustRecoverWork(t, store)
		if len(work.Sources) != 1 || work.Sources[0].CheckpointRevision != 1 || work.Sources[0].Watermark != 1 || work.Sources[0].RaftIndex != 11 {
			t.Fatalf("checkpoint cursor = %+v", work.Sources)
		}
		wantAuthority := CheckpointAuthority{JobControlRevision: 1, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest, SourceToken: source, CoordinatorEpoch: epoch}
		if work.Sources[0].CheckpointAuthority != wantAuthority {
			t.Fatalf("checkpoint authority=%+v want=%+v", work.Sources[0].CheckpointAuthority, wantAuthority)
		}
		if len(work.Deliveries) != 0 || len(work.Outboxes) != 0 || len(work.PendingEvents) != 1 {
			t.Fatalf("checkpoint compaction/event retention = %+v", work)
		}
		beforeDuplicate := work.Clone()
		if err := store.ApplyCheckpoint(notice); err != nil {
			t.Fatalf("exact checkpoint retry = %v", err)
		}
		if after := mustRecoverWork(t, store); !reflect.DeepEqual(after, beforeDuplicate) {
			t.Fatal("exact checkpoint retry changed durable work")
		}
		changed := notice
		changed.RaftIndex++
		if err := store.ApplyCheckpoint(changed); err == nil {
			t.Fatal("same checkpoint watermark with changed Raft identity accepted")
		}
		if after := mustRecoverWork(t, store); !reflect.DeepEqual(after, beforeDuplicate) {
			t.Fatal("changed checkpoint retry mutated durable work")
		}
		legacy := beforeDuplicate.Clone()
		legacy.Sources[0].CheckpointRevision = 0
		legacy.Sources[0].CheckpointAuthority = CheckpointAuthority{}
		if err := applyCheckpoint(&legacy, notice); err != nil {
			t.Fatalf("legacy checkpoint did not migrate from pending proof: %v", err)
		}
		if legacy.Sources[0].CheckpointRevision != 1 || legacy.Sources[0].CheckpointAuthority != wantAuthority {
			t.Fatalf("legacy checkpoint migration=%+v", legacy.Sources[0])
		}
		legacy = beforeDuplicate.Clone()
		legacy.Sources[0].CheckpointRevision = 0
		legacy.Sources[0].CheckpointAuthority = CheckpointAuthority{}
		legacy.PendingEvents = nil
		if err := applyCheckpoint(&legacy, notice); !errors.Is(err, ErrCheckpointAuthorityUnavailable) {
			t.Fatalf("legacy checkpoint without proof=%v", err)
		}

		if err := store.AcknowledgeEvents(1); err != nil {
			t.Fatal(err)
		}
		newEpoch := epoch
		newEpoch.Term++
		newEpoch.BeginIndex++
		newEpoch.Nonce[0]++
		if err := store.Fence(newEpoch); err != nil {
			t.Fatal(err)
		}
		tokens := append([]model.AssignmentToken(nil), assignment.Tasks...)
		for index := range tokens {
			tokens[index].AssignmentRevision++
		}
		replacement, err := model.NewAssignmentSet(assignment.JobID, assignment.Revision+1, tokens, assignment.ResultReplicas, work.Assignments[0].Topology)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.InstallAssignment(replacement, work.Assignments[0].Topology.Spec(), 2, model.Running, newEpoch); err != nil {
			t.Fatal(err)
		}
		beforeHistoricalRetry := mustRecoverWork(t, store)
		if err := store.ApplyCheckpoint(notice); err != nil {
			t.Fatalf("exact prior accepted notice after event ack/reassignment=%v", err)
		}
		if after := mustRecoverWork(t, store); !reflect.DeepEqual(after, beforeHistoricalRetry) {
			t.Fatal("historical exact checkpoint retry mutated durable work")
		}
		changedEpoch := notice
		changedEpoch.Epoch = newEpoch
		if err := store.ApplyCheckpoint(changedEpoch); !errors.Is(err, model.ErrIdentityReuse) {
			t.Fatalf("same checkpoint identity with changed epoch=%v", err)
		}
	})
}

func TestDeliveryLegacySourceSchemaDefaultsCheckpointProofFailClosed(t *testing.T) {
	w := newRecordWriter()
	w.u16(domainRecordSchema)
	source := model.TaskID{JobID: model.JobID{1}, StageID: 1}
	w.task(source)
	w.u64(2)
	w.u64(3)
	w.u64(1)
	w.u64(9)
	w.u16(0)
	cursor, outboxes, err := decodeSource(w.data)
	if err != nil {
		t.Fatal(err)
	}
	if len(outboxes) != 0 || cursor.Source != source || cursor.Watermark != 1 || cursor.RaftIndex != 9 || cursor.CheckpointRevision != 0 || cursor.CheckpointAuthority != (CheckpointAuthority{}) {
		t.Fatalf("legacy cursor=%+v outboxes=%+v", cursor, outboxes)
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

func TestDeliveryOutboxRetryPhaseAndDeadlineSurviveSnapshotReopen(t *testing.T) {
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

func TestDeliveryOutboxCodecReadsV1AndRejectsUnsetAcceptedV2(t *testing.T) {
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
