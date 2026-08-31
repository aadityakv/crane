package state

import (
	"testing"

	"github.com/aaditya/cs425mp3/internal/crane/model"
)

func TestSnapshotRejectsFabricatedMarkerForCurrentEligibleWorker(t *testing.T) {
	machine, jobID, _, assignment := task10AssignedJob(t, 2)
	install, _ := NewInstallAssignments(InternalCommandID{0xc1}, 1, assignment, machine.coordinatorEpoch)
	applyTask10(t, machine, 12, install)

	token := assignment.Tasks[0]
	worker := machine.workers[token.WorkerID]
	record := cloneJobRecord(machine.jobs[jobID])
	record.NeedsReassignment = markersForWorker(assignment, worker.NodeID, worker.Epoch)
	record.JobControlRevision++
	machine.jobs[jobID] = record
	recomputeSnapshotEstimateForReview(t, machine)

	assertCaptureAndRawRestoreRejectSnapshot(t, machine)
}

func TestSnapshotRejectsActiveInvalidationClaimingOverwritingRegistrationRevision(t *testing.T) {
	machine, jobID, _, assignment := task10AssignedJob(t, 4)
	install, _ := NewInstallAssignments(InternalCommandID{0xd2}, 1, assignment, machine.coordinatorEpoch)
	applyTask10(t, machine, 12, install)
	worker := machine.workers[assignment.Tasks[0].WorkerID]
	affected := []AffectedAssignment{{JobID: jobID, JobControlRevision: 2, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest}}
	deactivate, _ := NewDeactivateWorker(InternalCommandID{0xd3}, worker.Revision, worker.NodeID, worker.Epoch, affected, machine.coordinatorEpoch)
	applyTask10(t, machine, 13, deactivate)
	rejoined := machine.workers[worker.NodeID]
	rejoined.State = WorkerEligible
	rejoined.Revision++
	register, _ := NewRegisterWorker(InternalCommandID{0xd4}, rejoined.Revision-1, rejoined, machine.coordinatorEpoch)
	applyTask10(t, machine, 14, register)
	drain, _ := NewDrainWorker(InternalCommandID{0xd5}, rejoined.Revision, rejoined.NodeID, rejoined.Epoch, machine.coordinatorEpoch)
	applyTask10(t, machine, 15, drain)

	record := cloneJobRecord(machine.jobs[jobID])
	if record.invalidationHistory[0].Kind != 0 || record.invalidationHistory[0].WorkerRevision != 0 || machine.workers[worker.NodeID].Revision != 4 {
		t.Fatalf("canonical forgotten worker anchor kind/revision=%d/%d worker=%d, want 0/0 and 4", record.invalidationHistory[0].Kind, record.invalidationHistory[0].WorkerRevision, machine.workers[worker.NodeID].Revision)
	}
	record.invalidationHistory[0].Kind = workerInvalidationDeactivate
	record.invalidationHistory[0].WorkerRevision = 3
	machine.jobs[jobID] = record
	recomputeSnapshotEstimateForReview(t, machine)

	assertCaptureAndRawRestoreRejectSnapshot(t, machine)
}

func TestSnapshotRejectsJointlyShiftedRepairAndRetainedWorkerPredecessor(t *testing.T) {
	machine, worker, _ := round3RepairedDeactivation(t)
	var jobID model.JobID
	for id := range machine.jobs {
		jobID = id
	}
	record := cloneJobRecord(machine.jobs[jobID])
	provenance := &record.invalidationHistory[0]
	if provenance.JobControlRevision != 2 || provenance.RepairJobControlRevision != 4 {
		t.Fatalf("fixture revisions predecessor=%d repair=%d, want 2 and 4", provenance.JobControlRevision, provenance.RepairJobControlRevision)
	}
	provenance.JobControlRevision = 1
	provenance.RepairJobControlRevision = 3
	machine.jobs[jobID] = record

	key := SubjectKey{Kind: SubjectWorker, WorkerID: worker.NodeID}
	history := machine.subjects[key]
	shifted := []AffectedAssignment{{JobID: jobID, JobControlRevision: 1, AssignmentRevision: provenance.AssignmentRevision, AssignmentDigest: provenance.AssignmentDigest}}
	target := deactivateWorkerTarget(DeactivateWorker{WorkerID: worker.NodeID, WorkerEpoch: worker.Epoch, Affected: shifted})
	history.target = append([]byte(nil), target...)
	history.appliedTarget = append([]byte(nil), target...)
	machine.subjects[key] = history
	recomputeSnapshotEstimateForReview(t, machine)

	assertCaptureAndRawRestoreRejectSnapshot(t, machine)
}

func TestSnapshotRejectsCorruptRetainedAffectedFenceAfterRepair(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*AffectedAssignment)
	}{
		{name: "job control revision", mutate: func(item *AffectedAssignment) { item.JobControlRevision-- }},
		{name: "assignment digest", mutate: func(item *AffectedAssignment) { item.AssignmentDigest[0] ^= 0xff }},
	} {
		t.Run(test.name, func(t *testing.T) {
			machine, worker, affected := round3RepairedDeactivation(t)
			test.mutate(&affected[0])

			key := SubjectKey{Kind: SubjectWorker, WorkerID: worker.NodeID}
			history := machine.subjects[key]
			invalid := deactivateWorkerTarget(DeactivateWorker{WorkerID: worker.NodeID, WorkerEpoch: worker.Epoch, Affected: affected})
			history.target = append([]byte(nil), invalid...)
			history.appliedTarget = append([]byte(nil), invalid...)
			machine.subjects[key] = history
			recomputeSnapshotEstimateForReview(t, machine)

			assertCaptureAndRawRestoreRejectSnapshot(t, machine)
		})
	}
}

func TestSnapshotRejectsCorruptInvalidationProvenance(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*invalidationProvenance)
	}{
		{name: "worker revision", mutate: func(provenance *invalidationProvenance) { provenance.WorkerRevision++ }},
		{name: "worker incarnation", mutate: func(provenance *invalidationProvenance) { provenance.WorkerEpoch[0] ^= 0xff }},
		{name: "empty markers", mutate: func(provenance *invalidationProvenance) { provenance.Markers = nil }},
		{name: "partial repair", mutate: func(provenance *invalidationProvenance) {
			provenance.RepairJobControlRevision = provenance.JobControlRevision + 2
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			machine, jobID, _, assignment := task10AssignedJob(t, 4)
			install, _ := NewInstallAssignments(InternalCommandID{0xc0}, 1, assignment, machine.coordinatorEpoch)
			applyTask10(t, machine, 12, install)
			worker := machine.workers[assignment.Tasks[0].WorkerID]
			affected := []AffectedAssignment{{JobID: jobID, JobControlRevision: 2, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest}}
			deactivate, _ := NewDeactivateWorker(InternalCommandID{0xc1}, worker.Revision, worker.NodeID, worker.Epoch, affected, machine.coordinatorEpoch)
			applyTask10(t, machine, 13, deactivate)
			record := cloneJobRecord(machine.jobs[jobID])
			test.mutate(&record.invalidationHistory[0])
			machine.jobs[jobID] = record
			recomputeSnapshotEstimateForReview(t, machine)
			assertCaptureAndRawRestoreRejectSnapshot(t, machine)
		})
	}
}

func TestSnapshotRejectsCorruptRepairPredecessorProvenance(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*invalidationProvenance)
	}{
		{name: "predecessor revision", mutate: func(provenance *invalidationProvenance) { provenance.JobControlRevision-- }},
		{name: "predecessor digest", mutate: func(provenance *invalidationProvenance) { provenance.AssignmentDigest[0] ^= 0xff }},
		{name: "repair revision", mutate: func(provenance *invalidationProvenance) { provenance.RepairJobControlRevision++ }},
		{name: "repair marker digest", mutate: func(provenance *invalidationProvenance) { provenance.RepairMarkersDigest[0] ^= 0xff }},
	} {
		t.Run(test.name, func(t *testing.T) {
			machine, _, _ := round3RepairedDeactivation(t)
			var jobID model.JobID
			for id := range machine.jobs {
				jobID = id
			}
			record := cloneJobRecord(machine.jobs[jobID])
			test.mutate(&record.invalidationHistory[0])
			machine.jobs[jobID] = record
			recomputeSnapshotEstimateForReview(t, machine)
			assertCaptureAndRawRestoreRejectSnapshot(t, machine)
		})
	}
}

func TestRepeatedInvalidationOfAlreadyMarkedIncarnationDoesNotAdvanceJob(t *testing.T) {
	machine, jobID, _, assignment := task10AssignedJob(t, 2)
	install, _ := NewInstallAssignments(InternalCommandID{0xc5}, 1, assignment, machine.coordinatorEpoch)
	applyTask10(t, machine, 12, install)
	token := assignment.Tasks[0]
	worker := machine.workers[token.WorkerID]
	affected := []AffectedAssignment{{JobID: jobID, JobControlRevision: machine.jobs[jobID].JobControlRevision, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest}}
	deactivate, _ := NewDeactivateWorker(InternalCommandID{0xc6}, worker.Revision, worker.NodeID, worker.Epoch, affected, machine.coordinatorEpoch)
	applyTask10(t, machine, 13, deactivate)

	rejoined := machine.workers[worker.NodeID]
	rejoined.State = WorkerEligible
	rejoined.Revision++
	register, _ := NewRegisterWorker(InternalCommandID{0xc7}, rejoined.Revision-1, rejoined, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 14, register); got.Code != ResultSuccess {
		t.Fatalf("same-epoch rejoin: %#v", got)
	}
	jobBefore := cloneJobRecord(machine.jobs[jobID])

	second, _ := NewDeactivateWorker(InternalCommandID{0xc8}, rejoined.Revision, worker.NodeID, worker.Epoch, nil, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 15, second); got.Code != ResultSuccess {
		t.Fatalf("repeat deactivation: %#v", got)
	}
	jobAfter := machine.jobs[jobID]
	if jobAfter.JobControlRevision != jobBefore.JobControlRevision || len(jobAfter.NeedsReassignment) != len(jobBefore.NeedsReassignment) {
		t.Fatalf("repeat deactivation mutated marked job: before=%#v after=%#v", jobBefore, jobAfter)
	}
}

func TestSnapshotPreservesActiveInvalidationAcrossWorkerHistoryOverwrites(t *testing.T) {
	machine, jobID, _, assignment := task10AssignedJob(t, 4)
	install, _ := NewInstallAssignments(InternalCommandID{0xc9}, 1, assignment, machine.coordinatorEpoch)
	applyTask10(t, machine, 12, install)
	token := assignment.Tasks[0]
	worker := machine.workers[token.WorkerID]
	affected := []AffectedAssignment{{JobID: jobID, JobControlRevision: 2, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest}}
	deactivate, _ := NewDeactivateWorker(InternalCommandID{0xca}, worker.Revision, worker.NodeID, worker.Epoch, affected, machine.coordinatorEpoch)
	applyTask10(t, machine, 13, deactivate)

	rejoined := machine.workers[worker.NodeID]
	rejoined.State = WorkerEligible
	rejoined.Revision++
	register, _ := NewRegisterWorker(InternalCommandID{0xcb}, rejoined.Revision-1, rejoined, machine.coordinatorEpoch)
	applyTask10(t, machine, 14, register)
	drain, _ := NewDrainWorker(InternalCommandID{0xcc}, rejoined.Revision, rejoined.NodeID, rejoined.Epoch, machine.coordinatorEpoch)
	applyTask10(t, machine, 15, drain)
	if _, err := machine.Capture(100, 100); err != nil {
		t.Fatalf("active provenance after register/drain overwrite: %v", err)
	}

	cancel, _ := NewCancelJob(model.ClientRequestID{ClientID: model.ClientID{0xcc}, Sequence: 1}, jobID, machine.jobs[jobID].JobControlRevision, machine.coordinatorEpoch)
	applyTask10(t, machine, 16, cancel)
	if _, err := machine.Capture(101, 100); err != nil {
		t.Fatalf("canceled job with active provenance: %v", err)
	}
}

func TestSnapshotAcceptsActiveReplacementAndPrunesConsumedProvenance(t *testing.T) {
	machine, jobID, _, assignment := task10AssignedJob(t, 4)
	install, _ := NewInstallAssignments(InternalCommandID{0xcd}, 1, assignment, machine.coordinatorEpoch)
	applyTask10(t, machine, 12, install)
	token := assignment.Tasks[0]
	worker := machine.workers[token.WorkerID]
	affected := []AffectedAssignment{{JobID: jobID, JobControlRevision: 2, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest}}
	target := worker
	target.Epoch[15]++
	target.Revision++
	replace, _ := NewReplaceWorkerEpoch(InternalCommandID{0xce}, worker.Revision, worker.NodeID, worker.Epoch, target, affected, machine.coordinatorEpoch)
	applyTask10(t, machine, 13, replace)
	if _, err := machine.Capture(100, 100); err != nil {
		t.Fatalf("active epoch-replacement provenance: %v", err)
	}

	repaired, repairedWorker, _ := round3RepairedDeactivation(t)
	var repairedJob model.JobID
	for id := range repaired.jobs {
		repairedJob = id
	}
	if len(repaired.jobs[repairedJob].invalidationHistory) != 1 {
		t.Fatalf("repair did not retain referenced provenance: %#v", repaired.jobs[repairedJob].invalidationHistory)
	}
	rejoined := repaired.workers[repairedWorker.NodeID]
	rejoined.State = WorkerEligible
	rejoined.Revision++
	register, _ := NewRegisterWorker(InternalCommandID{0xcf}, rejoined.Revision-1, rejoined, repaired.coordinatorEpoch)
	applyTask10(t, repaired, 15, register)
	history := repaired.jobs[repairedJob].invalidationHistory
	if len(history) != 1 || history[0].Kind != 0 || history[0].WorkerRevision != 0 || history[0].RepairState != invalidationRepairAnchored {
		t.Fatalf("worker overwrite did not retain only job-control-anchored provenance: %#v", history)
	}
	running, _ := NewTransitionJob(InternalCommandID{0xd6}, repaired.jobs[repairedJob].JobControlRevision, repairedJob, JobDeploying, JobRunning, repaired.coordinatorEpoch)
	applyTask10(t, repaired, 16, running)
	if len(repaired.jobs[repairedJob].invalidationHistory) != 0 {
		t.Fatalf("overwriting both anchors did not prune consumed provenance: %#v", repaired.jobs[repairedJob].invalidationHistory)
	}
	if _, err := repaired.Capture(101, 100); err != nil {
		t.Fatalf("capture after consumed provenance prune: %v", err)
	}
}

func TestSnapshotAcceptsForgottenRepairWhileWorkerTargetRemains(t *testing.T) {
	machine, worker, _ := round3RepairedDeactivation(t)
	var jobID model.JobID
	for id := range machine.jobs {
		jobID = id
	}
	running, _ := NewTransitionJob(InternalCommandID{0xd7}, machine.jobs[jobID].JobControlRevision, jobID, JobDeploying, JobRunning, machine.coordinatorEpoch)
	applyTask10(t, machine, 15, running)
	history := machine.jobs[jobID].invalidationHistory
	if len(history) != 1 || history[0].RepairState != invalidationRepairForgotten || history[0].WorkerRevision != machine.workers[worker.NodeID].Revision || history[0].RepairJobControlRevision != 0 || history[0].RepairAssignmentRevision != 0 || history[0].RepairAssignmentDigest != ([32]byte{}) || history[0].RepairMarkersDigest != ([32]byte{}) {
		t.Fatalf("job-control overwrite did not retain only worker-anchored provenance: %#v", history)
	}
	if _, err := machine.Capture(103, 100); err != nil {
		t.Fatalf("forgotten repair with retained worker target: %v", err)
	}

	rejoined := machine.workers[worker.NodeID]
	rejoined.State = WorkerEligible
	rejoined.Revision++
	register, _ := NewRegisterWorker(InternalCommandID{0xd8}, rejoined.Revision-1, rejoined, machine.coordinatorEpoch)
	applyTask10(t, machine, 16, register)
	if len(machine.jobs[jobID].invalidationHistory) != 0 {
		t.Fatalf("worker overwrite did not drop provenance after both anchors were forgotten: %#v", machine.jobs[jobID].invalidationHistory)
	}
	if _, err := machine.Capture(104, 100); err != nil {
		t.Fatalf("capture after both anchors were forgotten: %v", err)
	}
}

func TestSnapshotNormalizesAnchoredRepairOnFailureButNotCancellation(t *testing.T) {
	t.Run("failure forgets job-control anchor", func(t *testing.T) {
		machine, _, _ := round3RepairedDeactivation(t)
		var jobID model.JobID
		for id := range machine.jobs {
			jobID = id
		}
		record := machine.jobs[jobID]
		var token model.AssignmentToken
		for _, candidate := range record.Assignment.Tasks {
			worker := machine.workers[candidate.WorkerID]
			if worker.Epoch == candidate.WorkerEpoch && worker.State != WorkerOffline {
				token = candidate
				break
			}
		}
		if token.WorkerID == 0 {
			t.Fatal("fixture has no available failure token")
		}
		report := model.JobFailureReport{JobID: jobID, JobControlRevision: record.JobControlRevision, AssignmentRevision: record.Assignment.Revision, Task: token, Epoch: machine.coordinatorEpoch, TransactionID: 1, Code: model.FailureOperator, DetailDigest: [32]byte{0xd9}}
		fail, _ := NewFailJob(InternalCommandID{0xd9}, record.JobControlRevision, report, machine.coordinatorEpoch)
		if got := applyTask10(t, machine, 15, fail); got.Code != ResultSuccess {
			t.Fatalf("fail repaired job: %#v", got)
		}
		history := machine.jobs[jobID].invalidationHistory
		if len(history) != 1 || history[0].RepairState != invalidationRepairForgotten || history[0].RepairJobControlRevision != 0 {
			t.Fatalf("failure did not forget prior job-control anchor: %#v", history)
		}
		if _, err := machine.Capture(105, 100); err != nil {
			t.Fatalf("failed job with forgotten repair: %v", err)
		}
	})

	t.Run("client cancellation preserves job-control anchor", func(t *testing.T) {
		machine, _, _ := round3RepairedDeactivation(t)
		var jobID model.JobID
		for id := range machine.jobs {
			jobID = id
		}
		cancel, _ := NewCancelJob(model.ClientRequestID{ClientID: model.ClientID{0xda}, Sequence: 1}, jobID, machine.jobs[jobID].JobControlRevision, machine.coordinatorEpoch)
		if got := applyTask10(t, machine, 15, cancel); got.Code != ResultSuccess {
			t.Fatalf("cancel repaired job: %#v", got)
		}
		history := machine.jobs[jobID].invalidationHistory
		if len(history) != 1 || history[0].RepairState != invalidationRepairAnchored || history[0].RepairJobControlRevision == 0 {
			t.Fatalf("client cancellation changed job-control anchor: %#v", history)
		}
		if _, err := machine.Capture(106, 100); err != nil {
			t.Fatalf("canceled job with anchored repair: %v", err)
		}
	})
}

func TestSnapshotAcceptsMultipleInvalidationRepairGenerations(t *testing.T) {
	machine, firstWorker, _ := round3RepairedDeactivation(t)
	var jobID model.JobID
	for id := range machine.jobs {
		jobID = id
	}
	record := machine.jobs[jobID]
	topology, err := model.DecodeTopology(record.TopologyBytes)
	if err != nil {
		t.Fatal(err)
	}
	var worker WorkerRecord
	var markers []NeedsReassignment
	for _, token := range record.Assignment.Tasks {
		candidate := machine.workers[token.WorkerID]
		candidateMarkers := markersForWorker(*record.Assignment, token.WorkerID, token.WorkerEpoch)
		if candidate.NodeID != firstWorker.NodeID && candidate.State == WorkerEligible && len(candidateMarkers) == 1 {
			worker, markers = candidate, candidateMarkers
			break
		}
	}
	if worker.NodeID == 0 {
		t.Fatal("fixture has no single-target worker for second generation")
	}
	affected := []AffectedAssignment{{JobID: jobID, JobControlRevision: record.JobControlRevision, AssignmentRevision: record.Assignment.Revision, AssignmentDigest: record.Assignment.Digest}}
	deactivate, _ := NewDeactivateWorker(InternalCommandID{0xd0}, worker.Revision, worker.NodeID, worker.Epoch, affected, machine.coordinatorEpoch)
	applyTask10(t, machine, 15, deactivate)
	marked := machine.jobs[jobID]
	target := round3SingleMarkerTarget(t, machine, *record.Assignment, topology, markers[0])
	replace, err := NewReplaceAssignments(InternalCommandID{0xd1}, marked.JobControlRevision, jobID, record.Assignment.Revision, record.Assignment.Digest, NeedsReassignmentDigest(marked.NeedsReassignment), target, machine.coordinatorEpoch)
	if err != nil {
		t.Fatalf("construct second repair: %v", err)
	}
	if got := applyTask10(t, machine, 16, replace); got.Code != ResultSuccess {
		t.Fatalf("second repair generation: %#v current=(job=%d assignment=%d/%x markers=%x) command=(job=%d assignment=%d/%x markers=%x)", got, machine.jobs[jobID].JobControlRevision, machine.jobs[jobID].Assignment.Revision, machine.jobs[jobID].Assignment.Digest, NeedsReassignmentDigest(machine.jobs[jobID].NeedsReassignment), replace.Envelope.Internal.ExpectedRevision, replace.ExpectedAssignmentRevision, replace.ExpectedDigest, replace.ExpectedMarkersDigest)
	}
	if len(machine.jobs[jobID].invalidationHistory) != 2 {
		t.Fatalf("retained repair generations=%d want=2", len(machine.jobs[jobID].invalidationHistory))
	}
	if _, err := machine.Capture(102, 100); err != nil {
		t.Fatalf("multiple repair generations: %v", err)
	}
}

func round3SingleMarkerTarget(t *testing.T, machine *Machine, old model.AssignmentSet, topology model.ValidatedTopology, marker NeedsReassignment) model.AssignmentSet {
	t.Helper()
	used := make(map[uint16]bool)
	for _, token := range old.Tasks {
		used[token.WorkerID] = true
	}
	for _, replica := range old.ResultReplicas {
		used[replica.PrimaryNodeID], used[replica.SecondaryNodeID] = true, true
	}
	var replacement WorkerRecord
	for id, worker := range machine.workers {
		if !used[id] && worker.State == WorkerEligible {
			replacement = worker
			break
		}
	}
	if replacement.NodeID == 0 {
		t.Fatal("fixture has no unused eligible replacement")
	}
	tasks := append([]model.AssignmentToken(nil), old.Tasks...)
	for index := range tasks {
		tasks[index].AssignmentRevision = old.Revision + 1
		if marker.Kind == TaskTarget && tasks[index].Task == marker.Task {
			tasks[index].WorkerID = replacement.NodeID
			tasks[index].WorkerEpoch = replacement.Epoch
			tasks[index].Attempt++
		}
	}
	replicas := append([]model.ResultReplicaSet(nil), old.ResultReplicas...)
	for index := range replicas {
		if marker.Kind != ResultReplicaTarget || replicas[index].SinkTask != marker.SinkTask {
			continue
		}
		if marker.ReplicaRole == model.PrimaryReplica {
			replicas[index].PrimaryNodeID, replicas[index].PrimaryEpoch = replacement.NodeID, replacement.Epoch
		} else {
			replicas[index].SecondaryNodeID, replicas[index].SecondaryEpoch = replacement.NodeID, replacement.Epoch
		}
	}
	target, err := model.NewAssignmentSet(old.JobID, old.Revision+1, tasks, replicas, topology)
	if err != nil {
		t.Fatal(err)
	}
	return target
}

func round3RepairedDeactivation(t *testing.T) (*Machine, WorkerRecord, []AffectedAssignment) {
	t.Helper()
	machine, jobID, topology, assignment := task10AssignedJob(t, 6)
	install, _ := NewInstallAssignments(InternalCommandID{0xc2}, 1, assignment, machine.coordinatorEpoch)
	applyTask10(t, machine, 12, install)
	token := assignment.Tasks[0]
	worker := machine.workers[token.WorkerID]
	affected := []AffectedAssignment{{
		JobID:              jobID,
		JobControlRevision: machine.jobs[jobID].JobControlRevision,
		AssignmentRevision: assignment.Revision,
		AssignmentDigest:   assignment.Digest,
	}}
	deactivate, _ := NewDeactivateWorker(InternalCommandID{0xc3}, worker.Revision, worker.NodeID, worker.Epoch, affected, machine.coordinatorEpoch)
	applyTask10(t, machine, 13, deactivate)
	record := machine.jobs[jobID]
	target, _ := replacementTargetWithUnchangedDraining(t, assignment, topology, record.NeedsReassignment[0], "primary")
	replace, _ := NewReplaceAssignments(InternalCommandID{0xc4}, record.JobControlRevision, jobID, assignment.Revision, assignment.Digest, NeedsReassignmentDigest(record.NeedsReassignment), target, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 14, replace); got.Code != ResultSuccess {
		t.Fatalf("repair assignment: %#v", got)
	}
	return machine, worker, affected
}
