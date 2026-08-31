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
	if len(repaired.jobs[repairedJob].invalidationHistory) != 0 {
		t.Fatalf("overwritten worker history did not prune consumed provenance: %#v", repaired.jobs[repairedJob].invalidationHistory)
	}
	if _, err := repaired.Capture(101, 100); err != nil {
		t.Fatalf("capture after consumed provenance prune: %v", err)
	}
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
