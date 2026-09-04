package state

import (
	"testing"

	"github.com/aadityakv/crane/internal/crane/model"
)

func TestSnapshotRejectsUnexplainedJobControlAppliedRevisionLag(t *testing.T) {
	machine, jobID, _, assignment := task10AssignedJob(t, 2)
	install, _ := NewInstallAssignments(InternalCommandID{0xa1}, 1, assignment, machine.coordinatorEpoch)
	applyTask10(t, machine, 12, install)
	installHistory := machine.subjects[SubjectKey{Kind: SubjectJobControl, JobID: jobID}]
	running, _ := NewTransitionJob(InternalCommandID{0xa2}, 2, jobID, JobDeploying, JobRunning, machine.coordinatorEpoch)
	applyTask10(t, machine, 13, running)
	machine.subjects[SubjectKey{Kind: SubjectJobControl, JobID: jobID}] = installHistory
	recomputeSnapshotEstimateForReview(t, machine)

	assertCaptureAndRawRestoreRejectSnapshot(t, machine)
}

func TestSnapshotRejectsSemanticallyInvalidAppliedReplacementTarget(t *testing.T) {
	machine, jobID, topology, old := replacementReviewFixture(t, "primary")
	target, _ := replacementTargetWithUnchangedDraining(t, old, topology, machine.jobs[jobID].NeedsReassignment[0], "primary")
	record := machine.jobs[jobID]
	replace, _ := NewReplaceAssignments(InternalCommandID{0xa3}, record.JobControlRevision, jobID, old.Revision, old.Digest, NeedsReassignmentDigest(record.NeedsReassignment), target, machine.coordinatorEpoch)
	applyTask10(t, machine, 40, replace)

	key := SubjectKey{Kind: SubjectJobControl, JobID: jobID}
	history := machine.subjects[key]
	invalid := replaceAssignmentsTarget(ReplaceAssignments{
		JobID:                      jobID,
		ExpectedAssignmentRevision: 99,
		ExpectedDigest:             replace.ExpectedDigest,
		ExpectedMarkersDigest:      replace.ExpectedMarkersDigest,
		Target:                     target,
	})
	history.target = append([]byte(nil), invalid...)
	history.appliedTarget = append([]byte(nil), invalid...)
	machine.subjects[key] = history
	recomputeSnapshotEstimateForReview(t, machine)

	assertCaptureAndRawRestoreRejectSnapshot(t, machine)
}

func TestSnapshotRejectsAppliedSucceededTransitionFollowedByCancellation(t *testing.T) {
	machine, jobID, _, _ := task10RunningJob(t)
	cancel, _ := NewCancelJob(model.ClientRequestID{ClientID: model.ClientID{0xab}, Sequence: 1}, jobID, machine.jobs[jobID].JobControlRevision, machine.coordinatorEpoch)
	applyTask10(t, machine, 90, cancel)

	key := SubjectKey{Kind: SubjectJobControl, JobID: jobID}
	history := machine.subjects[key]
	impossible := transitionJobTarget(TransitionJob{JobID: jobID, From: JobDraining, To: JobSucceeded})
	history.target = append([]byte(nil), impossible...)
	history.appliedTarget = append([]byte(nil), impossible...)
	machine.subjects[key] = history
	recomputeSnapshotEstimateForReview(t, machine)

	assertCaptureAndRawRestoreRejectSnapshot(t, machine)
}

func TestSnapshotRejectsAppliedWorkerInvalidationWithFalseAffectedList(t *testing.T) {
	machine, jobID, _, assignment := task10AssignedJob(t, 2)
	install, _ := NewInstallAssignments(InternalCommandID{0xa4}, 1, assignment, machine.coordinatorEpoch)
	applyTask10(t, machine, 12, install)
	token := assignment.Tasks[0]
	worker := machine.workers[token.WorkerID]
	affected := []AffectedAssignment{{JobID: jobID, JobControlRevision: machine.jobs[jobID].JobControlRevision, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest}}
	deactivate, _ := NewDeactivateWorker(InternalCommandID{0xa5}, worker.Revision, worker.NodeID, worker.Epoch, affected, machine.coordinatorEpoch)
	applyTask10(t, machine, 13, deactivate)

	key := SubjectKey{Kind: SubjectWorker, WorkerID: worker.NodeID}
	history := machine.subjects[key]
	invalid := deactivateWorkerTarget(DeactivateWorker{WorkerID: worker.NodeID, WorkerEpoch: worker.Epoch})
	history.target = append([]byte(nil), invalid...)
	history.appliedTarget = append([]byte(nil), invalid...)
	machine.subjects[key] = history
	recomputeSnapshotEstimateForReview(t, machine)

	assertCaptureAndRawRestoreRejectSnapshot(t, machine)
}

func TestSnapshotRejectsAppliedWorkerInvalidationWithFalseJobRevisionFence(t *testing.T) {
	machine, jobID, _, assignment := task10AssignedJob(t, 2)
	install, _ := NewInstallAssignments(InternalCommandID{0xa9}, 1, assignment, machine.coordinatorEpoch)
	applyTask10(t, machine, 12, install)
	token := assignment.Tasks[0]
	worker := machine.workers[token.WorkerID]
	affected := []AffectedAssignment{{JobID: jobID, JobControlRevision: machine.jobs[jobID].JobControlRevision, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest}}
	deactivate, _ := NewDeactivateWorker(InternalCommandID{0xaa}, worker.Revision, worker.NodeID, worker.Epoch, affected, machine.coordinatorEpoch)
	applyTask10(t, machine, 13, deactivate)

	affected[0].JobControlRevision--
	key := SubjectKey{Kind: SubjectWorker, WorkerID: worker.NodeID}
	history := machine.subjects[key]
	invalid := deactivateWorkerTarget(DeactivateWorker{WorkerID: worker.NodeID, WorkerEpoch: worker.Epoch, Affected: affected})
	history.target = append([]byte(nil), invalid...)
	history.appliedTarget = append([]byte(nil), invalid...)
	machine.subjects[key] = history
	recomputeSnapshotEstimateForReview(t, machine)

	assertCaptureAndRawRestoreRejectSnapshot(t, machine)
}

func TestSnapshotRejectsAppliedWorkerReplacementWithFalseAffectedList(t *testing.T) {
	machine, jobID, _, assignment := task10AssignedJob(t, 2)
	install, _ := NewInstallAssignments(InternalCommandID{0xb1}, 1, assignment, machine.coordinatorEpoch)
	applyTask10(t, machine, 12, install)
	token := assignment.Tasks[0]
	worker := machine.workers[token.WorkerID]
	affected := []AffectedAssignment{{JobID: jobID, JobControlRevision: machine.jobs[jobID].JobControlRevision, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest}}
	target := worker
	target.Epoch[15]++
	target.Revision++
	replace, _ := NewReplaceWorkerEpoch(InternalCommandID{0xb2}, worker.Revision, worker.NodeID, worker.Epoch, target, affected, machine.coordinatorEpoch)
	applyTask10(t, machine, 13, replace)

	key := SubjectKey{Kind: SubjectWorker, WorkerID: worker.NodeID}
	history := machine.subjects[key]
	invalid := replaceWorkerEpochTarget(ReplaceWorkerEpoch{WorkerID: worker.NodeID, OldEpoch: worker.Epoch, Target: target})
	history.target = append([]byte(nil), invalid...)
	history.appliedTarget = append([]byte(nil), invalid...)
	machine.subjects[key] = history
	recomputeSnapshotEstimateForReview(t, machine)

	assertCaptureAndRawRestoreRejectSnapshot(t, machine)
}

func TestSnapshotAcceptsExactlyExplainedJobControlLag(t *testing.T) {
	t.Run("client cancellation", func(t *testing.T) {
		machine, jobID, _, assignment := task10AssignedJob(t, 2)
		install, _ := NewInstallAssignments(InternalCommandID{0xb3}, 1, assignment, machine.coordinatorEpoch)
		applyTask10(t, machine, 12, install)
		cancel, _ := NewCancelJob(model.ClientRequestID{ClientID: model.ClientID{0xb3}, Sequence: 1}, jobID, 2, machine.coordinatorEpoch)
		applyTask10(t, machine, 13, cancel)
		if _, err := machine.Capture(100, 100); err != nil {
			t.Fatalf("canceled assigned job: %v", err)
		}
	})

	t.Run("two worker incarnations", func(t *testing.T) {
		machine, jobID, _, assignment := task10AssignedJob(t, 4)
		install, _ := NewInstallAssignments(InternalCommandID{0xb4}, 1, assignment, machine.coordinatorEpoch)
		applyTask10(t, machine, 12, install)
		workers := make([]uint16, 0, 2)
		seen := make(map[uint16]bool)
		for _, token := range assignment.Tasks {
			if !seen[token.WorkerID] {
				seen[token.WorkerID] = true
				workers = append(workers, token.WorkerID)
			}
			if len(workers) == 2 {
				break
			}
		}
		if len(workers) != 2 {
			t.Fatal("fixture does not use two task workers")
		}
		for index, workerID := range workers {
			worker := machine.workers[workerID]
			record := machine.jobs[jobID]
			affected := []AffectedAssignment{{JobID: jobID, JobControlRevision: record.JobControlRevision, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest}}
			deactivate, _ := NewDeactivateWorker(InternalCommandID{0xb5, byte(index)}, worker.Revision, worker.NodeID, worker.Epoch, affected, machine.coordinatorEpoch)
			applyTask10(t, machine, uint64(13+index), deactivate)
		}
		if _, err := machine.Capture(100, 100); err != nil {
			t.Fatalf("two explained invalidations: %v", err)
		}
	})
}

func TestSnapshotAcceptsWorkerAffectedHistoryAfterAssignmentRepair(t *testing.T) {
	machine, jobID, topology, assignment := task10AssignedJob(t, 6)
	install, _ := NewInstallAssignments(InternalCommandID{0xb6}, 1, assignment, machine.coordinatorEpoch)
	applyTask10(t, machine, 12, install)
	token := assignment.Tasks[0]
	worker := machine.workers[token.WorkerID]
	affected := []AffectedAssignment{{JobID: jobID, JobControlRevision: machine.jobs[jobID].JobControlRevision, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest}}
	deactivate, _ := NewDeactivateWorker(InternalCommandID{0xb7}, worker.Revision, worker.NodeID, worker.Epoch, affected, machine.coordinatorEpoch)
	applyTask10(t, machine, 13, deactivate)
	record := machine.jobs[jobID]
	target, _ := replacementTargetWithUnchangedDraining(t, assignment, topology, record.NeedsReassignment[0], "primary")
	replace, _ := NewReplaceAssignments(InternalCommandID{0xb8}, record.JobControlRevision, jobID, assignment.Revision, assignment.Digest, NeedsReassignmentDigest(record.NeedsReassignment), target, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 14, replace); got.Code != ResultSuccess {
		t.Fatalf("repair assignment: %#v", got)
	}
	if _, err := machine.Capture(100, 100); err != nil {
		t.Fatalf("repaired affected history: %v", err)
	}
}

func TestSnapshotRequiresCompleteSourceEOFsForEveryAssignedTerminalJob(t *testing.T) {
	tests := map[string]func(*testing.T) (*Machine, model.JobID){
		"canceled": func(t *testing.T) (*Machine, model.JobID) {
			machine, jobID, _, assignment := task10AssignedJob(t, 2)
			install, _ := NewInstallAssignments(InternalCommandID{0xa6}, 1, assignment, machine.coordinatorEpoch)
			applyTask10(t, machine, 12, install)
			cancel, _ := NewCancelJob(model.ClientRequestID{ClientID: model.ClientID{0xa6}, Sequence: 1}, jobID, 2, machine.coordinatorEpoch)
			applyTask10(t, machine, 13, cancel)
			return machine, jobID
		},
		"failed": func(t *testing.T) (*Machine, model.JobID) {
			machine, jobID, _, assignment := task10RunningJob(t)
			token := assignment.Tasks[0]
			report := model.JobFailureReport{JobID: jobID, JobControlRevision: machine.jobs[jobID].JobControlRevision, AssignmentRevision: assignment.Revision, Task: token, Epoch: machine.coordinatorEpoch, TransactionID: 91, Code: model.FailureOperator, DetailDigest: [32]byte{0xa7}}
			fail, _ := NewFailJob(InternalCommandID{0xa7}, machine.jobs[jobID].JobControlRevision, report, machine.coordinatorEpoch)
			applyTask10(t, machine, 90, fail)
			return machine, jobID
		},
	}
	for name, build := range tests {
		t.Run(name, func(t *testing.T) {
			machine, jobID := build(t)
			record := cloneJobRecord(machine.jobs[jobID])
			for source := range record.SourceEOFs {
				delete(record.SourceEOFs, source)
				delete(machine.subjects, SubjectKey{Kind: SubjectSourceEOF, JobID: jobID, TaskID: source})
				break
			}
			machine.jobs[jobID] = record
			recomputeSnapshotEstimateForReview(t, machine)
			assertCaptureAndRawRestoreRejectSnapshot(t, machine)
		})
	}
}

func TestSnapshotRejectsDuplicateDefiningRequestAfterClientHistoryAdvances(t *testing.T) {
	machine := NewMachine()
	begin, _ := NewBeginCoordinatorEpoch(InternalCommandID{0xa8}, 0, 1, [16]byte{0xa8})
	applyTask10(t, machine, 1, begin)
	clientID := model.ClientID{0xa8}
	firstRequest := model.ClientRequestID{ClientID: clientID, Sequence: 1}
	firstSpec := task10Topology(0)
	firstSpec.Name = "first"
	first, _ := NewSubmitJob(firstRequest, firstSpec, machine.coordinatorEpoch)
	applyTask10(t, machine, 2, first)
	secondRequest := model.ClientRequestID{ClientID: clientID, Sequence: 2}
	secondSpec := task10Topology(0)
	secondSpec.Name = "second"
	second, _ := NewSubmitJob(secondRequest, secondSpec, machine.coordinatorEpoch)
	applyTask10(t, machine, 3, second)

	oldID := second.JobID()
	record := cloneJobRecord(machine.jobs[oldID])
	record.DefiningRequest = firstRequest
	record.JobID = model.DeriveJobID(firstRequest, record.TopologyDigest)
	if record.JobID == first.JobID() {
		t.Fatal("fixture did not produce distinct job identities")
	}
	delete(machine.jobs, oldID)
	machine.jobs[record.JobID] = record
	recomputeSnapshotEstimateForReview(t, machine)

	assertCaptureAndRawRestoreRejectSnapshot(t, machine)
}

func recomputeSnapshotEstimateForReview(t *testing.T, machine *Machine) {
	t.Helper()
	machine.mu.Lock()
	defer machine.mu.Unlock()
	estimated, ok := machine.estimateCanonicalSnapshotBytesLocked()
	if !ok {
		t.Fatal("review fixture exceeds snapshot limit")
	}
	machine.estimatedSnapshotBytes = estimated
}
