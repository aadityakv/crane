package state

import (
	"testing"

	"crane/internal/crane/model"
)

// beginLaterEpoch commits one strictly later coordinator epoch (later term,
// later begin index) exactly as a leadership change does, and returns the
// superseded epoch.
func beginLaterEpoch(t *testing.T, machine *Machine, index, term uint64, nonce byte) model.CoordinatorEpoch {
	t.Helper()
	superseded := machine.coordinatorEpoch
	begin, err := NewBeginCoordinatorEpoch(InternalCommandID{0xD0, nonce}, machine.coordinatorRevision, 1, [16]byte{nonce})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := MarshalCommand(begin)
	if err != nil {
		t.Fatal(err)
	}
	result := mustApplyResult(t, machine, index, term, encoded)
	if result.Code != ResultSuccess {
		t.Fatalf("later epoch begin = %#v", result)
	}
	return superseded
}

// TestAdvanceCheckpointCommitsRetainedCustodyUnderSupersededEpoch pins the
// defect #5 ruling: a worker durably proved work under an assignment that is
// still current, so its pending CompletionReport carries an epoch ordered
// strictly before the machine's committed coordinator epoch. AdvanceCheckpoint
// must commit it when every other condition holds exactly (current token,
// current JobControlRevision and AssignmentRevision, live worker at the
// report's WorkerEpoch, watermark/EOF bounds), because no reassignment ever
// happened and the work is still the frontier. Reports ordered after the
// committed epoch stay rejected, and the envelope fence (current-leader
// proposal) is unchanged.
func TestAdvanceCheckpointCommitsRetainedCustodyUnderSupersededEpoch(t *testing.T) {
	machine, job, _, assignment := task10RunningJob(t)
	superseded := beginLaterEpoch(t, machine, 90, 2, 0xD1)
	var token model.AssignmentToken
	for _, candidate := range assignment.Tasks {
		if candidate.Task.StageID == 1 && machine.jobs[job].SourceEOFs[candidate.Task].EOF >= 2 {
			token = candidate
			break
		}
	}
	eof := machine.jobs[job].SourceEOFs[token.Task].EOF

	retained := model.CompletionReport{JobID: job, JobControlRevision: machine.jobs[job].JobControlRevision, AssignmentRevision: assignment.Revision, Source: token.Task, Token: token, Epoch: superseded, ExpectedCheckpointRevision: 0, Prior: 0, New: 1, EOF: eof, WorkerTransactionID: 500}
	retained.Digest = model.CompletionReportDigest(retained)
	retainedCommand, err := NewAdvanceCheckpoint(InternalCommandID{0xD2}, 0, retained, machine.coordinatorEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if got := applyTask10(t, machine, 91, retainedCommand); got.Code != ResultSuccess {
		t.Fatalf("retained-custody report under superseded epoch = %#v", got)
	}
	if checkpoint := machine.jobs[job].Checkpoints[token.Task]; checkpoint.Watermark != 1 || checkpoint.Revision != 1 {
		t.Fatalf("committed checkpoint = %#v", checkpoint)
	}

	// A report whose epoch is ordered strictly after the committed epoch is a
	// superseded coordinator's mutation attempt and stays rejected.
	future := retained
	future.Epoch = model.CoordinatorEpoch{Term: machine.coordinatorEpoch.Term + 3, BeginIndex: machine.coordinatorEpoch.BeginIndex + 3, Coordinator: machine.coordinatorEpoch.Coordinator, Nonce: machine.coordinatorEpoch.Nonce}
	future.ExpectedCheckpointRevision, future.Prior, future.New, future.WorkerTransactionID = 1, 1, 2, 501
	future.Digest = model.CompletionReportDigest(future)
	futureCommand, err := NewAdvanceCheckpoint(InternalCommandID{0xD3}, 1, future, machine.coordinatorEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if got := applyTask10(t, machine, 92, futureCommand); got.Code != ResultInvalidTarget {
		t.Fatalf("ordered-after epoch report = %#v", got)
	}

	// Equal-epoch reports keep committing exactly as before.
	current := retained
	current.ExpectedCheckpointRevision, current.Prior, current.New, current.WorkerTransactionID = 1, 1, 2, 502
	current.Digest = model.CompletionReportDigest(current)
	currentCommand, err := NewAdvanceCheckpoint(InternalCommandID{0xD4}, 1, current, machine.coordinatorEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if got := applyTask10(t, machine, 93, currentCommand); got.Code != ResultSuccess {
		t.Fatalf("current-epoch report = %#v", got)
	}
}

// TestAdvanceCheckpointStillRejectsReplacedFencesUnderSupersededEpoch pins the
// unchanged negative space once the superseded-epoch readoption exists: a
// genuinely replaced assignment revision or token never re-enters through an
// older-epoch report.
func TestAdvanceCheckpointStillRejectsReplacedFencesUnderSupersededEpoch(t *testing.T) {
	for name, mutate := range map[string]func(*model.CompletionReport){
		"replaced assignment revision": func(report *model.CompletionReport) {
			report.AssignmentRevision++
			report.Token.AssignmentRevision = report.AssignmentRevision
		},
		"replaced token attempt": func(report *model.CompletionReport) { report.Token.Attempt++ },
		"replaced job control":   func(report *model.CompletionReport) { report.JobControlRevision++ },
	} {
		t.Run(name, func(t *testing.T) {
			machine, job, _, assignment := task10RunningJob(t)
			superseded := beginLaterEpoch(t, machine, 90, 2, 0xD5)
			var token model.AssignmentToken
			for _, candidate := range assignment.Tasks {
				if candidate.Task.StageID == 1 && machine.jobs[job].SourceEOFs[candidate.Task].EOF >= 2 {
					token = candidate
					break
				}
			}
			eof := machine.jobs[job].SourceEOFs[token.Task].EOF
			report := model.CompletionReport{JobID: job, JobControlRevision: machine.jobs[job].JobControlRevision, AssignmentRevision: assignment.Revision, Source: token.Task, Token: token, Epoch: superseded, ExpectedCheckpointRevision: 0, Prior: 0, New: 1, EOF: eof, WorkerTransactionID: 510}
			mutate(&report)
			report.Digest = model.CompletionReportDigest(report)
			command, err := NewAdvanceCheckpoint(InternalCommandID{0xD6}, 0, report, machine.coordinatorEpoch)
			if err != nil {
				t.Fatal(err)
			}
			if got := applyTask10(t, machine, 91, command); got.Code != ResultInvalidTarget {
				t.Fatalf("replaced fence report = %#v", got)
			}
			if _, exists := machine.jobs[job].Checkpoints[token.Task]; exists {
				t.Fatal("rejected report mutated checkpoint state")
			}
		})
	}
}
