package state

import (
	"testing"

	"github.com/aaditya/cs425mp3/internal/crane/model"
)

func TestEOFCheckpointGlobalWorkerCursorAndIndependentRevisions(t *testing.T) {
	machine, job, _, assignment := task10RunningJob(t)
	sources := make([]model.AssignmentToken, 0)
	for _, token := range assignment.Tasks {
		if token.Task.StageID == 1 {
			sources = append(sources, token)
		}
	}
	var firstSource, secondSource model.AssignmentToken
	for left := range sources {
		for right := left + 1; right < len(sources); right++ {
			if sources[left].WorkerID == sources[right].WorkerID && sources[left].WorkerEpoch == sources[right].WorkerEpoch {
				firstSource, secondSource = sources[left], sources[right]
			}
		}
	}
	if firstSource == (model.AssignmentToken{}) {
		t.Fatal("fixture requires two source partitions on one worker epoch")
	}
	epoch := machine.coordinatorEpoch
	firstEOF := machine.jobs[job].SourceEOFs[firstSource.Task].EOF
	first := model.CompletionReport{JobID: job, JobControlRevision: machine.jobs[job].JobControlRevision, AssignmentRevision: assignment.Revision, Source: firstSource.Task, Token: firstSource, Epoch: epoch, ExpectedCheckpointRevision: 0, Prior: 0, New: 1, EOF: firstEOF, WorkerTransactionID: 5}
	first.Digest = model.CompletionReportDigest(first)
	advance1, _ := NewAdvanceCheckpoint(InternalCommandID{0x81}, 0, first)
	if got := applyTask10(t, machine, 50, advance1); got.Code != ResultSuccess || got.Revision != 1 {
		t.Fatalf("first checkpoint = %#v", got)
	}
	secondEOF := machine.jobs[job].SourceEOFs[secondSource.Task].EOF
	second := model.CompletionReport{JobID: job, JobControlRevision: machine.jobs[job].JobControlRevision, AssignmentRevision: assignment.Revision, Source: secondSource.Task, Token: secondSource, Epoch: epoch, ExpectedCheckpointRevision: 0, Prior: 0, New: secondEOF, EOF: secondEOF, WorkerTransactionID: 6}
	second.Digest = model.CompletionReportDigest(second)
	advance2, _ := NewAdvanceCheckpoint(InternalCommandID{0x82}, 0, second)
	if got := applyTask10(t, machine, 51, advance2); got.Code != ResultSuccess || got.Revision != 1 {
		t.Fatalf("second independent checkpoint = %#v", got)
	}
	if machine.jobs[job].JobControlRevision != first.JobControlRevision {
		t.Fatal("checkpoint advanced JobControlRevision")
	}

	duplicate := first
	duplicate.ExpectedCheckpointRevision, duplicate.Prior, duplicate.New, duplicate.WorkerTransactionID = 1, 1, 2, 6
	duplicate.Digest = model.CompletionReportDigest(duplicate)
	duplicateCommand, _ := NewAdvanceCheckpoint(InternalCommandID{0x83}, 1, duplicate)
	if got := applyTask10(t, machine, 52, duplicateCommand); got.Code != ResultStaleWorkerEvent {
		t.Fatalf("cross-source same transaction different digest = %#v", got)
	}
	stale := duplicate
	stale.WorkerTransactionID = 5
	stale.Digest = model.CompletionReportDigest(stale)
	staleCommand, _ := NewAdvanceCheckpoint(InternalCommandID{0x84}, 1, stale)
	if got := applyTask10(t, machine, 53, staleCommand); got.Code != ResultStaleWorkerEvent {
		t.Fatalf("cross-source decreasing worker transaction = %#v", got)
	}
}

func TestCheckpointRejectsStaleTokenEpochAndAdvanceBeyondEOFWithoutMutation(t *testing.T) {
	machine, job, _, assignment := task10RunningJob(t)
	var source model.AssignmentToken
	for _, token := range assignment.Tasks {
		if token.Task.StageID == 1 {
			source = token
			break
		}
	}
	eof := machine.jobs[job].SourceEOFs[source.Task].EOF
	report := model.CompletionReport{JobID: job, JobControlRevision: machine.jobs[job].JobControlRevision, AssignmentRevision: assignment.Revision, Source: source.Task, Token: source, Epoch: machine.coordinatorEpoch, ExpectedCheckpointRevision: 0, Prior: 0, New: eof + 1, EOF: eof + 1, WorkerTransactionID: 1}
	report.Digest = model.CompletionReportDigest(report)
	command, _ := NewAdvanceCheckpoint(InternalCommandID{0x91}, 0, report)
	if got := applyTask10(t, machine, 60, command); got.Code != ResultInvalidTarget {
		t.Fatalf("advance beyond committed EOF = %#v", got)
	}
	if _, exists := machine.jobs[job].Checkpoints[source.Task]; exists {
		t.Fatal("invalid checkpoint mutated state")
	}
}

func TestWorkerEventCursorResetsForNewEpochWhileOldTokenStaysFenced(t *testing.T) {
	machine, job, topology, assignment := task10RunningJob(t)
	var oldToken model.AssignmentToken
	for _, token := range assignment.Tasks {
		if token.Task.StageID == 1 && machine.jobs[job].SourceEOFs[token.Task].EOF > 1 {
			oldToken = token
			break
		}
	}
	if oldToken == (model.AssignmentToken{}) {
		t.Fatal("fixture requires a multi-record source token")
	}
	eof := machine.jobs[job].SourceEOFs[oldToken.Task].EOF
	first := model.CompletionReport{JobID: job, JobControlRevision: machine.jobs[job].JobControlRevision, AssignmentRevision: assignment.Revision, Source: oldToken.Task, Token: oldToken, Epoch: machine.coordinatorEpoch, ExpectedCheckpointRevision: 0, Prior: 0, New: 1, EOF: eof, WorkerTransactionID: 1000}
	first.Digest = model.CompletionReportDigest(first)
	advanceOld, _ := NewAdvanceCheckpoint(InternalCommandID{0x92}, 0, first)
	if got := applyTask10(t, machine, 61, advanceOld); got.Code != ResultSuccess {
		t.Fatalf("old epoch tx=1000 = %#v", got)
	}

	worker := machine.workers[oldToken.WorkerID]
	affected := []AffectedAssignment{{JobID: job, JobControlRevision: machine.jobs[job].JobControlRevision, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest}}
	newEpoch := model.WorkerEpoch{0x93, byte(oldToken.WorkerID)}
	targetWorker := worker
	targetWorker.Epoch, targetWorker.Revision = newEpoch, worker.Revision+1
	replaceWorker, _ := NewReplaceWorkerEpoch(InternalCommandID{0x93}, worker.Revision, worker.NodeID, worker.Epoch, targetWorker, affected)
	if got := applyTask10(t, machine, 62, replaceWorker); got.Code != ResultSuccess {
		t.Fatalf("replace worker epoch = %#v", got)
	}
	if _, retained := machine.workerEvents[workerEventKey{WorkerID: worker.NodeID, WorkerEpoch: worker.Epoch}]; retained {
		t.Fatal("replaced worker epoch retained an unreachable event cursor")
	}

	tokens := append([]model.AssignmentToken(nil), assignment.Tasks...)
	for index := range tokens {
		tokens[index].AssignmentRevision = assignment.Revision + 1
		if tokens[index].WorkerID == worker.NodeID && tokens[index].WorkerEpoch == worker.Epoch {
			tokens[index].WorkerEpoch = newEpoch
			tokens[index].Attempt++
		}
	}
	replicas := append([]model.ResultReplicaSet(nil), assignment.ResultReplicas...)
	for index := range replicas {
		if replicas[index].PrimaryNodeID == worker.NodeID && replicas[index].PrimaryEpoch == worker.Epoch {
			replicas[index].PrimaryEpoch = newEpoch
		}
		if replicas[index].SecondaryNodeID == worker.NodeID && replicas[index].SecondaryEpoch == worker.Epoch {
			replicas[index].SecondaryEpoch = newEpoch
		}
	}
	targetSet, err := model.NewAssignmentSet(job, assignment.Revision+1, tokens, replicas, topology)
	if err != nil {
		t.Fatal(err)
	}
	marked := machine.jobs[job]
	replaceSet, _ := NewReplaceAssignments(InternalCommandID{0x94}, marked.JobControlRevision, job, assignment.Revision, assignment.Digest, NeedsReassignmentDigest(marked.NeedsReassignment), targetSet)
	if got := applyTask10(t, machine, 63, replaceSet); got.Code != ResultSuccess {
		t.Fatalf("replace assignment = %#v markers=%#v", got, marked.NeedsReassignment)
	}
	newToken, ok := assignmentToken(machine.jobs[job].Assignment, oldToken.Task)
	if !ok || newToken.WorkerEpoch != newEpoch || newToken.Attempt != oldToken.Attempt+1 {
		t.Fatalf("new token = %#v", newToken)
	}
	second := model.CompletionReport{JobID: job, JobControlRevision: machine.jobs[job].JobControlRevision, AssignmentRevision: targetSet.Revision, Source: newToken.Task, Token: newToken, Epoch: machine.coordinatorEpoch, ExpectedCheckpointRevision: 1, Prior: 1, New: eof, EOF: eof, WorkerTransactionID: 5}
	second.Digest = model.CompletionReportDigest(second)
	advanceNew, _ := NewAdvanceCheckpoint(InternalCommandID{0x95}, 1, second)
	if got := applyTask10(t, machine, 64, advanceNew); got.Code != ResultSuccess {
		t.Fatalf("new epoch tx=5 = %#v", got)
	}

	staleFailure := model.JobFailureReport{JobID: job, JobControlRevision: machine.jobs[job].JobControlRevision, AssignmentRevision: assignment.Revision, Task: oldToken, Epoch: machine.coordinatorEpoch, TransactionID: 1001, Code: model.FailureOperator, DetailDigest: [32]byte{1}}
	fail, _ := NewFailJob(InternalCommandID{0x96}, machine.jobs[job].JobControlRevision, staleFailure)
	if got := applyTask10(t, machine, 65, fail); got.Code != ResultInvalidTarget || machine.jobs[job].Lifecycle != JobRunning {
		t.Fatalf("stale old token failure = %#v lifecycle=%d", got, machine.jobs[job].Lifecycle)
	}
}

func task10RunningJob(t *testing.T) (*Machine, model.JobID, model.ValidatedTopology, model.AssignmentSet) {
	t.Helper()
	machine := NewMachine()
	epochCommand, _ := NewBeginCoordinatorEpoch(InternalCommandID{0x60}, 0, 1, [16]byte{0x60})
	applyTask10(t, machine, 1, epochCommand)
	for index := 1; index <= 2; index++ {
		record := WorkerRecord{NodeID: uint16(index), Epoch: model.WorkerEpoch{byte(index), 0x44}, State: WorkerEligible, Revision: 1, Slots: 16, ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint()}
		register, _ := NewRegisterWorker(InternalCommandID{byte(index), 0x44}, 0, record, machine.coordinatorEpoch)
		applyTask10(t, machine, uint64(index), register)
	}
	topology, err := model.ValidateTopology(task10ProgressTopology())
	if err != nil {
		t.Fatal(err)
	}
	submit, _ := NewSubmitJob(model.ClientRequestID{ClientID: model.ClientID{0x61}, Sequence: 1}, topology.Spec(), machine.coordinatorEpoch)
	applyTask10(t, machine, 4, submit)
	job := submit.JobID()
	for partition := uint16(0); partition < 3; partition++ {
		source := model.TaskID{JobID: job, StageID: 1, Partition: partition}
		eof, _ := model.SourceEOF(topology, source)
		command, _ := NewRecordSourceEOF(InternalCommandID{0x62, byte(partition)}, 0, source, eof, machine.coordinatorEpoch)
		applyTask10(t, machine, uint64(5+partition), command)
	}
	assignment, err := model.BuildAssignmentSet(job, topology.Digest(), 1, topology, task10EligiblePlacements(machine))
	if err != nil {
		t.Fatal(err)
	}
	install, _ := NewInstallAssignments(InternalCommandID{0x63}, 1, assignment, machine.coordinatorEpoch)
	applyTask10(t, machine, 10, install)
	running, _ := NewTransitionJob(InternalCommandID{0x64}, 2, job, JobDeploying, JobRunning, machine.coordinatorEpoch)
	applyTask10(t, machine, 11, running)
	return machine, job, topology, assignment
}

func task10ProgressTopology() model.TopologySpec {
	topology := task10Topology(0)
	topology.Stages[0].Parallelism = 3
	topology.Stages[0].Operator.Settings[0].Value = "9"
	return topology
}
