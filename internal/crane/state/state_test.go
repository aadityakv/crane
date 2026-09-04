package state

import (
	"bytes"
	"testing"

	"crane/internal/crane/model"
)

func TestCoordinatorEpochApplyUsesActualPositionAndStableRetry(t *testing.T) {
	machine := NewMachine()
	command := validBeginCommand(t, 0, 3, 0x21)
	encoded, _ := MarshalBeginCoordinatorEpoch(command)
	firstBytes, err := machine.Apply(7, 5, encoded)
	if err != nil {
		t.Fatalf("Apply first: %v", err)
	}
	first := mustResult(t, firstBytes)
	wantEpoch := model.CoordinatorEpoch{Term: 5, BeginIndex: 7, Coordinator: 3, Nonce: command.Nonce}
	if first.Code != ResultSuccess || first.Revision != 1 || first.Epoch != wantEpoch {
		t.Fatalf("first result = %#v, want epoch %#v", first, wantEpoch)
	}
	retryBytes, err := machine.Apply(99, 8, encoded)
	if err != nil {
		t.Fatalf("Apply retry: %v", err)
	}
	if !bytes.Equal(retryBytes, firstBytes) {
		t.Fatalf("retry changed cached bytes: %x != %x", retryBytes, firstBytes)
	}
}

func TestCoordinatorEpochExactTargetReplayDifferentIdentityReturnsOriginalEpoch(t *testing.T) {
	machine := NewMachine()
	first := validBeginCommand(t, 0, 3, 0x22)
	firstBytes, _ := MarshalBeginCoordinatorEpoch(first)
	wantBytes, err := machine.Apply(7, 5, firstBytes)
	if err != nil {
		t.Fatal(err)
	}
	retry := validBeginCommand(t, 0, 3, 0x22)
	retry.Envelope.Internal.ID[1] = 1
	retry.Envelope.Internal.Digest = independentInternalDigest(retry.Envelope, beginTargetForTest(retry))
	retryBytes, _ := MarshalBeginCoordinatorEpoch(retry)
	got, err := machine.Apply(8, 5, retryBytes)
	if err != nil || !bytes.Equal(got, wantBytes) {
		t.Fatalf("exact target replay = %x, %v; want %x", got, err, wantBytes)
	}
}

func TestCoordinatorEpochSameCoordinatorAndNonceInLaterTermCreatesNewEpoch(t *testing.T) {
	machine := NewMachine()
	first := validBeginCommand(t, 0, 3, 0x22)
	firstBytes, _ := MarshalBeginCoordinatorEpoch(first)
	if _, err := machine.Apply(7, 5, firstBytes); err != nil {
		t.Fatal(err)
	}

	next := validBeginCommand(t, 1, 3, 0x22)
	next.Envelope.Internal.ID[1] = 1
	next.Envelope.Internal.Digest = independentInternalDigest(next.Envelope, beginTargetForTest(next))
	nextBytes, _ := MarshalBeginCoordinatorEpoch(next)
	got := mustApplyResult(t, machine, 8, 6, nextBytes)
	wantEpoch := model.CoordinatorEpoch{Term: 6, BeginIndex: 8, Coordinator: 3, Nonce: next.Nonce}
	if got.Code != ResultSuccess || got.Revision != 2 || got.Epoch != wantEpoch {
		t.Fatalf("later-term exact command target = %#v, want revision 2 epoch %#v", got, wantEpoch)
	}
}

func TestCoordinatorEpochRejectsIdentityReuseRevisionAndStaleFenceAsBusinessResults(t *testing.T) {
	machine := NewMachine()
	first := validBeginCommand(t, 0, 3, 0x23)
	encoded, _ := MarshalBeginCoordinatorEpoch(first)
	if _, err := machine.Apply(7, 5, encoded); err != nil {
		t.Fatal(err)
	}

	reused := cloneBegin(first)
	reused.Coordinator = 4
	reused.Envelope.Internal.Digest = independentInternalDigest(reused.Envelope, beginTargetForTest(reused))
	reusedBytes, _ := MarshalBeginCoordinatorEpoch(reused)
	if got := mustApplyResult(t, machine, 8, 6, reusedBytes); got.Code != ResultIdentityReuse || got.Revision != 1 {
		t.Fatalf("identity reuse = %#v", got)
	}

	staleRevision := validBeginCommand(t, 9, 4, 0x24)
	staleRevision.Envelope.Internal.ID[1] = 1
	staleRevision.Envelope.Internal.Digest = independentInternalDigest(staleRevision.Envelope, beginTargetForTest(staleRevision))
	staleRevisionBytes, _ := MarshalBeginCoordinatorEpoch(staleRevision)
	if got := mustApplyResult(t, machine, 9, 6, staleRevisionBytes); got.Code != ResultRevisionMismatch || got.Revision != 1 {
		t.Fatalf("revision mismatch = %#v", got)
	}

	staleFence := validBeginCommand(t, 1, 4, 0x25)
	staleFence.Envelope.Internal.ID[1] = 2
	staleFence.Envelope.Internal.Digest = independentInternalDigest(staleFence.Envelope, beginTargetForTest(staleFence))
	staleFenceBytes, _ := MarshalBeginCoordinatorEpoch(staleFence)
	if got := mustApplyResult(t, machine, 10, 4, staleFenceBytes); got.Code != ResultStaleEpoch || got.Revision != 1 {
		t.Fatalf("stale fence = %#v", got)
	}
}

func TestCommandApplySeparatesCorruptionErrorsFromBusinessRejection(t *testing.T) {
	machine := NewMachine()
	valid := validBeginCommand(t, 1, 2, 0x31)
	encoded, _ := MarshalBeginCoordinatorEpoch(valid)
	if result, err := machine.Apply(1, 1, encoded); err != nil || mustResult(t, result).Code != ResultRevisionMismatch {
		t.Fatalf("business rejection result=%x err=%v", result, err)
	}
	corrupt := append([]byte(nil), encoded...)
	corrupt[len(corrupt)-1] ^= 1
	if result, err := machine.Apply(2, 1, corrupt); err == nil || result != nil {
		t.Fatalf("corrupt command result=%x err=%v", result, err)
	}
	if result, err := machine.Apply(0, 1, encoded); err == nil || result != nil {
		t.Fatalf("zero index result=%x err=%v", result, err)
	}
	if result, err := machine.Apply(1, 0, encoded); err == nil || result != nil {
		t.Fatalf("zero term result=%x err=%v", result, err)
	}
}

func TestInternalSubjectHistoriesAreIndependentAndOwnTargetAndResultBytes(t *testing.T) {
	machine := NewMachine()
	job := model.JobID{1}
	task0 := model.TaskID{JobID: job, StageID: 1, Partition: 0}
	task1 := model.TaskID{JobID: job, StageID: 1, Partition: 1}
	keys := []SubjectKey{
		{Kind: SubjectCoordinator},
		{Kind: SubjectWorker, WorkerID: 1},
		{Kind: SubjectJobControl, JobID: job},
		{Kind: SubjectSourceEOF, JobID: job, TaskID: task0},
		{Kind: SubjectSourceCheckpoint, JobID: job, TaskID: task0},
		{Kind: SubjectSourceCheckpoint, JobID: job, TaskID: task1},
		{Kind: SubjectResultManifest, JobID: job, TaskID: model.TaskID{JobID: job, StageID: 2}},
	}
	for index, key := range keys {
		target := []byte{byte(index + 1)}
		result := []byte{byte(0xa0 + index)}
		envelope := testInternalEnvelope(CommandBeginCoordinatorEpoch, key, 0, byte(index+1), target)
		got := applyInternalTest(t, machine, envelope, target, func(uint64) mutationPlan { return mutationPlan{result: result} })
		if !bytes.Equal(got, result) {
			t.Fatalf("subject %d result=%x want=%x", index, got, result)
		}
		target[0] ^= 0xff
		result[0] ^= 0xff
	}
	if len(machine.subjects) != len(keys) {
		t.Fatalf("subject history count=%d want=%d", len(machine.subjects), len(keys))
	}
	for _, history := range machine.subjects {
		if history.revision != 1 || history.target[0] == 0 || history.result[0] < 0xa0 {
			t.Fatalf("unowned or wrong history: %#v", history)
		}
	}
}

func TestInternalDedupExactRetryIdentityReuseStaleAndExactTarget(t *testing.T) {
	machine := NewMachine()
	key := SubjectKey{Kind: SubjectWorker, WorkerID: 7}
	target := []byte("target-a")
	envelope := testInternalEnvelope(CommandBeginCoordinatorEpoch, key, 0, 1, target)
	var commits int
	first := applyInternalTest(t, machine, envelope, target, func(uint64) mutationPlan {
		return mutationPlan{result: []byte("success"), commit: func() { commits++ }}
	})
	if commits != 1 {
		t.Fatalf("commits=%d", commits)
	}
	retry := applyInternalTest(t, machine, envelope, target, func(uint64) mutationPlan {
		t.Fatal("exact retry executed callback")
		return mutationPlan{}
	})
	if !bytes.Equal(retry, first) || &retry[0] == &first[0] {
		t.Fatal("exact retry was not byte-identical owned copy")
	}

	changedTarget := []byte("target-b")
	reused := envelope
	reused.Internal = cloneInternal(envelope.Internal)
	reused.Internal.Digest = internalDigest(reused, changedTarget)
	if got := mustResult(t, applyInternalTest(t, machine, reused, changedTarget, func(uint64) mutationPlan { t.Fatal("identity reuse executed"); return mutationPlan{} })); got.Code != ResultIdentityReuse {
		t.Fatalf("identity reuse=%#v", got)
	}

	stale := testInternalEnvelope(CommandBeginCoordinatorEpoch, key, 9, 2, []byte("target-c"))
	staleBytes := applyInternalTest(t, machine, stale, []byte("target-c"), func(uint64) mutationPlan { t.Fatal("stale executed"); return mutationPlan{} })
	if got := mustResult(t, staleBytes); got.Code != ResultRevisionMismatch {
		t.Fatalf("stale=%#v", got)
	}
	staleRetry := applyInternalTest(t, machine, stale, []byte("target-c"), func(uint64) mutationPlan { t.Fatal("stale retry executed"); return mutationPlan{} })
	if !bytes.Equal(staleRetry, staleBytes) {
		t.Fatalf("stale retry=%x want byte-identical %x", staleRetry, staleBytes)
	}

	exactTarget := testInternalEnvelope(CommandBeginCoordinatorEpoch, key, 0, 3, []byte("target-a"))
	if got := applyInternalTest(t, machine, exactTarget, []byte("target-a"), func(uint64) mutationPlan { t.Fatal("exact target executed"); return mutationPlan{} }); !bytes.Equal(got, first) {
		t.Fatalf("exact target=%x want=%x", got, first)
	}
}

func TestInternalPreflightsSnapshotSizeBeforeMutation(t *testing.T) {
	machine := NewMachine()
	target := make([]byte, model.StateCommandMaxSnapshotBytesV1)
	target[0] = 1
	envelope := testInternalEnvelope(CommandBeginCoordinatorEpoch, SubjectKey{Kind: SubjectWorker, WorkerID: 1}, 0, 1, target)
	var commits int
	got := mustResult(t, applyInternalTest(t, machine, envelope, target, func(uint64) mutationPlan {
		return mutationPlan{result: []byte{1}, commit: func() { commits++ }}
	}))
	if got.Code != ResultCapacityExhausted || commits != 0 || len(machine.subjects) != 0 {
		t.Fatalf("preflight result=%#v commits=%d histories=%d", got, commits, len(machine.subjects))
	}
}

func TestInternalRevisionMismatchThatCannotBeRetainedReturnsStableCapacity(t *testing.T) {
	machine := NewMachine()
	target := make([]byte, model.StateCommandMaxSnapshotBytesV1)
	target[0] = 1
	envelope := testInternalEnvelope(CommandBeginCoordinatorEpoch, SubjectKey{Kind: SubjectWorker, WorkerID: 1}, 9, 1, target)

	first := applyInternalTest(t, machine, envelope, target, func(uint64) mutationPlan {
		t.Fatal("revision mismatch executed callback")
		return mutationPlan{}
	})
	firstResult := mustResult(t, first)
	if firstResult.Code != ResultCapacityExhausted || len(machine.subjects) != 0 {
		t.Fatalf("unretainable mismatch = %#v histories=%d, want capacity without mutation", firstResult, len(machine.subjects))
	}
	retry := applyInternalTest(t, machine, envelope, target, func(uint64) mutationPlan {
		t.Fatal("capacity retry executed callback")
		return mutationPlan{}
	})
	if !bytes.Equal(retry, first) {
		t.Fatalf("capacity retry = %x, want byte-identical %x", retry, first)
	}
}

func TestInternalExactTargetReplayThatCannotReplaceHistoryReturnsStableCapacity(t *testing.T) {
	machine := NewMachine()
	key := SubjectKey{Kind: SubjectWorker, WorkerID: 1}
	appliedTarget := make([]byte, 1<<20)
	appliedTarget[0] = 1
	firstEnvelope := testInternalEnvelope(CommandBeginCoordinatorEpoch, key, 0, 1, appliedTarget)
	appliedResult := mustBusinessResult(ResultSuccess, key, 1, model.CoordinatorEpoch{})
	first := applyInternalTest(t, machine, firstEnvelope, appliedTarget, func(uint64) mutationPlan {
		return mutationPlan{result: appliedResult}
	})

	rejectedTarget := []byte{2}
	rejectedEnvelope := testInternalEnvelope(CommandBeginCoordinatorEpoch, key, 9, 2, rejectedTarget)
	rejected := mustResult(t, applyInternalTest(t, machine, rejectedEnvelope, rejectedTarget, func(uint64) mutationPlan {
		t.Fatal("revision mismatch executed callback")
		return mutationPlan{}
	}))
	if rejected.Code != ResultRevisionMismatch {
		t.Fatalf("setup rejection = %#v", rejected)
	}

	fillerKey := SubjectKey{Kind: SubjectWorker, WorkerID: 2}
	fillerTarget := make([]byte, 3_500_000)
	fillerTarget[0] = 3
	fillerEnvelope := testInternalEnvelope(CommandBeginCoordinatorEpoch, fillerKey, 0, 3, fillerTarget)
	applyInternalTest(t, machine, fillerEnvelope, fillerTarget, func(uint64) mutationPlan {
		return mutationPlan{result: []byte{0xa2}}
	})

	replayEnvelope := testInternalEnvelope(CommandBeginCoordinatorEpoch, key, 1, 4, appliedTarget)
	beforeSize := machine.estimatedSnapshotBytes
	beforeHistory := machine.subjects[key]
	replay := applyInternalTest(t, machine, replayEnvelope, appliedTarget, func(uint64) mutationPlan {
		t.Fatal("exact target replay executed callback")
		return mutationPlan{}
	})
	replayResult := mustResult(t, replay)
	if replayResult.Code != ResultCapacityExhausted {
		t.Fatalf("unretainable exact-target replay = %#v, want capacity (prior success %x)", replayResult, first)
	}
	if machine.estimatedSnapshotBytes != beforeSize || machine.subjects[key].id != beforeHistory.id {
		t.Fatal("unretainable exact-target replay mutated dedup state")
	}
	retry := applyInternalTest(t, machine, replayEnvelope, appliedTarget, func(uint64) mutationPlan {
		t.Fatal("capacity retry executed callback")
		return mutationPlan{}
	})
	if !bytes.Equal(retry, replay) {
		t.Fatalf("capacity retry = %x, want byte-identical %x", retry, replay)
	}

	// CapacityExhausted is an unaccepted, stateless refusal. Once an unrelated
	// subject releases snapshot capacity, the same identity remains eligible.
	smallFillerTarget := []byte{4}
	smallFillerEnvelope := testInternalEnvelope(CommandBeginCoordinatorEpoch, fillerKey, 1, 5, smallFillerTarget)
	applyInternalTest(t, machine, smallFillerEnvelope, smallFillerTarget, func(uint64) mutationPlan {
		return mutationPlan{result: []byte{0xa3}}
	})
	accepted := applyInternalTest(t, machine, replayEnvelope, appliedTarget, func(uint64) mutationPlan {
		t.Fatal("accepted exact-target replay executed callback")
		return mutationPlan{}
	})
	if !bytes.Equal(accepted, first) || machine.subjects[key].id != replayEnvelope.Internal.ID {
		t.Fatalf("same identity after capacity release = %x history=%#v, want retained prior success", accepted, machine.subjects[key])
	}
}

func TestMachineSequentialApplyExecutesOneCoordinatorMutation(t *testing.T) {
	machine := NewMachine()
	command := validBeginCommand(t, 0, 2, 0x42)
	encoded, _ := MarshalBeginCoordinatorEpoch(command)
	const applications = 64
	results := make([][]byte, applications)
	for index := range results {
		var err error
		results[index], err = machine.Apply(uint64(index+1), uint64(index+1), encoded)
		if err != nil || !bytes.Equal(results[index], results[0]) {
			t.Fatalf("sequential result %d=%x err=%v; first=%x", index, results[index], err, results[0])
		}
	}
	if machine.coordinatorRevision != 1 {
		t.Fatalf("coordinator revision=%d", machine.coordinatorRevision)
	}
}

func applyInternalTest(t *testing.T, machine *Machine, envelope Envelope, target []byte, prepare func(uint64) mutationPlan) []byte {
	t.Helper()
	machine.mu.Lock()
	defer machine.mu.Unlock()
	result, err := machine.applyInternalLocked(envelope, target, func(next uint64) (mutationPlan, error) { return prepare(next), nil })
	if err != nil {
		t.Fatalf("applyInternalLocked: %v", err)
	}
	return result
}

func testInternalEnvelope(kind CommandKind, key SubjectKey, expected uint64, idByte byte, target []byte) Envelope {
	envelope := Envelope{SchemaVersion: CommandSchemaVersion, ConsensusFingerprint: model.ConsensusFingerprint(), Kind: kind, Internal: &InternalEnvelope{ID: InternalCommandID{idByte}, Subject: key, ExpectedRevision: expected}}
	envelope.Internal.Digest = internalDigest(envelope, target)
	return envelope
}

func cloneInternal(input *InternalEnvelope) *InternalEnvelope { copy := *input; return &copy }

func mustApplyResult(t *testing.T, machine *Machine, index, term uint64, command []byte) CommandResult {
	t.Helper()
	encoded, err := machine.Apply(index, term, command)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	return mustResult(t, encoded)
}

func mustResult(t *testing.T, encoded []byte) CommandResult {
	t.Helper()
	result, err := UnmarshalCommandResult(encoded)
	if err != nil {
		t.Fatalf("UnmarshalCommandResult(%x): %v", encoded, err)
	}
	return result
}

// TestEOFOrderingGuardsAssignmentInstallAndRunningTransition pins the Task 19
// EOF ordering contract: every partition's topology-recomputed EOF must commit
// exactly once before the initial complete assignment set, wrong values are
// rejected without mutation, and Running is unreachable with missing EOFs.
func TestEOFOrderingGuardsAssignmentInstallAndRunningTransition(t *testing.T) {
	machine := NewMachine()
	begin, _ := NewBeginCoordinatorEpoch(InternalCommandID{0xA0}, 0, 1, [16]byte{0xA0})
	applyTask10(t, machine, 1, begin)
	for index := 1; index <= 2; index++ {
		record := WorkerRecord{NodeID: uint16(index), Epoch: model.WorkerEpoch{byte(index), 0x55}, State: WorkerEligible, Revision: 1, Slots: 16, ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint()}
		register, _ := NewRegisterWorker(InternalCommandID{byte(index), 0x55}, 0, record, machine.coordinatorEpoch)
		applyTask10(t, machine, uint64(1+index), register)
	}
	topology, err := model.ValidateTopology(task10ProgressTopology())
	if err != nil {
		t.Fatal(err)
	}
	submit, _ := NewSubmitJob(model.ClientRequestID{ClientID: model.ClientID{0xA1}, Sequence: 1}, topology.Spec(), machine.coordinatorEpoch)
	applyTask10(t, machine, 4, submit)
	job := submit.JobID()
	assignment, err := model.BuildAssignmentSet(job, topology.Digest(), 1, topology, task10EligiblePlacements(machine))
	if err != nil {
		t.Fatal(err)
	}

	install, _ := NewInstallAssignments(InternalCommandID{0xA2}, 1, assignment, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 5, install); got.Code != ResultInvalidTransition {
		t.Fatalf("install without any committed EOF = %#v", got)
	}
	if machine.jobs[job].Assignment != nil {
		t.Fatal("rejected install mutated the assignment")
	}

	sources := make([]model.TaskID, 3)
	for partition := uint16(0); partition < 3; partition++ {
		sources[partition] = model.TaskID{JobID: job, StageID: 1, Partition: partition}
	}
	// A wrong empty claim for a nonempty partition never commits.
	wrongEmpty, _ := NewRecordSourceEOF(InternalCommandID{0xA3, 0xFF}, 0, sources[0], 0, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 6, wrongEmpty); got.Code != ResultInvalidTarget {
		t.Fatalf("wrong empty EOF = %#v", got)
	}
	if _, exists := machine.jobs[job].SourceEOFs[sources[0]]; exists {
		t.Fatal("wrong empty EOF mutated state")
	}
	for partition := uint16(0); partition < 2; partition++ {
		eof, eofErr := model.SourceEOF(topology, sources[partition])
		if eofErr != nil {
			t.Fatal(eofErr)
		}
		record, _ := NewRecordSourceEOF(InternalCommandID{0xA4, byte(partition)}, 0, sources[partition], eof, machine.coordinatorEpoch)
		if got := applyTask10(t, machine, uint64(7+partition), record); got.Code != ResultSuccess {
			t.Fatalf("partition %d EOF = %#v", partition, got)
		}
	}
	partialInstall, _ := NewInstallAssignments(InternalCommandID{0xA5}, 1, assignment, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 9, partialInstall); got.Code != ResultInvalidTransition {
		t.Fatalf("install with one missing EOF = %#v", got)
	}
	lastEOF, err := model.SourceEOF(topology, sources[2])
	if err != nil {
		t.Fatal(err)
	}
	wrongValue, _ := NewRecordSourceEOF(InternalCommandID{0xA6}, 0, sources[2], lastEOF+1, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 10, wrongValue); got.Code != ResultInvalidTarget {
		t.Fatalf("wrong recomputed EOF = %#v", got)
	}
	exact, _ := NewRecordSourceEOF(InternalCommandID{0xA7}, 0, sources[2], lastEOF, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 11, exact); got.Code != ResultSuccess {
		t.Fatalf("exact EOF = %#v", got)
	}
	completeInstall, _ := NewInstallAssignments(InternalCommandID{0xA8}, 1, assignment, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 12, completeInstall); got.Code != ResultSuccess {
		t.Fatalf("install after complete EOFs = %#v", got)
	}
	running, _ := NewTransitionJob(InternalCommandID{0xA9}, 2, job, JobDeploying, JobRunning, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 13, running); got.Code != ResultSuccess {
		t.Fatalf("running transition = %#v", got)
	}
	if machine.jobs[job].Lifecycle != JobRunning {
		t.Fatalf("lifecycle = %d", machine.jobs[job].Lifecycle)
	}
}

// TestCompletionProposalRejectionMatrix pins the exact fail-closed completion
// validation: token, epoch, revisions, contiguity, EOF equality, durable
// increasing worker transactions, and byte-bound digests all reject without
// any mutation.
func TestCompletionProposalRejectionMatrix(t *testing.T) {
	baseReport := func(machine *Machine, job model.JobID, assignment model.AssignmentSet) (model.CompletionReport, model.AssignmentToken) {
		var token model.AssignmentToken
		for _, candidate := range assignment.Tasks {
			if candidate.Task.StageID == 1 && machine.jobs[job].SourceEOFs[candidate.Task].EOF >= 2 {
				token = candidate
				break
			}
		}
		eof := machine.jobs[job].SourceEOFs[token.Task].EOF
		report := model.CompletionReport{JobID: job, JobControlRevision: machine.jobs[job].JobControlRevision, AssignmentRevision: assignment.Revision, Source: token.Task, Token: token, Epoch: machine.coordinatorEpoch, ExpectedCheckpointRevision: 0, Prior: 0, New: 1, EOF: eof, WorkerTransactionID: 11}
		return report, token
	}
	tests := []struct {
		name   string
		mutate func(machine *Machine, report *model.CompletionReport)
	}{
		{name: "gap before committed watermark", mutate: func(_ *Machine, report *model.CompletionReport) {
			report.Prior, report.New = 1, 2
		}},
		{name: "job control revision mismatch", mutate: func(_ *Machine, report *model.CompletionReport) {
			report.JobControlRevision++
		}},
		{name: "stale attempt token", mutate: func(_ *Machine, report *model.CompletionReport) {
			report.Token.Attempt++
		}},
		{name: "foreign worker epoch token", mutate: func(_ *Machine, report *model.CompletionReport) {
			report.Token.WorkerEpoch = model.WorkerEpoch{0xEE}
		}},
		{name: "coordinator epoch mismatch", mutate: func(_ *Machine, report *model.CompletionReport) {
			report.Epoch.Nonce[0] ^= 0xFF
		}},
		{name: "committed EOF mismatch", mutate: func(_ *Machine, report *model.CompletionReport) {
			report.EOF--
		}},
		{name: "uncommitted offline worker", mutate: func(machine *Machine, report *model.CompletionReport) {
			worker := machine.workers[report.Token.WorkerID]
			worker.State = WorkerOffline
			machine.workers[worker.NodeID] = worker
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			machine, job, _, assignment := task10RunningJob(t)
			report, token := baseReport(machine, job, assignment)
			test.mutate(machine, &report)
			report.Digest = model.CompletionReportDigest(report)
			command, err := NewAdvanceCheckpoint(InternalCommandID{0xB0, byte(len(test.name))}, report.ExpectedCheckpointRevision, report, machine.coordinatorEpoch)
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			if got := applyTask10(t, machine, 70, command); got.Code != ResultInvalidTarget {
				t.Fatalf("code = %#v", got)
			}
			if _, exists := machine.jobs[job].Checkpoints[token.Task]; exists {
				t.Fatal("rejected completion mutated checkpoint state")
			}
			if len(machine.workerEvents) != 0 {
				t.Fatal("rejected completion advanced the worker event cursor")
			}
		})
	}

	t.Run("replay under different bytes rejects stale worker transaction", func(t *testing.T) {
		machine, job, _, assignment := task10RunningJob(t)
		report, token := baseReport(machine, job, assignment)
		report.Digest = model.CompletionReportDigest(report)
		first, err := NewAdvanceCheckpoint(InternalCommandID{0xB1}, 0, report, machine.coordinatorEpoch)
		if err != nil {
			t.Fatal(err)
		}
		if got := applyTask10(t, machine, 71, first); got.Code != ResultSuccess {
			t.Fatalf("first advance = %#v", got)
		}
		replay := report
		replay.ExpectedCheckpointRevision, replay.Prior, replay.New = 1, 1, 2
		replay.Digest = model.CompletionReportDigest(replay)
		second, err := NewAdvanceCheckpoint(InternalCommandID{0xB2}, 1, replay, machine.coordinatorEpoch)
		if err != nil {
			t.Fatal(err)
		}
		if got := applyTask10(t, machine, 72, second); got.Code != ResultStaleWorkerEvent {
			t.Fatalf("different-bytes replay = %#v", got)
		}
		checkpoint := machine.jobs[job].Checkpoints[token.Task]
		if checkpoint.Watermark != 1 || checkpoint.Revision != 1 {
			t.Fatalf("replay mutated committed checkpoint: %#v", checkpoint)
		}
	})
}

// TestFailJobReplayCreatesNoSecondTransition pins deterministic failure
// deduplication: one committed FailJob transition, and every late duplicate is
// answered from retained state without another transition.
func TestFailJobReplayCreatesNoSecondTransition(t *testing.T) {
	machine, job, _, assignment := task10RunningJob(t)
	var token model.AssignmentToken
	for _, candidate := range assignment.Tasks {
		if candidate.Task.StageID == 1 {
			token = candidate
			break
		}
	}
	report := model.JobFailureReport{JobID: job, JobControlRevision: machine.jobs[job].JobControlRevision, AssignmentRevision: assignment.Revision, Task: token, Epoch: machine.coordinatorEpoch, TransactionID: 21, Code: model.FailureOperator, DetailDigest: [32]byte{0xC1}}
	firstDigest, secondDigest := failureEventDigest(report), failureEventDigest(report)
	if firstDigest != secondDigest {
		t.Fatal("failure digest is not deterministic")
	}
	fail, err := NewFailJob(InternalCommandID{0xC2}, report.JobControlRevision, report, machine.coordinatorEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if got := applyTask10(t, machine, 80, fail); got.Code != ResultSuccess {
		t.Fatalf("failure = %#v", got)
	}
	failed := machine.jobs[job]
	if failed.Lifecycle != JobFailed || failed.Failure == nil {
		t.Fatalf("job = %#v", failed)
	}
	terminalRevision := failed.JobControlRevision

	// A new leader replays the same durable event bytes under a fresh command
	// identity: the retained applied target answers it Completed from cache
	// without any second transition.
	duplicate, err := NewFailJob(InternalCommandID{0xC3}, report.JobControlRevision, report, machine.coordinatorEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if got := applyTask10(t, machine, 81, duplicate); got.Code != ResultSuccess || got.Revision != terminalRevision {
		t.Fatalf("duplicate replay = %#v", got)
	}
	after := machine.jobs[job]
	if after.JobControlRevision != terminalRevision || after.Lifecycle != JobFailed {
		t.Fatalf("duplicate replay transitioned again: %#v", after)
	}

	// A later distinct failure event for the terminal job is also rejected.
	late := report
	late.TransactionID = 22
	late.DetailDigest[1] = 0xC4
	late.JobControlRevision = terminalRevision
	lateCommand, err := NewFailJob(InternalCommandID{0xC5}, terminalRevision, late, machine.coordinatorEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if got := applyTask10(t, machine, 82, lateCommand); got.Code != ResultInvalidTarget {
		t.Fatalf("terminal-job failure = %#v", got)
	}
	if machine.jobs[job].JobControlRevision != terminalRevision {
		t.Fatal("late failure transitioned a terminal job")
	}
}
