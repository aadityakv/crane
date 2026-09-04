package state

import (
	"math"
	"reflect"
	"testing"

	"github.com/aadityakv/crane/internal/crane/model"
)

func TestWorkerSlotsAreSharedAcrossAllNonterminalJobsAndReleasedAtTerminal(t *testing.T) {
	machine := NewMachine()
	for node := uint16(1); node <= 2; node++ {
		worker := WorkerRecord{
			NodeID: node, Epoch: model.WorkerEpoch{byte(node)}, State: WorkerEligible,
			Revision: 1, Slots: 1, ConsensusFingerprint: model.ConsensusFingerprint(),
			RegistryFingerprint: model.RegistryFingerprint(),
		}
		register, _ := NewRegisterWorker(InternalCommandID{byte(node), 0xf1}, 0, worker)
		if got := applyTask10(t, machine, uint64(node), register); got.Code != ResultSuccess {
			t.Fatalf("register %d = %#v", node, got)
		}
	}

	installJob := func(client byte, start int64, index uint64) (model.JobID, model.AssignmentSet) {
		t.Helper()
		topology, err := model.ValidateTopology(task10Topology(start))
		if err != nil {
			t.Fatal(err)
		}
		submit, err := NewSubmitJob(model.ClientRequestID{ClientID: model.ClientID{client}, Sequence: 1}, topology.Spec(), machine.coordinatorEpoch)
		if err != nil {
			t.Fatal(err)
		}
		if got := applyTask10(t, machine, index, submit); got.Code != ResultSuccess {
			t.Fatalf("submit %x = %#v", client, got)
		}
		job := submit.JobID()
		source := model.TaskID{JobID: job, StageID: 1, Partition: 0}
		eof, _ := model.SourceEOF(topology, source)
		recordEOF, _ := NewRecordSourceEOF(InternalCommandID{client, 0xf2}, 0, source, eof, machine.coordinatorEpoch)
		if got := applyTask10(t, machine, index+1, recordEOF); got.Code != ResultSuccess {
			t.Fatalf("EOF %x = %#v", client, got)
		}
		assignment, err := model.BuildAssignmentSet(job, topology.Digest(), 1, topology, task10EligiblePlacements(machine))
		if err != nil {
			t.Fatal(err)
		}
		install, _ := NewInstallAssignments(InternalCommandID{client, 0xf3}, 1, assignment, machine.coordinatorEpoch)
		return job, func() model.AssignmentSet {
			if got := applyTask10(t, machine, index+2, install); got.Code != ResultSuccess {
				t.Fatalf("install %x = %#v", client, got)
			}
			return assignment
		}()
	}

	firstJob, _ := installJob(0xa1, 0, 10)
	secondTopology, _ := model.ValidateTopology(task10Topology(10))
	secondSubmit, _ := NewSubmitJob(model.ClientRequestID{ClientID: model.ClientID{0xa2}, Sequence: 1}, secondTopology.Spec(), machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 20, secondSubmit); got.Code != ResultSuccess {
		t.Fatalf("second submit = %#v", got)
	}
	secondJob := secondSubmit.JobID()
	secondSource := model.TaskID{JobID: secondJob, StageID: 1, Partition: 0}
	secondEOF, _ := model.SourceEOF(secondTopology, secondSource)
	recordEOF, _ := NewRecordSourceEOF(InternalCommandID{0xa2, 0xf2}, 0, secondSource, secondEOF, machine.coordinatorEpoch)
	applyTask10(t, machine, 21, recordEOF)
	secondSet, err := model.BuildAssignmentSet(secondJob, secondTopology.Digest(), 1, secondTopology, task10EligiblePlacements(machine))
	if err != nil {
		t.Fatal(err)
	}
	install, _ := NewInstallAssignments(InternalCommandID{0xa2, 0xf3}, 1, secondSet, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 22, install); got.Code != ResultInvalidTarget {
		t.Fatalf("cross-job slot overcommit = %#v, want invalid target", got)
	}
	if machine.jobs[secondJob].Assignment != nil || machine.jobs[secondJob].Lifecycle != JobPending {
		t.Fatal("rejected cross-job placement mutated the job")
	}

	cancel, _ := NewCancelJob(model.ClientRequestID{ClientID: model.ClientID{0xa1}, Sequence: 2}, firstJob, machine.jobs[firstJob].JobControlRevision, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 23, cancel); got.Code != ResultSuccess {
		t.Fatalf("cancel first job = %#v", got)
	}
	retry, _ := NewInstallAssignments(InternalCommandID{0xa2, 0xf4}, 1, secondSet, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 24, retry); got.Code != ResultSuccess {
		t.Fatalf("terminal job did not release exact task slots: %#v", got)
	}
}

func TestReplaceWorkerEpochPreservesOperatorDrainingState(t *testing.T) {
	machine := NewMachine()
	oldEpoch := model.WorkerEpoch{0xd1}
	worker := WorkerRecord{NodeID: 7, Epoch: oldEpoch, State: WorkerEligible, Revision: 1, Slots: 4, ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint()}
	register, _ := NewRegisterWorker(InternalCommandID{1}, 0, worker)
	applyTask10(t, machine, 1, register)
	drain, _ := NewDrainWorker(InternalCommandID{2}, 1, 7, oldEpoch)
	applyTask10(t, machine, 2, drain)

	target := worker
	target.Epoch = model.WorkerEpoch{0xd2}
	target.State = WorkerDraining
	target.Revision = 3
	replace, err := NewReplaceWorkerEpoch(InternalCommandID{3}, 2, 7, oldEpoch, target, nil, machine.coordinatorEpoch)
	if err != nil {
		t.Fatalf("Draining -> Draining epoch replacement rejected: %v", err)
	}
	if got := applyTask10(t, machine, 3, replace); got.Code != ResultSuccess {
		t.Fatalf("Draining -> Draining replacement = %#v", got)
	}
	if got := machine.workers[7]; !reflect.DeepEqual(got, target) {
		t.Fatalf("replacement erased operator Draining: got %#v want %#v", got, target)
	}
}

func TestReplaceWorkerEpochRequiresExactCurrentOperatorState(t *testing.T) {
	states := []WorkerState{WorkerEligible, WorkerDraining, WorkerOffline}
	for _, currentState := range states {
		for _, targetState := range states {
			t.Run(string(rune('0'+currentState))+"-to-"+string(rune('0'+targetState)), func(t *testing.T) {
				machine := NewMachine()
				current := WorkerRecord{NodeID: 7, Epoch: model.WorkerEpoch{1}, State: WorkerEligible, Revision: 1, Slots: 4, ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint()}
				register, err := NewRegisterWorker(InternalCommandID{0xb7}, 0, current)
				if err == nil {
					t.Fatal("unfenced register constructor unexpectedly succeeded")
				}
				// applyTask10 establishes the fence and binds the legacy setup command.
				if got := applyTask10(t, machine, 1, register); got.Code != ResultSuccess {
					t.Fatalf("register = %#v", got)
				}
				current = machine.workers[7]
				current.State = currentState
				machine.workers[7] = current
				target := current
				target.Epoch = model.WorkerEpoch{2}
				target.State = targetState
				target.Revision = 2
				command, err := NewReplaceWorkerEpoch(InternalCommandID{byte(currentState), byte(targetState)}, 1, 7, current.Epoch, target, nil, machine.coordinatorEpoch)
				if err != nil {
					t.Fatal(err)
				}
				before := machine.workers[7]
				got := applyTask10(t, machine, 2, command)
				if currentState == targetState {
					if got.Code != ResultSuccess || machine.workers[7] != target {
						t.Fatalf("same-state replacement = %#v worker=%#v", got, machine.workers[7])
					}
				} else if got.Code != ResultInvalidTransition || machine.workers[7] != before {
					t.Fatalf("cross-state replacement = %#v worker=%#v, want atomic rejection", got, machine.workers[7])
				}
			})
		}
	}
}

func TestStateCommandContractBindsAssignmentDigestDomain(t *testing.T) {
	contract := model.StateCommandContractV1()
	for _, domain := range contract.DigestDomains {
		if domain == "cs425/crane/assignment-set/v1" {
			return
		}
	}
	t.Fatal("state command contract omits the assignment-set digest domain")
}

func TestResultManifestRejectsImpossibleRecordCountAndByteCombinations(t *testing.T) {
	job := model.JobID{1}
	sink := model.TaskID{JobID: job, StageID: 2}
	replicas := model.ResultReplicaSet{SinkTask: sink, PrimaryNodeID: 1, SecondaryNodeID: 2, PrimaryEpoch: model.WorkerEpoch{1}, SecondaryEpoch: model.WorkerEpoch{2}}
	base := ResultManifest{JobID: job, SinkTask: sink, ManifestRevision: 1, SpecificationHash: [32]byte{1}, Checksum: [32]byte{2}, Replicas: replicas}
	for name, mutate := range map[string]func(*ResultManifest){
		"zero-count-nonzero-bytes":  func(value *ResultManifest) { value.TotalBytes = 1 },
		"nonzero-count-zero-bytes":  func(value *ResultManifest) { value.RecordCount = 1 },
		"count-too-large-for-bytes": func(value *ResultManifest) { value.RecordCount, value.TotalBytes = 2, 1 },
		"below-minimum-record": func(value *ResultManifest) {
			value.RecordCount, value.TotalBytes = 1, model.ResultArtifactMinRecordBytesV1-1
		},
		"above-maximum-record": func(value *ResultManifest) {
			value.RecordCount, value.TotalBytes = 1, model.ResultArtifactMaxRecordBytesV1+1
		},
		"count-over-bound": func(value *ResultManifest) {
			value.RecordCount, value.TotalBytes = model.ResultArtifactMaxRecordCountV1+1, model.LimitsV1().MaxResultRecordsBytesPerJob
		},
		"count-overflow": func(value *ResultManifest) {
			value.RecordCount, value.TotalBytes = math.MaxUint64, model.LimitsV1().MaxResultRecordsBytesPerJob
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := base
			mutate(&candidate)
			if err := candidate.Validate(); err == nil {
				t.Fatalf("accepted impossible manifest: %#v", candidate)
			}
		})
	}
	for name, pair := range map[string][2]uint64{
		"empty":           {0, 0},
		"one-minimum":     {1, model.ResultArtifactMinRecordBytesV1},
		"one-maximum":     {1, model.ResultArtifactMaxRecordBytesV1},
		"maximum-count":   {model.ResultArtifactMaxRecordCountV1, model.ResultArtifactMaxRecordCountV1 * model.ResultArtifactMinRecordBytesV1},
		"artifact-budget": {(model.LimitsV1().MaxResultRecordsBytesPerJob + model.ResultArtifactMaxRecordBytesV1 - 1) / model.ResultArtifactMaxRecordBytesV1, model.LimitsV1().MaxResultRecordsBytesPerJob},
	} {
		t.Run("valid-"+name, func(t *testing.T) {
			candidate := base
			candidate.RecordCount, candidate.TotalBytes = pair[0], pair[1]
			if err := candidate.Validate(); err != nil {
				t.Fatalf("rejected boundary manifest %#v: %v", candidate, err)
			}
		})
	}
}

func TestTask10CodecLayoutsAreTracedFromActualEncoder(t *testing.T) {
	if got, want := StateCommandEncodingLayouts(), model.StateCommandContractV1().EnvelopeLayouts; !reflect.DeepEqual(got, want) {
		t.Fatalf("actual state codec layouts\n got: %#v\nwant: %#v", got, want)
	}
	if got, want := StateCommandEnumDomains(), model.StateCommandContractV1().EnumDomains; !reflect.DeepEqual(got, want) {
		t.Fatalf("actual state codec enum domains\n got: %#v\nwant: %#v", got, want)
	}
}

func TestCanonicalSnapshotEstimatorMatchesEveryRetainedCollection(t *testing.T) {
	machine, _, _, _ := task10RunningJob(t)
	machine.mu.Lock()
	defer machine.mu.Unlock()
	got, ok := machine.estimateCanonicalSnapshotBytesLocked()
	if !ok {
		t.Fatal("legal retained state did not fit the canonical snapshot estimator")
	}
	if got != machine.estimatedSnapshotBytes {
		t.Fatalf("incremental snapshot estimate=%d, canonical recomputation=%d", machine.estimatedSnapshotBytes, got)
	}
}

func TestSnapshotPreflightAcceptsExactBoundaryAndRejectsPlusOneAtomically(t *testing.T) {
	setup := func(t *testing.T) (*Machine, RegisterWorker, uint64) {
		t.Helper()
		machine := NewMachine()
		begin, _ := NewBeginCoordinatorEpoch(InternalCommandID{0xca}, 0, 1, [16]byte{0xca})
		if got := applyTask10(t, machine, 1, begin); got.Code != ResultSuccess {
			t.Fatalf("begin = %#v", got)
		}
		worker := WorkerRecord{NodeID: 99, Epoch: model.WorkerEpoch{99}, State: WorkerEligible, Revision: 1, Slots: 1, ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint()}
		command, err := NewRegisterWorker(InternalCommandID{0xcb}, 0, worker, machine.coordinatorEpoch)
		if err != nil {
			t.Fatal(err)
		}
		targetBytes := registerWorkerTarget(command)
		resultBytes, err := marshalBusinessResult(ResultSuccess, command.Envelope.Internal.Subject, 1, model.CoordinatorEpoch{})
		if err != nil {
			t.Fatal(err)
		}
		increment, ok := checkedAddMany(subjectHistoryFixedBytes, 2*uint64(len(targetBytes)), 2*uint64(len(resultBytes)), uint64(workerRecordEstimatedBytes))
		if !ok {
			t.Fatal("test increment overflow")
		}
		return machine, command, increment
	}

	machine, command, increment := setup(t)
	machine.estimatedSnapshotBytes = model.StateCommandMaxSnapshotBytesV1 - increment
	if got := applyTask10(t, machine, 2, command); got.Code != ResultSuccess || machine.estimatedSnapshotBytes != model.StateCommandMaxSnapshotBytesV1 {
		t.Fatalf("exact boundary = %#v size=%d", got, machine.estimatedSnapshotBytes)
	}

	machine, command, increment = setup(t)
	machine.estimatedSnapshotBytes = model.StateCommandMaxSnapshotBytesV1 - increment + 1
	before := machine.estimatedSnapshotBytes
	if got := applyTask10(t, machine, 2, command); got.Code != ResultCapacityExhausted {
		t.Fatalf("boundary+1 = %#v, want capacity", got)
	}
	if _, exists := machine.workers[command.Worker.NodeID]; exists || machine.estimatedSnapshotBytes != before {
		t.Fatal("boundary+1 rejection mutated worker or retained-byte accounting")
	}
	if _, exists := machine.subjects[command.Envelope.Internal.Subject]; exists {
		t.Fatal("boundary+1 rejection retained an unaccepted history")
	}
}

func TestTask10EnvelopeCarriesCanonicalCoordinatorFence(t *testing.T) {
	fence := model.CoordinatorEpoch{Term: 1, BeginIndex: 2, Coordinator: 3, Nonce: [16]byte{4}}
	envelope := Envelope{CoordinatorEpoch: fence}
	if envelope.CoordinatorEpoch != fence {
		t.Fatal("common command envelope lost the coordinator fence")
	}
}

func TestEveryTask10MutationRejectsUnseenStaleFenceBeforeMutation(t *testing.T) {
	machine, job, topology, assignment := task10RunningJob(t)
	oldFence := machine.coordinatorEpoch

	worker := machine.workers[1]
	registerTarget := WorkerRecord{NodeID: 9, Epoch: model.WorkerEpoch{9}, State: WorkerEligible, Revision: 1, Slots: 1, ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint()}
	register, _ := NewRegisterWorker(InternalCommandID{0xe0, 1}, 0, registerTarget, oldFence)
	staleRegisterTarget := registerTarget
	staleRegisterTarget.NodeID = 10
	staleRegisterTarget.Epoch = model.WorkerEpoch{10}
	staleRegister, _ := NewRegisterWorker(InternalCommandID{0xe0, 12}, 0, staleRegisterTarget, oldFence)
	drain, _ := NewDrainWorker(InternalCommandID{0xe0, 2}, worker.Revision, worker.NodeID, worker.Epoch, oldFence)
	deactivate, _ := NewDeactivateWorker(InternalCommandID{0xe0, 3}, worker.Revision, worker.NodeID, worker.Epoch, nil, oldFence)
	replaceTarget := worker
	replaceTarget.Epoch = model.WorkerEpoch{0xee, 1}
	replaceTarget.Revision++
	replaceWorker, _ := NewReplaceWorkerEpoch(InternalCommandID{0xe0, 4}, worker.Revision, worker.NodeID, worker.Epoch, replaceTarget, nil, oldFence)
	submit, _ := NewSubmitJob(model.ClientRequestID{ClientID: model.ClientID{0xe1}, Sequence: 1}, task10Topology(100), oldFence)
	cancel, _ := NewCancelJob(model.ClientRequestID{ClientID: model.ClientID{0xe2}, Sequence: 1}, job, machine.jobs[job].JobControlRevision, oldFence)
	source := assignment.Tasks[0]
	for _, token := range assignment.Tasks {
		if token.Task.StageID == 1 {
			source = token
			break
		}
	}
	eof := machine.jobs[job].SourceEOFs[source.Task].EOF
	recordEOF, _ := NewRecordSourceEOF(InternalCommandID{0xe0, 5}, 0, source.Task, eof, oldFence)
	install, _ := NewInstallAssignments(InternalCommandID{0xe0, 6}, machine.jobs[job].JobControlRevision, assignment, oldFence)
	tokens := append([]model.AssignmentToken(nil), assignment.Tasks...)
	for index := range tokens {
		tokens[index].AssignmentRevision++
	}
	targetSet, err := model.NewAssignmentSet(job, assignment.Revision+1, tokens, assignment.ResultReplicas, topology)
	if err != nil {
		t.Fatal(err)
	}
	replaceSet, _ := NewReplaceAssignments(InternalCommandID{0xe0, 7}, machine.jobs[job].JobControlRevision, job, assignment.Revision, assignment.Digest, NeedsReassignmentDigest(nil), targetSet, oldFence)
	completion := model.CompletionReport{JobID: job, JobControlRevision: machine.jobs[job].JobControlRevision, AssignmentRevision: assignment.Revision, Source: source.Task, Token: source, Epoch: oldFence, ExpectedCheckpointRevision: 0, Prior: 0, New: eof, EOF: eof, WorkerTransactionID: 1}
	completion.Digest = model.CompletionReportDigest(completion)
	advance, _ := NewAdvanceCheckpoint(InternalCommandID{0xe0, 8}, 0, completion, oldFence)
	replica := assignment.ResultReplicas[0]
	manifest := ResultManifest{JobID: job, SinkTask: replica.SinkTask, ManifestRevision: 1, SpecificationHash: topology.Digest(), RecordCount: 1, TotalBytes: model.ResultArtifactMinRecordBytesV1, Checksum: [32]byte{1}, Replicas: replica}
	seal, _ := NewSealManifest(InternalCommandID{0xe0, 9}, 0, manifest, oldFence)
	transition, _ := NewTransitionJob(InternalCommandID{0xe0, 10}, machine.jobs[job].JobControlRevision, job, JobRunning, JobDraining, oldFence)
	failure := model.JobFailureReport{JobID: job, JobControlRevision: machine.jobs[job].JobControlRevision, AssignmentRevision: assignment.Revision, Task: source, Epoch: oldFence, TransactionID: 1, Code: model.FailureOperator, DetailDigest: [32]byte{1}}
	fail, _ := NewFailJob(InternalCommandID{0xe0, 11}, machine.jobs[job].JobControlRevision, failure, oldFence)

	// Cache one old-fence internal success before advancing the coordinator.
	cachedBytes, err := MarshalCommand(register)
	if err != nil {
		t.Fatal(err)
	}
	if got := mustApplyResult(t, machine, 90, 1, cachedBytes); got.Code != ResultSuccess {
		t.Fatalf("cached old-fence register = %#v", got)
	}
	newEpoch, _ := NewBeginCoordinatorEpoch(InternalCommandID{0xe3}, machine.coordinatorRevision, 2, [16]byte{0xe3})
	if got := applyTask10(t, machine, 100, newEpoch); got.Code != ResultSuccess {
		t.Fatalf("advance coordinator = %#v", got)
	}
	currentFence := machine.coordinatorEpoch
	if currentFence == oldFence {
		t.Fatal("coordinator fence did not advance")
	}

	// Exact cached identity wins even though its proposal carries the old fence.
	if got := mustApplyResult(t, machine, 101, 2, cachedBytes); got.Code != ResultSuccess || machine.workers[9] != registerTarget {
		t.Fatalf("cached old-fence replay = %#v", got)
	}

	commands := []any{staleRegister, drain, deactivate, replaceWorker, submit, cancel, recordEOF, install, replaceSet, advance, seal, transition, fail}
	for offset, command := range commands {
		clientsBefore, subjectsBefore := len(machine.clients), len(machine.subjects)
		bytesBefore := machine.estimatedSnapshotBytes
		workerBefore := machine.workers[1]
		jobBefore := cloneJobRecord(machine.jobs[job])
		encoded, err := MarshalCommand(command)
		if err != nil {
			t.Fatalf("MarshalCommand(%T): %v", command, err)
		}
		got := mustApplyResult(t, machine, 102+uint64(offset), 2, encoded)
		if got.Code != ResultStaleEpoch || got.Epoch != currentFence {
			t.Fatalf("%T stale result = %#v, want current fence %#v", command, got, currentFence)
		}
		if len(machine.clients) != clientsBefore || len(machine.subjects) != subjectsBefore || machine.estimatedSnapshotBytes != bytesBefore || machine.workers[1] != workerBefore || !reflect.DeepEqual(machine.jobs[job], jobBefore) {
			t.Fatalf("%T stale fence mutated replicated state", command)
		}
	}

	// The stale client refusal was stateless: the same logical sequence can be
	// rewrapped under the current fence and then execute/cache normally.
	currentSubmit := bindTask10CommandFenceForTest(submit, currentFence).(SubmitJob)
	if got := applyTask10(t, machine, 120, currentSubmit); got.Code != ResultSuccess {
		t.Fatalf("current-fence client recovery = %#v", got)
	}
	if history, ok := machine.clients[currentSubmit.Envelope.Client.Request.ClientID]; !ok || history.sequence != 1 {
		t.Fatal("current-fence client recovery was not cached")
	}

	// An unseen stale internal operation is likewise unretained, but its retry
	// uses a fresh identity and a digest recomputed under the current fence.
	currentDrain, _ := NewDrainWorker(InternalCommandID{0xe4}, worker.Revision, worker.NodeID, worker.Epoch, currentFence)
	if got := applyTask10(t, machine, 121, currentDrain); got.Code != ResultSuccess {
		t.Fatalf("fresh current-fence internal recovery = %#v", got)
	}
}
