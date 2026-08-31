package state

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/aaditya/cs425mp3/internal/crane/model"
)

func TestAuthoritativeRevisionDriftNeverReplaysObsoleteAppliedTarget(t *testing.T) {
	machine, job, _, assignment := task10AssignedJob(t, 4)
	install, _ := NewInstallAssignments(InternalCommandID{0xd0}, 1, assignment, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 20, install); got.Code != ResultSuccess {
		t.Fatalf("install = %#v", got)
	}
	running, _ := NewTransitionJob(InternalCommandID{0xd1}, 2, job, JobDeploying, JobRunning, machine.coordinatorEpoch)
	runningBytes, _ := MarshalCommand(running)
	firstBytes, err := machine.Apply(21, 1, runningBytes)
	if err != nil {
		t.Fatal(err)
	}
	if got := mustResult(t, firstBytes); got.Code != ResultSuccess || got.Revision != 3 {
		t.Fatalf("running = %#v", got)
	}
	cancel, _ := NewCancelJob(model.ClientRequestID{ClientID: model.ClientID{0x71}, Sequence: 2}, job, 3, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 22, cancel); got.Code != ResultSuccess || got.Revision != 4 {
		t.Fatalf("cancel = %#v", got)
	}

	// The exact old ID remains replayable even after a client advanced the
	// authoritative JobControl revision.
	replayed, err := machine.Apply(23, 1, runningBytes)
	if err != nil || !bytes.Equal(replayed, firstBytes) {
		t.Fatalf("exact-ID replay = %x,%v want %x", replayed, err, firstBytes)
	}

	fresh, _ := NewTransitionJob(InternalCommandID{0xd2}, 4, job, JobDeploying, JobRunning, machine.coordinatorEpoch)
	freshBytes, _ := MarshalCommand(fresh)
	rejected, err := machine.Apply(24, 1, freshBytes)
	if err != nil {
		t.Fatal(err)
	}
	if got := mustResult(t, rejected); got.Code != ResultInvalidTransition || got.Revision != 4 {
		t.Fatalf("fresh old target after authoritative drift = %#v, want current-revision rejection", got)
	}
	if machine.jobs[job].Lifecycle != JobCanceled || machine.jobs[job].JobControlRevision != 4 {
		t.Fatal("obsolete target replay mutated canceled job")
	}
	retry, err := machine.Apply(25, 1, freshBytes)
	if err != nil || !bytes.Equal(retry, rejected) {
		t.Fatalf("rejection retry = %x,%v want owned identical %x", retry, err, rejected)
	}
	retry[0] ^= 0xff
	again, _ := machine.Apply(26, 1, freshBytes)
	if !bytes.Equal(again, rejected) {
		t.Fatal("caller mutation aliased cached rejection")
	}
}

func TestGenericAuthoritativeRevisionDriftExecutesPrepareInsteadOfOldReplay(t *testing.T) {
	machine := NewMachine()
	key := SubjectKey{Kind: SubjectWorker, WorkerID: 7}
	target := []byte("same-target")
	first := testInternalEnvelope(CommandBeginCoordinatorEpoch, key, 0, 0xe1, target)
	machine.mu.Lock()
	firstBytes, err := machine.applyInternalResolvedAtLocked(first, target, target, uint64Pointer(0), func(next uint64) (mutationPlan, error) {
		return mutationPlan{result: mustBusinessResult(ResultSuccess, key, next, model.CoordinatorEpoch{})}, nil
	})
	machine.mu.Unlock()
	if err != nil || mustResult(t, firstBytes).Revision != 1 {
		t.Fatalf("first = %x,%v", firstBytes, err)
	}

	fresh := testInternalEnvelope(CommandBeginCoordinatorEpoch, key, 2, 0xe2, target)
	called := false
	machine.mu.Lock()
	second, err := machine.applyInternalResolvedAtLocked(fresh, target, target, uint64Pointer(2), func(next uint64) (mutationPlan, error) {
		called = true
		result := mustBusinessResult(ResultRevisionMismatch, key, next-1, model.CoordinatorEpoch{})
		return mutationPlan{result: result, reject: true}, nil
	})
	machine.mu.Unlock()
	if err != nil || !called {
		t.Fatalf("drift prepare called=%v err=%v", called, err)
	}
	if got := mustResult(t, second); got.Code != ResultRevisionMismatch || got.Revision != 2 {
		t.Fatalf("drift result = %#v", got)
	}
}

func TestWorkerInvalidationRevisionDriftNeverReplaysOldLifecycleTarget(t *testing.T) {
	machine, job, _, assignment := task10AssignedJob(t, 4)
	install, _ := NewInstallAssignments(InternalCommandID{0xd5}, 1, assignment, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 30, install); got.Code != ResultSuccess {
		t.Fatalf("install = %#v", got)
	}
	running, _ := NewTransitionJob(InternalCommandID{0xd6}, 2, job, JobDeploying, JobRunning, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 31, running); got.Code != ResultSuccess {
		t.Fatalf("running = %#v", got)
	}
	token := assignment.Tasks[0]
	worker := machine.workers[token.WorkerID]
	affected := []AffectedAssignment{{JobID: job, JobControlRevision: 3, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest}}
	deactivate, _ := NewDeactivateWorker(InternalCommandID{0xd7}, worker.Revision, worker.NodeID, worker.Epoch, affected, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 32, deactivate); got.Code != ResultSuccess || machine.jobs[job].JobControlRevision != 4 {
		t.Fatalf("deactivate = %#v jobrev=%d", got, machine.jobs[job].JobControlRevision)
	}
	fresh, _ := NewTransitionJob(InternalCommandID{0xd8}, 4, job, JobDeploying, JobRunning, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 33, fresh); got.Code != ResultInvalidTransition || got.Revision != 4 {
		t.Fatalf("worker-drift old lifecycle target = %#v", got)
	}
}

func uint64Pointer(value uint64) *uint64 { return &value }

func TestReplacementAllowsOnlyUnchangedDrainingPlacements(t *testing.T) {
	for _, unchanged := range []string{"task", "primary", "secondary"} {
		t.Run(unchanged, func(t *testing.T) {
			machine, job, topology, old := replacementReviewFixture(t, unchanged)
			target, drainingWorker := replacementTargetWithUnchangedDraining(t, old, topology, machine.jobs[job].NeedsReassignment[0], unchanged)
			worker := machine.workers[drainingWorker]
			worker.State = WorkerDraining
			machine.workers[drainingWorker] = worker
			command, _ := NewReplaceAssignments(InternalCommandID{0xf0, byte(len(unchanged))}, machine.jobs[job].JobControlRevision, job, old.Revision, old.Digest, NeedsReassignmentDigest(machine.jobs[job].NeedsReassignment), target, machine.coordinatorEpoch)
			if got := applyTask10(t, machine, 30, command); got.Code != ResultSuccess {
				t.Fatalf("unchanged %s on Draining worker rejected: %#v", unchanged, got)
			}
			assertCanonicalSnapshotEstimate(t, machine)
		})
	}
}

func assertCanonicalSnapshotEstimate(t *testing.T, machine *Machine) {
	t.Helper()
	got, ok := machine.estimateCanonicalSnapshotBytesLocked()
	if !ok || got != machine.estimatedSnapshotBytes {
		t.Fatalf("incremental snapshot estimate=%d canonical=%d fits=%v", machine.estimatedSnapshotBytes, got, ok)
	}
}

func TestReplacementRejectsChangedDrainingOfflineAndStalePlacementsAtomically(t *testing.T) {
	for _, state := range []WorkerState{WorkerDraining, WorkerOffline} {
		t.Run(string(rune('0'+state)), func(t *testing.T) {
			machine, job, topology, old := replacementReviewFixture(t, "primary")
			target, _ := replacementTargetWithUnchangedDraining(t, old, topology, machine.jobs[job].NeedsReassignment[0], "primary")
			changed := target.ResultReplicas[0].SecondaryNodeID
			worker := machine.workers[changed]
			worker.State = state
			machine.workers[changed] = worker
			before := cloneJobRecord(machine.jobs[job])
			command, _ := NewReplaceAssignments(InternalCommandID{0xf1, byte(state)}, before.JobControlRevision, job, old.Revision, old.Digest, NeedsReassignmentDigest(before.NeedsReassignment), target, machine.coordinatorEpoch)
			if got := applyTask10(t, machine, 31, command); got.Code != ResultInvalidTarget {
				t.Fatalf("changed placement state %d = %#v", state, got)
			}
			if !reflect.DeepEqual(machine.jobs[job], before) {
				t.Fatal("invalid changed placement mutated job")
			}
		})
	}
	for _, mode := range []string{"missing", "stale-epoch"} {
		t.Run(mode, func(t *testing.T) {
			machine, job, topology, old := replacementReviewFixture(t, "primary")
			target, _ := replacementTargetWithUnchangedDraining(t, old, topology, machine.jobs[job].NeedsReassignment[0], "primary")
			changed := target.ResultReplicas[0].SecondaryNodeID
			if mode == "missing" {
				delete(machine.workers, changed)
			} else {
				replicas := append([]model.ResultReplicaSet(nil), target.ResultReplicas...)
				replicas[0].SecondaryEpoch = model.WorkerEpoch{0xff}
				var err error
				target, err = model.NewAssignmentSet(target.JobID, target.Revision, target.Tasks, replicas, topology)
				if err != nil {
					t.Fatal(err)
				}
			}
			before := cloneJobRecord(machine.jobs[job])
			command, _ := NewReplaceAssignments(InternalCommandID{0xf2, byte(len(mode))}, before.JobControlRevision, job, old.Revision, old.Digest, NeedsReassignmentDigest(before.NeedsReassignment), target, machine.coordinatorEpoch)
			if got := applyTask10(t, machine, 32, command); got.Code != ResultInvalidTarget {
				t.Fatalf("%s changed placement = %#v", mode, got)
			}
			if !reflect.DeepEqual(machine.jobs[job], before) {
				t.Fatalf("%s changed placement mutated job", mode)
			}
		})
	}
}

func TestReplacementCountsUnchangedTokensAtGlobalSlotBoundary(t *testing.T) {
	run := func(t *testing.T, spare uint16) (CommandResult, *Machine, model.JobID, JobRecord) {
		t.Helper()
		machine, job, topology, old := replacementReviewFixture(t, "primary")
		target, _ := replacementTargetWithUnchangedDraining(t, old, topology, machine.jobs[job].NeedsReassignment[0], "primary")
		workerID := target.Tasks[0].WorkerID
		candidateCount := uint16(0)
		for _, token := range target.Tasks {
			if token.WorkerID == workerID {
				candidateCount++
			}
		}
		worker := machine.workers[workerID]
		worker.Slots = candidateCount + spare
		machine.workers[workerID] = worker
		otherJob := model.JobID{0xfa}
		otherToken := target.Tasks[0]
		otherToken.Task.JobID = otherJob
		otherSet := model.AssignmentSet{JobID: otherJob, Revision: 1, Tasks: []model.AssignmentToken{otherToken}}
		machine.jobs[otherJob] = JobRecord{JobID: otherJob, Lifecycle: JobRunning, Assignment: &otherSet}
		before := cloneJobRecord(machine.jobs[job])
		command, _ := NewReplaceAssignments(InternalCommandID{0xf8, byte(spare)}, before.JobControlRevision, job, old.Revision, old.Digest, NeedsReassignmentDigest(before.NeedsReassignment), target, machine.coordinatorEpoch)
		return applyTask10(t, machine, 40, command), machine, job, before
	}
	if got, _, _, _ := run(t, 1); got.Code != ResultSuccess {
		t.Fatalf("exact cluster slot boundary = %#v", got)
	}
	got, machine, job, before := run(t, 0)
	if got.Code != ResultInvalidTarget || !reflect.DeepEqual(machine.jobs[job], before) {
		t.Fatalf("slot boundary+1 = %#v job=%#v, want atomic rejection", got, machine.jobs[job])
	}
}

func TestReplacementRejectsInvalidUnchangedPlacementAtomically(t *testing.T) {
	for _, mode := range []string{"offline", "missing", "stale-epoch"} {
		t.Run(mode, func(t *testing.T) {
			machine, job, topology, old := replacementReviewFixture(t, "primary")
			target, unchangedWorker := replacementTargetWithUnchangedDraining(t, old, topology, machine.jobs[job].NeedsReassignment[0], "primary")
			switch mode {
			case "offline":
				worker := machine.workers[unchangedWorker]
				worker.State = WorkerOffline
				machine.workers[unchangedWorker] = worker
			case "missing":
				delete(machine.workers, unchangedWorker)
			case "stale-epoch":
				worker := machine.workers[unchangedWorker]
				worker.Epoch = model.WorkerEpoch{0xfd}
				machine.workers[unchangedWorker] = worker
			}
			before := cloneJobRecord(machine.jobs[job])
			command, _ := NewReplaceAssignments(InternalCommandID{0xf9, byte(len(mode))}, before.JobControlRevision, job, old.Revision, old.Digest, NeedsReassignmentDigest(before.NeedsReassignment), target, machine.coordinatorEpoch)
			if got := applyTask10(t, machine, 41, command); got.Code != ResultInvalidTarget {
				t.Fatalf("%s unchanged placement = %#v", mode, got)
			}
			if !reflect.DeepEqual(machine.jobs[job], before) {
				t.Fatalf("%s unchanged placement mutated job", mode)
			}
		})
	}
}

func replacementReviewFixture(t *testing.T, unchanged string) (*Machine, model.JobID, model.ValidatedTopology, model.AssignmentSet) {
	t.Helper()
	machine, job, topology, old := task10AssignedJob(t, 6)
	install, _ := NewInstallAssignments(InternalCommandID{0xee}, 1, old, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 15, install); got.Code != ResultSuccess {
		t.Fatalf("install = %#v", got)
	}
	record := cloneJobRecord(machine.jobs[job])
	marker := NeedsReassignment{Kind: ResultReplicaTarget, SinkTask: old.ResultReplicas[0].SinkTask, ReplicaRole: model.SecondaryReplica, OldWorkerID: old.ResultReplicas[0].SecondaryNodeID, OldWorkerEpoch: old.ResultReplicas[0].SecondaryEpoch}
	if unchanged == "secondary" {
		source := old.Tasks[0]
		for _, token := range old.Tasks {
			if token.Task.StageID == 1 {
				source = token
				break
			}
		}
		marker = NeedsReassignment{Kind: TaskTarget, Task: source.Task, OldWorkerID: source.WorkerID, OldWorkerEpoch: source.WorkerEpoch}
	}
	record.NeedsReassignment = []NeedsReassignment{marker}
	record.JobControlRevision++
	machine.jobs[job] = record
	machine.estimatedSnapshotBytes += reassignmentMarkerEstimatedBytes
	return machine, job, topology, old
}

func replacementTargetWithUnchangedDraining(t *testing.T, old model.AssignmentSet, topology model.ValidatedTopology, marker NeedsReassignment, unchanged string) (model.AssignmentSet, uint16) {
	t.Helper()
	tokens := append([]model.AssignmentToken(nil), old.Tasks...)
	for i := range tokens {
		tokens[i].AssignmentRevision = old.Revision + 1
	}
	replicas := append([]model.ResultReplicaSet(nil), old.ResultReplicas...)
	used := map[uint16]bool{replicas[0].PrimaryNodeID: true, replicas[0].SecondaryNodeID: true}
	for _, token := range tokens {
		used[token.WorkerID] = true
	}
	var replacement uint16
	for node := uint16(1); node <= 6; node++ {
		if !used[node] {
			replacement = node
			break
		}
	}
	if marker.Kind == ResultReplicaTarget {
		replicas[0].SecondaryNodeID = replacement
		replicas[0].SecondaryEpoch = model.WorkerEpoch{byte(replacement)}
	} else {
		for index := range tokens {
			if tokens[index].Task == marker.Task {
				tokens[index].WorkerID, tokens[index].WorkerEpoch, tokens[index].Attempt = replacement, model.WorkerEpoch{byte(replacement)}, tokens[index].Attempt+1
			}
		}
	}
	target, err := model.NewAssignmentSet(old.JobID, old.Revision+1, tokens, replicas, topology)
	if err != nil {
		t.Fatal(err)
	}
	switch unchanged {
	case "task":
		return target, tokens[0].WorkerID
	case "primary":
		return target, replicas[0].PrimaryNodeID
	case "secondary":
		return target, old.ResultReplicas[0].SecondaryNodeID
	default:
		t.Fatal("unknown unchanged fixture")
	}
	return model.AssignmentSet{}, 0
}

func TestTask10ExportedPackageSymbolsHaveUtilityDocs(t *testing.T) {
	for _, name := range []string{"command.go", "workers.go", "jobs.go", "assignments.go", "checkpoints.go", "manifests.go", "codec_contract.go"} {
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(".", name), nil, parser.ParseComments)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range file.Decls {
			switch value := declaration.(type) {
			case *ast.FuncDecl:
				if value.Recv == nil && value.Name.IsExported() && value.Doc == nil {
					t.Errorf("%s: exported function %s lacks utility docs", name, value.Name.Name)
				}
			case *ast.GenDecl:
				for _, spec := range value.Specs {
					typeSpec, ok := spec.(*ast.TypeSpec)
					if ok && typeSpec.Name.IsExported() && value.Doc == nil {
						t.Errorf("%s: exported type %s lacks utility docs", name, typeSpec.Name.Name)
					}
				}
			}
		}
	}
}

func TestCanonicalEstimatorCoversEveryRetainedCollectionAndOptionalField(t *testing.T) {
	machine, job, topology, assignment := task10RunningJob(t)
	if len(machine.clients) == 0 || len(machine.subjects) == 0 || machine.jobs[job].Assignment == nil || len(machine.jobs[job].SourceEOFs) == 0 {
		t.Fatal("fixture lacks required retained client/subject/assignment/EOF state")
	}
	record := cloneJobRecord(machine.jobs[job])
	token := assignment.Tasks[0]
	record.NeedsReassignment = []NeedsReassignment{{Kind: TaskTarget, Task: token.Task, OldWorkerID: token.WorkerID, OldWorkerEpoch: token.WorkerEpoch}}
	record.Checkpoints[token.Task] = CheckpointRecord{Watermark: 1, Revision: 1}
	replica := assignment.ResultReplicas[0]
	record.Manifests[replica.SinkTask] = ResultManifest{JobID: job, SinkTask: replica.SinkTask, ManifestRevision: 1, SpecificationHash: topology.Digest(), RecordCount: 1, TotalBytes: model.ResultArtifactMinRecordBytesV1, Checksum: [32]byte{1}, Replicas: replica}
	failure := model.JobFailureReport{JobID: job, JobControlRevision: record.JobControlRevision, AssignmentRevision: assignment.Revision, Task: token, Epoch: machine.coordinatorEpoch, TransactionID: 1, Code: model.FailureOperator, DetailDigest: [32]byte{1}}
	record.Failure = &failure
	machine.jobs[job] = record
	cursorKey := workerEventKey{WorkerID: token.WorkerID, WorkerEpoch: token.WorkerEpoch}
	machine.workerEvents[cursorKey] = workerEventCursor{TransactionID: 1, Digest: [32]byte{1}}
	canonical, ok := machine.estimateCanonicalSnapshotBytesLocked()
	if !ok {
		t.Fatal("maximally populated estimator fixture overflowed")
	}
	machine.estimatedSnapshotBytes = canonical
	assertCanonicalSnapshotEstimate(t, machine)
	delete(machine.workerEvents, cursorKey)
	machine.estimatedSnapshotBytes -= workerEventEntryEstimatedBytes
	assertCanonicalSnapshotEstimate(t, machine)
}
