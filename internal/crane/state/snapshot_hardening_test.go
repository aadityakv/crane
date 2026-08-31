package state

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"testing"

	"github.com/aaditya/cs425mp3/internal/crane/model"
)

func TestSnapshotRejectsMissingMandatoryReverseSubjectHistories(t *testing.T) {
	tests := map[string]func(*Machine){
		"coordinator": func(machine *Machine) {
			delete(machine.subjects, SubjectKey{Kind: SubjectCoordinator})
		},
		"worker": func(machine *Machine) {
			for id := range machine.workers {
				delete(machine.subjects, SubjectKey{Kind: SubjectWorker, WorkerID: id})
				return
			}
		},
		"job-control": func(machine *Machine) {
			for job := range machine.jobs {
				delete(machine.subjects, SubjectKey{Kind: SubjectJobControl, JobID: job})
				return
			}
		},
		"source-eof": func(machine *Machine) {
			deleteFirstSubjectKind(machine, SubjectSourceEOF)
		},
		"source-checkpoint": func(machine *Machine) {
			deleteFirstSubjectKind(machine, SubjectSourceCheckpoint)
		},
		"result-manifest": func(machine *Machine) {
			deleteFirstSubjectKind(machine, SubjectResultManifest)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			machine := completeSnapshotMachine(t, false)
			mutate(machine)
			assertCaptureAndRawRestoreRejectSnapshot(t, machine)
		})
	}
}

func TestSnapshotAllowsJobControlHistoryToLagOnlyThroughReachableDrift(t *testing.T) {
	machine, job, _, assignment := task10AssignedJob(t, 2)
	install, _ := NewInstallAssignments(InternalCommandID{0xd1}, 1, assignment, machine.coordinatorEpoch)
	applyTask10(t, machine, 12, install)
	token := assignment.Tasks[0]
	worker := machine.workers[token.WorkerID]
	affected := []AffectedAssignment{{JobID: job, JobControlRevision: machine.jobs[job].JobControlRevision, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest}}
	deactivate, _ := NewDeactivateWorker(InternalCommandID{0xd2}, worker.Revision, worker.NodeID, worker.Epoch, affected, machine.coordinatorEpoch)
	applyTask10(t, machine, 13, deactivate)
	record := machine.jobs[job]
	history := machine.subjects[SubjectKey{Kind: SubjectJobControl, JobID: job}]
	if history.appliedRevision >= record.JobControlRevision {
		t.Fatalf("fixture does not exercise retained-history drift: applied=%d authoritative=%d", history.appliedRevision, record.JobControlRevision)
	}
	if _, err := machine.Capture(100, 9); err != nil {
		t.Fatalf("reachable job-control drift rejected: %v", err)
	}

	pending, pendingJob, _, _ := task10AssignedJob(t, 2)
	delete(pending.subjects, SubjectKey{Kind: SubjectJobControl, JobID: pendingJob})
	if _, err := pending.Capture(100, 9); err != nil {
		t.Fatalf("client-created Pending job incorrectly requires internal JobControl history: %v", err)
	}
}

func TestSnapshotRejectsMalformedRetainedCommandResults(t *testing.T) {
	t.Run("client", func(t *testing.T) {
		machine := completeSnapshotMachine(t, false)
		for id, history := range machine.clients {
			history.result = []byte{1}
			machine.clients[id] = history
			break
		}
		assertCaptureAndRawRestoreRejectSnapshot(t, machine)
	})

	for _, kind := range []SubjectKind{SubjectCoordinator, SubjectWorker, SubjectJobControl, SubjectSourceEOF, SubjectSourceCheckpoint, SubjectResultManifest} {
		kind := kind
		t.Run(subjectKindName(kind)+"-current", func(t *testing.T) {
			machine := completeSnapshotMachine(t, false)
			corruptFirstSubjectKind(machine, kind, func(history *subjectHistory) { history.result = []byte{1} })
			assertCaptureAndRawRestoreRejectSnapshot(t, machine)
		})
		t.Run(subjectKindName(kind)+"-applied", func(t *testing.T) {
			machine := completeSnapshotMachine(t, false)
			corruptFirstSubjectKind(machine, kind, func(history *subjectHistory) { history.appliedResult = []byte{1} })
			assertCaptureAndRawRestoreRejectSnapshot(t, machine)
		})
	}
}

func TestSnapshotRejectsCanonicalButMismatchedRetainedCommandResults(t *testing.T) {
	wrong := mustBusinessResult(ResultIdentityReuse, SubjectKey{}, 0, model.CoordinatorEpoch{})
	for _, kind := range []SubjectKind{SubjectCoordinator, SubjectWorker, SubjectJobControl, SubjectSourceEOF, SubjectSourceCheckpoint, SubjectResultManifest} {
		kind := kind
		t.Run(subjectKindName(kind)+"-current", func(t *testing.T) {
			machine := completeSnapshotMachine(t, false)
			corruptFirstSubjectKind(machine, kind, func(history *subjectHistory) { history.result = append([]byte(nil), wrong...) })
			assertCaptureAndRawRestoreRejectSnapshot(t, machine)
		})
		t.Run(subjectKindName(kind)+"-applied", func(t *testing.T) {
			machine := completeSnapshotMachine(t, false)
			corruptFirstSubjectKind(machine, kind, func(history *subjectHistory) { history.appliedResult = append([]byte(nil), wrong...) })
			assertCaptureAndRawRestoreRejectSnapshot(t, machine)
		})
	}
}

func TestSnapshotRejectsSourceEOFRevisionOtherThanOne(t *testing.T) {
	machine := completeSnapshotMachine(t, false)
	for jobID, record := range machine.jobs {
		for source, eof := range record.SourceEOFs {
			eof.Revision = 2
			record.SourceEOFs[source] = eof
			history := machine.subjects[SubjectKey{Kind: SubjectSourceEOF, JobID: jobID, TaskID: source}]
			history.revision, history.appliedRevision = 2, 2
			machine.subjects[SubjectKey{Kind: SubjectSourceEOF, JobID: jobID, TaskID: source}] = history
			assertCaptureAndRawRestoreRejectSnapshot(t, machine)
			return
		}
	}
	t.Fatal("fixture has no source EOF")
}

func TestSnapshotRejectsAppliedJobControlTargetThatConflictsAtAuthoritativeRevision(t *testing.T) {
	machine, jobID, topology, assignment := task10AssignedJob(t, 2)
	install, _ := NewInstallAssignments(InternalCommandID{0xe3}, 1, assignment, machine.coordinatorEpoch)
	applyTask10(t, machine, 12, install)

	tasks := append([]model.AssignmentToken(nil), assignment.Tasks...)
	tasks[0].Attempt++
	conflicting, err := model.NewAssignmentSet(jobID, assignment.Revision, tasks, assignment.ResultReplicas, topology)
	if err != nil {
		t.Fatal(err)
	}
	key := SubjectKey{Kind: SubjectJobControl, JobID: jobID}
	history := machine.subjects[key]
	target := installAssignmentsTarget(InstallAssignments{Assignment: conflicting})
	history.target = append([]byte(nil), target...)
	history.appliedTarget = append([]byte(nil), target...)
	machine.subjects[key] = history
	assertCaptureAndRawRestoreRejectSnapshot(t, machine)
}

func TestSnapshotEnforcesAggregateManifestArtifactBytes(t *testing.T) {
	machine, job, sinks := multiManifestSnapshotMachine(t)
	if _, err := machine.Capture(1000, 1000); err != nil {
		t.Fatalf("exact aggregate boundary rejected: %v", err)
	}

	record := cloneJobRecord(machine.jobs[job])
	sink := sinks[len(sinks)-1]
	manifest := record.Manifests[sink]
	manifest.TotalBytes++
	manifest.RecordCount = recordCountForArtifactBytes(manifest.TotalBytes)
	record.Manifests[sink] = manifest
	machine.jobs[job] = record
	key := SubjectKey{Kind: SubjectResultManifest, JobID: job, TaskID: sink}
	history := machine.subjects[key]
	target := sealManifestTarget(SealManifest{Manifest: manifest})
	history.target = append([]byte(nil), target...)
	history.appliedTarget = append([]byte(nil), target...)
	machine.subjects[key] = history
	assertCaptureAndRawRestoreRejectSnapshot(t, machine)
}

func TestSnapshotErrorsPreserveTopLevelAndNestedSentinels(t *testing.T) {
	tests := []struct {
		name   string
		err    func() error
		nested error
	}{
		{name: "schema", nested: ErrUnsupportedCommandSchema, err: func() error {
			return NewMachine().Restore(SnapshotSchemaVersion+1, nil)
		}},
		{name: "fingerprint", nested: ErrConsensusFingerprintMismatch, err: func() error {
			capture, _ := NewMachine().Capture(1, 1)
			encoded, _ := capture.MarshalBinary()
			encoded[6] ^= 1
			return NewMachine().Restore(SnapshotSchemaVersion, encoded)
		}},
		{name: "command-result", nested: ErrMalformedCommandResult, err: func() error {
			machine := completeSnapshotMachine(t, false)
			for id, history := range machine.clients {
				history.result = []byte{1}
				machine.clients[id] = history
				break
			}
			_, err := machine.Capture(1000, 1000)
			return err
		}},
		{name: "command-target", nested: ErrMalformedCommand, err: func() error {
			machine := completeSnapshotMachine(t, false)
			corruptFirstSubjectKind(machine, SubjectSourceCheckpoint, func(history *subjectHistory) { history.target = []byte{1} })
			_, err := machine.Capture(1000, 1000)
			return err
		}},
		{name: "topology", nested: ErrInvalidCommand, err: func() error {
			machine := completeSnapshotMachine(t, false)
			for id, job := range machine.jobs {
				job.TopologyBytes[0] ^= 1
				machine.jobs[id] = job
				history := machine.clients[job.DefiningRequest.ClientID]
				history.digest = model.PublicSubmitCommandDigest(job.DefiningRequest, job.TopologyBytes)
				machine.clients[job.DefiningRequest.ClientID] = history
				break
			}
			_, err := machine.Capture(1000, 1000)
			return err
		}},
		{name: "assignment", nested: ErrInvalidCommandSubject, err: func() error {
			machine := completeSnapshotMachine(t, false)
			for id, job := range machine.jobs {
				job.Assignment.Tasks[0].WorkerID = 0
				machine.jobs[id] = job
				break
			}
			_, err := machine.Capture(1000, 1000)
			return err
		}},
		{name: "job-control-target-topology", nested: ErrInvalidCommand, err: func() error {
			machine, jobID, _, assignment := task10AssignedJob(t, 2)
			install, _ := NewInstallAssignments(InternalCommandID{0xe1}, 1, assignment, machine.coordinatorEpoch)
			applyTask10(t, machine, 12, install)
			job := machine.jobs[jobID]
			job.TopologyBytes = []byte{1}
			machine.jobs[jobID] = job
			client := machine.clients[job.DefiningRequest.ClientID]
			client.digest = model.PublicSubmitCommandDigest(job.DefiningRequest, job.TopologyBytes)
			machine.clients[job.DefiningRequest.ClientID] = client
			_, err := machine.Capture(1000, 1000)
			return err
		}},
		{name: "job-control-target-assignment", nested: ErrInvalidCommandSubject, err: func() error {
			machine, jobID, _, assignment := task10AssignedJob(t, 2)
			install, _ := NewInstallAssignments(InternalCommandID{0xe2}, 1, assignment, machine.coordinatorEpoch)
			applyTask10(t, machine, 12, install)
			assignment.Tasks[0].WorkerID = 0
			invalidTarget := installAssignmentsTarget(InstallAssignments{Assignment: assignment})
			key := SubjectKey{Kind: SubjectJobControl, JobID: jobID}
			history := machine.subjects[key]
			history.target = append([]byte(nil), invalidTarget...)
			history.appliedTarget = append([]byte(nil), invalidTarget...)
			machine.subjects[key] = history
			_, err := machine.Capture(1000, 1000)
			return err
		}},
		{name: "cross-reference", nested: ErrSnapshotCrossReference, err: func() error {
			machine := completeSnapshotMachine(t, false)
			deleteFirstSubjectKind(machine, SubjectResultManifest)
			_, err := machine.Capture(1000, 1000)
			return err
		}},
		{name: "order", nested: ErrSnapshotOrder, err: func() error {
			machine, _, _, _ := task10AssignedJob(t, 2)
			capture, _ := machine.Capture(100, 100)
			encoded, _ := capture.MarshalBinary()
			first := appendWorkerEntry(nil, 1, machine.workers[1], nil)
			second := appendWorkerEntry(nil, 2, machine.workers[2], nil)
			offset := bytes.Index(encoded, first)
			copy(encoded[offset:offset+len(first)], second)
			copy(encoded[offset+len(first):offset+2*len(first)], first)
			return NewMachine().Restore(SnapshotSchemaVersion, encoded)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.err()
			if !errors.Is(err, ErrInvalidSnapshot) || !errors.Is(err, test.nested) {
				t.Fatalf("error=%v, want ErrInvalidSnapshot and %v", err, test.nested)
			}
		})
	}
}

func TestSnapshotCaptureRejectsIncrementalEstimatorDriftBeforeAndAfterCursorDeletion(t *testing.T) {
	machine := completeSnapshotMachine(t, false)
	if _, err := machine.Capture(1000, 1000); err != nil {
		t.Fatalf("valid pre-replacement capture: %v", err)
	}

	var cursorKey workerEventKey
	for key := range machine.workerEvents {
		cursorKey = key
		break
	}
	worker := machine.workers[cursorKey.WorkerID]
	target := worker
	target.Epoch[15]++
	target.Revision++
	replace, err := NewReplaceWorkerEpoch(InternalCommandID{0xf1}, worker.Revision, worker.NodeID, worker.Epoch, target, nil, machine.coordinatorEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if got := applyTask10(t, machine, 100, replace); got.Code != ResultSuccess {
		t.Fatalf("replace worker: %#v", got)
	}
	if _, retained := machine.workerEvents[cursorKey]; retained {
		t.Fatal("replacement did not delete the old cursor")
	}
	if _, err := machine.Capture(1000, 1000); err != nil {
		t.Fatalf("valid post-deletion capture: %v", err)
	}

	machine.estimatedSnapshotBytes++
	if _, err := machine.Capture(1000, 1000); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("positive estimator drift capture error=%v", err)
	}
	machine.estimatedSnapshotBytes -= 2
	if _, err := machine.Capture(1000, 1000); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("negative estimator drift capture error=%v", err)
	}
}

func TestReachableStateCapacityRejectsNextCanonicalMutationWithoutStateChange(t *testing.T) {
	machine := NewMachine()
	nextIndex := uint64(1)
	begin, _ := NewBeginCoordinatorEpoch(capacityCommandID(0, 0, 0), 0, 1, [16]byte{1})
	applyTask10(t, machine, nextIndex, begin)
	nextIndex++
	for node := uint16(1); node <= 128; node++ {
		record := WorkerRecord{NodeID: node, Epoch: model.WorkerEpoch{byte(node >> 8), byte(node)}, State: WorkerEligible, Revision: 1, Slots: uint16(model.LimitsV1().MaxWorkerSlots), ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint()}
		register, err := NewRegisterWorker(capacityCommandID(1, uint64(node), 0), 0, record, machine.coordinatorEpoch)
		if err != nil {
			t.Fatal(err)
		}
		if result := applyTask10(t, machine, nextIndex, register); result.Code != ResultSuccess {
			t.Fatalf("register worker %d: %#v", node, result)
		}
		nextIndex++
	}

	validated, err := model.ValidateTopology(task10MaximumTopology())
	if err != nil {
		t.Fatal(err)
	}
	var capacityCommand any
	var capacityResult []byte
	for jobNumber := uint64(1); jobNumber <= model.LimitsV1().MaxActiveJobs && capacityCommand == nil; jobNumber++ {
		var client model.ClientID
		binary.BigEndian.PutUint64(client[8:], jobNumber)
		request := model.ClientRequestID{ClientID: client, Sequence: 1}
		submit, err := NewSubmitJob(request, validated.Spec(), machine.coordinatorEpoch)
		if err != nil {
			t.Fatal(err)
		}
		if result, encoded := applyCapacityProofCommand(t, machine, nextIndex, submit); result.Code == ResultCapacityExhausted {
			capacityCommand, capacityResult = submit, encoded
			break
		}
		nextIndex++
		job := submit.JobID()
		for _, stage := range validated.Spec().Stages {
			if stage.Role != model.Source {
				continue
			}
			for partition := uint16(0); partition < stage.Parallelism; partition++ {
				source := model.TaskID{JobID: job, StageID: stage.StageID, Partition: partition}
				eof, err := model.SourceEOF(validated, source)
				if err != nil {
					t.Fatal(err)
				}
				record, err := NewRecordSourceEOF(capacityCommandID(2, jobNumber, uint64(partition)), 0, source, eof, machine.coordinatorEpoch)
				if err != nil {
					t.Fatal(err)
				}
				if result, encoded := applyCapacityProofCommand(t, machine, nextIndex, record); result.Code == ResultCapacityExhausted {
					capacityCommand, capacityResult = record, encoded
					break
				}
				nextIndex++
			}
			if capacityCommand != nil {
				break
			}
		}
		if capacityCommand != nil {
			break
		}
		assignment, err := model.BuildAssignmentSet(job, validated.Digest(), 1, validated, machine.residualEligiblePlacements(job))
		if err != nil {
			t.Fatal(err)
		}
		install, err := NewInstallAssignments(capacityCommandID(3, jobNumber, 0), 1, assignment, machine.coordinatorEpoch)
		if err != nil {
			t.Fatal(err)
		}
		if result, encoded := applyCapacityProofCommand(t, machine, nextIndex, install); result.Code == ResultCapacityExhausted {
			capacityCommand, capacityResult = install, encoded
			break
		}
		nextIndex++
	}
	if capacityCommand == nil {
		t.Fatal("reachable canonical commands did not encounter the snapshot capacity fence")
	}
	if machine.estimatedSnapshotBytes > model.StateCommandMaxSnapshotBytesV1 {
		t.Fatalf("admitted state estimate=%d exceeds bound", machine.estimatedSnapshotBytes)
	}
	if _, err := machine.Capture(nextIndex, nextIndex); err != nil {
		t.Fatalf("last admitted real state cannot be captured: %v", err)
	}

	beforeRetry := machine.View()
	retryResult, retryEncoded := applyCapacityProofCommand(t, machine, nextIndex+1, capacityCommand)
	if retryResult.Code != ResultCapacityExhausted || !reflect.DeepEqual(retryEncoded, capacityResult) {
		t.Fatalf("capacity retry result=%#v bytes=%x want=%x", retryResult, retryEncoded, capacityResult)
	}
	afterRetry := machine.View()
	afterRetry.AppliedIndex = beforeRetry.AppliedIndex
	if !reflect.DeepEqual(afterRetry, beforeRetry) {
		t.Fatal("capacity retry mutated replicated state beyond the required Apply index")
	}
}

func TestSnapshotCompatibilityPoliciesMechanicallyMatchProduction(t *testing.T) {
	contract := model.StateCommandContractV1()
	if !reflect.DeepEqual(contract.SnapshotSortRules, SnapshotSortRules()) {
		t.Fatalf("sort contract=%#v production=%#v", contract.SnapshotSortRules, SnapshotSortRules())
	}
	if !reflect.DeepEqual(contract.SnapshotMigrationRules, SnapshotMigrationRules()) {
		t.Fatalf("migration contract=%#v production=%#v", contract.SnapshotMigrationRules, SnapshotMigrationRules())
	}
	if !reflect.DeepEqual(contract.SnapshotValidationRules, SnapshotValidationRules()) {
		t.Fatalf("validation contract=%#v production=%#v", contract.SnapshotValidationRules, SnapshotValidationRules())
	}
	if !reflect.DeepEqual(contract.SnapshotEstimatorConstants, SnapshotEstimatorConstants()) {
		t.Fatalf("estimator contract=%#v production=%#v", contract.SnapshotEstimatorConstants, SnapshotEstimatorConstants())
	}
}

func TestSnapshotComparatorsFollowEveryFingerprintedSortRule(t *testing.T) {
	if compareSnapshotClientID(model.ClientID{0, 0xff}, model.ClientID{1}) >= 0 {
		t.Fatal("client comparator is not unsigned canonical byte order")
	}
	leftSubject := SubjectKey{Kind: SubjectJobControl, JobID: model.JobID{0, 0xff}}
	rightSubject := SubjectKey{Kind: SubjectJobControl, JobID: model.JobID{1}}
	if compareSnapshotSubject(leftSubject, rightSubject) >= 0 {
		t.Fatal("subject comparator is not canonical SubjectKey byte order")
	}
	if compareSnapshotWorkerID(255, 256) >= 0 {
		t.Fatal("worker comparator is not unsigned numeric order")
	}
	if compareSnapshotJobID(model.JobID{0, 0xff}, model.JobID{1}) >= 0 {
		t.Fatal("job comparator is not unsigned canonical byte order")
	}
	leftEvent := workerEventKey{WorkerID: 9, WorkerEpoch: model.WorkerEpoch{0, 0xff}}
	rightEvent := workerEventKey{WorkerID: 9, WorkerEpoch: model.WorkerEpoch{1}}
	if compareSnapshotWorkerEvent(leftEvent, rightEvent) >= 0 {
		t.Fatal("worker-event comparator is not WorkerID then epoch byte order")
	}
	job := model.JobID{1}
	leftTask := model.TaskID{JobID: job, StageID: 1, Partition: 255}
	rightTask := model.TaskID{JobID: job, StageID: 2, Partition: 0}
	if compareSnapshotTask(leftTask, rightTask) >= 0 {
		t.Fatal("task comparator is not canonical TaskID byte order")
	}
	leftMarker := NeedsReassignment{Kind: TaskTarget, Task: leftTask, OldWorkerID: 1, OldWorkerEpoch: model.WorkerEpoch{1}}
	rightMarker := NeedsReassignment{Kind: TaskTarget, Task: rightTask, OldWorkerID: 1, OldWorkerEpoch: model.WorkerEpoch{1}}
	if compareSnapshotMarker(leftMarker, rightMarker) >= 0 {
		t.Fatal("marker comparator is not the fingerprinted union order")
	}
}

func TestTask11ExportedSnapshotAPIHasDocumentation(t *testing.T) {
	want := map[string]bool{
		"ErrSnapshotTooLarge":       false,
		"ErrSnapshotOrder":          false,
		"ErrSnapshotCrossReference": false,
	}
	parsed, err := parser.ParseFile(token.NewFileSet(), "errors.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range parsed.Decls {
		generic, ok := declaration.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, raw := range generic.Specs {
			spec, ok := raw.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for _, name := range spec.Names {
				if _, required := want[name.Name]; required {
					want[name.Name] = spec.Doc != nil || generic.Doc != nil && len(generic.Specs) == 1
				}
			}
		}
	}
	for name, documented := range want {
		if !documented {
			t.Errorf("%s lacks a declaration comment", name)
		}
	}
}

func applyCapacityProofCommand(t *testing.T, machine *Machine, index uint64, command any) (CommandResult, []byte) {
	t.Helper()
	before := machine.View()
	encodedCommand, err := MarshalCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	encodedResult, err := machine.Apply(index, index, encodedCommand)
	if err != nil {
		t.Fatal(err)
	}
	result, err := UnmarshalCommandResult(encodedResult)
	if err != nil {
		t.Fatal(err)
	}
	machine.mu.Lock()
	canonical, fits := machine.estimateCanonicalSnapshotBytesLocked()
	incremental := machine.estimatedSnapshotBytes
	machine.mu.Unlock()
	if !fits || canonical != incremental || canonical > model.StateCommandMaxSnapshotBytesV1 {
		t.Fatalf("admitted state accounting canonical=%d incremental=%d fits=%t", canonical, incremental, fits)
	}
	if result.Code != ResultSuccess && result.Code != ResultCapacityExhausted {
		t.Fatalf("capacity proof command returned non-capacity rejection: %#v", result)
	}
	if result.Code == ResultCapacityExhausted {
		after := machine.View()
		after.AppliedIndex = before.AppliedIndex
		if !reflect.DeepEqual(after, before) {
			t.Fatal("capacity rejection mutated state beyond the required Apply index")
		}
	}
	return result, encodedResult
}

func capacityCommandID(kind byte, first, second uint64) InternalCommandID {
	var input [17]byte
	input[0] = kind
	binary.BigEndian.PutUint64(input[1:9], first)
	binary.BigEndian.PutUint64(input[9:17], second)
	return InternalCommandID(sha256.Sum256(input[:]))
}

func assertCaptureAndRawRestoreRejectSnapshot(t *testing.T, machine *Machine) {
	t.Helper()
	if _, err := machine.Capture(1000, 1000); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("Capture error=%v, want ErrInvalidSnapshot", err)
	}
	machine.mu.Lock()
	encoded := machine.appendCanonicalSnapshotLocked(nil, nil)
	machine.mu.Unlock()
	if err := NewMachine().Restore(SnapshotSchemaVersion, encoded); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("Restore error=%v, want ErrInvalidSnapshot", err)
	}
}

func deleteFirstSubjectKind(machine *Machine, kind SubjectKind) {
	for key := range machine.subjects {
		if key.Kind == kind {
			delete(machine.subjects, key)
			return
		}
	}
}

func corruptFirstSubjectKind(machine *Machine, kind SubjectKind, mutate func(*subjectHistory)) {
	for key, history := range machine.subjects {
		if key.Kind == kind {
			mutate(&history)
			machine.subjects[key] = history
			return
		}
	}
}

func subjectKindName(kind SubjectKind) string {
	return map[SubjectKind]string{
		SubjectCoordinator: "coordinator", SubjectWorker: "worker", SubjectJobControl: "job-control",
		SubjectSourceEOF: "source-eof", SubjectSourceCheckpoint: "source-checkpoint", SubjectResultManifest: "result-manifest",
	}[kind]
}

func multiManifestSnapshotMachine(t *testing.T) (*Machine, model.JobID, []model.TaskID) {
	t.Helper()
	machine := NewMachine()
	begin, _ := NewBeginCoordinatorEpoch(InternalCommandID{0xe0}, 0, 1, [16]byte{0xe0})
	applyTask10(t, machine, 1, begin)
	for node := uint16(1); node <= 3; node++ {
		record := WorkerRecord{NodeID: node, Epoch: model.WorkerEpoch{byte(node)}, State: WorkerEligible, Revision: 1, Slots: 16, ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint()}
		register, _ := NewRegisterWorker(InternalCommandID{byte(node), 0xe1}, 0, record, machine.coordinatorEpoch)
		applyTask10(t, machine, uint64(node+1), register)
	}
	spec := task10Topology(0)
	spec.Stages[1].Parallelism = 2
	topology, err := model.ValidateTopology(spec)
	if err != nil {
		t.Fatal(err)
	}
	request := model.ClientRequestID{ClientID: model.ClientID{0xe2}, Sequence: 1}
	submit, _ := NewSubmitJob(request, topology.Spec(), machine.coordinatorEpoch)
	applyTask10(t, machine, 10, submit)
	job := submit.JobID()
	source := model.TaskID{JobID: job, StageID: 1}
	eof, _ := model.SourceEOF(topology, source)
	recordEOF, _ := NewRecordSourceEOF(InternalCommandID{0xe3}, 0, source, eof, machine.coordinatorEpoch)
	applyTask10(t, machine, 11, recordEOF)
	assignment, err := model.BuildAssignmentSet(job, topology.Digest(), 1, topology, task10EligiblePlacements(machine))
	if err != nil {
		t.Fatal(err)
	}
	install, _ := NewInstallAssignments(InternalCommandID{0xe4}, 1, assignment, machine.coordinatorEpoch)
	applyTask10(t, machine, 12, install)
	running, _ := NewTransitionJob(InternalCommandID{0xe5}, 2, job, JobDeploying, JobRunning, machine.coordinatorEpoch)
	applyTask10(t, machine, 13, running)
	token, _ := assignmentToken(machine.jobs[job].Assignment, source)
	report := model.CompletionReport{JobID: job, JobControlRevision: 3, AssignmentRevision: 1, Source: source, Token: token, Epoch: machine.coordinatorEpoch, New: eof, EOF: eof, WorkerTransactionID: 1}
	report.Digest = model.CompletionReportDigest(report)
	advance, _ := NewAdvanceCheckpoint(InternalCommandID{0xe6}, 0, report, machine.coordinatorEpoch)
	applyTask10(t, machine, 14, advance)
	draining, _ := NewTransitionJob(InternalCommandID{0xe7}, 3, job, JobRunning, JobDraining, machine.coordinatorEpoch)
	applyTask10(t, machine, 15, draining)

	replicas := append([]model.ResultReplicaSet(nil), assignment.ResultReplicas...)
	limit := model.LimitsV1().MaxResultRecordsBytesPerJob
	parts := []uint64{limit / 2, limit - limit/2}
	sinks := make([]model.TaskID, len(replicas))
	for index, replica := range replicas {
		manifest := ResultManifest{JobID: job, SinkTask: replica.SinkTask, ManifestRevision: 1, SpecificationHash: topology.Digest(), RecordCount: recordCountForArtifactBytes(parts[index]), TotalBytes: parts[index], Checksum: [32]byte{byte(index + 1)}, Replicas: replica}
		seal, err := NewSealManifest(InternalCommandID{byte(index + 1), 0xe8}, 0, manifest, machine.coordinatorEpoch)
		if err != nil {
			t.Fatal(err)
		}
		if got := applyTask10(t, machine, uint64(16+index), seal); got.Code != ResultSuccess {
			t.Fatalf("seal manifest %d: %#v", index, got)
		}
		sinks[index] = replica.SinkTask
	}
	return machine, job, sinks
}

func recordCountForArtifactBytes(total uint64) uint64 {
	if total == 0 {
		return 0
	}
	return (total + model.ResultArtifactMaxRecordBytesV1 - 1) / model.ResultArtifactMaxRecordBytesV1
}
