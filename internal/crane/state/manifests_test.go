package state

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/aaditya/cs425mp3/internal/crane/model"
)

func TestTask10ConcreteCommandCanonicalGoldenBundle(t *testing.T) {
	machine, job, topology, assignment := task10RunningJob(t)
	worker := machine.workers[1]
	register, _ := NewRegisterWorker(InternalCommandID{0xd1}, 0, WorkerRecord{NodeID: worker.NodeID, Epoch: worker.Epoch, State: WorkerEligible, Revision: 1, Slots: worker.Slots, ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint()}, machine.coordinatorEpoch)
	drain, _ := NewDrainWorker(InternalCommandID{0xd2}, 1, worker.NodeID, worker.Epoch, machine.coordinatorEpoch)
	deactivate, _ := NewDeactivateWorker(InternalCommandID{0xd3}, 1, worker.NodeID, worker.Epoch, nil, machine.coordinatorEpoch)
	replacementRecord := WorkerRecord{NodeID: worker.NodeID, Epoch: model.WorkerEpoch{0xd4}, State: WorkerEligible, Revision: 2, Slots: worker.Slots, ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint()}
	replaceWorker, _ := NewReplaceWorkerEpoch(InternalCommandID{0xd4}, 1, worker.NodeID, worker.Epoch, replacementRecord, nil, machine.coordinatorEpoch)
	submit, _ := NewSubmitJob(model.ClientRequestID{ClientID: model.ClientID{0xd5}, Sequence: 1}, topology.Spec(), machine.coordinatorEpoch)
	cancel, _ := NewCancelJob(model.ClientRequestID{ClientID: model.ClientID{0xd5}, Sequence: 2}, submit.JobID(), 1, machine.coordinatorEpoch)
	var source model.AssignmentToken
	for _, token := range assignment.Tasks {
		if token.Task.StageID == 1 {
			source = token
			break
		}
	}
	eof := machine.jobs[job].SourceEOFs[source.Task].EOF
	recordEOF, _ := NewRecordSourceEOF(InternalCommandID{0xd6}, 0, source.Task, eof, machine.coordinatorEpoch)
	install, _ := NewInstallAssignments(InternalCommandID{0xd7}, 1, assignment, machine.coordinatorEpoch)
	targetTokens := append([]model.AssignmentToken(nil), assignment.Tasks...)
	for index := range targetTokens {
		targetTokens[index].AssignmentRevision++
	}
	target, err := model.NewAssignmentSet(job, assignment.Revision+1, targetTokens, assignment.ResultReplicas, topology)
	if err != nil {
		t.Fatal(err)
	}
	replaceAssignments, _ := NewReplaceAssignments(InternalCommandID{0xd8}, machine.jobs[job].JobControlRevision, job, assignment.Revision, assignment.Digest, NeedsReassignmentDigest(nil), target, machine.coordinatorEpoch)
	report := model.CompletionReport{JobID: job, JobControlRevision: machine.jobs[job].JobControlRevision, AssignmentRevision: assignment.Revision, Source: source.Task, Token: source, Epoch: machine.coordinatorEpoch, ExpectedCheckpointRevision: 0, Prior: 0, New: eof, EOF: eof, WorkerTransactionID: 1}
	report.Digest = model.CompletionReportDigest(report)
	advance, _ := NewAdvanceCheckpoint(InternalCommandID{0xd9}, 0, report, machine.coordinatorEpoch)
	replica := assignment.ResultReplicas[0]
	manifest := ResultManifest{JobID: job, SinkTask: replica.SinkTask, ManifestRevision: 1, SpecificationHash: topology.Digest(), RecordCount: 1, TotalBytes: model.ResultArtifactMinRecordBytesV1, Checksum: [32]byte{0xda}, Replicas: replica}
	seal, _ := NewSealManifest(InternalCommandID{0xda}, 0, manifest, machine.coordinatorEpoch)
	transition, _ := NewTransitionJob(InternalCommandID{0xdb}, machine.jobs[job].JobControlRevision, job, JobRunning, JobDraining, machine.coordinatorEpoch)
	failure := model.JobFailureReport{JobID: job, JobControlRevision: machine.jobs[job].JobControlRevision, AssignmentRevision: assignment.Revision, Task: source, Epoch: machine.coordinatorEpoch, TransactionID: 2, Code: model.FailureStorage, DetailDigest: [32]byte{0xdc}}
	fail, _ := NewFailJob(InternalCommandID{0xdc}, machine.jobs[job].JobControlRevision, failure, machine.coordinatorEpoch)

	commands := []any{register, drain, deactivate, replaceWorker, submit, cancel, recordEOF, install, replaceAssignments, advance, seal, transition, fail}
	bundle := make([]byte, 0)
	for _, command := range commands {
		encoded, err := MarshalCommand(command)
		if err != nil {
			t.Fatalf("MarshalCommand(%T): %v", command, err)
		}
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(encoded)))
		bundle = append(bundle, length[:]...)
		bundle = append(bundle, encoded...)
	}
	digest := sha256.Sum256(bundle)
	const want = "43939f04015aba45c2ee177f644a83a9a502492f45d566f01e0553c1891a86e6"
	if got := hex.EncodeToString(digest[:]); got != want {
		t.Fatalf("Task 10 canonical command bundle SHA-256 = %s, want %s", got, want)
	}
}

func TestProgressCommandsConcreteCanonicalRoundTrip(t *testing.T) {
	machine, job, topology, assignment := task10RunningJob(t)
	source := assignment.Tasks[0]
	for _, token := range assignment.Tasks {
		if token.Task.StageID == 1 {
			source = token
			break
		}
	}
	eof := machine.jobs[job].SourceEOFs[source.Task].EOF
	report := model.CompletionReport{JobID: job, JobControlRevision: machine.jobs[job].JobControlRevision, AssignmentRevision: assignment.Revision, Source: source.Task, Token: source, Epoch: machine.coordinatorEpoch, ExpectedCheckpointRevision: 0, Prior: 0, New: eof, EOF: eof, WorkerTransactionID: 1}
	report.Digest = model.CompletionReportDigest(report)
	advance, _ := NewAdvanceCheckpoint(InternalCommandID{0xa1}, 0, report, machine.coordinatorEpoch)
	replica := assignment.ResultReplicas[0]
	manifest := ResultManifest{JobID: job, SinkTask: replica.SinkTask, ManifestRevision: 1, SpecificationHash: topology.Digest(), RecordCount: 3, TotalBytes: 3 * model.ResultArtifactMinRecordBytesV1, Checksum: [32]byte{1}, Replicas: replica}
	seal, _ := NewSealManifest(InternalCommandID{0xa2}, 0, manifest, machine.coordinatorEpoch)
	transition, _ := NewTransitionJob(InternalCommandID{0xa3}, machine.jobs[job].JobControlRevision, job, JobRunning, JobDraining, machine.coordinatorEpoch)
	failure := model.JobFailureReport{JobID: job, JobControlRevision: machine.jobs[job].JobControlRevision, AssignmentRevision: assignment.Revision, Task: source, Epoch: machine.coordinatorEpoch, TransactionID: 2, Code: model.FailureOperator, DetailDigest: [32]byte{2}}
	fail, _ := NewFailJob(InternalCommandID{0xa4}, machine.jobs[job].JobControlRevision, failure, machine.coordinatorEpoch)
	for _, command := range []any{advance, seal, transition, fail} {
		encoded, err := MarshalCommand(command)
		if err != nil {
			t.Fatalf("MarshalCommand(%T): %v", command, err)
		}
		decoded, err := UnmarshalCommand(encoded)
		if err != nil || !reflect.DeepEqual(decoded, command) {
			t.Fatalf("round trip %T = %#v,%v want %#v", command, decoded, err, command)
		}
	}
}

func TestManifestAndSucceededRequireFinalCheckpointsAndTwoCurrentCopies(t *testing.T) {
	machine, job, topology, assignment := task10RunningJob(t)
	transaction := uint64(1)
	for _, token := range assignment.Tasks {
		if token.Task.StageID != 1 {
			continue
		}
		eof := machine.jobs[job].SourceEOFs[token.Task].EOF
		report := model.CompletionReport{JobID: job, JobControlRevision: machine.jobs[job].JobControlRevision, AssignmentRevision: assignment.Revision, Source: token.Task, Token: token, Epoch: machine.coordinatorEpoch, ExpectedCheckpointRevision: 0, Prior: 0, New: eof, EOF: eof, WorkerTransactionID: transaction}
		transaction++
		report.Digest = model.CompletionReportDigest(report)
		advance, _ := NewAdvanceCheckpoint(InternalCommandID{0xb0, byte(transaction)}, 0, report)
		if got := applyTask10(t, machine, 70+transaction, advance); got.Code != ResultSuccess {
			t.Fatalf("checkpoint = %#v", got)
		}
	}
	draining, _ := NewTransitionJob(InternalCommandID{0xb1}, machine.jobs[job].JobControlRevision, job, JobRunning, JobDraining)
	if got := applyTask10(t, machine, 80, draining); got.Code != ResultSuccess {
		t.Fatalf("draining = %#v", got)
	}
	replica := assignment.ResultReplicas[0]
	manifest := ResultManifest{JobID: job, SinkTask: replica.SinkTask, ManifestRevision: 1, SpecificationHash: topology.Digest(), RecordCount: 9, TotalBytes: 9 * model.ResultArtifactMinRecordBytesV1, Checksum: [32]byte{3}, Replicas: replica}
	seal, _ := NewSealManifest(InternalCommandID{0xb2}, 0, manifest)
	jobRevision := machine.jobs[job].JobControlRevision
	if got := applyTask10(t, machine, 81, seal); got.Code != ResultSuccess || got.Revision != 1 {
		t.Fatalf("seal manifest = %#v", got)
	}
	if machine.jobs[job].JobControlRevision != jobRevision {
		t.Fatal("manifest advanced JobControlRevision")
	}
	assertCanonicalSnapshotEstimate(t, machine)
	succeed, _ := NewTransitionJob(InternalCommandID{0xb3}, jobRevision, job, JobDraining, JobSucceeded)
	if got := applyTask10(t, machine, 82, succeed); got.Code != ResultSuccess || machine.jobs[job].Lifecycle != JobSucceeded {
		t.Fatalf("succeed = %#v lifecycle=%d", got, machine.jobs[job].Lifecycle)
	}
}

func TestFailureReportUsesGlobalCursorAndCurrentAssignmentFence(t *testing.T) {
	machine, job, _, assignment := task10RunningJob(t)
	token := assignment.Tasks[0]
	report := model.JobFailureReport{JobID: job, JobControlRevision: machine.jobs[job].JobControlRevision, AssignmentRevision: assignment.Revision, Task: token, Epoch: machine.coordinatorEpoch, TransactionID: 7, Code: model.FailureOperator, DetailDigest: [32]byte{7}}
	fail, _ := NewFailJob(InternalCommandID{0xc1}, machine.jobs[job].JobControlRevision, report)
	if got := applyTask10(t, machine, 90, fail); got.Code != ResultSuccess || machine.jobs[job].Lifecycle != JobFailed {
		t.Fatalf("fail job = %#v lifecycle=%d", got, machine.jobs[job].Lifecycle)
	}
	if machine.workerEvents[workerEventKey{WorkerID: token.WorkerID, WorkerEpoch: token.WorkerEpoch}].TransactionID != 7 {
		t.Fatal("failure did not advance global worker-event cursor")
	}
	assertCanonicalSnapshotEstimate(t, machine)
}

func TestLifecycleEveryNormalTransitionPairAndSpecialTerminalPath(t *testing.T) {
	allowed := map[[2]JobLifecycle]bool{{JobDeploying, JobRunning}: true, {JobRunning, JobDraining}: true, {JobDraining, JobSucceeded}: true}
	for from := JobPending; from <= JobCanceled; from++ {
		for to := JobPending; to <= JobCanceled; to++ {
			if from == to {
				continue
			}
			from, to := from, to
			t.Run(lifecycleName(from)+"-to-"+lifecycleName(to), func(t *testing.T) {
				machine, job, topology, assignment := task10RunningJob(t)
				record := cloneJobRecord(machine.jobs[job])
				record.Lifecycle = from
				for source, eof := range record.SourceEOFs {
					record.Checkpoints[source] = CheckpointRecord{Watermark: eof.EOF, Revision: 1}
				}
				for _, replica := range assignment.ResultReplicas {
					record.Manifests[replica.SinkTask] = ResultManifest{JobID: job, SinkTask: replica.SinkTask, ManifestRevision: 1, SpecificationHash: topology.Digest(), RecordCount: 1, TotalBytes: model.ResultArtifactMinRecordBytesV1, Checksum: [32]byte{1}, Replicas: replica}
				}
				machine.jobs[job] = record
				delete(machine.subjects, SubjectKey{Kind: SubjectJobControl, JobID: job})
				command, err := NewTransitionJob(InternalCommandID{byte(from), byte(to), 0xee}, record.JobControlRevision, job, from, to, machine.coordinatorEpoch)
				if err != nil {
					t.Fatal(err)
				}
				got := applyTask10(t, machine, 100+uint64(from)*10+uint64(to), command)
				if allowed[[2]JobLifecycle{from, to}] {
					if got.Code != ResultSuccess || machine.jobs[job].Lifecycle != to {
						t.Fatalf("legal transition result=%#v lifecycle=%d", got, machine.jobs[job].Lifecycle)
					}
				} else if got.Code != ResultInvalidTransition || machine.jobs[job].Lifecycle != from {
					t.Fatalf("illegal transition result=%#v lifecycle=%d", got, machine.jobs[job].Lifecycle)
				}
			})
		}
	}

	for lifecycle := JobPending; lifecycle <= JobCanceled; lifecycle++ {
		lifecycle := lifecycle
		t.Run("fail-from-"+lifecycleName(lifecycle), func(t *testing.T) {
			machine, job, _, assignment := task10RunningJob(t)
			record := cloneJobRecord(machine.jobs[job])
			record.Lifecycle = lifecycle
			machine.jobs[job] = record
			token := assignment.Tasks[0]
			report := model.JobFailureReport{JobID: job, JobControlRevision: record.JobControlRevision, AssignmentRevision: assignment.Revision, Task: token, Epoch: machine.coordinatorEpoch, TransactionID: 1, Code: model.FailureOperator, DetailDigest: [32]byte{1}}
			command, _ := NewFailJob(InternalCommandID{byte(lifecycle), 0xef}, record.JobControlRevision, report)
			got := applyTask10(t, machine, 200+uint64(lifecycle), command)
			legal := lifecycle == JobDeploying || lifecycle == JobRunning || lifecycle == JobDraining
			if legal && (got.Code != ResultSuccess || machine.jobs[job].Lifecycle != JobFailed) {
				t.Fatalf("legal FailJob result=%#v lifecycle=%d", got, machine.jobs[job].Lifecycle)
			}
			if !legal && (got.Code != ResultInvalidTarget || machine.jobs[job].Lifecycle != lifecycle) {
				t.Fatalf("illegal FailJob result=%#v lifecycle=%d", got, machine.jobs[job].Lifecycle)
			}
		})
	}
}

func TestManifestAggregateResultBytesBound(t *testing.T) {
	job := model.JobID{1}
	first := model.TaskID{JobID: job, StageID: 2, Partition: 0}
	second := model.TaskID{JobID: job, StageID: 2, Partition: 1}
	limit := model.LimitsV1().MaxResultRecordsBytesPerJob
	current := map[model.TaskID]ResultManifest{first: {SinkTask: first, TotalBytes: limit - 1}}
	if manifestTotalWithinLimit(current, ResultManifest{SinkTask: second, TotalBytes: 2}) {
		t.Fatal("aggregate result artifact bytes above the per-job bound were accepted")
	}
	if !manifestTotalWithinLimit(current, ResultManifest{SinkTask: second, TotalBytes: 1}) {
		t.Fatal("exact aggregate result artifact byte bound was rejected")
	}
}

func lifecycleName(lifecycle JobLifecycle) string {
	return []string{"zero", "pending", "deploying", "running", "draining", "succeeded", "failed", "canceled"}[int(lifecycle)]
}
