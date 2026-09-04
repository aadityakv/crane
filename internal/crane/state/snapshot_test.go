package state

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/raft"
)

func TestMachineImplementsRaftStateMachine(t *testing.T) {
	var _ raft.StateMachine = (*Machine)(nil)
}

func TestSnapshotContractMechanicallyMatchesCodecAndEstimator(t *testing.T) {
	contract := model.StateCommandContractV1()
	if contract.SnapshotSchemaVersion != SnapshotSchemaVersion {
		t.Fatalf("contract snapshot schema=%d codec=%d", contract.SnapshotSchemaVersion, SnapshotSchemaVersion)
	}
	if !reflect.DeepEqual(contract.SnapshotLayouts, SnapshotEncodingLayouts()) {
		t.Fatalf("contract snapshot layouts=%#v\ncodec layouts=%#v", contract.SnapshotLayouts, SnapshotEncodingLayouts())
	}
	if contract.SnapshotBaseBytes != model.StateCommandSnapshotBaseBytesV1 || contract.MaxSnapshotBytes != model.StateCommandMaxSnapshotBytesV1 {
		t.Fatalf("snapshot estimator contract=%#v", contract)
	}
}

func TestSnapshotBootstrapMigrationAndCurrentSchema(t *testing.T) {
	for _, schema := range []uint32{0, 1} {
		machine := completeSnapshotMachine(t, false)
		if err := machine.Restore(schema, nil); err != nil {
			t.Fatalf("Restore(%d,nil): %v", schema, err)
		}
		if got := machine.View(); !reflect.DeepEqual(got, NewMachine().View()) {
			t.Fatalf("legacy bootstrap schema %d did not produce empty state: %#v", schema, got)
		}
		if err := machine.Restore(schema, []byte{1}); err == nil {
			t.Fatalf("Restore(%d,nonempty) succeeded", schema)
		}
	}
	for _, schema := range []uint32{3, 9, ^uint32(0)} {
		if err := NewMachine().Restore(schema, nil); err == nil {
			t.Fatalf("Restore(%d,nil) succeeded", schema)
		}
	}

	capture, err := NewMachine().Capture(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if capture.SchemaVersion() != SnapshotSchemaVersion {
		t.Fatalf("capture schema=%d want=%d", capture.SchemaVersion(), SnapshotSchemaVersion)
	}
	encoded, err := capture.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) != int(model.StateCommandSnapshotBaseBytesV1) {
		t.Fatalf("empty payload bytes=%d want=%d", len(encoded), model.StateCommandSnapshotBaseBytesV1)
	}
	if string(encoded[:4]) != "CRSN" || binary.BigEndian.Uint16(encoded[4:6]) != uint16(SnapshotSchemaVersion) {
		t.Fatalf("snapshot prefix=%x", encoded[:6])
	}
	fingerprint := model.ConsensusFingerprint()
	if !bytes.Equal(encoded[6:38], fingerprint[:]) {
		t.Fatalf("snapshot fingerprint=%x want=%x", encoded[6:38], fingerprint)
	}
}

func TestEmptySnapshotCanonicalGolden(t *testing.T) {
	capture, err := NewMachine().Capture(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := capture.MarshalBinary()
	const wantPrefix = "4352534e00027732f1f226ca753e085cb5f963f79c7de666dfc7882a733353f08522aca1028b"
	wantHex := wantPrefix + strings.Repeat("00", 90)
	if got := hex.EncodeToString(encoded); got != wantHex {
		t.Fatalf("empty snapshot golden=%s", got)
	}
}

func TestSnapshotRoundTripIsCompleteDeterministicOwnedAndEstimatorExact(t *testing.T) {
	left := completeSnapshotMachine(t, false)
	right := completeSnapshotMachine(t, true)

	leftCapture, err := left.Capture(40, 3)
	if err != nil {
		t.Fatal(err)
	}
	rightCapture, err := right.Capture(40, 3)
	if err != nil {
		t.Fatal(err)
	}
	leftBytes, err := leftCapture.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	rightBytes, err := rightCapture.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(leftBytes, rightBytes) {
		t.Fatalf("insertion order changed snapshot\nleft=%x\nright=%x", leftBytes, rightBytes)
	}
	left.mu.Lock()
	wantEstimate, ok := left.estimateCanonicalSnapshotBytesLocked()
	left.mu.Unlock()
	if !ok || uint64(len(leftBytes)) != wantEstimate {
		t.Fatalf("encoded=%d canonical estimate=%d fits=%t", len(leftBytes), wantEstimate, ok)
	}

	wantView := left.View()
	restored := NewMachine()
	if err := restored.Restore(SnapshotSchemaVersion, leftBytes); err != nil {
		t.Fatal(err)
	}
	if got := restored.View(); !reflect.DeepEqual(got, wantView) {
		t.Fatalf("restored view mismatch\n got=%#v\nwant=%#v", got, wantView)
	}

	// The capture and restore own their bytes independently of callers and live state.
	leftBytes[0] ^= 0xff
	left.clients[model.ClientID{0xdb}] = clientHistory{sequence: 999, digest: [32]byte{9}, result: []byte("mutated")}
	again, err := leftCapture.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if string(again[:4]) != "CRSN" || !reflect.DeepEqual(restored.View(), wantView) {
		t.Fatal("captured/restored state aliased caller or later live mutation")
	}
}

func TestSnapshotRoundTripOwnsFailureOptional(t *testing.T) {
	machine := failedSnapshotMachine(t)
	capture, err := machine.Capture(50, 4)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := capture.MarshalBinary()
	restored := NewMachine()
	if err := restored.Restore(SnapshotSchemaVersion, encoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(restored.View(), machine.View()) {
		t.Fatal("failure snapshot did not round-trip exactly")
	}
	view := restored.View()
	view.Jobs[0].Failure.DetailDigest[0] ^= 1
	if reflect.DeepEqual(view, restored.View()) {
		t.Fatal("failure view aliases restored state")
	}
}

func TestSnapshotRestoreRejectsMalformedNoncanonicalAndOversizedAtomically(t *testing.T) {
	source := completeSnapshotMachine(t, false)
	capture, err := source.Capture(40, 3)
	if err != nil {
		t.Fatal(err)
	}
	good, err := capture.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	target := completeSnapshotMachine(t, true)
	want := target.View()

	cases := map[string][]byte{
		"nil-current": nil,
		"truncated":   append([]byte(nil), good[:len(good)-1]...),
		"trailing":    append(append([]byte(nil), good...), 0),
		"magic":       mutateSnapshotByte(good, 0),
		"version":     mutateSnapshotByte(good, 5),
		"fingerprint": mutateSnapshotByte(good, 6),
		"reserved-applied-index": func() []byte {
			copy := append([]byte(nil), good...)
			binary.BigEndian.PutUint64(copy[38:46], 0)
			return copy
		}(),
		"declared-client-count": func() []byte {
			copy := append([]byte(nil), good...)
			binary.BigEndian.PutUint64(copy[88:96], model.StateCommandMaxClientSessionsV1+1)
			return copy
		}(),
		"oversized": make([]byte, model.StateCommandMaxSnapshotBytesV1+1),
	}
	for name, encoded := range cases {
		if err := target.Restore(SnapshotSchemaVersion, encoded); err == nil {
			t.Errorf("%s restore succeeded", name)
		}
		if got := target.View(); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s failed restore mutated state\n got=%#v\nwant=%#v", name, got, want)
		}
	}
}

func TestSnapshotCaptureAndRestoreRejectEveryStateCrossReference(t *testing.T) {
	mutations := map[string]func(*Machine){
		"job-map-key": func(machine *Machine) {
			for key, record := range machine.jobs {
				delete(machine.jobs, key)
				key[15]++
				machine.jobs[key] = record
				return
			}
		},
		"topology-digest": func(machine *Machine) {
			for key, record := range machine.jobs {
				record.TopologyDigest[0] ^= 1
				machine.jobs[key] = record
			}
		},
		"source-eof": func(machine *Machine) {
			for key, record := range machine.jobs {
				for source, eof := range record.SourceEOFs {
					eof.EOF++
					record.SourceEOFs[source] = eof
					break
				}
				machine.jobs[key] = record
			}
		},
		"checkpoint": func(machine *Machine) {
			for key, record := range machine.jobs {
				for source, checkpoint := range record.Checkpoints {
					checkpoint.Watermark = record.SourceEOFs[source].EOF + 1
					record.Checkpoints[source] = checkpoint
					break
				}
				machine.jobs[key] = record
			}
		},
		"manifest": func(machine *Machine) {
			for key, record := range machine.jobs {
				for sink, manifest := range record.Manifests {
					manifest.SpecificationHash[0] ^= 1
					record.Manifests[sink] = manifest
					break
				}
				machine.jobs[key] = record
			}
		},
		"subject-revision": func(machine *Machine) {
			for key, history := range machine.subjects {
				if key.Kind == SubjectSourceCheckpoint {
					history.revision++
					machine.subjects[key] = history
					return
				}
			}
		},
		"worker-event-epoch": func(machine *Machine) {
			for key, cursor := range machine.workerEvents {
				delete(machine.workerEvents, key)
				key.WorkerEpoch[15]++
				machine.workerEvents[key] = cursor
				return
			}
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			machine := completeSnapshotMachine(t, false)
			mutate(machine)
			if _, err := machine.Capture(100, 9); !errors.Is(err, ErrInvalidSnapshot) {
				t.Fatalf("Capture error=%v want ErrInvalidSnapshot", err)
			}
			machine.mu.Lock()
			encoded := machine.appendCanonicalSnapshotLocked(nil, nil)
			machine.mu.Unlock()
			if err := NewMachine().Restore(SnapshotSchemaVersion, encoded); !errors.Is(err, ErrInvalidSnapshot) {
				t.Fatalf("Restore error=%v want ErrInvalidSnapshot", err)
			}
		})
	}
}

func TestSnapshotRestoreRejectsUnknownEnumsAndUnsortedKeys(t *testing.T) {
	machine, _, _, _ := task10AssignedJob(t, 2)
	capture, err := machine.Capture(100, 100)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := capture.MarshalBinary()
	firstEntry := appendWorkerEntry(nil, 1, machine.workers[1], nil)
	secondEntry := appendWorkerEntry(nil, 2, machine.workers[2], nil)
	firstOffset := bytes.Index(encoded, firstEntry)
	secondOffset := bytes.Index(encoded, secondEntry)
	if firstOffset < 0 || secondOffset != firstOffset+len(firstEntry) {
		t.Fatalf("worker entries are not contiguous in canonical payload: first=%d second=%d", firstOffset, secondOffset)
	}
	unknownState := append([]byte(nil), encoded...)
	unknownState[firstOffset+20] = 0xff // map key + record NodeID + Epoch.
	if err := NewMachine().Restore(SnapshotSchemaVersion, unknownState); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("unknown worker state error=%v", err)
	}
	unsorted := append([]byte(nil), encoded...)
	first := append([]byte(nil), unsorted[firstOffset:secondOffset]...)
	second := append([]byte(nil), unsorted[secondOffset:secondOffset+len(secondEntry)]...)
	copy(unsorted[firstOffset:secondOffset], second)
	copy(unsorted[secondOffset:secondOffset+len(secondEntry)], first)
	if err := NewMachine().Restore(SnapshotSchemaVersion, unsorted); !errors.Is(err, ErrInvalidSnapshot) || !errors.Is(err, ErrSnapshotOrder) {
		t.Fatalf("unsorted worker keys error=%v", err)
	}
	duplicate := append([]byte(nil), encoded...)
	copy(duplicate[secondOffset:secondOffset+len(firstEntry)], firstEntry)
	if err := NewMachine().Restore(SnapshotSchemaVersion, duplicate); !errors.Is(err, ErrInvalidSnapshot) || !errors.Is(err, ErrSnapshotOrder) {
		t.Fatalf("duplicate worker keys error=%v", err)
	}
}

func TestSnapshotRawEightMiBDecoderBoundaryAndPlusOne(t *testing.T) {
	capture, err := NewMachine().Capture(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	base, _ := capture.MarshalBinary()
	exact := make([]byte, model.StateCommandMaxSnapshotBytesV1)
	copy(exact, base)
	if err := NewMachine().Restore(SnapshotSchemaVersion, exact); !errors.Is(err, ErrInvalidSnapshot) || errors.Is(err, ErrSnapshotTooLarge) {
		t.Fatalf("exact-limit decode error=%v, want non-size ErrInvalidSnapshot", err)
	}
	plusOne := append(exact, 0)
	if err := NewMachine().Restore(SnapshotSchemaVersion, plusOne); !errors.Is(err, ErrInvalidSnapshot) || !errors.Is(err, ErrSnapshotTooLarge) {
		t.Fatalf("plus-one decode error=%v, want ErrInvalidSnapshot and ErrSnapshotTooLarge", err)
	}
}

func FuzzRestoreSnapshot(f *testing.F) {
	capture, err := NewMachine().Capture(1, 1)
	if err != nil {
		f.Fatal(err)
	}
	seed, err := capture.MarshalBinary()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Fuzz(func(t *testing.T, encoded []byte) {
		if uint64(len(encoded)) > model.StateCommandMaxSnapshotBytesV1+1 {
			t.Skip()
		}
		machine := NewMachine()
		_ = machine.Restore(SnapshotSchemaVersion, encoded)
	})
}

func mutateSnapshotByte(input []byte, offset int) []byte {
	result := append([]byte(nil), input...)
	result[offset] ^= 0xff
	return result
}

func completeSnapshotMachine(t *testing.T, reverse bool) *Machine {
	t.Helper()
	machine, job, _, assignment := task10AssignedJob(t, 2)
	install, _ := NewInstallAssignments(InternalCommandID{0x81}, 1, assignment, machine.coordinatorEpoch)
	applyTask10(t, machine, 12, install)
	running, _ := NewTransitionJob(InternalCommandID{0x82}, 2, job, JobDeploying, JobRunning, machine.coordinatorEpoch)
	applyTask10(t, machine, 13, running)
	record := machine.jobs[job]
	var source model.TaskID
	for candidate := range record.SourceEOFs {
		source = candidate
		break
	}
	token, ok := assignmentToken(record.Assignment, source)
	if !ok {
		t.Fatal("complete snapshot source token missing")
	}
	eof := record.SourceEOFs[source].EOF
	report := model.CompletionReport{
		JobID: job, JobControlRevision: record.JobControlRevision, AssignmentRevision: record.Assignment.Revision,
		Source: source, Token: token, Epoch: machine.coordinatorEpoch, New: eof, EOF: eof, WorkerTransactionID: 1,
	}
	report.Digest = model.CompletionReportDigest(report)
	advance, _ := NewAdvanceCheckpoint(InternalCommandID{0x83}, 0, report, machine.coordinatorEpoch)
	applyTask10(t, machine, 14, advance)
	draining, _ := NewTransitionJob(InternalCommandID{0x84}, 3, job, JobRunning, JobDraining, machine.coordinatorEpoch)
	applyTask10(t, machine, 15, draining)
	replica := record.Assignment.ResultReplicas[0]
	manifest := ResultManifest{JobID: job, SinkTask: replica.SinkTask, ManifestRevision: 1, SpecificationHash: record.TopologyDigest, Checksum: [32]byte{0x85}, Replicas: replica}
	seal, _ := NewSealManifest(InternalCommandID{0x85}, 0, manifest, machine.coordinatorEpoch)
	applyTask10(t, machine, 16, seal)
	succeeded, _ := NewTransitionJob(InternalCommandID{0x86}, 4, job, JobDraining, JobSucceeded, machine.coordinatorEpoch)
	applyTask10(t, machine, 17, succeeded)
	if reverse {
		reverseMachineMapInsertion(machine)
	}
	return machine
}

func failedSnapshotMachine(t *testing.T) *Machine {
	t.Helper()
	machine, job, _, assignment := task10AssignedJob(t, 2)
	install, _ := NewInstallAssignments(InternalCommandID{0x91}, 1, assignment, machine.coordinatorEpoch)
	applyTask10(t, machine, 12, install)
	running, _ := NewTransitionJob(InternalCommandID{0x92}, 2, job, JobDeploying, JobRunning, machine.coordinatorEpoch)
	applyTask10(t, machine, 13, running)
	record := machine.jobs[job]
	token := record.Assignment.Tasks[0]
	report := model.JobFailureReport{JobID: job, JobControlRevision: record.JobControlRevision, AssignmentRevision: record.Assignment.Revision, Task: token, Epoch: machine.coordinatorEpoch, TransactionID: 1, Code: model.FailureOperator, DetailDigest: [32]byte{0x93}}
	fail, _ := NewFailJob(InternalCommandID{0x93}, record.JobControlRevision, report, machine.coordinatorEpoch)
	applyTask10(t, machine, 14, fail)
	return machine
}

func reverseMachineMapInsertion(machine *Machine) {
	clients := make(map[model.ClientID]clientHistory, len(machine.clients))
	for key, value := range machine.clients {
		clients[key] = value
	}
	subjects := make(map[SubjectKey]subjectHistory, len(machine.subjects))
	for key, value := range machine.subjects {
		subjects[key] = value
	}
	workers := make(map[uint16]WorkerRecord, len(machine.workers))
	for id := uint16(len(machine.workers)); id > 0; id-- {
		workers[id] = machine.workers[id]
	}
	jobs := make(map[model.JobID]JobRecord, len(machine.jobs))
	for key, value := range machine.jobs {
		clone := cloneJobRecord(value)
		clone.SourceEOFs = reverseEOFMap(clone.SourceEOFs)
		clone.Checkpoints = reverseCheckpointMap(clone.Checkpoints)
		clone.Manifests = reverseManifestMap(clone.Manifests)
		jobs[key] = clone
	}
	events := make(map[workerEventKey]workerEventCursor, len(machine.workerEvents))
	for key, value := range machine.workerEvents {
		events[key] = value
	}
	machine.clients, machine.subjects, machine.workers, machine.jobs, machine.workerEvents = clients, subjects, workers, jobs, events
}

func reverseEOFMap(input map[model.TaskID]SourceEOFRecord) map[model.TaskID]SourceEOFRecord {
	keys := sortedTaskKeysEOF(input)
	result := make(map[model.TaskID]SourceEOFRecord, len(input))
	for index := len(keys) - 1; index >= 0; index-- {
		result[keys[index]] = input[keys[index]]
	}
	return result
}

func reverseCheckpointMap(input map[model.TaskID]CheckpointRecord) map[model.TaskID]CheckpointRecord {
	keys := sortedTaskKeysCheckpoint(input)
	result := make(map[model.TaskID]CheckpointRecord, len(input))
	for index := len(keys) - 1; index >= 0; index-- {
		result[keys[index]] = input[keys[index]]
	}
	return result
}

func reverseManifestMap(input map[model.TaskID]ResultManifest) map[model.TaskID]ResultManifest {
	keys := sortedTaskKeysManifest(input)
	result := make(map[model.TaskID]ResultManifest, len(input))
	for index := len(keys) - 1; index >= 0; index-- {
		result[keys[index]] = input[keys[index]]
	}
	return result
}
