package state

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/aadityakv/crane/internal/crane/model"
)

func TestWorkerCommandsCanonicalRoundTripAndOwnedCollections(t *testing.T) {
	epoch1 := model.WorkerEpoch{1}
	epoch2 := model.WorkerEpoch{2}
	fence := model.CoordinatorEpoch{Term: 1, BeginIndex: 1, Coordinator: 1, Nonce: [16]byte{1}}
	register, err := NewRegisterWorker(InternalCommandID{1}, 0, WorkerRecord{
		NodeID: 7, Epoch: epoch1, State: WorkerEligible, Revision: 1, Slots: 4,
		ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint(),
	}, fence)
	if err != nil {
		t.Fatal(err)
	}
	drain, err := NewDrainWorker(InternalCommandID{2}, 1, 7, epoch1, fence)
	if err != nil {
		t.Fatal(err)
	}
	deactivate, err := NewDeactivateWorker(InternalCommandID{3}, 2, 7, epoch1, nil, fence)
	if err != nil {
		t.Fatal(err)
	}
	replace, err := NewReplaceWorkerEpoch(InternalCommandID{4}, 3, 7, epoch1, WorkerRecord{
		NodeID: 7, Epoch: epoch2, State: WorkerEligible, Revision: 4, Slots: 8,
		ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint(),
	}, nil, fence)
	if err != nil {
		t.Fatal(err)
	}

	for _, command := range []any{register, drain, deactivate, replace} {
		encoded, err := MarshalCommand(command)
		if err != nil {
			t.Fatalf("MarshalCommand(%T): %v", command, err)
		}
		decoded, err := UnmarshalCommand(encoded)
		if err != nil {
			t.Fatalf("UnmarshalCommand(%T): %v", command, err)
		}
		if !reflect.DeepEqual(decoded, command) {
			t.Fatalf("round trip %T = %#v, want %#v", command, decoded, command)
		}
		encoded[0] ^= 0xff
		again, err := MarshalCommand(command)
		if err != nil || bytes.Equal(encoded, again) {
			t.Fatalf("%T returned aliased bytes or failed: %v", command, err)
		}
		for length := 0; length < len(again); length++ {
			if _, err := UnmarshalCommand(again[:length]); err == nil {
				t.Fatalf("%T accepted truncation %d", command, length)
			}
		}
		if _, err := UnmarshalCommand(append(again, 0)); err == nil {
			t.Fatalf("%T accepted trailing byte", command)
		}
	}
}

func TestWorkerLifecycleOfflineReregistrationAndDrainingFence(t *testing.T) {
	machine := NewMachine()
	epoch := model.WorkerEpoch{0x41}
	record := WorkerRecord{NodeID: 7, Epoch: epoch, State: WorkerEligible, Revision: 1, Slots: 4, ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint()}
	register, _ := NewRegisterWorker(InternalCommandID{1}, 0, record)
	if got := applyTask10(t, machine, 1, register); got.Code != ResultSuccess || got.Subject != SubjectWorker || got.WorkerID != 7 || got.Revision != 1 {
		t.Fatalf("register = %#v", got)
	}

	deactivate, _ := NewDeactivateWorker(InternalCommandID{2}, 1, 7, epoch, nil)
	if got := applyTask10(t, machine, 2, deactivate); got.Code != ResultSuccess || got.Revision != 2 {
		t.Fatalf("deactivate = %#v", got)
	}
	if got := machine.workers[7]; got.State != WorkerOffline || got.Epoch != epoch || got.Revision != 2 {
		t.Fatalf("offline record = %#v", got)
	}

	record.Revision = 3
	reregister, _ := NewRegisterWorker(InternalCommandID{3}, 2, record)
	if got := applyTask10(t, machine, 3, reregister); got.Code != ResultSuccess || got.Revision != 3 {
		t.Fatalf("same-epoch re-register = %#v", got)
	}

	drain, _ := NewDrainWorker(InternalCommandID{4}, 3, 7, epoch)
	if got := applyTask10(t, machine, 4, drain); got.Code != ResultSuccess || got.Revision != 4 {
		t.Fatalf("drain = %#v", got)
	}
	record.Revision = 5
	fromDraining, _ := NewRegisterWorker(InternalCommandID{5}, 4, record)
	if got := applyTask10(t, machine, 5, fromDraining); got.Code != ResultInvalidTransition || got.Revision != 4 {
		t.Fatalf("registration revived Draining worker: %#v", got)
	}
	if got := machine.workers[7]; got.State != WorkerDraining || got.Revision != 4 {
		t.Fatalf("rejected registration mutated worker: %#v", got)
	}
}

func TestWorkerCapacityIsExactAndDoesNotEvict(t *testing.T) {
	machine := NewMachine()
	begin, _ := NewBeginCoordinatorEpoch(InternalCommandID{0xfa}, 0, 1, [16]byte{0xfa})
	applyTask10(t, machine, 1, begin)
	limit := int(model.LimitsV1().MaxRegisteredWorkers)
	for index := 1; index <= limit; index++ {
		nodeID := uint16(index)
		command, err := NewRegisterWorker(InternalCommandID{byte(index), byte(index >> 8), 1}, 0, WorkerRecord{
			NodeID: nodeID, Epoch: model.WorkerEpoch{byte(index), byte(index >> 8), 1}, State: WorkerEligible, Revision: 1, Slots: 1,
			ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint(),
		}, machine.coordinatorEpoch)
		if err != nil {
			t.Fatalf("constructor %d: %v", index, err)
		}
		if got := applyTask10(t, machine, uint64(index), command); got.Code != ResultSuccess {
			t.Fatalf("register %d = %#v", index, got)
		}
	}
	extra, _ := NewRegisterWorker(InternalCommandID{0xff, 0xff, 2}, 0, WorkerRecord{
		NodeID: uint16(limit + 1), Epoch: model.WorkerEpoch{0xff, 0xff, 2}, State: WorkerEligible, Revision: 1, Slots: 1,
		ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint(),
	}, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, uint64(limit+1), extra); got.Code != ResultCapacityExhausted {
		t.Fatalf("worker capacity = %#v", got)
	}
	if len(machine.workers) != limit {
		t.Fatalf("worker count = %d, want %d", len(machine.workers), limit)
	}
}

func applyTask10(t *testing.T, machine *Machine, index uint64, command any) CommandResult {
	t.Helper()
	if _, begin := command.(BeginCoordinatorEpoch); !begin {
		if machine.coordinatorEpoch == (model.CoordinatorEpoch{}) {
			beginCommand, err := NewBeginCoordinatorEpoch(InternalCommandID{0xfe, 0xce}, machine.coordinatorRevision, 1, [16]byte{0xfe})
			if err != nil {
				t.Fatal(err)
			}
			encodedBegin, err := MarshalCommand(beginCommand)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := machine.Apply(1, 1, encodedBegin); err != nil {
				t.Fatal(err)
			}
		}
		command = bindTask10CommandFenceForTest(command, machine.coordinatorEpoch)
	}
	encoded, err := MarshalCommand(command)
	if err != nil {
		t.Fatalf("MarshalCommand(%T): %v", command, err)
	}
	if index <= machine.lastAppliedIndex {
		index = machine.lastAppliedIndex + 1
	}
	result, err := machine.Apply(index, 1, encoded)
	if err != nil {
		t.Fatalf("Apply(%T): %v", command, err)
	}
	return mustResult(t, result)
}

func bindTask10CommandFenceForTest(command any, fence model.CoordinatorEpoch) any {
	rebind := func(envelope *Envelope, target []byte) {
		envelope.CoordinatorEpoch = fence
		if envelope.Internal != nil {
			envelope.Internal.Digest = internalDigest(*envelope, target)
		}
	}
	switch value := command.(type) {
	case RegisterWorker:
		rebind(&value.Envelope, registerWorkerTarget(value))
		return value
	case DrainWorker:
		rebind(&value.Envelope, drainWorkerTarget(value))
		return value
	case DeactivateWorker:
		rebind(&value.Envelope, deactivateWorkerTarget(value))
		return value
	case ReplaceWorkerEpoch:
		rebind(&value.Envelope, replaceWorkerEpochTarget(value))
		return value
	case SubmitJob:
		value.Envelope.CoordinatorEpoch = fence
		return value
	case CancelJob:
		value.Envelope.CoordinatorEpoch = fence
		return value
	case RecordSourceEOF:
		rebind(&value.Envelope, recordSourceEOFTarget(value))
		return value
	case InstallAssignments:
		rebind(&value.Envelope, installAssignmentsTarget(value))
		return value
	case ReplaceAssignments:
		rebind(&value.Envelope, replaceAssignmentsTarget(value))
		return value
	case AdvanceCheckpoint:
		rebind(&value.Envelope, advanceCheckpointTarget(value))
		return value
	case SealManifest:
		rebind(&value.Envelope, sealManifestTarget(value))
		return value
	case TransitionJob:
		rebind(&value.Envelope, transitionJobTarget(value))
		return value
	case FailJob:
		rebind(&value.Envelope, failJobTarget(value))
		return value
	default:
		return command
	}
}
