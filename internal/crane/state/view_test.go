package state

import (
	"errors"
	"reflect"
	"sync"
	"testing"
)

func TestViewOwnsEveryNestedField(t *testing.T) {
	machine := completeSnapshotMachine(t, false)
	want := machine.View()
	got := machine.View()
	if len(got.Clients) == 0 || len(got.Subjects) == 0 || len(got.Workers) == 0 || len(got.Jobs) == 0 {
		t.Fatalf("view omitted retained state: %#v", got)
	}
	got.Clients[0].Result[0] ^= 1
	got.Subjects[0].Target[0] ^= 1
	got.Subjects[0].Result[0] ^= 1
	got.Subjects[0].AppliedTarget[0] ^= 1
	got.Subjects[0].AppliedResult[0] ^= 1
	got.Jobs[0].TopologyBytes[0] ^= 1
	if got.Jobs[0].Assignment != nil {
		got.Jobs[0].Assignment.Tasks[0].Attempt++
	}
	got.Jobs[0].SourceEOFs = nil
	got.Workers[0].Slots++
	if after := machine.View(); !viewsEqual(after, want) {
		t.Fatalf("caller mutation escaped into machine\n got=%#v\nwant=%#v", after, want)
	}
}

func TestApplyIndexIsStrictlyMonotonicAndMalformedDoesNotAdvanceView(t *testing.T) {
	machine := NewMachine()
	command := validBeginCommand(t, 0, 1, 0xa1)
	encoded, _ := MarshalCommand(command)
	if _, err := machine.Apply(4, 2, encoded); err != nil {
		t.Fatal(err)
	}
	if got := machine.View().AppliedIndex; got != 4 {
		t.Fatalf("applied index=%d want=4", got)
	}
	if _, err := machine.Apply(4, 2, encoded); !errorsIsInvalidApplyIndex(err) {
		t.Fatalf("duplicate apply index error=%v", err)
	}
	if _, err := machine.Apply(3, 2, encoded); !errorsIsInvalidApplyIndex(err) {
		t.Fatalf("decreasing apply index error=%v", err)
	}
	if _, err := machine.Apply(5, 2, []byte{1}); err == nil {
		t.Fatal("malformed Apply succeeded")
	}
	if got := machine.View().AppliedIndex; got != 4 {
		t.Fatalf("rejected apply advanced index to %d", got)
	}
	if _, err := machine.Apply(5, 2, encoded); err != nil {
		t.Fatalf("valid command at rejected index did not remain admissible: %v", err)
	}
	if got := machine.View().AppliedIndex; got != 5 {
		t.Fatalf("successful retry applied index=%d want=5", got)
	}
	rejected, err := NewBeginCoordinatorEpoch(InternalCommandID{0xa3}, 99, 1, [16]byte{0xa3})
	if err != nil {
		t.Fatal(err)
	}
	rejectedBytes, _ := MarshalCommand(rejected)
	if _, err := machine.Apply(6, 2, rejectedBytes); err != nil {
		t.Fatalf("business rejection returned Apply error: %v", err)
	}
	if got := machine.View().AppliedIndex; got != 6 {
		t.Fatalf("valid business rejection applied index=%d want=6", got)
	}
}

func TestBarrierPositionCaptureDoesNotReplaceLastCraneCommandIndex(t *testing.T) {
	machine := NewMachine()
	command := validBeginCommand(t, 0, 1, 0xa2)
	encoded, _ := MarshalCommand(command)
	if _, err := machine.Apply(7, 3, encoded); err != nil {
		t.Fatal(err)
	}
	// A Raft barrier/no-op at index 8 is not passed to Apply. Capture receives
	// the Raft snapshot position but the Crane view retains command index 7.
	capture, err := machine.Capture(8, 3)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := capture.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	restored := NewMachine()
	if err := restored.Restore(SnapshotSchemaVersion, payload); err != nil {
		t.Fatal(err)
	}
	if got := restored.View().AppliedIndex; got != 7 {
		t.Fatalf("restored Crane index=%d want=7 below barrier index 8", got)
	}
}

func TestConcurrentViewsAndCapturesNeverObservePartialApply(t *testing.T) {
	machine := NewMachine()
	first := validBeginCommand(t, 0, 1, 1)
	encoded, _ := MarshalCommand(first)
	if _, err := machine.Apply(1, 1, encoded); err != nil {
		t.Fatal(err)
	}

	var readers sync.WaitGroup
	stop := make(chan struct{})
	for reader := 0; reader < 8; reader++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				view := machine.View()
				if view.CoordinatorRevision != view.CoordinatorEpoch.Term || view.AppliedIndex != view.CoordinatorEpoch.BeginIndex {
					t.Errorf("partial view: %#v", view)
					return
				}
				capture, err := machine.Capture(1000, 1000)
				if err != nil {
					t.Errorf("Capture: %v", err)
					return
				}
				payload, err := capture.MarshalBinary()
				if err != nil || len(payload) == 0 {
					t.Errorf("MarshalBinary=%d,%v", len(payload), err)
					return
				}
			}
		}()
	}
	for revision := uint64(2); revision <= 50; revision++ {
		command, err := NewBeginCoordinatorEpoch(InternalCommandID{byte(revision), 0xb1}, revision-1, 1, [16]byte{byte(revision)})
		if err != nil {
			t.Fatal(err)
		}
		encoded, _ := MarshalCommand(command)
		if _, err := machine.Apply(revision, revision, encoded); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	readers.Wait()
}

func viewsEqual(left, right View) bool {
	return reflect.DeepEqual(left, right)
}

func errorsIsInvalidApplyIndex(err error) bool {
	return errors.Is(err, ErrInvalidApplyIndex)
}
