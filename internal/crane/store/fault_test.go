package store

import (
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestFaultSnapshotReplacementMatrixRecoversOldOrNewWholeState(t *testing.T) {
	oldPoints := []FaultPoint{
		FaultSnapshotTempCreate, FaultSnapshotTempWrite, FaultSnapshotTempSync, FaultSnapshotTempClose,
		FaultSnapshotRename, FaultSnapshotDirectorySync,
		FaultReplacementWALCreate, FaultReplacementWALWrite, FaultReplacementWALSync, FaultReplacementWALClose,
		FaultReplacementWALRename, FaultReplacementWALDirectorySync,
		FaultCurrentTempCreate, FaultCurrentTempWrite, FaultCurrentTempSync, FaultCurrentTempClose, FaultCurrentRename,
	}
	newPoints := []FaultPoint{FaultCurrentDirectorySync, FaultPreviousWALClose, FaultObsoleteCleanup, FaultObsoleteDirectorySync}
	for _, test := range append(faultCases(oldPoints, false), faultCases(newPoints, true)...) {
		t.Run(test.name, func(t *testing.T) {
			path, identity, options, store, _ := populatedSnapshotStore(t)
			beforeState, beforeWork := store.Recovered(), mustRecoverWork(t, store)
			injected := errors.New("injected " + test.name)
			store.options.Faults = &oneShotFault{point: test.point, err: injected}
			if _, err := store.Snapshot(); !errors.Is(err, injected) {
				t.Fatalf("Snapshot error=%v", err)
			}
			if err := store.Fence(beforeWork.Fence); !errors.Is(err, ErrUnavailable) {
				t.Fatalf("ambiguous store was not poisoned: %v", err)
			}
			_ = store.Close()
			reopened, err := Open(path, identity, Options{MaxBytes: options.MaxBytes})
			if err != nil {
				t.Fatalf("reopen after %s: %v", test.name, err)
			}
			defer reopened.Close()
			gotState, gotWork := reopened.Recovered(), mustRecoverWork(t, reopened)
			if !reflect.DeepEqual(gotWork, beforeWork) || gotState.LastSequence != beforeState.LastSequence || gotState.TransactionCount != beforeState.TransactionCount {
				t.Fatalf("mixed/lost state after %s: metadata=%+v work=%+v", test.name, gotState, gotWork)
			}
			wantGeneration := uint64(0)
			if test.newGeneration {
				wantGeneration = 1
			}
			if gotState.SnapshotGeneration != wantGeneration {
				t.Fatalf("generation after %s=%d want=%d", test.name, gotState.SnapshotGeneration, wantGeneration)
			}
		})
	}
}

func TestFaultAppendShortZeroWriteSyncPoisonAndReopen(t *testing.T) {
	for _, test := range []struct {
		name        string
		write       func(*os.File, []byte) (int, error)
		wantSuccess bool
	}{
		{name: "zero", write: func(*os.File, []byte) (int, error) { return 0, nil }},
		{name: "short", write: func(file *os.File, data []byte) (int, error) {
			if len(data) < 2 {
				return file.Write(data)
			}
			return file.Write(data[:len(data)/2])
		}, wantSuccess: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			path, identity, options, store, fixture := populatedSnapshotStore(t)
			beforeState, beforeWork := store.Recovered(), mustRecoverWork(t, store)
			store.operations.writeFile = test.write
			operationErr := store.Fence(fixture.epoch)
			if test.wantSuccess {
				if operationErr != nil {
					t.Fatalf("short writes were not completed: %v", operationErr)
				}
			} else {
				if operationErr == nil {
					t.Fatal("zero append succeeded")
				}
				if err := store.Fence(fixture.epoch); !errors.Is(err, ErrUnavailable) {
					t.Fatalf("write failure did not poison: %v", err)
				}
			}
			_ = store.Close()
			reopened, err := Open(path, identity, Options{MaxBytes: options.MaxBytes})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if !reflect.DeepEqual(mustRecoverWork(t, reopened), beforeWork) {
				t.Fatal("short/zero append changed logical state")
			}
			if test.wantSuccess {
				if reopened.Recovered().LastSequence != beforeState.LastSequence+3 {
					t.Fatal("completed short append was lost")
				}
			} else if reopened.Recovered() != beforeState {
				t.Fatal("zero append was published")
			}
		})
	}

	t.Run("sync ambiguity", func(t *testing.T) {
		path, identity, options, store, fixture := populatedSnapshotStore(t)
		beforeState, beforeWork := store.Recovered(), mustRecoverWork(t, store)
		fault := &oneShotFault{point: FaultBeforeSync, err: errors.New("sync")}
		store.options.Faults = fault
		event := domainFailureEvent(store, fixture.assignment, fixture.epoch, beforeWork.NextTransactionID)
		if err := store.PersistEvent(event); !errors.Is(err, fault.err) {
			t.Fatalf("sync error=%v", err)
		}
		if err := store.PersistEvent(event); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("sync failure did not poison=%v", err)
		}
		_ = store.Close()
		reopened, err := Open(path, identity, Options{MaxBytes: options.MaxBytes})
		if err != nil {
			t.Fatal(err)
		}
		defer reopened.Close()
		afterState, afterWork := reopened.Recovered(), mustRecoverWork(t, reopened)
		oldState := afterState == beforeState && reflect.DeepEqual(afterWork, beforeWork)
		newState := afterState.TransactionCount == beforeState.TransactionCount+1 && afterState.LastSequence == beforeState.LastSequence+3 && len(afterWork.PendingEvents) == len(beforeWork.PendingEvents)+1 && afterWork.NextTransactionID == beforeWork.NextTransactionID+1
		if !oldState && !newState {
			t.Fatal("sync ambiguity recovered neither old nor new whole transaction")
		}
	})
}

func TestFaultCloseFailuresReleaseResourcesAndReopen(t *testing.T) {
	for _, point := range []FaultPoint{FaultCloseWAL, FaultCloseLock, FaultCloseRoot, FaultCloseDirectory} {
		t.Run(point.String(), func(t *testing.T) {
			path, identity, options, store, _ := populatedSnapshotStore(t)
			injected := errors.New("close")
			store.options.Faults = &oneShotFault{point: point, err: injected}
			if err := store.Close(); !errors.Is(err, injected) {
				t.Fatalf("Close=%v", err)
			}
			reopened, err := Open(path, identity, Options{MaxBytes: options.MaxBytes})
			if err != nil {
				t.Fatalf("close failure retained ownership: %v", err)
			}
			reopened.Close()
		})
	}
}

func TestFaultSnapshotReplacementShortAndZeroWritesAtEveryFile(t *testing.T) {
	for _, target := range []string{snapshotTempFilename(1), generationWALTempFilename(1), currentTempFilename} {
		for _, zero := range []bool{false, true} {
			name := "short/" + target
			if zero {
				name = "zero/" + target
			}
			t.Run(name, func(t *testing.T) {
				path, identity, options, store, _ := populatedSnapshotStore(t)
				beforeState, beforeWork := store.Recovered(), mustRecoverWork(t, store)
				store.operations.writeFile = func(file *os.File, data []byte) (int, error) {
					if !strings.HasSuffix(file.Name(), target) {
						return file.Write(data)
					}
					if zero {
						return 0, nil
					}
					if len(data) < 2 {
						return file.Write(data)
					}
					return file.Write(data[:len(data)/2])
				}
				_, snapshotErr := store.Snapshot()
				if zero && snapshotErr == nil {
					t.Fatal("zero write succeeded")
				}
				if !zero && snapshotErr != nil {
					t.Fatalf("short writes were not completed: %v", snapshotErr)
				}
				_ = store.Close()
				reopened, err := Open(path, identity, Options{MaxBytes: options.MaxBytes})
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close()
				if !reflect.DeepEqual(mustRecoverWork(t, reopened), beforeWork) || reopened.Recovered().LastSequence != beforeState.LastSequence {
					t.Fatal("short/zero replacement recovered mixed state")
				}
				wantGeneration := uint64(1)
				if zero {
					wantGeneration = 0
				}
				if reopened.Recovered().SnapshotGeneration != wantGeneration {
					t.Fatalf("generation=%d want=%d", reopened.Recovered().SnapshotGeneration, wantGeneration)
				}
			})
		}
	}
}

func TestFaultDefaultInjectorIsNoOpAndPointsHaveStableNames(t *testing.T) {
	var injector FaultInjector = NoopFaultInjector{}
	for _, point := range allFaultPoints() {
		if point.String() == "" {
			t.Fatalf("unnamed fault point %d", point)
		}
		if err := injector.Inject(point); err != nil {
			t.Fatalf("default injector at %s: %v", point, err)
		}
	}
}

type faultCase struct {
	name          string
	point         FaultPoint
	newGeneration bool
}

func faultCases(points []FaultPoint, newGeneration bool) []faultCase {
	result := make([]faultCase, len(points))
	for i, point := range points {
		result[i] = faultCase{name: point.String(), point: point, newGeneration: newGeneration}
	}
	return result
}

type oneShotFault struct {
	point FaultPoint
	err   error
	fired bool
}

func (fault *oneShotFault) Inject(point FaultPoint) error {
	if point == fault.point && !fault.fired {
		fault.fired = true
		return fault.err
	}
	return nil
}
