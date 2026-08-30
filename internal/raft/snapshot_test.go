package raft

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestSnapshotEnvelopeMatchesPinnedPortableV1BytesAndZeroedChecksumRule(t *testing.T) {
	identity, alternate := snapshotTestIdentities(t)
	metadata := SnapshotMetadata{
		LastIncludedIndex:         0x0102030405060708,
		LastIncludedTerm:          0x1112131415161718,
		StateMachineSchemaVersion: 0x21222324,
	}
	snapshot, err := NewSnapshot(identity, metadata, []byte("state"), 5)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}

	want, err := hex.DecodeString("5246534e0001000102030405060708090a0b0c0d0e0f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f010203040506070811121314151617182122232400000000000000054ba69735ca53765ed6a709edb56c6ea236b7193a3b29a6b390c346f0f4340e4ec0e4e74a34b5fcc8975bd6b5bf14e443d9a4754fed68c1e2164dcd17dcd67eb27374617465")
	if err != nil {
		t.Fatal(err)
	}
	if got := snapshot.EnvelopeBytes(); string(got) != string(want) {
		t.Fatalf("envelope = %x, want pinned %x", got, want)
	}

	zeroed := append([]byte(nil), want...)
	clear(zeroed[snapshotEnvelopeChecksumOffset : snapshotEnvelopeChecksumOffset+sha256.Size])
	wantEnvelopeChecksum := sha256.Sum256(zeroed)
	if got := want[snapshotEnvelopeChecksumOffset : snapshotEnvelopeChecksumOffset+sha256.Size]; string(got) != string(wantEnvelopeChecksum[:]) {
		t.Fatalf("pinned envelope checksum = %x, want checksum over zeroed field %x", got, wantEnvelopeChecksum)
	}

	// Snapshot envelopes bind the cluster and voter set, not the local voter ID.
	decoded, err := DecodeSnapshotEnvelope(want, alternate, 5)
	if err != nil {
		t.Fatalf("DecodeSnapshotEnvelope on another fixed voter: %v", err)
	}
	if decoded.Metadata != metadata || string(decoded.StateBytes()) != "state" || decoded.ID == (SnapshotID{}) {
		t.Fatalf("decoded portable snapshot = %#v", decoded)
	}
}

func TestSnapshotEnvelopeRejectsEveryTruncationAndCorruptionBeforeReturningState(t *testing.T) {
	identity, _ := snapshotTestIdentities(t)
	metadata := SnapshotMetadata{LastIncludedIndex: 7, LastIncludedTerm: 3, StateMachineSchemaVersion: 2}
	snapshot, err := NewSnapshot(identity, metadata, []byte("immutable"), 9)
	if err != nil {
		t.Fatal(err)
	}
	envelope := snapshot.EnvelopeBytes()
	for length := 0; length < len(envelope); length++ {
		if _, err := DecodeSnapshotEnvelope(envelope[:length], identity, 9); !errors.Is(err, ErrInvalidSnapshot) {
			t.Fatalf("truncation %d error = %v, want ErrInvalidSnapshot", length, err)
		}
	}

	tests := []struct {
		name   string
		mutate func([]byte)
		limit  uint64
	}{
		{name: "magic", mutate: func(value []byte) { value[0] ^= 0xff }, limit: 9},
		{name: "version", mutate: func(value []byte) { value[5]++ }, limit: 9},
		{name: "cluster", mutate: func(value []byte) { value[6]++ }, limit: 9},
		{name: "voters", mutate: func(value []byte) { value[22]++ }, limit: 9},
		{name: "zero index", mutate: func(value []byte) { clear(value[54:62]) }, limit: 9},
		{name: "zero term", mutate: func(value []byte) { clear(value[62:70]) }, limit: 9},
		{name: "zero schema", mutate: func(value []byte) { clear(value[70:74]) }, limit: 9},
		{name: "impossible length", mutate: func(value []byte) { value[81]++ }, limit: 9},
		{name: "state checksum", mutate: func(value []byte) { value[82]++ }, limit: 9},
		{name: "envelope checksum", mutate: func(value []byte) { value[114]++ }, limit: 9},
		{name: "state", mutate: func(value []byte) { value[len(value)-1]++ }, limit: 9},
		{name: "over configured limit", mutate: func([]byte) {}, limit: 8},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := append([]byte(nil), envelope...)
			test.mutate(candidate)
			if _, err := DecodeSnapshotEnvelope(candidate, identity, test.limit); !errors.Is(err, ErrInvalidSnapshot) {
				t.Fatalf("DecodeSnapshotEnvelope error = %v, want ErrInvalidSnapshot", err)
			}
		})
	}

	withTrailing := append(append([]byte(nil), envelope...), 0)
	if _, err := DecodeSnapshotEnvelope(withTrailing, identity, 10); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("trailing-byte error = %v, want ErrInvalidSnapshot", err)
	}
}

func TestSnapshotOwnsStateAndReturnedBytes(t *testing.T) {
	identity, _ := snapshotTestIdentities(t)
	state := []byte("owned")
	snapshot, err := NewSnapshot(identity, SnapshotMetadata{LastIncludedIndex: 1, LastIncludedTerm: 1, StateMachineSchemaVersion: 1}, state, 5)
	if err != nil {
		t.Fatal(err)
	}
	state[0] = 'X'
	firstState := snapshot.StateBytes()
	firstEnvelope := snapshot.EnvelopeBytes()
	firstState[0] = 'Y'
	firstEnvelope[0] = 'Z'
	if got := string(snapshot.StateBytes()); got != "owned" {
		t.Fatalf("owned state = %q, want owned", got)
	}
	if got := snapshot.EnvelopeBytes()[:4]; string(got) != "RFSN" {
		t.Fatalf("owned envelope magic = %q, want RFSN", got)
	}
}

func TestSnapshotCompactionPersistsExactPayloadAndRetainsCompleteSuffix(t *testing.T) {
	identity, voters := testStorageIdentity(t, 1)
	factories := []struct {
		name string
		open func(*testing.T) StableStore
	}{
		{name: "memory", open: func(t *testing.T) StableStore {
			store, err := NewMemoryStore(identity, voters)
			if err != nil {
				t.Fatal(err)
			}
			return store
		}},
		{name: "file", open: func(t *testing.T) StableStore {
			store, err := OpenFileStore(t.TempDir(), identity, voters)
			if err != nil {
				t.Fatal(err)
			}
			return store
		}},
	}
	for _, factory := range factories {
		t.Run(factory.name, func(t *testing.T) {
			store := factory.open(t)
			defer store.Close()
			entries := []Entry{
				mustStorageEntry(t, 1, 1, "one"),
				mustStorageEntry(t, 2, 2, "two"),
				mustStorageEntry(t, 3, 3, "three"),
				mustStorageEntry(t, 4, 4, "uncommitted-four"),
			}
			if err := store.Persist(PersistenceBatch{
				HardState: hardStatePointer(HardState{Term: 4, CommitIndex: 3}), ReplaceFrom: 1, Entries: entries,
			}); err != nil {
				t.Fatal(err)
			}
			snapshot, err := NewSnapshot(identity, SnapshotMetadata{
				LastIncludedIndex: 2, LastIncludedTerm: 2, StateMachineSchemaVersion: 9,
			}, []byte("at-two"), 64)
			if err != nil {
				t.Fatal(err)
			}
			snapshotStore, ok := store.(interface{ PersistSnapshot(Snapshot) error })
			if !ok {
				t.Fatalf("%T does not implement snapshot persistence", store)
			}
			if err := snapshotStore.PersistSnapshot(snapshot); err != nil {
				t.Fatalf("PersistSnapshot: %v", err)
			}
			state, err := store.Recover()
			if err != nil {
				t.Fatal(err)
			}
			if state.Snapshot == nil || state.Snapshot.Metadata != snapshot.Metadata || string(state.Snapshot.StateBytes()) != "at-two" {
				t.Fatalf("recovered snapshot = %#v", state.Snapshot)
			}
			if state.SnapshotBase != snapshot.Metadata || state.AppliedIndex != 2 {
				t.Fatalf("recovered base/applied = %+v/%d", state.SnapshotBase, state.AppliedIndex)
			}
			if len(state.Entries) != 2 || state.Entries[0].Index != 3 || state.Entries[1].Index != 4 || string(state.Entries[1].CommandBytes()) != "uncommitted-four" {
				t.Fatalf("retained suffix = %+v", state.Entries)
			}
		})
	}
}

func TestSnapshotRecoveryAcceptsNewSnapshotWithOldFullerWAL(t *testing.T) {
	directory := t.TempDir()
	identity, voters := testStorageIdentity(t, 1)
	store, err := OpenFileStore(directory, identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(PersistenceBatch{
		HardState: hardStatePointer(HardState{Term: 3, CommitIndex: 3}), ReplaceFrom: 1,
		Entries: []Entry{
			mustStorageEntry(t, 1, 1, "one"),
			mustStorageEntry(t, 2, 2, "two"),
			mustStorageEntry(t, 3, 3, "three"),
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	snapshot, err := NewSnapshot(identity, SnapshotMetadata{LastIncludedIndex: 2, LastIncludedTerm: 2, StateMachineSchemaVersion: 4}, []byte("new"), 64)
	if err != nil {
		t.Fatal(err)
	}
	snapshotPath := filepath.Join(directory, RaftStorageDirectoryName, RaftSnapshotFilename)
	if err := os.WriteFile(snapshotPath, snapshot.EnvelopeBytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenFileStore(directory, identity, voters)
	if err != nil {
		t.Fatalf("OpenFileStore mixed pair: %v", err)
	}
	defer reopened.Close()
	state, err := reopened.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if state.Snapshot == nil || state.SnapshotBase != snapshot.Metadata || state.AppliedIndex != 2 || len(state.Entries) != 1 || state.Entries[0].Index != 3 {
		t.Fatalf("reconciled mixed pair = %+v", state)
	}
}

func TestSnapshotRecoveryBuildsCoreThenRestoresAndReplaysCommittedSuffixFresh(t *testing.T) {
	identity, _ := testStorageIdentity(t, 1)
	snapshot, err := NewSnapshot(identity, SnapshotMetadata{LastIncludedIndex: 2, LastIncludedTerm: 2, StateMachineSchemaVersion: 7}, []byte("at-two"), 64)
	if err != nil {
		t.Fatal(err)
	}
	state := RecoveredState{
		HardState:    HardState{Term: 4, CommitIndex: 4},
		SnapshotBase: snapshot.Metadata,
		Snapshot:     &snapshot,
		AppliedIndex: 2,
		Entries: []Entry{
			mustStorageEntry(t, 3, 3, "three"),
			mustTask8NoOp(t, 4, 4),
			mustStorageEntry(t, 5, 4, "uncommitted-five"),
		},
	}
	events := &task8EventLog{}
	options, store, machine, _ := task8NodeOptions(t, state)
	store.events = events
	machine.events = events
	node, err := NewNode(options)
	if err != nil {
		t.Fatal(err)
	}
	node.newCore = func(options CoreOptions) (*Core, error) {
		events.add("core")
		return NewCore(options)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- node.Run(ctx) }()
	select {
	case <-node.Ready():
	case err := <-result:
		t.Fatalf("Run before Ready: %v", err)
	}

	machine.mu.Lock()
	if machine.restoreCalls != 1 || len(machine.restoreBytes) != 1 || string(machine.restoreBytes[0]) != "at-two" {
		t.Fatalf("Restore calls=%d bytes=%q", machine.restoreCalls, machine.restoreBytes)
	}
	if len(machine.applyCalls) != 1 || machine.applyCalls[0].index != 3 || string(machine.applyCalls[0].command) != "three" {
		t.Fatalf("replayed suffix = %#v", machine.applyCalls)
	}
	machine.mu.Unlock()
	if got := node.Status(); got.AppliedIndex != 4 || got.CommitIndex != 4 || got.LastIndex != 5 {
		t.Fatalf("recovered status = %#v", got)
	}

	cancel()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if got, want := events.snapshot(), []string{"recover", "core", "restore", "apply:3", "close"}; !equalStrings(got, want) {
		t.Fatalf("recovery order = %v, want %v", got, want)
	}
}

type captureTestHandle struct {
	schema  uint32
	state   []byte
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (capture *captureTestHandle) SchemaVersion() uint32 { return capture.schema }

func (capture *captureTestHandle) MarshalBinary() ([]byte, error) {
	if capture.started != nil {
		capture.once.Do(func() { close(capture.started) })
	}
	if capture.release != nil {
		<-capture.release
	}
	return cloneBytes(capture.state), nil
}

type captureTestMachine struct {
	mu       sync.Mutex
	live     []byte
	calls    []string
	started  chan struct{}
	release  chan struct{}
	captures int
}

func (machine *captureTestMachine) Apply(index, term uint64, command []byte) ([]byte, error) {
	machine.mu.Lock()
	defer machine.mu.Unlock()
	machine.live = cloneBytes(command)
	machine.calls = append(machine.calls, fmt.Sprintf("apply:%d:%d", index, term))
	return nil, nil
}

func (machine *captureTestMachine) Capture(index, term uint64) (SnapshotCapture, error) {
	machine.mu.Lock()
	defer machine.mu.Unlock()
	machine.captures++
	machine.calls = append(machine.calls, fmt.Sprintf("capture:%d:%d", index, term))
	return &captureTestHandle{schema: 5, state: cloneBytes(machine.live), started: machine.started, release: machine.release}, nil
}

func (*captureTestMachine) Restore(uint32, []byte) error { return nil }

func TestCaptureTriggersOnlyPastBoundaryUsesImmutableHandleAndCompactsAfterStore(t *testing.T) {
	node, store, machine := captureTestNode(t, 2)
	started, err := node.maybeStartSnapshotCapture()
	if err != nil {
		t.Fatal(err)
	}
	if started || machine.captures != 0 {
		t.Fatal("capture started at the exact entry threshold boundary")
	}

	entry3 := mustStorageEntry(t, 3, 3, "at-three")
	if _, err := node.core.log.Append(2, 2, []Entry{entry3}); err != nil {
		t.Fatal(err)
	}
	if err := node.core.log.AdvanceCommit(3); err != nil {
		t.Fatal(err)
	}
	if err := node.core.log.AdvanceApplied(3); err != nil {
		t.Fatal(err)
	}
	node.core.hardState.CommitIndex = 3
	hardState := HardState{Term: 3, CommitIndex: 3}
	if err := store.Persist(PersistenceBatch{HardState: &hardState, ReplaceFrom: 3, Entries: []Entry{entry3}}); err != nil {
		t.Fatal(err)
	}
	node.durableState.HardState = hardState
	node.durableState.Entries = append(node.durableState.Entries, entry3.Clone())
	if _, err := machine.Apply(3, 3, []byte("at-three")); err != nil {
		t.Fatal(err)
	}
	started, err = node.maybeStartSnapshotCapture()
	if err != nil || !started {
		t.Fatalf("maybeStartSnapshotCapture = %t, %v", started, err)
	}
	<-machine.started
	if startedAgain, err := node.maybeStartSnapshotCapture(); err != nil || startedAgain || machine.captures != 1 {
		t.Fatalf("second capture = %t, %v calls=%d", startedAgain, err, machine.captures)
	}
	if _, err := machine.Apply(4, 3, []byte("later-live-state")); err != nil {
		t.Fatal(err)
	}
	close(machine.release)
	result := (<-node.events).(snapshotCaptureResult)
	if err := node.finishSnapshotCapture(result); err != nil {
		t.Fatal(err)
	}
	recovered, err := store.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Snapshot == nil || string(recovered.Snapshot.StateBytes()) != "at-three" {
		t.Fatalf("captured immutable state = %#v", recovered.Snapshot)
	}
	if node.core.LogState().SnapshotIndex != 3 || recovered.SnapshotBase.LastIncludedIndex != 3 {
		t.Fatalf("compacted bases core=%d store=%d", node.core.LogState().SnapshotIndex, recovered.SnapshotBase.LastIncludedIndex)
	}
	if got := machine.calls; len(got) < 2 || got[1] != "capture:3:3" {
		t.Fatalf("capture calls = %v", got)
	}
}

func TestCaptureStoreFailureLeavesCorePrefixUntouched(t *testing.T) {
	node, store, machine := captureTestNode(t, 1)
	store.FailNext(StorageOperationSnapshotPersist, errors.New("snapshot durable failure"))
	started, err := node.maybeStartSnapshotCapture()
	if err != nil || !started {
		t.Fatalf("start capture = %t, %v", started, err)
	}
	close(machine.release)
	result := (<-node.events).(snapshotCaptureResult)
	if err := node.finishSnapshotCapture(result); err == nil {
		t.Fatal("snapshot persistence failure was acknowledged")
	}
	if got := node.core.LogState().SnapshotIndex; got != 0 {
		t.Fatalf("Core compacted to %d before Store durability", got)
	}
}

func captureTestNode(t *testing.T, threshold uint64) (*Node, *MemoryStore, *captureTestMachine) {
	t.Helper()
	identity, voters := testStorageIdentity(t, 1)
	entries := []Entry{mustStorageEntry(t, 1, 1, "one"), mustStorageEntry(t, 2, 2, "at-two")}
	store, err := NewMemoryStore(identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(PersistenceBatch{HardState: hardStatePointer(HardState{Term: 3, CommitIndex: 2}), ReplaceFrom: 1, Entries: entries}); err != nil {
		t.Fatal(err)
	}
	log, err := NewLog(0, 0, 2, 2, entries)
	if err != nil {
		t.Fatal(err)
	}
	core, err := NewCore(CoreOptions{LocalID: 1, Voters: voters, HardState: HardState{Term: 3, CommitIndex: 2}, Log: log, AppliedIndex: 2, ElectionTimeoutMin: 5, ElectionTimeoutMax: 10, HeartbeatInterval: 1, Random: task8ZeroOffsetRandom{}})
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	machine := &captureTestMachine{live: []byte("at-two"), started: make(chan struct{}), release: release}
	options := NodeOptions{LocalID: 1, Voters: voters, Identity: identity, Store: store, StateMachine: machine,
		Transport: &task8Transport{result: TransportAccepted}, Random: task8ZeroOffsetRandom{},
		ElectionTimeoutMin: 5, ElectionTimeoutMax: 10, HeartbeatInterval: 1,
		SnapshotEntryThreshold: threshold, SnapshotByteThreshold: math.MaxUint64, MaxSnapshotBytes: 64,
	}
	node := &Node{options: options, core: core, durableState: RecoveredState{Identity: identity, HardState: HardState{Term: 3, CommitIndex: 2}, Entries: cloneEntries(entries)}, events: make(chan any, 4), done: make(chan struct{})}
	return node, store, machine
}

func TestSnapshotCompactionFaultsReopenAsExactOldOrNewPair(t *testing.T) {
	type faultPoint struct {
		name string
		arm  func(*fileStoreOperations, *bool)
	}
	faults := []faultPoint{
		{name: "snapshot temp create", arm: func(ops *fileStoreOperations, armed *bool) {
			original := ops.rootOpenFile
			ops.rootOpenFile = func(root *os.Root, name string, flags int, mode os.FileMode) (*os.File, error) {
				if *armed && strings.HasPrefix(name, ".snapshot.tmp-") {
					return nil, errors.New("injected snapshot create")
				}
				return original(root, name, flags, mode)
			}
		}},
		{name: "snapshot write", arm: snapshotWriteFault(false)},
		{name: "snapshot zero progress", arm: snapshotWriteFault(true)},
		{name: "snapshot sync", arm: syncFileFault(".snapshot.tmp-", 0)},
		{name: "snapshot close", arm: closeFileFault(".snapshot.tmp-")},
		{name: "snapshot rename", arm: renameFault(RaftSnapshotFilename)},
		{name: "snapshot directory sync", arm: syncFileFault("raft", 1)},
		{name: "wal temp create", arm: func(ops *fileStoreOperations, armed *bool) {
			original := ops.rootOpenFile
			ops.rootOpenFile = func(root *os.Root, name string, flags int, mode os.FileMode) (*os.File, error) {
				if *armed && strings.HasPrefix(name, ".wal.tmp-") {
					return nil, errors.New("injected WAL create")
				}
				return original(root, name, flags, mode)
			}
		}},
		{name: "wal write", arm: walWriteFault()},
		{name: "wal sync", arm: syncFileFault(".wal.tmp-", 0)},
		{name: "wal rename", arm: renameFault(RaftWALFilename)},
		{name: "wal directory sync", arm: syncFileFault("raft", 2)},
		{name: "replaced wal close", arm: closeFileFault(RaftWALFilename)},
		{name: "snapshot cleanup", arm: snapshotCleanupFault()},
		{name: "wal cleanup", arm: walCleanupFault()},
	}

	for _, point := range faults {
		t.Run(point.name, func(t *testing.T) {
			directory := t.TempDir()
			identity, voters := testStorageIdentity(t, 1)
			initializeSnapshotFaultState(t, directory, identity, voters)
			operations := defaultFileStoreOperations()
			armed := false
			point.arm(&operations, &armed)
			store, err := openFileStore(directory, identity, voters, operations)
			if err != nil {
				t.Fatalf("open injected store: %v", err)
			}
			snapshot, err := NewSnapshot(identity, SnapshotMetadata{LastIncludedIndex: 2, LastIncludedTerm: 2, StateMachineSchemaVersion: 8}, []byte("at-two"), 64)
			if err != nil {
				t.Fatal(err)
			}
			armed = true
			if err := store.PersistSnapshot(snapshot); err == nil {
				t.Fatal("injected snapshot compaction unexpectedly succeeded")
			}
			armed = false
			_ = store.Close()

			reopened, err := OpenFileStore(directory, identity, voters)
			if err != nil {
				t.Fatalf("reopen after %s: %v", point.name, err)
			}
			defer reopened.Close()
			state, err := reopened.Recover()
			if err != nil {
				t.Fatal(err)
			}
			oldPair := state.Snapshot == nil && state.SnapshotBase == (SnapshotMetadata{}) && state.AppliedIndex == 0 && len(state.Entries) == 3
			newPair := state.Snapshot != nil && state.Snapshot.ID == snapshot.ID && state.SnapshotBase == snapshot.Metadata && state.AppliedIndex == 2 && len(state.Entries) == 1 && state.Entries[0].Index == 3
			if !oldPair && !newPair {
				t.Fatalf("recovered mixed state after %s: %+v", point.name, state)
			}
		})
	}
}

func initializeSnapshotFaultState(t *testing.T, directory string, identity StorageIdentity, voters VoterSet) {
	t.Helper()
	store, err := OpenFileStore(directory, identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(PersistenceBatch{HardState: hardStatePointer(HardState{Term: 3, CommitIndex: 3}), ReplaceFrom: 1, Entries: []Entry{
		mustStorageEntry(t, 1, 1, "one"), mustStorageEntry(t, 2, 2, "two"), mustStorageEntry(t, 3, 3, "three"),
	}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func snapshotWriteFault(zero bool) func(*fileStoreOperations, *bool) {
	return func(ops *fileStoreOperations, armed *bool) {
		original := ops.write
		ops.write = func(file *os.File, content []byte) (int, error) {
			if *armed && strings.HasPrefix(filepath.Base(file.Name()), ".snapshot.tmp-") {
				if zero {
					return 0, nil
				}
				return 0, errors.New("injected snapshot write")
			}
			return original(file, content)
		}
	}
}

func walWriteFault() func(*fileStoreOperations, *bool) {
	return func(ops *fileStoreOperations, armed *bool) {
		original := ops.write
		ops.write = func(file *os.File, content []byte) (int, error) {
			if *armed && strings.HasPrefix(filepath.Base(file.Name()), ".wal.tmp-") {
				return 0, errors.New("injected WAL write")
			}
			return original(file, content)
		}
	}
}

func syncFileFault(basePrefix string, directoryOrdinal int) func(*fileStoreOperations, *bool) {
	return func(ops *fileStoreOperations, armed *bool) {
		original := ops.sync
		directoryCalls := 0
		ops.sync = func(file *os.File) error {
			if *armed {
				base := filepath.Base(file.Name())
				if directoryOrdinal == 0 && strings.HasPrefix(base, basePrefix) {
					return errors.New("injected file sync")
				}
				if directoryOrdinal != 0 && base == basePrefix {
					directoryCalls++
					if directoryCalls == directoryOrdinal {
						return errors.New("injected directory sync")
					}
				}
			}
			return original(file)
		}
	}
}

func closeFileFault(basePrefix string) func(*fileStoreOperations, *bool) {
	return func(ops *fileStoreOperations, armed *bool) {
		original := ops.close
		fired := false
		ops.close = func(file *os.File) error {
			if *armed && !fired && strings.HasPrefix(filepath.Base(file.Name()), basePrefix) {
				fired = true
				return errors.New("injected close")
			}
			return original(file)
		}
	}
}

func renameFault(newName string) func(*fileStoreOperations, *bool) {
	return func(ops *fileStoreOperations, armed *bool) {
		original := ops.rootRename
		ops.rootRename = func(root *os.Root, oldName, target string) error {
			if *armed && target == newName {
				return errors.New("injected rename")
			}
			return original(root, oldName, target)
		}
	}
}

func snapshotCleanupFault() func(*fileStoreOperations, *bool) {
	return func(ops *fileStoreOperations, armed *bool) {
		snapshotWriteFault(false)(ops, armed)
		original := ops.rootRemove
		ops.rootRemove = func(root *os.Root, name string) error {
			if *armed && strings.HasPrefix(name, ".snapshot.tmp-") {
				return errors.New("injected snapshot cleanup")
			}
			return original(root, name)
		}
	}
}

func walCleanupFault() func(*fileStoreOperations, *bool) {
	return func(ops *fileStoreOperations, armed *bool) {
		walWriteFault()(ops, armed)
		original := ops.rootRemove
		ops.rootRemove = func(root *os.Root, name string) error {
			if *armed && strings.HasPrefix(name, ".wal.tmp-") {
				return errors.New("injected WAL cleanup")
			}
			return original(root, name)
		}
	}
}

func snapshotTestIdentities(t *testing.T) (StorageIdentity, StorageIdentity) {
	t.Helper()
	cluster := [16]byte{}
	for index := range cluster {
		cluster[index] = byte(index)
	}
	fingerprint := VoterFingerprint{}
	for index := range fingerprint {
		fingerprint[index] = byte(0x20 + index)
	}
	return StorageIdentity{FormatVersion: StorageFormatVersion1, ClusterID: cluster, LocalVoterID: 1, VoterFingerprint: fingerprint},
		StorageIdentity{FormatVersion: StorageFormatVersion1, ClusterID: cluster, LocalVoterID: 2, VoterFingerprint: fingerprint}
}
