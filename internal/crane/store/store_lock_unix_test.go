//go:build unix

package store

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/aadityakv/crane/internal/crane/model"
)

func TestLockIsExclusiveForStoreLifetimeAndCloseIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker")
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 2}
	store := mustOpen(t, path, identity, 1<<20, model.WorkerEpoch{3})
	if _, err := Open(path, identity, Options{MaxBytes: 1 << 20}); !errors.Is(err, ErrLocked) {
		t.Fatalf("second open err=%v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, identity, Options{MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	reopened.Close()
}

func TestStoreRejectsSymlinkAndNonOwnerOnlyPaths(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(link, Identity{ClusterID: [16]byte{1}, NodeID: 2}, Options{MaxBytes: 1 << 20}); err == nil {
		t.Fatal("accepted directory symlink")
	}
	path := filepath.Join(root, "worker")
	store := mustOpen(t, path, Identity{ClusterID: [16]byte{1}, NodeID: 2}, 1<<20, model.WorkerEpoch{3})
	store.Close()
	if err := os.Chmod(filepath.Join(path, WorkerWALFilename), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, Identity{ClusterID: [16]byte{1}, NodeID: 2}, Options{MaxBytes: 1 << 20}); err == nil {
		t.Fatal("accepted 0644 WAL")
	}
}

func TestStoreRejectsWALSymlink(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "worker")
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 2}
	store := mustOpen(t, path, identity, 1<<20, model.WorkerEpoch{3})
	store.Close()
	wal := filepath.Join(path, WorkerWALFilename)
	target := filepath.Join(root, "copied.wal")
	if err := os.Rename(wal, target); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, wal); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, identity, Options{MaxBytes: 1 << 20}); err == nil {
		t.Fatal("accepted WAL symlink")
	}
}

func TestStoreRejectsHardlinkedLockOrWAL(t *testing.T) {
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 2}
	for _, filename := range []string{WorkerLockFilename, WorkerWALFilename} {
		t.Run(filename, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "worker")
			store := mustOpen(t, path, identity, 1<<20, model.WorkerEpoch{3})
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.Link(filepath.Join(path, filename), filepath.Join(root, filename+".copy")); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(path, identity, Options{MaxBytes: 1 << 20}); err == nil {
				t.Fatalf("accepted hardlinked %s", filename)
			}
		})
	}
}

func TestStoreRejectsUnsafeRootPathsBeforeTouchingThem(t *testing.T) {
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 2}
	for _, path := range []string{"", ".", string(filepath.Separator)} {
		if _, err := Open(path, identity, Options{MaxBytes: 1 << 20}); !errors.Is(err, ErrInvalidOptions) {
			t.Fatalf("Open(%q) error=%v, want ErrInvalidOptions", path, err)
		}
	}
}

func TestStoreRejectsDirectoryReplacementBetweenOpenAndRootAnchor(t *testing.T) {
	parent := t.TempDir()
	path := filepath.Join(parent, "worker")
	moved := filepath.Join(parent, "worker-moved")
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 2}
	operations := defaultStoreOperations()
	originalOpenRoot := operations.openRoot
	operations.openRoot = func(openPath string) (*os.Root, error) {
		if err := os.Rename(path, moved); err != nil {
			return nil, err
		}
		if err := os.Mkdir(path, 0o700); err != nil {
			return nil, err
		}
		return originalOpenRoot(openPath)
	}
	if _, err := openWithOperations(path, identity, Options{MaxBytes: 1 << 20}, operations); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("replacement error=%v, want ErrCorrupt", err)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("replacement directory was touched: %v", entries)
	}
}

func TestStoreRepeatsCreationSyncBarriersAfterFailedAttempts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker")
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 2}
	injected := errors.New("injected creation sync failure")
	operations := defaultStoreOperations()
	operations.syncFile = func(*os.File) error { return injected }
	epochCalls := 0
	options := Options{MaxBytes: 1 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) {
		epochCalls++
		return model.WorkerEpoch{3}, nil
	}}
	if _, err := openWithOperations(path, identity, options, operations); !errors.Is(err, injected) {
		t.Fatalf("first open error=%v, want injected", err)
	}
	if epochCalls != 0 {
		t.Fatalf("epoch generated before parent creation barrier: %d", epochCalls)
	}

	operations = defaultStoreOperations()
	syncCalls := 0
	originalSync := operations.syncFile
	operations.syncFile = func(file *os.File) error {
		syncCalls++
		return originalSync(file)
	}
	store, err := openWithOperations(path, identity, options, operations)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if syncCalls < 4 {
		t.Fatalf("creation sync calls=%d, want parent, lock directory, WAL, and WAL directory barriers", syncCalls)
	}
	if epochCalls != 1 {
		t.Fatalf("epoch calls=%d, want 1", epochCalls)
	}
}

func TestStoreRuntimeWritesStayOnAnchoredDirectoryAfterPathReplacement(t *testing.T) {
	parent := t.TempDir()
	path := filepath.Join(parent, "worker")
	moved := filepath.Join(parent, "worker-moved")
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 2}
	store := mustOpen(t, path, identity, 1<<20, model.WorkerEpoch{3})
	defer store.Close()
	before, err := os.Stat(filepath.Join(path, WorkerWALFilename))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := commitRawForTest(store, Transaction{Records: []Record{{Type: 100, Payload: []byte("anchored")}}}); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(filepath.Join(moved, WorkerWALFilename))
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() <= before.Size() {
		t.Fatalf("anchored WAL size=%d, want >%d", after.Size(), before.Size())
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("replacement directory was touched: %v", entries)
	}
}

func TestLiveLockReplacementCannotCreateSecondStoreOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker")
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 2}
	first := mustOpen(t, path, identity, 1<<20, model.WorkerEpoch{3})
	defer first.Close()
	lockPath := filepath.Join(path, WorkerLockFilename)
	if err := os.Remove(lockPath); err != nil {
		t.Fatal(err)
	}
	replacement := []byte("replacement-lock")
	if err := os.WriteFile(lockPath, replacement, 0o600); err != nil {
		t.Fatal(err)
	}
	epochCalls := 0
	second, err := Open(path, identity, Options{MaxBytes: 1 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) {
		epochCalls++
		return model.WorkerEpoch{4}, nil
	}})
	if second != nil {
		defer second.Close()
	}
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("second Open error=%v, want ErrLocked", err)
	}
	if epochCalls != 0 {
		t.Fatalf("second owner generated epoch %d times", epochCalls)
	}
	after, readErr := os.ReadFile(lockPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if string(after) != string(replacement) {
		t.Fatalf("replacement lock mutated: %q", after)
	}
	if err := commitRawForTest(first, Transaction{Records: []Record{{Type: 100, Payload: []byte("first-owner")}}}); err != nil {
		t.Fatalf("first owner became unsafe: %v", err)
	}
}

func TestLiveWALReplacementCannotBeOpenedOrMutatedBySecondOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker")
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 2}
	first := mustOpen(t, path, identity, 1<<20, model.WorkerEpoch{3})
	defer first.Close()
	walPath := filepath.Join(path, WorkerWALFilename)
	if err := os.Remove(walPath); err != nil {
		t.Fatal(err)
	}
	replacement := []byte("replacement-wal")
	if err := os.WriteFile(walPath, replacement, 0o600); err != nil {
		t.Fatal(err)
	}
	second, err := Open(path, identity, Options{MaxBytes: 1 << 20})
	if second != nil {
		defer second.Close()
	}
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("second Open error=%v, want ErrLocked", err)
	}
	if err := commitRawForTest(first, Transaction{Records: []Record{{Type: 100, Payload: []byte("anchored-wal")}}}); err != nil {
		t.Fatalf("first owner became unsafe: %v", err)
	}
	after, readErr := os.ReadFile(walPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if string(after) != string(replacement) {
		t.Fatalf("replacement WAL mutated: %q", after)
	}
}
