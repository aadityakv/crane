//go:build darwin || linux

package raft

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestStorageFileStoreInitializesExactOwnerOnlyLayout(t *testing.T) {
	directory := t.TempDir()
	identity, voters := testStorageIdentity(t, 1)
	store, err := OpenFileStore(directory, identity, voters)
	if err != nil {
		t.Fatalf("OpenFileStore: %v", err)
	}
	raftDirectory := filepath.Join(directory, RaftStorageDirectoryName)
	assertPathMode(t, raftDirectory, true, 0o700)
	assertPathMode(t, filepath.Join(raftDirectory, RaftLockFilename), false, 0o600)
	assertPathMode(t, filepath.Join(raftDirectory, RaftIdentityFilename), false, 0o600)
	assertPathMode(t, filepath.Join(raftDirectory, RaftWALFilename), false, 0o600)
	if _, err := os.Stat(filepath.Join(raftDirectory, RaftSnapshotFilename)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("snapshot reserve exists before Task 9: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLockRejectsLifetimeDoubleOpenAndAllowsReopenAfterClose(t *testing.T) {
	directory := t.TempDir()
	identity, voters := testStorageIdentity(t, 1)
	first, err := OpenFileStore(directory, identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenFileStore(directory, identity, voters); !errors.Is(err, ErrStorageLocked) {
		t.Fatalf("second open error = %v, want ErrStorageLocked", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := OpenFileStore(directory, identity, voters)
	if err != nil {
		t.Fatalf("reopen after close: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("second Close error = %v, want ErrStoreClosed", err)
	}
}

func TestLockFailureDoesNotAttemptUnlockForUnacquiredLock(t *testing.T) {
	directory := t.TempDir()
	identity, voters := testStorageIdentity(t, 1)
	injected := errors.New("injected exclusive lock failure")
	operations := defaultFileStoreOperations()
	unlockCalls := 0
	operations.flock = func(_ int, operation int) error {
		if operation&syscall.LOCK_EX != 0 {
			return injected
		}
		if operation&syscall.LOCK_UN != 0 {
			unlockCalls++
		}
		return nil
	}
	if _, err := openFileStore(directory, identity, voters, operations); !errors.Is(err, injected) {
		t.Fatalf("open error = %v, want injected", err)
	}
	if unlockCalls != 0 {
		t.Fatalf("unlock calls after failed acquisition = %d, want 0", unlockCalls)
	}
}

func TestIdentityEveryFieldAndCopiedVoterAreRejectedBeforeRecovery(t *testing.T) {
	identity, voters := testStorageIdentity(t, 1)
	tests := []struct {
		name   string
		offset int
	}{
		{name: "format", offset: 5},
		{name: "cluster", offset: 6},
		{name: "local voter", offset: 23},
		{name: "fingerprint", offset: 24},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			store, err := OpenFileStore(directory, identity, voters)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(directory, RaftStorageDirectoryName, RaftIdentityFilename)
			content, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			content[test.offset] ^= 1
			binaryChecksumIdentity(content)
			if err := os.WriteFile(path, content, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := OpenFileStore(directory, identity, voters); !errors.Is(err, ErrStorageIdentityMismatch) {
				t.Fatalf("open mutated identity error = %v, want ErrStorageIdentityMismatch", err)
			}
		})
	}

	directory := t.TempDir()
	store, err := OpenFileStore(directory, identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	otherIdentity, _ := testStorageIdentity(t, 2)
	if _, err := OpenFileStore(directory, otherIdentity, voters); !errors.Is(err, ErrStorageIdentityMismatch) {
		t.Fatalf("copied voter storage error = %v, want ErrStorageIdentityMismatch", err)
	}
}

func TestIdentityMissingInNonemptyDirectoryFailsClosed(t *testing.T) {
	identity, voters := testStorageIdentity(t, 1)
	for _, name := range []string{RaftWALFilename, RaftSnapshotFilename, "staging", "unknown"} {
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			raftDirectory := filepath.Join(directory, RaftStorageDirectoryName)
			if err := os.Mkdir(raftDirectory, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(raftDirectory, name), []byte("present"), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := OpenFileStore(directory, identity, voters); !errors.Is(err, ErrStorageCorrupt) {
				t.Fatalf("OpenFileStore error = %v, want ErrStorageCorrupt", err)
			}
		})
	}
}

func TestStorageRejectsSymlinkNonregularHardlinkAndWrongModes(t *testing.T) {
	identity, voters := testStorageIdentity(t, 1)
	tests := []struct {
		name  string
		alter func(t *testing.T, raftDirectory string)
	}{
		{
			name: "identity symlink",
			alter: func(t *testing.T, directory string) {
				path := filepath.Join(directory, RaftIdentityFilename)
				content, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				target := filepath.Join(directory, "identity-target")
				if err := os.WriteFile(target, content, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "wal directory",
			alter: func(t *testing.T, directory string) {
				path := filepath.Join(directory, RaftWALFilename)
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "identity hardlink",
			alter: func(t *testing.T, directory string) {
				if err := os.Link(filepath.Join(directory, RaftIdentityFilename), filepath.Join(directory, "identity-copy")); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "wal mode",
			alter: func(t *testing.T, directory string) {
				if err := os.Chmod(filepath.Join(directory, RaftWALFilename), 0o644); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			store, err := OpenFileStore(directory, identity, voters)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			raftDirectory := filepath.Join(directory, RaftStorageDirectoryName)
			test.alter(t, raftDirectory)
			if _, err := OpenFileStore(directory, identity, voters); err == nil {
				t.Fatal("OpenFileStore accepted unsafe path")
			}
		})
	}
}

func TestStorageRejectsUnsafeRaftDirectoryWithoutRepair(t *testing.T) {
	identity, voters := testStorageIdentity(t, 1)
	directory := t.TempDir()
	raftDirectory := filepath.Join(directory, RaftStorageDirectoryName)
	if err := os.Mkdir(raftDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(raftDirectory, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenFileStore(directory, identity, voters); err == nil {
		t.Fatal("OpenFileStore accepted permissive directory")
	}
	info, err := os.Stat(raftDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o755 {
		t.Fatalf("unsafe directory silently repaired to %o", info.Mode().Perm())
	}
}

func assertPathMode(t *testing.T, path string, directory bool, mode os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.IsDir() != directory || info.Mode().Perm() != mode {
		t.Fatalf("%s mode = %v, want directory=%v mode=%o", path, info.Mode(), directory, mode)
	}
}

func binaryChecksumIdentity(content []byte) {
	checksum := crc32.Checksum(content[:len(content)-identityChecksumBytes], crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(content[len(content)-identityChecksumBytes:], checksum)
}

var _ = syscall.LOCK_EX
