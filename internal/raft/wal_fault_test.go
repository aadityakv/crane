//go:build darwin || linux

package raft

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestWALWriteAllHandlesRepeatedShortWritesAndRejectsZeroProgress(t *testing.T) {
	content := []byte("abcdef")
	var got []byte
	if err := writeAll(content, func(remaining []byte) (int, error) {
		length := 2
		if len(remaining) < length {
			length = len(remaining)
		}
		got = append(got, remaining[:length]...)
		return length, nil
	}); err != nil {
		t.Fatalf("writeAll short writes: %v", err)
	}
	if string(got) != "abcdef" {
		t.Fatalf("short-write result = %q, want abcdef", got)
	}
	if err := writeAll(content, func([]byte) (int, error) { return 0, nil }); !errors.Is(err, io.ErrNoProgress) {
		t.Fatalf("zero-progress error = %v, want io.ErrNoProgress", err)
	}
	if err := writeAll(content, func(remaining []byte) (int, error) { return len(remaining) + 1, nil }); err == nil {
		t.Fatal("writeAll accepted an impossible write count")
	}
}

func TestWALFailedWriteKeepsAcknowledgedStateAndReopensAtPriorTransaction(t *testing.T) {
	directory := t.TempDir()
	identity, voters := testStorageIdentity(t, 1)
	store, err := OpenFileStore(directory, identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(PersistenceBatch{HardState: hardStatePointer(HardState{Term: 1})}); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected partial write")
	wrote := false
	store.ops.write = func(file *os.File, content []byte) (int, error) {
		if wrote {
			return 0, injected
		}
		wrote = true
		length := len(content) / 2
		written, writeErr := file.Write(content[:length])
		if writeErr != nil {
			return written, writeErr
		}
		return written, injected
	}
	if err := store.Persist(PersistenceBatch{HardState: hardStatePointer(HardState{Term: 2})}); !errors.Is(err, injected) {
		t.Fatalf("Persist error = %v, want injected", err)
	}
	state, err := store.Recover()
	if err != nil {
		t.Fatalf("Recover acknowledged state after failed persist: %v", err)
	}
	if state.HardState.Term != 1 {
		t.Fatalf("acknowledged term after failed persist = %d, want 1", state.HardState.Term)
	}
	store.ops.write = defaultFileStoreOperations().write
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenFileStore(directory, identity, voters)
	if err != nil {
		t.Fatalf("reopen after partial write: %v", err)
	}
	defer reopened.Close()
	state, err = reopened.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if state.HardState.Term != 1 {
		t.Fatalf("reopened term = %d, want last successful term 1", state.HardState.Term)
	}
}

func TestWALRepeatedRealShortWritesPersistCompleteTransaction(t *testing.T) {
	directory := t.TempDir()
	identity, voters := testStorageIdentity(t, 1)
	store, err := OpenFileStore(directory, identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	store.ops.write = func(file *os.File, content []byte) (int, error) {
		if len(content) > 3 {
			content = content[:3]
		}
		return file.Write(content)
	}
	if err := store.Persist(PersistenceBatch{HardState: hardStatePointer(HardState{Term: 1})}); err != nil {
		t.Fatalf("Persist through short writes: %v", err)
	}
	store.ops.write = defaultFileStoreOperations().write
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenFileStore(directory, identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	state, err := reopened.Recover()
	if err != nil || state.HardState.Term != 1 {
		t.Fatalf("Recover = (%+v, %v), want term 1", state, err)
	}
}

func TestWALZeroProgressWriteReturnsErrorWithoutProtocolSuccess(t *testing.T) {
	directory := t.TempDir()
	identity, voters := testStorageIdentity(t, 1)
	store, err := OpenFileStore(directory, identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	store.ops.write = func(*os.File, []byte) (int, error) { return 0, nil }
	if err := store.Persist(PersistenceBatch{HardState: hardStatePointer(HardState{Term: 1})}); !errors.Is(err, io.ErrNoProgress) {
		t.Fatalf("Persist error = %v, want io.ErrNoProgress", err)
	}
	state, err := store.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if state.HardState != (HardState{}) {
		t.Fatalf("zero-progress persist mutated acknowledged state: %+v", state.HardState)
	}
	store.ops.write = defaultFileStoreOperations().write
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenFileStore(directory, identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	state, err = reopened.Recover()
	if err != nil || state.HardState != (HardState{}) {
		t.Fatalf("reopen after zero progress = (%+v, %v), want empty state", state, err)
	}
}

func TestWALSyncFailureKeepsOldAcknowledgementButCompleteTransactionMayRecover(t *testing.T) {
	directory := t.TempDir()
	identity, voters := testStorageIdentity(t, 1)
	store, err := OpenFileStore(directory, identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected WAL sync failure")
	store.ops.sync = func(file *os.File) error {
		if file == store.wal {
			return injected
		}
		return file.Sync()
	}
	if err := store.Persist(PersistenceBatch{HardState: hardStatePointer(HardState{Term: 1})}); !errors.Is(err, injected) {
		t.Fatalf("Persist error = %v, want injected", err)
	}
	state, err := store.Recover()
	if err != nil {
		t.Fatalf("Recover acknowledged state: %v", err)
	}
	if state.HardState != (HardState{}) {
		t.Fatalf("failed sync mutated acknowledgement: %+v", state.HardState)
	}
	store.ops.sync = defaultFileStoreOperations().sync
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenFileStore(directory, identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	state, err = reopened.Recover()
	if err != nil || state.HardState.Term != 1 {
		t.Fatalf("complete written transaction after failed sync recovered as (%+v, %v), want term 1", state, err)
	}
}

func TestPartialRecoveryCallsTruncateThenWALSync(t *testing.T) {
	directory := t.TempDir()
	identity, voters := testStorageIdentity(t, 1)
	initializeOneTransaction(t, directory, identity, voters)
	wallet := filepath.Join(directory, RaftStorageDirectoryName, RaftWALFilename)
	transaction, err := encodeWALTransaction(2, PersistenceBatch{HardState: hardStatePointer(HardState{Term: 2, CommitIndex: 1})})
	if err != nil {
		t.Fatal(err)
	}
	appendBytes(t, wallet, transaction[:len(transaction)-1])
	operations := defaultFileStoreOperations()
	truncated := false
	syncedAfterTruncate := false
	operations.truncate = func(file *os.File, size int64) error {
		truncated = true
		return file.Truncate(size)
	}
	operations.sync = func(file *os.File) error {
		if truncated && filepath.Base(file.Name()) == RaftWALFilename {
			syncedAfterTruncate = true
		}
		return file.Sync()
	}
	store, err := openFileStore(directory, identity, voters, operations)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if !truncated || !syncedAfterTruncate {
		t.Fatalf("crash-tail recovery truncated=%v syncedAfter=%v, want both true", truncated, syncedAfterTruncate)
	}
}

func TestPartialRecoveryPropagatesTruncateAndSyncFailures(t *testing.T) {
	identity, voters := testStorageIdentity(t, 1)
	for _, test := range []struct {
		name  string
		alter func(*fileStoreOperations, error)
	}{
		{
			name: "truncate",
			alter: func(operations *fileStoreOperations, injected error) {
				operations.truncate = func(*os.File, int64) error { return injected }
			},
		},
		{
			name: "sync",
			alter: func(operations *fileStoreOperations, injected error) {
				original := operations.sync
				operations.sync = func(file *os.File) error {
					if filepath.Base(file.Name()) == RaftWALFilename {
						return injected
					}
					return original(file)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			initializeOneTransaction(t, directory, identity, voters)
			transaction, err := encodeWALTransaction(2, PersistenceBatch{HardState: hardStatePointer(HardState{Term: 2, CommitIndex: 1})})
			if err != nil {
				t.Fatal(err)
			}
			appendBytes(t, filepath.Join(directory, RaftStorageDirectoryName, RaftWALFilename), transaction[:len(transaction)-1])
			injected := errors.New("injected " + test.name + " failure")
			operations := defaultFileStoreOperations()
			test.alter(&operations, injected)
			if _, err := openFileStore(directory, identity, voters, operations); !errors.Is(err, injected) {
				t.Fatalf("open error = %v, want injected", err)
			}
		})
	}
}

func TestIdentityRenameAndDirectorySyncFailuresAreFatalAndRecoverable(t *testing.T) {
	identity, voters := testStorageIdentity(t, 1)
	for _, test := range []struct {
		name  string
		alter func(*fileStoreOperations, string, error)
	}{
		{
			name: "rename",
			alter: func(operations *fileStoreOperations, _ string, injected error) {
				operations.rename = func(string, string) error { return injected }
			},
		},
		{
			name: "directory sync",
			alter: func(operations *fileStoreOperations, raftDirectory string, injected error) {
				original := operations.sync
				operations.sync = func(file *os.File) error {
					if file.Name() == raftDirectory {
						return injected
					}
					return original(file)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			raftDirectory := filepath.Join(directory, RaftStorageDirectoryName)
			injected := errors.New("injected " + test.name + " failure")
			operations := defaultFileStoreOperations()
			test.alter(&operations, raftDirectory, injected)
			if _, err := openFileStore(directory, identity, voters, operations); !errors.Is(err, injected) {
				t.Fatalf("open error = %v, want injected", err)
			}
			matches, err := filepath.Glob(filepath.Join(raftDirectory, ".identity.tmp-*"))
			if err != nil {
				t.Fatal(err)
			}
			if len(matches) != 0 {
				t.Fatalf("temporary identities remain: %v", matches)
			}
			store, err := OpenFileStore(directory, identity, voters)
			if err != nil {
				t.Fatalf("retry after injected failure: %v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorageDirectoryCreationRequiresParentDirectorySync(t *testing.T) {
	directory := t.TempDir()
	identity, voters := testStorageIdentity(t, 1)
	injected := errors.New("injected parent directory sync failure")
	operations := defaultFileStoreOperations()
	originalSync := operations.sync
	operations.sync = func(file *os.File) error {
		if file.Name() == directory {
			return injected
		}
		return originalSync(file)
	}
	store, err := openFileStore(directory, identity, voters, operations)
	if store != nil {
		_ = store.Close()
	}
	if !errors.Is(err, injected) {
		t.Fatalf("open error = %v, want parent directory sync failure", err)
	}
}

func TestLockAndCloseFailuresPropagateWithoutSkippingUnlock(t *testing.T) {
	identity, voters := testStorageIdentity(t, 1)
	t.Run("lock", func(t *testing.T) {
		directory := t.TempDir()
		injected := errors.New("injected lock failure")
		operations := defaultFileStoreOperations()
		originalFlock := operations.flock
		operations.flock = func(fd int, operation int) error {
			if operation&syscall.LOCK_EX != 0 {
				return injected
			}
			return originalFlock(fd, operation)
		}
		if _, err := openFileStore(directory, identity, voters, operations); !errors.Is(err, injected) {
			t.Fatalf("open error = %v, want injected", err)
		}
	})
	t.Run("close", func(t *testing.T) {
		directory := t.TempDir()
		store, err := OpenFileStore(directory, identity, voters)
		if err != nil {
			t.Fatal(err)
		}
		injected := errors.New("injected WAL close failure")
		originalClose := store.ops.close
		store.ops.close = func(file *os.File) error {
			err := originalClose(file)
			if filepath.Base(file.Name()) == RaftWALFilename {
				return errors.Join(err, injected)
			}
			return err
		}
		if err := store.Close(); !errors.Is(err, injected) {
			t.Fatalf("Close error = %v, want injected", err)
		}
		reopened, err := OpenFileStore(directory, identity, voters)
		if err != nil {
			t.Fatalf("lock remained held after close failure: %v", err)
		}
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestIdentityReadCloseFailurePreventsParticipation(t *testing.T) {
	directory := t.TempDir()
	identity, voters := testStorageIdentity(t, 1)
	store, err := OpenFileStore(directory, identity, voters)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected identity close failure")
	operations := defaultFileStoreOperations()
	originalClose := operations.close
	operations.close = func(file *os.File) error {
		err := originalClose(file)
		if filepath.Base(file.Name()) == RaftIdentityFilename {
			return errors.Join(err, injected)
		}
		return err
	}
	opened, err := openFileStore(directory, identity, voters, operations)
	if opened != nil {
		_ = opened.Close()
	}
	if !errors.Is(err, injected) {
		t.Fatalf("open error = %v, want identity close failure", err)
	}
}

func TestWALCreationBarrierFailureIsRepeatedOnRetry(t *testing.T) {
	identity, voters := testStorageIdentity(t, 1)
	for _, test := range []struct {
		name      string
		failPoint func(file *os.File, raftDirectory string, walSyncs, raftDirectorySyncs int) bool
	}{
		{
			name: "file sync",
			failPoint: func(file *os.File, _ string, walSyncs, _ int) bool {
				return filepath.Base(file.Name()) == RaftWALFilename && walSyncs == 1
			},
		},
		{
			name: "directory sync",
			failPoint: func(file *os.File, raftDirectory string, _ int, raftDirectorySyncs int) bool {
				return file.Name() == raftDirectory && raftDirectorySyncs == 2
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			raftDirectory := filepath.Join(directory, RaftStorageDirectoryName)
			injected := errors.New("injected WAL creation " + test.name + " failure")
			operations := defaultFileStoreOperations()
			originalSync := operations.sync
			walSyncs := 0
			raftDirectorySyncs := 0
			failed := false
			operations.sync = func(file *os.File) error {
				if filepath.Base(file.Name()) == RaftWALFilename {
					walSyncs++
				}
				if file.Name() == raftDirectory {
					raftDirectorySyncs++
				}
				if !failed && test.failPoint(file, raftDirectory, walSyncs, raftDirectorySyncs) {
					failed = true
					return injected
				}
				return originalSync(file)
			}
			if _, err := openFileStore(directory, identity, voters, operations); !errors.Is(err, injected) {
				t.Fatalf("first open error = %v, want injected", err)
			}
			walBefore := walSyncs
			directoryBefore := raftDirectorySyncs
			store, err := openFileStore(directory, identity, voters, operations)
			if err != nil {
				t.Fatalf("retry open: %v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if walSyncs <= walBefore || raftDirectorySyncs <= directoryBefore {
				t.Fatalf("retry barriers did not repeat: WAL %d→%d directory %d→%d", walBefore, walSyncs, directoryBefore, raftDirectorySyncs)
			}
		})
	}
}
