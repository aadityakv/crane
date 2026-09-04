package swim

import (
	"crane/internal/testutil"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestFileIncarnationLoadMissingReturnsZero(t *testing.T) {
	store := NewFileIncarnationStore(filepath.Join(t.TempDir(), "incarnation"))

	got, err := store.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got != 0 {
		t.Fatalf("Load() = %d, want 0", got)
	}
}

func TestFileIncarnationStoreRoundTrip(t *testing.T) {
	store := NewFileIncarnationStore(filepath.Join(t.TempDir(), "incarnation"))

	if err := store.Store(8); err != nil {
		t.Fatalf("Store(8) error = %v", err)
	}
	got, err := store.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got != 8 {
		t.Fatalf("Load() = %d, want 8", got)
	}
}

func TestFileIncarnationLoadRejectsCorruptOrPartialState(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{name: "empty", content: ""},
		{name: "partial_missing_newline", content: "8"},
		{name: "non_decimal", content: "eight\n"},
		{name: "signed", content: "+8\n"},
		{name: "extra_line", content: "8\n9\n"},
		{name: "overflow", content: "18446744073709551616\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "incarnation")
			if err := os.WriteFile(path, []byte(tt.content), 0o600); err != nil {
				t.Fatal(err)
			}

			if _, err := NewFileIncarnationStore(path).Load(); !errors.Is(err, ErrInvalidIncarnationState) {
				t.Fatalf("Load() error = %v, want ErrInvalidIncarnationState", err)
			}
		})
	}
}

func TestFileIncarnationLoadRejectsFIFOWithoutBlocking(t *testing.T) {
	path := filepath.Join(t.TempDir(), "incarnation")
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}

	result := make(chan error, 1)
	go func() {
		_, err := NewFileIncarnationStore(path).Load()
		result <- err
	}()
	select {
	case err := <-result:
		if !errors.Is(err, ErrInvalidIncarnationState) {
			t.Fatalf("Load() error = %v, want ErrInvalidIncarnationState", err)
		}
	case <-time.After(testutil.Scale(250 * time.Millisecond)):
		// Release an implementation that incorrectly performs a blocking FIFO
		// read so the failed test does not strand a syscall during cleanup.
		writer, _ := os.OpenFile(path, os.O_WRONLY|syscall.O_NONBLOCK, 0)
		if writer != nil {
			_ = writer.Close()
		}
		t.Fatal("Load() blocked while opening a FIFO")
	}
}

func TestFileIncarnationLoadRejectsSymlink(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "target")
	if err := os.WriteFile(target, []byte("7\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "incarnation")
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}

	if _, err := NewFileIncarnationStore(path).Load(); !errors.Is(err, ErrInvalidIncarnationState) {
		t.Fatalf("Load() error = %v, want ErrInvalidIncarnationState", err)
	}
}

func TestFileIncarnationLoadRejectsPermissiveState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "incarnation")
	if err := os.WriteFile(path, []byte("7\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := NewFileIncarnationStore(path).Load(); !errors.Is(err, ErrInvalidIncarnationState) {
		t.Fatalf("Load() error = %v, want ErrInvalidIncarnationState", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o644 {
		t.Fatalf("Load() silently changed state mode to %o", got)
	}
}

func TestFileIncarnationLoadRejectsOversizedStateBeforeParsing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "incarnation")
	if err := os.WriteFile(path, []byte(strings.Repeat("1", 128)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := NewFileIncarnationStore(path).Load()
	if !errors.Is(err, ErrInvalidIncarnationState) || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("Load() error = %v, want bounded-size ErrInvalidIncarnationState", err)
	}
}

func TestFileIncarnationLoadRejectsDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "incarnation")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}

	if _, err := NewFileIncarnationStore(path).Load(); !errors.Is(err, ErrInvalidIncarnationState) {
		t.Fatalf("Load() error = %v, want ErrInvalidIncarnationState", err)
	}
}

func TestEnsureStorageDirectoryCreatesOwnerOnlyDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "node-1")
	if err := EnsureStorageDirectory(path); err != nil {
		t.Fatalf("EnsureStorageDirectory() error = %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("storage directory mode = %v, want directory 0700", info.Mode())
	}
}

func TestEnsureStorageDirectoryAcceptsSecureExistingDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node-1")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}

	if err := EnsureStorageDirectory(path); err != nil {
		t.Fatalf("EnsureStorageDirectory() error = %v", err)
	}
}

func TestEnsureStorageDirectoryRejectsUnsafeExistingPathWithoutRepair(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T, string)
	}{
		{
			name: "permissive_directory",
			setup: func(t *testing.T, path string) {
				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(path, 0o755); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "regular_file",
			setup: func(t *testing.T, path string) {
				if err := os.WriteFile(path, []byte("not a directory"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "symlink",
			setup: func(t *testing.T, path string) {
				target := filepath.Join(filepath.Dir(path), "target")
				if err := os.Mkdir(target, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "node-1")
			test.setup(t, path)
			if err := EnsureStorageDirectory(path); !errors.Is(err, ErrInvalidStorageDirectory) {
				t.Fatalf("EnsureStorageDirectory() error = %v, want ErrInvalidStorageDirectory", err)
			}
			if test.name == "permissive_directory" {
				info, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				if got := info.Mode().Perm(); got != 0o755 {
					t.Fatalf("unsafe existing permissions silently changed to %o", got)
				}
			}
		})
	}
}

func TestFileIncarnationStoreRejectsDecrease(t *testing.T) {
	store := NewFileIncarnationStore(filepath.Join(t.TempDir(), "incarnation"))
	if err := store.Store(8); err != nil {
		t.Fatal(err)
	}

	if err := store.Store(7); !errors.Is(err, ErrIncarnationRegression) {
		t.Fatalf("Store(7) error = %v, want ErrIncarnationRegression", err)
	}
	got, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got != 8 {
		t.Fatalf("Load() = %d after rejected decrease, want 8", got)
	}
}

func TestFileIncarnationStoreRefusesToOverwriteCorruptState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "incarnation")
	if err := os.WriteFile(path, []byte("broken\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := NewFileIncarnationStore(path).Store(9); !errors.Is(err, ErrInvalidIncarnationState) {
		t.Fatalf("Store(9) error = %v, want ErrInvalidIncarnationState", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "broken\n" {
		t.Fatalf("state after rejected store = %q, want original corrupt bytes", got)
	}
}

func TestFileIncarnationStoreRenameFailurePreservesPreviousValue(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "incarnation")
	base := NewFileIncarnationStore(path)
	if err := base.Store(8); err != nil {
		t.Fatal(err)
	}

	renameFailure := errors.New("injected rename failure")
	store := newFileIncarnationStore(path, fileOperations{
		createTemporary: os.CreateTemp,
		rename: func(string, string) error {
			return renameFailure
		},
		openDirectory: func(path string) (*os.File, error) {
			return os.Open(path)
		},
	})
	if err := store.Store(9); !errors.Is(err, renameFailure) {
		t.Fatalf("Store(9) error = %v, want injected failure", err)
	}
	got, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got != 8 {
		t.Fatalf("Load() after rename failure = %d, want 8", got)
	}
	temporary, err := filepath.Glob(filepath.Join(directory, ".incarnation.tmp-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(temporary) != 0 {
		t.Fatalf("temporary files after rename failure = %v, want none", temporary)
	}
}

func TestFileIncarnationStoreUsesSameDirectoryTemporaryFile(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "incarnation")
	var temporaryDirectory string
	store := newFileIncarnationStore(path, fileOperations{
		createTemporary: func(directory, pattern string) (*os.File, error) {
			temporaryDirectory = directory
			return os.CreateTemp(directory, pattern)
		},
		rename: os.Rename,
		openDirectory: func(path string) (*os.File, error) {
			return os.Open(path)
		},
	})

	if err := store.Store(1); err != nil {
		t.Fatal(err)
	}
	if temporaryDirectory != directory {
		t.Fatalf("temporary directory = %q, want %q", temporaryDirectory, directory)
	}
}

func TestFileIncarnationStoreDirectoryOpenFailureReturnsErrorWithoutCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "incarnation")
	directoryFailure := errors.New("injected directory open failure")
	store := newFileIncarnationStore(path, fileOperations{
		createTemporary: os.CreateTemp,
		rename:          os.Rename,
		openDirectory: func(string) (*os.File, error) {
			return nil, directoryFailure
		},
	})

	if err := store.Store(11); !errors.Is(err, directoryFailure) {
		t.Fatalf("Store(11) error = %v, want injected failure", err)
	}
	got, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got != 11 {
		t.Fatalf("Load() after directory failure = %d, want complete renamed value 11", got)
	}
}

func TestFileIncarnationStoreRetryAfterDirectoryFailureRepeatsDurabilityBarrier(t *testing.T) {
	path := filepath.Join(t.TempDir(), "incarnation")
	directoryFailure := errors.New("injected directory open failure")
	openCalls := 0
	store := newFileIncarnationStore(path, fileOperations{
		createTemporary: os.CreateTemp,
		rename:          os.Rename,
		openDirectory: func(directory string) (*os.File, error) {
			openCalls++
			if openCalls == 1 {
				return nil, directoryFailure
			}
			return os.Open(directory)
		},
	})

	if err := store.Store(11); !errors.Is(err, directoryFailure) {
		t.Fatalf("first Store(11) error = %v, want injected failure", err)
	}
	if err := store.Store(11); err != nil {
		t.Fatalf("retry Store(11) error = %v", err)
	}
	if openCalls != 2 {
		t.Fatalf("directory durability barriers = %d, want 2", openCalls)
	}
}

func TestFileIncarnationConcurrentStoresRetainMaximum(t *testing.T) {
	store := NewFileIncarnationStore(filepath.Join(t.TempDir(), "incarnation"))
	const highest = 64
	start := make(chan struct{})
	errorsSeen := make(chan error, highest)
	var workers sync.WaitGroup
	for value := uint64(1); value <= highest; value++ {
		workers.Add(1)
		go func(value uint64) {
			defer workers.Done()
			<-start
			errorsSeen <- store.Store(value)
		}(value)
	}
	close(start)
	workers.Wait()
	close(errorsSeen)

	for err := range errorsSeen {
		if err != nil && !errors.Is(err, ErrIncarnationRegression) {
			t.Fatalf("concurrent Store error = %v", err)
		}
	}
	got, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got != highest {
		t.Fatalf("Load() after concurrent stores = %d, want %d", got, highest)
	}
}

func TestFileIncarnationStoreAcceptsMaximumUint64(t *testing.T) {
	store := NewFileIncarnationStore(filepath.Join(t.TempDir(), "incarnation"))
	if err := store.Store(math.MaxUint64); err != nil {
		t.Fatalf("Store(MaxUint64) error = %v", err)
	}
	got, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got != math.MaxUint64 {
		t.Fatalf("Load() = %d, want %d", got, uint64(math.MaxUint64))
	}
}
