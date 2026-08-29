package swim

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

var (
	// ErrInvalidIncarnationState reports state that cannot be trusted after a
	// partial write, corruption, or manual modification.
	ErrInvalidIncarnationState = errors.New("invalid incarnation state")
	// ErrIncarnationRegression reports an attempt to replace durable state with
	// an older incarnation.
	ErrIncarnationRegression = errors.New("incarnation would decrease")
)

// IncarnationStateFilename is the durable SWIM identity file stored beneath a node's storage directory.
const IncarnationStateFilename = "swim.incarnation"

// IncarnationStore loads and monotonically persists one node identity's
// incarnation.
type IncarnationStore interface {
	Load() (uint64, error)
	Store(uint64) error
}

type fileOperations struct {
	createTemporary func(string, string) (*os.File, error)
	rename          func(string, string) error
	openDirectory   func(string) (*os.File, error)
}

// FileIncarnationStore persists one incarnation in a crash-safe text file.
type FileIncarnationStore struct {
	path string
	ops  fileOperations
	mu   sync.Mutex
}

// NewFileIncarnationStore returns a file-backed incarnation store. The parent
// directory must already exist.
func NewFileIncarnationStore(path string) *FileIncarnationStore {
	return newFileIncarnationStore(path, fileOperations{
		createTemporary: os.CreateTemp,
		rename:          os.Rename,
		openDirectory: func(path string) (*os.File, error) {
			return os.Open(path)
		},
	})
}

func newFileIncarnationStore(path string, operations fileOperations) *FileIncarnationStore {
	return &FileIncarnationStore{path: path, ops: operations}
}

// Load returns the durable incarnation. A missing state file represents no
// known incarnation and returns zero; malformed state is never treated as
// missing.
func (s *FileIncarnationStore) Load() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.load()
}

func (s *FileIncarnationStore) load() (uint64, error) {
	content, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read incarnation state %q: %w", s.path, err)
	}
	if len(content) < 2 || content[len(content)-1] != '\n' || bytes.Count(content, []byte{'\n'}) != 1 {
		return 0, fmt.Errorf("%w in %q: expected one decimal line", ErrInvalidIncarnationState, s.path)
	}
	digits := content[:len(content)-1]
	for _, digit := range digits {
		if digit < '0' || digit > '9' {
			return 0, fmt.Errorf("%w in %q: non-decimal byte", ErrInvalidIncarnationState, s.path)
		}
	}
	value, err := strconv.ParseUint(string(digits), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w in %q: %v", ErrInvalidIncarnationState, s.path, err)
	}
	return value, nil
}

// Store atomically replaces the durable incarnation after syncing the new
// file, then syncs the parent directory so the rename survives a crash.
func (s *FileIncarnationStore) Store(value uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	current, err := s.load()
	if err != nil {
		return err
	}
	if value < current {
		return fmt.Errorf("%w: current %d, proposed %d", ErrIncarnationRegression, current, value)
	}
	directory := filepath.Dir(s.path)
	pattern := "." + filepath.Base(s.path) + ".tmp-*"
	temporary, err := s.ops.createTemporary(directory, pattern)
	if err != nil {
		return fmt.Errorf("create temporary incarnation state in %q: %w", directory, err)
	}
	temporaryPath := temporary.Name()
	temporaryPresent := true
	defer func() {
		if temporaryPresent {
			_ = temporary.Close()
			_ = os.Remove(temporaryPath)
		}
	}()

	if _, err := io.WriteString(temporary, strconv.FormatUint(value, 10)+"\n"); err != nil {
		return fmt.Errorf("write temporary incarnation state %q: %w", temporaryPath, err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync temporary incarnation state %q: %w", temporaryPath, err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary incarnation state %q: %w", temporaryPath, err)
	}
	if err := s.ops.rename(temporaryPath, s.path); err != nil {
		return fmt.Errorf("replace incarnation state %q: %w", s.path, err)
	}
	temporaryPresent = false

	parent, err := s.ops.openDirectory(directory)
	if err != nil {
		return fmt.Errorf("open incarnation state directory %q: %w", directory, err)
	}
	if err := parent.Sync(); err != nil {
		_ = parent.Close()
		return fmt.Errorf("sync incarnation state directory %q: %w", directory, err)
	}
	if err := parent.Close(); err != nil {
		return fmt.Errorf("close incarnation state directory %q: %w", directory, err)
	}
	return nil
}
