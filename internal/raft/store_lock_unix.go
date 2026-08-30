//go:build darwin || linux

package raft

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/aaditya/cs425mp3/internal/config"
)

const (
	// RaftStorageDirectoryName is the isolated directory below a node storage root.
	RaftStorageDirectoryName = "raft"
	// RaftLockFilename is held exclusively for the FileStore lifetime.
	RaftLockFilename = "lock"
	// RaftIdentityFilename binds durable state to one configured voter.
	RaftIdentityFilename = "identity"
	// RaftWALFilename contains committed persistence transactions.
	RaftWALFilename = "wal"
	// RaftSnapshotFilename reserves the Task 9 snapshot path.
	RaftSnapshotFilename         = "snapshot"
	raftSnapshotPreviousFilename = ".snapshot.previous"
	raftSnapshotStageFilename    = "snapshot-stage"

	identityBytes         = 60
	identityChecksumBytes = 4
)

var identityMagic = [4]byte{'R', 'I', 'D', '1'}

type fileStoreOperations struct {
	mkdir        func(string, os.FileMode) error
	lstat        func(string) (os.FileInfo, error)
	openFile     func(string, int, os.FileMode) (*os.File, error)
	openRoot     func(string) (*os.Root, error)
	rootLstat    func(*os.Root, string) (os.FileInfo, error)
	rootOpenFile func(*os.Root, string, int, os.FileMode) (*os.File, error)
	rootRename   func(*os.Root, string, string) error
	rootRemove   func(*os.Root, string) error
	closeRoot    func(*os.Root) error
	random       func([]byte) (int, error)
	write        func(*os.File, []byte) (int, error)
	sync         func(*os.File) error
	truncate     func(*os.File, int64) error
	close        func(*os.File) error
	flock        func(int, int) error
}

func defaultFileStoreOperations() fileStoreOperations {
	return fileStoreOperations{
		mkdir:    os.Mkdir,
		lstat:    os.Lstat,
		openFile: os.OpenFile,
		openRoot: os.OpenRoot,
		rootLstat: func(root *os.Root, name string) (os.FileInfo, error) {
			return root.Lstat(name)
		},
		rootOpenFile: func(root *os.Root, name string, flags int, mode os.FileMode) (*os.File, error) {
			return root.OpenFile(name, flags, mode)
		},
		rootRename: func(root *os.Root, oldName, newName string) error {
			return root.Rename(oldName, newName)
		},
		rootRemove: func(root *os.Root, name string) error { return root.Remove(name) },
		closeRoot:  func(root *os.Root) error { return root.Close() },
		random:     rand.Read,
		write: func(file *os.File, content []byte) (int, error) {
			return file.Write(content)
		},
		sync: func(file *os.File) error { return file.Sync() },
		truncate: func(file *os.File, size int64) error {
			return file.Truncate(size)
		},
		close: func(file *os.File) error { return file.Close() },
		flock: syscall.Flock,
	}
}

// FileStore is an identity-bound, lifetime-locked, checksummed WAL StableStore.
type FileStore struct {
	mu            sync.Mutex
	identity      StorageIdentity
	voters        VoterSet
	directoryPath string
	directory     *os.File
	root          *os.Root
	lock          *os.File
	lockHeld      bool
	wal           *os.File
	ops           fileStoreOperations
	state         RecoveredState
	nextTxnID     uint64
	closed        bool
	poisoned      bool
	stage         *fileSnapshotStage
}

type fileSnapshotStage struct {
	metadata   snapshotTransferMetadata
	file       *os.File
	nextOffset uint64
}

// OpenFileStore opens or securely initializes <storageDir>/raft and recovers
// only complete committed WAL transactions.
func OpenFileStore(storageDir string, identity StorageIdentity, voters VoterSet) (*FileStore, error) {
	return openFileStore(storageDir, identity, voters, defaultFileStoreOperations())
}

// NewFileStore is an alias for OpenFileStore.
func NewFileStore(storageDir string, identity StorageIdentity, voters VoterSet) (*FileStore, error) {
	return OpenFileStore(storageDir, identity, voters)
}

func openFileStore(storageDir string, identity StorageIdentity, voters VoterSet, operations fileStoreOperations) (_ *FileStore, resultErr error) {
	if storageDir == "" || filepath.Clean(storageDir) == string(filepath.Separator) {
		return nil, fmt.Errorf("%w: unsafe storage root %q", ErrStorageCorrupt, storageDir)
	}
	if err := ValidateRecoveredState(RecoveredState{Identity: identity}, identity, voters); err != nil {
		return nil, err
	}
	directoryPath := filepath.Join(storageDir, RaftStorageDirectoryName)
	if err := operations.mkdir(directoryPath, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("create raft directory %q: %w", directoryPath, err)
	}
	parentDirectory, err := operations.openFile(storageDir, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, fmt.Errorf("open raft parent directory %q: %w", storageDir, err)
	}
	parentInfo, statErr := parentDirectory.Stat()
	if statErr != nil || !parentInfo.IsDir() {
		_ = operations.close(parentDirectory)
		return nil, fmt.Errorf("%w: raft parent %q must be a real directory: %v", ErrStorageCorrupt, storageDir, statErr)
	}
	if err := operations.sync(parentDirectory); err != nil {
		_ = operations.close(parentDirectory)
		return nil, fmt.Errorf("sync raft parent directory %q: %w", storageDir, err)
	}
	if err := operations.close(parentDirectory); err != nil {
		return nil, fmt.Errorf("close raft parent directory %q: %w", storageDir, err)
	}
	directory, err := operations.openFile(directoryPath, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, fmt.Errorf("open raft directory %q without following links: %w", directoryPath, err)
	}
	store := &FileStore{
		identity:      identity,
		voters:        voters,
		directoryPath: directoryPath,
		directory:     directory,
		ops:           operations,
		state:         RecoveredState{Identity: identity},
	}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, store.releaseOpenResources())
		}
	}()
	if err := validateOpenedDirectory(directory, directoryPath); err != nil {
		return nil, err
	}
	directoryInfo, err := directory.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat anchored raft directory %q: %w", directoryPath, err)
	}
	pathInfo, err := operations.lstat(directoryPath)
	if err != nil {
		return nil, fmt.Errorf("lstat raft directory %q after open: %w", directoryPath, err)
	}
	if err := validateDirectoryInfo(pathInfo, directoryPath); err != nil {
		return nil, err
	}
	if !os.SameFile(directoryInfo, pathInfo) {
		return nil, fmt.Errorf("%w: raft directory %q changed while opening", ErrStorageCorrupt, directoryPath)
	}
	root, err := operations.openRoot(directoryPath)
	if err != nil {
		return nil, fmt.Errorf("anchor raft directory %q: %w", directoryPath, err)
	}
	store.root = root
	rootDirectory, err := operations.rootOpenFile(root, ".", os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, fmt.Errorf("open anchored raft directory %q: %w", directoryPath, err)
	}
	rootDirectoryInfo, statErr := rootDirectory.Stat()
	closeErr := operations.close(rootDirectory)
	if statErr != nil {
		return nil, fmt.Errorf("stat anchored raft root %q: %w", directoryPath, statErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close anchored raft root %q: %w", directoryPath, closeErr)
	}
	if err := validateDirectoryInfo(rootDirectoryInfo, directoryPath); err != nil {
		return nil, err
	}
	if !os.SameFile(directoryInfo, rootDirectoryInfo) {
		return nil, fmt.Errorf("%w: raft directory %q changed before anchoring", ErrStorageCorrupt, directoryPath)
	}

	lockPath := filepath.Join(directoryPath, RaftLockFilename)
	lock, _, err := store.openAnchoredRegularFile(RaftLockFilename, os.O_RDWR, true)
	if err != nil {
		return nil, fmt.Errorf("open raft lock %q: %w", lockPath, err)
	}
	store.lock = lock
	if err := operations.flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, fmt.Errorf("%w: %q", ErrStorageLocked, directoryPath)
		}
		return nil, fmt.Errorf("acquire raft lock %q: %w", lockPath, err)
	}
	store.lockHeld = true

	if err := store.loadOrCreateIdentity(); err != nil {
		return nil, err
	}
	snapshot, err := store.loadSnapshotFile(RaftSnapshotFilename)
	if err != nil {
		return nil, err
	}
	previousSnapshot, err := store.loadSnapshotFile(raftSnapshotPreviousFilename)
	if err != nil {
		return nil, err
	}
	wal, err := store.openOrCreateWAL()
	if err != nil {
		return nil, err
	}
	store.wal = wal
	state, lastTransaction, err := store.recoverWAL()
	if err != nil {
		return nil, err
	}
	state, err = store.selectRecoveredSnapshot(state, snapshot, previousSnapshot)
	if err != nil {
		return nil, err
	}
	store.state = state
	if lastTransaction == math.MaxUint64 {
		store.nextTxnID = 0
	} else {
		store.nextTxnID = lastTransaction + 1
	}
	return store, nil
}

// PersistSnapshot writes and syncs the canonical snapshot before atomically
// replacing the WAL with the exact compacted base and retained suffix.
func (store *FileStore) PersistSnapshot(snapshot Snapshot) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrStoreClosed
	}
	if store.poisoned {
		return fmt.Errorf("%w: persistence outcome requires reopen", ErrStorageCorrupt)
	}
	prospective, err := compactRecoveredState(store.state, snapshot, store.identity, store.voters)
	if err != nil {
		return err
	}
	return store.persistSnapshotStateLocked(snapshot, prospective)
}

func (store *FileStore) persistSnapshotStateLocked(snapshot Snapshot, prospective RecoveredState) error {
	snapshotTemporary, err := store.newTemporaryRegularFile(".snapshot.tmp-")
	if err != nil {
		return fmt.Errorf("create temporary raft snapshot: %w", err)
	}
	snapshotName, snapshotFile := snapshotTemporary.name, snapshotTemporary.file
	removeSnapshotTemporary := true
	defer func() {
		if snapshotFile != nil {
			_ = store.ops.close(snapshotFile)
		}
		if removeSnapshotTemporary {
			_ = store.ops.rootRemove(store.root, snapshotName)
		}
	}()
	if err := writeAll(snapshot.EnvelopeBytes(), func(content []byte) (int, error) { return store.ops.write(snapshotFile, content) }); err != nil {
		store.poisoned = true
		return fmt.Errorf("write temporary raft snapshot: %w", err)
	}
	if err := store.ops.sync(snapshotFile); err != nil {
		store.poisoned = true
		return fmt.Errorf("sync temporary raft snapshot: %w", err)
	}
	if err := store.ops.close(snapshotFile); err != nil {
		snapshotFile = nil
		store.poisoned = true
		return fmt.Errorf("close temporary raft snapshot: %w", err)
	}
	snapshotFile = nil
	hadPreviousSnapshot := store.state.Snapshot != nil
	if hadPreviousSnapshot {
		if err := store.ops.rootRename(store.root, RaftSnapshotFilename, raftSnapshotPreviousFilename); err != nil {
			store.poisoned = true
			return fmt.Errorf("preserve previous raft snapshot: %w", err)
		}
		if err := store.ops.sync(store.directory); err != nil {
			store.poisoned = true
			return fmt.Errorf("sync raft directory after preserving previous snapshot: %w", err)
		}
	}
	if err := store.ops.rootRename(store.root, snapshotName, RaftSnapshotFilename); err != nil {
		store.poisoned = true
		return fmt.Errorf("install raft snapshot: %w", err)
	}
	removeSnapshotTemporary = false
	if err := store.ops.sync(store.directory); err != nil {
		store.poisoned = true
		return fmt.Errorf("sync raft directory after snapshot install: %w", err)
	}

	replacementBatch, err := replacementBatchForState(prospective)
	if err != nil {
		store.poisoned = true
		return err
	}
	replacement, err := encodeWALTransaction(1, replacementBatch)
	if err != nil {
		store.poisoned = true
		return err
	}
	walTemporary, err := store.newTemporaryRegularFile(".wal.tmp-")
	if err != nil {
		store.poisoned = true
		return fmt.Errorf("create replacement raft WAL: %w", err)
	}
	walName, newWAL := walTemporary.name, walTemporary.file
	removeWALTemporary := true
	defer func() {
		if newWAL != nil {
			_ = store.ops.close(newWAL)
		}
		if removeWALTemporary {
			_ = store.ops.rootRemove(store.root, walName)
		}
	}()
	if err := writeAll(replacement, func(content []byte) (int, error) { return store.ops.write(newWAL, content) }); err != nil {
		store.poisoned = true
		return fmt.Errorf("write replacement raft WAL: %w", err)
	}
	if err := store.ops.sync(newWAL); err != nil {
		store.poisoned = true
		return fmt.Errorf("sync replacement raft WAL: %w", err)
	}
	if err := store.ops.rootRename(store.root, walName, RaftWALFilename); err != nil {
		store.poisoned = true
		return fmt.Errorf("install replacement raft WAL: %w", err)
	}
	removeWALTemporary = false
	if err := store.ops.sync(store.directory); err != nil {
		store.poisoned = true
		return fmt.Errorf("sync raft directory after WAL replacement: %w", err)
	}
	oldWAL := store.wal
	store.wal = newWAL
	newWAL = nil
	if err := store.ops.close(oldWAL); err != nil {
		store.poisoned = true
		return fmt.Errorf("close replaced raft WAL: %w", err)
	}
	if hadPreviousSnapshot {
		if err := store.ops.rootRemove(store.root, raftSnapshotPreviousFilename); err != nil {
			store.poisoned = true
			return fmt.Errorf("remove previous raft snapshot: %w", err)
		}
		if err := store.ops.sync(store.directory); err != nil {
			store.poisoned = true
			return fmt.Errorf("sync raft directory after previous snapshot removal: %w", err)
		}
	}
	store.state = prospective
	store.nextTxnID = 2
	return nil
}

// StageSnapshotChunk writes, syncs, and only then reports one exact follower offset.
func (store *FileStore) StageSnapshotChunk(request InstallSnapshotRequest) (SnapshotStageResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return SnapshotStageResult{}, ErrStoreClosed
	}
	if store.poisoned {
		return SnapshotStageResult{}, fmt.Errorf("%w: persistence outcome requires reopen", ErrStorageCorrupt)
	}
	if err := validateSnapshotRequest(request, DefaultCodecLimits()); err != nil {
		return SnapshotStageResult{}, store.rejectFileStageLocked(fmt.Errorf("%w: %v", ErrSnapshotRejected, err))
	}
	metadata := snapshotTransferMetadataFor(request)
	if store.stage == nil {
		if request.Offset != 0 {
			return SnapshotStageResult{}, fmt.Errorf("%w: transfer must begin at offset zero", ErrSnapshotRejected)
		}
		file, created, err := store.openAnchoredRegularFile(raftSnapshotStageFilename, os.O_RDWR, true)
		if err != nil {
			return SnapshotStageResult{}, fmt.Errorf("create raft snapshot stage: %w", err)
		}
		if !created {
			_ = store.ops.close(file)
			return SnapshotStageResult{}, fmt.Errorf("%w: unexpected existing snapshot stage", ErrStorageCorrupt)
		}
		if err := store.ops.sync(store.directory); err != nil {
			_ = store.ops.close(file)
			_ = store.ops.rootRemove(store.root, raftSnapshotStageFilename)
			return SnapshotStageResult{}, fmt.Errorf("sync raft directory after stage creation: %w", err)
		}
		store.stage = &fileSnapshotStage{metadata: metadata, file: file}
	} else if store.stage.metadata != metadata {
		return SnapshotStageResult{}, store.rejectFileStageLocked(fmt.Errorf("%w: transfer metadata changed", ErrSnapshotRejected))
	}

	nextOffset := store.stage.nextOffset
	end := request.Offset + uint64(len(request.Chunk))
	switch {
	case request.Offset == nextOffset:
		if _, err := store.stage.file.Seek(int64(nextOffset), io.SeekStart); err != nil {
			store.poisoned = true
			return SnapshotStageResult{}, fmt.Errorf("seek raft snapshot stage: %w", err)
		}
		if err := writeAll(request.Chunk, func(content []byte) (int, error) { return store.ops.write(store.stage.file, content) }); err != nil {
			store.poisoned = true
			return SnapshotStageResult{}, fmt.Errorf("write raft snapshot stage at %d: %w", request.Offset, err)
		}
		if err := store.ops.sync(store.stage.file); err != nil {
			store.poisoned = true
			return SnapshotStageResult{}, fmt.Errorf("sync raft snapshot stage through %d: %w", end, err)
		}
		store.stage.nextOffset = end
		nextOffset = end
	case end <= nextOffset:
		if end > uint64(math.MaxInt) {
			return SnapshotStageResult{}, store.rejectFileStageLocked(fmt.Errorf("%w: duplicate offset exceeds local integer domain", ErrSnapshotRejected))
		}
		staged := make([]byte, len(request.Chunk))
		if _, err := store.stage.file.ReadAt(staged, int64(request.Offset)); err != nil {
			store.poisoned = true
			return SnapshotStageResult{}, fmt.Errorf("read duplicate raft snapshot bytes: %w", err)
		}
		if !bytes.Equal(staged, request.Chunk) {
			return SnapshotStageResult{}, store.rejectFileStageLocked(fmt.Errorf("%w: duplicate bytes changed", ErrSnapshotRejected))
		}
	case request.Offset < nextOffset && end > nextOffset:
		return SnapshotStageResult{}, store.rejectFileStageLocked(fmt.Errorf("%w: chunk partially overlaps durable bytes", ErrSnapshotRejected))
	default:
		return SnapshotStageResult{}, store.rejectFileStageLocked(fmt.Errorf("%w: chunk leaves an offset gap", ErrSnapshotRejected))
	}
	if !request.Done {
		return SnapshotStageResult{NextOffset: nextOffset}, nil
	}
	if nextOffset > uint64(math.MaxInt) {
		return SnapshotStageResult{}, store.rejectFileStageLocked(fmt.Errorf("%w: complete stage exceeds local integer domain", ErrSnapshotRejected))
	}
	stateBytes := make([]byte, int(nextOffset))
	if len(stateBytes) != 0 {
		if _, err := store.stage.file.ReadAt(stateBytes, 0); err != nil {
			store.poisoned = true
			return SnapshotStageResult{}, fmt.Errorf("read complete raft snapshot stage: %w", err)
		}
	}
	checksum := sha256.Sum256(stateBytes)
	if SnapshotChecksum(checksum) != request.Checksum {
		return SnapshotStageResult{}, store.rejectFileStageLocked(fmt.Errorf("%w: complete state checksum mismatch", ErrSnapshotRejected))
	}
	snapshot, err := NewSnapshot(store.identity, metadata.metadata, stateBytes, config.MaxRaftSnapshotBytes)
	if err != nil || snapshot.ID != request.SnapshotID {
		return SnapshotStageResult{}, store.rejectFileStageLocked(fmt.Errorf("%w: complete snapshot identity mismatch: %v", ErrSnapshotRejected, err))
	}
	prospective, err := installRecoveredSnapshot(store.state, snapshot, store.identity, store.voters)
	if err != nil {
		return SnapshotStageResult{}, store.rejectFileStageLocked(err)
	}
	stageFile := store.stage.file
	store.stage = nil
	if err := store.ops.close(stageFile); err != nil {
		store.poisoned = true
		return SnapshotStageResult{}, fmt.Errorf("close complete raft snapshot stage: %w", err)
	}
	if err := store.persistSnapshotStateLocked(snapshot, prospective); err != nil {
		return SnapshotStageResult{}, err
	}
	if err := store.ops.rootRemove(store.root, raftSnapshotStageFilename); err != nil {
		store.poisoned = true
		return SnapshotStageResult{}, fmt.Errorf("remove installed raft snapshot stage: %w", err)
	}
	if err := store.ops.sync(store.directory); err != nil {
		store.poisoned = true
		return SnapshotStageResult{}, fmt.Errorf("sync raft directory after stage removal: %w", err)
	}
	return SnapshotStageResult{NextOffset: nextOffset, Done: true, State: prospective.Clone()}, nil
}

// AbortSnapshotStage discards one incomplete rooted file before a transfer changes.
func (store *FileStore) AbortSnapshotStage() error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrStoreClosed
	}
	return store.abortFileStageLocked()
}

func (store *FileStore) rejectFileStageLocked(reason error) error {
	cleanupErr := store.abortFileStageLocked()
	return errors.Join(reason, cleanupErr)
}

func (store *FileStore) abortFileStageLocked() error {
	if store.stage == nil {
		return nil
	}
	stage := store.stage
	store.stage = nil
	closeErr := store.ops.close(stage.file)
	removeErr := store.ops.rootRemove(store.root, raftSnapshotStageFilename)
	var syncErr error
	if removeErr == nil {
		syncErr = store.ops.sync(store.directory)
	}
	if closeErr != nil || removeErr != nil || syncErr != nil {
		return fmt.Errorf("clean raft snapshot stage: %w", errors.Join(closeErr, removeErr, syncErr))
	}
	return nil
}

// Recover returns an independently owned copy of the checked durable state.
func (store *FileStore) Recover() (RecoveredState, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return RecoveredState{}, ErrStoreClosed
	}
	return store.state.Clone(), nil
}

// RetainedWALBytes returns the current replacement/append WAL size.
func (store *FileStore) RetainedWALBytes() (uint64, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return 0, ErrStoreClosed
	}
	info, err := store.wal.Stat()
	if err != nil {
		return 0, err
	}
	if info.Size() < 0 {
		return 0, fmt.Errorf("%w: negative WAL size", ErrStorageCorrupt)
	}
	return uint64(info.Size()), nil
}

// Persist appends, syncs, and only then acknowledges one complete transaction.
func (store *FileStore) Persist(batch PersistenceBatch) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrStoreClosed
	}
	if store.poisoned {
		return fmt.Errorf("%w: persistence outcome requires reopen", ErrStorageCorrupt)
	}
	if err := validatePersistenceBatchBounds(batch); err != nil {
		return err
	}
	if store.nextTxnID == 0 {
		return fmt.Errorf("%w: WAL transaction ID exhausted", ErrInvalidStorageState)
	}
	prospective, err := applyValidatedPersistenceBatch(store.state, batch, store.identity, store.voters)
	if err != nil {
		return err
	}
	encoded, err := encodeWALTransaction(store.nextTxnID, batch)
	if err != nil {
		return err
	}
	if _, err := store.wal.Seek(0, io.SeekEnd); err != nil {
		store.poisoned = true
		return fmt.Errorf("seek raft WAL for transaction %d: %w", store.nextTxnID, err)
	}
	if err := writeAll(encoded, func(content []byte) (int, error) { return store.ops.write(store.wal, content) }); err != nil {
		store.poisoned = true
		return fmt.Errorf("write raft WAL transaction %d: %w", store.nextTxnID, err)
	}
	if err := store.ops.sync(store.wal); err != nil {
		store.poisoned = true
		return fmt.Errorf("sync raft WAL transaction %d: %w", store.nextTxnID, err)
	}
	store.state = prospective
	if store.nextTxnID == math.MaxUint64 {
		store.nextTxnID = 0
	} else {
		store.nextTxnID++
	}
	return nil
}

// Close attempts every release once and reports any close or unlock failure.
func (store *FileStore) Close() error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrStoreClosed
	}
	store.closed = true
	return store.releaseOpenResources()
}

func (store *FileStore) releaseOpenResources() error {
	var failures []error
	if store.stage != nil {
		if err := store.abortFileStageLocked(); err != nil {
			failures = append(failures, err)
		}
	}
	if store.wal != nil {
		if err := store.ops.close(store.wal); err != nil {
			failures = append(failures, fmt.Errorf("close raft WAL: %w", err))
		}
		store.wal = nil
	}
	if store.lock != nil {
		if store.lockHeld {
			if err := store.ops.flock(int(store.lock.Fd()), syscall.LOCK_UN); err != nil {
				failures = append(failures, fmt.Errorf("unlock raft store: %w", err))
			}
			store.lockHeld = false
		}
		if err := store.ops.close(store.lock); err != nil {
			failures = append(failures, fmt.Errorf("close raft lock: %w", err))
		}
		store.lock = nil
	}
	if store.root != nil {
		if err := store.ops.closeRoot(store.root); err != nil {
			failures = append(failures, fmt.Errorf("close anchored raft root: %w", err))
		}
		store.root = nil
	}
	if store.directory != nil {
		if err := store.ops.close(store.directory); err != nil {
			failures = append(failures, fmt.Errorf("close raft directory: %w", err))
		}
		store.directory = nil
	}
	return errors.Join(failures...)
}

func (store *FileStore) loadOrCreateIdentity() (resultErr error) {
	directory, err := store.ops.rootOpenFile(store.root, ".", os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return fmt.Errorf("open anchored raft directory for enumeration: %w", err)
	}
	entries, readErr := directory.ReadDir(-1)
	closeErr := store.ops.close(directory)
	if readErr != nil {
		return fmt.Errorf("list raft directory %q: %w", store.directoryPath, readErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close raft directory enumeration %q: %w", store.directoryPath, closeErr)
	}
	names := make(map[string]bool, len(entries))
	for _, entry := range entries {
		names[entry.Name()] = true
	}
	if !names[RaftIdentityFilename] {
		if len(names) != 1 || !names[RaftLockFilename] {
			return fmt.Errorf("%w: missing identity in nonempty raft directory", ErrStorageCorrupt)
		}
		return store.createIdentity()
	}
	removedTemporary := false
	for name := range names {
		if !validStoreTemporaryName(name) && name != raftSnapshotStageFilename {
			continue
		}
		info, err := store.ops.rootLstat(store.root, name)
		if err != nil {
			return fmt.Errorf("lstat stale raft temporary %q: %w", name, err)
		}
		if err := validateRegularFileInfo(info, filepath.Join(store.directoryPath, name)); err != nil {
			return err
		}
		if err := store.ops.rootRemove(store.root, name); err != nil {
			return fmt.Errorf("remove stale raft temporary %q: %w", name, err)
		}
		delete(names, name)
		removedTemporary = true
	}
	if removedTemporary {
		if err := store.ops.sync(store.directory); err != nil {
			return fmt.Errorf("sync raft directory after temporary cleanup: %w", err)
		}
	}
	for name := range names {
		if name != RaftLockFilename && name != RaftIdentityFilename && name != RaftWALFilename && name != RaftSnapshotFilename && name != raftSnapshotPreviousFilename {
			return fmt.Errorf("%w: unexpected raft directory entry %q", ErrStorageCorrupt, name)
		}
	}
	identityPath := filepath.Join(store.directoryPath, RaftIdentityFilename)
	file, _, err := store.openAnchoredRegularFile(RaftIdentityFilename, os.O_RDONLY, false)
	if err != nil {
		return fmt.Errorf("open raft identity %q: %w", identityPath, err)
	}
	defer func() {
		if file != nil {
			resultErr = errors.Join(resultErr, store.ops.close(file))
		}
	}()
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat raft identity %q: %w", identityPath, err)
	}
	if info.Size() != identityBytes {
		return fmt.Errorf("%w: identity size=%d want=%d", ErrStorageCorrupt, info.Size(), identityBytes)
	}
	content, err := io.ReadAll(io.LimitReader(file, identityBytes+1))
	if err != nil {
		return fmt.Errorf("read raft identity %q: %w", identityPath, err)
	}
	persisted, err := decodeStorageIdentity(content)
	if err != nil {
		return err
	}
	if err := store.ops.close(file); err != nil {
		file = nil
		return fmt.Errorf("close raft identity %q: %w", identityPath, err)
	}
	file = nil
	if persisted != store.identity {
		return fmt.Errorf("%w: persisted format=%d cluster=%x local=%d voters=%x", ErrStorageIdentityMismatch, persisted.FormatVersion, persisted.ClusterID, persisted.LocalVoterID, persisted.VoterFingerprint)
	}
	return nil
}

func validStoreTemporaryName(name string) bool {
	for _, prefix := range []string{".snapshot.tmp-", ".wal.tmp-"} {
		if !strings.HasPrefix(name, prefix) || len(name) != len(prefix)+32 {
			continue
		}
		_, err := hex.DecodeString(name[len(prefix):])
		return err == nil
	}
	return false
}

type temporaryRegularFile struct {
	name string
	file *os.File
}

func (store *FileStore) newTemporaryRegularFile(prefix string) (temporaryRegularFile, error) {
	var suffix [16]byte
	if read, err := store.ops.random(suffix[:]); err != nil || read != len(suffix) {
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		return temporaryRegularFile{}, err
	}
	name := prefix + hex.EncodeToString(suffix[:])
	file, err := store.ops.rootOpenFile(store.root, name, os.O_RDWR|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0o600)
	if err != nil {
		return temporaryRegularFile{}, err
	}
	if err := validateOpenedRegularFile(file, name); err != nil {
		return temporaryRegularFile{}, errors.Join(err, store.ops.close(file))
	}
	return temporaryRegularFile{name: name, file: file}, nil
}

func (store *FileStore) loadSnapshotFile(name string) (*Snapshot, error) {
	info, err := store.ops.rootLstat(store.root, name)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lstat raft snapshot: %w", err)
	}
	if err := validateRegularFileInfo(info, filepath.Join(store.directoryPath, name)); err != nil {
		return nil, err
	}
	maximumEnvelopeBytes := uint64(snapshotEnvelopeHeaderBytes) + config.MaxRaftSnapshotBytes
	if info.Size() < 0 || uint64(info.Size()) > maximumEnvelopeBytes {
		return nil, fmt.Errorf("%w: snapshot file size %d exceeds maximum", ErrStorageCorrupt, info.Size())
	}
	file, _, err := store.openAnchoredRegularFile(name, os.O_RDONLY, false)
	if err != nil {
		return nil, fmt.Errorf("open raft snapshot: %w", err)
	}
	content, readErr := io.ReadAll(io.LimitReader(file, int64(maximumEnvelopeBytes)+1))
	closeErr := store.ops.close(file)
	if readErr != nil {
		return nil, fmt.Errorf("read raft snapshot: %w", readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close raft snapshot: %w", closeErr)
	}
	snapshot, err := DecodeSnapshotEnvelope(content, store.identity, config.MaxRaftSnapshotBytes)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrStorageCorrupt, err)
	}
	return &snapshot, nil
}

func (store *FileStore) reconcileRecoveredSnapshot(state RecoveredState, snapshot *Snapshot) (RecoveredState, error) {
	if snapshot == nil {
		if state.SnapshotBase.LastIncludedIndex != 0 {
			return RecoveredState{}, fmt.Errorf("%w: WAL snapshot base has no snapshot bytes", ErrStorageCorrupt)
		}
		return state.Clone(), nil
	}
	if snapshot.Metadata.LastIncludedIndex < state.SnapshotBase.LastIncludedIndex {
		return RecoveredState{}, fmt.Errorf("%w: snapshot bytes are older than WAL base", ErrStorageCorrupt)
	}
	if snapshot.Metadata.LastIncludedIndex == state.SnapshotBase.LastIncludedIndex {
		if snapshot.Metadata != state.SnapshotBase {
			return RecoveredState{}, fmt.Errorf("%w: snapshot metadata differs from WAL base", ErrStorageCorrupt)
		}
		owned := snapshot.Clone()
		state.Snapshot = &owned
		if state.AppliedIndex < snapshot.Metadata.LastIncludedIndex {
			state.AppliedIndex = snapshot.Metadata.LastIncludedIndex
		}
	} else {
		var err error
		state, err = compactRecoveredState(state, snapshot.Clone(), store.identity, store.voters)
		if err != nil {
			return RecoveredState{}, fmt.Errorf("%w: reconcile snapshot with fuller WAL: %v", ErrStorageCorrupt, err)
		}
	}
	if err := ValidateRecoveredState(state, store.identity, store.voters); err != nil {
		return RecoveredState{}, fmt.Errorf("%w: recovered snapshot state: %v", ErrStorageCorrupt, err)
	}
	return state.Clone(), nil
}

func (store *FileStore) selectRecoveredSnapshot(state RecoveredState, current, previous *Snapshot) (RecoveredState, error) {
	if current != nil {
		if recovered, err := store.reconcileRecoveredSnapshot(state, current); err == nil {
			if previous != nil {
				if err := store.ops.rootRemove(store.root, raftSnapshotPreviousFilename); err != nil {
					return RecoveredState{}, fmt.Errorf("remove obsolete previous raft snapshot: %w", err)
				}
				if err := store.ops.sync(store.directory); err != nil {
					return RecoveredState{}, fmt.Errorf("sync raft directory after previous snapshot cleanup: %w", err)
				}
			}
			return recovered, nil
		}
	}
	if previous != nil {
		if recovered, err := store.reconcileRecoveredSnapshot(state, previous); err == nil {
			if current != nil {
				if err := store.ops.rootRemove(store.root, RaftSnapshotFilename); err != nil {
					return RecoveredState{}, fmt.Errorf("remove interrupted raft snapshot: %w", err)
				}
			}
			if err := store.ops.rootRename(store.root, raftSnapshotPreviousFilename, RaftSnapshotFilename); err != nil {
				return RecoveredState{}, fmt.Errorf("restore previous raft snapshot: %w", err)
			}
			if err := store.ops.sync(store.directory); err != nil {
				return RecoveredState{}, fmt.Errorf("sync raft directory after restoring previous snapshot: %w", err)
			}
			return recovered, nil
		}
	}
	if state.SnapshotBase.LastIncludedIndex == 0 {
		removed := false
		for _, candidate := range []struct {
			name     string
			snapshot *Snapshot
		}{{RaftSnapshotFilename, current}, {raftSnapshotPreviousFilename, previous}} {
			if candidate.snapshot == nil {
				continue
			}
			if err := store.ops.rootRemove(store.root, candidate.name); err != nil {
				return RecoveredState{}, fmt.Errorf("remove interrupted raft snapshot %q: %w", candidate.name, err)
			}
			removed = true
		}
		if removed {
			if err := store.ops.sync(store.directory); err != nil {
				return RecoveredState{}, fmt.Errorf("sync raft directory after interrupted snapshot cleanup: %w", err)
			}
		}
		return state.Clone(), nil
	}
	return RecoveredState{}, fmt.Errorf("%w: no snapshot file matches recovered WAL base", ErrStorageCorrupt)
}

func replacementBatchForState(state RecoveredState) (PersistenceBatch, error) {
	if state.Snapshot == nil || state.Snapshot.Metadata != state.SnapshotBase {
		return PersistenceBatch{}, fmt.Errorf("%w: replacement WAL requires exact snapshot payload", ErrInvalidStorageState)
	}
	hardState := state.HardState
	base := state.SnapshotBase
	applied := state.AppliedIndex
	snapshot := state.Snapshot.Clone()
	batch := PersistenceBatch{HardState: &hardState, SnapshotBase: &base, Snapshot: &snapshot, AppliedIndex: &applied}
	if len(state.Entries) != 0 {
		next, ok := checkedNextIndex(base.LastIncludedIndex)
		if !ok {
			return PersistenceBatch{}, ErrLogOverflow
		}
		batch.ReplaceFrom = next
		batch.Entries = cloneEntries(state.Entries)
	}
	return batch, nil
}

func (store *FileStore) createIdentity() (resultErr error) {
	var suffix [16]byte
	if read, err := store.ops.random(suffix[:]); err != nil || read != len(suffix) {
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		return fmt.Errorf("generate temporary raft identity name: %w", err)
	}
	temporaryName := ".identity.tmp-" + hex.EncodeToString(suffix[:])
	temporary, err := store.ops.rootOpenFile(store.root, temporaryName, os.O_RDWR|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0o600)
	if err != nil {
		return fmt.Errorf("create temporary raft identity: %w", err)
	}
	removeTemporary := true
	defer func() {
		if temporary != nil {
			resultErr = errors.Join(resultErr, store.ops.close(temporary))
		}
		if removeTemporary {
			_ = store.ops.rootRemove(store.root, temporaryName)
		}
	}()
	if err := validateOpenedRegularFile(temporary, temporaryName); err != nil {
		return err
	}
	if err := writeAll(encodeStorageIdentity(store.identity), func(content []byte) (int, error) { return store.ops.write(temporary, content) }); err != nil {
		return fmt.Errorf("write temporary raft identity: %w", err)
	}
	if err := store.ops.sync(temporary); err != nil {
		return fmt.Errorf("sync temporary raft identity: %w", err)
	}
	if err := store.ops.close(temporary); err != nil {
		temporary = nil
		return fmt.Errorf("close temporary raft identity: %w", err)
	}
	temporary = nil
	if err := store.ops.rootRename(store.root, temporaryName, RaftIdentityFilename); err != nil {
		return fmt.Errorf("install raft identity: %w", err)
	}
	removeTemporary = false
	if err := store.ops.sync(store.directory); err != nil {
		return fmt.Errorf("sync raft directory after identity install: %w", err)
	}
	return nil
}

func (store *FileStore) openOrCreateWAL() (*os.File, error) {
	walPath := filepath.Join(store.directoryPath, RaftWALFilename)
	file, _, err := store.openAnchoredRegularFile(RaftWALFilename, os.O_RDWR, true)
	if err != nil {
		return nil, fmt.Errorf("open raft WAL %q: %w", walPath, err)
	}
	// Repeat both creation barriers on every open. A prior attempt can observe
	// the created path after either sync failed, so existence alone is not proof
	// that the file and directory entry reached stable storage.
	if err := store.ops.sync(file); err != nil {
		_ = store.ops.close(file)
		return nil, fmt.Errorf("sync raft WAL before recovery: %w", err)
	}
	if err := store.ops.sync(store.directory); err != nil {
		_ = store.ops.close(file)
		return nil, fmt.Errorf("sync raft directory before WAL recovery: %w", err)
	}
	return file, nil
}

func (store *FileStore) openAnchoredRegularFile(name string, flags int, allowCreate bool) (*os.File, bool, error) {
	path := filepath.Join(store.directoryPath, name)
	before, err := store.ops.rootLstat(store.root, name)
	created := false
	if err != nil {
		if !allowCreate || !errors.Is(err, os.ErrNotExist) {
			return nil, false, err
		}
		created = true
		flags |= os.O_CREATE | os.O_EXCL
	} else if err := validateRegularFileInfo(before, path); err != nil {
		return nil, false, err
	}
	file, err := store.ops.rootOpenFile(store.root, name, flags|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0o600)
	if err != nil {
		return nil, false, err
	}
	opened, err := file.Stat()
	if err == nil {
		err = validateRegularFileInfo(opened, path)
	}
	if err == nil && !created && !os.SameFile(before, opened) {
		err = fmt.Errorf("%w: raft path %q changed while opening", ErrStorageCorrupt, path)
	}
	if err != nil {
		return nil, false, errors.Join(err, store.ops.close(file))
	}
	return file, created, nil
}

func (store *FileStore) recoverWAL() (RecoveredState, uint64, error) {
	info, err := store.wal.Stat()
	if err != nil {
		return RecoveredState{}, 0, fmt.Errorf("stat raft WAL: %w", err)
	}
	if info.Size() < 0 {
		return RecoveredState{}, 0, fmt.Errorf("%w: negative WAL size", ErrStorageCorrupt)
	}
	size := info.Size()
	state := RecoveredState{Identity: store.identity}
	var offset int64
	var verifiedBoundary int64
	var transactionStart int64
	var lastTransaction uint64
	var active bool
	var transactionID uint64
	var flags transactionFlags
	var count uint8
	var expected []walRecordType
	var effectIndex int
	var batch PersistenceBatch

	for offset < size {
		recordType, payload, next, partial, err := readWALRecordAt(store.wal, offset, size)
		if err != nil {
			return RecoveredState{}, 0, err
		}
		if partial {
			boundary := verifiedBoundary
			if active {
				boundary = transactionStart
			}
			if err := store.truncateCrashTail(boundary); err != nil {
				return RecoveredState{}, 0, err
			}
			return state.Clone(), lastTransaction, nil
		}
		if !active {
			if recordType != walRecordTransactionBegin {
				return RecoveredState{}, 0, fmt.Errorf("%w: invalid record sequence outside transaction: type %d", ErrStorageCorrupt, recordType)
			}
			transactionID, flags, count, err = decodeTransactionBoundaryPayload(payload)
			if err != nil {
				return RecoveredState{}, 0, err
			}
			if lastTransaction == math.MaxUint64 || transactionID != lastTransaction+1 {
				return RecoveredState{}, 0, fmt.Errorf("%w: non-canonical transaction ID %d after %d", ErrStorageCorrupt, transactionID, lastTransaction)
			}
			active = true
			transactionStart = offset
			expected = expectedRecordTypes(flags)
			effectIndex = 0
			batch = PersistenceBatch{}
			offset = next
			continue
		}
		if uint64(next-transactionStart) > MaxWALTransactionBytes {
			return RecoveredState{}, 0, fmt.Errorf("%w: WAL transaction exceeds maximum %d", ErrStorageCorrupt, MaxWALTransactionBytes)
		}
		if effectIndex < len(expected) {
			if recordType != expected[effectIndex] {
				return RecoveredState{}, 0, fmt.Errorf("%w: invalid transaction record sequence: got %d want %d", ErrStorageCorrupt, recordType, expected[effectIndex])
			}
			if err := decodeEffectRecord(recordType, payload, transactionID, &batch); err != nil {
				return RecoveredState{}, 0, err
			}
			effectIndex++
			offset = next
			continue
		}
		if recordType != walRecordTransactionCommit {
			return RecoveredState{}, 0, fmt.Errorf("%w: invalid transaction commit sequence", ErrStorageCorrupt)
		}
		commitID, commitFlags, commitCount, err := decodeTransactionBoundaryPayload(payload)
		if err != nil {
			return RecoveredState{}, 0, err
		}
		if commitID != transactionID || commitFlags != flags || commitCount != count || count != uint8(len(expected)) || flagsForBatch(batch) != flags {
			return RecoveredState{}, 0, fmt.Errorf("%w: transaction commit does not match begin/effects", ErrStorageCorrupt)
		}
		prospective, err := applyPersistenceBatch(state, batch, store.identity, store.voters)
		if err != nil {
			return RecoveredState{}, 0, fmt.Errorf("%w: transaction %d semantics: %v", ErrStorageCorrupt, transactionID, err)
		}
		state = prospective
		lastTransaction = transactionID
		verifiedBoundary = next
		active = false
		offset = next
	}
	if active {
		if err := store.truncateCrashTail(transactionStart); err != nil {
			return RecoveredState{}, 0, err
		}
	}
	if err := ValidateRecoveredState(state, store.identity, store.voters); err != nil {
		return RecoveredState{}, 0, fmt.Errorf("%w: recovered state: %v", ErrStorageCorrupt, err)
	}
	return state.Clone(), lastTransaction, nil
}

func decodeEffectRecord(recordType walRecordType, payload []byte, transactionID uint64, batch *PersistenceBatch) error {
	switch recordType {
	case walRecordSnapshotBase:
		base, err := decodeSnapshotPayload(payload, transactionID)
		if err != nil {
			return err
		}
		batch.SnapshotBase = &base
	case walRecordTruncate:
		from, err := decodeTwoUint64Payload(payload, transactionID, "truncate")
		if err != nil {
			return err
		}
		if from == 0 {
			return fmt.Errorf("%w: zero truncation index", ErrStorageCorrupt)
		}
		batch.ReplaceFrom = from
	case walRecordEntries:
		entries, err := decodeEntriesPayload(payload, transactionID)
		if err != nil {
			return err
		}
		batch.Entries = entries
	case walRecordAppliedIndex:
		index, err := decodeTwoUint64Payload(payload, transactionID, "applied-index")
		if err != nil {
			return err
		}
		batch.AppliedIndex = &index
	case walRecordHardState:
		state, err := decodeHardStatePayload(payload, transactionID)
		if err != nil {
			return err
		}
		batch.HardState = &state
	default:
		return fmt.Errorf("%w: non-effect record type %d", ErrStorageCorrupt, recordType)
	}
	return nil
}

func readWALRecordAt(file *os.File, offset, size int64) (walRecordType, []byte, int64, bool, error) {
	remaining := size - offset
	if remaining < walRecordHeaderBytes {
		return 0, nil, offset, true, nil
	}
	header := make([]byte, walRecordHeaderBytes)
	if _, err := file.ReadAt(header, offset); err != nil {
		return 0, nil, offset, false, fmt.Errorf("read WAL header at %d: %w", offset, err)
	}
	if !bytes.Equal(header[:4], walMagic[:]) {
		return 0, nil, offset, false, fmt.Errorf("%w: invalid WAL magic at %d", ErrStorageCorrupt, offset)
	}
	version := StorageFormatVersion(binary.BigEndian.Uint16(header[4:6]))
	if version != StorageFormatVersion1 {
		return 0, nil, offset, false, fmt.Errorf("%w: unsupported WAL format %d", ErrStorageCorrupt, version)
	}
	recordType := walRecordType(header[6])
	if !validWALRecordType(recordType) {
		return 0, nil, offset, false, fmt.Errorf("%w: unknown WAL record type %d", ErrStorageCorrupt, recordType)
	}
	if header[7] != 0 {
		return 0, nil, offset, false, fmt.Errorf("%w: nonzero WAL header flags", ErrStorageCorrupt)
	}
	payloadLength := binary.BigEndian.Uint64(header[8:16])
	if payloadLength > MaxWALRecordPayloadBytes {
		return 0, nil, offset, false, fmt.Errorf("%w: WAL payload length %d exceeds maximum %d", ErrStorageCorrupt, payloadLength, MaxWALRecordPayloadBytes)
	}
	total := uint64(walRecordHeaderBytes) + payloadLength + walRecordChecksumBytes
	if total > math.MaxInt64 || uint64(offset) > math.MaxInt64-total {
		return 0, nil, offset, false, fmt.Errorf("%w: WAL record offset arithmetic overflow", ErrStorageCorrupt)
	}
	next := offset + int64(total)
	if next > size {
		return 0, nil, offset, true, nil
	}
	if payloadLength > uint64(math.MaxInt) {
		return 0, nil, offset, false, fmt.Errorf("%w: WAL payload exceeds local integer domain", ErrStorageCorrupt)
	}
	remainder := make([]byte, int(payloadLength)+walRecordChecksumBytes)
	if _, err := file.ReadAt(remainder, offset+walRecordHeaderBytes); err != nil {
		return 0, nil, offset, false, fmt.Errorf("read complete WAL record at %d: %w", offset, err)
	}
	payload := remainder[:len(remainder)-walRecordChecksumBytes]
	wantChecksum := binary.BigEndian.Uint32(remainder[len(payload):])
	checksum := crc32.New(walCRC32CTable)
	_, _ = checksum.Write(header)
	_, _ = checksum.Write(payload)
	if checksum.Sum32() != wantChecksum {
		return 0, nil, offset, false, fmt.Errorf("%w: invalid WAL CRC32C at %d", ErrStorageCorrupt, offset)
	}
	return recordType, payload, next, false, nil
}

func (store *FileStore) truncateCrashTail(boundary int64) error {
	if boundary < 0 {
		return fmt.Errorf("%w: negative crash-tail boundary", ErrStorageCorrupt)
	}
	if err := store.ops.truncate(store.wal, boundary); err != nil {
		return fmt.Errorf("truncate incomplete WAL transaction to %d: %w", boundary, err)
	}
	if err := store.ops.sync(store.wal); err != nil {
		return fmt.Errorf("sync truncated WAL at %d: %w", boundary, err)
	}
	return nil
}

func encodeStorageIdentity(identity StorageIdentity) []byte {
	content := make([]byte, identityBytes)
	copy(content[:4], identityMagic[:])
	binary.BigEndian.PutUint16(content[4:6], uint16(identity.FormatVersion))
	copy(content[6:22], identity.ClusterID[:])
	binary.BigEndian.PutUint16(content[22:24], identity.LocalVoterID)
	copy(content[24:56], identity.VoterFingerprint[:])
	binary.BigEndian.PutUint32(content[56:60], crc32.Checksum(content[:56], walCRC32CTable))
	return content
}

func decodeStorageIdentity(content []byte) (StorageIdentity, error) {
	if len(content) != identityBytes {
		return StorageIdentity{}, fmt.Errorf("%w: identity length=%d want=%d", ErrStorageCorrupt, len(content), identityBytes)
	}
	if !bytes.Equal(content[:4], identityMagic[:]) {
		return StorageIdentity{}, fmt.Errorf("%w: invalid identity magic", ErrStorageCorrupt)
	}
	checksum := binary.BigEndian.Uint32(content[56:60])
	if crc32.Checksum(content[:56], walCRC32CTable) != checksum {
		return StorageIdentity{}, fmt.Errorf("%w: invalid identity CRC32C", ErrStorageCorrupt)
	}
	var identity StorageIdentity
	identity.FormatVersion = StorageFormatVersion(binary.BigEndian.Uint16(content[4:6]))
	copy(identity.ClusterID[:], content[6:22])
	identity.LocalVoterID = binary.BigEndian.Uint16(content[22:24])
	copy(identity.VoterFingerprint[:], content[24:56])
	return identity, nil
}

func validateOpenedDirectory(file *os.File, path string) error {
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat opened raft directory %q: %w", path, err)
	}
	return validateDirectoryInfo(info, path)
}

func validateDirectoryInfo(info os.FileInfo, path string) error {
	if !info.IsDir() || info.Mode().Perm() != 0o700 {
		return fmt.Errorf("%w: raft directory %q must be a real 0700 directory", ErrStorageCorrupt, path)
	}
	return validateOpenedOwner(info, path, false)
}

func validateOpenedRegularFile(file *os.File, path string) error {
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat opened raft path %q: %w", path, err)
	}
	return validateRegularFileInfo(info, path)
}

func validateRegularFileInfo(info os.FileInfo, path string) error {
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return fmt.Errorf("%w: raft path %q must be a real 0600 regular file", ErrStorageCorrupt, path)
	}
	return validateOpenedOwner(info, path, true)
}

func validateOpenedOwner(info os.FileInfo, path string, requireSingleLink bool) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("%w: ownership unavailable for %q", ErrStorageCorrupt, path)
	}
	if stat.Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("%w: %q is not owned by the current user", ErrStorageCorrupt, path)
	}
	if requireSingleLink && stat.Nlink != 1 {
		return fmt.Errorf("%w: %q has %d hard links", ErrStorageCorrupt, path, stat.Nlink)
	}
	return nil
}

func writeAll(content []byte, write func([]byte) (int, error)) error {
	for len(content) != 0 {
		written, err := write(content)
		if written < 0 || written > len(content) {
			return fmt.Errorf("invalid write count %d for %d bytes", written, len(content))
		}
		content = content[written:]
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}
