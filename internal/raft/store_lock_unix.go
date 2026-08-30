//go:build darwin || linux

package raft

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"syscall"
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
	RaftSnapshotFilename = "snapshot"

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
	wal, err := store.openOrCreateWAL()
	if err != nil {
		return nil, err
	}
	store.wal = wal
	state, lastTransaction, err := store.recoverWAL()
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

// Recover returns an independently owned copy of the checked durable state.
func (store *FileStore) Recover() (RecoveredState, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return RecoveredState{}, ErrStoreClosed
	}
	return store.state.Clone(), nil
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
	for name := range names {
		if name != RaftLockFilename && name != RaftIdentityFilename && name != RaftWALFilename && name != RaftSnapshotFilename {
			return fmt.Errorf("%w: unexpected raft directory entry %q", ErrStorageCorrupt, name)
		}
	}
	if names[RaftSnapshotFilename] {
		return fmt.Errorf("%w: snapshot bytes are unsupported before Task 9", ErrStorageCorrupt)
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
