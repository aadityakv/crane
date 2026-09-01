package store

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/aaditya/cs425mp3/internal/crane/model"
)

// Store is the exclusive process-lifetime owner of one worker WAL.
type Store struct {
	mu         sync.Mutex
	options    Options
	operations storeOperations
	wal        *os.File
	lock       *os.File
	directory  *os.File
	root       *os.Root
	state      RecoveredState
	closed     bool
	failed     bool
}

type storeOperations struct {
	openRoot func(string) (*os.Root, error)
	syncFile func(*os.File) error
}

func defaultStoreOperations() storeOperations {
	return storeOperations{
		openRoot: os.OpenRoot,
		syncFile: func(file *os.File) error {
			return file.Sync()
		},
	}
}

func Open(path string, identity Identity, options Options) (result *Store, resultErr error) {
	return openWithOperations(path, identity, options, defaultStoreOperations())
}

func openWithOperations(path string, identity Identity, options Options, operations storeOperations) (result *Store, resultErr error) {
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	if path == "" || filepath.Clean(path) == "." || filepath.Clean(path) == string(filepath.Separator) {
		return nil, fmt.Errorf("%w: unsafe worker path %q", ErrInvalidOptions, path)
	}
	if options.MaxBytes == 0 {
		options.MaxBytes = DefaultMaxBytes
	}
	if options.MaxBytes < uint64(walHeaderBytes+identityPayloadBytes+walChecksumBytes) {
		return nil, fmt.Errorf("%w: MaxBytes too small", ErrInvalidOptions)
	}
	if operations.openRoot == nil || operations.syncFile == nil {
		return nil, fmt.Errorf("%w: incomplete file operations", ErrInvalidOptions)
	}
	fresh, directory, err := prepareDirectory(path, operations.syncFile)
	if err != nil {
		return nil, err
	}
	store := &Store{options: options, operations: operations, directory: directory}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, store.release())
		}
	}()
	root, err := operations.openRoot(path)
	if err != nil {
		return nil, fmt.Errorf("anchor worker directory: %w", err)
	}
	store.root = root
	if err := validateRootAnchor(root, directory, path); err != nil {
		return nil, err
	}
	lock, err := openLockedFile(root, WorkerLockFilename)
	if err != nil {
		return nil, err
	}
	store.lock = lock
	// Repeat this barrier on every open. It covers a newly created lock and
	// resolves uncertainty left by a previous failed directory sync.
	if err := operations.syncFile(directory); err != nil {
		return nil, fmt.Errorf("sync worker directory after lock: %w", err)
	}
	if fresh {
		wal, err := openWAL(root, WorkerWALFilename, true)
		if err != nil {
			return nil, err
		}
		store.wal = wal
		epoch, err := newEpoch(options.NewWorkerEpoch)
		if err != nil {
			return nil, err
		}
		encoded, err := encodeIdentity(identity, epoch)
		if err != nil {
			return nil, err
		}
		if uint64(len(encoded)) > options.MaxBytes {
			return nil, ErrCapacity
		}
		if err := writeFull(wal, encoded); err != nil {
			return nil, err
		}
		if err := operations.syncFile(wal); err != nil {
			return nil, err
		}
		if err := operations.syncFile(directory); err != nil {
			return nil, err
		}
		store.state = RecoveredState{Identity: identity, WorkerEpoch: epoch, LastSequence: 1, WALBytes: uint64(len(encoded))}
		return store, nil
	}
	wal, err := openWAL(root, WorkerWALFilename, false)
	if err != nil {
		return nil, fmt.Errorf("%w: open existing WAL: %v", ErrCorrupt, err)
	}
	store.wal = wal
	info, err := wal.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() <= 0 || uint64(info.Size()) > options.MaxBytes || uint64(info.Size()) > uint64(math.MaxInt) {
		return nil, fmt.Errorf("%w: WAL size %d", ErrCorrupt, info.Size())
	}
	data := make([]byte, int(info.Size()))
	if _, err := io.ReadFull(wal, data); err != nil {
		return nil, err
	}
	state, truncateAt, err := recoverWAL(data, identity)
	if err != nil {
		return nil, err
	}
	if truncateAt != len(data) {
		if err := wal.Truncate(int64(truncateAt)); err != nil {
			return nil, err
		}
		if err := operations.syncFile(wal); err != nil {
			return nil, err
		}
	}
	if _, err := wal.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}
	store.state = state
	return store, nil
}

func newEpoch(generator func() (model.WorkerEpoch, error)) (model.WorkerEpoch, error) {
	if generator != nil {
		epoch, err := generator()
		if err != nil {
			return model.WorkerEpoch{}, err
		}
		if err := validateEpoch(epoch); err != nil {
			return model.WorkerEpoch{}, err
		}
		return epoch, nil
	}
	for {
		var epoch model.WorkerEpoch
		if _, err := rand.Read(epoch[:]); err != nil {
			return model.WorkerEpoch{}, err
		}
		if epoch != (model.WorkerEpoch{}) {
			return epoch, nil
		}
	}
}

// WorkerEpoch returns the immutable persisted worker incarnation.
func (store *Store) WorkerEpoch() model.WorkerEpoch {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.state.WorkerEpoch
}

// Recovered returns a complete independently owned durable view.
func (store *Store) Recovered() RecoveredState {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.state.Clone()
}

// Commit validates, bounds, appends, and fsyncs one atomic transaction.
func (store *Store) Commit(transaction Transaction) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrClosed
	}
	if store.failed {
		return ErrUnavailable
	}
	if err := transaction.Validate(); err != nil {
		return err
	}
	if store.state.LastSequence > math.MaxUint64-uint64(len(transaction.Records))-2 {
		return fmt.Errorf("%w: sequence overflow", ErrInvalidTransaction)
	}
	encodedBytes, err := transactionEncodedSize(transaction)
	if err != nil {
		return err
	}
	if store.state.WALBytes > store.options.MaxBytes || encodedBytes > store.options.MaxBytes-store.state.WALBytes {
		return ErrCapacity
	}
	transaction = transaction.Clone()
	encoded, err := encodeTransaction(store.state.LastSequence+1, transaction)
	if err != nil {
		return err
	}
	if uint64(len(encoded)) != encodedBytes {
		store.failed = true
		return fmt.Errorf("%w: encoded transaction size drift", ErrUnavailable)
	}
	if store.options.Faults != nil {
		if err := store.options.Faults.Inject(FaultBeforeAppend); err != nil {
			return err
		}
	}
	if err := writeFull(store.wal, encoded); err != nil {
		store.failed = true
		return err
	}
	if store.options.Faults != nil {
		if err := store.options.Faults.Inject(FaultBeforeSync); err != nil {
			store.failed = true
			return err
		}
	}
	if err := store.operations.syncFile(store.wal); err != nil {
		store.failed = true
		return err
	}
	store.state.Transactions = append(store.state.Transactions, transaction)
	store.state.LastSequence += uint64(len(transaction.Records)) + 2
	store.state.WALBytes += uint64(len(encoded))
	return nil
}

// Close releases WAL, directory, and exclusive lock resources idempotently.
func (store *Store) Close() error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return nil
	}
	store.closed = true
	return store.release()
}

func (store *Store) release() error {
	walErr := error(nil)
	if store.wal != nil {
		walErr = store.wal.Close()
		store.wal = nil
	}
	dirErr := error(nil)
	if store.directory != nil {
		dirErr = store.directory.Close()
		store.directory = nil
	}
	rootErr := error(nil)
	if store.root != nil {
		rootErr = store.root.Close()
		store.root = nil
	}
	lockErr := unlockAndClose(store.lock)
	store.lock = nil
	return errors.Join(walErr, dirErr, rootErr, lockErr)
}

func writeFull(file *os.File, data []byte) error {
	for len(data) > 0 {
		written, err := file.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}
