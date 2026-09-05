package store

import (
	"crypto/rand"
	"errors"
	"fmt"
	"github.com/aadityakv/crane/internal/crane/integrationhook"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/aadityakv/crane/internal/crane/model"
)

// Store is the exclusive process-lifetime owner of one worker WAL.
type Store struct {
	mu              sync.Mutex
	options         Options
	operations      storeOperations
	wal             *os.File
	lock            *os.File
	directory       *os.File
	directoryLocked bool
	root            *os.Root
	state           RecoveredState
	work            RecoveredWork
	// validatedOutboxes caches, under mu, the DeliveryIDs whose expensive
	// outbox proof (TupleDelivery marshal + assignment containment +
	// deterministic route) already succeeded for their immutable definition.
	// Outbox definitions never change after creation and the assignment
	// context of a proof is pinned by revision monotonicity, so each proof is
	// executed at most once per recovery generation: at creation, and again
	// only after recovery rebuilds the state and clears this set. Cheap
	// structural, fence, and retry-state checks remain per-commit.
	validatedOutboxes map[model.DeliveryID]struct{}
	closed            bool
	failed            bool
}

type storeOperations struct {
	openRoot  func(string) (*os.Root, error)
	syncFile  func(*os.File) error
	writeFile func(*os.File, []byte) (int, error)
}

func defaultStoreOperations() storeOperations {
	return storeOperations{
		openRoot: os.OpenRoot,
		syncFile: func(file *os.File) error {
			return file.Sync()
		},
		writeFile: func(file *os.File, data []byte) (int, error) { return file.Write(data) },
	}
}

// Open exclusively locks, initializes or validates, and recovers one worker store.
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
	if options.Hook == nil {
		options.Hook = integrationhook.Noop{}
	}
	if options.MaxBytes < uint64(walHeaderBytes+identityPayloadBytes+walChecksumBytes) {
		return nil, fmt.Errorf("%w: MaxBytes too small", ErrInvalidOptions)
	}
	if operations.openRoot == nil || operations.syncFile == nil || operations.writeFile == nil {
		return nil, fmt.Errorf("%w: incomplete file operations", ErrInvalidOptions)
	}
	fresh, directory, err := prepareDirectory(path, operations.syncFile)
	if err != nil {
		return nil, err
	}
	store := &Store{options: options, operations: operations, directory: directory, validatedOutboxes: make(map[model.DeliveryID]struct{})}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, store.release())
		}
	}()
	if err := lockDirectory(directory); err != nil {
		return nil, err
	}
	store.directoryLocked = true
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
		if err := writeFullWith(wal, encoded, operations.writeFile); err != nil {
			return nil, err
		}
		if err := operations.syncFile(wal); err != nil {
			return nil, err
		}
		if err := operations.syncFile(directory); err != nil {
			return nil, err
		}
		store.state = RecoveredState{Identity: identity, WorkerEpoch: epoch, LastSequence: 1, WALBytes: uint64(len(encoded))}
		store.work = newRecoveredWork()
		return store, nil
	}
	if err := store.recoverExisting(identity); err != nil {
		return nil, err
	}
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
	if err := validateRegisteredTransaction(transaction); err != nil {
		return err
	}
	prospective, err := store.reduceWorkLocked(transaction)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidTransaction, err)
	}
	if err := validateRecoveredWorkLocal(prospective, store.state.Identity.NodeID, store.state.WorkerEpoch, store.validatedOutboxes); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidTransaction, err)
	}
	return store.commitWorkLocked(transaction, prospective)
}

func (store *Store) commitLocked(transaction Transaction) error {
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
	used, ok := checkedAdd(store.state.SnapshotBytes, store.state.WALBytes)
	if !ok || used > store.options.MaxBytes || encodedBytes > store.options.MaxBytes-used {
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
	if err := writeFullWith(store.wal, encoded, store.operations.writeFile); err != nil {
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
	store.state.TransactionCount++
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
	store.validatedOutboxes = nil
	return store.release()
}

func (store *Store) release() error {
	walErr := error(nil)
	if store.wal != nil {
		walErr = store.wal.Close()
		walErr = errors.Join(walErr, store.inject(FaultCloseWAL))
		store.wal = nil
	}
	lockErr := unlockAndClose(store.lock)
	lockErr = errors.Join(lockErr, store.inject(FaultCloseLock))
	store.lock = nil
	rootErr := error(nil)
	if store.root != nil {
		rootErr = store.root.Close()
		rootErr = errors.Join(rootErr, store.inject(FaultCloseRoot))
		store.root = nil
	}
	dirErr := error(nil)
	if store.directory != nil {
		if store.directoryLocked {
			dirErr = unlockAndClose(store.directory)
			store.directoryLocked = false
		} else {
			dirErr = store.directory.Close()
		}
		dirErr = errors.Join(dirErr, store.inject(FaultCloseDirectory))
		store.directory = nil
	}
	return errors.Join(walErr, lockErr, rootErr, dirErr)
}

// durable publishes one named boundary to the configured hook. It is called
// only after commitLocked has returned success, i.e. after the transaction's
// WAL fsync, and before the public mutation returns; callers therefore write
// any Accepted/Completed/status acknowledgement strictly after it.
func (store *Store) durable(name string) {
	store.options.Hook.DurableBoundary(name)
}

func (store *Store) inject(point FaultPoint) error {
	if store.options.Faults == nil {
		return nil
	}
	return store.options.Faults.Inject(point)
}

func writeFullWith(file *os.File, data []byte, write func(*os.File, []byte) (int, error)) error {
	for len(data) > 0 {
		written, err := write(file, data)
		if err != nil {
			return err
		}
		if written <= 0 || written > len(data) {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}
