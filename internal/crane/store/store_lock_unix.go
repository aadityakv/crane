//go:build unix

package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

const (
	// WorkerWALFilename is the fixed anchored WAL child name.
	WorkerWALFilename = "worker.wal"
	// WorkerLockFilename is the fixed anchored process-lifetime lock child name.
	WorkerLockFilename = "worker.lock"
)

func prepareDirectory(path string, syncFile func(*os.File) error) (bool, *os.File, error) {
	info, err := os.Lstat(path)
	fresh := false
	if errors.Is(err, os.ErrNotExist) {
		if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return false, nil, fmt.Errorf("create worker directory: %w", err)
		} else if err == nil {
			fresh = true
		}
	} else if err != nil {
		return false, nil, err
	} else if !info.IsDir() || info.Mode().Perm() != 0o700 {
		return false, nil, fmt.Errorf("%w: worker directory must be real 0700 directory", ErrCorrupt)
	}
	// Sync on every attempt so a failed initialization cannot cause a retry to
	// skip the store-directory creation barrier.
	parent, openErr := os.OpenFile(filepath.Dir(path), os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if openErr != nil {
		return false, nil, fmt.Errorf("open worker parent directory: %w", openErr)
	}
	syncErr := syncFile(parent)
	closeErr := parent.Close()
	if err := errors.Join(syncErr, closeErr); err != nil {
		return false, nil, fmt.Errorf("sync worker parent directory: %w", err)
	}
	directory, err := os.OpenFile(path, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return false, nil, fmt.Errorf("open worker directory without following links: %w", err)
	}
	opened, err := directory.Stat()
	if err != nil || !opened.IsDir() || opened.Mode().Perm() != 0o700 {
		directory.Close()
		return false, nil, fmt.Errorf("%w: invalid opened directory", ErrCorrupt)
	}
	if stat, ok := opened.Sys().(*syscall.Stat_t); !ok || stat.Uid != uint32(os.Geteuid()) {
		directory.Close()
		return false, nil, fmt.Errorf("%w: worker directory owner mismatch", ErrCorrupt)
	}
	pathInfo, err := os.Lstat(path)
	if err != nil || !os.SameFile(opened, pathInfo) {
		directory.Close()
		return false, nil, fmt.Errorf("%w: worker directory changed while opening", ErrCorrupt)
	}
	entries, err := directory.Readdirnames(-1)
	if err != nil {
		directory.Close()
		return false, nil, err
	}
	if !fresh {
		fresh = len(entries) == 0
	}
	for _, name := range entries {
		if !recognizedStoreFilename(name) {
			directory.Close()
			return false, nil, fmt.Errorf("%w: unexpected worker file %q", ErrCorrupt, name)
		}
	}
	return fresh, directory, nil
}

func validateRootAnchor(root *os.Root, directory *os.File, path string) error {
	anchored, err := root.OpenFile(".", os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return fmt.Errorf("open anchored worker directory %q: %w", path, err)
	}
	anchoredInfo, statErr := anchored.Stat()
	closeErr := anchored.Close()
	if statErr != nil || closeErr != nil {
		return fmt.Errorf("stat anchored worker directory %q: %w", path, errors.Join(statErr, closeErr))
	}
	directoryInfo, err := directory.Stat()
	if err != nil {
		return fmt.Errorf("stat worker directory %q: %w", path, err)
	}
	if !os.SameFile(directoryInfo, anchoredInfo) {
		return fmt.Errorf("%w: worker directory %q changed before anchoring", ErrCorrupt, path)
	}
	return nil
}

func lockDirectory(directory *os.File) error {
	if err := syscall.Flock(int(directory.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return ErrLocked
		}
		return fmt.Errorf("acquire worker directory lock: %w", err)
	}
	return nil
}

func openLockedFile(root *os.Root, name string) (*os.File, error) {
	file, err := root.OpenFile(name, os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0o600)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	pathInfo, pathErr := root.Lstat(name)
	if err != nil || pathErr != nil || !os.SameFile(info, pathInfo) || validateOwnedRegular(info) != nil {
		file.Close()
		return nil, fmt.Errorf("%w: lock must be real 0600 regular file", ErrCorrupt)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, ErrLocked
		}
		return nil, err
	}
	return file, nil
}

func openWAL(root *os.Root, name string, create bool) (*os.File, error) {
	flags := os.O_RDWR | syscall.O_NOFOLLOW | syscall.O_NONBLOCK
	if create {
		flags |= os.O_CREATE | os.O_EXCL
	}
	file, err := root.OpenFile(name, flags, 0o600)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	pathInfo, pathErr := root.Lstat(name)
	if err != nil || pathErr != nil || !os.SameFile(info, pathInfo) || validateOwnedRegular(info) != nil {
		file.Close()
		return nil, fmt.Errorf("%w: WAL must be real 0600 regular file", ErrCorrupt)
	}
	return file, nil
}

func validateOwnedRegular(info os.FileInfo) error {
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return ErrCorrupt
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) || stat.Nlink != 1 {
		return ErrCorrupt
	}
	return nil
}

func unlockAndClose(file *os.File) error {
	if file == nil {
		return nil
	}
	unlock := syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	closeErr := file.Close()
	return errors.Join(unlock, closeErr)
}
