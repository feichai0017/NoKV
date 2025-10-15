package utils

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
)

// DirLock represents an exclusive filesystem lock on a directory.
type DirLock struct {
	file *os.File
	path string
}

// AcquireDirLock attempts to obtain an exclusive lock on the provided directory.
// The lock is implemented using a platform flock on a dedicated LOCK file. The
// returned DirLock must be released via (*DirLock).Release.
func AcquireDirLock(dir string) (*DirLock, error) {
	if dir == "" {
		return nil, fmt.Errorf("dirlock: directory required")
	}
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, err
	}
	lockPath := filepath.Join(dir, "LOCK")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			_ = f.Close()
		}
	}()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("dirlock: directory %q already in use", dir)
		}
		return nil, err
	}
	if err := f.Truncate(0); err == nil {
		pid := os.Getpid()
		host := ""
		if h, herr := os.Hostname(); herr == nil {
			host = h
		}
		_, _ = fmt.Fprintf(f, "pid=%d host=%s goos=%s\n", pid, host, runtime.GOOS)
		_ = f.Sync()
	}
	success = true
	return &DirLock{file: f, path: lockPath}, nil
}

// Release unlocks the directory and removes the lock file.
func (l *DirLock) Release() error {
	if l == nil || l.file == nil {
		return nil
	}
	var firstErr error
	if err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN); err != nil {
		firstErr = err
	}
	if err := l.file.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := os.Remove(l.path); err != nil && !errors.Is(err, os.ErrNotExist) && firstErr == nil {
		firstErr = err
	}
	l.file = nil
	return firstErr
}