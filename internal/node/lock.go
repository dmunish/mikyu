package node

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ErrLocked reports that the data directory is in use by another process.
var ErrLocked = errors.New("data directory is in use by a running server — use --target to connect to it")

type dirLock struct {
	f *os.File
}

// acquireDirLock takes an exclusive, non-blocking lock on <dir>/mikyu.lock.
func acquireDirLock(dir string) (*dirLock, error) {
	path := filepath.Join(dir, "mikyu.lock")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open lock file %s: %w", path, err)
	}
	ok, err := tryFlock(f)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("lock %s: %w", path, err)
	}
	if !ok {
		_ = f.Close()
		return nil, fmt.Errorf("%w (%s)", ErrLocked, path)
	}
	return &dirLock{f: f}, nil
}

func (l *dirLock) release() error {
	if l == nil || l.f == nil {
		return nil
	}
	err := funlock(l.f)
	cerr := l.f.Close()
	l.f = nil
	if err != nil {
		return err
	}
	return cerr
}
