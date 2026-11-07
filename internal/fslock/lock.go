// Package fslock provides exclusive, process-owned locks with stable lock files.
package fslock

import (
	"errors"
	"os"
	"sync"
)

var ErrLocked = errors.New("file lock: already held by another owner")

type Lock struct {
	file *os.File
	once sync.Once
	err  error
}

// Acquire locks path until Release or process exit. Never unlink the lock file:
// its inode coordinates all owners even when the protected state is replaced.
func Acquire(path string) (*Lock, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := lockFile(f); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &Lock{file: f}, nil
}

// Release is idempotent, including for an unlocked Store's nil lock.
func (l *Lock) Release() error {
	if l == nil {
		return nil
	}
	l.once.Do(func() { l.err = l.file.Close() })
	return l.err
}
