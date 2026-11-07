package state

import (
	"os"
	"path/filepath"

	"github.com/fastabc/fastmesh/internal/fslock"
)

var ErrLocked = fslock.ErrLocked

// Open acquires exclusive ownership of dir until Close or process exit.
func Open(dir string) (*Store, error) {
	if dir == "" {
		return nil, os.ErrInvalid
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	lock, err := fslock.Acquire(filepath.Join(dir, ".lock"))
	if err != nil {
		return nil, err
	}
	return &Store{dir: dir, lock: lock}, nil
}

// Close releases directory ownership. A closed store cannot write state.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return s.lock.Release()
}
