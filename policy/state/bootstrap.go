package state

import (
	"errors"
	"io"
	"os"
	"path/filepath"
)

const enrollmentFile = "credentials.json"
const enrollmentMaxSize = 512 << 10

// SaveEnrollmentIdentity publishes the enrollment key and response together.
func (s *Store) SaveEnrollmentIdentity(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(data) > enrollmentMaxSize {
		return ErrStateTooLarge
	}
	return s.writeAtomic(enrollmentFile, data)
}

// ReadEnrollmentIdentity reads a complete identity file, including while a
// renewal atomically replaces it. Writers must hold the directory's Store lock.
func ReadEnrollmentIdentity(dir string) ([]byte, error) {
	f, err := os.Open(filepath.Join(dir, enrollmentFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, enrollmentMaxSize+1))
	if len(data) > enrollmentMaxSize {
		return nil, ErrStateTooLarge
	}
	return data, err
}
