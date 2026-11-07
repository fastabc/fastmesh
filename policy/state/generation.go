package state

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// NextMeshGeneration reserves a durable generation before endpoint publication.
// Instance IDs derive from the persisted NodeID and service/port; generations
// increase across reconnects, listener replacement and process restart.
func (s *Store) NextMeshGeneration() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	const filename = "mesh-generation"
	data, err := os.ReadFile(filepath.Join(s.dir, filename))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, err
	}
	var value uint64
	if len(data) != 0 {
		value, err = strconv.ParseUint(string(data), 10, 64)
		if err != nil {
			return 0, err
		}
	}
	if value == ^uint64(0) {
		return 0, fmt.Errorf("mesh generation exhausted")
	}
	value++
	if err := s.writeAtomic(filename, []byte(strconv.FormatUint(value, 10))); err != nil {
		return 0, err
	}
	return value, nil
}
