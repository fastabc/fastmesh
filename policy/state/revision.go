package state

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

// InputRevision reads the durable high-water mark. A committed journal wins
// even when the process crashed before publishing or checkpointing its target;
// a merely prepared transaction must not raise the accepted watermark.
func (s *Store) InputRevision() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadInputRevision()
}

func (s *Store) loadInputRevision() (int64, error) {
	journal, err := s.loadApplyJournalEnvelope()
	if err == nil && journal.Decision == ApplyDecisionCommitted {
		return journal.Target.InputRevision, nil
	}
	if err != nil && !errors.Is(err, ErrNotFound) {
		return 0, err
	}
	f, err := os.Open(filepath.Join(s.dir, snapshotFile))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return 0, err
	}
	defer gz.Close()
	var metadata struct {
		InputRevision int64 `json:"input_revision"`
	}
	if err = json.NewDecoder(gz).Decode(&metadata); err != nil {
		return 0, err
	}
	return metadata.InputRevision, nil
}
