package state

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/fastabc/fastmesh/policy/material"
	"github.com/fastabc/fastmesh/policy/snapshot"
)

// ApplyDecision records whether a prepared snapshot is still safe to reject
// or has crossed the durable commit decision and must be completed forward.
type ApplyDecision string

// ApplyJournal is the decrypted view of an in-progress apply transaction.
// Callers that inspect Materials must clear them after use.
type ApplyJournal struct {
	InputRevision int64
	Decision      ApplyDecision
	Snapshot      *snapshot.Snapshot
	Materials     []material.ResolvedMaterial
}

// SaveApplyPending durably records a fully prepared target before any hook is
// allowed to publish it.
func (s *Store) SaveApplyPending(snap *snapshot.Snapshot, materials []material.ResolvedMaterial) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.loadApplyJournalEnvelope(); err == nil {
		return errors.New("state: apply journal already exists")
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}
	target, err := s.makeLastGoodEnvelope(snap, materials)
	if err != nil {
		return err
	}
	journal := applyJournalEnvelope{
		Version:  applyJournalVersion,
		Decision: ApplyDecisionPending,
		Target:   target,
	}
	data, err := encodeGzipJSON(journal)
	if err != nil {
		return err
	}
	if err := s.writeAtomic(applyJournalFile, data); err != nil {
		return errors.Join(err, s.clearApplyJournal())
	}
	return nil
}

// CommitApply durably records the commit decision. Once this succeeds, or
// returns ErrCommitDecisionUncertain, the target must never be NACKed or
// compensated back to the previous revision.
func (s *Store) CommitApply() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	journal, err := s.loadApplyJournalEnvelope()
	if err != nil {
		return err
	}
	if journal.Decision == ApplyDecisionCommitted {
		return nil
	}
	if journal.Decision != ApplyDecisionPending {
		return fmt.Errorf("state: invalid apply decision %q", journal.Decision)
	}
	journal.Decision = ApplyDecisionCommitted
	data, err := encodeGzipJSON(journal)
	if err != nil {
		return err
	}
	renamed, err := s.writeAtomicClassified(applyJournalFile, data)
	if err != nil && renamed {
		return errors.Join(ErrCommitDecisionUncertain, err)
	}
	return err
}

// CompleteApply checkpoints the committed journal as the conventional
// last-good file, then removes the journal. The journal remains authoritative
// if either cleanup step fails.
func (s *Store) CompleteApply() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	journal, err := s.loadApplyJournalEnvelope()
	if err != nil {
		return err
	}
	if journal.Decision != ApplyDecisionCommitted {
		return fmt.Errorf("state: cannot complete %q apply journal", journal.Decision)
	}
	data, err := encodeGzipJSON(journal.Target)
	if err != nil {
		return err
	}
	if err := s.writeAtomic(snapshotFile, data); err != nil {
		return err
	}
	return s.clearApplyJournal()
}

// ClearApplyJournal removes a pending journal after a pre-decision failure.
func (s *Store) ClearApplyJournal() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.clearApplyJournal()
}

// LoadApplyJournal returns the decrypted current journal.
func (s *Store) LoadApplyJournal() (ApplyJournal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	journal, err := s.loadApplyJournalEnvelope()
	if err != nil {
		return ApplyJournal{}, err
	}
	snap, materials, err := s.decodeLastGoodEnvelope(journal.Target)
	if err != nil {
		return ApplyJournal{}, err
	}
	return ApplyJournal{Decision: journal.Decision, Snapshot: snap, Materials: materials, InputRevision: journal.Target.InputRevision}, nil
}

type applyJournalEnvelope struct {
	Version  int              `json:"version"`
	Decision ApplyDecision    `json:"decision"`
	Target   lastGoodEnvelope `json:"target"`
}

func (s *Store) loadApplyJournalEnvelope() (applyJournalEnvelope, error) {
	f, err := os.Open(filepath.Join(s.dir, applyJournalFile))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return applyJournalEnvelope{}, ErrNotFound
		}
		return applyJournalEnvelope{}, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return applyJournalEnvelope{}, err
	}
	defer gz.Close()
	var journal applyJournalEnvelope
	if err := json.NewDecoder(gz).Decode(&journal); err != nil {
		return applyJournalEnvelope{}, err
	}
	if journal.Version != applyJournalVersion {
		return applyJournalEnvelope{}, fmt.Errorf("state: unsupported apply journal version %d", journal.Version)
	}
	if journal.Decision != ApplyDecisionPending && journal.Decision != ApplyDecisionCommitted {
		return applyJournalEnvelope{}, fmt.Errorf("state: invalid apply decision %q", journal.Decision)
	}
	if journal.Target.Snapshot == nil {
		return applyJournalEnvelope{}, errors.New("state: apply journal missing target snapshot")
	}
	return journal, nil
}

func (s *Store) clearApplyJournal() error {
	if s.closed {
		return os.ErrClosed
	}
	err := os.Remove(filepath.Join(s.dir, applyJournalFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	dir, err := os.Open(s.dir)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
