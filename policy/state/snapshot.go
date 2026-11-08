package state

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/fastabc/fastmesh/policy/material"
	"github.com/fastabc/fastmesh/policy/snapshot"
)

// SaveLastGood atomically persists snap and an encrypted copy of the exact
// materials used to prepare it.
func (s *Store) SaveLastGood(snap *snapshot.Snapshot, materials []material.ResolvedMaterial) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	envelope, err := s.makeLastGoodEnvelope(snap, materials)
	if err != nil {
		return err
	}
	data, err := encodeGzipJSON(envelope)
	if err != nil {
		return err
	}
	return s.writeAtomic(snapshotFile, data)
}

func (s *Store) makeLastGoodEnvelope(snap *snapshot.Snapshot, materials []material.ResolvedMaterial) (lastGoodEnvelope, error) {
	if snap == nil {
		return lastGoodEnvelope{}, errors.New("state: last-good snapshot is nil")
	}
	envelope := lastGoodEnvelope{Version: lastGoodVersion, Snapshot: snap}
	highWater, err := s.loadInputRevision()
	if err != nil {
		return lastGoodEnvelope{}, err
	}
	if snap.MeshPlan != nil {
		if snap.MeshPlan.InputRevision < highWater {
			return lastGoodEnvelope{}, ErrRevisionRegression
		}
		highWater = snap.MeshPlan.InputRevision
	}
	envelope.InputRevision = highWater
	if len(materials) > 0 {
		key, err := s.loadOrCreateMaterialKey()
		if err != nil {
			return lastGoodEnvelope{}, err
		}
		defer zero(key)
		encrypted, err := encryptMaterials(key, snap.Revision, materials)
		if err != nil {
			return lastGoodEnvelope{}, err
		}
		envelope.Materials = encrypted
	}
	return envelope, nil
}

// LoadSnapshot returns the last-good snapshot, or ErrNotFound if none exists.
func (s *Store) LoadSnapshot() (*snapshot.Snapshot, error) {
	snap, _, err := s.LoadLastGood()
	return snap, err
}

// LoadLastGood returns the persisted snapshot and decrypts its bound material
// cache. Only versioned last-good envelopes are accepted.
func (s *Store) LoadLastGood() (*snapshot.Snapshot, []material.ResolvedMaterial, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	journal, err := s.loadApplyJournalEnvelope()
	if err == nil && journal.Decision == ApplyDecisionCommitted {
		return s.decodeLastGoodEnvelope(journal.Target)
	}
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, nil, err
	}
	return s.loadLastGoodFile()
}

func (s *Store) loadLastGoodFile() (*snapshot.Snapshot, []material.ResolvedMaterial, error) {
	f, err := os.Open(filepath.Join(s.dir, snapshotFile))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil, ErrNotFound
		}
		return nil, nil, err
	}
	defer f.Close()
	var envelope lastGoodEnvelope
	if err := readGzipJSON(f, &envelope); err != nil {
		return nil, nil, err
	}
	return s.decodeLastGoodEnvelope(envelope)
}

func (s *Store) decodeLastGoodEnvelope(envelope lastGoodEnvelope) (*snapshot.Snapshot, []material.ResolvedMaterial, error) {
	if envelope.Version != lastGoodVersion {
		return nil, nil, fmt.Errorf("state: unsupported last-good version %d", envelope.Version)
	}
	if envelope.Snapshot == nil {
		return nil, nil, errors.New("state: last-good envelope missing snapshot")
	}
	if envelope.Materials == nil {
		return envelope.Snapshot, nil, nil
	}
	key, err := s.loadMaterialKey()
	if err != nil {
		return nil, nil, err
	}
	defer zero(key)
	materials, err := decryptMaterials(key, envelope.Snapshot.Revision, envelope.Materials)
	if err != nil {
		return nil, nil, err
	}
	return envelope.Snapshot, materials, nil
}

type lastGoodEnvelope struct {
	InputRevision int64               `json:"input_revision,omitempty"`
	Version       int                 `json:"version"`
	Snapshot      *snapshot.Snapshot  `json:"snapshot"`
	Materials     *encryptedMaterials `json:"materials,omitempty"`
}
