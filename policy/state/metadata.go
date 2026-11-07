package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// SavePolicyContact persists the local time of the last successful controller
// contact. It is never populated from a controller-supplied timestamp.
func (s *Store) SavePolicyContact(at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeAtomic(policyContactFile, []byte(at.UTC().Format(time.RFC3339Nano)))
}

// LoadPolicyContact returns the last locally observed successful contact.
func (s *Store) LoadPolicyContact() (time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(filepath.Join(s.dir, policyContactFile))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return time.Time{}, ErrNotFound
		}
		return time.Time{}, err
	}
	at, err := time.Parse(time.RFC3339Nano, string(data))
	if err != nil {
		return time.Time{}, fmt.Errorf("state: invalid policy contact: %w", err)
	}
	return at, nil
}

// SaveNodeID persists the assigned node id.
func (s *Store) SaveNodeID(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeAtomic(nodeIDFile, []byte(id))
}

// LoadNodeID returns the persisted node id, or ErrNotFound if none exists.
func (s *Store) LoadNodeID() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(filepath.Join(s.dir, nodeIDFile))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", ErrNotFound
		}
		return "", err
	}
	return string(data), nil
}

// SaveControllerEndpoints persists the last non-empty controller endpoint set.
func (s *Store) SaveControllerEndpoints(endpoints []string) error {
	normalized := normalizeEndpoints(endpoints)
	if len(normalized) == 0 {
		return errors.New("state: controller endpoints are empty")
	}
	data, err := json.Marshal(normalized)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeAtomic(endpointsFile, data)
}

// LoadControllerEndpoints returns the last controller endpoint set.
func (s *Store) LoadControllerEndpoints() ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(filepath.Join(s.dir, endpointsFile))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	var endpoints []string
	if err := json.Unmarshal(data, &endpoints); err != nil {
		return nil, err
	}
	endpoints = normalizeEndpoints(endpoints)
	if len(endpoints) == 0 {
		return nil, errors.New("state: controller endpoints are empty")
	}
	return endpoints, nil
}

func normalizeEndpoints(endpoints []string) []string {
	out := make([]string, 0, len(endpoints))
	seen := make(map[string]struct{}, len(endpoints))
	for _, endpoint := range endpoints {
		endpoint = strings.TrimSpace(endpoint)
		if endpoint == "" {
			continue
		}
		if _, ok := seen[endpoint]; ok {
			continue
		}
		seen[endpoint] = struct{}{}
		out = append(out, endpoint)
	}
	return out
}
