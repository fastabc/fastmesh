package memory

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
)

// Extension returns an owned copy, or nil for a missing section.
func (s *Store) Extension(name string) (json.RawMessage, error) {
	if key := strings.ToLower(name); key == "" || key == "version" || key == "objects" || key == "leases" {
		return nil, errors.New("memory store: reserved extension name")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return append(json.RawMessage(nil), s.extensions[name]...), nil
}

// UpdateExtension atomically updates one opaque product-owned section.
func (s *Store) UpdateExtension(ctx context.Context, name string, fn func(json.RawMessage) (json.RawMessage, error)) error {
	if key := strings.ToLower(name); key == "" || key == "version" || key == "objects" || key == "leases" {
		return errors.New("memory store: reserved extension name")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	value, err := fn(append(json.RawMessage(nil), s.extensions[name]...))
	if err != nil {
		return err
	}
	if value != nil && !json.Valid(value) {
		return errors.New("memory store: invalid extension JSON")
	}
	if s.extensions == nil {
		s.extensions = map[string]json.RawMessage{}
	}
	if value == nil {
		delete(s.extensions, name)
	} else {
		s.extensions[name] = append(json.RawMessage(nil), value...)
	}
	s.version++
	return nil
}
