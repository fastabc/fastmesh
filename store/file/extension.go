package file

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
)

func extensionName(name string) bool {
	name = strings.ToLower(name)
	return name != "" && name != "version" && name != "objects" && name != "leases"
}

// Extension returns an owned copy of a product's opaque top-level section.
// An absent section returns nil. Core sections cannot be accessed this way.
func (s *Store) Extension(name string) (json.RawMessage, error) {
	if !extensionName(name) {
		return nil, errors.New("file store: reserved extension name")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return append(json.RawMessage(nil), s.state.Extensions[name]...), nil
}

// UpdateExtension serializes one section with object/lease writes and uses the
// same atomic durable commit. Returning nil removes the section.
func (s *Store) UpdateExtension(ctx context.Context, name string, fn func(json.RawMessage) (json.RawMessage, error)) error {
	if !extensionName(name) {
		return errors.New("file store: reserved extension name")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return os.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	value, err := fn(append(json.RawMessage(nil), s.state.Extensions[name]...))
	if err != nil {
		return err
	}
	if value != nil && !json.Valid(value) {
		return errors.New("file store: invalid extension JSON")
	}
	next := s.state
	next.Extensions = make(map[string]json.RawMessage, len(s.state.Extensions)+1)
	for key, old := range s.state.Extensions {
		next.Extensions[key] = old
	}
	if value == nil {
		delete(next.Extensions, name)
	} else {
		next.Extensions[name] = append(json.RawMessage(nil), value...)
	}
	next.Version++
	return s.commitStateLocked(next)
}

func (s *state) UnmarshalJSON(data []byte) error {
	type plain state
	var known plain
	if err := json.Unmarshal(data, &known); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for name := range fields {
		if !extensionName(name) {
			delete(fields, name)
		}
	}
	*s = state(known)
	s.Extensions = fields
	return nil
}

func (s state) MarshalJSON() ([]byte, error) {
	fields := make(map[string]any, len(s.Extensions)+3)
	for name, value := range s.Extensions {
		fields[name] = value
	}
	fields["version"] = s.Version
	if len(s.Objects) > 0 {
		fields["objects"] = s.Objects
	}
	if len(s.Leases) > 0 {
		fields["leases"] = s.Leases
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(fields); err != nil {
		return nil, err
	}
	return bytes.TrimSpace(buf.Bytes()), nil
}
