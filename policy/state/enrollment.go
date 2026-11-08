package state

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

// EnrollmentRequestID durably identifies an initial enrollment before its RPC.
// Only a digest of the token/workload binding is persisted, never the raw token.
// A changed token or workload starts a separate registration attempt.
func (s *Store) EnrollmentRequestID(token, workload string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return "", os.ErrClosed
	}
	binding, _ := json.Marshal([]string{token, workload})
	digest := sha256.Sum256(binding)
	scope := hex.EncodeToString(digest[:])
	var pending struct {
		Scope string `json:"scope"`
		ID    string `json:"id"`
	}
	data, err := os.ReadFile(filepath.Join(s.dir, "enrollment.json"))
	if err == nil {
		if err := json.Unmarshal(data, &pending); err != nil {
			return "", err
		}
		decoded, err := hex.DecodeString(pending.ID)
		if err != nil || len(decoded) != 16 {
			return "", errors.New("state: invalid enrollment request ID")
		}
		if pending.Scope == scope {
			return pending.ID, nil
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	pending.Scope, pending.ID = scope, hex.EncodeToString(id[:])
	data, err = json.Marshal(pending)
	if err != nil {
		return "", err
	}
	if err := s.writeAtomic("enrollment.json", data); err != nil {
		return "", err
	}
	return pending.ID, nil
}
