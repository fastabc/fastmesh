package state

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
)

func (s *Store) writeAtomic(name string, data []byte) error {
	_, err := s.writeAtomicClassified(name, data)
	return err
}

func (s *Store) writeAtomicClassified(name string, data []byte) (renamed bool, err error) {
	if s.closed {
		return false, os.ErrClosed
	}
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return false, err
	}
	if err := os.Chmod(s.dir, 0o700); err != nil {
		return false, err
	}
	tmp, err := os.CreateTemp(s.dir, name+".tmp-*")
	if err != nil {
		return false, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return false, err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return false, err
	}
	if err := tmp.Close(); err != nil {
		return false, err
	}
	if err := os.Chmod(tmpName, 0o600); err != nil {
		return false, err
	}
	if err := os.Rename(tmpName, filepath.Join(s.dir, name)); err != nil {
		return false, err
	}
	renamed = true
	dir, err := os.Open(s.dir)
	if err != nil {
		return true, err
	}
	defer dir.Close()
	return true, dir.Sync()
}

func encodeGzipJSON(value any) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if err := json.NewEncoder(&stateWriter{writer: gz}).Encode(value); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// MaxStateDecodedBytes bounds both last-good and apply journal JSON before gzip.
// It also covers host snapshots and encrypted material, not only the mesh body.
const MaxStateDecodedBytes = 64 << 20

var ErrStateTooLarge = errors.New("state: decoded size limit exceeded")

type stateWriter struct {
	writer  io.Writer
	written int
}

func (w *stateWriter) Write(p []byte) (int, error) {
	if len(p) > MaxStateDecodedBytes-w.written {
		return 0, ErrStateTooLarge
	}
	n, err := w.writer.Write(p)
	w.written += n
	return n, err
}

func readGzipJSON(reader io.Reader, value any) error {
	gz, err := gzip.NewReader(reader)
	if err != nil {
		return err
	}
	defer gz.Close()
	data, err := io.ReadAll(io.LimitReader(gz, MaxStateDecodedBytes+1))
	if err != nil {
		return err
	}
	if len(data) > MaxStateDecodedBytes {
		return ErrStateTooLarge
	}
	return json.Unmarshal(data, value)
}
