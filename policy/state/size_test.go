package state

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func readTestWatermark(t *testing.T, encoded []byte) error {
	t.Helper()
	dir := t.TempDir()
	s := openState(t, dir)
	if err := os.WriteFile(filepath.Join(dir, snapshotFile), encoded, 0600); err != nil {
		t.Fatal(err)
	}
	_, err := s.InputRevision()
	return err
}

func TestStateDecodedSizeBoundary(t *testing.T) {
	for _, extra := range []int{0, 1} {
		var b bytes.Buffer
		gz := gzip.NewWriter(&b)
		// Valid JSON with whitespace padding avoids expanding a giant string object.
		_, _ = gz.Write([]byte("{}"))
		chunk := bytes.Repeat([]byte(" "), 1<<20)
		left := MaxStateDecodedBytes - 2 + extra
		for left > 0 {
			n := min(left, len(chunk))
			_, _ = gz.Write(chunk[:n])
			left -= n
		}
		if err := gz.Close(); err != nil {
			t.Fatal(err)
		}
		var out map[string]any
		for _, err := range []error{readGzipJSON(bytes.NewReader(b.Bytes()), &out), readTestWatermark(t, b.Bytes())} {
			if extra == 0 && err != nil {
				t.Fatal(err)
			}
			if extra == 1 && !errors.Is(err, ErrStateTooLarge) {
				t.Fatalf("oversize: %v", err)
			}
		}
	}
}
func TestStateWriterRejectsBeforeOverflow(t *testing.T) {
	w := stateWriter{writer: io.Discard, written: MaxStateDecodedBytes - 1}
	if _, err := w.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if n, err := w.Write([]byte("x")); n != 0 || !errors.Is(err, ErrStateTooLarge) {
		t.Fatalf("write: %d %v", n, err)
	}
}
func TestStateRejectsTrailingJSONAndCorruptGzip(t *testing.T) {
	for _, raw := range []string{"{}{}", "{"} {
		var b bytes.Buffer
		gz := gzip.NewWriter(&b)
		_, _ = gz.Write([]byte(raw))
		_ = gz.Close()
		var out any
		if readTestWatermark(t, b.Bytes()) == nil || readGzipJSON(&b, &out) == nil {
			t.Fatal("accepted malformed JSON")
		}
	}
	b, err := encodeGzipJSON(map[string]string{"a": "b"})
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-5] ^= 1
	var out any
	if readTestWatermark(t, b) == nil || readGzipJSON(bytes.NewReader(b), &out) == nil {
		t.Fatal("accepted corrupt gzip")
	}
}
