package state

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"testing"
)

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
		err := readGzipJSON(bytes.NewReader(b.Bytes()), &out)
		if extra == 0 && err != nil {
			t.Fatal(err)
		}
		if extra == 1 && !errors.Is(err, ErrStateTooLarge) {
			t.Fatalf("oversize: %v", err)
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
		if err := readGzipJSON(&b, &out); err == nil {
			t.Fatal("accepted malformed JSON")
		}
	}
	b, err := encodeGzipJSON(map[string]string{"a": "b"})
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-5] ^= 1
	var out any
	if err := readGzipJSON(bytes.NewReader(b), &out); err == nil {
		t.Fatal("accepted corrupt gzip")
	}
}
