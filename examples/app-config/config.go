package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync/atomic"
	"time"
)

const maxConfigBytes = 64 << 10

type appConfig struct {
	Revision        int64  `json:"revision"`
	Greeting        string `json:"greeting"`
	CheckoutEnabled bool   `json:"checkout_enabled"`
}

func decodeConfig(r io.Reader) (appConfig, error) {
	var cfg appConfig
	data, err := io.ReadAll(io.LimitReader(r, maxConfigBytes+1))
	if err != nil {
		return cfg, err
	}
	if len(data) > maxConfigBytes {
		return cfg, errors.New("config exceeds 64 KiB")
	}
	// Pointer fields distinguish omitted values from an intentional false.
	var input struct {
		Revision        *int64  `json:"revision"`
		Greeting        *string `json:"greeting"`
		CheckoutEnabled *bool   `json:"checkout_enabled"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		return cfg, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return cfg, errors.New("config must contain exactly one JSON object")
	}
	if input.Revision == nil || input.Greeting == nil || input.CheckoutEnabled == nil || *input.Revision < 1 || len(*input.Greeting) == 0 || len(*input.Greeting) > 200 {
		return cfg, errors.New("revision >= 1, greeting (1..200 bytes) and checkout_enabled are required")
	}
	return appConfig{*input.Revision, *input.Greeting, *input.CheckoutEnabled}, nil
}

func (c appConfig) etag() string {
	data, _ := json.Marshal(c)
	return fmt.Sprintf(`"%x"`, sha256.Sum256(data))
}

// Read on every poll so atomic file replacement takes effect without restart.
func configHandler(path string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/config" {
			http.NotFound(w, r)
			return
		}
		f, err := os.Open(path)
		if err != nil {
			http.Error(w, "config unavailable", http.StatusServiceUnavailable)
			return
		}
		defer f.Close()
		cfg, err := decodeConfig(f)
		if err != nil {
			http.Error(w, "invalid source config", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("ETag", cfg.etag())
		if r.Header.Get("If-None-Match") == cfg.etag() {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(cfg)
	})
}

type appliedConfig struct {
	appConfig
	LastSuccess time.Time `json:"last_success"`
}

type configState struct{ current atomic.Pointer[appliedConfig] }

// One polling goroutine owns writes; handlers see one complete immutable value.
func (s *configState) refresh(ctx context.Context, client *http.Client) (bool, error) {
	previous := s.current.Load()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://app-config/config", nil)
	if err != nil {
		return false, err
	}
	if previous != nil {
		req.Header.Set("If-None-Match", previous.etag())
	}
	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotModified && previous != nil {
		next := *previous
		next.LastSuccess = time.Now().UTC()
		s.current.Store(&next)
		return false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("config fetch status %d", resp.StatusCode)
	}
	cfg, err := decodeConfig(resp.Body)
	if err != nil {
		return false, err
	}
	if resp.Header.Get("ETag") != cfg.etag() {
		return false, errors.New("config ETag mismatch")
	}
	if previous != nil && (cfg.Revision < previous.Revision || cfg.Revision == previous.Revision && cfg != previous.appConfig) {
		return false, errors.New("config revision must increase when content changes")
	}
	s.current.Store(&appliedConfig{cfg, time.Now().UTC()})
	return previous == nil || cfg != previous.appConfig, nil
}

func (s *configState) handler(name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cfg := s.current.Load()
		if cfg == nil {
			http.Error(w, "waiting for first valid config", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/config":
			_ = json.NewEncoder(w).Encode(cfg)
		case "/", "/checkout":
			if r.URL.Path == "/checkout" && !cfg.CheckoutEnabled {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
			_ = json.NewEncoder(w).Encode(struct {
				Instance string `json:"instance"`
				appConfig
			}{name, cfg.appConfig})
		default:
			http.NotFound(w, r)
		}
	})
}
