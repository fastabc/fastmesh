package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConfigValidation(t *testing.T) {
	for name, input := range map[string]string{
		"missing flag":   `{"revision":1,"greeting":"hello"}`,
		"zero revision":  `{"revision":0,"greeting":"hello","checkout_enabled":true}`,
		"unknown field":  `{"revision":1,"greeting":"hello","checkout_enabled":true,"typo":1}`,
		"trailing value": `{"revision":1,"greeting":"hello","checkout_enabled":true} {}`,
		"empty greeting": `{"revision":1,"greeting":"","checkout_enabled":true}`,
		"too large":      strings.Repeat(" ", maxConfigBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeConfig(strings.NewReader(input)); err == nil {
				t.Fatal("invalid input accepted")
			}
		})
	}
	if _, err := decodeConfig(strings.NewReader(`{"revision":1,"greeting":"hello","checkout_enabled":false}`)); err != nil {
		t.Fatal(err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSyncAppliesConfigAndRetainsLastGood(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.json")
	write := func(s string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(s), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(`{"revision":1,"greeting":"v1","checkout_enabled":true}`)
	server := httptest.NewServer(configHandler(path))
	defer server.Close()
	target, _ := url.Parse(server.URL)
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		r = r.Clone(r.Context())
		r.URL.Scheme, r.URL.Host = target.Scheme, target.Host
		return server.Client().Transport.RoundTrip(r)
	})}
	state := &configState{}
	probe := func(path string) *httptest.ResponseRecorder {
		t.Helper()
		w := httptest.NewRecorder()
		state.handler("app-1").ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		return w
	}
	if probe("/").Code != http.StatusServiceUnavailable {
		t.Fatal("served without valid config")
	}
	if changed, err := state.refresh(context.Background(), client); err != nil || !changed {
		t.Fatalf("initial apply: %v %v", changed, err)
	}
	if probe("/checkout").Code != http.StatusOK {
		t.Fatal("initial checkout not enabled")
	}
	previous := state.current.Load()
	if changed, err := state.refresh(context.Background(), client); err != nil || changed {
		t.Fatalf("ETag revalidation: %v %v", changed, err)
	}
	if !state.current.Load().LastSuccess.After(previous.LastSuccess) {
		t.Fatal("304 did not refresh contact")
	}
	for _, bad := range []string{
		`{"revision":2,"greeting":"broken"}`,
		`{"revision":1,"greeting":"changed","checkout_enabled":false}`,
	} {
		write(bad)
		previous = state.current.Load()
		if _, err := state.refresh(context.Background(), client); err == nil {
			t.Fatal("invalid update accepted")
		}
		if state.current.Load() != previous {
			t.Fatal("invalid update replaced last-good config")
		}
	}
	write(`{"revision":2,"greeting":"v2","checkout_enabled":false}`)
	if changed, err := state.refresh(context.Background(), client); err != nil || !changed {
		t.Fatalf("update: %v %v", changed, err)
	}
	if cfg := state.current.Load(); cfg.Revision != 2 || cfg.Greeting != "v2" || cfg.CheckoutEnabled {
		t.Fatal("mixed configuration")
	}
	if probe("/checkout").Code != http.StatusServiceUnavailable {
		t.Fatal("disabled checkout still available")
	}
	write(`{"revision":1,"greeting":"v1","checkout_enabled":true}`)
	previous = state.current.Load()
	if _, err := state.refresh(context.Background(), client); err == nil {
		t.Fatal("revision rollback accepted")
	}
	if state.current.Load() != previous {
		t.Fatal("rollback replaced last-good config")
	}
	server.Close()
	if _, err := state.refresh(context.Background(), client); err == nil {
		t.Fatal("expected disconnected server")
	}
	if state.current.Load() != previous {
		t.Fatal("disconnect replaced last-good config")
	}
}

func TestETagMismatchDoesNotPublish(t *testing.T) {
	state := &configState{}
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		w := httptest.NewRecorder()
		w.Header().Set("ETag", `"incorrect"`)
		fmt.Fprint(w, `{"revision":1,"greeting":"hello","checkout_enabled":true}`)
		return w.Result(), nil
	})}
	if _, err := state.refresh(context.Background(), client); err == nil {
		t.Fatal("mismatched ETag accepted")
	}
	if state.current.Load() != nil {
		t.Fatal("unvalidated value published")
	}
}
