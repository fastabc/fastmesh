package etcd

import (
	"encoding/pem"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadTLSConfigUsesExplicitCAAndTLS12Floor(t *testing.T) {
	t.Parallel()
	server := httptest.NewTLSServer(nil)
	defer server.Close()
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	config, err := loadTLSConfig(caFile, "", "")
	if err != nil {
		t.Fatalf("loadTLSConfig: %v", err)
	}
	if config.RootCAs == nil || config.MinVersion == 0 {
		t.Fatalf("TLS config=%+v want explicit roots and minimum version", config)
	}
}

func TestLoadTLSConfigRejectsPartialClientIdentity(t *testing.T) {
	t.Parallel()
	if _, err := loadTLSConfig("ca.pem", "client.pem", ""); err == nil {
		t.Fatal("loadTLSConfig accepted certificate without key")
	}
}
