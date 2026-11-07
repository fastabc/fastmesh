package dataplane

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/identity/spiffe"
	"github.com/fastabc/fastmesh/model"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func TestRuntimeHTTPPrefixRejectsTraversal(t *testing.T) {
	client, server, _ := lifecycleIdentities(t)
	clientID := spiffeid.RequireFromString(client.svid.ID.String())
	serverID := spiffeid.RequireFromString(server.svid.ID.String())
	_, p := runtimePlans(clientID, serverID)
	p.Inbound[0].RequiresL7 = true
	p.Inbound[0].Port.Protocol = "http"
	p.Inbound[0].Allow[0].L7 = &model.HTTPMatch{Method: "GET", PathPrefix: "/public/"}
	r := New(server, time.Second)
	installPlan(t, r, p)
	root := t.TempDir()
	for name, body := range map[string]string{"public/ok.txt": "public", "admin/secret.txt": "SECRET"} {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(root, name)), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	ref := ServiceRef{Service: "payments", Port: "api"}
	lis, err := r.Listen(context.Background(), ref, "127.0.0.1:0", true)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: r.HTTPHandler(ref, http.FileServer(http.Dir(root))), ConnContext: PeerContext}
	go func() { _ = httpServer.Serve(lis) }()
	defer httpServer.Close()
	tlsConfig, err := spiffe.ClientTLSConfig(context.Background(), client, []string{serverID.String()}, "test")
	if err != nil {
		t.Fatal(err)
	}
	for path, code := range map[string]int{
		"/public/ok.txt": http.StatusOK, "/admin/secret.txt": http.StatusForbidden,
		"/public/../admin/secret.txt":     http.StatusForbidden,
		"/public/%2e%2e/admin/secret.txt": http.StatusForbidden,
		"/public/%2E%2e/admin/secret.txt": http.StatusForbidden,
		"/public//ok.txt":                 http.StatusForbidden, "/public%2Fok.txt": http.StatusForbidden,
	} {
		t.Run(path, func(t *testing.T) {
			conn, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", lis.Addr().String(), tlsConfig)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(time.Second))
			if _, err := fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: mesh\r\nConnection: close\r\n\r\n", path); err != nil {
				t.Fatal(err)
			}
			response, err := http.ReadResponse(bufio.NewReader(conn), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != code || strings.Contains(string(body), "SECRET") {
				t.Fatalf("status=%d want=%d body=%q", response.StatusCode, code, body)
			}
		})
	}
}
