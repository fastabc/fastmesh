// orders-payments is a three-process example using only fastmesh's public API.
package main

import (
	"context"
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/fastabc/fastmesh/controller"
	"github.com/fastabc/fastmesh/controller/enrollment"
	"github.com/fastabc/fastmesh/identity/spiffe"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/node"
	"github.com/fastabc/fastmesh/store/file"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

const domain = "example.test"

func main() {
	role := flag.String("role", "orders", "setup, controller, payments or orders")
	dir := flag.String("state", "demo-state", "demo credential and state directory")
	requestPath := flag.String("path", "/pay", "orders request path")
	address := flag.String("controller", "127.0.0.1:19190", "controller listen/dial address")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, *role, *dir, *address, *requestPath); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, role, dir, address, requestPath string) error {
	if role == "setup" {
		return setup(dir)
	}
	source := fileIdentity{dir: dir, role: role}
	if role == "controller" {
		backing, err := file.Open(filepath.Join(dir, "controller-store.json"))
		if err != nil {
			return err
		}
		defer backing.Close()
		c, err := controller.New(controller.Config{ID: "demo-controller", Objects: backing, Leases: backing, Identity: source, TrustDomain: domain})
		if err != nil {
			return err
		}
		listener, err := net.Listen("tcp", address)
		if err != nil {
			return err
		}
		done := make(chan error, 1)
		go func() { done <- c.Serve(ctx, listener) }()
		for !c.Active() {
			select {
			case err := <-done:
				return err
			case <-ctx.Done():
				return nil
			case <-time.After(time.Millisecond):
			}
		}
		if err := c.PutService(ctx, model.Service{Tenant: "default", Name: "payments", RequiresL7: true, Ports: []model.ServicePort{{Name: "api", Number: 8443, Protocol: "http"}}}); err != nil {
			return err
		}
		if err := c.PutBindGrant(ctx, "payments", model.BindGrant{Tenant: "default", Service: "payments", Workload: model.Principal{ID: id("payments")}}); err != nil {
			return err
		}
		if err := c.PutServiceAccess(ctx, "orders-payments", model.ServiceAccess{Tenant: "default", Service: "payments", Source: model.Principal{ID: id("orders")}, L7: &model.HTTPMatch{Method: "GET", PathPrefix: "/pay"}}); err != nil {
			return err
		}
		token, err := c.CreateJoinToken(ctx, enrollment.CreateRequest{Tenant: "default", TTL: time.Hour})
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, "join-token"), []byte(token.Raw), 0600); err != nil {
			return err
		}
		fmt.Println("controller-ready")
		return <-done
	}
	if role != "orders" && role != "payments" {
		return errors.New("unknown role")
	}
	token, err := os.ReadFile(filepath.Join(dir, "join-token"))
	if err != nil {
		return err
	}
	n, err := node.New(node.Config{Identity: source, Controllers: []string{address}, AllowedControllerIDs: []string{id("controller").String()}, TrustDomain: domain, Tenant: "default", JoinToken: strings.TrimSpace(string(token)), StateDir: filepath.Join(dir, role), ConnectTimeout: time.Second, DrainTimeout: time.Second})
	if err != nil {
		return err
	}
	defer n.Close()
	if err := n.Start(ctx); err != nil {
		return err
	}
	service := node.ServiceRef{Service: "payments", Port: "api"}
	if role == "payments" {
		listener, err := n.HTTPHandler(ctx, service, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { _, _ = io.WriteString(w, "paid\n") }))
		if err != nil {
			return err
		}
		defer listener.Close()
		fmt.Println("payments-ready")
		<-ctx.Done()
		return nil
	}
	client, err := n.HTTPClientWithProtocol(service, "http")
	if err != nil {
		return err
	}
	requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := n.WaitService(requestCtx, service); err != nil {
		return err
	}
	defer client.CloseIdleConnections()
	for {
		req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, "https://payments"+requestPath, nil)
		if err != nil {
			return err
		}
		response, err := client.Do(req)
		if err == nil {
			defer response.Body.Close()
			if response.StatusCode != http.StatusOK {
				return fmt.Errorf("payments status: %d", response.StatusCode)
			}
			_, err = io.Copy(os.Stdout, response.Body)
			return err
		}
		select {
		case <-requestCtx.Done():
			return requestCtx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func id(role string) spiffeid.ID {
	return spiffeid.RequireFromString("spiffe://" + domain + "/" + role)
}

// Reload the leaf, private key and bundle on every source read. Real deployments
// can pass the Workload API source directly without changing the node API.
type fileIdentity struct{ dir, role string }

func (s fileIdentity) GetSVID(context.Context) (spiffe.SVID, error) {
	read := func(name string) ([]byte, error) {
		raw, err := os.ReadFile(filepath.Join(s.dir, name))
		if err != nil {
			return nil, err
		}
		block, _ := pem.Decode(raw)
		if block == nil {
			return nil, errors.New("invalid PEM material")
		}
		return block.Bytes, nil
	}
	der, err := read(s.role + ".pem")
	if err != nil {
		return spiffe.SVID{}, err
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		return spiffe.SVID{}, err
	}
	keyDER, err := read(s.role + "-key.pem")
	if err != nil {
		return spiffe.SVID{}, err
	}
	key, err := x509.ParsePKCS8PrivateKey(keyDER)
	if err != nil {
		return spiffe.SVID{}, err
	}
	signer, ok := key.(crypto.Signer)
	if !ok {
		return spiffe.SVID{}, errors.New("unsupported private key")
	}
	bundle, err := read("ca.pem")
	if err != nil {
		return spiffe.SVID{}, err
	}
	svid, err := spiffe.X509SVIDFromCertificates([]*x509.Certificate{leaf}, signer, spiffe.VerifyOptions{TrustDomain: domain})
	svid.Bundle = [][]byte{bundle}
	return svid, err
}

// setup creates short-lived demo credentials; it refuses to overwrite a PKI.
func setup(dir string) error {
	if _, err := os.Stat(filepath.Join(dir, "ca.pem")); err == nil {
		return errors.New("demo PKI already exists")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: now.Add(-time.Minute), NotAfter: now.Add(24 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, pub, key)
	if err != nil {
		return err
	}
	ca, err = x509.ParseCertificate(der)
	if err != nil {
		return err
	}
	write := func(name, kind string, der []byte) error {
		return os.WriteFile(filepath.Join(dir, name), pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der}), 0600)
	}
	if err := write("ca.pem", "CERTIFICATE", der); err != nil {
		return err
	}
	for i, role := range []string{"controller", "orders", "payments"} {
		pub, leafKey, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return err
		}
		uri, _ := url.Parse(id(role).String())
		leaf := &x509.Certificate{SerialNumber: big.NewInt(int64(i + 2)), NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}, URIs: []*url.URL{uri}}
		der, err := x509.CreateCertificate(rand.Reader, leaf, ca, pub, key)
		if err != nil {
			return err
		}
		if err := write(role+".pem", "CERTIFICATE", der); err != nil {
			return err
		}
		keyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
		if err != nil {
			return err
		}
		if err := write(role+"-key.pem", "PRIVATE KEY", keyDER); err != nil {
			return err
		}
	}
	return nil
}
