package main

import (
	"context"
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/fastabc/fastmesh/identity/spiffe"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

const domain = "example.test"

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
	if err := os.Mkdir(dir, 0700); err != nil {
		return fmt.Errorf("create fresh demo directory: %w", err)
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
	for i, role := range []string{"controller", "app-1", "app-2"} {
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
	return os.WriteFile(filepath.Join(dir, "app.json"), []byte(`{
  "revision": 1,
  "greeting": "hello from configuration v1",
  "checkout_enabled": true
}
`), 0600)
}
