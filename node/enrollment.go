package node

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/fastabc/fastmesh/identity/spiffe"
	"github.com/fastabc/fastmesh/internal/protocol/bootstrap"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/policy/state"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

// EnrollmentOptions selects the host's enrollment endpoint and local state.
// ServerRoots overrides the system trust store for the HTTPS server only.
type EnrollmentOptions struct {
	URL, Token, StateDir string
	ServerRoots          *x509.CertPool
	// Optional self-reported metadata for the initial registration.
	Labels                map[string]string
	Capabilities          []string
	AgentVersion, Runtime string
}

type enrollmentRecord struct {
	Key      []byte              `json:"key"`
	Response *bootstrap.Response `json:"response,omitempty"`
}

type enrollmentSource struct{ dir string }

func (s enrollmentSource) GetSVID(context.Context) (spiffe.SVID, error) {
	record, err := loadEnrollment(s.dir)
	if err != nil {
		return spiffe.SVID{}, err
	}
	return record.svid()
}

// Join enrolls a workload and returns the identity and Controller settings for
// New. Reopening a completed StateDir needs no token or network request. Join
// must run before New starts using the state directory.
func Join(ctx context.Context, opts EnrollmentOptions) (Config, error) {
	st, err := state.Open(opts.StateDir)
	if err != nil {
		return Config{}, err
	}
	defer st.Close()
	existingID, err := st.LoadNodeID()
	if err != nil && !errors.Is(err, state.ErrNotFound) {
		return Config{}, err
	}
	dir := enrollmentDir(opts.StateDir)
	identity, err := state.Open(dir)
	if err != nil {
		return Config{}, err
	}
	defer identity.Close()
	record, err := loadEnrollment(dir)
	if err != nil && !errors.Is(err, state.ErrNotFound) {
		return Config{}, err
	}
	if record.Response == nil {
		if existingID != "" {
			return Config{}, errors.New("mesh enrollment: state already has a node ID")
		}
		if opts.Token == "" {
			return Config{}, errors.New("mesh enrollment: token required")
		}
		if _, err := enrollmentURL(opts.URL); err != nil {
			return Config{}, err
		}
		if len(record.Key) == 0 {
			key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
			if err != nil {
				return Config{}, err
			}
			record.Key, err = x509.MarshalPKCS8PrivateKey(key)
			if err != nil {
				return Config{}, err
			}
			// Keep the same key across crashes, timeouts and response loss.
			if err := saveEnrollment(identity, record); err != nil {
				return Config{}, err
			}
		}
		if err := record.exchange(ctx, opts, nil); err != nil {
			return Config{}, err
		}
		if err := saveEnrollment(identity, record); err != nil {
			return Config{}, err
		}
	}
	if _, err := record.svid(); err != nil {
		return Config{}, err
	}
	resp := record.Response
	if existingID != "" && existingID != resp.NodeID {
		return Config{}, errors.New("mesh enrollment: persisted node ID does not match identity")
	}
	if existingID == "" {
		if err := st.SaveNodeID(resp.NodeID); err != nil {
			return Config{}, err
		}
	}
	return Config{StateDir: opts.StateDir, Identity: enrollmentSource{dir}, Tenant: resp.Tenant,
		TrustDomain: resp.TrustDomain, Controllers: resp.Controllers, AllowedControllerIDs: resp.AllowedControllerIDs}, nil
}

// RenewEnrollment uses the current SVID and key to renew a managed identity
// without a token. The returned Join identity source observes replacements on
// its next read, including while a Node is running.
func RenewEnrollment(ctx context.Context, opts EnrollmentOptions) error {
	if opts.StateDir == "" {
		return errors.New("mesh enrollment: state directory required")
	}
	dir := enrollmentDir(opts.StateDir)
	st, err := state.Open(dir)
	if err != nil {
		return err
	}
	defer st.Close()
	record, err := loadEnrollment(dir)
	if err != nil {
		return err
	}
	svid, err := record.svid()
	if err != nil {
		return err
	}
	cert := &tls.Certificate{Certificate: svid.CertChain, PrivateKey: svid.Key}
	if err := record.exchange(ctx, opts, cert); err != nil {
		return err
	}
	return saveEnrollment(st, record)
}

func enrollmentDir(stateDir string) string {
	return filepath.Join(stateDir, "enrollment-identity")
}

func loadEnrollment(dir string) (enrollmentRecord, error) {
	data, err := state.ReadEnrollmentIdentity(dir)
	if err != nil {
		return enrollmentRecord{}, err
	}
	var record enrollmentRecord
	err = json.Unmarshal(data, &record)
	return record, err
}

func saveEnrollment(st *state.Store, record enrollmentRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return st.SaveEnrollmentIdentity(data)
}

func (r enrollmentRecord) key() (crypto.Signer, error) {
	parsed, err := x509.ParsePKCS8PrivateKey(r.Key)
	if err != nil {
		return nil, spiffe.ErrInvalidSVID
	}
	key, ok := parsed.(*ecdsa.PrivateKey)
	if !ok || key.Curve != elliptic.P256() {
		return nil, spiffe.ErrInvalidSVID
	}
	return key, nil
}

func (r enrollmentRecord) svid() (spiffe.SVID, error) {
	if r.Response == nil {
		return spiffe.SVID{}, spiffe.ErrInvalidSVID
	}
	key, err := r.key()
	if err != nil {
		return spiffe.SVID{}, err
	}
	resp := r.Response
	leaf, rawID, err := bootstrap.VerifyChain(resp.Chain, resp.Bundle, key.Public(), resp.TrustDomain)
	if err != nil {
		return spiffe.SVID{}, err
	}
	id, err := spiffe.ParseID(rawID)
	if err != nil {
		return spiffe.SVID{}, err
	}
	return spiffe.SVID{ID: id, NotAfter: leaf.NotAfter, Key: key, CertChain: resp.Chain, Bundle: resp.Bundle}, nil
}

func (r *enrollmentRecord) exchange(ctx context.Context, opts EnrollmentOptions, cert *tls.Certificate) error {
	endpoint, err := enrollmentURL(opts.URL)
	if err != nil {
		return err
	}
	key, err := r.key()
	if err != nil {
		return err
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		return err
	}
	request := bootstrap.Request{CSR: csr}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: opts.ServerRoots}
	if cert != nil {
		tlsConfig.Certificates = []tls.Certificate{*cert}
		request.NodeID = r.Response.NodeID
	} else {
		request.Token, request.Labels, request.Capabilities = opts.Token, opts.Labels, opts.Capabilities
		request.AgentVersion, request.Runtime = opts.AgentVersion, opts.Runtime
	}
	body, err := json.Marshal(request)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	transport := &http.Transport{TLSClientConfig: tlsConfig, Proxy: http.ProxyFromEnvironment}
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport: transport, Timeout: 15 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("mesh enrollment: HTTPS request returned %s", res.Status)
	}
	data, err := io.ReadAll(io.LimitReader(res.Body, bootstrap.MaxResponseSize+1))
	if err != nil {
		return err
	}
	if len(data) > bootstrap.MaxResponseSize {
		return errors.New("mesh enrollment: HTTPS response too large")
	}
	var resp bootstrap.Response
	if err := json.Unmarshal(data, &resp); err != nil {
		return err
	}
	if err := validateEnrollmentResponse(resp); err != nil {
		return err
	}
	next := enrollmentRecord{Key: r.Key, Response: &resp}
	issued, err := next.svid()
	if err != nil {
		return err
	}
	if cert != nil {
		prior, err := r.svid()
		if err != nil {
			return err
		}
		if resp.NodeID != r.Response.NodeID || resp.Tenant != r.Response.Tenant || issued.ID != prior.ID {
			return errors.New("mesh enrollment: renewal changed the workload binding")
		}
	}
	r.Response = &resp
	return nil
}

func enrollmentURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return nil, errors.New("mesh enrollment: valid HTTPS endpoint required")
	}
	return u, nil
}

func validateEnrollmentResponse(resp bootstrap.Response) error {
	if !model.ExactName(resp.NodeID) || !model.ExactName(resp.Tenant) || len(resp.Controllers) == 0 || len(resp.AllowedControllerIDs) == 0 {
		return errors.New("mesh enrollment: incomplete controller response")
	}
	for _, addr := range resp.Controllers {
		if strings.TrimSpace(addr) == "" {
			return errors.New("mesh enrollment: empty controller address")
		}
	}
	for _, raw := range resp.AllowedControllerIDs {
		id, err := spiffeid.FromString(raw)
		if err != nil || id.String() != raw || id.TrustDomain().String() != resp.TrustDomain {
			return errors.New("mesh enrollment: invalid controller identity")
		}
	}
	return nil
}
