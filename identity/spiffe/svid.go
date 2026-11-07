package spiffe

import (
	"context"
	"crypto"
	"errors"
	"net/url"
	"strings"
	"time"
)

const scheme = "spiffe"

var (
	// ErrInvalidTrustDomain is returned when a trust domain is empty or is not
	// representable as the authority part of a SPIFFE ID.
	ErrInvalidTrustDomain = errors.New("spiffe: invalid trust domain")

	// ErrInvalidID is returned when a workload ID is not a valid SPIFFE URI.
	ErrInvalidID = errors.New("spiffe: invalid id")
)

// TrustDomain is the authority segment in a SPIFFE ID.
type TrustDomain string

// ParseTrustDomain validates a trust domain string.
func ParseTrustDomain(raw string) (TrustDomain, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.Contains(raw, "://") || strings.ContainsAny(raw, "/?#") {
		return "", ErrInvalidTrustDomain
	}
	// The placeholder path lets url.Parse validate the authority consistently.
	u, err := url.Parse(scheme + "://" + raw + "/x")
	if err != nil || u.Host != raw {
		return "", ErrInvalidTrustDomain
	}
	return TrustDomain(strings.ToLower(raw)), nil
}

// String returns the canonical trust domain.
func (td TrustDomain) String() string { return string(td) }

// ID is a parsed SPIFFE workload identity.
type ID struct {
	TrustDomain TrustDomain `json:"trust_domain"`
	Path        string      `json:"path"`
}

// ParseID parses a SPIFFE URI such as spiffe://example.org/workload/node-a.
func ParseID(raw string) (ID, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != scheme || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return ID{}, ErrInvalidID
	}
	if u.Path == "" || u.Path == "/" || !strings.HasPrefix(u.Path, "/") {
		return ID{}, ErrInvalidID
	}
	td, err := ParseTrustDomain(u.Host)
	if err != nil {
		return ID{}, ErrInvalidID
	}
	return ID{TrustDomain: td, Path: u.Path}, nil
}

// String returns the canonical SPIFFE URI form.
func (id ID) String() string {
	if id.TrustDomain == "" || id.Path == "" {
		return ""
	}
	return scheme + "://" + id.TrustDomain.String() + id.Path
}

// SVID is the current workload credential material delivered by SPIRE.
type SVID struct {
	ID        ID
	NotAfter  time.Time
	CertChain [][]byte
	Bundle    [][]byte
	Key       crypto.Signer
}

// Source abstracts the SPIRE Workload API for controller and agent code.
type Source interface {
	GetSVID(ctx context.Context) (SVID, error)
}
