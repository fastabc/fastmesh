package verify

import (
	"context"
	"fmt"

	"github.com/fastabc/fastmesh/identity/spiffe"
	"github.com/fastabc/fastmesh/policy/snapshot"
)

// FromSource follows root rotation and binds every snapshot to its recipient.
func FromSource(source spiffe.Source, allowed []string, nodeID, tenant string) SignatureVerifyFunc {
	allowed = append([]string(nil), allowed...)
	return func(ctx context.Context, snap *snapshot.Snapshot, sig snapshot.Signature) error {
		if snap == nil || snap.NodeID != nodeID || snap.Tenant != tenant {
			return fmt.Errorf("%w: wrong recipient", ErrVerificationFailed)
		}
		if source == nil || len(allowed) == 0 {
			return fmt.Errorf("%w: missing identity or controller allowlist", ErrVerificationFailed)
		}
		material, err := source.GetSVID(ctx)
		if err != nil {
			return err
		}
		if len(material.Bundle) == 0 {
			return fmt.Errorf("%w: missing trust bundle", ErrVerificationFailed)
		}
		roots, err := spiffe.BundlePool(material)
		if err != nil {
			return err
		}
		return (SnapshotVerifier{NodeID: nodeID, Tenant: tenant, Roots: roots, TrustDomain: material.ID.TrustDomain, AllowedControllerIDs: allowed}).Verify(ctx, snap, sig)
	}
}
