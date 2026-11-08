# Changelog

## v0.1.0 — pending release

Initial FastMesh SDK baseline:

- Embedded TCP, HTTP and gRPC with SPIFFE mutual authentication.
- Controller enrollment, catalog authorization, groups and independent Endpoint leases.
- Single `fastmesh.plan.v1` header/topology protocol with raw-byte asymmetric signatures.
- Transactional apply, durable recovery, revision fences, revocation and draining.
- Memory, file and etcd storage; Controller HA through shared etcd.
- Current behavior tests, isolated examples and source-based documentation.

Reliability and performance changes before release:

- Atomic enrollment with an optional persisted request ID; revoked/expired tokens
  and deleted/disabled Nodes cannot replay a result. Preserve committed retry records
  on rollback. Custom Controller stores must support fenced object transactions.
- Change coalescing uses a fixed window from the first event, including during churn.
- Expired leases cannot renew their token; reacquisition creates a new token.
- Mesh body/envelope limits are 3/4 MiB; decoded local state is limited to 64 MiB.
  Previously accepted oversized local state now fails closed and needs operator review.
- Bound serialized topology cache retention and expose byte accounting. Large
  standalone plans use service indexes; memory transactions copy only changed objects.

This baseline uses fresh state and has no previous-version compatibility paths.
See [release preparation](docs/release.md) for validation and publication criteria.
