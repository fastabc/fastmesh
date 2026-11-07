# Changelog

## v0.1.0 — pending release

Initial FastMesh SDK baseline:

- Embedded TCP, HTTP and gRPC with SPIFFE mutual authentication.
- Controller enrollment, catalog authorization, groups and independent Endpoint leases.
- Single `fastmesh.plan.v1` header/topology protocol with raw-byte asymmetric signatures.
- Transactional apply, durable recovery, revision fences, revocation and draining.
- Memory, file and etcd storage; Controller HA through shared etcd.
- Current behavior tests, isolated examples and source-based documentation.

This baseline uses fresh state and has no previous-version compatibility paths.
See [release preparation](docs/release.md) for validation and publication criteria.
