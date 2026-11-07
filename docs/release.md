# v0.1.0 release preparation

This is the initial release baseline. The supported mesh plan is a single v1
header/topology contract signed over original bytes. Its TypeURL is
`fastmesh.plan.v1` and enrollment capability is `mesh.plan.v1`.

## Included

- Embedded TCP, HTTP and gRPC with SPIFFE identity.
- Controller catalog, group authorization, independent Endpoint leases and discovery.
- Durable Node apply/recovery and revision fences.
- Memory, exclusively owned file, and shared-etcd storage.
- Bounded cache, connection draining and observer integration.

Use fresh state for this baseline. There is no old plan projection, capability
downgrade, inline endpoint migration, unversioned snapshot reader or YAML object
coercion. Protocol version v1 and module version v0.1.0 are separate identifiers.

## Release gate

Before tagging, require unit/integration tests, race detector, vet, isolated example
smoke, real-etcd tests without skips, fuzz seeds, protobuf regeneration without drift
and the CI vulnerability scan. Review the diff and security/recovery behavior.
Record actual local results in [review](review.md); CI still has to validate Linux.

A release is published only after these checks and creation of the `v0.1.0` tag.
This document and changelog do not create a tag or publish a module. Go API changes
remain possible during v0; consumers should pin a released tag.
