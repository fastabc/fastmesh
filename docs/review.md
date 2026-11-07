# v0.1.0 code review and validation

Reviewed against the current source on 2026-11-07. Archived documentation was not
used as a specification. Local validation passed; the release remains untagged.

## Decisions and resolved findings

- Keep the original-byte signature and header/topology design as the single v1
  protocol. Remove dual projections, V2 names and capability downgrade. Both
  network apply and persisted recovery authenticate the same raw-byte contract.
- Remove unsigned/digest signature compatibility, including the unused bare
  FetchSnapshot signature field. `verify.Content` checks content consistency;
  asymmetric verification authenticates the sender. Protobuf field 4 is reserved.
- Use independent Endpoint records and retained generation tombstones. Remove
  Service.status migration, generation backfill and migration expiry work.
- Keep reported Node metadata in status; remove top-level status fallback.
  The bootstrap-owned SPIFFE identity remains the immutable admission binding.
- Require complete revision watches when constructing a Controller. Individual
  object events cannot establish transaction boundaries for catalog compilation.
- Remove YAML coercion, unversioned snapshot reading and file namespace migration.
  Remove now-unused YAML dependencies. Fresh state is required for this baseline.
- Replace historical package/name/API snapshots with current wire-envelope,
  rejected-input, backend-contract and documentation-link checks. Update affected
  behavior fixtures; retain security, concurrency, durability and recovery coverage.
- Remove the nonexistent cookbook requirement from smoke. Rebuild current docs
  and remove archived links and stale validation claims from active guides.

## Local results

Environment: macOS arm64, Go 1.27.3; dedicated temporary etcd 3.7.3.

| Check | Result |
| --- | --- |
| Uncached full suite | 621 tests/subtests passed; 21 etcd-dependent skips; no failures |
| Full uncached race suite | Passed |
| Dedicated etcd race integration | 99 tests/subtests passed; zero skips/failures |
| go vet | Passed |
| Isolated consumer smoke | HTTP authorization, gRPC method authorization and two-app configuration/recovery passed |
| Plan decoder fuzz | 190,382 executions over a 5-second requested run; passed |
| Protobuf regeneration | Pinned generator v1.36.12; repeated generation byte-identical |
| govulncheck v1.6.0 | Rebuilt with project toolchain; no vulnerabilities found |
| Dependency tidy / whitespace | Passed |
| Documentation links | Passed as part of release tests |

Unreachable-code analysis still reports exported SDK/host extension entry points
and their private callees, including host discovery watching and telemetry hooks.
In-repository reachability alone does not prove these public extension APIs are dead;
they remain available to external consumers. Removed orphaned test helpers.

## Publication boundary

Linux CI, its pinned etcd 3.7.2 service, and scheduled fuzz durations still need to
run on the reviewed commit. Local short fuzzing does not replace scheduled fuzzing.
No tag, remote push or release publication was performed. See
[release criteria](release.md) before creating `v0.1.0`.
