# v0.1.0 code review and validation

## Follow-up validation on 2026-11-18

The review covered changes from `454b700` through `7a865f0`. The exported
source passed 691 tests and subtests in each uncached run, with and without
the race detector, on macOS arm64 with Go 1.27.4 and a dedicated etcd 3.7.3.
Both runs executed all four integration cases required by CI with zero skips.
The stale leader's direct and metered writers both rejected deletion of the
successor's object. An interrupted batch reported its confirmed count and
completed the remaining deletion on retry.

All three isolated consumer examples built and ran without etcd in their
production package graphs. The minimum Go version was raised to 1.27.4 after
the reachable vulnerability scan flagged the HTTP server in Go 1.27.3. The
new scan, vet, all six 60-second fuzz targets and pinned protobuf regeneration
passed locally. [Go vulnerability record](https://pkg.go.dev/vuln/GO-2026-6859)

In a 600-object, four-tenant etcd fixture, 30 six-range views issued 30 data
reads, down from 180, and returned 13,342,770 protobuf bytes, down from
80,056,620. At 1,000 services, ten same-toolchain trials reduced median
Catalog decode time from 4.142 to 2.943 ms and allocated bytes by 35.93%.
End-to-end latency varied by scenario. For 20 changes that forced a watch
reconnect, change-to-ACK P95 went from 49.09 to 51.67 ms.

Keep one Go module with an independently imported etcd adapter. The compile
profile supports a separate experiment with per-service grant indexes.
Hosted Linux CI, its pinned etcd 3.7.2 service and production HA rollout
remain release-environment checks. [Release criteria](release.md)

## Review on 2026-11-07

### Decisions and resolved findings

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

### Local results

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

### Publication boundary

Linux CI, its pinned etcd 3.7.2 service, and scheduled fuzz durations still need to
run on the reviewed commit. Local short fuzzing does not replace scheduled fuzzing.
No tag, remote push or release publication was performed. See
[release criteria](release.md) before creating `v0.1.0`.
