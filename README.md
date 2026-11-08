# FastMesh

[English](README.md) · [中文](README.zh.md)

FastMesh is an embeddable Go SDK for authenticated TCP, HTTP and gRPC between
services. Controllers distribute signed authorization and discovery plans;
application traffic travels directly between Nodes.

The next release is **v0.1.0**, with one plan contract: `fastmesh.plan.v1`.
This checkout is the source of truth. Use the Go toolchain declared in [go.mod](go.mod).

## Architecture and scope

FastMesh fits Go services that can explicitly integrate a communication SDK.
It provides SPIFFE mTLS, service discovery, publication and consumption permissions,
and local recovery of signed policy. Applications communicate directly; the
Controller coordinates enrollment, endpoint leases and policy updates.

```text
Host configuration → Controller → signed plan → Node verification + durable apply
                                               ↓
                                    application ↔ mTLS ↔ application
```

Nodes verify the original signed bytes, recipient, expiry and revision before
publishing execution state. Restart recovery reauthenticates saved policy and
preserves its original deadlines. A disconnected Node can use valid last-good
policy; expired policy cannot authorize new connections or HTTP/gRPC requests.
Established raw TCP connections are not reauthorized on every read or write;
policy updates reconcile their owner permissions.

| Capability | Current behavior |
| --- | --- |
| TCP | Authenticated direct connections, endpoint selection and circuit breaking |
| HTTP | Per-request method/path authorization, including segment-aware prefixes |
| gRPC | Method authorization for unary and streaming RPCs |
| Discovery | Complete signed plans, ACK/NACK, endpoint leases and reconnect backoff |
| Storage | Memory for volatile state; exclusively owned file store for one Controller; shared etcd for HA |
| Observability | Status and observer hooks; applications supply exporters |

The mesh uses `fastmesh.plan.v1`. Its ADS-style API is a project-specific contract;
mesh delta responses still carry complete signed plans. The `node` and `controller`
production package dependency graphs do not include etcd; the backend is selected
by the host. The root Go module currently includes the etcd adapter and its tests.

Enrollment commits the token use, complete Node and retry result atomically.
New Nodes persist a request ID before enrollment; retries recover the same active
Node while the token remains valid. Older clients without this ID only gain atomicity.
The signed plan body is limited to 3 MiB, the complete mesh protobuf response to
4 MiB, and decoded local snapshot/journal JSON to 64 MiB. Oversized plans fail
closed; existing last-good policy keeps its original expiry. Compiler cache retention
defaults to 64 MiB of serialized topology; this is not a process memory limit.

## Quick start

```sh
GOWORK=off go build -o /tmp/fastmesh-config ./examples/app-config
/tmp/fastmesh-config -role setup -dir /tmp/fastmesh-demo
/tmp/fastmesh-config -role controller -dir /tmp/fastmesh-demo
```

After `controller-ready`, start each application in a separate terminal:

```sh
/tmp/fastmesh-config -role app -name app-1 -dir /tmp/fastmesh-demo -http 127.0.0.1:8081
/tmp/fastmesh-config -role app -name app-2 -dir /tmp/fastmesh-demo -http 127.0.0.1:8082
curl -fsS http://127.0.0.1:8081/config
```

Setup requires a new directory and creates demo credentials valid for 24 hours.
See the [configuration example](examples/app-config/README.md) for updates and deployment.
After the release tag is published, applications can install it with
`go get github.com/fastabc/fastmesh@v0.1.0`.

## Integration

Configure `node.Config` with a SPIFFE identity source, Controller addresses and
identity allowlist, trust domain, tenant, join token and exclusive state directory.
Call `node.New`, `Start`, then bind a service with `Listen`, `HTTPHandler` or
`NewGRPCServer`; clients use `Dial`, `HTTPClient` or `GRPCDialOptions`. Always call `Close`.
The Controller owns Services, BindGrants, ServiceAccess rules and WorkloadGroups.
Providers register independent Endpoint leases through the authenticated Catalog API.
Close client idle connections when finished, and close a shared identity Source
after the Nodes using it have stopped. Observer callbacks must return promptly.

| Goal | Guide |
| --- | --- |
| HTTP service calls | [Orders and payments](examples/orders-payments/README.md) |
| gRPC methods | [gRPC](examples/grpc/README.md) |
| Application configuration | [App configuration](examples/app-config/README.md) |
| Observer hooks | [Observability example](examples/observability/README.md) |
| SDK configuration and ownership | [Integration](docs/integration.md) |
| Runtime and package boundaries | [Architecture](docs/architecture.md) |
| Trust, authorization and recovery | [Security](docs/security.md) |
| Wire contracts and generation | [API](api/README.md) |
| Local and CI checks | [Testing](docs/testing.md) |
| Release criteria | [v0.1.0](docs/release.md) |
| Architecture assessment and tradeoffs | [2026-11-07 review (Chinese)](docs/plans/archive/2026-11-07-architecture-review.md) |
| Refactoring specification and findings | [Spec](docs/plans/archive/2026-11-07-refactoring-spec.md) · [Issues](docs/plans/archive/2026-11-07-bugs.md) · [Phased plan](docs/plans/archive/2026-11-07-phased-plan.md) |

## Boundaries

FastMesh requires explicit Go SDK integration. It provides method/path HTTP
predicates, including segment-aware prefixes; it does not provide header/query/body
predicates or transparent interception. Controller HA uses shared etcd. Memory is
volatile; file storage has a single exclusive owner. Applications own business
configuration validation, persistence and rollout acknowledgements.
SDK upgrades ship with applications. Transparent cross-language interception,
general traffic routing and header/query/body policy are outside the current scope.
Implementation status and measurements are recorded in the
[execution report](docs/plans/archive/2026-11-08-execution-report.md); conditional work and
release validation remain explicit.

## Development

```sh
GOWORK=off go test ./...
GOWORK=off go test -race ./...
GOWORK=off go vet ./...
python3 test/smoke/examples.py
```

See [testing](docs/testing.md) for the required real-etcd and fuzz checks.

[MIT License](LICENSE).
