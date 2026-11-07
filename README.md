# FastMesh

[English](README.md) · [中文](README.zh.md)

FastMesh is an embeddable Go SDK for authenticated TCP, HTTP and gRPC between
services. Controllers distribute signed authorization and discovery plans;
application traffic travels directly between Nodes.

The next release is **v0.1.0**, with one plan contract: `fastmesh.plan.v1`.
This checkout is the source of truth. Use the Go toolchain declared in [go.mod](go.mod).

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

## Boundaries

FastMesh requires explicit Go SDK integration. It provides method/path HTTP
predicates, including segment-aware prefixes; it does not provide header/query/body
predicates or transparent interception. Controller HA uses shared etcd. Memory is
volatile; file storage has a single exclusive owner. Applications own business
configuration validation, persistence and rollout acknowledgements.

## Development

```sh
GOWORK=off go test ./...
GOWORK=off go test -race ./...
GOWORK=off go vet ./...
python3 test/smoke/examples.py
```

See [testing](docs/testing.md) for the required real-etcd and fuzz checks.

[MIT License](LICENSE).
