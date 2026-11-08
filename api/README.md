# Wire API

The API tree contains protobuf contracts and their generated Go packages.
Application entry points live in `node` and `controller`; generated
messages describe the wire protocol rather than the application's domain model.

```text
api/
  proto/fastmesh/catalog/v1/catalog.proto       Catalog source
  proto/fastmesh/discovery/v1/discovery.proto   Discovery and Bootstrap source
  catalog/v1/                                 generated Go package catalogv1
  discovery/v1/                               generated Go package discoveryv1
  third_party/                                external protoc includes
```

The protobuf namespaces are `fastmesh.catalog.v1` and `fastmesh.discovery.v1`.
Discovery and Bootstrap share a protobuf package and descriptor owner. Internal
implementation packages can evolve independently of these protocol namespaces.

## Generation

Use the same versions as `.github/workflows/ci.yml`:

- protoc 36.2 (reports `libprotoc 36.2`; generated headers use `v7.36.2`).
- protoc-gen-go v1.36.12.
- protoc-gen-go-grpc v1.5.1.

With protoc available, install the plugins and generate from the repository root:

```sh
GOWORK=off go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
GOWORK=off go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
GOWORK=off go generate ./api/...
git diff --exit-code -- api
```

The plugins must be on PATH. Edit `.proto` sources and regenerate; do not edit
`*.pb.go` by hand. Generation is offline once the tools are installed: standard
protobuf imports come from protoc, and google/rpc/status.proto comes from
[third_party](third_party/README.md). CI regenerates and rejects unexpected diffs.

## Mesh plan payload

Discovery carries one complete `fastmesh.plan.v1` JSON resource with `header` and
`topology` fields. Header binds tenant, workload, Node ID, input revision, deadline
and topology hash. The enrollment capability is `mesh.plan.v1`. A detached
asymmetric signature uses DSSE PAE over the TypeURL and exact body bytes.
Verify before decoding. Unknown permission requirements fail closed.

`model.WirePlan` describes the wire body; `model.Plan` is local execution state.
The internal planwire codec does not verify signatures. Full and delta Discovery
both replace a complete signed plan. Catalog endpoint messages carry serving and
terminating state plus generation fencing. See [security](../docs/security.md).

## Enrollment retries

Bootstrap `RegisterRequest.request_id` (field 7) is optional: 16 random bytes as
32 hexadecimal characters, persisted before sending. The server keys the result by
join-token ID, transport-authenticated workload and request ID, and commits it with
token consumption and complete Node creation. Retry while the token remains valid;
a revoked/expired token or deleted/disabled Node rejects replay. Omit the field for
legacy atomic registration without response-loss recovery. Preserve the field number
and committed result records during a rollback.
