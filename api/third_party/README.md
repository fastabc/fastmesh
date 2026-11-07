# Protobuf includes

`google/rpc/status.proto` is a compact local mirror of the Google RPC Status
schema, used only to resolve imports during protoc generation. It is not a
locally generated Go package: Go code uses
`google.golang.org/genproto/googleapis/rpc/status`, whose module version is pinned
in the repository's go.mod/go.sum.

The local fields and file options were checked against googleapis commit
`ebd1d23ac613b177828dad42ad8dfb13ba498279`:
[upstream status.proto](https://github.com/googleapis/googleapis/blob/ebd1d23ac613b177828dad42ad8dfb13ba498279/google/rpc/status.proto).
The local copy omits the upstream explanatory comments; it retains the Status
field numbers/types and language package options. This records a comparison
revision, not a claim that the original compact copy came from that commit.

Keep the protobuf namespace, field numbers and `go_package` aligned with the
upstream schema and the pinned Go runtime dependency when updating this include.
The standard `google/protobuf/*.proto` includes are supplied by protoc itself.
