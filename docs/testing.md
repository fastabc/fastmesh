# Testing

Run from the repository root with the toolchain in [go.mod](../go.mod).

```sh
GOWORK=off go test -count=1 -timeout=180s ./...
GOWORK=off go test -race -count=1 -timeout=180s ./...
GOWORK=off go vet ./...
python3 test/smoke/examples.py
```

Package tests cover recipient/signature checks, revision fences, apply journal
recovery, endpoint generation/expiry, authorization, draining, cancellation,
Controller failover, cache concurrency and storage durability. Tests exercise the
single current protocol rather than snapshots of past names or package layouts.
The smoke runner builds isolated consumer modules and runs authenticated HTTP,
gRPC method authorization, and two-application configuration update/recovery.

## Real etcd

Use a dedicated disposable etcd, never production data:

```sh
FASTMESH_ETCD_ENDPOINTS=http://127.0.0.1:2379 GOWORK=off \
  go test -race -count=1 -timeout=180s ./store/etcd ./controller ./controller/enrollment
```

Without the environment variable, backend integration tests skip. Release validation
must run them with no skips. CI retains JSON results and requires the revision-watch
and shared-watch handoff, registration atomicity and server lease expiry scenarios
to execute. Production dependency tests also reject etcd imports from node/controller.

## Fuzz and generated code

The scheduled fuzz workflow exercises plan decoding, unknown permissions, executable
plan filtering, principals, HTTP prefixes and raw-body signature verification.
For a local boundary check:

```sh
GOWORK=off go test ./internal/protocol/planwire -run '^$' -fuzz '^FuzzDecodePlan$' -fuzztime=10s
```

Generation instructions are in [API](../api/README.md). CI regenerates protobufs
and rejects unexpected generated changes. CI also runs govulncheck and records
unreachable-function analysis; exported SDK functions can have external callers.
