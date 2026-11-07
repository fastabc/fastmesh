# Authenticated gRPC

A Controller, a payments health server and an orders client run as separate
processes. The policy allows unary Health/Check and rejects streaming Health/Watch.
Only public fastmesh APIs are used. Requires the Go version in go.mod.

From the repository root:

```sh
GOWORK=off go build -o /tmp/fastmesh-grpc ./examples/grpc
/tmp/fastmesh-grpc -role setup -state /tmp/grpc-demo
/tmp/fastmesh-grpc -role controller -state /tmp/grpc-demo
```

After `controller-ready`, open another terminal:

```sh
/tmp/fastmesh-grpc -role payments -state /tmp/grpc-demo
```

After `payments-ready`, run the client:

```sh
/tmp/fastmesh-grpc -role orders -state /tmp/grpc-demo
```

Expected output:

```text
health: SERVING
watch: PermissionDenied
```

`NewGRPCServer` supplies mesh transport credentials and both authorization
interceptors; register services before Serve. `GRPCDialOptions` routes through
the authenticated Node connection. Methods are authorized as POST plus their
complete `/package.Service/Method` path. Stream authorization is rechecked at
message boundaries. A client-side Watch object is not an authorization result;
the first Recv observes the denial.

Use a fresh `-state` directory; setup refuses to overwrite a PKI. Demo credentials
last 24 hours and the enrollment token lasts one hour. Each role owns a separate
Node state directory. `-controller` selects another reachable control address;
pass the same value to all processes. For cross-host use, distribute only the
CA and each host's own leaf/key/token, configure reachable listen/advertise
addresses in the embedding application, and provision production identities.
The defaults intentionally demonstrate local processes only.

Stop payments and the Controller with Ctrl-C, then remove only your demo state.
The example uses Stop for bounded shutdown. For production, first withdraw
readiness and give GracefulStop an external budget before forcing Stop.

`python3 test/smoke/examples.py` copies this directory into an independent module,
uses random loopback ports, runs the three processes and checks both responses.
Mid-stream revocation regressions are in node's gRPC tests; see the
[gRPC cookbook](../../docs/security.md).
