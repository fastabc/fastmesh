# Integration

Use the Go version in [go.mod](../go.mod). Full executable programs are in
[HTTP](../examples/orders-payments/README.md), [gRPC](../examples/grpc/README.md)
and [application configuration](../examples/app-config/README.md).

## Node

Supply `node.Config.Identity` with a SPIFFE Source, `Controllers` with reachable
addresses, `AllowedControllerIDs` with exact identities, and `TrustDomain` and
`Tenant` for admission. `JoinToken` is used for initial enrollment. Give each Node
its own `StateDir`; concurrent owners are rejected.

Call `node.New(cfg)` and `Start(ctx)`. Select a `node.ServiceRef{Service: ..., Port: ...}`.
Providers use `Listen`, `HTTPHandler` or `NewGRPCServer`; consumers use `Dial`,
`HTTPClient` or `GRPCDialOptions`. Wait for availability with `WaitService` where needed.
Always close the Node and client idle connections. `ConnectTimeout` bounds an
attempt; `DialTimeout` bounds the overall dial; `DrainTimeout` bounds listener
and connection draining. `Close` also waits for control/apply work, so blocking
hooks or observers can outlive that timeout. Configure pipeline hooks before
starting it, and honor callback cancellation. The caller owns the identity Source
and injected composition state; Node closes its standalone state after apply stops.
Wildcard listener addresses require a concrete advertised address.

## Controller

Provide `controller.Config` with ID, SPIFFE identity, trust domain, ObjectStore and
LeaseStore. Built-in backends are `store/memory`, `store/file` and `store/etcd`.
The object backend must provide coherent View, complete revision watches, and
atomic fenced object transactions for registration;
use the shared store conformance helpers for custom backends. Run `Serve(ctx, listener)`.
Use shared etcd for multiple Controllers and assign Region/CellLabel consistently.
Use the same backend for objects and leases. The etcd writer checks the leader lease
in its write transaction, including when the object store is wrapped with `store.NewMetered`.
Memory and file target a single Controller owner. Their generic single-object write
path checks the lease before writing. Registration uses an atomic fenced transaction.

Import the backend in the host that creates it. The root module maintains the etcd
adapter and tests together with the SDK. Applications can inspect their compiled
package dependencies with `GOWORK=off go list -deps .`.

Create a Service and its named protocol ports with `PutService`. BindGrant permits
a workload or WorkloadGroup to publish the service. ServiceAccess permits a caller
or group to consume it; an optional port and HTTP method/path rule narrow access.
No matching permission means deny. Provider endpoint leases bind to the enrolled
Node and authenticated workload, and renew without changing topology.

## Managed enrollment for new nodes

Managed enrollment lets an application start a Node with a server-authenticated
HTTPS endpoint and a short-lived token. The Node generates its key locally.
The Controller signs the assigned SPIFFE identity and returns the Node ID,
Controller addresses and identity allowlist, trust domain, tenant and mesh
trust bundle. Subsequent control and mesh connections use SPIFFE mTLS.

Enable it on the Controller with
`controller.WithEnrollment(controller.EnrollmentConfig{Issuer: issuer})`.
The issuer can be a `spiffe.X509Issuer` backed by an existing CA or an
implementation of `controller.SVIDIssuer`. Its leaf certificates must chain
to the bundle of the Controller's `spiffe.Source`. In HA, configure
`EnrollmentConfig.AllowedControllerIDs` with every trusted Controller ID and
route enrollment requests to the active Controller for the token's cell.
Upgrade all Controllers serving that cell before enabling managed enrollment.
For embedded controllers, set `Config.ControllerAddrs` to advertised gRPC
addresses.

Mount `mesh.EnrollmentHandler()` on the host's HTTPS server. Configure a
server certificate that matches the enrollment URL. For renewal, configure
`tls.RequestClientCert` on that listener and pass the actual TLS connection
through to the handler. The initial POST works without a client certificate.
The handler verifies renewal certificates against the mesh trust bundle.

The host authorizes the requested tenant and workload identity before invoking
the local token creation API on the leader:

```go
created, err := mesh.CreateEnrollmentToken(ctx, enrollment.ManagedTokenRequest{
    Tenant:     "acme",
    NodeName:   "payments-01",
    WorkloadID: "spiffe://example.org/workload/payments",
})
```

Deliver `created.Raw` securely. A zero TTL defaults to 15 minutes. The Controller
can revoke the token by ID with `RevokeEnrollmentToken`. Revoking access for an
already enrolled node uses `DisableNode` or `RevokeNode`.

On the new host, pass the HTTPS endpoint and token to `node.Join`, then use
the returned configuration with `node.New`. Enrollment options can also carry
reported labels and capabilities for the embedding application.

```go
cfg, err := node.Join(ctx, node.EnrollmentOptions{
    URL:      "https://control.example.org/mesh/enroll",
    Token:    token,
    StateDir: stateDir,
})
if err != nil {
    return err
}
n, err := node.New(cfg)
```

The HTTPS client uses the system roots by default. Supply `ServerRoots` for
a private HTTPS CA. Use the same state directory on a retry so the Node can
prove possession of the original key. After a successful join, `node.Join`
needs only `StateDir` and reloads the configuration offline. Use
`node.RenewEnrollment` with the endpoint and state directory before the current
SVID expires. Applications schedule renewal. The live identity source reads
the renewed material on subsequent calls.

## Host extensions and observability

`controller.WithDiscovery`, `WithBuilder` and `WithGRPCRegistrars` provide typed
composition. `node.WithProjection` connects host-owned apply/runtime integration;
see its Go documentation for the complete contract. Host resources use independent
TypeURLs and schemas. Mesh plans always use the single signed v1 contract.

Observer callbacks must return promptly. Hosts supply metrics exporters and
product-specific status owners; see the [observer example](../examples/observability/README.md).
Applications own business configuration semantics and rollout acknowledgements.
