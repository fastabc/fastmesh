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
Use shared etcd for multiple Controllers; assign Region/CellLabel consistently.

Create a Service and its named protocol ports with `PutService`. BindGrant permits
a workload or WorkloadGroup to publish the service. ServiceAccess permits a caller
or group to consume it; an optional port and HTTP method/path rule narrow access.
No matching permission means deny. Provider endpoint leases bind to the enrolled
Node and authenticated workload, and renew without changing topology.

## Host extensions and observability

`controller.WithDiscovery`, `WithBuilder` and `WithGRPCRegistrars` provide typed
composition. `node.WithProjection` connects host-owned apply/runtime integration;
see its Go documentation for the complete contract. Host resources use independent
TypeURLs and schemas. Mesh plans always use the single signed v1 contract.

Observer callbacks must return promptly. Hosts supply metrics exporters and
product-specific status owners; see the [observer example](../examples/observability/README.md).
Applications own business configuration semantics and rollout acknowledgements.
