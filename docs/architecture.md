# Architecture

FastMesh has two runtime owners: an embedded Node for each application and a
Controller for coordination. The Controller is outside the application data path.

1. Bootstrap binds an enrolled Node ID to the authenticated workload and tenant.
2. The Controller reads a coherent object View and watches complete revision batches.
3. Its tenant index compiles recipient-specific service bindings, inbound permissions,
   authorized owners and live endpoints. A bounded cache reuses topology on heartbeats.
4. Discovery signs the exact header/topology JSON body and sends a complete plan.
5. The Node verifies bytes before decoding, checks recipient, expiry and revision,
   then persists prepare/commit decisions before publishing execution state.
6. TCP, HTTP and gRPC connections authenticate peers directly. Revocation and draining
   reconcile listeners, pending handshakes and established connections.

## Package ownership

| Package | Responsibility |
| --- | --- |
| `node` | Application clients, handlers, lifecycle, readiness and bounded shutdown |
| `controller` | Leadership, catalog mutation, enrollment, reconciliation and discovery |
| `model` | Service, access, group, endpoint, Node and plan contracts |
| `identity/spiffe` | Identity source, SVID and trust bundle validation |
| `policy` | Snapshot signatures, verification, transactional apply and durable state |
| `store` | Object CAS, coherent View, revisions, watches, leases and backend contracts |
| `internal/node/dataplane` | Peer transport, authorization and connection reconciliation |
| `internal/controller` | Catalog compiler, endpoint lifecycle, index, expiry and plan cache |
| `api` | Protobuf Discovery, Bootstrap and Catalog contracts |

## Persistence and failure behavior

Endpoint leases and retained generation tombstones are independent objects;
Service.status is not an endpoint database. Reported Node metadata lives in
Node.status; the bootstrap-owned SPIFFE binding remains on the Node object.
Controller object stores and Node policy state have separate ownership.

Memory storage loses state on restart. File storage uses exclusive locking and
atomic durable writes. Shared etcd provides Controller coordination and revision
watches. Controller indexing requires the RevisionWatcher contract. Watch loss
invalidates the index and rebuilds a baseline using watch-before-View ordering.

Node restart reauthenticates the persisted signed body and restores revision fences.
It does not extend signed deadlines. Expired policy cannot admit new connections.
Hosts own product schemas, administrative APIs and telemetry exporters.
