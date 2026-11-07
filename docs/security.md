# Security model

Production peers use SPIFFE X.509 identities and mutual TLS. Controller IDs are
explicitly allowlisted. Enrollment binds a Node ID to its transport-authenticated
workload; a request cannot replace that binding with a claimed Node or tenant.
`DevInsecure` is for local development only.

## Signed plan boundary

The only mesh contract is `fastmesh.plan.v1`, with capability `mesh.plan.v1`.
DSSE PAE binds the type and exact received JSON body. The Node verifies an
asymmetric Controller signature and current trust before decoding. It checks
recipient tenant/Node/workload, deadline and durable input revision watermark.
Unsigned input, digest-only signatures and unsupported plan types are rejected.
The topology hash is an authenticated declaration, not a replacement for a signature.

Unknown execution requirements cannot broaden authorization. Invalid entries are
filtered before execution, while valid siblings may continue. Raw signed input is
retained for recovery; parsed journal fields are reconstructed from verified bytes.

## Authorization and lifecycle

BindGrant controls publication; ServiceAccess controls consumption. Group membership
is resolved during compilation. Established outbound connections follow authorized
owners; new connections also require usable endpoint leases. Generation tombstones
prevent delayed registrations from reviving an instance during retention.
HTTP authorization rejects ambiguous paths, dot segments and encoded separators.
PathPrefix matches segments: `/api` includes `/api/x`, but excludes `/apix`.

## Durable recovery

A Node exclusively owns its state directory. Prepare/commit records and last-good
state preserve revision fences. Only versioned last-good envelopes are accepted.
Recovery authenticates the exact saved plan and checks recipient and revision.
An expired leaf signer may be authenticated at its previous validity only for local
recovery, with execution bounded by that certificate's original expiry; this cannot
admit fresh network input or extend policy validity. Non-leaf trust must remain valid.

Protect identity material, state directories, Controller stores and join tokens.
Plan validity bounds offline authorization; tune it to deployment requirements.
