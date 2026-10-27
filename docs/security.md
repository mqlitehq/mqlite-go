# Security model

## Credentials and transport

Bearer tokens authorize the whole broker, not an individual queue. Use separate
brokers when tenants require a hard isolation boundary. Give producers `send`,
consumers `listen`, and operational tooling `manage` only when required by the
selected broker release.

Use HTTPS outside a trusted local network. `mqlites://` selects HTTPS and supplies
the MQLite default port when omitted. The SDK never logs a token and does not put
one in returned errors. Prefer `WithToken` or `MQLITE_TOKEN`; a command-line token
can leak through shell history and process inspection.

Malformed connection strings are reported without returning the original DSN.
The `mqlite://` and `mqlites://` forms accept a token as the username component
but reject a separate password component, malformed query escaping, and an empty
trailing query marker.

Callers may pass a configured `*http.Client` with `WithHTTPClient` for custom
roots, proxies, or transport controls. Do not set a short global timeout on that
client: Receive uses request contexts for long polling. Set deadlines on each
operation's context instead. A custom `RoundTripper` must not retry queue or
subscription deletion: doing so would bypass the SDK's at-most-once deletion
guard and could act on a later resource recreated under the same name.
A custom transport must also honor `Request.Context()`. The managed receiver
waits for an in-flight renewal to exit before terminal settlement; it cannot
safely abandon that wait and race Complete or Abandon against the same token.

## Delivery and deletion

Delivery is at-least-once. Application side effects must be idempotent. A live
lock token fences settlement but cannot undo an external effect that happened
before a lease was lost.

Queue and subscription deletion are name-addressed destructive operations. The
SDK disables redirect following and never transparently retries them. A transport
failure, redirect, invalid success acknowledgement, or gateway 5xx returns
`ErrOutcomeUnknown`. Reconcile with `ListQueues` or lifecycle-aware subscription
inventory before deciding what happened. Never blindly replay a delayed deletion
after recreating the same name.

`force` is explicit permission to discard retained messages. It changes the
empty-resource precondition, not the authorization level. A subscription TTL is
advance authorization for automatic discard at a fixed deadline.

## Untrusted responses

The client bounds response bodies. Lifecycle deletion and subscription inventory
also receive semantic validation: required fields, state combinations, page size,
scope, ordering, cursor progress, and timestamp relationships must be coherent.
Batch settlement responses must contain one ordered result per requested message;
a successful renewal must also contain its committed deadline. Unknown fields
remain forward compatible.

## Go floor

Go 1.13 compatibility helps old source trees but does not make the Go 1.13
runtime safe. It is end-of-life. Build production consumers and use official CLI
artifacts with a currently supported, fully patched Go release.

Report vulnerabilities privately through the process in [SECURITY.md](../SECURITY.md).
