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
operation's context instead. Size a custom transport's idle pool to at least the
managed receiver concurrency plus its effective prefetch; settlement can overlap
the next claim wave. The SDK default owns a cloned transport with 64 idle
connections per host. `Client.Close` closes only that SDK-owned pool, never a
caller-supplied transport. The SDK shallow-clones a supplied `http.Client` to
enforce no-redirect RPC behavior without mutating the caller's client.

A custom `RoundTripper` must not retry queue or subscription deletion: doing so
would bypass the SDK's at-most-once deletion guard and could act on a later
resource recreated under the same name. A custom transport must also honor
`Request.Context()`. The managed receiver waits for an in-flight renewal to exit
before terminal settlement; it cannot safely abandon that wait and race Complete
or Abandon against the same token.

## Delivery and deletion

Delivery is at-least-once. Application side effects must be idempotent. A live
lock token fences settlement but cannot undo an external effect that happened
before a lease was lost.

Managed renewal uses a single-item RenewBatch when available so each successful
response supplies the committed deadline. Once an older broker reports that
route unsupported, the receiver permanently uses legacy Renew with a
conservative inferred deadline. Broker response time is translated to a local
monotonic deadline using `X-Mqlite-Server-Time-Ms`; measured response RTT starts
renewal earlier but never extends a still-usable exact mapped ownership horizon.
An already-expired conservative long-poll lower bound gets one bounded
confirmation before dispatch. The standard HTTP `Date` header is a
lower-precision fallback for older brokers. A coarse horizon that cannot fit its
one-second precision plus measured RTT uses bounded untrusted confirmation. If
an intermediary strips both headers, the Receiver reports degraded clock mode
once through its error handler, confirms ownership before dispatch, and keeps a
bounded untrusted cadence rather than trusting the raw absolute deadline. Exact
confirmed leases below the minimum renewal interval fail closed rather than
producing a renewal hot loop. For an existing queue created by an older client,
without either time header there is no relative lease duration to prove, so
preserve one header or keep the configured lock well above the 250ms degraded
cadence plus network latency.
Transient failures are retried only while the known lease is live. An untrusted
confirmation has a separate one-second request watchdog and conservative
cadence. New queue configurations must use zero (the broker default) or a lock
duration from 1s through 1h; `CreateQueue` rejects values outside that range
locally. Older queues outside the range can still exist, and a managed delivery
on a very short legacy lease fails closed when it cannot be renewed safely.
Queue updates affect future lease extensions and never shorten a deadline already
issued to a consumer. Recovery is silent; confirmed loss cancels that message's
handler context and reports one `ErrLockLost`.
Handlers must stop promptly when their context is canceled.

Managed Receive keeps one `receive_attempt_id` across transport backoff. This
replays a batch already claimed by the broker instead of claiming another one.
An HTTP 499 response produced by a broker or intermediary is retryable in this
managed loop only while the Receiver's controlling context remains live; caller
cancellation is never replayed.

The first failure gets one silent replay with no client-generated delay; a
second consecutive failure is reported and starts equal-jitter exponential
backoff with a 500ms base and a three-second generated cap. A valid
delta-seconds or HTTP-date `Retry-After` on HTTP 429 or 503 is a lower bound for
any retry and may exceed the generated cap. An HTTP-date value uses the `Date`
header from that same response as its clock reference, falling back to local
response receipt time when `Date` is absent or malformed. Zero does not suppress
normal later backoff, malformed or negative values are ignored, and caller
cancellation interrupts the wait. A successful Receive, including an empty
result, ends that retry round and the next Receive uses a fresh attempt ID.
Every replayed Peek-Lock batch is fenced by one broker renewal before handler
dispatch, even when continuing auto-renew is disabled. Complete and Abandon then
replay the exact same fenced request within a five-second bound; matching broker
settlement receipts make a lost-response replay idempotent. A handler slot is
released when the handler returns, while a separate total-in-flight slot remains
held through renewal shutdown and terminal settlement. The latter is capped at
concurrency plus effective prefetch (at most twice concurrency), so settlement
latency can overlap one claim wave without building an unbounded local lock
queue.

Queue and subscription deletion are name-addressed destructive operations. The
SDK disables redirect following for every RPC and never transparently retries
deletion. A deletion transport failure, redirect, invalid success
acknowledgement, or gateway 5xx returns `ErrOutcomeUnknown`. Reconcile with
`ListQueues` or lifecycle-aware subscription inventory before deciding what
happened. Never blindly replay a delayed deletion after recreating the same name.
Other redirected RPCs fail at the original endpoint without forwarding their
Bearer token or request body to the redirect destination. Their errors expose
only the HTTP status, not an untrusted `Location` header or response body.

A 2xx response is not accepted as proof of a commit-capable operation unless its
acknowledgement is present and structurally valid. Send, schedule cancel,
Complete, Abandon, Reject, Defer, Renew, CompleteBatch, RenewBatch, and
AtMostOnce Receive surface an invalid success acknowledgement as
`ErrOutcomeUnknown`; the response body is not reflected into the error.
Peek-Lock Receive can instead replay the
same attempt ID. Only the managed receiver automatically retries identical
fenced renewal, Complete, or Abandon requests.

`force` is explicit permission to discard retained messages. It changes the
empty-resource precondition, not the authorization level. A subscription TTL is
advance authorization for automatic discard at a fixed deadline.

## Untrusted responses

The client bounds response bodies. Lifecycle deletion and subscription inventory
also receive semantic validation: required fields, state combinations, page size,
scope, ordering, cursor progress, and timestamp relationships must be coherent.
Batch settlement responses must contain one ordered result per requested message;
a successful renewal must also contain its committed deadline. Unknown fields
remain forward compatible. An intermediary error without a JSON error code still
retains its HTTP status in the returned error. Callers can use `errors.As` with
`*mqlite.HTTPError` and inspect `StatusCode` without parsing error text;
`errors.Is` sentinel matching remains intact for broker error codes.
Resource-aware brokers add a non-secret `resource` discriminator to `not_found`.
For a missing queue, the resulting error matches both `ErrQueueNotFound` and the
broader `ErrNotFound`; old brokers without that additive field retain the legacy
general match.

## Go floor

Go 1.13 compatibility helps old source trees but does not make the Go 1.13
runtime safe. It is end-of-life. Build production consumers and use official CLI
artifacts with a currently supported, fully patched Go release.

Report vulnerabilities privately through the process in [SECURITY.md](../SECURITY.md).
