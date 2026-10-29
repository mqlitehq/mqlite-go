# Changelog

## Unreleased

- Make managed delivery recovery exact: retain one receive-attempt ID across
  transient retries, replay ambiguous Complete/Abandon operations, retry lease
  renewal within the known lease window, and cancel the message-scoped handler
  once ownership can no longer be confirmed.
- Translate broker response time (`X-Mqlite-Server-Time-Ms`, with HTTP `Date` as
  an older-broker fallback) into a conservative local monotonic lease deadline.
  Renewal request watchdogs are independent from renewal cadence, so ordinary
  cross-region RTT does not cancel every skewed-clock delivery; initial lease
  confirmation is bounded as well.
- Fence a replayed receive with one broker renewal even when managed auto-renew
  is disabled. This avoids discarding a broker-valid lease solely because the
  client wall clock is ahead.
- Reuse a tuned owned HTTP connection pool, reject every RPC redirect, expose
  HTTP status through `HTTPError`, and bound successful and error responses.
  When the process-wide `http.DefaultTransport` is a wrapper, the wrapper is
  preserved and remains externally owned; callers are responsible for its pool
  sizing and lifecycle.
- Let `errors.Is(err, ErrNotFound)` also match queue-not-found responses while
  retaining the more specific `ErrQueueNotFound` match.

## v0.1.1 — 2026-10-28

- Present the package as MQLite Remote Client for Go while preserving its
  repository, module, import, package, and CLI names.
- Derive managed auto-renew cadence from half the initially observed remaining
  lease, including sub-two-second leases; retry one transient renewal and stop
  the renewal loop before terminal settlement.
- Sanitize malformed connection-string errors, reject ambiguous userinfo and
  query forms, and request JSON explicitly from HTTP endpoints.
- Reject nil batch inputs and incomplete, reordered, or internally inconsistent
  batch settlement acknowledgements instead of panicking or accepting malformed
  success responses.
- Compare linked size with an equivalent handwritten HTTP/JSON Status client so
  the Go network stack is not misreported as SDK-specific overhead.
- Document and continuously verify the supported long-lived topic-listener
  pattern: a permanent subscription consumed by one managed `Receiver.Run`,
  with explicit competing-consumer, fan-out, monitoring, and at-least-once
  boundaries.

## v0.1.0 — 2026-10-27

- Add the standard-library-only remote MQLite SDK with a Go 1.13 source floor.
- Cover queue data-plane operations, fenced settlement, managed receiving,
  queue/subscription lifecycle, fixed subscription TTL, DLQ operations, and
  broker status.
- Add `mqlitec`, a current-Go-built diagnostic client.
- Verify Go 1.13.15, Go 1.18.10, current stable, a real v0.4.1 broker, dependency
  isolation, release binaries, and linked size.
