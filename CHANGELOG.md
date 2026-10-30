# Changelog

## Unreleased

- Reject negative and sub-second nonzero queue lock durations before sending a
  CreateQueue request. Zero still selects the broker's documented default.
- Make managed delivery recovery exact: retain one receive-attempt ID across
  transient retries, replay ambiguous Complete/Abandon operations, retry lease
  renewal within the known lease window, and cancel the message-scoped handler
  once ownership can no longer be confirmed.
- Translate broker response time (`X-Mqlite-Server-Time-Ms`, with HTTP `Date` as
  an older-broker fallback) into a conservative local monotonic lease deadline.
  Measured response RTT starts renewal early without extending a still-usable
  exact mapped lease watchdog. An already-expired conservative long-poll lower
  bound gets one bounded confirmation before dispatch. Coarse `Date` horizons
  use bounded untrusted confirmation, pathologically short exact leases fail
  closed instead of hot-looping, and a missing-header degraded mode is reported
  once through the error handler and never schedules from an untrusted raw
  absolute deadline. Document that this fallback requires a lock comfortably
  above its 250ms cadence plus network latency when both headers are stripped.
- Retry a broker-produced HTTP 499 in managed exact-operation loops while the
  controlling context remains live. This prevents an older broker or
  intermediary cancellation response from stopping a Receiver without retrying
  caller cancellation, and Receive retains the same attempt ID.
- Stop managed receiving before releasing handler capacity when renewal returns
  a permanent authentication or permission error. A synchronous error callback
  can no longer leave a window for one more claim with rejected credentials.
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
