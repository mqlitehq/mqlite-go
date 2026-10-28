# Changelog

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
