# MQLite Remote Client for Go

[![CI](https://github.com/mqlitehq/mqlite-go/actions/workflows/ci.yml/badge.svg)](https://github.com/mqlitehq/mqlite-go/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/mqlitehq/mqlite-go.svg)](https://pkg.go.dev/github.com/mqlitehq/mqlite-go)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

MQLite Remote Client for Go (`mqlite-go`) is the official, dependency-free,
remote-only Go client for an [MQLite](https://github.com/mqlitehq/mqlite) broker.
It keeps a Go 1.13 source floor for older applications without pulling the
embedded SQLite engine, libSQL/Turso drivers, CGO, or any third-party runtime
module into their builds.

Use the main `github.com/mqlitehq/mqlite` module when you need an embedded queue,
same-database transactions, or the combined local/remote API. Use this module
when the application only talks to a broker over HTTP and a low Go floor matters.

## Install

```sh
go get github.com/mqlitehq/mqlite-go@v0.1.1
```

The module declares `go 1.13` and is tested with Go 1.13.15, Go 1.18.10, and
current stable. Go 1.13 itself is end-of-life: this is a source compatibility
promise, not a recommendation to build production binaries with an unmaintained
toolchain. Official `mqlitec` release binaries use a supported current Go stable.

## Quick start

```go
package main

import (
	"context"
	"log"
	"time"

	mqlite "github.com/mqlitehq/mqlite-go"
)

func main() {
	ctx := context.Background()
	client, err := mqlite.Open(ctx, "https://broker.example.com",
		mqlite.WithToken("mqk_..."))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	seq, err := client.SendOne(ctx, "orders", mqlite.OutMessage{
		Body:      []byte(`{"order_id":42}`),
		MessageID: "order-42",
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("sent sequence %d", seq)

	messages, err := client.Receive(ctx, "orders", mqlite.RecvOpts{
		Max:  10,
		Wait: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	for _, message := range messages {
		if err := process(message.Body); err != nil {
			_ = message.Abandon(ctx)
			continue
		}
		if err := message.Complete(ctx); err != nil {
			log.Fatal(err)
		}
	}
}

func process(body []byte) error { return nil }
```

`Client.Receiver` provides a managed consume loop with bounded concurrency,
idempotent receive retries, optional lock renewal, and automatic Complete or
Abandon based on the handler result. `WithConcurrency(C)` limits simultaneous
handler calls. After a handler returns, its terminal settlement may overlap the
next claim wave; total handler-plus-settlement work is bounded at
`C + effective prefetch` (at most `2C`, and `2C` by default), while each Receive
still claims no more messages than reserved handler slots. Auto-renew schedules
from the delivery's remaining lease. It uses the deadline-bearing batch renewal
route when the broker supports it and permanently falls back to legacy
single-message renewal otherwise. Transient renewal failures are retried with
backoff while the known lease is live; a recovered retry is not reported. If a
transient failure exhausts the known lease, or the broker reports that the lock
was lost, the message-specific handler context is canceled and the error handler
receives one `ErrLockLost`. Permanent authorization or resource errors retain
their specific sentinel and stop `Run`. The configured lock duration must still
leave enough time for the network round trip. The receiver translates the
broker's millisecond server-time header onto the local monotonic clock and uses
measured response RTT to start renewal early; an exact mapped lease horizon
that is still outside the safety window remains the hard request watchdog. A
conservative lower bound already consumed by a long poll gets one bounded
confirmation before dispatch. Older brokers use the standard HTTP `Date` header.
Its one-second precision is charged against the lease, and a coarse
horizon that cannot safely fit that precision plus measured RTT uses bounded
untrusted confirmation. If an intermediary strips both headers, the SDK reports
degraded clock mode once through `WithErrorHandler`, confirms the lease before
dispatch, and keeps using bounded untrusted confirmation instead of scheduling
from the raw absolute `LockedUntil`. Exact leases too short to support the 50ms
minimum renewal interval fail closed rather than hot-looping.
The no-header fallback has no relative lease duration to prove: preserve either
time header, or configure the lock well above its 250ms cadence plus network
latency. New queue configurations must use zero (the broker default) or a lock
duration from 1s through 1h; `CreateQueue` rejects values outside that range
before sending a request. Older queues outside the range can still exist, and a
managed delivery on a very short legacy lease fails closed when it cannot be
renewed safely. Updating a queue changes future lease extensions only; it does
not shorten a `LockedUntil` deadline already issued to a consumer.
If no response-time sample yields a usable deadline, a successful renewal still
releases the handler and the receiver uses a conservative confirmation cadence.
The first later renewal failure cancels the handler because no safe retry window
is known.
Shutdown is cooperative. Trusted renewal requests are bounded by the known lease
window and untrusted confirmations use a one-second watchdog. Handlers and custom
HTTP transports must honor request contexts, and an error handler must return
promptly. The receiver waits for an in-flight renewal to exit before it settles
the same lock token.
Complete and Abandon use bounded exact-request retries so a lost broker response
can be reconciled through the broker's settlement receipt without running the
handler again.
When a broker or intermediary returns HTTP 499 for a managed request, the
Receiver treats that response as transient only while its own controlling
context remains live. Receive reuses the same attempt ID; canceling the caller
context never triggers another request.
Managed Receive gives the first transient failure in a round no client-generated
delay and replays it with that same attempt ID. Later failures use equal-jitter
exponential delays with a 500ms base and a three-second generated cap. A valid
`Retry-After` value on HTTP 429 or 503 may raise one retry delay up to 60 seconds.
When it raises the generated delay, the error handler is called before waiting,
including on the first failure. HTTP-date values use the `Date` header from the
same response as their clock reference, falling back to local response receipt
time when that header is absent or malformed. `Retry-After: 0` never suppresses
the normal later backoff. Canceling `Run` interrupts every reconnect wait. A
successful Receive, including an empty result, ends the retry round; the next
Receive uses a fresh attempt ID.

## Topics and subscriptions

For a long-lived listener, provision one stable subscription without a TTL and
run one managed receiver for the subscription name:

```go
if err := admin.Subscribe(ctx, "sentinel-events", "sentinel-monitor", nil); err != nil {
	log.Fatal(err)
}

runErr := listener.Receiver("sentinel-monitor",
	mqlite.WithConcurrency(4),
	mqlite.WithPrefetch(4),
	mqlite.WithAutoRenew(),
	mqlite.WithErrorHandler(reportReceiverError),
).Run(ctx, func(ctx context.Context, message *mqlite.Message) error {
	return processIdempotently(ctx, message)
})
if runErr != nil && !errors.Is(runErr, context.Canceled) {
	log.Fatal(runErr)
}
```

Call `Run` once and cancel its context to stop it; the SDK performs the repeated
20-second long polls, bounded dispatch, lock renewal, Complete, and immediate
Abandon. The receiver target is `sentinel-monitor`, not `sentinel-events`.
Against broker v0.4.1, use a manager to provision the subscription and a
listener credential for the running worker.

Processes sharing one subscription are competing consumers: each fan-out copy
goes to one of them. Give each logical observer a distinct subscription when
every observer needs its own copy. A publish has no topic-level history: a
subscription created later receives no earlier messages. On an existing topic,
a successful publish matching no subscription filter returns sequence zero and
stores no copy; after the last active subscription is gone, the implicit topic
is unavailable.

Use application counters around the handler and `WithErrorHandler`, plus
`client.Stats(ctx, subscription)` and broker metrics for active, locked, and
dead-lettered messages. The SDK does not add always-on telemetry to the hot
path. Reuse the client and receiver, make the handler idempotent, do not retain
message bodies after processing, and keep the error callback non-blocking.

For a temporary listener, set a fixed subscription TTL:

```go
err := client.SubscribeWithOptions(ctx, "replies", "worker-7", nil,
	mqlite.SubscribeOptions{TTL: 15 * time.Minute})
if err != nil {
	log.Fatal(err)
}
```

A subscription TTL is a fixed lifetime from first creation. Retries, filter
updates, traffic, and receives do not extend it. Expiry preauthorizes retained
messages to be discarded during bounded cleanup. Explicit deletion, with or
without `force`, is an administrative operation.

Managed handlers intentionally choose only Complete (`nil`) or immediate
Abandon (non-nil error). Use an explicit `Receive` loop when one handler needs
delayed Abandon, Reject, Defer, or receive-and-delete semantics.

Authorization is enforced by the broker. The currently released MQLite v0.4.1
requires `manage` for subscription provisioning; later broker releases may allow
`listen` according to their release notes. This SDK does not weaken or emulate
server permissions.

## Included API

- Send, schedule, cancel, receive, deferred receive, peek, and queue statistics.
- Complete, abandon/backoff, reject, defer, renew, and batch complete/renew.
- Managed `Receiver.Run` with concurrency, prefetch, and auto-renew.
- Queue create/list/delete, DLQ redrive/purge, and broker status.
- Subscription create/filter/TTL, bounded inventory, filter test, and deletion.
- Strict error mapping with `errors.Is` sentinels and `errors.As` to
  `*mqlite.HTTPError` for ordinary non-deletion HTTP error responses.

Against a resource-aware broker, a missing queue matches both
`mqlite.ErrQueueNotFound` and the broader `mqlite.ErrNotFound`. Older brokers do
not send the additive `resource` discriminator, so their legacy response can only
match `ErrNotFound`.

The initial release intentionally omits embedded mode, transactional outbox,
canonical observability decoding, and managed access-key issuance. Those remain
available in the main MQLite module and broker CLI.

## Diagnostic CLI

Release archives include `mqlitec`, a small remote diagnostic client:

```sh
export MQLITE_ENDPOINT=https://broker.example.com
export MQLITE_TOKEN=mqk_...

mqlitec status
mqlitec send --message-id order-42 orders '{"order_id":42}'
mqlitec receive --wait 5s orders
```

See [CLI reference](docs/cli.md). The token flag exists for controlled tests;
prefer `MQLITE_TOKEN` so credentials do not appear in process listings or shell
history.

## Reliability boundary

MQLite delivery is at-least-once. Handlers must be idempotent. Lock tokens fence
settlement, and managed receive retries retain the same attempt ID across
backoff so a lost response does not claim another batch. A replay whose lease is
already expired on the local clock is fenced by one broker renewal before
dispatch, even when managed auto-renew is disabled. The SDK does not follow HTTP
redirects for any RPC. Destructive queue/subscription deletion is sent at most
once: redirects, transport failures, and gateway 5xx responses return
`ErrOutcomeUnknown` for read-only reconciliation rather than automatic replay.
Other redirected RPCs fail at the configured endpoint without sending the token
or body to the redirect destination.
Commit-capable sends, scheduling cancellation, Complete, Abandon, Reject,
Defer, Renew, `CompleteBatch`, and `RenewBatch` report `ErrOutcomeUnknown` when
a successful HTTP response does not acknowledge the requested operation
coherently; the write may already have committed. An invalid success
acknowledgement for AtMostOnce Receive is also outcome-unknown because
receive-and-delete may have removed messages. Peek-Lock Receive remains
replayable with the same attempt ID. The managed receiver replays only exact
fenced renewal, Complete, or Abandon requests within its retry budget.
HTTP 499 from a broker or intermediary is retryable in these managed loops only
while their controlling context remains live; caller cancellation is not replayed.
Managed Receive bounds reconnect latency with no client-generated delay on the
first replay, followed by equal-jitter exponential delays (500ms base,
three-second generated cap). HTTP 429 and 503 may raise any individual delay
through `Retry-After`, up to 60 seconds; a raised delay is reported before the
wait. Hints never change the attempt ID, and cancellation interrupts the wait.

`Receiver.Run` always uses Peek-Lock and replays an identical Complete or
Abandon request for up to five seconds after a retryable failure. A process
crash, an expired lock, or a settlement outage beyond that bound can still cause
redelivery rather than silent removal. Explicit
`Receive(..., RecvOpts{AtMostOnce: true})`, subscription TTL expiry, force
deletion, purge, cancel, and configured DLQ retention are deliberate data
removal boundaries. Message TTL expiry enters the DLQ by default and discards
only when the queue disables dead-letter-on-expire. A send transport failure can
be ambiguous, so producers that cannot recreate work should use an application
outbox and stable message identity; consumers must still be idempotent.

All SDK times cross the wire as UTC epoch milliseconds. Resource names are the
public identity; callers must serialize same-name deletion and recreation.

## Documentation

- [Compatibility and support](docs/compatibility.md)
- [Security model](docs/security.md)
- [`mqlitec` reference](docs/cli.md)
- [Binary-size methodology](docs/size.md)
- [Release process](docs/releasing.md)
- [MQLite broker documentation](https://mqlitehq.github.io/)

## Development

```sh
go build ./...
go test -race -count=1 ./...
go vet ./...
./test/integration.sh
./tools/measure-size.sh
```

CI proves that `go list -m all` contains only this module. See
[CONTRIBUTING.md](CONTRIBUTING.md) for the complete compatibility rules.

MIT licensed. Protocol code is adapted from the MIT-licensed MQLite main
repository and retains the same copyright notice.
