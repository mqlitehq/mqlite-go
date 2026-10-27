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
Abandon based on the handler result. Auto-renew schedules from the delivery's
remaining lease and safely retries one transient renewal; the configured lock
duration must still leave enough time for the network round trip, and the
application and broker clocks should be synchronized. Shutdown is cooperative:
handlers and custom HTTP transports must honor request contexts, and an error
handler must return promptly. The receiver waits for an in-flight renewal to
exit before it completes or abandons the same lock token.

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
- Strict error mapping with `errors.Is` sentinels.

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
settlement, and a receive retry reuses the same attempt ID so a lost response
does not claim another batch. Destructive queue/subscription deletion is sent at
most once: redirects, transport failures, and gateway 5xx responses return
`ErrOutcomeUnknown` for read-only reconciliation rather than automatic replay.
`CompleteBatch` also reports `ErrOutcomeUnknown` when a successful HTTP response
does not acknowledge the requested messages coherently; some completions may
already have committed.

`Receiver.Run` always uses Peek-Lock; a process crash or lost Complete response
can cause redelivery rather than silent removal. Explicit
`Receive(..., RecvOpts{AtMostOnce: true})`, message or subscription TTL expiry,
force deletion, purge, cancel, and configured DLQ retention are deliberate data
removal boundaries. A send transport failure can be ambiguous, so producers
that cannot recreate work should use an application outbox and stable message
identity; consumers must still be idempotent.

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
