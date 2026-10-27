# mqlite-go

[![CI](https://github.com/mqlitehq/mqlite-go/actions/workflows/ci.yml/badge.svg)](https://github.com/mqlitehq/mqlite-go/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/mqlitehq/mqlite-go.svg)](https://pkg.go.dev/github.com/mqlitehq/mqlite-go)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

`mqlite-go` is the dependency-free, remote-only Go SDK for an
[MQLite](https://github.com/mqlitehq/mqlite) broker. It keeps a Go 1.13 source
floor for older applications without pulling the embedded SQLite engine,
libSQL/Turso drivers, CGO, or any third-party runtime module into their builds.

Use the main `github.com/mqlitehq/mqlite` module when you need an embedded queue,
same-database transactions, or the combined local/remote API. Use this module
when the application only talks to a broker over HTTP and a low Go floor matters.

## Install

```sh
go get github.com/mqlitehq/mqlite-go@v0.1.0
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
Abandon based on the handler result.

## Topics and temporary subscriptions

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
