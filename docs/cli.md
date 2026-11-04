# `mqlitec` reference

`mqlitec` is a compact remote diagnostic and compatibility client. It is not a
replacement for the full broker CLI.

## Configuration

```sh
export MQLITE_ENDPOINT=https://broker.example.com
export MQLITE_TOKEN=mqk_...
```

Global flags, when used, must precede the command:

```sh
mqlitec --endpoint http://127.0.0.1:6754 --token test-token status
```

Prefer environment or secret-manager injection for the token. `--token` may be
visible in process listings and shell history.

## Commands

```text
mqlitec version
mqlitec status
mqlitec send [flags] <queue-or-topic> <body>
mqlitec receive [flags] <queue-or-subscription>
mqlitec subscribe [flags] <topic> <subscription>
mqlitec delete-subscription [--force] <topic> <subscription>
```

`status` requires `manage` and emits the broker's desensitized backend snapshot
as JSON.

`send` accepts `--message-id`, `--subject`, and `--ttl`. It requires `send`.

`receive` accepts `--max`, `--wait`, and `--no-ack`. The default writes the complete
JSON batch to stdout before completing any returned message. A closed stdout or
null-device redirect is rejected before claiming; a later output error or partial
write completes none of the batch. Those messages remain locked until their
ordinary lease expiry and can then be redelivered or reach the delivery limit.
Use auto-ack only when printing the body is the intended processing action.
A successful write is not an acknowledgement from a downstream program: a pipe
reader can still fail after accepting the bytes. If completion fails after output,
the command exits nonzero and messages may be redelivered, so keep processing
idempotent. `--no-ack` leaves the delivery locked and prints its lock token for
deliberate diagnostics. It requires `listen`.

`subscribe` accepts `--expr` and `--ttl`. TTL is fixed at first creation and is
not renewed by traffic or updates. Permission is broker-version dependent; the
released v0.4.1 broker requires `manage`.

`delete-subscription` requires `manage` with or without `--force`. Force permits
discarding retained work. An ambiguous result exits nonzero with an
`ErrOutcomeUnknown` message; reconcile through an administrative inventory call.

All successful command output is JSON except `version`. Errors go to stderr and
produce a nonzero exit status. Tokens are never printed.
