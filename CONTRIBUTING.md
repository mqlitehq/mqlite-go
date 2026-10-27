# Contributing

Read [CLAUDE.md](CLAUDE.md) before changing this repository. All contributions
must preserve the Go 1.13 source floor and standard-library-only runtime.

Before opening a pull request:

```sh
go build ./...
go vet ./...
test -z "$(gofmt -l .)"
go test -race -count=1 ./...
test "$(go list -m all | wc -l | tr -d ' ')" = 1
```

Run `./test/integration.sh` when changing routes, payloads, error mapping,
settlement, or lifecycle behavior. Include the output of
`./tools/measure-size.sh` when a change materially affects linked code.

The integration script starts the exact v0.4.1 broker image by default. Set
`DOCKER_BIN` when Docker is outside `PATH`, or provide both
`MQLITE_TEST_ENDPOINT` and `MQLITE_TEST_TOKEN` to test an already-running broker.
`MQLITE_TEST_IMAGE`, `MQLITE_TEST_PORT`, and `MQLITE_TEST_TOKEN_VALUE` customize
the disposable-container path. Never commit a live token or its test output.

Use APIs available in Go 1.13. Tests must not use helpers introduced later.
Keep public names aligned with the main MQLite SDK where doing so does not import
the engine or raise this module's floor.
