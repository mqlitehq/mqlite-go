# MQLite low-floor Go SDK

This repository contains the official remote-only Go client for an MQLite
broker. It deliberately does not embed the MQLite engine and must never depend
on SQLite, libSQL/Turso, CGO, or the main `github.com/mqlitehq/mqlite` module.

## Non-negotiable constraints

- The module and language floor is Go 1.13. Code and tests must compile with
  Go 1.13.15 on `linux/amd64`.
- Runtime dependencies are standard-library only. `go list -m all` must contain
  exactly this module.
- Official CLI release binaries are built with the current supported Go stable,
  not Go 1.13. The old floor is source compatibility, not a security endorsement.
- Public code, documentation, commits, PRs, and release notes are English-only.
- The package name is `mqlite`; module path is `github.com/mqlitehq/mqlite-go`.
- Preserve the broker's JSON-over-HTTP contract and UTC epoch-millisecond times.
- Destructive lifecycle requests are never transparently replayed or redirected.
  An ambiguous delete returns `ErrOutcomeUnknown` and must be reconciled by list.
- Receive retries reuse one `receive_attempt_id`; settlement remains fenced by
  the message's lock token.
- Resource names remain the public identity. Do not introduce generation or
  incarnation fields.

## Go 1.13 compatibility

Do not use `any`, generics, `io.ReadAll`, `os.ReadFile`, `strings.Cut`,
`time.UnixMilli`, `signal.NotifyContext`, `testing.T.TempDir`, fuzz tests, or
other APIs introduced after Go 1.13. Use `interface{}`, `io/ioutil`, explicit
epoch conversion, `signal.Notify`, and test helpers based on `ioutil.TempDir`.

## Commands

```sh
go build ./...
go vet ./...
gofmt -l .
go test -race -count=1 ./...
go list -m all
./test/integration.sh   # Docker-backed real broker test when Docker is available
./tools/measure-size.sh # stripped linux/amd64 size report
```

CI additionally compiles and tests with Go 1.13.15, Go 1.18.10, and current
stable. A release tag must equal `v` plus `internal/version.Version`, must point
at a green main commit, and publishes only binaries built with supported stable.

## Change verification and release acceptance

- For every code-change candidate, run targeted functional and new regression
  tests, the complete existing race suite, build/vet/format checks, and
  `go mod tidy` with no diff. Verify standard-library-only dependencies and
  actual Go 1.13.15 compatibility; record which platform checks CI supplies.
  Cover affected behavior with the broker and native SDK parity scenarios.
- Require every candidate CI job to pass before merge. After merge, verify the
  actual main SHA and all required push CI jobs (currently 11) passing in one
  run/attempt, plus the broker/client combination. Prior PR greens do not replace
  that evidence. Record skips, missing and unavailable checks without calling them passes.
- Exercise local performance and sustained stress against a comparison baseline.
  Declare durations, workload, leases, budgets, resource limits and acceptance
  criteria before execution. Use Actions for dedicated real-backend verification
  through the broker; never add engine or database dependencies to this SDK.
- Verify exact message identities/payloads, settlement, expected redelivery/DLQ,
  resource bounds, cancellation, shutdown and restart. Preserve first failures and
  diagnose them before rerunning; do not relax budgets or assertions to turn green.
  Functional nightly success and read-only CI monitoring are not performance soaks.
- Freeze source revisions, artifact digests, harness and configuration before final
  acceptance. Coordinate with the broker's production checklist: at least 24 hours
  of supervised workload, or the maintainer's stricter plan. Changes require renewed
  verification; never splice different sources or windows into a pass. Review the
  scope of documentation-only changes explicitly without inventing runtime evidence.
- Accept final evidence only after supervised completion, drain, exact ledgers,
  integrity/restore checks and resource review. A short smoke, runner-only PASS,
  skipped test or infrastructure/read error does not close an unverified gate.
  Keep credentials and private deployment details out of published evidence.
- Obtain separate, explicit maintainer approval before creating a tag or Release.
  Prepare concise English release notes from the last tag's net changes, with
  accurate compatibility, upgrade guidance, verified coverage and known limits.

## Repository shape

- Root package: public remote SDK.
- `internal/wire`: private JSON transport contract and strict decoders.
- `cmd/mqlitec`: small diagnostic/verification CLI, not a clone of the broker CLI.
- `test/`: live-broker blackbox controls and size probes.
- `docs/`: compatibility, security, CLI, and release documentation.

Keep tests grouped by concern rather than creating one file per method. Prefer
small explicit code over abstractions that raise the language floor or hide wire
semantics.
