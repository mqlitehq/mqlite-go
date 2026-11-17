# Release process

Releases use permanent version tags and GitHub Releases. The Go module proxy
discovers the same tag; no separate package upload exists.

Do not infer server-enforced immutability from an annotated tag or a workflow
step name. [GitHub immutable releases](https://docs.github.com/en/code-security/concepts/supply-chain-security/immutable-releases)
are a separate repository setting. The published v0.1.2 release is not marked
immutable by GitHub; the project policy still forbids moving published tags or
replacing published assets.

## Before tagging

1. Keep `main` clean and pushed.
2. Set `internal/version.Version` to the intended semantic version without `v`.
3. Move changelog entries from Unreleased into that version.
4. Run build, vet, gofmt, race tests, real-broker integration, dependency guard,
   vulnerability scan, and size measurement.
5. Wait for the exact main commit's complete CI run to succeed.
6. Verify documentation examples use the intended tag and broker compatibility.

## Tag and publish

Create an annotated tag `vX.Y.Z` on the verified main commit and push only that
tag. `.github/workflows/release.yml` refuses a tag when:

- it is not an annotated tag,
- it differs from `v` plus `internal/version.Version`,
- the tagged commit is not the current remote `main`, or
- no successful push-triggered CI run exists for that exact SHA.

The read-only verification job reruns tests and pinned vulnerability analysis,
measures linked size, then cross-compiles `mqlitec` with current stable for
Linux, macOS, and Windows on amd64 and arm64. A separate write-scoped job only
downloads those verified artifacts and creates the release. It publishes
`.tar.gz`/`.zip` archives, `SHA256SUMS`, and `size.tsv`; the factual release
notes include the same measurement. All builds are `CGO_ENABLED=0`, trimmed,
and stripped. No Go 1.13-built binary is published.

After publication, verify the release checksums, install one archive, run
`mqlitec version`, and run a broker round-trip. Query the module through the Go
proxy so pkg.go.dev and downstream users can discover the tag.

## Verify the published release

The current client release is
[v0.1.2](https://github.com/mqlitehq/mqlite-go/releases/tag/v0.1.2).
Download the archive for your OS/architecture and `SHA256SUMS` from that page,
check its SHA256 against the named checksum entry, then run `mqlitec version`.
The reported version must be `0.1.2`.

To build the same published version with a supported current Go toolchain:

```sh
go install github.com/mqlitehq/mqlite-go/cmd/mqlitec@v0.1.2
mqlitec version
go list -m -json github.com/mqlitehq/mqlite-go@v0.1.2
```

Use the installed binary from `GOBIN`, or `$(go env GOPATH)/bin` when `GOBIN`
is unset. These commands query a published module version rather than a local
checkout. The source floor remains Go 1.13; official binaries use supported
current stable Go.

For a broker round-trip, create a disposable queue with an administrator, then
inject a producer/consumer credential through the environment:

```sh
export MQLITE_ENDPOINT=http://127.0.0.1:6754
export MQLITE_TOKEN='<test-credential>'
mqlitec send release-check 'release-check-body'
mqlitec receive --max 1 --wait 1s release-check
mqlitec receive --max 1 --wait 10ms release-check
```

The first receive must print the expected body; the second must be empty after
successful auto-settlement. Use a disposable broker/queue for this check and
account for its messages before deleting it. Token scopes and the output-before-
settlement boundary remain as documented in [the CLI reference](cli.md).

The v0.1.2 release notes disclose the maintainer-approved bounded verification.
The planned 24/48-hour acceptance and eight spaced Turso windows were not
completed; these install and round-trip checks do not turn that plan into a pass.

Never move or reuse a published tag. Correct a bad release with a new patch.
