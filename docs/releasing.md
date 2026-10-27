# Release process

Releases are immutable tags and GitHub Releases. The Go module proxy discovers
the same tag; no separate package upload exists.

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

Never move or reuse a published tag. Correct a bad release with a new patch.
