# Compatibility and support

## Two Go modules, two use cases

| Module | Go floor | Runtime dependencies | Embedded SQLite | Remote broker |
|---|---:|---|---:|---:|
| `github.com/mqlitehq/mqlite-go` | 1.13 | standard library only | no | yes |
| `github.com/mqlitehq/mqlite` | 1.21 | engine and storage stack | yes | yes |

The low floor applies to source and module compatibility. Go 1.13 no longer
receives security fixes, and consumers should compile production programs with a
supported Go release whenever their environment permits. Official `mqlitec`
artifacts always use current stable.

Go 1.13 predates `darwin/arm64`. The floor is therefore verified on
`linux/amd64`; current stable verifies Linux, macOS, and Windows, including
Apple Silicon release artifacts.

## Broker compatibility

The v0.1.0 SDK is blackbox-tested against the exact
`ghcr.io/mqlitehq/mqlite:0.4.1` broker image. Core queue calls use the stable
JSON-over-HTTP routes shared by MQLite clients. Lifecycle-aware queue/subscription
deletion, fixed subscription TTL, and paged subscription inventory require a
broker that implements those routes (v0.4.0 or later).

Mixed-TTL batches, where messages in one `Send` call use different positive TTL
values, require broker v0.4.1 or later. Uniform-TTL batches retain the older
request-level wire shape for compatibility.

An older broker returns `ErrUnsupported` when a route does not exist. The SDK
does not silently translate lifecycle calls into a less-safe legacy operation.

Permissions are a broker contract, not a client-side policy. Against v0.4.1,
subscription creation and filter testing require `manage`. Consult the selected
broker release notes before assigning a narrower credential.

## Stability

Tags follow semantic versioning. Before v1.0, a minor release may adjust the
public Go API; patch releases preserve it. The on-wire broker API remains the
source of truth. New JSON fields are ignored for forward compatibility, while
security- or lifecycle-sensitive acknowledgements require all defined fields and
reject internally inconsistent values.

The module does not promise compatibility with undocumented broker internals,
SQLite files, Turso URLs, or the embedded engine. It accepts only broker HTTP(S)
or `mqlite://` / `mqlites://` connection strings.

## Matrix enforced by CI

- Go 1.13.15 on Linux amd64: build, unit tests, vet, and tidy cleanliness.
- Go 1.18.10 on Linux amd64: build, unit tests, vet, and tidy cleanliness.
- Current stable on Linux, macOS, and Windows: build and tests.
- Current stable on Linux: race detector, coverage, vulnerability scan, real
  v0.4.1 broker round-trip, and stripped-binary size report.
- Module dependency guard: this module plus the Go standard library only.
