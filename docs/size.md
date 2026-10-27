# Binary-size methodology

A Go library has no standalone deployment binary. Measure the cost in linked
programs instead of quoting compiler-cache archives.

`tools/measure-size.sh` builds four `linux/amd64`, `CGO_ENABLED=0` artifacts with
`-trimpath -ldflags '-s -w -buildid='`:

1. an empty Go program (toolchain/runtime baseline),
2. a handwritten standard-library HTTP/JSON client that calls Status,
3. a minimal SDK client that opens a broker and calls Status,
4. the complete `mqlitec` diagnostic CLI.

It reports raw bytes, gzip bytes, and each program's raw delta from the empty
baseline. It separately reports the SDK probe's raw and compressed increment
over the equivalent handwritten HTTP/JSON probe. That second comparison isolates
the SDK wrapper from the much larger cold cost of linking Go's HTTP, TLS, X.509,
DNS, JSON, reflection, and supporting runtime paths. When run from a commit the
script also reports the gzip size of `git archive HEAD`.

The release notes record the exact toolchain, target, and results. Neither delta
is universal because Go linker reachability depends on the consuming program;
an application that already uses HTTP and JSON shares most of that linked code.

Run:

```sh
./tools/measure-size.sh
```

Override `GOOS` and `GOARCH` only when comparing the same commit and toolchain.
Official v0.1.0 measurements use current supported Go stable, Linux amd64, and
CGO disabled. Go 1.13 builds prove compatibility but are not release-size inputs.

## v0.1.0 reference measurement

The release candidate was measured with Go 1.27.3 in a Linux arm64 container,
cross-compiling the stripped, `CGO_ENABLED=0` Linux amd64 target:

| Artifact | Raw bytes | gzip bytes | Raw delta from empty |
|---|---:|---:|---:|
| Empty Go program | 1,216,636 | 558,944 | 0 |
| Minimal SDK status probe | 6,672,508 | 2,809,817 | 5,455,872 |
| Complete `mqlitec` | 6,783,100 | 2,856,003 | 5,566,464 |

This historical script did not include the handwritten HTTP/JSON row. Its
5,455,872-byte SDK-probe delta therefore includes the complete cold network and
serialization stack; it is not the size of MQLite's SDK code. Starting with
v0.1.1, the tagged `size.tsv` contains the reproducible same-toolchain HTTP/JSON
comparison and is the authoritative measurement of the SDK wrapper increment.

The tag workflow repeats this measurement with its exact stable toolchain and
attaches `size.tsv` to the GitHub Release. That tagged result is authoritative
if a stable toolchain update changes linker output.
