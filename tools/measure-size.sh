#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/mqlite-go-size.XXXXXX")
cleanup() { find "$tmp" -depth -delete; }
trap cleanup EXIT INT TERM

goos=${GOOS:-linux}
goarch=${GOARCH:-amd64}
flags='-s -w -buildid='

build() {
  name=$1
  package=$2
  CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" \
    go build -trimpath -ldflags "$flags" -o "$tmp/$name" "$package"
}

size() { wc -c < "$1" | tr -d ' '; }
compressed_size() { gzip -9 -c < "$1" | wc -c | tr -d ' '; }

cd "$root"
build empty ./test/size/empty
build http-json ./test/size/httpjson
build client ./test/size/client
build mqlitec ./cmd/mqlitec

empty=$(size "$tmp/empty")
http_json=$(size "$tmp/http-json")
client=$(size "$tmp/client")
cli=$(size "$tmp/mqlitec")
empty_gzip=$(compressed_size "$tmp/empty")
http_json_gzip=$(compressed_size "$tmp/http-json")
client_gzip=$(compressed_size "$tmp/client")
cli_gzip=$(compressed_size "$tmp/mqlitec")

printf 'toolchain\t%s\n' "$(go version)"
printf 'target\t%s/%s cgo=0 stripped=true\n' "$goos" "$goarch"
printf 'artifact\traw_bytes\tgzip_bytes\tdelta_from_empty\n'
printf 'empty\t%s\t%s\t0\n' "$empty" "$empty_gzip"
printf 'http-json-status-probe\t%s\t%s\t%s\n' "$http_json" "$http_json_gzip" "$((http_json - empty))"
printf 'sdk-status-probe\t%s\t%s\t%s\n' "$client" "$client_gzip" "$((client - empty))"
printf 'mqlitec\t%s\t%s\t%s\n' "$cli" "$cli_gzip" "$((cli - empty))"
printf 'sdk_increment_over_http_json_raw_bytes\t%s\n' "$((client - http_json))"
printf 'sdk_increment_over_http_json_gzip_bytes\t%s\n' "$((client_gzip - http_json_gzip))"

if git rev-parse --verify HEAD >/dev/null 2>&1; then
  archive_bytes=$(git archive --format=tar HEAD | gzip -9 | wc -c | tr -d ' ')
  printf 'source_archive_gzip_bytes\t%s\n' "$archive_bytes"
fi
