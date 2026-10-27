#!/bin/sh
set -eu

if [ -n "${MQLITE_TEST_ENDPOINT:-}" ] && [ -n "${MQLITE_TEST_TOKEN:-}" ]; then
  exec go test -count=1 -run TestBrokerRoundTrip -v ./test/live
fi

docker_bin=${DOCKER_BIN:-docker}
image=${MQLITE_TEST_IMAGE:-ghcr.io/mqlitehq/mqlite:0.4.1@sha256:16e2e90311eaf97100a7820b475c09a9a771d121a222c6469b537b6d369a0f34}
port=${MQLITE_TEST_PORT:-16754}
token=${MQLITE_TEST_TOKEN_VALUE:-mqlite_go_integration_token}
container=mqlite-go-integration-$$

cleanup() {
  "$docker_bin" rm -f "$container" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

"$docker_bin" run -d --name "$container" \
  -p "127.0.0.1:${port}:6754" \
  -e MQLITE_DB=:memory: \
  -e "MQLITE_TOKENS=${token}" \
  "$image" >/dev/null

i=0
while [ "$i" -lt 60 ]; do
  if curl -fsS "http://127.0.0.1:${port}/healthz" >/dev/null 2>&1; then
    MQLITE_TEST_ENDPOINT="http://127.0.0.1:${port}" \
      MQLITE_TEST_TOKEN="$token" \
      go test -count=1 -run TestBrokerRoundTrip -v ./test/live
    exit $?
  fi
  i=$((i + 1))
  sleep 1
done

"$docker_bin" logs "$container" >&2 || true
echo "broker did not become healthy" >&2
exit 1
