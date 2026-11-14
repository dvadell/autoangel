#!/usr/bin/env bash
set -euo pipefail

# Startup smoke test for the built releases (Requirements 76.5 to 76.7).
#
#   scripts/smoke_release.sh engine  <engine-release-dir>
#   scripts/smoke_release.sh console <console-release-dir>
#   scripts/smoke_release.sh push    <engine-release-dir> <cli-binary>
#
# `engine`  starts the Engine release (no storage) and waits for /readyz.
# `console` starts the Console release against a throw-away TimescaleDB
#           (CONSOLE_DATABASE_URL, or a podman container started here) and waits for /healthz.
# `push`    starts the Engine, runs a CLI push against it and checks it was accepted.
#
# Everything binds to 127.0.0.1 on random ports and is stopped on exit.

mode=${1:-}
release=${2:-}
cli=${3:-}
scratch=$(mktemp -d)
pids=()

cleanup() {
  for pid in "${pids[@]:-}"; do
    [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true
  done
  rm -rf "$scratch"
}
trap cleanup EXIT

free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

wait_for() { # url, header, seconds
  local url=$1 header=$2 seconds=$3
  for _ in $(seq 1 "$((seconds * 2))"); do
    if curl -fsS -H "$header" "$url" >/dev/null 2>&1; then return 0; fi
    sleep 0.5
  done
  return 1
}

start_engine() {
  ingest_port=$(free_port)
  control_port=$(free_port)
  ingest_token="smoke-ingest-$RANDOM$RANDOM"
  control_token="smoke-control-$RANDOM$RANDOM"
  cat >"$scratch/engine.toml" <<EOF
[ingest]
token = "$ingest_token"
[control_api]
token = "$control_token"
[http]
ingest_port = $ingest_port
control_port = $control_port
EOF
  AUTOANGEL_CONFIG="$scratch/engine.toml" AUTOANGEL_STATE_PATH="$scratch/state" \
    RELEASE_NODE="smoke_engine_$RANDOM" RELEASE_COOKIE="smoke-cookie" \
    "$release/bin/autoangel" start >"$scratch/engine.log" 2>&1 &
  pids+=("$!")
  if ! wait_for "http://127.0.0.1:$control_port/readyz" "Authorization: Bearer $control_token" 60; then
    echo "the Engine did not become ready" >&2
    tail -30 "$scratch/engine.log" >&2
    exit 1
  fi
}

case "$mode" in
  engine)
    start_engine
    echo "engine smoke test: ready on $control_port"
    ;;

  push)
    [[ -x "$cli" ]] || { echo "usage: $0 push <engine-release> <cli>" >&2; exit 2; }
    start_engine
    "$cli" push smoke_metric 1.5 --label job=smoke \
      --ingest-endpoint "http://127.0.0.1:$ingest_port/api/v1/push" \
      --bearer-token "$ingest_token" --retry-duration-secs 5
    metrics=$(curl -fsS -H "Authorization: Bearer $control_token" "http://127.0.0.1:$control_port/metrics")
    if ! grep -Eq 'autoangel_ingest_samples_accepted[^ ]* [1-9]' <<<"$metrics"; then
      echo "the Engine did not count the pushed Sample" >&2
      exit 1
    fi
    echo "push smoke test: the Sample was accepted"
    ;;

  console)
    port=$(free_port)
    db_url=${CONSOLE_DATABASE_URL:-}
    if [[ -z "$db_url" ]]; then
      db_url=$("$(dirname "$0")/test_db.sh" up | sed -n 's/^export AUTOANGEL_TEST_DB_URL=//p')
      trap '"$(dirname "$0")/test_db.sh" down; cleanup' EXIT
    fi
    AUTOANGEL_CONSOLE_DATABASE_URL="$db_url" AUTOANGEL_CONSOLE_PORT="$port" \
      AUTOANGEL_CONSOLE_TOKEN="smoke-console-$RANDOM$RANDOM" \
      AUTOANGEL_CONSOLE_HEALTH_EXEMPT=true \
      AUTOANGEL_CONSOLE_SECRET_KEY_BASE=$(python3 -c "import secrets; print(secrets.token_hex(48))") \
      RELEASE_NODE="smoke_console_$RANDOM" RELEASE_COOKIE="smoke-cookie" \
      "$release/bin/autoangel_console" start >"$scratch/console.log" 2>&1 &
    pids+=("$!")
    if ! wait_for "http://127.0.0.1:$port/healthz" "Accept: application/json" 60; then
      echo "the Console did not become healthy" >&2
      tail -30 "$scratch/console.log" >&2
      exit 1
    fi
    echo "console smoke test: healthy on $port"
    ;;

  *)
    echo "usage: $0 engine|console|push <release> [cli]" >&2
    exit 2
    ;;
esac
