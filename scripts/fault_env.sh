#!/usr/bin/env bash
set -euo pipefail

# Fault-injection environment (Task 29.1): a Toxiproxy in front of the test
# TimescaleDB, both on 127.0.0.1 with random ports.
#
#   eval "$(scripts/fault_env.sh up)"     # starts the DB (scripts/test_db.sh) and Toxiproxy
#   (cd faults && mix test --include fault)
#   scripts/fault_env.sh down
#
# Prints `export ...` lines on stdout; everything else goes to stderr.

root=$(cd "$(dirname "$0")/.." && pwd)
cli=${CONTAINER_CLI:-$(command -v podman || command -v /opt/podman/bin/podman || echo docker)}
name=${AUTOANGEL_FAULT_PROXY_CONTAINER:-autoangel-fault-proxy}
image=ghcr.io/shopify/toxiproxy:2.9.0

free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

up() {
  local db_exports db_port api_port proxy_port
  db_exports=$("$root/scripts/test_db.sh" up)
  eval "$db_exports"
  db_port=$AUTOANGEL_TEST_DB_PORT

  "$cli" rm -f "$name" >/dev/null 2>&1 || true
  api_port=$(free_port)
  proxy_port=$(free_port)
  # The proxy listens on 15432 inside the container; only loopback is published.
  "$cli" run -d --rm --name "$name" \
    -p "127.0.0.1:${api_port}:8474" -p "127.0.0.1:${proxy_port}:15432" \
    "$image" >/dev/null

  for _ in $(seq 1 30); do
    curl -fsS "http://127.0.0.1:${api_port}/version" >/dev/null 2>&1 && break
    sleep 1
  done

  curl -fsS -X POST "http://127.0.0.1:${api_port}/proxies" \
    -d "{\"name\":\"timescaledb\",\"listen\":\"0.0.0.0:15432\",\"upstream\":\"host.containers.internal:${db_port}\",\"enabled\":true}" \
    >/dev/null

  printf '%s\n' "$db_exports"
  printf 'export AUTOANGEL_FAULT_PROXY_API=http://127.0.0.1:%s\n' "$api_port"
  printf 'export AUTOANGEL_FAULT_PROXY_PORT=%s\n' "$proxy_port"
}

down() {
  "$cli" rm -f "$name" >/dev/null 2>&1 || true
  "$root/scripts/test_db.sh" down
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  *)
    echo "usage: $0 up|down" >&2
    exit 2
    ;;
esac
