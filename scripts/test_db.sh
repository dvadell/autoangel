#!/usr/bin/env bash
set -euo pipefail

# Throw-away TimescaleDB for integration tests, on a random loopback port.
#
#   scripts/test_db.sh up     # start, wait until ready, migrate; prints `export ...` lines
#   scripts/test_db.sh down   # remove the container and its volume
#   eval "$(scripts/test_db.sh up)" && (cd engine && mix test --include integration)
#
# Uses podman, or docker when CONTAINER_CLI=docker. The container binds only to
# 127.0.0.1. The image matches compose.yml.

root=$(cd "$(dirname "$0")/.." && pwd)
cli=${CONTAINER_CLI:-$(command -v podman || command -v /opt/podman/bin/podman || echo docker)}
name=${AUTOANGEL_TEST_DB_CONTAINER:-autoangel-test-db}
image=timescale/timescaledb:2.17.2-pg16

free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

up() {
  "$cli" rm -f "$name" >/dev/null 2>&1 || true
  local port
  port=$(free_port)
  "$cli" run -d --rm --name "$name" \
    -e POSTGRES_USER=autoangel -e POSTGRES_PASSWORD=autoangel -e POSTGRES_DB=autoangel \
    -p "127.0.0.1:${port}:5432" "$image" >/dev/null

  for _ in $(seq 1 60); do
    if "$cli" exec "$name" pg_isready -U autoangel -d autoangel -h 127.0.0.1 >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
  # The entrypoint restarts the server once after init; wait for the final one.
  for _ in $(seq 1 60); do
    if "$cli" exec "$name" psql -U autoangel -d autoangel -h 127.0.0.1 -tAc 'SELECT 1' >/dev/null 2>&1 &&
      python3 -c "import socket; socket.create_connection(('127.0.0.1', $port), 1)" 2>/dev/null; then
      sleep 2
      if "$cli" exec "$name" psql -U autoangel -d autoangel -h 127.0.0.1 -tAc 'SELECT 1' >/dev/null 2>&1; then
        break
      fi
    fi
    sleep 1
  done

  (cd "$root/engine" && AUTOANGEL_TEST_DB_PORT=$port mix run --no-start -e '
    {:ok, _} = Application.ensure_all_started(:postgrex)
    port = String.to_integer(System.fetch_env!("AUTOANGEL_TEST_DB_PORT"))
    {:ok, conn} = Postgrex.start_link(hostname: "127.0.0.1", port: port, username: "autoangel",
      password: "autoangel", database: "autoangel")
    :ok = AutoangelTsdb.Migrations.run(conn)
  ' >&2)

  printf 'export AUTOANGEL_TEST_DB_PORT=%s\n' "$port"
  printf 'export AUTOANGEL_TEST_DB_URL=ecto://autoangel:autoangel@127.0.0.1:%s/autoangel\n' "$port"
}

down() {
  "$cli" rm -f -v "$name" >/dev/null 2>&1 || true
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  *)
    printf 'usage: %s up|down\n' "$0" >&2
    exit 2
    ;;
esac
