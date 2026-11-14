#!/usr/bin/env bash
set -euo pipefail

# Privacy egress test (Requirements 75.1 to 75.5).
#
#   scripts/privacy_egress_test.sh <engine-release> <console-release> <cli-binary>
#
# The Engine, the Console and the CLI run in a network namespace that holds only
# loopback, so nothing can leave the machine. `strace` logs every connect() they
# try, and `egress_check.py` fails the test if any attempt goes anywhere but a
# destination this script configured. That covers telemetry (no component may
# phone home unless an operator opts in, and no opt-in exists), name lookups
# (a DNS query is a connect to port 53) and any address outside loopback, even
# though such a connection could not succeed here.
#
# Linux only: it needs `unshare` (user and network namespaces), `strace`, `ip`
# and python3. It exits 77 (skipped) where one of them is missing.

if [[ "${1:-}" != "--inner" ]]; then
  [[ "$(uname -s)" == "Linux" ]] || { echo "skipped: the test needs Linux namespaces" >&2; exit 77; }
  for tool in unshare strace ip python3 curl; do
    command -v "$tool" >/dev/null || { echo "skipped: $tool is not installed" >&2; exit 77; }
  done
  [[ $# -eq 3 ]] || { echo "usage: $0 <engine-release> <console-release> <cli-binary>" >&2; exit 2; }
  exec unshare --map-root-user --net bash "$0" --inner "$@"
fi
shift
engine=$(cd "$1" && pwd)
console=$(cd "$2" && pwd)
cli=$(cd "$(dirname "$3")" && pwd)/$(basename "$3")
here=$(cd "$(dirname "$0")" && pwd)

scratch=$(mktemp -d)
pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true; done
  rm -rf "$scratch"
}
trap cleanup EXIT

ip link set lo up
port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'; }
ingest_port=$(port); control_port=$(port); hook_port=$(port); db_port=$(port); console_port=$(port)

# A webhook receiver and a stand-in database. They are the configured destinations.
python3 - "$hook_port" "$db_port" <<'PY' &
import socketserver, sys, threading, http.server
class Hook(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        self.rfile.read(int(self.headers.get("content-length", 0)))
        self.send_response(204); self.end_headers()
    def log_message(self, *a): pass
class Drop(socketserver.BaseRequestHandler):
    def handle(self): self.request.close()
hook = http.server.ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), Hook)
db = socketserver.ThreadingTCPServer(("127.0.0.1", int(sys.argv[2])), Drop)
threading.Thread(target=db.serve_forever, daemon=True).start()
hook.serve_forever()
PY
pids+=("$!")

cat >"$scratch/engine.toml" <<EOF
[ingest]
token = "privacy-ingest"
[control_api]
token = "privacy-control"
[http]
ingest_port = $ingest_port
control_port = $control_port
[plugins]
enabled = ["webhook"]
[plugins.webhook]
url = "http://127.0.0.1:$hook_port/hook"
EOF

trace() { # name, command...
  local name=$1; shift
  strace -f -qq -e trace=connect -o "$scratch/$name.strace" "$@"
}

export RELEASE_DISTRIBUTION=none RELEASE_COOKIE=privacy
# The Engine: started, pushed to, asked for readiness and metrics.
AUTOANGEL_CONFIG="$scratch/engine.toml" AUTOANGEL_STATE_PATH="$scratch/state" \
  trace engine "$engine/bin/autoangel" start >"$scratch/engine.log" 2>&1 &
pids+=("$!")
for _ in $(seq 1 120); do
  curl -fsS -H "Authorization: Bearer privacy-control" "http://127.0.0.1:$control_port/readyz" >/dev/null 2>&1 && break
  sleep 0.5
done
curl -fsS -H "Authorization: Bearer privacy-control" "http://127.0.0.1:$control_port/readyz" >/dev/null \
  || { echo "the Engine did not become ready" >&2; tail -20 "$scratch/engine.log" >&2; exit 1; }

# The CLI: a push and a wrapped run.
trace cli "$cli" push privacy_metric 1 --label job=privacy \
  --ingest-endpoint "http://127.0.0.1:$ingest_port/api/v1/push" --bearer-token privacy-ingest --retry-duration-secs 5
trace cli_run "$cli" run --name privacy_job \
  --ingest-endpoint "http://127.0.0.1:$ingest_port/api/v1/push" --bearer-token privacy-ingest --retry-duration-secs 5 -- true

# The Console: it starts against the stand-in database. It may stop at its start-up
# checks (the stand-in is not a database); what is checked is where it tries to connect.
AUTOANGEL_CONSOLE_DATABASE_URL="ecto://u:p@127.0.0.1:$db_port/d" AUTOANGEL_CONSOLE_PORT="$console_port" \
  AUTOANGEL_CONSOLE_TOKEN=privacy-console AUTOANGEL_CONSOLE_ENGINE_URL="http://127.0.0.1:$control_port" \
  AUTOANGEL_CONSOLE_SECRET_KEY_BASE=$(python3 -c "import secrets; print(secrets.token_hex(48))") \
  trace console "$console/bin/autoangel_console" start >"$scratch/console.log" 2>&1 &
pids+=("$!")

# Idle time: telemetry, if any, would show up here.
sleep "${PRIVACY_IDLE_SECONDS:-20}"

cat "$scratch"/*.strace >"$scratch/all.strace"
python3 "$here/egress_check.py" "$scratch/all.strace" "$ingest_port" "$control_port" "$hook_port" "$db_port" "$console_port"
echo "privacy egress test: every connection went to a configured destination"
