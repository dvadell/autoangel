#!/usr/bin/env bash
set -euo pipefail

# This is a command surface for a future CI workflow. CI is not configured here.
root=$(cd "$(dirname "$0")/.." && pwd)
engine="$root/engine"

cd "$engine"

dep_tree=$(mix deps.tree --umbrella-only --format plain)
core_tree=$(awk '
  /^==> / { in_core = ($0 == "==> autoangel_core"); next }
  in_core { print }
' <<<"$dep_tree")

if [[ -z "$core_tree" ]]; then
  printf 'dependency tree did not contain an autoangel_core section\n' >&2
  exit 1
fi

if grep -Eq 'engine_plugin_[[:alnum:]_]+' <<<"$core_tree"; then
  plugins=$(grep -oE 'engine_plugin_[[:alnum:]_]+' <<<"$core_tree" | sort -u | tr '\n' ' ')
  printf 'autoangel_core dependency closure contains plugin app(s): %s\n' "$plugins" >&2
  exit 1
fi

# xref's JSON graph contains file-level edges. Keep the graph query here so the
# check follows actual compiler references instead of only searching source text.
xref_json=$(mix xref graph --format json --output -)
XREF_JSON="$xref_json" python3 - <<'PY'
import json
import os
import sys

try:
    raw = os.environ["XREF_JSON"]
    graph = json.loads(raw[raw.find("{") :])
except (json.JSONDecodeError, ValueError) as error:
    print(f"could not parse mix xref JSON: {error}", file=sys.stderr)
    sys.exit(1)

protected = (
    "/autoangel/detect/",
    "/autoangel/correlate/",
    "/autoangel/alert/",
    "/autoangel/detect.ex",
    "/autoangel/correlate.ex",
    "/autoangel/alert.ex",
)

def protected_source(path):
    return any(marker in f"/{path}" for marker in protected)

def tsdb_sink(path):
    return "autoangel_tsdb" in path.lower()

violations = []
for source in graph:
    if not protected_source(source):
        continue
    pending = [(source, [source])]
    visited = {source}
    while pending:
        current, path = pending.pop()
        for sink in graph.get(current, {}):
            if tsdb_sink(sink):
                violations.append((source, path + [sink]))
            elif sink not in visited:
                visited.add(sink)
                pending.append((sink, path + [sink]))

if violations:
    print("detection/correlation/alert xref references AutoangelTsdb:", file=sys.stderr)
    for source, path in violations:
        print(f"  {source}: {' -> '.join(path)}", file=sys.stderr)
    sys.exit(1)
PY

printf 'dependency boundary checks passed (CI workflow not configured)\n'
