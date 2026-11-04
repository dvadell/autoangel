#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
output=$(cd /tmp && QUALITY_GATE_DRY_RUN=1 "$root/scripts/quality_gate.sh")

projects=(engine console shared/autoangel_schema replay)
commands=(
  "mix format --check-formatted"
  "mix credo --strict"
  "mix dialyzer"
  "mix test"
)

for project in "${projects[@]}"; do
  for command in "${commands[@]}"; do
    expected="$project: $command"
    if ! grep -Fqx "$expected" <<<"$output"; then
      printf 'quality gate is missing command: %s\n' "$expected" >&2
      exit 1
    fi
  done
done

for command in \
  "cargo fmt --all -- --check" \
  "cargo clippy --workspace --all-targets --all-features -- -D warnings" \
  "cargo test --workspace --all-features" \
  "cargo audit"; do
  if ! grep -Fqx "$command" <<<"$output"; then
    printf 'quality gate is missing command: %s\n' "$command" >&2
    exit 1
  fi
done

printf 'quality gate command matrix is complete\n'
