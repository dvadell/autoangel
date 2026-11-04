#!/usr/bin/env bash
set -euo pipefail

# This script is the single command surface for a future CI workflow.
# CI is intentionally not configured in this repository yet.

root=$(cd "$(dirname "$0")/.." && pwd)

run() {
  if [[ "${QUALITY_GATE_DRY_RUN:-0}" == "1" ]]; then
    printf '%s\n' "$*"
  else
    "$@"
  fi
}

run_labeled() {
  local label=$1
  shift
  if [[ "${QUALITY_GATE_DRY_RUN:-0}" == "1" ]]; then
    printf '%s: %s\n' "$label" "$*"
  else
    run "$@"
  fi
}

run_elixir_checks() {
  local project=$1
  (
    cd "$root/$project"
    run_labeled "$project" mix format --check-formatted
    run_labeled "$project" mix credo --strict
    run_labeled "$project" mix dialyzer
    run_labeled "$project" mix test
  )
}

run_rust_checks() {
  (
    cd "$root"
    run cargo fmt --all -- --check
    run cargo clippy --workspace --all-targets --all-features -- -D warnings
    run cargo test --workspace --all-features
    run cargo audit
    run "$root/scripts/rust_safety_checks.sh"
  )
}

run_elixir_checks engine
run_elixir_checks console
run_elixir_checks shared/autoangel_schema
run_elixir_checks replay
run_rust_checks
