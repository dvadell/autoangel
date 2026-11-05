#!/usr/bin/env bash
set -euo pipefail

# Build the CLI in a clean, locked configuration for each supported target.
# Install both Rust targets and the corresponding musl cross-linkers in the
# build image before invoking this script.
root=$(cd "$(dirname "$0")/.." && pwd)
crate="$root/cli"
target_dir="${CARGO_TARGET_DIR:-$root/target}"

for target in x86_64-unknown-linux-musl aarch64-unknown-linux-musl; do
  cargo build \
    --manifest-path "$crate/Cargo.toml" \
    --package autoangel-cli \
    --release \
    --locked \
    --target "$target"

  binary="$target_dir/$target/release/autoangel-cli"
  if [[ ! -x "$binary" ]]; then
    printf 'expected executable was not produced: %s\n' "$binary" >&2
    exit 1
  fi

  case "$target" in
    x86_64-unknown-linux-musl) expected_machine='Advanced Micro Devices X86-64' ;;
    aarch64-unknown-linux-musl) expected_machine='AArch64' ;;
  esac
  if ! file_output=$(file "$binary") || ! grep -Eqi 'ELF .*executable' <<<"$file_output" || ! readelf -h "$binary" | grep -q "Machine:.*$expected_machine"; then
    printf 'unexpected ELF architecture or file type for %s\n%s\n' "$binary" "$file_output" >&2
    exit 1
  fi

  if ! program_headers=$(readelf -lW "$binary"); then
    printf 'could not inspect ELF program headers: %s\n' "$binary" >&2
    exit 1
  fi
  if grep -q ' INTERP ' <<<"$program_headers"; then
    printf 'binary has an interpreter and is not fully static: %s\n' "$binary" >&2
    exit 1
  fi
  if ! dynamic_section=$(readelf -dW "$binary"); then
    printf 'could not inspect ELF dynamic section: %s\n' "$binary" >&2
    exit 1
  fi
  if grep -q '(NEEDED)' <<<"$dynamic_section"; then
    printf 'binary has dynamic dependencies: %s\n' "$binary" >&2
    exit 1
  fi

  sha256sum "$binary"
done
