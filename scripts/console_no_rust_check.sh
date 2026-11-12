#!/usr/bin/env bash
set -euo pipefail

# The Console must contain no Rust source, Rustler dependency or NIF: it is a
# read-only web release that never links the Engine's numeric core.
# Usage: console_no_rust_check.sh [path ...]   (default: the Console tree, and its
# release or image root when CONSOLE_RELEASE_DIR / CONSOLE_IMAGE_DIR are set)

root=$(cd "$(dirname "$0")/.." && pwd)
paths=("$@")
if [[ ${#paths[@]} -eq 0 ]]; then
  paths=("$root/console")
  [[ -n "${CONSOLE_RELEASE_DIR:-}" ]] && paths+=("$CONSOLE_RELEASE_DIR")
  [[ -n "${CONSOLE_IMAGE_DIR:-}" ]] && paths+=("$CONSOLE_IMAGE_DIR")
fi

failed=0
fail() {
  printf 'console no-Rust check: %s\n' "$1" >&2
  failed=1
}

for path in "${paths[@]}"; do
  if [[ ! -d "$path" ]]; then
    printf 'console no-Rust check: %s is not a directory\n' "$path" >&2
    exit 2
  fi

  # Fetched dependencies and build output are checked for Rustler below, not by name.
  while IFS= read -r file; do
    fail "Rust source or manifest found: $file"
  done < <(find "$path" \( -name deps -o -name _build \) -prune -o \
    \( -name '*.rs' -o -name Cargo.toml -o -name Cargo.lock -o -name 'rust-toolchain*' \) -print)

  while IFS= read -r file; do
    fail "native library found (NIF): $file"
  done < <(find "$path" \( -name '*.so' -o -name '*.dylib' -o -name '*.dll' \) \
    -not -path '*/erts-*' -not -path '*/lib/crypto-*' -not -path '*/lib/asn1rt_nif-*' \
    -not -path '*/lib/runtime_tools-*' -not -path '*/lib/os_mon-*' -not -path '*/lib/jinterface-*' \
    -not -path '*/lib/erl_interface-*' -print)

  for manifest in "$path/mix.exs" "$path/mix.lock"; do
    if [[ -f "$manifest" ]] && grep -Eiq ':rustler|"rustler' "$manifest"; then
      fail "Rustler referenced in $manifest"
    fi
  done

  if [[ -d "$path/deps" ]] && find "$path/deps" -maxdepth 1 -iname '*rustler*' | grep -q .; then
    fail "Rustler dependency present under $path/deps"
  fi
done

if [[ $failed -ne 0 ]]; then
  exit 1
fi
printf 'console no-Rust check: ok\n'
