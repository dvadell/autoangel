#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
crate="$root/engine/native/autoangel_kernels"

profile=$(awk '
  /^\[profile\.release\]/{in_release=1; next}
  /^\[/{in_release=0}
  in_release && $1 == "panic" && $2 == "=" {gsub(/[" ]/, "", $3); print $3}
' "$root/Cargo.toml")
if [[ "$profile" != "unwind" ]]; then
  printf 'release profile must set panic = "unwind"\n' >&2
  exit 1
fi

# Every occurrence of the unsafe keyword must carry a local justification and a reviewed record.
unsafe_occurrences=$(grep -Rnw --include='*.rs' -E 'unsafe' "$crate" || true)
if [[ -n "$unsafe_occurrences" ]]; then
  while IFS=: read -r file line _; do
    if ! sed -n "$((line > 3 ? line - 3 : 1)),$((line + 1))p" "$file" | grep -q '// SAFETY:'; then
      printf 'unsafe code lacks adjacent // SAFETY: comment: %s:%s\n' "$file" "$line" >&2
      exit 1
    fi
    relative_file="${file#"$root/"}"
    if ! awk -v key="${relative_file}:${line}" '$1 == key { found = 1 } END { exit !found }' "$root/engine/native/UNSAFE_REVIEW.md"; then
      printf 'unsafe code lacks a review record: %s:%s\n' "$relative_file" "$line" >&2
      exit 1
    fi
  done <<< "$unsafe_occurrences"
fi

# The compile-time guard must make a release build with test-panic fail.
log_file=$(mktemp)
trap 'rm -f "$log_file"' EXIT
if cargo build --manifest-path "$crate/Cargo.toml" --release --features test-panic >"$log_file" 2>&1; then
  printf 'release builds must reject the test-panic feature\n' >&2
  exit 1
fi
if ! grep -q 'test-panic feature is forbidden in release builds' "$log_file"; then
  cat "$log_file" >&2
  printf 'release test-panic rejection did not use the expected guard\n' >&2
  exit 1
fi
