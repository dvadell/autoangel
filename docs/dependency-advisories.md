# Rust dependency advisories

The quality gate runs `cargo audit` against the workspace lockfile and treats every
reported vulnerability advisory as a failure. The command was clean on 2026-11-04
for the committed workspace dependencies.

If an advisory is reported, it must be fixed by updating or replacing the dependency.
If an immediate fix is unavailable, record the advisory identifier, why it cannot be
fixed, and the mitigation here before merging. Run `scripts/quality_gate_test.sh` to
verify that the advisory check remains part of the configured command matrix.
