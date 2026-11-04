# NIF golden fixtures

M0 promoted no numeric kernel, so this directory intentionally contains no
numeric dataset. The Elixir reference kernel remains selected. When a kernel is
promoted, add deterministic fixtures here and compare every returned numeric
field against the named Elixir reference implementation.

The comparison policy follows the design's per-field tolerances:

- anomaly scores: absolute deviation at most `1e-9`;
- expected-range bounds: relative deviation at most `1e-12`.

A failure must report the kernel, dataset path, field, expected value, actual
value, and absolute deviation. Kernels with a documented domain-specific
tolerance must state it next to their fixture manifest.
