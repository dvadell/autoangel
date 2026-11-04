# Architecture decisions

## Storage gate

- CubDB checkpoint (20000 Series, 4500 byte state): write 2249.613 ms, restore 349.488 ms, replacement 7277.94 ms, on-disk size 600578091 bytes (restored 20000 entries).

The M0 storage benchmark results and the decision on checkpoint and TimescaleDB write paths will be recorded here.

- TimescaleDB write benchmark implementation: `mix autoangel.benchmark_timescaledb` in `engine`, using `AUTOANGEL_TSDB_URL`. It generates 20,000 Series with six Samples each at deterministic 10 s to 60 s intervals, writes 5,000-row batches, and records Samples/s, client write round trips per batch, elapsed time, host memory, logical CPU count and the client filesystem disk type after a successful run. The round-trip figure counts Postgrex write operations, not wire-level protocol exchanges. `--fallback` measures the parameterized multi-row INSERT path.
- This environment has 2 CPU schedulers and 16,331,076 kB host memory; the database benchmark was not run because `AUTOANGEL_TSDB_URL` is not configured, so no Samples/s or disk result is claimed.

## NIF gate

The M0 NIF benchmark results will determine which numeric kernels, if any, move from the Elixir reference implementation to Rust NIFs. The selected implementation and measured limits will be recorded here.

## Provisional defaults

Defaults marked provisional in the design remain subject to the M0 benchmark. Changes and their measured rationale will be recorded here.
