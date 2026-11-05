# Architecture decisions

## Storage gate

- CubDB checkpoint (20000 Series, 4500 byte state): write 2249.613 ms, restore 349.488 ms, replacement 7277.94 ms, on-disk size 600578091 bytes (restored 20000 entries).

The M0 storage benchmark results and the decision on checkpoint and TimescaleDB write paths will be recorded here.

- TimescaleDB write benchmark implementation: `mix autoangel.benchmark_timescaledb` in `engine`, using `AUTOANGEL_TSDB_URL`. It generates 20,000 Series with six Samples each at deterministic 10 s to 60 s intervals, writes 5,000-row batches, and records Samples/s, client write round trips per batch, elapsed time, host memory, logical CPU count and the client filesystem disk type after a successful run. The round-trip figure counts Postgrex write operations, not wire-level protocol exchanges. `--fallback` measures the parameterized multi-row INSERT path.
- This environment has 2 CPU schedulers and 16,331,076 kB host memory; the database benchmark was not run because `AUTOANGEL_TSDB_URL` is not configured, so no Samples/s or disk result is claimed.

The TimescaleDB benchmark remains blocked in this environment. A Docker Compose service and initialization schema are provided in `compose.yml` and `docker/timescaledb/init.sql`; starting the service with `sudo -n docker compose up -d --wait` failed while pulling `timescale/timescaledb:2.17.2-pg16` because the Docker storage filesystem ran out of space. No Samples/s, elapsed time, or disk result is claimed. The M0 storage gate therefore remains an explicit checkpoint blocker until the image can start and `AUTOANGEL_TSDB_URL` can be benchmarked.

## NIF gate

### NIF gate benchmark (2026-11-04T22:13:40.836644Z)

Machine: 2 BEAM schedulers, 16331076 kB host memory.
Workload: warmed Elixir scoring kernel, 20000 Series, batch sizes 1, 2, 4, 8, 16, 32, 64, 128, 256.
Throughput gate: 2000 Samples/s (20,000 Series at one sample per 10 seconds).
- batch 1: 9271.82 Samples/s, 107.854 us/call average, 790.0 us max, 644.0 us p99 (2157073 us total)
- batch 2: 13802.07 Samples/s, 144.906 us/call average, 703.0 us max, 650.0 us p99 (2898117 us total)
- batch 4: 18432.44 Samples/s, 217.009 us/call average, 1342.0 us max, 939.0 us p99 (4340175 us total)
- batch 8: 21645.01 Samples/s, 369.6 us/call average, 800.0 us max, 741.0 us p99 (7392003 us total)
- batch 16: 22820.64 Samples/s, 701.12 us/call average, 1391.0 us max, 1357.0 us p99 (14022391 us total)
- batch 32: 23685.95 Samples/s, 1351.012 us/call average, 1975.0 us max, 1794.0 us p99 (27020236 us total)
- batch 64: 23939.48 Samples/s, 2673.408 us/call average, 2462.0 us max, 2452.0 us p99 (53468152 us total)
- batch 128: 24134.07 Samples/s, 5303.706 us/call average, 4654.0 us max, 4650.0 us p99 (106074125 us total)
- batch 256: 23999.21 Samples/s, 10667.017 us/call average, 9463.0 us max, 9013.0 us p99 (213340340 us total)
Memory: 4295.0 encoded-state bytes/Series (85900000 bytes for 20000 full-window states); BEAM term reference overhead 64.0 bytes/Series. No independent memory promotion target is defined by the requirements.
Exposition parsing CPU share: 4.96% (116 ms parsing, 2225 ms scoring, 20000 Samples).
Decision: selected elixir; promoted kernels: none; final maximum batch size 2; Elixir call duration target 1000 us at maximum batch size. Duration target met by maximum observed call: true. Throughput target met: true. Elixir scoring met the 2,000 Samples/s throughput gate and remains selected; no kernel is promoted to Rust. Calls above 1 ms require dirty scheduling or chunking under Requirement 47.3.
Dependency status: The engine/native crate exists, but Rustler is unavailable in the current Mix environment; only the Elixir reference kernel was measured.

Task 4.3 (Rust score-bounds property) is intentionally skipped because this gate promoted no kernels. The native crate keeps an explicit empty promotion registry and a guard test; if a future M0 gate promotes a kernel, the Rust mirror property must be added for that kernel before the registry is populated.

Task 4.5 (Rust state encode/decode round-trip property) is intentionally guarded rather than implemented because this gate promoted no kernels and the Rust crate has no state codec. The Elixir reference state contract remains covered by `Autoangel.Kernel.State`; a future Rust promotion must add shared-contract round-trip coverage before the promotion registry is populated.

Compile-time selection: `Autoangel.Kernel.Selected` is fixed to `:elixir` while the M0 promotion set is empty. It delegates to `Autoangel.Kernel.Scoring.Elixir` and does not load or call a Rust kernel. A future promotion must change this selector together with the Rust implementation, shared state-encoding coverage, and NIF boundary tests.

## Provisional defaults

Defaults marked provisional in the design remain subject to the M0 benchmark. Changes and their measured rationale will be recorded here.

The measured NIF gate fixes the maximum Elixir batch size at 2 Samples and retains a 1 ms call-duration target. The M0 decision is recorded in the design defaults; no Rust kernel is promoted.
