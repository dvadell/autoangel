# Implementation Plan: v1 (autoangel-engine, autoangel-console, CLI)

## Overview

The plan follows the PRD milestones. M0 builds the numeric core, configures and runs the required cross-language code-quality gate, and runs the storage and NIF benchmark gates, with results recorded in `docs/decisions.md`. M1 builds the Engine core (config, ingest, registry, Workers, checkpointing, storage, plugins, self-monitoring, Control API health) and the CLI. M2 adds explanation: correlation, enriched alerts, stored anomaly records, Control_API feedback, Heartbeat and the Replay_Harness. M3 builds the Console web frontend and the fault-injection suite. M4 builds the Console MCP_Server. M5 adds the P1 detection and alerting features and Telegram buttons. M6 adds more plugins, packaging, multi-arch CI, privacy and scale benchmarks, and documentation.

Languages follow the design: Elixir/OTP for the Engine, Console and Replay_Harness, Rust for the NIF crate and the CLI. P1 work and all test sub-tasks are marked optional with `*`. Property tests use StreamData (Elixir) or proptest (Rust) and carry the tag `Feature: v1, Property N: <title>`.

## Tasks

### M0: Spike and benchmark gates

- [x] 1. Set up the monorepo and shared test tooling
  - [x] 1.1 Scaffold the monorepo layout from the design
    - Create the `engine` umbrella with `apps/autoangel_core`, `apps/autoangel_tsdb` and `rel/`, plus `shared/autoangel_schema`, `console` (empty Mix project placeholder), `cli` (Rust crate), `replay`, and `docs/decisions.md` with sections for the storage gate, the NIF gate and provisional defaults
    - Add StreamData/ExUnitProperties to the Elixir apps and proptest to the Rust crates, with the minimum run counts from the Testing Strategy
    - Define the `Autoangel.Clock` behaviour with a `System` implementation and a simulated clock for tests
    - _Requirements: 22.4, 47.1, 65.1, 72.5_

- [x] 1.2 Configure and enforce the required code-quality gate
  - Configure Mix Credo with strict mode and Mix Dialyzer for the Elixir projects, and configure the Rust gate for the workspace and all targets/features.
  - Configure the gate to run these Elixir checks: `mix format --check-formatted`, `mix credo --strict`, `mix dialyzer` and `mix test`.
  - Configure the gate to run these Rust checks: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, `cargo test --workspace --all-features` and `cargo audit`.
  - Add an automated test that verifies the configured commands run in CI when CI is added, without claiming that CI already exists; require dependency advisories reported by `cargo audit` to be resolved or explicitly documented.
  - _Requirements: 49.4, 71.1, 71.2, 71.3, 71.4, 72.5_

- [x] 2. Implement the kernel state encoding and the Elixir reference scoring kernel
  - [x] 2.1 Implement the versioned kernel state encoding (`Autoangel.Kernel.State`)
    - Little-endian layout with magic `AANG1`, version, kind, flags, window ring and sorted copy, `prev`, spacing ring, reserved level/seasonal/longterm sections, CRC32
    - Decoding checks magic, version, every length bound and the CRC, and returns `{:error, :bad_state}` on any mismatch without raising
    - _Requirements: 47.6, 47.10_

  - [x]* 2.2 Write property test for malformed state handling
    - **Property 48: Malformed state is an error, not a crash**
    - **Validates: Requirements 47.10**

  - [x] 2.3 Implement `Autoangel.Kernel.Scoring` behaviour and the Elixir implementation
    - `score(state_bin, samples, params) -> {:ok, new_state, results} | {:error, reason}` as a pure function, with `Autoangel.Kernel.Params`
    - Robust median/MAD over a W = 256 ring plus sorted array, scale floor, `100 × −expm1(−z·ln2/3)` mapping clamped to [0, 100], expected range from `z_T`, clamped to finite doubles, Baseline_Type `:robust`
    - Score against the window before adding the Sample; empty window scores 0 with range `[x, x]`; non-finite values return no score and leave the Baseline unchanged
    - Counter handling: no score for the first Sample or non-increasing timestamps, rate `(v − prev)/Δs`, reset rate `v/Δs`, `prev` kept as the latest-timestamp Sample
    - Spacing ring of the last 16 positive spacings and Typical_Interval as their median
    - _Requirements: 5.4, 5.5, 5.8, 5.9, 6.1, 6.2, 6.3, 6.4, 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 9.7, 9.8, 17.1, 17.2, 17.3_

  - [x]* 2.4 Write property test for counter rates
    - **Property 8: Counter rates are never negative**
    - **Validates: Requirements 5.5, 5.8, 5.9**

  - [x]* 2.5 Write property test against the naive reference model
    - **Property 9: Scoring kernel matches the reference model**
    - **Validates: Requirements 5.4, 6.2, 9.1, 9.2, 16.1**

  - [x]* 2.6 Write property test for Typical_Interval
    - **Property 10: Typical_Interval comes only from positive spacings**
    - **Validates: Requirements 6.1, 6.3, 6.4**

  - [x]* 2.7 Write property test for score bounds and explanations
    - **Property 12: Scores are bounded and always explained**
    - **Validates: Requirements 9.4, 12.3, 13.1, 17.1, 17.2**

  - [x]* 2.8 Write property test for score monotonicity
    - **Property 13: Score monotonicity**
    - **Validates: Requirements 9.5**

  - [x]* 2.9 Write property test for zero score at the median
    - **Property 14: Zero exactly at the median**
    - **Validates: Requirements 9.3, 9.6, 9.7**

  - [x]* 2.10 Write property test for non-finite values
    - **Property 15: Non-finite values change nothing**
    - **Validates: Requirements 9.8, 17.3**

  - [x]* 2.11 Write property test for kernel determinism
    - **Property 46: Kernel calls are deterministic**
    - **Validates: Requirements 47.6, 49.2**

- [x] 3. Implement the M0 storage gate and NIF gate
  - [x] 3.1 Implement `Autoangel.StateStore` on CubDB (storage gate, State_Store part)
    - Keys from the State_Store records table; two-phase checkpoint write (entries under a new id, atomic switch of `:current`, delete old entries); synchronous writes for Incidents, mutes and sensitivity overrides
    - Add a benchmark Mix task that writes and restores a checkpoint for 20,000 Series and appends checkpoint duration and size to `docs/decisions.md`
    - _Requirements: 43.2, 43.3, 43.4, 43.9_

  - [x] 3.2 Implement a TimescaleDB write benchmark (storage gate, database part)
    - Prototype `COPY samples FROM STDIN BINARY` through `Postgrex.stream/3` with a multi-row INSERT fallback in `autoangel_tsdb`
    - Add a benchmark Mix task that writes 20,000 Series at 10 s to 60 s intervals and appends Samples/s, round trips per batch, machine CPU count, memory and disk type to `docs/decisions.md`
    - _Requirements: 36.3, 71.1_

  - [x] 3.3 Implement the NIF gate benchmark for the Elixir kernels
    - Benchmark Mix task measuring Elixir scoring throughput, memory per Series and per-call duration at batch sizes 1 to 256 for 20,000 Series; also measure exposition parsing CPU share
    - Write results and the decision (which kernels, if any, are promoted to Rust; final maximum batch size and NIF call duration target) to `docs/decisions.md`
    - _Requirements: 47.2, 47.8, 71.1, 71.4_

- [x] 4. Set up the NIF crate and boundary safety
  - [x] 4.1 Scaffold `engine/native/autoangel_kernels`
    - Kernel logic in a plain library module, thin `#[rustler::nif]` wrapper behind a `nif` cargo feature, `test-panic` feature for panic tests
    - Cargo release profile `panic = "unwind"`; CI scripts for the `panic = "unwind"` check, the `unsafe` audit (`// SAFETY:` comment plus an entry in `native/UNSAFE_REVIEW.md`) and rejection of `test-panic` in release builds
    - _Requirements: 47.1, 47.4, 47.5, 49.1_

  - [x] 4.2 Port each kernel promoted by the NIF gate to Rust
    - Implement `Autoangel.Kernel.Scoring.Nif` (and any other promoted kernel) reading and writing the same state encoding, selected at compile time; run on a dirty CPU scheduler or chunk calls when the benchmarked duration exceeds the target
    - Return an error value for undecodable state without panicking; retain no state between calls
    - If task 3.3 promoted no kernel, leave the Elixir implementation selected and note this in `docs/decisions.md`
    - _Requirements: 47.2, 47.3, 47.4, 47.6, 47.10_

  - [x]* 4.3 Write Rust property test for score bounds of promoted kernels
    - **Property 12: Scores are bounded and always explained** (Rust mirror)
    - **Validates: Requirements 49.2, 9.4**

  - [x]* 4.4 Write Rust property test for determinism of promoted kernels
    - **Property 46: Kernel calls are deterministic** (Rust mirror)
    - **Validates: Requirements 49.2, 47.6**

  - [x]* 4.5 Write Rust property test for state encode/decode round trip
    - **Property 42: Checkpoint round trip** (Rust mirror of state round trip)
    - **Validates: Requirements 49.2**

  - [x] 4.6 Implement NIF-boundary tests for every loaded NIF
    - Elixir tests with valid input asserting returned terms, and invalid input (malformed state, oversized batch, wrongly typed argument) asserting `{:error, _}`
    - Panic test with the sentinel batch under `test-panic`, asserting an Elixir error, a running BEAM and a successful next call
    - Golden comparison against fixtures in `native/autoangel_kernels/tests/data/` with the tolerances from the design, reporting kernel, dataset, expected, actual and deviation on failure
    - _Requirements: 47.4, 49.3, 49.4, 49.5, 49.6_

- [x] 5. Checkpoint - M0 gates complete
  - Ensure all tests pass, ask the user if questions arise.

### M1: Engine core and CLI

- [x] 6. Implement Engine configuration
  - [x] 6.1 Implement `Autoangel.Config` with validation and secret redaction
    - Load TOML plus environment overrides for secrets, apply the defaults table, validate every setting before any child starts, log one line per invalid setting by name and exit non-zero
    - Validate cardinality limits, learning duration and cycles, missing-data multiple and sweep interval, persistence N and M, candidate pool limit and support threshold, retention windows, Heartbeat interval, ingest rate limit, storage backend selection, Scrape_Targets, separate ingest and Control_API credential settings, and auth exemptions (health and readiness only)
    - Wrap secrets in `%Autoangel.Secret{}` with redacting `Inspect`/`String.Chars`, and add a Logger filter that redacts configured secret values as substrings
    - _Requirements: 7.3, 8.1, 8.6, 10.5, 11.9, 14.1, 14.6, 19.8, 33.2, 33.3, 39.8, 41.5, 74.2, 74.3, 74.6, 74.8_

  - [x]* 6.2 Write unit tests for configuration validation
    - One test per invalid setting class, asserting the exit and the logged setting name
    - _Requirements: 8.6, 10.5, 11.9, 14.6, 19.8, 33.3, 39.8, 41.5, 74.8_

- [x] 7. Implement the exposition parser and printer
  - [x] 7.1 Implement `Autoangel.Exposition.parse/1` and `print/1`
    - Prometheus text format 0.0.4: HELP, TYPE, labels with escapes, NaN and ±Inf, optional integer ms timestamps, name grammar; errors carry the first invalid line number
    - Attach `type_hint` to each Sample; printer sorts labels and uses shortest round-trip floats
    - _Requirements: 1.5, 1.6, 1.7, 1.9, 2.5_

  - [x]* 7.2 Write property test for the exposition round trip
    - **Property 1: Exposition round trip**
    - **Validates: Requirements 1.6, 1.7**

  - [x]* 7.3 Write property test for invalid line reporting
    - **Property 2: An invalid line is reported by its line number**
    - **Validates: Requirements 1.5, 2.5**

- [x] 8. Implement the Series registry and Series_Kind inference
  - [x] 8.1 Implement `Autoangel.SeriesRegistry`
    - Canonical `series_key`, UUIDv5 `series_id`, `worker_index = phash2(key, pool_size)`, ETS table plus atomics counters
    - Atomic `resolve_or_create/2` that never exceeds per-name or total limits under concurrency; rejection increments `autoangel_rejected_series_total{limit}` and logs a rate-limited warning naming the metric and limit
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 8.1, 8.2, 8.3, 8.4, 8.5, 48.1, 48.2_

  - [x] 8.2 Implement Series_Kind inference
    - TYPE counter/gauge wins, histogram/summary expansion by `_bucket`/`_count`/`_sum` and `quantile`, otherwise case-sensitive suffix rule; a later TYPE line changes the kind of an existing Series
    - _Requirements: 1.9, 5.1, 5.2, 5.3, 5.7_

  - [x]* 8.3 Write property test for canonical identity and Worker mapping
    - **Property 5: Canonical Series identity and Worker mapping**
    - **Validates: Requirements 4.3, 48.1, 48.2**

  - [x]* 8.4 Write property test for cardinality limits under concurrency
    - **Property 6: Cardinality limits hold under concurrency**
    - **Validates: Requirements 4.5, 8.2, 8.4, 8.5**

  - [x]* 8.5 Write property test for Series_Kind inference
    - **Property 7: Series_Kind inference**
    - **Validates: Requirements 1.9, 5.1, 5.2, 5.3, 5.7**

- [x] 9. Implement Workers and detection decisions
  - [x] 9.1 Implement `Autoangel.Detect.Decision.step/4`
    - Learning start and end (duration rule; seasonal-cycles rule as a hook for M5), learning-to-healthy transition
    - Anomalous-Sample test against the effective threshold (default T = 75), run tracking with N-or-M persistence, start time = first Sample of the run, nil scores neither extend nor end the run, no second Anomaly while one is open
    - Symmetric hysteresis resolution of value Anomalies, `learning? = true` on Anomalies raised during learning, kernel reset on Series_Kind change keeping status and learning progress
    - Missing-data rules: raise at sweep time when `now − last_recv > multiple × typical_interval`, classification target down or metric silent, silent status only after learning, resolve at the next Sample's receive time and recompute status
    - _Requirements: 4.4, 5.1, 6.5, 10.1, 10.2, 10.4, 11.1, 11.2, 11.4, 11.5, 11.6, 11.7, 11.8, 14.1, 14.2, 14.3, 14.4, 14.5_

  - [x] 9.2 Implement the Worker GenServer and `Autoangel.Workers.Supervisor`
    - Own `series_key -> %SeriesState{}`, decrement the atomic queue counter on dequeue, batch pending Samples per Series, sort by timestamp and split into chunks of at most `max_batch`
    - Call the selected kernel, fold results through `Decision.step/4`, handle `{:error, :bad_state}` and NIF errors by resetting only that Series
    - One sweep timer per Worker for missing-data checks, reading Scrape_Target failing state from ETS
    - `one_for_one` pool sized from `System.schedulers_online()` or config, fixed while running
    - _Requirements: 6.2, 9.1, 11.1, 11.3, 11.4, 47.7, 47.9, 48.1, 48.3, 48.4, 48.5, 48.7_

  - [x]* 9.3 Write property test for constant gauges
    - **Property 11: A constant gauge never raises an Anomaly**
    - **Validates: Requirements 6.5**

  - [x]* 9.4 Write property test for the learning period
    - **Property 16: Learning blocks alerts and ends on time**
    - **Validates: Requirements 10.1, 10.2, 10.3, 10.4, 11.8**

  - [x]* 9.5 Write property test for the persistence rule
    - **Property 17: Persistence rule matches the run model**
    - **Validates: Requirements 14.2, 14.3, 14.4, 14.5**

  - [x]* 9.6 Write property test for missing-data detection
    - **Property 19: Missing-data detection**
    - **Validates: Requirements 11.1, 11.2, 11.3, 11.4, 11.5, 11.6, 11.7, 15.6**

  - [x]* 9.7 Write property test for chunking
    - **Property 47: Chunking does not change results**
    - **Validates: Requirements 47.7, 47.9**

- [x] 10. Implement state checkpointing and restore
  - [x] 10.1 Implement the checkpointer, restore at boot and Worker restart restore
    - Periodic checkpoint (default 60 s) that snapshots one Worker at a time, stores kernel state and `SeriesMeta`, and keeps the previous checkpoint readable on failure
    - Restore Baselines, statuses, learning progress, open Incidents, mutes and sensitivity overrides before the first score; skip and log undecodable entries so those Series start fresh
    - Final checkpoint in `Autoangel.Application.prep_stop/1`; a restarted Worker restores only its own Series filtered by `worker_index`
    - _Requirements: 43.1, 43.2, 43.3, 43.5, 43.6, 43.7, 43.8, 43.9, 43.10, 48.8, 48.9_

  - [x]* 10.2 Write property test for the checkpoint round trip
    - **Property 42: Checkpoint round trip**
    - **Validates: Requirements 43.5, 43.7, 49.2**

  - [x]* 10.3 Write property test for corrupt checkpoint entries
    - **Property 43: Corrupt Checkpoint entries are isolated**
    - **Validates: Requirements 43.10**

  - [x]* 10.4 Write unit tests for the State_Store
    - Periodic and shutdown checkpoints, a failure halfway through a write, mute and Incident persistence ordering
    - _Requirements: 43.2, 43.3, 43.8, 43.9_

- [x] 11. Implement ingest HTTP, authentication and rate limiting
  - [x] 11.1 Implement `Autoangel.Auth` for the ingest and Control_API roles
    - Bearer or basic credentials compared in constant time against the role's own credential set; 401 for missing, malformed, wrong or other-role credentials and when the role has no configured credential
    - Explicit exemptions allowed only for `/healthz` and `/readyz`, returning status only
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 74.1, 74.2, 74.6, 74.7_

  - [x] 11.2 Implement `Autoangel.Ingest.RateLimit`
    - Per-credential token bucket in ETS (default 50 req/s, burst 200), lazily refilled; 429 with `Retry-After`
    - _Requirements: 74.4, 74.5_

  - [x] 11.3 Implement the push pipeline at `POST /api/v1/push`
    - Plug order: auth, rate limit, body read with max size (413 `request body too large`), parse (400 with line), admission with all-or-nothing per-Worker reservation (503 `engine overloaded`, load-shedding counter), registry resolution with per-Sample cardinality rejection, cast to Workers, non-blocking cast of raw Samples to the Storage_Writer when configured, reply 202 with accepted and rejected counts
    - Use the pushed timestamp or the receive time; one Sample per pushed line per push
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.8, 4.1, 5.6, 8.2, 8.5, 34.5, 48.6_

  - [x] 11.4 Implement the Ingest and Control listeners with optional TLS
    - Bandit listeners on 9400 and 9401; serve TLS when cert and key are configured; log which file failed and exit, never falling back to plain HTTP
    - _Requirements: 7.5, 7.6_

  - [x]* 11.5 Write property test for the accepted Sample flow
    - **Property 3: Accepted Samples flow unchanged**
    - **Validates: Requirements 1.1, 1.2, 1.3, 1.4, 5.6, 8.2, 36.1**

  - [x]* 11.6 Write property test for load shedding
    - **Property 49: Load shedding is all or nothing**
    - **Validates: Requirements 48.6**

  - [x]* 11.7 Write property test for authentication on Engine endpoints
    - **Property 50: Authentication requires an exact credential of the right role**
    - **Validates: Requirements 7.1, 7.2, 7.4, 30.7, 45.5, 46.4, 74.1, 74.2, 74.7**

  - [x]* 11.8 Write property test for per-credential rate limits
    - **Property 51: Rate limits are per credential**
    - **Validates: Requirements 74.4, 74.5**

  - [x]* 11.9 Write unit tests for ingest status codes and TLS
    - One example per row of the ingest error table, body size at limit and limit + 1, TLS misconfiguration
    - _Requirements: 1.5, 1.8, 7.2, 7.6, 48.6, 74.5_

- [x] 12. Implement the Scraper
  - [x] 12.1 Implement `Autoangel.Scrape.Target` and its supervisor
    - One GenServer per valid target, ticks aligned to the interval, at most one in-flight fetch (skips counted), `receive_timeout = scrape_timeout`
    - Successful bodies go through the push processing path with the scrape receive time; failures and invalid bodies ingest nothing, mark the target failing in ETS and log a warning with the first invalid line; success clears failing; invalid targets are logged and skipped
    - Tag Samples with `{:scrape, target_id}`; do not start the subtree when no targets are configured
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 11.4_

  - [x]* 12.2 Write unit tests for the Scraper with a Bypass target
    - Skipped tick while a fetch is in flight, timeout, non-2xx, invalid body, recovery
    - _Requirements: 2.2, 2.4, 2.5, 2.6_

- [x] 13. Implement the storage behaviour, Storage_Writer and TimescaleDB backend
  - [x] 13.1 Define `Autoangel.Storage` behaviour and backend selection
    - Callbacks `init`, `append_samples`, `write_anomalies`, `write_series`, `write_bands`, `read_range`, `read_aggregates`, each returning `:ok | {:retryable, _} | {:permanent, _}`
    - Zero or one backend; refuse to start for several backends or a module missing callbacks
    - _Requirements: 33.1, 33.2, 33.3, 33.5, 33.6_

  - [x] 13.2 Implement `Autoangel.Storage.Writer` under its own supervisor
    - Bounded queue (default 200,000 items) that drops the oldest Sample items first, FIFO batches up to 5,000, retry with delay at least doubling (500 ms to 60 s), permanent failures dropped and logged, counters for successes, retryable, permanent and dropped Samples, `batch_uuid` per batch
    - Only enqueue until the Migrator reports success; emit drop events for the Self_Monitor
    - _Requirements: 34.1, 34.2, 34.3, 34.4, 34.5, 34.6, 34.7, 34.8, 34.9, 36.5, 36.6, 38.10_

  - [x] 13.3 Implement the `AutoangelTsdb` backend and migrations
    - SCHEMA_VERSION 1 migrations for series, samples hypertable, `ingest_batches`, 1-minute and 1-hour continuous aggregates (finite values only), `baseline_bands` and its 1-hour aggregate, anomalies, anomaly_candidates with cascade, engine_status and schema_version; idempotent re-runs and retry-safe partial migrations; `Storage.Migrator` retrying every 60 s
    - `append_samples` via COPY with transactional `batch_uuid` deduplication; series upserts; `read_range` and `read_aggregates`; raw values and ms timestamps stored exactly
    - _Requirements: 5.6, 33.4, 36.1, 36.2, 36.3, 36.4, 36.5, 38.1, 38.2, 38.3, 38.4, 38.5, 38.6, 38.7, 38.8, 38.9, 38.10_

  - [x] 13.4 Implement retention policies and anomaly retention
    - Apply raw (14 d) and aggregate (13 months) retention policies at each start, replacing differing intervals; continuous aggregate refresh policy whose start offset is smaller than raw retention; periodic delete of Anomalies ended before the anomaly retention (90 d) with candidates removed by cascade
    - _Requirements: 39.1, 39.2, 39.3, 39.4, 39.5, 39.6, 39.7_

  - [x]* 13.5 Write property test for Storage_Queue capacity
    - **Property 37: Storage_Queue never exceeds capacity**
    - **Validates: Requirements 34.4, 34.6, 34.7**

  - [x]* 13.6 Write property test for Storage_Writer FIFO and retries
    - **Property 38: Storage_Writer follows FIFO and the retry rules**
    - **Validates: Requirements 34.2, 34.3, 34.4, 34.8, 36.6**

  - [x]* 13.7 Write property test for the Sample storage round trip
    - **Property 39: Sample storage round trip**
    - **Validates: Requirements 36.2, 36.4**

  - [x]* 13.8 Write property test for exactly-once writes
    - **Property 40: Exactly once after a lost acknowledgement**
    - **Validates: Requirements 36.5**

  - [x]* 13.9 Write TimescaleDB integration tests
    - Migrations from empty and re-run with no changes, failed migration leaves the last state and blocks writes, retention policies and their replacement
    - _Requirements: 38.1, 38.9, 38.10, 39.2, 39.7_

- [x] 14. Implement plugins and alert delivery
  - [x] 14.1 Define `Autoangel.Plugin` and the plugin registry
    - Documented `validate_config/1` and `deliver/2` callbacks with success, retryable and permanent outcomes
    - Registry of compiled-in modules, enable only configured plugins, call `validate_config/1` exactly once per enabled plugin before the Dispatcher starts, refuse to start for invalid config (field names only) or unknown plugins
    - _Requirements: 22.1, 22.2, 22.3, 22.6, 22.7, 22.8_

  - [x] 14.2 Implement the `PluginRunner` and `Alert.PluginSupervisor`
    - Bounded queue of 1,000 alerts per plugin, each `deliver/2` in `Task.Supervisor.async_nolink` with a 10 s timeout, up to 5 attempts with strictly increasing jittered backoff, failure records naming plugin, Anomaly and error class
    - Circuit breaker (opens above 5 consecutive failed deliveries, 5 min cool-off, half-open retry), skips recorded, per-plugin outcome counters for `/metrics`; runners never wait on each other
    - _Requirements: 25.1, 25.2, 25.3, 25.4, 25.5, 25.6, 25.7, 25.8, 45.6, 48.5, 48.7_

  - [x] 14.3 Implement the Email plugin app `engine_plugin_email`
    - SMTP delivery to configured server, sender and recipients; validation of missing fields; retryable vs permanent classification of SMTP replies
    - _Requirements: 22.4, 23.1, 23.3, 23.4_

  - [x] 14.4 Implement the Telegram plugin app `engine_plugin_telegram`
    - Bot API `sendMessage` to the configured chat; validation of missing fields; retryable vs permanent classification of HTTP replies
    - _Requirements: 22.4, 23.2, 23.3, 23.5_

  - [x] 14.5 Implement `Autoangel.Alert.Policy` and `Alert.Dispatcher`
    - Pure `on_event/3` for Incidents per Series, deduplication while an Incident is open, close on resolution, cooldown (default 30 min) from close, one alert per enabled plugin for a new Incident, learning events as a no-op guard
    - Persist Incident open and close to the State_Store; build a basic `%Alert{}` with name, labels, observed value, range, score and start time
    - _Requirements: 10.1, 26.1, 27.1, 27.2, 27.3, 27.4, 27.5, 27.6, 27.7, 43.3_

  - [x] 14.6 Add CI dependency checks
    - `mix xref` plus dependency-tree check that fails and names any `engine_plugin_*` app in the closure of `autoangel_core`; xref check that `Autoangel.Detect`, `Autoangel.Correlate` and `Autoangel.Alert` never reference `AutoangelTsdb.*`
    - _Requirements: 22.5, 33.5_

  - [x]* 14.7 Write property test for Incident membership
    - **Property 27: Incident membership invariant**
    - **Validates: Requirements 27.4, 28.4**

  - [x]* 14.8 Write property test for the retry schedule
    - **Property 30: Retry schedule**
    - **Validates: Requirements 25.2, 25.3, 25.4**

  - [x]* 14.9 Write property test for the circuit breaker
    - **Property 31: Circuit breaker state machine**
    - **Validates: Requirements 25.5, 25.6, 25.7**

  - [x]* 14.10 Write unit tests for plugins
    - SMTP and Bot API classification tables against fake servers, `validate_config/1` missing-field cases, validation called once per enabled plugin
    - _Requirements: 22.6, 23.3, 23.4, 23.5_

- [x] 15. Implement self-monitoring
  - [x] 15.1 Implement `Autoangel.SelfMonitor`
    - Telemetry-driven state machines with a 5 s tick for no-ingest, database-unreachable, storage-overflow (with drop count) and per-plugin failure (above 3 consecutive failures)
    - One alert per episode through the self-monitoring plugin set minus the failing plugin, using the PluginRunners; log the full alert when no plugin remains; do not register storage conditions in Topology_A
    - _Requirements: 32.1, 32.2, 32.3, 32.4, 32.5, 32.6, 32.7, 32.8, 35.5_

  - [x]* 15.2 Write property test for self-monitoring episodes
    - **Property 35: One self-monitoring alert per episode**
    - **Validates: Requirements 32.1, 32.2, 32.3, 32.4, 32.7**

  - [x]* 15.3 Write property test for self-monitoring recipients
    - **Property 36: Self-monitoring alerts avoid the failing Plugin**
    - **Validates: Requirements 32.4, 32.5, 32.8**

- [x] 16. Implement the Control API health endpoints and wire the Engine application
  - [x] 16.1 Implement `/healthz`, `/readyz` and `/metrics` on the Control listener
    - Health always 200 when authenticated; readiness 200 only when the boot phase is `:ready`, otherwise 503
    - `/metrics` printed with `Autoangel.Exposition.print/1`, including ingest rate, Series count, detection latency histogram, dropped Samples, queue depth, storage outcomes, per-plugin delivery outcomes, rejected Series, load-shed Samples and NIF call duration
    - _Requirements: 8.3, 34.4, 45.1, 45.2, 45.3, 45.4, 45.5, 45.6, 48.6_

  - [x] 16.2 Wire `Autoangel.Application` and the supervision tree
    - Boot order from the design (Config, State_Store, registry, Workers, Correlator and Alert supervisors, optional Storage and Scrape supervisors, SelfMonitor, Background supervisor, Control listener early, Ingest listener last setting the boot phase to `:ready`)
    - Topology_A: no database settings or connections, no Storage_Writer or storage enqueue, startup log naming DET-5, COR-7 and STE-4 as unavailable
    - Exit non-zero when the State_Store volume cannot be opened
    - _Requirements: 34.1, 34.9, 35.1, 35.2, 35.3, 35.4, 43.4, 45.2, 48.5, 48.7_

  - [x]* 16.3 Write property test for secret redaction
    - **Property 52: Secrets never appear in logs**
    - **Validates: Requirements 22.7, 74.3**

  - [x]* 16.4 Write integration and unit tests for the wired Engine
    - Topology_A push-to-alert flow with a test capture plugin; readiness 503 before boot and 200 after; `/metrics` contents; exemption only for health and readiness
    - _Requirements: 35.1, 45.2, 45.3, 45.4, 74.6_

- [x] 17. Implement the CLI
  - [x] 17.1 Implement the CLI skeleton, configuration and shared grammar validation
    - clap commands `run` and `push`, configuration for Ingest_Endpoint address, bearer or basic credentials, per-attempt timeout and retry duration (validated positive), metric and label name grammar shared with the Engine parser spec
    - _Requirements: 68.3, 68.8, 70.2, 70.3_

  - [x] 17.2 Implement `tool run`
    - Spawn the command with arguments unchanged, inherit stdio, record exit code and wall-clock duration, optional CPU time and max RSS through `libc::wait4`, pass the exit code through, `128 + signo` on signal, error messages on stderr only, missing-argument and spawn-failure exits
    - _Requirements: 67.1, 67.2, 67.3, 67.4, 67.5, 67.6, 67.7_

  - [x] 17.3 Implement the push client, body builder and textfile writer
    - Build the Prometheus body with `autoangel_job_*` metrics labelled with the job name, all sharing the run start timestamp; bearer or basic auth; retry with backoff until the retry duration elapses, then one stderr line and drop
    - Optional node_exporter textfile without timestamps written via temp file, fsync and rename; one stderr line on failure while still pushing
    - _Requirements: 68.1, 68.2, 68.3, 68.4, 68.5, 68.6, 68.7_

  - [x] 17.4 Implement `tool push`
    - Validate NAME, VALUE and `--label k=v` arguments, send one request, exit 0 on success and non-zero with a stderr message on failure
    - _Requirements: 70.1, 70.2, 70.3, 70.4, 70.5_

  - [x] 17.5 Configure the static musl build
    - Build targets `x86_64-unknown-linux-musl` and `aarch64-unknown-linux-musl` with rustls and no dynamic dependencies
    - _Requirements: 68.9_

  - [x]* 17.6 Write property test for CLI transparency
    - **Property 64: The CLI is transparent to the wrapped command**
    - **Validates: Requirements 67.1, 67.3, 67.4, 69.5**

  - [x]* 17.7 Write property test for CLI push bodies
    - **Property 66: CLI bodies are valid and share one run timestamp**
    - **Validates: Requirements 68.1, 68.2, 68.6, 69.3, 70.1**

  - [x]* 17.8 Write property test for CLI argument validation
    - **Property 67: CLI validation agrees with the Engine grammar**
    - **Validates: Requirements 70.2, 70.3**

  - [x]* 17.9 Write property test for CLI retries
    - **Property 68: CLI retries stay inside the retry duration**
    - **Validates: Requirements 68.4, 68.5**

  - [x]* 17.10 Write unit tests for CLI error paths
    - Missing arguments, command not found, signal termination, atomic textfile replace, textfile write failure
    - _Requirements: 67.5, 67.6, 67.7, 68.6, 68.7_

- [x] 18. Checkpoint - M1 Engine core and CLI complete
  - Ensure all tests pass, ask the user if questions arise.

### M2: Explain

- [x] 19. Implement the Correlator
  - [x] 19.1 Implement `Autoangel.Correlate`
    - ETS ordered-set Anomaly_Event_Index pruned after 2 × window, LRU pair co-occurrence table (100,000 pairs)
    - On `anomaly_started`: insert, select one event per other Series inside the inclusive window, compute factors (prox, before, mag, shared host/job/service, hist), rank by weighted strength with tuple and series-id tie breaks, compute evidence and the weak marker, then increment pair counts
    - Forward to the Dispatcher only when not learning; send to the Storage_Writer in every case when configured; never read TimescaleDB
    - _Requirements: 18.1, 18.2, 18.3, 18.4, 18.5, 18.6, 18.7, 18.8, 18.9, 19.1, 19.2, 19.3, 35.2_

  - [x]* 19.2 Write property test for Candidate selection
    - **Property 23: Candidate selection invariant**
    - **Validates: Requirements 18.1, 18.3, 18.7, 18.8, 20.4**

  - [x]* 19.3 Write property test for ranking
    - **Property 24: Ranking is a permutation with one-factor dominance**
    - **Validates: Requirements 18.4, 18.5, 20.4**

  - [x]* 19.4 Write property test for weak evidence marking
    - **Property 25: Weak evidence marking**
    - **Validates: Requirements 19.1, 19.2, 19.3, 20.4**

- [x] 20. Implement enriched alert content
  - [x] 20.1 Implement `Autoangel.Alert.Render.build/3` and plugin formatting
    - Full `%Alert{}`: name, all labels, observed value, both bounds, score labelled "anomaly score" without `%`, ISO 8601 start with zone, top candidates (default 5) in ascending rank with "(weak evidence)", the "no candidates found inside the Correlation_Window" statement, Console link `{base}/anomalies/{anomaly_id}` only when configured
    - Self-monitoring alert rendering stating that it concerns the Engine itself; Email and Telegram format the same struct
    - _Requirements: 17.4, 17.5, 17.7, 19.4, 19.5, 19.7, 23.1, 23.2, 26.1, 26.2, 26.3, 26.4, 26.5, 32.6_

  - [x]* 20.2 Write property test for alert rendering
    - **Property 32: Alert rendering is complete and uses neutral wording**
    - **Validates: Requirements 17.4, 17.5, 17.7, 19.4, 19.5, 19.7, 26.1, 26.2, 26.3, 26.4, 26.5, 28.2, 29.2, 29.3, 32.6**

  - [x]* 20.3 Write unit test for banned words in templates
    - Grep rendered templates for `cause`, `caused`, `root cause`, `probability`, `likelihood`, `chance` and `%`
    - _Requirements: 17.7, 19.7_

- [x] 21. Store anomaly records, candidates, series rows and bands
  - [x] 21.1 Write Anomalies, Candidates, Series rows and baseline bands through the Storage_Writer
    - Enqueue a `{:series, row}` item at Series creation and on status changes, ahead of dependent items
    - Write each Anomaly with all its Candidates (rank, evidence, weak) in one transaction; later updates touch only the anomalies row
    - Write `baseline_bands` rows per Series once per checkpoint interval
    - _Requirements: 17.4, 21.1, 21.3, 21.4, 21.5, 38.2, 38.5, 38.6, 53.1_

  - [x]* 21.2 Write property test for stored Candidates
    - **Property 26: Stored Candidates round trip**
    - **Validates: Requirements 21.1, 21.3, 21.4, 21.5**

- [x] 22. Implement schema version and Heartbeat
  - [x] 22.1 Record the Schema_Version and implement `Storage.Heartbeat`
    - Write the Schema_Version after migrations succeed (unchanged on failure); insert `engine_status(ts, version)` at the configured interval (default 15 s) after migrations, log failures and retry at the next interval, delete rows older than 1 day
    - _Requirements: 41.1, 41.2, 41.3, 41.4_

  - [x]* 22.2 Write integration tests for Heartbeat and schema version
    - Heartbeat cadence, no Heartbeat before migrations succeed, schema version after a no-op start
    - _Requirements: 41.1, 41.2, 41.3_

- [x] 23. Implement feedback through the Control API
  - [~] 23.1 Implement `Autoangel.Feedback.apply/2` and per-Series effective threshold
    - `mute` for a Series or Incident; `this was normal` sets `min(current_or_base, base × 0.5)` with a floor of 0.25 for each affected Series
    - Write the State_Store first, then update memory, then reply; leave memory unchanged on store failure
    - Apply per-Series sensitivity in `Decision` via `effective_threshold = clamp(100 − (100 − T) × s, 1, 99.9)`
    - _Requirements: 30.1, 30.2, 30.6, 30.8, 43.3_

  - [x] 23.2 Implement `POST /api/v1/feedback`
    - Validate action and target, 400 naming the invalid field, 401 for missing or wrong credentials, 500 on store failure, 503 before boot completes; accept feedback from no other channel
    - _Requirements: 30.3, 30.4, 30.7, 30.8_

  - [x] 23.3 Honour mutes in the Dispatcher
    - No new alert for a muted Series and no further notification for a muted Incident, while detection continues
    - _Requirements: 30.1_

  - [x]* 23.4 Write property test for sensitivity
    - **Property 22: Sensitivity is monotone and follows precedence**
    - **Validates: Requirements 30.2, 31.1, 31.2, 31.3, 31.4**

  - [x]* 23.5 Write property test for dispatch decisions
    - **Property 28: Dispatch decisions match the policy model**
    - Note: 28.1/28.3 (grouping, P1) not yet covered; extend with task 36 grouping
    - **Validates: Requirements 18.9, 27.1, 27.2, 27.3, 27.5, 27.6, 27.7, 28.1, 28.3, 30.1**

  - [x]* 23.6 Write property test for feedback idempotence
    - **Property 33: Feedback is idempotent**
    - **Validates: Requirements 30.6**

  - [x]* 23.7 Write property test for invalid feedback
    - **Property 34: Invalid feedback changes nothing**
    - **Validates: Requirements 30.3**

  - [x]* 23.8 Write unit tests for feedback failure paths
    - State_Store write failure returns 500 with no state change, 503 before boot
    - _Requirements: 30.8_

- [x] 24. Implement the Replay_Harness
  - [x] 24.1 Implement replay input readers and anomaly injectors
    - CSV and Prometheus range-export JSON readers that report an error naming an unreadable or empty Series
    - Seeded injectors for spike, drop, level shift, silence and slow drift writing ground-truth labels
    - _Requirements: 77.1, 77.2, 77.3, 77.7_

  - [x] 24.2 Implement replay execution, matching and reporting
    - Start an Engine in-process with default config, a test-only `engine_plugin_capture` and the simulated clock; replay in timestamp order through the push API
    - Apply the design's matching rule and tolerance; report recall, precision and alerts per simulated day overall and per type with the config digest and seed
    - _Requirements: 77.4, 77.5, 77.6_

  - [x]* 24.3 Write property test for replay metrics
    - **Property 69: Replay metrics follow the matching rule**
    - **Validates: Requirements 77.4**

  - [x]* 24.4 Write property test for replay determinism
    - **Property 70: Replay is deterministic**
    - **Validates: Requirements 77.6**

  - [x]* 24.5 Write unit tests for readers and injectors
    - Unreadable file, empty Series, expected Sample changes and label per injector
    - _Requirements: 77.2, 77.3, 77.7_

- [x] 25. Checkpoint - M2 Explain complete
  - Ensure all tests pass, ask the user if questions arise.

  - [-] 25.1 Wire configured detection settings into the Workers
    - Found at the M2 checkpoint: `Autoangel.Application` starts the Workers with an empty `decision_cfg`, so configured `learning`, `persistence` and `missing_data` values never reach `Decision`; detection always uses the built-in defaults (which equal the Config defaults), and a replay's config digest can name settings that were not applied
    - Pass the detection sections of the loaded configuration to every Worker; test that a non-default value changes detection through the started application
    - _Requirements: 10.2, 10.3, 11.1, 14.1, 14.2, 77.5_

### M3: Console web

- [ ] 26. Set up the Console project, schema queries, startup checks and authentication
  - [~] 26.1 Create the Console Phoenix LiveView project
    - Separate Mix project and release with Bandit, a read-only `Console.Repo`, no Rust or Rustler, optional TLS, configuration validation for the Engine-not-reporting threshold (default 60 s) and other Console settings
    - Add a CI check that the Console tree, release and image contain no Rust source, Rustler dependency or NIF
    - _Requirements: 56.5, 56.6, 57.5, 65.1, 65.2, 65.3, 65.7_

  - [~] 26.2 Implement Ecto schemas and bounded query helpers in `shared/autoangel_schema`
    - Read-only `AutoangelSchema.Query.*` helpers requiring range, Series limit and row limit, clamped to the Console maximums (400 d, 50, 10,000) and returning which clamp applied; `AutoangelSchema.supported_versions/0`
    - _Requirements: 65.3, 65.4, 65.5, 65.6_

  - [~] 26.3 Implement Console startup checks
    - Before the Endpoint starts: database configured, connection succeeds, `schema_version` exists and has a value, value is supported; otherwise log the specific reason (including found and supported versions) and halt non-zero; read-only operations only
    - _Requirements: 58.1, 58.2, 58.3, 58.4, 58.5, 58.6_

  - [~] 26.4 Implement `Console.Auth`, the router and `/healthz`
    - Static basic credentials or bearer token for pages and LiveView sockets; deny with no metric data; health endpoint authenticated unless explicitly exempted, returning only `{"status":"ok"}`
    - _Requirements: 57.1, 57.2, 57.3, 57.4, 74.1, 74.6, 74.7, 76.1, 76.2_

  - [~]* 26.5 Write property test for Console query bounds
    - **Property 57: Console queries stay within bounds**
    - **Validates: Requirements 65.5, 65.6, 66.5**

  - [~]* 26.6 Adopt the shared schema library in the Engine and add the version check (P1)
    - Make `autoangel_tsdb` depend on `autoangel_schema` definitions, declare the Schema_Version in the library, and add a CI check that fails and reports both values when it differs from the newest migration
    - _Requirements: 66.1, 66.2, 66.3, 66.4, 66.5_

  - [~]* 26.7 Write unit tests for startup checks and authentication
    - Each startup failure case, denied requests return no data and trigger no Control_API action
    - _Requirements: 57.2, 57.4, 58.1, 58.3, 58.4_

- [ ] 27. Implement the Console views
  - [~] 27.1 Implement `AnomalyListLive`
    - Open Anomalies and those ended within the recent period (default 24 h), open first, newest first within groups; all fields with the score labelled "anomaly score"; stored candidates by rank with weak markers; empty, no-candidates and database-error states
    - _Requirements: 17.6, 17.7, 19.4, 19.6, 19.7, 51.1, 51.2, 51.3, 51.4, 51.5, 51.6, 51.7_

  - [~] 27.2 Implement `SeriesLive.Index`
    - List all Series with stored status; case-sensitive substring search over name, label names and label values via `strpos`; no-match and database-error states keeping the term
    - _Requirements: 52.1, 52.2, 52.3, 52.4, 52.5_

  - [~] 27.3 Implement chart data selection and `SeriesLive.Show`
    - Range selector (1 h to 13 months), tier selection raw / 1-minute / 1-hour by retention and the chart point limit (default 2,000), counter rates with reset handling, expected band from `baseline_bands` and its aggregate, Anomaly markers by interval overlap, no-data and error states
    - _Requirements: 53.1, 53.2, 53.3, 53.4, 53.5, 53.6, 53.7_

  - [~] 27.4 Implement `AnomalyLive.Show` at `/anomalies/:id`
    - All Anomaly fields, chart around the Anomaly with the same tier rules, stored candidates or the no-candidates statement, not-found and database-unreachable pages
    - _Requirements: 21.2, 21.3, 54.1, 54.2, 54.3, 54.4, 54.5, 54.6, 54.7_

  - [~] 27.5 Implement the Engine-not-reporting banner
    - Global component reading the latest Heartbeat on mount and every 10 s; show the time or "no Heartbeat recorded"; keep serving stored data
    - _Requirements: 56.1, 56.2, 56.3, 56.4, 65.9_

  - [~] 27.6 Implement `Console.EngineClient` and feedback buttons
    - "mute" and "this is normal" on Series views and detail pages, one Control_API request per press targeting the Series or the Incident, confirmation or "not applied" error, buttons disabled while `/readyz` polling fails and re-enabled after recovery, no database writes
    - _Requirements: 30.4, 55.1, 55.2, 55.3, 55.4, 55.5, 55.6, 55.7, 55.8, 65.10_

  - [~]* 27.7 Write property test for the anomaly list
    - **Property 53: Anomaly list selection and order**
    - **Validates: Requirements 51.1, 51.3**

  - [~]* 27.8 Write property test for Series search
    - **Property 54: Series search is case-sensitive substring matching**
    - **Validates: Requirements 52.1, 52.2, 52.3**

  - [~]* 27.9 Write property test for chart tier selection
    - **Property 55: Chart tier selection**
    - **Validates: Requirements 53.2, 53.3, 53.4**

  - [~]* 27.10 Write property test for the banner predicate
    - **Property 56: Engine-not-reporting banner predicate**
    - **Validates: Requirements 56.1, 56.2, 56.4**

  - [~]* 27.11 Write LiveView tests for empty, error and feedback states
    - Every empty, error and not-found state in Requirements 51 to 56; feedback flows against a fake Control_API
    - _Requirements: 51.6, 51.7, 52.4, 52.5, 53.6, 53.7, 54.5, 54.6, 55.5, 55.7, 55.8_

- [ ] 28. Implement the read-only role SQL
  - [~] 28.1 Add `docs/readonly-role.sql` and its integration test
    - Role creation with USAGE and SELECT on all tables plus default privileges; test that the role reads every object and is refused every write and DDL statement
    - _Requirements: 42.1, 42.2, 42.3_

- [ ] 29. Implement the fault-injection suite
  - [~] 29.1 Build the fault-injection environment and harness
    - Compose environment with the Engine release, TimescaleDB behind Toxiproxy, the Console, and fake SMTP and Telegram servers; harness that runs a reference and a fault run on the same replay fixtures with the simulated clock, compares delivered alerts and reports pass or fail per check
    - _Requirements: 73.1_

  - [~] 29.2 Implement storage and Console fault tests
    - TimescaleDB stopped and restarted, slow writes, Console stopped, Storage_Writer killed, with the checks from the design table
    - _Requirements: 65.8, 73.2, 73.3, 73.4, 73.5, 73.6_

  - [~] 29.3 Implement plugin, Worker and restart fault tests
    - One plugin failing, a Worker killed, an Engine restart on the same State_Store volume
    - _Requirements: 73.7, 73.8, 73.9_

- [~] 30. Checkpoint - M3 Console web complete
  - Ensure all tests pass, ask the user if questions arise.

### M4: Console MCP

- [ ] 31. Implement the MCP_Server
  - [~] 31.1 Implement the streamable HTTP transport at `/mcp`
    - Transport adapter (library or direct JSON-RPC, open question 8, with the choice recorded in `docs/decisions.md`) that only translates to `Console.MCP.Tools`; the Engine exposes no MCP endpoint
    - _Requirements: 59.1, 59.5_

  - [~] 31.2 Implement the `Console.MCP.Tools` core
    - Output budget (32 KiB) with whole items and a truncation flag only when items were left out; untrusted-value marking, escaping and 256-character truncation; parameter validation naming each invalid parameter; stable error codes with no partial data on store failure
    - _Requirements: 60.4, 60.6, 60.7, 63.4, 63.5, 63.6_

  - [~] 31.3 Implement `list_anomalies`, `explain_anomaly` and `query_series`
    - Filters by `since`, `min_score` and `filter`; explanation with stored candidates or the no-candidates note; `query_series` with the 500-point cap, coarser tier when needed and `resolution_used`; not-found errors
    - _Requirements: 17.6, 17.7, 19.4, 19.6, 19.7, 21.2, 21.3, 60.1, 60.2, 60.3, 60.5_

  - [~] 31.4 Implement write-tool gating
    - Tool list contains only read tools unless `mcp.write_tools = true`; calling a write tool while disabled returns "write tools are disabled" without calling the Control_API
    - _Requirements: 63.1, 63.2, 63.3_

  - [~] 31.5 Implement the stdio entry point
    - `autoangel-console mcp-stdio` escript forwarding bytes unchanged to `/mcp` with the MCP credential, writing a JSON-RPC error and continuing when the endpoint is unreachable
    - _Requirements: 59.2, 59.3, 59.4_

  - [~] 31.6 Implement `Console.MCP.Auth` with a separate MCP credential setting
    - Require an MCP credential on every `/mcp` request, read from settings separate from the web credentials; reject missing, malformed, wrong or web-only credentials with no data; reject MCP-only credentials on web routes
    - Required by the P0 security defaults; it also covers the P1 Requirement 64 criteria
    - _Requirements: 64.1, 64.2, 64.3, 74.1, 74.2, 74.7_

  - [~]* 31.7 Implement `list_series` (P1)
    - Series with stored status matching `filter` under the `list_anomalies` rules, output budget, parameter and store errors
    - _Requirements: 61.1, 61.2, 61.3, 61.4_

  - [~]* 31.8 Implement `give_feedback` (P1)
    - Registered only with write tools enabled; validate `id` and `verdict`; one Control_API request; success only after a success status; error results for client errors and an unavailable Control_API
    - _Requirements: 62.1, 62.2, 62.3, 62.4, 62.5, 65.10_

  - [~]* 31.9 Write property test for transport equivalence
    - **Property 58: stdio and HTTP MCP transports are equivalent**
    - **Validates: Requirements 59.2, 59.3**

  - [~]* 31.10 Write property test for MCP list filters
    - **Property 59: MCP list filters**
    - **Validates: Requirements 60.1, 61.1**

  - [~]* 31.11 Write property test for the query_series point cap
    - **Property 60: query_series respects the point cap**
    - **Validates: Requirements 60.3**

  - [~]* 31.12 Write property test for the MCP output budget
    - **Property 61: MCP output budget**
    - **Validates: Requirements 60.4, 61.2**

  - [~]* 31.13 Write property test for invalid MCP parameters
    - **Property 62: Invalid MCP parameters are named**
    - **Validates: Requirements 60.6, 61.3**

  - [~]* 31.14 Write property test for untrusted value marking
    - **Property 63: Untrusted values cannot escape their marking**
    - **Validates: Requirements 63.4, 63.5, 63.6**

  - [~]* 31.15 Write unit tests for MCP tools and transport
    - Tool list with write tools on and off, `give_feedback` against a fake Control_API per response class, stdio forwarder with the endpoint down, credential rotation after restart
    - _Requirements: 59.4, 62.4, 62.5, 63.1, 63.3, 64.4_

- [~] 32. Checkpoint - M4 Console MCP complete
  - Ensure all tests pass, ask the user if questions arise.

### M5: Smarter (P1)

- [ ] 33. Implement history reads and seasonality detection
  - [~]* 33.1 Implement `Autoangel.History.read/4` and background analysis tasks (P1)
    - Clamp range (default 15 d) and Series count (default 200) before calling the backend; run reads as `Task`s under `Autoangel.Background.Supervisor`, never in Workers; log warnings naming Series and analysis on failure; issue no reads in Topology_A
    - Seed the pair co-occurrence table from stored anomalies at startup (deep correlation)
    - _Requirements: 37.1, 37.2, 37.3, 37.4, 37.5, 37.6_

  - [~]* 33.2 Write property test for history read bounds
    - **Property 41: History reads stay within bounds**
    - **Validates: Requirements 37.3**

  - [~]* 33.3 Implement seasonality detection and per-phase Baselines (P1)
    - Every 6 h, test daily and weekly periods with at least 2 cycles of history using ACF ≥ 0.3 on 1-hour means; install 24 or 168 phase cells seeded from aggregates; score with Baseline_Type per-phase seasonal and the period; fall back to robust when no period is significant; keep the Baseline on read failure; end learning by seasonal cycles
    - Validate the significance threshold setting
    - _Requirements: 10.3, 12.1, 12.2, 12.3, 12.4, 12.5, 12.6, 12.7, 35.2_

  - [~]* 33.4 Write unit tests for seasonality classification
    - Pure daily sine, weekly pattern, noise only, insufficient history
    - _Requirements: 12.2, 12.5, 12.6_

- [ ] 34. Implement warm start from history
  - [~]* 34.1 Implement the warming flow in the Worker (P1)
    - Mark new Series without checkpoint as warming, buffer live Samples, read history with a 500 ms timeout, fold stored Samples de-duplicated against the buffer, evaluate learning with the earliest stored timestamp, apply the buffer, raise no alerts for stored Samples; start fresh on failure or timeout
    - _Requirements: 44.1, 44.2, 44.3, 44.4, 44.5_

  - [~]* 34.2 Write property test for warm start
    - **Property 44: Warm start applies each Sample once**
    - **Validates: Requirements 44.1, 44.2, 44.3, 44.5**

- [ ] 35. Implement level shift, drift and direction awareness
  - [~]* 35.1 Implement level-shift detection (P1)
    - CUSUM on robust-standardised values in the kernel with the minimum Samples setting (default 10, validated positive); one level-shift Anomaly per changepoint; close an open value Anomaly into it; rebuild the window and reset the long-term reference from post-changepoint Samples
    - _Requirements: 13.1, 13.2, 13.3, 13.4, 13.5_

  - [~]* 35.2 Write property test for level shifts
    - **Property 20: Level shift happens once and then becomes normal**
    - **Validates: Requirements 13.1, 13.2, 13.3, 13.4**

  - [~]* 35.3 Implement the long-term reference Baseline and drift (P1)
    - Quantile-sketch long-term window (default 4,096 Samples) updated from scored values; divergence in long-term scaled-MAD units with threshold 3.0; one drift Anomaly outside learning, resolved at or below the threshold; validate threshold and window length
    - _Requirements: 16.1, 16.2, 16.3, 16.4, 16.5_

  - [~]* 35.4 Write property test for drift
    - **Property 21: Drift state machine**
    - **Validates: Requirements 16.2, 16.3, 16.4**

  - [~]* 35.5 Implement direction awareness (P1)
    - Infer `:high` for floor-bound Series and `:both` otherwise, treat both as anomalous until inferred, apply validated per-Series overrides, ignore non-anomalous directions for runs, never filter missing-data Anomalies
    - _Requirements: 15.1, 15.2, 15.3, 15.4, 15.5, 15.6, 15.7_

  - [~]* 35.6 Write property test for direction filtering
    - **Property 18: Direction filtering**
    - **Validates: Requirements 15.3, 15.4, 15.5**

- [ ] 36. Implement sensitivity overrides, event-like correlation, grouping and resolutions
  - [~]* 36.1 Implement global and configured per-Series sensitivity overrides (P1)
    - Precedence per-Series, then global, then default; valid range 0.25 to 4.0 validated at startup; restore overrides from the State_Store before the first score
    - _Requirements: 31.1, 31.2, 31.3, 31.4, 31.5, 31.6_

  - [~]* 36.2 Implement event-like classification (P1)
    - Classify in Worker meta (Typical_Interval ≥ 10 min or spacing coefficient of variation > 1.0), pass the flag to the Correlator and describe event-like evidence as window co-occurrence
    - _Requirements: 20.1, 20.2, 20.3, 20.4_

  - [~]* 36.3 Implement Incident grouping (P1)
    - Join the closest qualifying open Incident within the window without a separate alert; keep the Incident open until the last member resolves; render member Series in notifications
    - _Requirements: 28.1, 28.2, 28.3, 28.4_

  - [~]* 36.4 Implement resolution notifications (P1)
    - On close, send one resolution per plugin whose original delivery succeeded, with resolved indication, members, start and end times and the Console link when configured; same timeout, retry and breaker handling
    - _Requirements: 29.1, 29.2, 29.3, 29.4_

  - [~]* 36.5 Write property test for resolution routing
    - **Property 29: Resolution notifications go only where the alert arrived**
    - **Validates: Requirements 29.1**

  - [~]* 36.6 Write unit tests for grouping and sensitivity configuration
    - Closest-Incident tie breaks, invalid sensitivity values, restore order
    - _Requirements: 28.1, 31.5, 31.6_

- [ ] 37. Implement remaining P1 Engine features
  - [~]* 37.1 Implement JSON ad-hoc ingestion at `POST /api/v1/push/json` (P1)
    - Validate one JSON object with name, numeric value, optional integer timestamp and string labels; error bodies `missing_field`, `invalid_field` and `not_a_json_object`; same processing path as pushes
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_

  - [~]* 37.2 Write property test for JSON ingestion
    - **Property 4: JSON ingestion validation**
    - **Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5**

  - [~]* 37.3 Implement Telegram feedback buttons (P1)
    - `inline_keyboard` "mute" and "this was normal" buttons per alert targeting its Incident; long-poll `getUpdates` and pass callbacks to `Autoangel.Feedback.apply/2`
    - _Requirements: 30.5, 30.9_

  - [~]* 37.4 Implement `GET /api/v1/incidents` and `GET /api/v1/series` (P1)
    - Read-only listings from Engine state, empty lists when nothing is held, Control_API auth
    - _Requirements: 46.1, 46.2, 46.3, 46.4, 46.5_

  - [~]* 37.5 Write property test for listings
    - **Property 45: Listings reflect Engine state and change nothing**
    - **Validates: Requirements 46.1, 46.2, 46.3, 46.5**

  - [~]* 37.6 Implement the compression policy (P1)
    - Ensure exactly one compression policy with the configured age (default 1 d, validated) after migrations, replace on change, log and continue uncompressed on rejection; integration test that compressed reads equal uncompressed reads
    - _Requirements: 40.1, 40.2, 40.3, 40.4, 40.5, 40.6_

- [~] 38. Checkpoint - M5 Smarter complete
  - Ensure all tests pass, ask the user if questions arise.

### M6: Polish

- [ ] 39. Implement additional plugins
  - [~]* 39.1 Implement webhook, ntfy, Slack and Discord plugin apps (P1)
    - One Mix app each depending on `autoangel_core`, implementing every Plugin_Behaviour callback, validating destination fields and classifying failures as retryable or permanent
    - _Requirements: 24.1, 24.2, 24.3, 24.4_

  - [~]* 39.2 Write unit tests for the additional plugins
    - Validation failures and reply classification against fake servers
    - _Requirements: 24.3, 24.4_

- [ ] 40. Implement CLI output measurement
  - [~]* 40.1 Implement `--measure-output` (P1)
    - Pipe stdout through unchanged while counting bytes and hashing with SHA-256; push `autoangel_job_output_bytes` and the 48-bit hash with the run timestamp, also on non-zero exit; omit both when disabled
    - _Requirements: 69.1, 69.2, 69.3, 69.4, 69.5, 69.6_

  - [~]* 40.2 Write property test for output size and hash
    - **Property 65: Output size and hash**
    - **Validates: Requirements 69.1, 69.2**

- [ ] 41. Implement packaging and CI builds
  - [~] 41.1 Configure Engine and Console releases and container images
    - Engine release with compiled NIFs and the build-time plugin set fixed in `rel/`; separate Console release; Dockerfiles for `linux/amd64` and `linux/arm64`
    - _Requirements: 22.2, 65.1, 72.5, 76.3, 76.4_

  - [~]* 41.2 Implement the multi-architecture CI pipeline (P1)
    - Native amd64 and arm64 jobs building the NIF, Engine release and image from the same revision, packaging the matching NIF, labelling artifacts `rev-arch`, and a publish job that needs every build job
    - _Requirements: 50.1, 50.2, 50.3, 50.4, 50.5_

  - [~] 41.3 Implement startup smoke tests and the macOS development build job
    - Per-architecture smoke tests that start the Engine to readiness and the Console to health; a macOS CI job that builds the Engine, Console and CLI and runs a CLI push against the Engine
    - _Requirements: 76.5, 76.6, 76.7_

- [ ] 42. Implement privacy and scale verification
  - [~] 42.1 Implement the privacy egress integration test
    - Run the Engine, Console and CLI in a network namespace with logged egress and assert connections only to configured destinations and no telemetry without an explicit opt-in per deliverable
    - _Requirements: 75.1, 75.2, 75.3, 75.4, 75.5_

  - [~] 42.2 Implement the scale and footprint benchmark suite
    - 20,000-Series load at 10 s to 60 s intervals recording machine details, topology, duration, detection latency histogram, queue depth, maximum NIF call duration, memory per 1,000 Series and disk per Series-day with compression on and off into `docs/decisions.md`; fail the run on latency above 1 s or queue overflow
    - _Requirements: 71.1, 71.2, 71.3, 71.4, 72.1, 72.2, 72.3_

- [ ] 43. Write the operator documentation
  - [~] 43.1 Write the Engine documentation
    - Running without TLS behind a reverse proxy, read-only role usage, pg_dump and physical backup and restore, data that is safe to lose, State_Store location outside the database, and footprint estimates with benchmark conditions and Engine version
    - _Requirements: 7.7, 42.1, 42.4, 42.5, 42.6, 42.7, 72.1, 72.2, 72.3, 72.4_

  - [~] 43.2 Write the Console and CLI documentation
    - TLS termination at a reverse proxy for the Console; CLI run timestamp definition, retry settings and textfile behaviour
    - _Requirements: 57.6, 68.2, 68.6_

- [~] 44. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional. They are either test sub-tasks or P1 features (Requirements 3, 12, 13, 15, 16, 20, 24, 28, 29, 31, 37, 40, 44, 46, 50, 61, 62, 66, 69, and criteria 30.5 and 30.9).
- Requirement 64 is P1, but the P0 security defaults (Requirements 74.1, 74.2, 74.7) need a separate MCP credential, so task 31.6 is required.
- Tests that are themselves the deliverable of a P0 requirement (NIF-boundary tests in 4.6, the Replay_Harness in task 24, the fault-injection suite in task 29, the privacy test in 42.1 and the benchmark suite in 42.2) are required. Requirement 49.2 asks for Rust property tests when a kernel is promoted; tasks 4.3 to 4.5 cover this.
- Task 4.2 depends on the NIF gate result in 3.3. If no kernel is promoted, the Elixir kernels stay selected.
- Each property test carries the tag `Feature: v1, Property N: <title>` and runs at least 100 cases. Database-backed properties (26, 39, 40, 53, 54, 59) run against a TimescaleDB container.
- Checkpoints follow each milestone.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["1.2", "2.1", "3.1", "6.1", "7.1", "17.1"] },
    { "id": 2, "tasks": ["2.2", "2.3", "3.2", "6.2", "7.2", "8.2", "17.2"] },
    { "id": 3, "tasks": ["2.4", "2.5", "2.6", "2.7", "2.8", "2.9", "2.10", "2.11", "3.3", "7.3", "8.1", "17.3"] },
    { "id": 4, "tasks": ["4.1", "8.3", "8.4", "8.5", "9.1", "17.4"] },
    { "id": 5, "tasks": ["4.2", "9.2", "11.1", "11.2", "17.5"] },
    { "id": 6, "tasks": ["4.3", "4.4", "4.5", "4.6", "9.3", "9.4", "9.5", "9.6", "9.7", "10.1", "11.3", "13.1", "17.6", "17.7", "17.8", "17.9", "17.10"] },
    { "id": 7, "tasks": ["10.2", "10.3", "10.4", "11.4", "12.1", "13.2", "14.1"] },
    { "id": 8, "tasks": ["11.5", "11.6", "11.7", "11.8", "11.9", "12.2", "13.3", "13.5", "13.6", "14.2"] },
    { "id": 9, "tasks": ["13.4", "13.7", "13.8", "14.3", "14.4", "14.5", "14.8", "14.9"] },
    { "id": 10, "tasks": ["13.9", "14.6", "14.7", "14.10", "15.1"] },
    { "id": 11, "tasks": ["15.2", "15.3", "16.1"] },
    { "id": 12, "tasks": ["16.2"] },
    { "id": 13, "tasks": ["16.3", "16.4"] },
    { "id": 14, "tasks": ["19.1", "22.1", "23.1", "24.1"] },
    { "id": 15, "tasks": ["19.2", "19.3", "19.4", "20.1", "21.1", "22.2", "23.2", "24.2"] },
    { "id": 16, "tasks": ["20.2", "20.3", "21.2", "23.3", "24.3", "24.4", "24.5"] },
    { "id": 17, "tasks": ["23.4", "23.5", "23.6", "23.7", "23.8"] },
    { "id": 18, "tasks": ["26.1"] },
    { "id": 19, "tasks": ["26.2", "26.4", "28.1"] },
    { "id": 20, "tasks": ["26.3", "26.5", "26.6", "26.7", "27.1"] },
    { "id": 21, "tasks": ["27.2", "27.7"] },
    { "id": 22, "tasks": ["27.3", "27.8"] },
    { "id": 23, "tasks": ["27.4", "27.9"] },
    { "id": 24, "tasks": ["27.5", "27.6"] },
    { "id": 25, "tasks": ["27.10", "27.11", "29.1"] },
    { "id": 26, "tasks": ["29.2"] },
    { "id": 27, "tasks": ["29.3"] },
    { "id": 28, "tasks": ["31.1", "31.2"] },
    { "id": 29, "tasks": ["31.3", "31.5", "31.6"] },
    { "id": 30, "tasks": ["31.4", "31.10", "31.11", "31.12", "31.14"] },
    { "id": 31, "tasks": ["31.7", "31.9", "31.13"] },
    { "id": 32, "tasks": ["31.8"] },
    { "id": 33, "tasks": ["31.15"] },
    { "id": 34, "tasks": ["33.1", "35.1", "36.3", "37.1", "37.4", "37.6"] },
    { "id": 35, "tasks": ["33.2", "33.3", "35.2", "36.4", "37.2", "37.3", "37.5"] },
    { "id": 36, "tasks": ["33.4", "34.1", "35.3", "36.5"] },
    { "id": 37, "tasks": ["34.2", "35.4", "35.5"] },
    { "id": 38, "tasks": ["35.6", "36.1"] },
    { "id": 39, "tasks": ["36.2", "36.6"] },
    { "id": 40, "tasks": ["39.1", "40.1", "41.1", "42.1"] },
    { "id": 41, "tasks": ["39.2", "40.2", "41.2", "41.3", "42.2"] },
    { "id": 42, "tasks": ["43.1", "43.2"] }
  ]
}
```
