# Running the Engine

This page is for the person who runs the Engine. It covers the settings file, TLS,
the read-only database role, backups, what is safe to lose, where the State_Store
lives, and how much disk and memory to plan for.

The numbers in "Footprint" were measured with Engine version **0.1.0**. Their
conditions are listed next to them. Measure again on your own hardware before you
size a machine from them.

## Start it

The Engine is a release (`engine/mix.exs`, built by `mix release autoangel`) and an
image (`docker/engine.Dockerfile`). It reads one TOML file, named by `AUTOANGEL_CONFIG`.

```sh
AUTOANGEL_CONFIG=/etc/autoangel/engine.toml \
AUTOANGEL_STATE_PATH=/var/lib/autoangel/state \
  /opt/autoangel/bin/autoangel start
```

The smallest working file has the two credentials. They must differ:

```toml
[ingest]
token = "..."          # for pushes (CLI, scrapes of your own code)
[control_api]
token = "..."          # for /readyz, /metrics, feedback, the Console
[http]
ingest_port = 9400
control_port = 9401
```

An invalid file stops the start. The log names every wrong setting, one line each,
and never prints a secret.

Secrets can come from the environment instead of the file, so they stay out of the
file: `AUTOANGEL_INGEST_TOKEN`, `AUTOANGEL_INGEST_PASSWORD`,
`AUTOANGEL_CONTROL_API_TOKEN`, `AUTOANGEL_CONTROL_API_PASSWORD` and
`AUTOANGEL_STORAGE_PASSWORD`. An environment value wins over the file.

### Settings and their defaults

| Section | Setting | Default |
|---|---|---|
| `[learning]` | `duration` (seconds), `cycles` | 86400, 2 |
| `[missing_data]` | `multiple`, `sweep_interval` (s) | 3.0, 15 |
| `[persistence]` | `n` Samples, `m` minutes | 3, 5 |
| `[correlation]` | `window` (s), `candidate_pool_limit`, `support_threshold` | 600, 20, 2 |
| `[alerts]` | `max_candidates`, `console_base_url` | 5, none |
| `[cardinality]` | `per_metric`, `total` | 2000, 50000 |
| `[ingest_rate_limit]` | `rate` per second, `burst` | 50, 200 |
| `[retention]` | `raw`, `aggregate`, `anomaly` (s) | 14 d, 390 d, 90 d |
| `[compression]` | `age` (s) | 86400 |
| `[seasonality]` | `threshold` (0 to 1) | 0.3 |
| `[level_shift]` | `min_samples` | 10 |
| `[drift]` | `threshold`, `window` | 3.0, 4096 |
| top level | `sensitivity` (0.25 to 4.0), `heartbeat_interval` (s) | 1.0, 15 |
| `[storage]` | `backend` (`none` or `timescaledb`), `connection` | `none` |
| `[state_store]` | `path` | see "State_Store" |

Per-Series settings go in `[[series_overrides]]` with a metric `name`, and
optionally `sensitivity` and `direction` (`high`, `low` or `both`).

With `backend = "none"` the Engine runs without a database (Topology_A). Detection
and alerts work. Seasonality, warm start from history and deep correlation need a
database, and the Console has nothing to read.

## TLS: terminate it at a reverse proxy

The Engine's two listeners speak plain HTTP unless you give them a certificate
(`[tls] cert_file` and `key_file`, both or neither). The advised set-up is plain HTTP
behind a reverse proxy that holds the certificate:

```nginx
server {
  listen 443 ssl;
  server_name autoangel.example.com;
  ssl_certificate     /etc/ssl/autoangel.crt;
  ssl_certificate_key /etc/ssl/autoangel.key;

  location /api/v1/push { proxy_pass http://127.0.0.1:9400; client_max_body_size 10m; }
}
# The Control_API (port 9401) is for the Console and for operators: keep it on a
# private network and do not publish it.
```

Without TLS on a network you do not control, every token crosses it in clear text.
The listeners bind to all addresses, so close the two ports at the firewall or the
container network, and publish only what the proxy needs.

## The read-only database role

The Console reads the database with a role that cannot write. `docs/readonly-role.sql`
creates the group role `autoangel_readonly`: it can read every table, view and
continuous aggregate in `public`, now and later, and cannot change anything.

```sh
psql "$ADMIN_URL" -f docs/readonly-role.sql
psql "$ADMIN_URL" -c "CREATE ROLE console_ro LOGIN PASSWORD '...' IN ROLE autoangel_readonly"
```

Run it after the Engine has migrated once. Put the `console_ro` login in
`AUTOANGEL_CONSOLE_DATABASE_URL`. The test `engine/apps/autoangel_tsdb/test/readonly_role_integration_test.exs` checks
that it reads every object and is refused every write and every change of schema.

## Backup and restore

Two ways, and they suit different needs.

**Logical, with `pg_dump`.** Small and portable. Use it for the data you must keep.

```sh
pg_dump --format=custom --file=autoangel.dump "$DATABASE_URL"
```

To restore into an empty database that has the TimescaleDB extension:

```sh
psql "$NEW_URL" -c "CREATE EXTENSION IF NOT EXISTS timescaledb"
psql "$NEW_URL" -c "SELECT timescaledb_pre_restore()"
pg_restore --dbname="$NEW_URL" --no-owner autoangel.dump
psql "$NEW_URL" -c "SELECT timescaledb_post_restore()"
```

Stop the Engine while you restore. Start it afterwards; it checks the schema version
and migrates what is missing.

**Physical, with `pg_basebackup`** (or a volume snapshot taken while PostgreSQL is
stopped). It is fast for a large database and restores everything, including the
policies. The server version and the TimescaleDB version must match on restore.

```sh
pg_basebackup --pgdata=/backup/autoangel --format=tar --gzip --wal-method=stream
```

Test a restore before you need one.

## What is safe to lose

| Data | If lost |
|---|---|
| `engine_status` (the Heartbeat) | Nothing. It is rewritten every 15 s and pruned after a day. |
| Continuous aggregates (`samples_1m`, `samples_1h`, `baseline_bands_1h`) | They rebuild from raw Samples. |
| `baseline_bands` | The Console shows no expected band until new ones are written. |
| Raw Samples older than you care about | Charts and warm start have less history. Retention deletes them on purpose. |
| The State_Store | Each Series learns again, and mutes, sensitivity overrides and open Incidents are forgotten. With a database the Engine warms from stored history. |
| `anomalies` and `anomaly_candidates` | **Not safe.** They are the record of what happened. Back them up. |
| `series` | **Not safe.** Everything else points at it. |

## State_Store

The State_Store is a small embedded database that holds checkpoints (one per minute),
mutes, sensitivity overrides and open Incidents. It is **not** in PostgreSQL.

- Its folder is `[state_store] path`, or `AUTOANGEL_STATE_PATH`, or `data/state` next
  to the working folder. The image uses `/var/lib/autoangel/state`, a volume.
- Keep it on local disk that survives a restart. Do not put it on the database volume
  and do not share it between two Engines.
- A copy of the folder taken while the Engine runs is consistent as of the last
  checkpoint. A restart on the same folder resumes where it stopped.

## Footprint

Measured on 14 November 2026 with Engine 0.1.0, kernel in Elixir (no Rust kernel is
promoted). The full records are in `docs/decisions.md`, under "Scale and footprint".

| What | Result | Conditions |
|---|---|---|
| Load handled | 20,000 Series, one Sample each every 10 s (2,000 per second) | Apple silicon, 14 cores, 48 GiB, no database, Samples over HTTP |
| Same, every 60 s | 20,000 Series | same machine |
| Detection latency | at least 96.8% within 100 ms, largest 106 ms | both loads, 160,000 Samples |
| Deepest Worker queue | 0 Samples at every sample of 250 ms | both loads |
| Longest scoring call | 5.3 ms | both loads |
| Memory | about 11.7 MiB per 1,000 Series (about 230 MiB at 20,000) | Erlang VM total, after the load |
| Disk, no compression | about 196 KiB per Series-day | TimescaleDB 2.17, one Sample per 60 s, index included |
| Disk, compression on | about 11.6 KiB per Series-day (17 times smaller) | same, after the compression policy ran |

Disk grows with the Sample rate: a Series that reports every 10 s needs about six times
more than one that reports every 60 s. Add retention: 14 days of raw data for 20,000
Series at 60 s is about 20,000 x 14 x 196 KiB = 55 GB before compression and about
3 GB after. These are estimates from one synthetic series shape. Real metrics compress
differently.
