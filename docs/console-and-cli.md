# The Console and the CLI

## The Console

The Console is a separate release (`console/`, built by `mix release autoangel_console`,
image `docker/console.Dockerfile`). It reads the database with a read-only role
(`docs/readonly-role.sql`) and talks to the Engine only through the Control_API, for
feedback. It never writes to the database.

Every setting is an environment variable that starts with `AUTOANGEL_CONSOLE_`. The
full list, with defaults, is the table at the top of `console/lib/console/config.ex`.
The ones you always set:

| Variable | Meaning |
|---|---|
| `AUTOANGEL_CONSOLE_DATABASE_URL` | the read-only database login |
| `AUTOANGEL_CONSOLE_TOKEN`, or `_USER` with `_PASSWORD` | the web credential |
| `AUTOANGEL_CONSOLE_SECRET_KEY_BASE` | signs the session cookie, at least 64 bytes |
| `AUTOANGEL_CONSOLE_ENGINE_URL` and `_ENGINE_TOKEN` | the Control_API, for the feedback buttons |
| `AUTOANGEL_CONSOLE_MCP_TOKEN` | the credential for `/mcp`; it must differ from the web credential |

The Console refuses to start without a credential, a long secret and a database it
can read. It logs the reason and stops.

### TLS: terminate it at a reverse proxy

The Console listens on `127.0.0.1:4000` unless you change `_BIND` and `_PORT`, and it
speaks plain HTTP. Put a reverse proxy in front of it and let the proxy hold the
certificate. Two things must pass through, or the pages load but go dead:

- the `Host` header, unchanged (the live socket checks that its origin matches the page);
- the WebSocket upgrade on `/live`.

```nginx
server {
  listen 443 ssl;
  server_name console.example.com;
  ssl_certificate     /etc/ssl/console.crt;
  ssl_certificate_key /etc/ssl/console.key;

  location / {
    proxy_pass http://127.0.0.1:4000;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_read_timeout 3600s;
  }
}
```

If you cannot run a proxy, `AUTOANGEL_CONSOLE_TLS_CERT_FILE` and `_TLS_KEY_FILE` (both
or neither) give the Console its own certificate.

`/healthz` answers `{"status":"ok"}`. It needs the credential unless
`AUTOANGEL_CONSOLE_HEALTH_EXEMPT=true`.

### MCP

`POST /mcp` serves MCP tools (`list_anomalies`, `explain_anomaly`, `query_series`,
`list_series`, and `give_feedback` with `AUTOANGEL_CONSOLE_MCP_WRITE_TOOLS=true`). Send
`Authorization: Bearer <MCP token>`. For a client that speaks stdio, run
`autoangel-console mcp-stdio` with `AUTOANGEL_CONSOLE_MCP_URL` and
`AUTOANGEL_CONSOLE_MCP_TOKEN` set; it forwards each line to `/mcp`.

## The CLI

`autoangel run` runs a command, then pushes what it measured. `autoangel push` pushes
one Sample.

```sh
autoangel run --name nightly_backup --measure-resources --measure-output -- ./backup.sh
autoangel push queue_depth 42 --label queue=email
```

The command's own output, error output and exit code pass through unchanged. The CLI
exits with the command's exit code, whether or not the push worked.

### Settings

| Flag | Variable | Default |
|---|---|---|
| `--ingest-endpoint` | `AUTOANGEL_INGEST_ENDPOINT` | none (required) |
| `--bearer-token`, or `--basic-username` with `--basic-password` | `AUTOANGEL_BEARER_TOKEN`, `AUTOANGEL_BASIC_USERNAME`, `AUTOANGEL_BASIC_PASSWORD` | none (one is required) |
| `--attempt-timeout-secs` | `AUTOANGEL_ATTEMPT_TIMEOUT_SECS` | 10 |
| `--retry-duration-secs` | `AUTOANGEL_RETRY_DURATION_SECS` | 60 |
| `--textfile-path` (run only) | `AUTOANGEL_TEXTFILE_PATH` | none |

### What `run` pushes

Gauges labelled `job="<name>"`: `autoangel_job_duration_seconds`,
`autoangel_job_exit_code`, `autoangel_job_success`, and, when asked,
`autoangel_job_cpu_seconds` and `autoangel_job_max_rss_bytes` (`--measure-resources`),
and `autoangel_job_output_bytes` and `autoangel_job_output_hash` (`--measure-output`).

**The run timestamp** is the moment the command was started, in milliseconds since
1970 (UTC), read from the clock just before the process is spawned. It is not the
moment the command ended and not the moment of the push. Every value of one run
carries it, so a retried push still lands at the time the job began. The duration is
measured on a monotonic clock, so a clock change during the run does not distort it.

The output hash is the first 48 bits of the SHA-256 of everything the command wrote to
standard output, so it fits in a metric value without rounding. Use it to see that a
job's output changed, not as a security check.

### Retries

A failed push is retried until it works or until `--retry-duration-secs` has passed
since the first attempt. Each attempt may take at most `--attempt-timeout-secs`. The
wait between attempts starts at 0.5 s and doubles up to 10 s, and no attempt starts
after the window has closed. When the window closes, the CLI writes one line to
standard error (`push failed; measurements were dropped`) and drops the numbers. It
still exits with the command's exit code.

### Textfile output

With `--textfile-path /var/lib/node_exporter/textfile/job.prom` the CLI also writes the
same values for node_exporter's textfile collector. The file is written to a temporary
name in the same folder and moved into place, so the collector never reads half a file,
and it replaces the earlier file. It has no timestamps (the collector stamps the
scrape). The textfile is written **before** the push. If it cannot be written, the CLI
reports the path on standard error and still pushes.
