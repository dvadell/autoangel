use std::collections::HashSet;
use std::ffi::OsString;
use std::fmt;
use std::process::{Command as ProcessCommand, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::{Args, Parser, Subcommand};

/// Validate a Prometheus metric name according to the Engine exposition grammar.
pub fn is_valid_metric_name(name: &str) -> bool {
    is_name(name, true)
}

/// Validate a Prometheus label name according to the Engine exposition grammar.
pub fn is_valid_label_name(name: &str) -> bool {
    is_name(name, false)
}

fn is_name(name: &str, allow_colon: bool) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first == '_' || first.is_ascii_alphabetic() || (allow_colon && first == ':')) {
        return false;
    }
    chars.all(|character| {
        character == '_' || character.is_ascii_alphanumeric() || (allow_colon && character == ':')
    })
}

/// Validate a Prometheus text exposition sample value.
pub fn parse_sample_value(value: &str) -> Result<f64, String> {
    match value {
        "NaN" => Ok(f64::NAN),
        "+Inf" => Ok(f64::INFINITY),
        "-Inf" => Ok(f64::NEG_INFINITY),
        _ if is_decimal_literal(value) => value
            .parse::<f64>()
            .ok()
            .filter(|number| number.is_finite())
            .ok_or_else(|| format!("invalid sample value `{value}`")),
        _ => Err(format!("invalid sample value `{value}`")),
    }
}

fn is_decimal_literal(value: &str) -> bool {
    let value = value.strip_prefix(['+', '-']).unwrap_or(value);
    let (mantissa, exponent) = value.split_once(['e', 'E']).unwrap_or((value, ""));
    if exponent.is_empty() && (value.contains('e') || value.contains('E')) {
        return false;
    }
    let exponent = exponent.strip_prefix(['+', '-']).unwrap_or(exponent);
    if !exponent.is_empty() && !exponent.chars().all(|character| character.is_ascii_digit()) {
        return false;
    }
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    !whole.is_empty()
        && whole.chars().all(|character| character.is_ascii_digit())
        && (fraction.is_empty() || fraction.chars().all(|character| character.is_ascii_digit()))
        && (!mantissa.contains('.') || !fraction.is_empty())
}

#[derive(Clone, PartialEq, Eq)]
pub enum Credential {
    Bearer(String),
    Basic { username: String, password: String },
}

#[derive(Clone, PartialEq, Eq)]
pub struct Config {
    pub ingest_endpoint: String,
    pub credential: Credential,
    pub attempt_timeout: Duration,
    pub retry_duration: Duration,
}

impl Config {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.ingest_endpoint.trim().is_empty() {
            return Err(ConfigError::Missing("ingest endpoint"));
        }
        if self.attempt_timeout.is_zero() {
            return Err(ConfigError::NonPositive("attempt timeout"));
        }
        if self.retry_duration.is_zero() {
            return Err(ConfigError::NonPositive("retry duration"));
        }
        match &self.credential {
            Credential::Bearer(token) if token.is_empty() => {
                Err(ConfigError::Missing("bearer token"))
            }
            Credential::Basic { username, password } if username.is_empty() => {
                Err(ConfigError::Missing("basic username"))
            }
            Credential::Basic {
                username: _,
                password,
            } if password.is_empty() => Err(ConfigError::Missing("basic password")),
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    Missing(&'static str),
    NonPositive(&'static str),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing(setting) => write!(f, "missing {setting}"),
            Self::NonPositive(setting) => write!(f, "{setting} must be positive"),
        }
    }
}

impl std::error::Error for ConfigError {}

#[derive(Clone, Parser)]
#[command(name = "autoangel", about = "Run jobs and push metrics to Autoangel")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Clone, Subcommand)]
pub enum Command {
    Run(RunArgs),
    Push(PushArgs),
}

#[derive(Clone, Args)]
pub struct CommonArgs {
    /// Engine ingest endpoint, for example http://localhost:9400/api/v1/push.
    #[arg(
        long,
        env = "AUTOANGEL_INGEST_ENDPOINT",
        global = true,
        hide_env_values = true
    )]
    pub ingest_endpoint: Option<String>,
    /// Bearer token used to authenticate with the Engine.
    #[arg(long, env = "AUTOANGEL_BEARER_TOKEN", global = true, hide_env_values = true, conflicts_with_all = ["basic_username", "basic_password"])]
    pub bearer_token: Option<String>,
    /// Basic-auth username used to authenticate with the Engine.
    #[arg(
        long,
        env = "AUTOANGEL_BASIC_USERNAME",
        hide_env_values = true,
        requires = "basic_password",
        conflicts_with = "bearer_token"
    )]
    pub basic_username: Option<String>,
    /// Basic-auth password used to authenticate with the Engine.
    #[arg(
        long,
        env = "AUTOANGEL_BASIC_PASSWORD",
        hide_env_values = true,
        requires = "basic_username",
        conflicts_with = "bearer_token"
    )]
    pub basic_password: Option<String>,
    /// Maximum time allowed for one push attempt, in seconds.
    #[arg(long, env = "AUTOANGEL_ATTEMPT_TIMEOUT_SECS", default_value_t = 10, global = true, value_parser = positive_seconds)]
    pub attempt_timeout_secs: u64,
    /// How long to retry a failed push, in seconds.
    #[arg(long, env = "AUTOANGEL_RETRY_DURATION_SECS", default_value_t = 60, global = true, value_parser = positive_seconds)]
    pub retry_duration_secs: u64,
}

impl CommonArgs {
    pub fn config(&self) -> Result<Config, ConfigError> {
        let credential = match (
            &self.bearer_token,
            &self.basic_username,
            &self.basic_password,
        ) {
            (Some(token), None, None) => Credential::Bearer(token.clone()),
            (None, Some(username), Some(password)) => Credential::Basic {
                username: username.clone(),
                password: password.clone(),
            },
            _ => return Err(ConfigError::Missing("ingest credentials")),
        };
        let config = Config {
            ingest_endpoint: self.ingest_endpoint.clone().unwrap_or_default(),
            credential,
            attempt_timeout: Duration::from_secs(self.attempt_timeout_secs),
            retry_duration: Duration::from_secs(self.retry_duration_secs),
        };
        config.validate().map(|()| config)
    }
}

#[derive(Clone, Args)]
pub struct RunArgs {
    #[command(flatten)]
    pub common: CommonArgs,
    #[arg(long)]
    pub name: String,
    /// Optional node_exporter textfile destination.
    #[arg(long, env = "AUTOANGEL_TEXTFILE_PATH", hide_env_values = true)]
    pub textfile_path: Option<std::path::PathBuf>,
    /// Measure CPU time and maximum resident memory with wait4.
    #[arg(long)]
    pub measure_resources: bool,
    /// Command and arguments, passed unchanged after `--`.
    #[arg(last = true, required = true, value_parser = clap::value_parser!(OsString))]
    pub command: Vec<OsString>,
}

#[derive(Clone, Args)]
pub struct PushArgs {
    #[command(flatten)]
    pub common: CommonArgs,
    pub name: String,
    pub value: String,
    #[arg(long = "label", value_name = "NAME=VALUE")]
    pub labels: Vec<String>,
}

impl PushArgs {
    pub fn validate(&self) -> Result<(), String> {
        if !is_valid_metric_name(&self.name) {
            return Err(format!("invalid metric name `{}`", self.name));
        }
        parse_sample_value(&self.value)?;
        let mut names = HashSet::new();
        for label in &self.labels {
            let (name, _) = label
                .split_once('=')
                .ok_or_else(|| format!("invalid label argument `{label}`: expected NAME=VALUE"))?;
            if !is_valid_label_name(name) {
                return Err(format!("invalid label argument `{label}`: invalid name"));
            }
            if !names.insert(name) {
                return Err(format!("invalid label argument `{label}`: repeated name"));
            }
        }
        Ok(())
    }
}

/// Build the single-sample Prometheus exposition body used by `tool push`.
pub fn build_push_body(args: &PushArgs) -> String {
    let mut body = String::new();
    if args.labels.is_empty() {
        body.push_str(&args.name);
    } else {
        body.push_str(&args.name);
        body.push('{');
        for (index, label) in args.labels.iter().enumerate() {
            if index > 0 {
                body.push(',');
            }
            let (name, value) = label
                .split_once('=')
                .expect("build_push_body requires validated labels");
            body.push_str(name);
            body.push_str("=\"");
            append_prometheus_label_value(&mut body, value);
            body.push('"');
        }
        body.push('}');
    }
    body.push(' ');
    body.push_str(&args.value);
    body.push('\n');
    body
}

fn append_prometheus_label_value(output: &mut String, value: &str) {
    for character in value.chars() {
        match character {
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '"' => output.push_str("\\\""),
            character => output.push(character),
        }
    }
}

fn basic_auth_header(username: &str, password: &str) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let input = format!("{username}:{password}");
    let bytes = input.as_bytes();
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);
        encoded.push(ALPHABET[(first >> 2) as usize] as char);
        encoded.push(ALPHABET[((first & 0x03) << 4 | second >> 4) as usize] as char);
        encoded.push(if chunk.len() > 1 {
            ALPHABET[((second & 0x0f) << 2 | third >> 6) as usize] as char
        } else {
            '='
        });
        encoded.push(if chunk.len() > 2 {
            ALPHABET[(third & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    format!("Basic {encoded}")
}

fn push_sample(config: &Config, body: &str) -> Result<(), String> {
    let agent = ureq::AgentBuilder::new()
        .timeout(config.attempt_timeout)
        .redirects(0)
        .build();
    let mut request = agent
        .post(&config.ingest_endpoint)
        .set("Content-Type", "text/plain; version=0.0.4");
    request = match &config.credential {
        Credential::Bearer(token) => request.set("Authorization", &format!("Bearer {token}")),
        Credential::Basic { username, password } => {
            request.set("Authorization", &basic_auth_header(username, password))
        }
    };
    match request.send_string(body) {
        Ok(response) if (200..300).contains(&response.status()) => Ok(()),
        Ok(response) => Err(format!("server returned HTTP {}", response.status())),
        Err(_) => Err("request could not be completed".to_owned()),
    }
}

fn positive_seconds(value: &str) -> Result<u64, String> {
    let seconds = value
        .parse::<u64>()
        .map_err(|_| "duration must be a positive number of seconds".to_owned())?;
    if seconds == 0 {
        Err("duration must be positive".to_owned())
    } else {
        Ok(seconds)
    }
}

fn validate_cli(cli: &Cli) -> Result<(), String> {
    match &cli.command {
        Command::Run(args) => args
            .common
            .config()
            .map(|_| ())
            .map_err(|error| error.to_string()),
        Command::Push(args) => {
            args.common.config().map_err(|error| error.to_string())?;
            args.validate()
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct RunMeasurement {
    pub start_timestamp_ms: i64,
    pub exit_code: i32,
    pub wall_seconds: f64,
    pub cpu_seconds: Option<f64>,
    pub max_rss_bytes: Option<u64>,
}

fn run_command(args: &RunArgs) -> Result<RunMeasurement, String> {
    let program = args
        .command
        .first()
        .ok_or_else(|| "missing command after `--`".to_owned())?;
    let start = Instant::now();
    let start_timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let child = ProcessCommand::new(program)
        .args(&args.command[1..])
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|error| format!("could not start `{}`: {error}", program.to_string_lossy()))?;

    #[cfg(unix)]
    let (exit_code, cpu_seconds, max_rss_bytes) = wait_for_child(child, args.measure_resources)?;
    #[cfg(not(unix))]
    let (exit_code, cpu_seconds, max_rss_bytes) = {
        let mut child = child;
        let status = child.wait().map_err(|error| {
            format!(
                "could not wait for `{}`: {error}",
                program.to_string_lossy()
            )
        })?;
        (status.code().unwrap_or(1), None, None)
    };

    Ok(RunMeasurement {
        start_timestamp_ms,
        exit_code,
        wall_seconds: start.elapsed().as_secs_f64(),
        cpu_seconds,
        max_rss_bytes,
    })
}

#[cfg(unix)]
fn wait_for_child(
    child: std::process::Child,
    measure_resources: bool,
) -> Result<(i32, Option<f64>, Option<u64>), String> {
    use std::os::unix::process::ExitStatusExt;

    let pid = child.id() as libc::pid_t;
    drop(child);
    let mut status = 0;
    // SAFETY: rusage is a C plain-data struct, and zero is valid for every field.
    let mut usage = unsafe { std::mem::zeroed::<libc::rusage>() };
    loop {
        // SAFETY: status and usage are valid writable pointers, and pid is the spawned child.
        let result = unsafe { libc::wait4(pid, &mut status, 0, &mut usage) };
        if result == pid {
            break;
        }
        if result == -1 && std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted
        {
            continue;
        }
        return Err(format!(
            "could not wait for process {pid}: {}",
            std::io::Error::last_os_error()
        ));
    }

    let status = std::process::ExitStatus::from_raw(status);
    let exit_code = status
        .code()
        .or_else(|| status.signal().map(|signal| 128 + signal))
        .unwrap_or(1);
    let (cpu_seconds, max_rss_bytes) = if measure_resources {
        let user = usage.ru_utime.tv_sec as f64 + usage.ru_utime.tv_usec as f64 / 1_000_000.0;
        let system = usage.ru_stime.tv_sec as f64 + usage.ru_stime.tv_usec as f64 / 1_000_000.0;
        #[cfg(any(target_os = "macos", target_os = "ios"))]
        let max_rss = usage.ru_maxrss as u64;
        #[cfg(not(any(target_os = "macos", target_os = "ios")))]
        let max_rss = (usage.ru_maxrss as u64).saturating_mul(1024);
        (Some(user + system), Some(max_rss))
    } else {
        (None, None)
    };

    Ok((exit_code, cpu_seconds, max_rss_bytes))
}

#[derive(Debug, Clone, PartialEq)]
pub struct RunMetrics {
    pub name: String,
    pub timestamp_ms: i64,
    pub duration_seconds: f64,
    pub exit_code: i32,
    pub cpu_seconds: Option<f64>,
    pub max_rss_bytes: Option<u64>,
}

impl RunMetrics {
    pub fn from_measurement(
        name: impl Into<String>,
        timestamp_ms: i64,
        measurement: &RunMeasurement,
    ) -> Self {
        Self {
            name: name.into(),
            timestamp_ms,
            duration_seconds: measurement.wall_seconds,
            exit_code: measurement.exit_code,
            cpu_seconds: measurement.cpu_seconds,
            max_rss_bytes: measurement.max_rss_bytes,
        }
    }
}

fn format_sample_value(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value == f64::INFINITY {
        "+Inf".to_owned()
    } else if value == f64::NEG_INFINITY {
        "-Inf".to_owned()
    } else {
        value.to_string()
    }
}

fn escape_label_value(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    append_prometheus_label_value(&mut escaped, value);
    escaped
}

/// Build the Prometheus exposition body for one completed run.
pub fn build_run_body(metrics: &RunMetrics, include_timestamp: bool) -> String {
    let job = escape_label_value(&metrics.name);
    let timestamp = if include_timestamp {
        format!(" {}", metrics.timestamp_ms)
    } else {
        String::new()
    };
    let mut output = String::new();
    let mut add = |name: &str, value: f64| {
        output.push_str("# TYPE ");
        output.push_str(name);
        output.push_str(" gauge\n");
        output.push_str(name);
        output.push_str("{job=\"");
        output.push_str(&job);
        output.push_str("\"} ");
        output.push_str(&format_sample_value(value));
        output.push_str(&timestamp);
        output.push('\n');
    };

    add("autoangel_job_duration_seconds", metrics.duration_seconds);
    add("autoangel_job_exit_code", metrics.exit_code as f64);
    add("autoangel_job_success", f64::from(metrics.exit_code == 0));
    if let Some(value) = metrics.cpu_seconds {
        add("autoangel_job_cpu_seconds", value);
    }
    if let Some(value) = metrics.max_rss_bytes {
        add("autoangel_job_max_rss_bytes", value as f64);
    }
    output
}

/// Write a node_exporter textfile atomically, replacing its previous contents.
pub fn write_textfile(path: &std::path::Path, metrics: &RunMetrics) -> std::io::Result<()> {
    use std::fs::{self, File, OpenOptions};
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;

    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    let file_name = path.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "textfile path has no file name",
        )
    })?;
    let temporary = parent.join(format!(
        ".{}.tmp.{}",
        file_name.to_string_lossy(),
        std::process::id()
    ));
    let result = (|| {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true).mode(0o644);
        let mut file = options.open(&temporary)?;
        file.write_all(build_run_body(metrics, false).as_bytes())?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary, path)?;
        let directory = File::open(parent)?;
        directory.sync_all()
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

/// Write a textfile and report a single path-specific failure without panicking.
pub fn write_textfile_or_report(path: &std::path::Path, metrics: &RunMetrics) {
    if let Err(error) = write_textfile(path, metrics) {
        eprintln!("could not write textfile {}: {error}", path.display());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushError {
    Failed,
}

fn report_push_drop() -> Result<(), PushError> {
    eprintln!("push failed; measurements were dropped");
    Err(PushError::Failed)
}

/// Push one completed run, retrying failed attempts until the configured deadline.
pub fn push_run(config: &Config, metrics: &RunMetrics) -> Result<(), PushError> {
    let agent = ureq::AgentBuilder::new()
        .timeout(config.attempt_timeout)
        .build();
    let body = build_run_body(metrics, true);
    let started = Instant::now();
    let mut backoff = Duration::from_millis(500);

    loop {
        let mut request = agent
            .post(&config.ingest_endpoint)
            .set("Content-Type", "text/plain; version=0.0.4");
        request = match &config.credential {
            Credential::Bearer(token) => request.set("Authorization", &format!("Bearer {token}")),
            Credential::Basic { username, password } => {
                request.set("Authorization", &basic_auth_header(username, password))
            }
        };
        let succeeded = request
            .send_string(&body)
            .is_ok_and(|response| response.status() / 100 == 2);
        if succeeded {
            return Ok(());
        }
        if started.elapsed() >= config.retry_duration {
            return report_push_drop();
        }
        std::thread::sleep(backoff.min(config.retry_duration.saturating_sub(started.elapsed())));
        if started.elapsed() >= config.retry_duration {
            return report_push_drop();
        }
        backoff = (backoff * 2).min(Duration::from_secs(10));
    }
}

fn main() {
    let cli = Cli::parse();
    if let Err(error) = validate_cli(&cli) {
        eprintln!("configuration error: {error}");
        std::process::exit(2);
    }
    match &cli.command {
        Command::Run(args) => match run_command(args) {
            Ok(measurement) => {
                let config = args.common.config().expect("validated configuration");
                let metrics = RunMetrics::from_measurement(
                    &args.name,
                    measurement.start_timestamp_ms,
                    &measurement,
                );
                if let Some(path) = &args.textfile_path {
                    write_textfile_or_report(path, &metrics);
                }
                let _ = push_run(&config, &metrics);
                std::process::exit(measurement.exit_code);
            }
            Err(error) => {
                eprintln!("{error}");
                std::process::exit(127);
            }
        },
        Command::Push(args) => {
            let body = build_push_body(args);
            match push_sample(
                &args.common.config().expect("validated configuration"),
                &body,
            ) {
                Ok(()) => std::process::exit(0),
                Err(error) => {
                    eprintln!("push failed: {error}");
                    std::process::exit(1);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn accepts_engine_metric_and_label_grammars() {
        assert!(is_valid_metric_name("http_requests_total"));
        assert!(is_valid_metric_name(":request_count"));
        assert!(is_valid_label_name("instance_2"));
        assert!(!is_valid_label_name("instance:name"));
        assert!(!is_valid_metric_name("9xx_requests"));
    }

    #[test]
    fn accepts_prometheus_special_values() {
        assert!(parse_sample_value("NaN").unwrap().is_nan());
        assert_eq!(parse_sample_value("+Inf").unwrap(), f64::INFINITY);
        assert_eq!(parse_sample_value("-Inf").unwrap(), f64::NEG_INFINITY);
        assert_eq!(parse_sample_value("1.25").unwrap(), 1.25);
        assert!(parse_sample_value("not-a-number").is_err());
    }

    #[test]
    fn rejects_values_outside_engine_float_grammar() {
        for value in [
            "inf",
            "Inf",
            "infinity",
            "+infinity",
            "nan",
            "-nan",
            ".5",
            "1.",
            "1e400",
        ] {
            assert!(parse_sample_value(value).is_err(), "accepted {value}");
        }
        for value in ["+1", "1E5", "-0", "1.25e-2"] {
            assert!(parse_sample_value(value).is_ok(), "rejected {value}");
        }
    }

    #[test]
    fn help_does_not_render_secret_environment_values() {
        let marker = "do-not-print-this-secret";
        std::env::set_var("AUTOANGEL_INGEST_ENDPOINT", marker);
        std::env::set_var("AUTOANGEL_BEARER_TOKEN", marker);
        std::env::set_var("AUTOANGEL_BASIC_USERNAME", marker);
        std::env::set_var("AUTOANGEL_BASIC_PASSWORD", marker);

        let mut command = Cli::command();
        for subcommand in ["run", "push"] {
            let help = command
                .find_subcommand_mut(subcommand)
                .expect("subcommand exists")
                .render_long_help()
                .to_string();
            assert!(!help.contains(marker));
        }

        std::env::remove_var("AUTOANGEL_INGEST_ENDPOINT");
        std::env::remove_var("AUTOANGEL_BEARER_TOKEN");
        std::env::remove_var("AUTOANGEL_BASIC_USERNAME");
        std::env::remove_var("AUTOANGEL_BASIC_PASSWORD");
    }

    #[test]
    fn builds_one_prometheus_sample_with_escaped_labels() {
        let args = PushArgs {
            common: CommonArgs {
                ingest_endpoint: Some("http://localhost".into()),
                bearer_token: Some("secret".into()),
                basic_username: None,
                basic_password: None,
                attempt_timeout_secs: 10,
                retry_duration_secs: 60,
            },
            name: "job_value".into(),
            value: "NaN".into(),
            labels: vec!["path=a\\b\"c\nd".into(), "job=nightly".into()],
        };
        assert_eq!(
            build_push_body(&args),
            "job_value{path=\"a\\\\b\\\"c\\nd\",job=\"nightly\"} NaN\n"
        );
    }

    #[test]
    fn builds_sample_without_labels() {
        let args = PushArgs {
            common: CommonArgs {
                ingest_endpoint: Some("http://localhost".into()),
                bearer_token: Some("secret".into()),
                basic_username: None,
                basic_password: None,
                attempt_timeout_secs: 10,
                retry_duration_secs: 60,
            },
            name: "metric".into(),
            value: "1.25".into(),
            labels: vec![],
        };
        assert_eq!(build_push_body(&args), "metric 1.25\n");
    }

    #[test]
    fn builds_run_body_with_one_timestamp_and_textfile_omits_it() {
        let metrics = RunMetrics {
            name: "backup\\\"nightly".into(),
            timestamp_ms: 1_793_520_000_000,
            duration_seconds: 12.84,
            exit_code: 0,
            cpu_seconds: Some(2.5),
            max_rss_bytes: Some(4096),
        };
        let body = build_run_body(&metrics, true);
        assert_eq!(body.matches(" 1793520000000\n").count(), 5);
        assert!(body.contains("backup"));
        assert!(body.contains("nightly"));
        assert!(!build_run_body(&metrics, false).contains("1793520000000"));
    }

    #[test]
    fn basic_auth_header_is_rfc7617_shape() {
        assert_eq!(basic_auth_header("user", "pass"), "Basic dXNlcjpwYXNz");
    }

    #[test]
    fn textfile_replacement_is_atomic_and_replaces_content() {
        let directory = std::env::temp_dir().join(format!("autoangel-cli-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("jobs.prom");
        std::fs::write(&path, "old\n").unwrap();
        let metrics = RunMetrics {
            name: "job".into(),
            timestamp_ms: 10,
            duration_seconds: 1.0,
            exit_code: 0,
            cpu_seconds: None,
            max_rss_bytes: None,
        };
        write_textfile(&path, &metrics).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("autoangel_job_duration_seconds{job=\"job\"} 1"));
        assert!(!content.contains(" 10"));
        let _ = std::fs::remove_dir_all(directory);
    }

    #[test]
    fn textfile_supports_relative_paths() {
        let path =
            std::env::temp_dir().join(format!("autoangel-relative-{}.prom", std::process::id()));
        let metrics = RunMetrics {
            name: "job".into(),
            timestamp_ms: 10,
            duration_seconds: 1.0,
            exit_code: 0,
            cpu_seconds: None,
            max_rss_bytes: None,
        };
        write_textfile(&path, &metrics).unwrap();
        assert!(path.exists());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn textfile_failure_returns_an_error_for_reporting() {
        let path = std::path::Path::new("/definitely/missing/jobs.prom");
        let metrics = RunMetrics {
            name: "job".into(),
            timestamp_ms: 10,
            duration_seconds: 1.0,
            exit_code: 0,
            cpu_seconds: None,
            max_rss_bytes: None,
        };
        assert!(write_textfile(path, &metrics).is_err());
    }

    #[test]
    fn rejects_duplicate_labels() {
        let args = PushArgs {
            common: CommonArgs {
                ingest_endpoint: Some("http://localhost".into()),
                bearer_token: Some("secret".into()),
                basic_username: None,
                basic_password: None,
                attempt_timeout_secs: 10,
                retry_duration_secs: 60,
            },
            name: "metric".into(),
            value: "1".into(),
            labels: vec!["job=one".into(), "job=two".into()],
        };
        assert!(args.validate().is_err());
    }

    #[test]
    fn validates_positive_retry_settings() {
        assert!(positive_seconds("0").is_err());
        assert_eq!(positive_seconds("3").unwrap(), 3);
    }

    #[cfg(unix)]
    #[test]
    fn records_exit_code_and_resource_measurements() {
        let args = RunArgs {
            common: CommonArgs {
                ingest_endpoint: Some("http://localhost".to_owned()),
                bearer_token: Some("token".to_owned()),
                basic_username: None,
                basic_password: None,
                attempt_timeout_secs: 10,
                retry_duration_secs: 60,
            },
            name: "job".to_owned(),
            textfile_path: None,
            measure_resources: true,
            command: vec!["sh".into(), "-c".into(), "exit 7".into()],
        };
        let measurement = run_command(&args).expect("command should run");
        assert_eq!(measurement.exit_code, 7);
        assert!(measurement.wall_seconds >= 0.0);
        assert!(measurement.cpu_seconds.is_some());
        assert!(measurement.max_rss_bytes.is_some());
    }

    #[cfg(unix)]
    #[test]
    fn maps_signal_termination_to_shell_status() {
        let args = RunArgs {
            common: CommonArgs {
                ingest_endpoint: Some("http://localhost".to_owned()),
                bearer_token: Some("token".to_owned()),
                basic_username: None,
                basic_password: None,
                attempt_timeout_secs: 10,
                retry_duration_secs: 60,
            },
            name: "job".to_owned(),
            textfile_path: None,
            measure_resources: false,
            command: vec!["sh".into(), "-c".into(), "kill -TERM $$".into()],
        };
        assert_eq!(
            run_command(&args).expect("command should run").exit_code,
            143
        );
    }

    #[test]
    fn reports_spawn_failures_with_the_command_name() {
        let args = RunArgs {
            common: CommonArgs {
                ingest_endpoint: Some("http://localhost".to_owned()),
                bearer_token: Some("token".to_owned()),
                basic_username: None,
                basic_password: None,
                attempt_timeout_secs: 10,
                retry_duration_secs: 60,
            },
            name: "job".to_owned(),
            textfile_path: None,
            measure_resources: false,
            command: vec!["definitely-not-a-real-command".into()],
        };
        let error = run_command(&args).expect_err("missing command should fail");
        assert!(error.contains("definitely-not-a-real-command"));
    }

    #[test]
    fn clap_requires_run_name_and_command() {
        assert!(Cli::try_parse_from(["autoangel", "run", "--name", "job", "--"]).is_err());
        assert!(Cli::try_parse_from(["autoangel", "run", "--", "true"]).is_err());
    }

    #[test]
    fn clap_command_factory_is_valid() {
        Cli::command().debug_assert();
    }
}

/// Property 64: the CLI is transparent to the wrapped command.
///
/// Validates Requirement 67.1 (arguments passed in order unchanged) and the
/// exit-code mapping of 67.3/67.7 at the `run_command` boundary: any exit code
/// in 0..=255 is recorded exactly, and a signal maps to `128 + signo`. The
/// byte-for-byte stdout/stderr passthrough of 67.4/69.5 and "exits with the
/// wrapped code regardless of push outcome" of 67.3 are covered end-to-end by
/// the binary-level tests in `cli/tests/transparency.rs`.
#[cfg(all(test, unix))]
mod transparency_property_tests {
    use super::*;
    use proptest::prelude::*;
    use std::io::Read;
    use std::os::unix::process::ExitStatusExt;

    fn run_args(command: Vec<OsString>, measure_resources: bool) -> RunArgs {
        RunArgs {
            common: CommonArgs {
                ingest_endpoint: Some("http://localhost".to_owned()),
                bearer_token: Some("token".to_owned()),
                basic_username: None,
                basic_password: None,
                attempt_timeout_secs: 10,
                retry_duration_secs: 60,
            },
            name: "job".to_owned(),
            textfile_path: None,
            measure_resources,
            command,
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// Any exit code in 0..=255 chosen by the wrapped command is the exact
        /// code recorded by the CLI, with resource measurement on or off so
        /// both `wait_for_child` paths are exercised (Req 67.1, 67.3, 69.5).
        #[test]
        fn exit_code_is_passed_through_unchanged(code in 0u8..=255, measure in any::<bool>()) {
            let args = run_args(
                vec!["sh".into(), "-c".into(), format!("exit {code}").into()],
                measure,
            );
            let measurement = run_command(&args).expect("command should run");
            prop_assert_eq!(measurement.exit_code, i32::from(code));
            prop_assert_eq!(measurement.cpu_seconds.is_some(), measure);
            prop_assert_eq!(measurement.max_rss_bytes.is_some(), measure);
        }

        /// A wrapped command killed by a signal that the shell neither ignores
        /// nor cannot deliver is reported as `128 + signo`, matching shell
        /// convention, with measurement on or off (Req 67.7, 67.3, 69.5).
        #[test]
        fn signal_termination_maps_to_128_plus_signo(
            signo in prop::sample::select(vec![6i32, 9, 15]),
            measure in any::<bool>(),
        ) {
            // `ulimit -c 0` prevents SIGABRT (6) from writing a core file on
            // hosts where core dumps are enabled.
            let args = run_args(
                vec!["sh".into(), "-c".into(), format!("ulimit -c 0; kill -{signo} $$; sleep 5").into()],
                measure,
            );
            let measurement = run_command(&args).expect("command should run");
            prop_assert_eq!(measurement.exit_code, 128 + signo);
            prop_assert_eq!(measurement.cpu_seconds.is_some(), measure);
            prop_assert_eq!(measurement.max_rss_bytes.is_some(), measure);
        }

        /// Arguments after `--` reach the wrapped command in order and
        /// byte-for-byte unchanged, including shell metacharacters such as
        /// quotes, `$`, backticks, `*` and `;`. The child echoes each
        /// positional argument on its own line to a temp file, which the test
        /// then compares against the input. Carriage return and newline are
        /// excluded only because this readback is newline-delimited, not
        /// because the CLI treats them specially (Req 67.1).
        #[test]
        fn arguments_reach_the_child_in_order_and_unchanged(
            arguments in prop::collection::vec("[^\n\r\x00]{0,16}", 0..6)
        ) {
            let output = unique_temp_path("args");
            // Remove any stale file from an earlier aborted run with the same name.
            let _ = std::fs::remove_file(&output);
            // The destination path is passed as $1 (never interpolated into the
            // script) so paths containing quotes cannot break the command.
            let script = "out=$1; shift; for a in \"$@\"; do printf '%s\\n' \"$a\" >> \"$out\"; done";
            let mut command: Vec<OsString> = vec![
                "sh".into(),
                "-c".into(),
                script.into(),
                "sh".into(),
                output.clone().into_os_string(),
            ];
            for argument in &arguments {
                command.push(OsString::from(argument));
            }
            let args = run_args(command, false);
            let result = run_command(&args);
            let cleanup = scopeguard_remove(&output);
            let measurement = result.expect("command should run");
            prop_assert_eq!(measurement.exit_code, 0);

            let mut received = String::new();
            if output.exists() {
                std::fs::File::open(&output)
                    .expect("temp file opens")
                    .read_to_string(&mut received)
                    .expect("temp file reads");
            }
            let seen: Vec<&str> = received.lines().collect();
            prop_assert_eq!(seen.len(), arguments.len());
            for (expected, actual) in arguments.iter().zip(seen.iter()) {
                prop_assert_eq!(expected.as_str(), *actual);
            }
            drop(cleanup);
        }
    }

    fn unique_temp_path(tag: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("autoangel-{tag}-{}-{n}.txt", std::process::id()))
    }

    /// Returns a guard that removes `path` when dropped, so the temp file is
    /// cleaned up even if an assertion fails and unwinds.
    fn scopeguard_remove(path: &std::path::Path) -> impl Drop {
        struct Guard(std::path::PathBuf);
        impl Drop for Guard {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.0);
            }
        }
        Guard(path.to_path_buf())
    }

    /// The exit-code property relies on `ExitStatus::from_raw` reporting a
    /// signal (no exit code) for a signal-terminated status; this pins that
    /// standard-library behaviour the production mapping depends on.
    #[test]
    fn raw_signal_status_reports_signal_not_code() {
        let status = std::process::ExitStatus::from_raw(15);
        assert_eq!(status.code(), None);
        assert_eq!(status.signal(), Some(15));
    }
}
