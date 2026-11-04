use std::collections::HashSet;
use std::ffi::OsString;
use std::fmt;
use std::process::{Command as ProcessCommand, Stdio};
use std::time::{Duration, Instant};

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
struct RunMeasurement {
    exit_code: i32,
    wall_seconds: f64,
    cpu_seconds: Option<f64>,
    max_rss_bytes: Option<u64>,
}

fn run_command(args: &RunArgs) -> Result<RunMeasurement, String> {
    let program = args
        .command
        .first()
        .ok_or_else(|| "missing command after `--`".to_owned())?;
    let start = Instant::now();
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

fn main() {
    let cli = Cli::parse();
    if let Err(error) = validate_cli(&cli) {
        eprintln!("configuration error: {error}");
        std::process::exit(2);
    }
    match &cli.command {
        Command::Run(args) => match run_command(args) {
            Ok(measurement) => std::process::exit(measurement.exit_code),
            Err(error) => {
                eprintln!("{error}");
                std::process::exit(127);
            }
        },
        Command::Push(_) => {
            eprintln!("push client is not implemented yet");
            std::process::exit(2);
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
