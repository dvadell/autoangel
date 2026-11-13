//! Binary-level tests for `autoangel run --measure-output` (Requirements 69.1 to 69.6).
//!
//! The real binary runs a shell command, the push goes to a loopback server that
//! keeps the request body, and the tests check what reached stdout and what was
//! pushed.
#![cfg(unix)]

use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use sha2::{Digest, Sha256};

const BIN: &str = env!("CARGO_BIN_EXE_autoangel-cli");

/// A loopback server that answers `200` to one push and sends its body to the test.
fn capture_server() -> (String, mpsc::Receiver<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    let endpoint = format!("http://{}/api/v1/push", listener.local_addr().unwrap());
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
        let mut request = Vec::new();
        let mut buffer = [0u8; 4096];
        loop {
            let read = stream.read(&mut buffer).unwrap_or(0);
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..read]);
            let text = String::from_utf8_lossy(&request).into_owned();
            if let Some(split) = text.find("\r\n\r\n") {
                let length = text[..split]
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|value| value.trim().parse::<usize>().unwrap_or(0))
                    })
                    .unwrap_or(0);
                if request.len() >= split + 4 + length {
                    break;
                }
            }
        }
        let _ =
            stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        let text = String::from_utf8_lossy(&request).into_owned();
        let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_owned();
        let _ = sender.send(body);
    });
    (endpoint, receiver)
}

fn run(endpoint: &str, measure: bool, script: &str) -> (std::process::Output, String) {
    let mut command = Command::new(BIN);
    command
        .args(["run", "--name", "job", "--ingest-endpoint", endpoint])
        .args(["--bearer-token", "token", "--retry-duration-secs", "2"])
        .args(["--attempt-timeout-secs", "2"]);
    if measure {
        command.arg("--measure-output");
    }
    command
        .args(["--", "sh", "-c", script])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let output = command.output().expect("run the CLI");
    (output, String::new())
}

fn value_of(body: &str, metric: &str) -> Option<u64> {
    body.lines()
        .find(|line| line.starts_with(&format!("{metric}{{")))
        .and_then(|line| line.split("} ").nth(1))
        .and_then(|rest| rest.split(' ').next())
        .and_then(|value| value.parse::<f64>().ok())
        .map(|value| value as u64)
}

fn hash48(data: &[u8]) -> u64 {
    let digest = Sha256::digest(data);
    u64::from_be_bytes([
        0, 0, digest[0], digest[1], digest[2], digest[3], digest[4], digest[5],
    ])
}

#[test]
fn output_passes_through_and_is_measured_even_on_a_failing_exit() {
    let (endpoint, body) = capture_server();
    let (output, _) = run(&endpoint, true, "printf 'hello\\nworld\\n'; exit 3");

    assert_eq!(output.stdout, b"hello\nworld\n");
    assert_eq!(output.status.code(), Some(3));

    let body = body.recv_timeout(Duration::from_secs(5)).expect("a push");
    assert_eq!(value_of(&body, "autoangel_job_output_bytes"), Some(12));
    assert_eq!(
        value_of(&body, "autoangel_job_output_hash"),
        Some(hash48(b"hello\nworld\n"))
    );
    assert_eq!(value_of(&body, "autoangel_job_exit_code"), Some(3));
    // Both new metrics carry the run timestamp, like the others.
    for line in body
        .lines()
        .filter(|l| l.contains("autoangel_job_output") && !l.starts_with('#'))
    {
        assert!(
            line.rsplit(' ').next().unwrap().parse::<u64>().is_ok(),
            "{line}"
        );
    }
}

#[test]
fn binary_output_is_untouched() {
    let (endpoint, body) = capture_server();
    // Every byte value twice, including NUL and bytes that are not UTF-8.
    let script =
        "i=0; while [ $i -lt 256 ]; do printf \"\\\\$(printf '%03o' $i)\"; i=$((i+1)); done";
    let (output, _) = run(&endpoint, true, script);

    let expected: Vec<u8> = (0u8..=255).collect();
    assert_eq!(output.stdout, expected);
    let body = body.recv_timeout(Duration::from_secs(5)).expect("a push");
    assert_eq!(value_of(&body, "autoangel_job_output_bytes"), Some(256));
    assert_eq!(
        value_of(&body, "autoangel_job_output_hash"),
        Some(hash48(&expected))
    );
}

#[test]
fn a_large_output_is_counted_whole() {
    let (endpoint, body) = capture_server();
    let (output, _) = run(&endpoint, true, "head -c 1000000 /dev/zero");
    assert_eq!(output.stdout.len(), 1_000_000);
    let body = body.recv_timeout(Duration::from_secs(5)).expect("a push");
    assert_eq!(
        value_of(&body, "autoangel_job_output_bytes"),
        Some(1_000_000)
    );
    assert_eq!(
        value_of(&body, "autoangel_job_output_hash"),
        Some(hash48(&vec![0u8; 1_000_000]))
    );
}

#[test]
fn no_output_is_zero_bytes_and_the_hash_of_nothing() {
    let (endpoint, body) = capture_server();
    let (output, _) = run(&endpoint, true, "true");
    assert!(output.stdout.is_empty());
    let body = body.recv_timeout(Duration::from_secs(5)).expect("a push");
    assert_eq!(value_of(&body, "autoangel_job_output_bytes"), Some(0));
    assert_eq!(
        value_of(&body, "autoangel_job_output_hash"),
        Some(0xe3b0_c442_98fc)
    );
}

#[test]
fn without_the_flag_neither_value_is_pushed() {
    let (endpoint, body) = capture_server();
    let (output, _) = run(&endpoint, false, "echo hello");
    assert_eq!(output.stdout, b"hello\n");
    let body = body.recv_timeout(Duration::from_secs(5)).expect("a push");
    assert!(!body.contains("autoangel_job_output"), "{body}");
    assert!(body.contains("autoangel_job_duration_seconds"));
}
