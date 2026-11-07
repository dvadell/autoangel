//! Timing tests for `push_run` retry behaviour (Property 68).
//!
//! Validates Requirement 68.4 (retry a failed push until it succeeds or the
//! configured retry duration has elapsed since the first attempt) and 68.5
//! (when the retry duration elapses without success, stop retrying, drop the
//! measurements, write one stderr line, and return failure).
//!
//! These are driven through the real compiled binary against a loopback server
//! so the actual `ureq` client, backoff and wall-clock deadline are exercised.
//! Durations are kept small (sub-second) so the suite stays fast, and the
//! assertions use generous slack so they are not timing-flaky on a loaded host.
#![cfg(unix)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_autoangel-cli");

/// Reply to one connection with the given minimal HTTP status line.
fn answer(mut stream: TcpStream, status_line: &str) {
    // Accepted sockets may inherit non-blocking mode on some platforms; force
    // blocking with a read timeout so the request is drained without a hang.
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    let mut buffer = [0u8; 4096];
    let _ = stream.read(&mut buffer);
    let response =
        format!("HTTP/1.1 {status_line}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

/// A loopback server that answers every connection with `503 Service
/// Unavailable`, counting how many push attempts it received, until stopped.
struct FailingServer {
    endpoint: String,
    stop: Arc<AtomicBool>,
    attempts: Arc<std::sync::atomic::AtomicUsize>,
    handle: Option<thread::JoinHandle<()>>,
}

impl FailingServer {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let endpoint = format!("http://{}/api/v1/push", listener.local_addr().unwrap());
        listener.set_nonblocking(true).expect("non-blocking");
        let stop = Arc::new(AtomicBool::new(false));
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let stop_thread = Arc::clone(&stop);
        let attempts_thread = Arc::clone(&attempts);
        let handle = thread::spawn(move || {
            while !stop_thread.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        attempts_thread.fetch_add(1, Ordering::Relaxed);
                        answer(stream, "503 Service Unavailable");
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            endpoint,
            stop,
            attempts,
            handle: Some(handle),
        }
    }

    fn attempts(&self) -> usize {
        self.attempts.load(Ordering::Relaxed)
    }
}

impl Drop for FailingServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn run_against(
    endpoint: &str,
    retry_secs: &str,
    attempt_secs: &str,
    shell_command: &str,
) -> std::process::Output {
    Command::new(BIN)
        .arg("run")
        .arg("--name")
        .arg("job")
        .arg("--ingest-endpoint")
        .arg(endpoint)
        .arg("--bearer-token")
        .arg("token")
        .arg("--retry-duration-secs")
        .arg(retry_secs)
        .arg("--attempt-timeout-secs")
        .arg(attempt_secs)
        .arg("--")
        .arg("sh")
        .arg("-c")
        .arg(shell_command)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("spawn CLI")
}

#[test]
fn retry_stops_within_the_retry_duration_and_reports_the_drop() {
    // The CLI only accepts whole seconds for the retry duration, so use 1s and
    // allow a generous overrun bound: one in-flight attempt (timeout 1s) plus
    // process startup and scheduling slack.
    let server = FailingServer::start();
    let retry = Duration::from_secs(1);
    let max_overrun = Duration::from_secs(3);

    let start = Instant::now();
    // Exit 7 so we can prove the wrapped code is preserved despite the drop.
    let output = run_against(&server.endpoint, "1", "1", "exit 7");
    let elapsed = start.elapsed();

    // 68.4 / 68.5: retrying never runs much past the retry duration.
    assert!(
        elapsed <= retry + max_overrun,
        "push retried for {elapsed:?}, which exceeds the {retry:?} window plus slack"
    );
    // The retry window must actually be used: more than one attempt was made.
    assert!(
        server.attempts() >= 2,
        "expected the push to be retried at least once; saw {} attempt(s)",
        server.attempts()
    );
    // 68.5: the wrapped command's exit code is preserved despite the dropped push.
    assert_eq!(output.status.code(), Some(7));
    // 68.5: exactly one stderr line reports the dropped push.
    let stderr = String::from_utf8_lossy(&output.stderr);
    let drop_lines: Vec<&str> = stderr
        .lines()
        .filter(|line| line.contains("push failed") || line.contains("dropped"))
        .collect();
    assert_eq!(
        drop_lines.len(),
        1,
        "expected exactly one drop line on stderr; got {stderr:?}"
    );
}

#[test]
fn successful_push_stops_retrying_immediately() {
    // A server that fails the first attempt, then succeeds, must make the CLI
    // stop retrying and return success well inside the retry window.
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    let endpoint = format!("http://{}/api/v1/push", listener.local_addr().unwrap());
    listener.set_nonblocking(true).expect("non-blocking");
    let stop = Arc::new(AtomicBool::new(false));
    let stop_thread = Arc::clone(&stop);
    let handle = thread::spawn(move || {
        let mut served = 0u32;
        while !stop_thread.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, _)) => {
                    // Fail the first attempt, succeed on every attempt after.
                    // Keep serving (rather than returning) so a retried request
                    // caused by a dropped response is still answered.
                    if served == 0 {
                        answer(stream, "503 Service Unavailable");
                    } else {
                        answer(stream, "200 OK");
                    }
                    served += 1;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(_) => return,
            }
        }
    });

    let start = Instant::now();
    let output = run_against(&endpoint, "30", "2", "exit 0");
    let elapsed = start.elapsed();
    stop.store(true, Ordering::Relaxed);
    let _ = handle.join();

    // It succeeded on the second attempt, so it must return long before the
    // 30s retry window: one failed attempt plus the ~500ms backoff.
    assert!(
        elapsed <= Duration::from_secs(10),
        "a push that succeeds on retry should not wait near the retry window; took {elapsed:?}"
    );
    assert_eq!(output.status.code(), Some(0));
    // A successful push emits no drop line.
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("push failed"),
        "a successful push must not report a drop; got {stderr:?}"
    );
}
