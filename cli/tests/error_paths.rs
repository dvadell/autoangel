//! Binary-level unit tests for CLI error paths (task 17.10).
//!
//! Covers:
//! * Req 67.5 — missing `--name` or missing command: error on stderr, non-zero
//!   exit, and no command started.
//! * Req 67.6 — command not found: error on stderr naming the command and the
//!   reason, non-zero exit.
//! * Req 68.7 — a node_exporter textfile that cannot be written: exactly one
//!   stderr line naming the path, the push is still attempted, and the CLI
//!   exits with the wrapped command's exit code.
//!
//! Req 67.7 (signal termination) and 68.6 (atomic textfile replace) are also
//! covered at the function level by the in-crate `tests` module.
#![cfg(unix)]

use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

const BIN: &str = env!("CARGO_BIN_EXE_autoangel-cli");

fn cli() -> Command {
    let mut command = Command::new(BIN);
    command
        .env_remove("AUTOANGEL_TEXTFILE_PATH")
        .env_remove("AUTOANGEL_INGEST_ENDPOINT")
        .env_remove("AUTOANGEL_BEARER_TOKEN")
        .env_remove("AUTOANGEL_BASIC_USERNAME")
        .env_remove("AUTOANGEL_BASIC_PASSWORD")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

/// A loopback server that answers requests with `200 OK` until stopped, used
/// to confirm that a push still happens even when the textfile write fails. The
/// returned counter records how many requests were received. Keeping the server
/// alive (rather than closing after one request) avoids a connection reset if a
/// retried request arrives, which would otherwise look like a push failure.
fn spawn_accepting_server() -> (
    String,
    Arc<AtomicUsize>,
    Arc<AtomicBool>,
    thread::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    let endpoint = format!("http://{}/api/v1/push", listener.local_addr().unwrap());
    listener.set_nonblocking(true).expect("non-blocking");
    let requests = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let requests_thread = Arc::clone(&requests);
    let stop_thread = Arc::clone(&stop);
    let handle = thread::spawn(move || {
        while !stop_thread.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    requests_thread.fetch_add(1, Ordering::Relaxed);
                    let _ = stream.set_nonblocking(false);
                    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                    let mut buffer = [0u8; 4096];
                    let _ = stream.read(&mut buffer);
                    let _ = stream.write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    );
                    let _ = stream.flush();
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(_) => return,
            }
        }
    });
    (endpoint, requests, stop, handle)
}

#[test]
fn missing_name_is_rejected_without_running_a_command() {
    // A sentinel file the command would create if it were ever started.
    let sentinel = std::env::temp_dir().join(format!(
        "autoangel-missing-name-{}.flag",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&sentinel);

    let output = cli()
        .arg("run")
        .arg("--ingest-endpoint")
        .arg("http://127.0.0.1:1/api/v1/push")
        .arg("--bearer-token")
        .arg("token")
        .arg("--")
        .arg("touch")
        .arg(&sentinel)
        .output()
        .expect("spawn CLI");

    assert!(
        !output.status.success(),
        "missing --name must exit non-zero"
    );
    assert!(output.stdout.is_empty(), "nothing should go to stdout");
    assert!(
        !output.stderr.is_empty(),
        "missing --name must report an error on stderr"
    );
    assert!(
        !sentinel.exists(),
        "no command should run when --name is missing"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--name"),
        "the error must name the missing --name argument; got {stderr:?}"
    );
    let _ = std::fs::remove_file(&sentinel);
}

#[test]
fn missing_command_is_rejected() {
    let output = cli()
        .arg("run")
        .arg("--name")
        .arg("job")
        .arg("--ingest-endpoint")
        .arg("http://127.0.0.1:1/api/v1/push")
        .arg("--bearer-token")
        .arg("token")
        .arg("--")
        .output()
        .expect("spawn CLI");

    assert!(
        !output.status.success(),
        "missing command must exit non-zero"
    );
    assert!(
        !output.stderr.is_empty(),
        "missing command must report an error on stderr"
    );
}

#[test]
fn command_not_found_names_the_command_on_stderr() {
    let output = cli()
        .arg("run")
        .arg("--name")
        .arg("job")
        .arg("--ingest-endpoint")
        .arg("http://127.0.0.1:1/api/v1/push")
        .arg("--bearer-token")
        .arg("token")
        .arg("--")
        .arg("autoangel-definitely-not-a-real-command")
        .output()
        .expect("spawn CLI");

    assert!(
        !output.status.success(),
        "an unstartable command must exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("autoangel-definitely-not-a-real-command"),
        "stderr must name the command that could not be started; got {stderr:?}"
    );
    assert!(
        stderr.contains("could not start"),
        "stderr must give the reason the command could not be started; got {stderr:?}"
    );
}

#[test]
fn textfile_write_failure_reports_the_path_and_still_pushes() {
    // A textfile path whose parent directory does not exist cannot be written,
    // so the atomic temp-file create fails.
    let bad_path = std::env::temp_dir()
        .join(format!("autoangel-no-such-dir-{}", std::process::id()))
        .join("jobs.prom");
    let (endpoint, requests, stop, server) = spawn_accepting_server();

    let output = cli()
        .arg("run")
        .arg("--name")
        .arg("job")
        .arg("--ingest-endpoint")
        .arg(&endpoint)
        .arg("--bearer-token")
        .arg("token")
        .arg("--textfile-path")
        .arg(&bad_path)
        .arg("--retry-duration-secs")
        .arg("5")
        .arg("--")
        .arg("sh")
        .arg("-c")
        .arg("exit 3")
        .output()
        .expect("spawn CLI");
    stop.store(true, Ordering::Relaxed);
    let _ = server.join();

    // 68.7: exits with the wrapped command's exit code despite the failure.
    assert_eq!(output.status.code(), Some(3));

    let stderr = String::from_utf8_lossy(&output.stderr);
    // 68.7: exactly one stderr line names the textfile path.
    let textfile_lines: Vec<&str> = stderr
        .lines()
        .filter(|line| line.contains("textfile"))
        .collect();
    assert_eq!(
        textfile_lines.len(),
        1,
        "expected exactly one textfile-failure line; got {stderr:?}"
    );
    assert!(
        textfile_lines[0].contains(&*bad_path.to_string_lossy()),
        "the textfile-failure line must name the path; got {:?}",
        textfile_lines[0]
    );
    // 68.7: the push was still attempted (the server received the request) and
    // succeeded, so there is no drop line.
    assert_eq!(
        requests.load(Ordering::Relaxed),
        1,
        "the push must still be attempted even when the textfile write fails"
    );
    assert!(
        !stderr.contains("push failed"),
        "the push should still have been attempted and succeeded; got {stderr:?}"
    );
    assert!(!bad_path.exists(), "no file should have been created");
}
