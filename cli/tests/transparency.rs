//! Binary-level transparency tests for `autoangel run` (Property 64).
//!
//! These complement the in-crate `transparency_property_tests` by exercising
//! the real compiled binary end-to-end, which is the only way to observe the
//! parts of Property 64 that live in `main`:
//!
//! * Req 67.4 / 69.5 — the wrapped command's stdout is passed through
//!   byte-for-byte and the CLI writes nothing of its own to stdout; its own
//!   messages go only to stderr.
//! * Req 67.3 — the CLI exits with the wrapped command's exit code regardless
//!   of whether the push to the Ingest_Endpoint succeeds or fails.
#![cfg(unix)]

use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::thread;

const BIN: &str = env!("CARGO_BIN_EXE_autoangel-cli");

/// Base arguments pointing the push at `endpoint`, with a short retry window so
/// the "push fails" case returns quickly.
fn run_invocation(endpoint: &str, retry_secs: &str, script: &str, exit_code: i32) -> Command {
    let mut command = Command::new(BIN);
    command
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
        .arg("1")
        .arg("--")
        .arg("sh")
        .arg("-c")
        .arg(format!("{script}; exit {exit_code}"))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

/// Spawn a throwaway HTTP server on localhost that replies `200 OK` to one
/// request, so the push succeeds. The accept loop polls against a deadline so a
/// CLI that never connects (for example, a future regression that skips the
/// push) makes the test fail rather than hang. Returns the bound endpoint.
fn spawn_accepting_server() -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    let address = listener.local_addr().expect("local addr");
    let endpoint = format!("http://{address}/api/v1/push");
    listener
        .set_nonblocking(true)
        .expect("set listener non-blocking");
    let handle = thread::spawn(move || {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    // Drain the request so ureq's write completes before we reply.
                    stream
                        .set_read_timeout(Some(std::time::Duration::from_secs(2)))
                        .ok();
                    let mut buffer = [0u8; 4096];
                    let _ = stream.read(&mut buffer);
                    let _ = stream.write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    );
                    let _ = stream.flush();
                    return;
                }
                Err(ref error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if std::time::Instant::now() >= deadline {
                        return;
                    }
                    thread::sleep(std::time::Duration::from_millis(10));
                }
                Err(_) => return,
            }
        }
    });
    (endpoint, handle)
}

#[test]
fn stdout_is_passed_through_and_cli_writes_nothing_to_stdout() {
    // A payload with bytes that a naive line- or UTF-8-based passthrough could
    // mangle: embedded NUL, no trailing newline, and a non-ASCII byte.
    // Emit it from the child via printf of octal escapes so the exact bytes are
    // deterministic regardless of locale.
    let (endpoint, server) = spawn_accepting_server();
    let script = r#"printf 'out-\000-\342\234\224-tail'; printf 'err-line' 1>&2"#;
    let expected_stdout = b"out-\x00-\xe2\x9c\x94-tail";

    let output = run_invocation(&endpoint, "5", script, 0)
        .output()
        .expect("spawn CLI");

    assert_eq!(
        output.stdout, expected_stdout,
        "CLI stdout must equal the wrapped command's stdout byte-for-byte, \
         with nothing of the CLI's own added"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    // The CLI's own diagnostics, if any, go to stderr; the child's stderr bytes
    // must appear there and must come first.
    assert!(
        output.stderr.starts_with(b"err-line"),
        "wrapped command's stderr must be passed through first; got {stderr:?}"
    );
    // The push must have succeeded against the loopback server, so the drop
    // line must be absent (otherwise the server path was not exercised).
    assert!(
        !stderr.contains("push failed"),
        "push should have succeeded against the loopback server; got {stderr:?}"
    );
    assert_eq!(output.status.code(), Some(0));
    let _ = server.join();
}

#[test]
fn exits_with_wrapped_code_when_push_succeeds() {
    let (endpoint, server) = spawn_accepting_server();
    let output = run_invocation(&endpoint, "5", "true", 42)
        .output()
        .expect("spawn CLI");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("push failed"),
        "push should have succeeded against the loopback server; got {stderr:?}"
    );
    assert_eq!(
        output.status.code(),
        Some(42),
        "CLI must exit with the wrapped command's code when the push succeeds"
    );
    let _ = server.join();
}

#[test]
fn exits_with_wrapped_code_when_push_fails() {
    // Port 1 on loopback refuses connections, so every push attempt fails and
    // the retry window (1s) elapses. The CLI must still exit with code 42.
    let output = run_invocation("http://127.0.0.1:1/api/v1/push", "1", "true", 42)
        .output()
        .expect("spawn CLI");
    assert_eq!(
        output.status.code(),
        Some(42),
        "CLI must exit with the wrapped command's code even when the push fails"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("push failed"),
        "a dropped push must emit a stderr line reporting the failure; got {stderr:?}"
    );
}
