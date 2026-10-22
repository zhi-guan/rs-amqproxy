//! 黑盒测试：只运行 `amqproxy` 二进制，检查退出码和输出。

use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

fn run(args: &[&str], envs: &[(&str, &str)]) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_amqproxy"));
    cmd.args(args)
        .env_remove("LISTEN_ADDRESS")
        .env_remove("LISTEN_PORT")
        .env_remove("AMQP_URL");
    for (k, v) in envs {
        cmd.env(k, v);
    }
    // 配置正确时代理会一直运行：给它一点时间打印日志，然后终止。
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().unwrap();
    let deadline = Instant::now() + Duration::from_millis(500);
    while Instant::now() < deadline && child.try_wait().unwrap().is_none() {
        std::thread::sleep(Duration::from_millis(10));
    }
    let _ = child.kill();
    child.wait_with_output().unwrap()
}

fn stdout(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}

fn stderr(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

#[test]
fn version_prints_version_and_exits_zero() {
    let o = run(&["--version"], &[]);
    assert!(o.status.success());
    assert!(stdout(&o).trim().starts_with("0."));
    let o = run(&["-v"], &[]);
    assert!(o.status.success());
}

#[test]
fn help_lists_every_option() {
    let o = run(&["--help"], &[]);
    assert!(o.status.success());
    let out = stdout(&o);
    for opt in [
        "--listen",
        "--port",
        "--debug",
        "--idle-connection-timeout",
        "--help",
        "--version",
    ] {
        assert!(out.contains(opt), "help should mention {opt}:\n{out}");
    }
}

#[test]
fn missing_upstream_fails_with_usage() {
    let o = run(&[], &[]);
    assert!(!o.status.success());
    assert!(stderr(&o).contains("Usage"));
}

#[test]
fn invalid_scheme_fails() {
    let o = run(&["http://example.com"], &[]);
    assert!(!o.status.success());
    assert!(stderr(&o).contains("amqps://hostname"));
}

#[test]
fn url_without_host_fails() {
    let o = run(&["amqp://"], &[]);
    assert!(!o.status.success());
}

#[test]
fn defaults_are_localhost_5673_and_amqp_5672() {
    let o = run(&["amqp://broker.example.com"], &[]);
    let out = stdout(&o);
    assert!(out.contains("upstream=broker.example.com:5672"), "{out}");
    assert!(out.contains("tls=false"), "{out}");
    assert!(out.contains("listen=localhost:5673"), "{out}");
    assert!(out.contains("idle_timeout=5s"), "{out}");
}

#[test]
fn amqps_defaults_to_port_5671_with_tls() {
    let o = run(&["amqps://broker.example.com"], &[]);
    let out = stdout(&o);
    assert!(out.contains("upstream=broker.example.com:5671"), "{out}");
    assert!(out.contains("tls=true"), "{out}");
}

#[test]
fn explicit_upstream_port_wins_over_scheme_default() {
    let o = run(&["amqp://broker.example.com:1234"], &[]);
    assert!(stdout(&o).contains("upstream=broker.example.com:1234"));
}

#[test]
fn credentials_and_vhost_in_url_are_ignored() {
    let o = run(&["amqp://user:secret@broker.example.com/vh"], &[]);
    let out = stdout(&o);
    assert!(out.contains("upstream=broker.example.com:5672"), "{out}");
    assert!(
        !out.contains("secret"),
        "password must not be logged: {out}"
    );
}

#[test]
fn environment_variables_configure_the_proxy() {
    let o = run(
        &[],
        &[
            ("AMQP_URL", "amqp://env.example.com"),
            ("LISTEN_ADDRESS", "0.0.0.0"),
            ("LISTEN_PORT", "7000"),
        ],
    );
    let out = stdout(&o);
    assert!(out.contains("upstream=env.example.com:5672"), "{out}");
    assert!(out.contains("listen=0.0.0.0:7000"), "{out}");
}

#[test]
fn command_line_wins_over_environment() {
    let o = run(
        &["-l", "127.0.0.1", "--port=8000", "amqp://cli.example.com"],
        &[
            ("AMQP_URL", "amqp://env.example.com"),
            ("LISTEN_ADDRESS", "0.0.0.0"),
            ("LISTEN_PORT", "7000"),
        ],
    );
    let out = stdout(&o);
    assert!(out.contains("upstream=cli.example.com:5672"), "{out}");
    assert!(out.contains("listen=127.0.0.1:8000"), "{out}");
}

#[test]
fn idle_connection_timeout_option() {
    let o = run(&["-t", "30", "amqp://x.example.com"], &[]);
    assert!(stdout(&o).contains("idle_timeout=30s"));
    let o = run(
        &["--idle-connection-timeout=9", "amqp://x.example.com"],
        &[],
    );
    assert!(stdout(&o).contains("idle_timeout=9s"));
}

#[test]
fn invalid_option_fails() {
    let o = run(&["--bogus", "amqp://x.example.com"], &[]);
    assert!(!o.status.success());
}

#[test]
fn debug_flag_enables_debug_logging() {
    let quiet = run(&["amqp://x.example.com"], &[]);
    let debug = run(&["-d", "amqp://x.example.com"], &[]);
    assert!(!stdout(&quiet).contains("DEBUG"));
    assert!(stdout(&debug).contains("DEBUG"));
}

#[test]
fn log_lines_have_timestamps() {
    let o = run(&["amqp://x.example.com"], &[]);
    let first = stdout(&o).lines().next().unwrap().to_string();
    // 例如 2026-10-22T08:00:00.123456Z  INFO ...
    assert!(first.starts_with("20"), "{first}");
    assert!(first.contains('T') && first.contains("INFO"), "{first}");
}
