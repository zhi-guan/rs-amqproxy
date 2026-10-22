//! 统计日志（#7）：运行真实的二进制，检查每 5 秒一行的统计。

mod support;

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use support::*;

#[tokio::test]
async fn periodic_stats_line_reports_clients_upstreams_and_rss() {
    let broker = FakeBroker::start().await;
    let mut child = Command::new(env!("CARGO_BIN_EXE_amqproxy"))
        .args(["-l", "127.0.0.1", "-p", "0"])
        .arg(format!("amqp://127.0.0.1:{}", broker.addr.port()))
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    let out = child.stdout.take().unwrap();
    std::thread::spawn(move || {
        for line in BufReader::new(out).lines().map_while(Result::ok) {
            let _ = tx.send(line);
        }
    });

    let wait_for = |needle: &str| -> String {
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let line = rx
                .recv_timeout(left)
                .unwrap_or_else(|_| panic!("no log line containing {needle:?}"));
            if line.contains(needle) {
                return line;
            }
        }
    };

    let listening = wait_for("Proxy listening on");
    let proxy: std::net::SocketAddr = listening.rsplit(' ').next().unwrap().parse().unwrap();

    let (client, _conn) = tokio::join!(
        TestClient::connect(proxy, Auth::Plain, GUEST),
        broker.accept()
    );
    let _client = client.unwrap();

    let stats = wait_for("clients=1 upstreams=1 rss=");
    assert!(stats.trim_end().ends_with("MB"), "{stats}");
    let _ = child.kill();
    let _ = child.wait();
}
