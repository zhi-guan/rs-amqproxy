//! 优雅关闭（#11）。

mod support;

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use amqproxy::methods::{self, CONNECTION, CONNECTION_CLOSE};
use support::*;
use tokio::net::TcpStream;

#[tokio::test]
async fn active_clients_are_told_and_all_upstream_connections_are_closed() {
    let broker = FakeBroker::start().await;
    let (proxy, stop, done) = start_stoppable_proxy(broker.addr).await;
    let (client, mut conn) = tokio::join!(
        TestClient::connect(proxy, Auth::Plain, GUEST),
        broker.accept()
    );
    let mut client = client.unwrap();

    stop.send(()).unwrap();

    let f = client.recv().await;
    assert_eq!(ids(&f), (CONNECTION, CONNECTION_CLOSE));
    let close = methods::parse_connection_close(&f.payload).unwrap();
    assert_eq!(close.code, 320);
    assert_eq!(close.text, "AMQProxy shutdown");
    conn.expect_closed().await;
    within("the proxy to stop", done).await.unwrap();
}

#[tokio::test]
async fn pooled_upstream_connections_are_closed_and_new_connections_are_refused() {
    let broker = FakeBroker::start().await;
    let (proxy, stop, done) = start_stoppable_proxy(broker.addr).await;
    let (client, mut conn) = tokio::join!(
        TestClient::connect(proxy, Auth::Plain, GUEST),
        broker.accept()
    );
    client.unwrap().close().await;

    stop.send(()).unwrap();
    conn.expect_closed().await;
    within("the proxy to stop", done).await.unwrap();
    assert!(
        TcpStream::connect(proxy).await.is_err(),
        "the listener must be closed"
    );
}

#[tokio::test]
async fn sigterm_stops_the_real_binary_cleanly() {
    let broker = FakeBroker::start().await;
    let mut child = Command::new(env!("CARGO_BIN_EXE_amqproxy"))
        .args(["-l", "127.0.0.1", "-p", "0"])
        .arg(format!("amqp://127.0.0.1:{}", broker.addr.port()))
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let listening = loop {
        let line = lines.next().expect("the proxy exited early").unwrap();
        if line.contains("Proxy listening on") {
            break line;
        }
    };
    let proxy: std::net::SocketAddr = listening.rsplit(' ').next().unwrap().parse().unwrap();
    let (client, _conn) = tokio::join!(
        TestClient::connect(proxy, Auth::Plain, GUEST),
        broker.accept()
    );
    let mut client = client.unwrap();

    Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();

    let f = client.recv().await;
    assert_eq!(ids(&f), (CONNECTION, CONNECTION_CLOSE));
    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(s) = child.try_wait().unwrap() {
            break s;
        }
        assert!(
            Instant::now() < deadline,
            "the proxy did not exit after SIGTERM"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(status.success(), "{status:?}");
}
