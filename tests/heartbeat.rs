//! 上游心跳（#10）：ADR-0005。

mod support;

use std::time::Duration;

use amqproxy::frame::Frame;
use support::*;

async fn connected(broker: &FakeBroker) -> (std::net::SocketAddr, TestClient, BrokerConn) {
    let proxy = start_proxy(broker.addr).await;
    let (client, conn) = tokio::join!(
        TestClient::connect(proxy, Auth::Plain, GUEST),
        broker.accept()
    );
    (proxy, client.unwrap(), conn)
}

#[tokio::test]
async fn the_broker_suggested_interval_is_used() {
    let mut broker = FakeBroker::start().await;
    broker.heartbeat = 30;
    let (_proxy, _client, conn) = connected(&broker).await;
    assert_eq!(conn.heartbeat, 30);
}

#[tokio::test]
async fn sixty_seconds_is_used_when_the_broker_suggests_none() {
    let broker = FakeBroker::start().await; // heartbeat = 0
    let (_proxy, _client, conn) = connected(&broker).await;
    assert_eq!(conn.heartbeat, 60);
}

#[tokio::test]
async fn heartbeats_are_sent_to_the_broker_and_never_forwarded_to_the_client() {
    let mut broker = FakeBroker::start().await;
    broker.heartbeat = 1;
    let (_proxy, mut client, mut conn) = connected(&broker).await;

    for _ in 0..3 {
        assert_eq!(conn.recv().await, Frame::heartbeat());
        conn.send(Frame::heartbeat()).await;
    }
    // 客户端协商的是 heartbeat=0：Broker 的心跳不能转给它。
    client.expect_quiet_for(Duration::from_millis(200)).await;
}

#[tokio::test]
async fn idle_pooled_connections_keep_sending_heartbeats() {
    let mut broker = FakeBroker::start().await;
    broker.heartbeat = 1;
    let proxy = start_proxy_with_idle(broker.addr, Duration::from_secs(60)).await;
    let (client, mut conn) = tokio::join!(
        TestClient::connect(proxy, Auth::Plain, GUEST),
        broker.accept()
    );
    client.unwrap().close().await;

    for _ in 0..3 {
        assert_eq!(conn.recv().await, Frame::heartbeat());
        conn.send(Frame::heartbeat()).await;
    }
}

#[tokio::test]
async fn a_silent_broker_is_declared_dead_and_the_client_is_told() {
    let mut broker = FakeBroker::start().await;
    broker.heartbeat = 1;
    let (_proxy, mut client, mut conn) = connected(&broker).await;

    // Broker 不再发送任何东西（连心跳也没有）：2 个周期后判定死亡。
    let started = std::time::Instant::now();
    let f = client.recv().await;
    let close = amqproxy::methods::parse_connection_close(&f.payload).unwrap();
    assert_eq!(close.code, 320);
    assert!(close.text.contains("UPSTREAM_ERROR"), "{}", close.text);
    let waited = started.elapsed();
    assert!(
        waited >= Duration::from_millis(1500),
        "declared dead too early: {waited:?}"
    );
    assert!(
        waited < Duration::from_secs(4),
        "declared dead too late: {waited:?}"
    );
    conn.expect_closed().await;
}

#[tokio::test]
async fn a_silent_broker_connection_in_the_pool_is_not_handed_out() {
    let mut broker = FakeBroker::start().await;
    broker.heartbeat = 1;
    let proxy = start_proxy_with_idle(broker.addr, Duration::from_secs(60)).await;
    let (client, mut conn) = tokio::join!(
        TestClient::connect(proxy, Auth::Plain, GUEST),
        broker.accept()
    );
    client.unwrap().close().await;

    conn.expect_closed().await; // 沉默 2 个周期后，代理关闭它
    let (c2, _conn2) = tokio::join!(
        TestClient::connect(proxy, Auth::Plain, GUEST),
        broker.accept()
    );
    c2.unwrap();
}
