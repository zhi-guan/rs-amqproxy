//! 连接池（#7）：借出/归还、空闲超时、已死连接。

mod support;

use std::time::{Duration, Instant};

use support::*;

#[tokio::test]
async fn a_second_client_with_the_same_credentials_reuses_the_upstream_connection() {
    let broker = FakeBroker::start().await;
    let proxy = start_proxy(broker.addr).await;

    let (c1, mut conn) = tokio::join!(
        TestClient::connect(proxy, Auth::Plain, GUEST),
        broker.accept()
    );
    c1.unwrap().close().await;

    // 不应该有新的 Broker 连接；第二个客户端的帧到达同一个上游连接。
    let mut c2 = TestClient::connect(proxy, Auth::Plain, GUEST)
        .await
        .unwrap();
    let publish = method(1, 60, 40, b"x");
    c2.send(publish.clone()).await;
    assert_eq!(conn.recv().await, publish);
    assert!(
        tokio::time::timeout(Duration::from_millis(300), broker.accept())
            .await
            .is_err(),
        "the proxy must not open a second upstream connection"
    );
}

#[tokio::test]
async fn different_vhost_or_password_do_not_share_an_upstream_connection() {
    let broker = FakeBroker::start().await;
    let proxy = start_proxy(broker.addr).await;

    let (c1, _conn1) = tokio::join!(
        TestClient::connect(proxy, Auth::Plain, GUEST),
        broker.accept()
    );
    c1.unwrap().close().await;

    let other_vhost = Creds {
        vhost: "/other",
        ..GUEST
    };
    let (c2, conn2) = tokio::join!(
        TestClient::connect(proxy, Auth::Plain, other_vhost),
        broker.accept()
    );
    c2.unwrap().close().await;
    assert_eq!(conn2.vhost, "/other");

    let other_pw = Creds {
        password: "different",
        ..GUEST
    };
    let (c3, conn3) = tokio::join!(
        TestClient::connect(proxy, Auth::Plain, other_pw),
        broker.accept()
    );
    c3.unwrap();
    assert_eq!(conn3.password, "different");
}

#[tokio::test]
async fn idle_connections_are_closed_only_after_the_idle_timeout() {
    let broker = FakeBroker::start().await;
    let proxy = start_proxy_with_idle(broker.addr, Duration::from_secs(2)).await;

    let (c1, mut conn) = tokio::join!(
        TestClient::connect(proxy, Auth::Plain, GUEST),
        broker.accept()
    );
    c1.unwrap().close().await;
    let returned = Instant::now();

    conn.expect_quiet_for(Duration::from_millis(1200)).await;
    conn.expect_closed().await;
    let idle_for = returned.elapsed();
    assert!(
        idle_for >= Duration::from_millis(1900),
        "closed too early: {idle_for:?}"
    );
    assert!(
        idle_for < Duration::from_secs(4),
        "closed too late: {idle_for:?}"
    );
}

#[tokio::test]
async fn a_pooled_connection_that_the_broker_closed_is_not_handed_out() {
    let broker = FakeBroker::start().await;
    let proxy = start_proxy(broker.addr).await;

    let (c1, conn) = tokio::join!(
        TestClient::connect(proxy, Auth::Plain, GUEST),
        broker.accept()
    );
    c1.unwrap().close().await;
    drop(conn); // Broker 关闭了池中的空闲连接
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (c2, _conn2) = tokio::join!(
        TestClient::connect(proxy, Auth::Plain, GUEST),
        broker.accept()
    );
    c2.expect("the client must get a working connection");
}

#[tokio::test]
async fn a_client_that_disconnects_abruptly_returns_a_clean_connection_to_the_pool() {
    let broker = FakeBroker::start().await;
    let proxy = start_proxy(broker.addr).await;

    let (c1, mut conn) = tokio::join!(
        TestClient::connect(proxy, Auth::Plain, GUEST),
        broker.accept()
    );
    c1.unwrap().drop_abruptly();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut c2 = TestClient::connect(proxy, Auth::Plain, GUEST)
        .await
        .unwrap();
    c2.send(method(1, 60, 40, b"y")).await;
    assert_eq!(conn.recv().await, method(1, 60, 40, b"y"));
}
