//! Channel 复用（#8）：安全 / 不安全 Channel、伪造回复、断开清理。

mod support;

use std::time::Duration;

use amqproxy::methods::{self, CHANNEL, CHANNEL_CLOSE, CHANNEL_CLOSE_OK, CHANNEL_OPEN_OK};
use support::*;

/// 第一个客户端留下上游连接和（可能的）channel，第二个客户端接手。
async fn two_clients() -> (std::net::SocketAddr, FakeBroker, TestClient, BrokerConn) {
    let broker = FakeBroker::start().await;
    let proxy = start_proxy(broker.addr).await;
    let (client, conn) = tokio::join!(
        TestClient::connect(proxy, Auth::Plain, GUEST),
        broker.accept()
    );
    (proxy, broker, client.unwrap(), conn)
}

#[tokio::test]
async fn a_safe_channel_stays_open_upstream_and_is_reused_by_the_next_client() {
    let (proxy, _broker, mut c1, mut conn) = two_clients().await;
    open_channel(&mut c1, &mut conn, 1).await;
    c1.send(publish(1)).await;
    c1.send(empty_header(1)).await;
    assert_eq!(conn.recv().await, publish(1));
    assert_eq!(conn.recv().await, empty_header(1));

    // 关闭 channel：代理伪造 close-ok，Broker 什么都收不到。
    c1.send(methods::build_channel_close(1, 200, "bye")).await;
    assert_eq!(ids(&c1.recv().await), (CHANNEL, CHANNEL_CLOSE_OK));
    c1.close().await;

    // 第二个客户端打开同一个 channel：代理伪造 open-ok。
    let mut c2 = TestClient::connect(proxy, Auth::Plain, GUEST)
        .await
        .unwrap();
    c2.send(methods::build_channel_open(1)).await;
    assert_eq!(ids(&c2.recv().await), (CHANNEL, CHANNEL_OPEN_OK));
    c2.send(publish(1)).await;
    // Broker 看到的下一个帧是第二个客户端的 publish，中间没有 channel.close / channel.open。
    assert_eq!(conn.recv().await, publish(1));
}

#[tokio::test]
async fn a_client_that_disconnects_with_only_safe_channels_leaves_them_open() {
    let (proxy, _broker, mut c1, mut conn) = two_clients().await;
    open_channel(&mut c1, &mut conn, 1).await;
    c1.drop_abruptly();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut c2 = TestClient::connect(proxy, Auth::Plain, GUEST)
        .await
        .unwrap();
    c2.send(methods::build_channel_open(1)).await;
    assert_eq!(ids(&c2.recv().await), (CHANNEL, CHANNEL_OPEN_OK));
    conn.expect_quiet_for(Duration::from_millis(200)).await;
}

#[tokio::test]
async fn a_consumer_channel_is_really_closed_when_the_client_closes_it() {
    let (proxy, _broker, mut c1, mut conn) = two_clients().await;
    open_channel(&mut c1, &mut conn, 1).await;
    c1.send(basic_consume(1)).await;
    assert_eq!(conn.recv().await, basic_consume(1));

    c1.send(methods::build_channel_close(1, 200, "bye")).await;
    let f = conn.recv().await;
    assert_eq!(ids(&f), (CHANNEL, CHANNEL_CLOSE));
    conn.send(methods::build_channel_close_ok(1)).await;
    assert_eq!(ids(&c1.recv().await), (CHANNEL, CHANNEL_CLOSE_OK));
    c1.close().await;

    // 下一个客户端的 channel.open 必须真正到达 Broker。
    let mut c2 = TestClient::connect(proxy, Auth::Plain, GUEST)
        .await
        .unwrap();
    open_channel(&mut c2, &mut conn, 1).await;
}

#[tokio::test]
async fn unsafe_channels_are_closed_and_drained_when_the_client_disconnects() {
    let (proxy, _broker, mut c1, mut conn) = two_clients().await;
    open_channel(&mut c1, &mut conn, 1).await;
    open_channel(&mut c1, &mut conn, 2).await; // 安全，保持打开
    c1.send(basic_consume(1)).await;
    conn.recv().await;
    c1.drop_abruptly();

    let f = conn.recv().await;
    assert_eq!(ids(&f), (CHANNEL, CHANNEL_CLOSE));
    assert_eq!(f.channel, 1);
    assert_eq!(
        methods::parse_channel_close(&f.payload).unwrap().text,
        "Client disconnected"
    );

    // 关闭确认之前到达的投递不能泄露给下一个客户端。
    conn.send(method(1, 60, 60, b"stale delivery")).await;
    conn.send(methods::build_channel_close_ok(1)).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut c2 = TestClient::connect(proxy, Auth::Plain, GUEST)
        .await
        .unwrap();
    open_channel(&mut c2, &mut conn, 1).await; // channel 1 已关闭，真正打开
    c2.send(methods::build_channel_open(2)).await; // channel 2 仍然打开
    assert_eq!(ids(&c2.recv().await), (CHANNEL, CHANNEL_OPEN_OK));
}

#[tokio::test]
async fn the_upstream_connection_is_dropped_when_close_ok_never_arrives() {
    let (proxy, broker, mut c1, mut conn) = two_clients().await;
    open_channel(&mut c1, &mut conn, 1).await;
    c1.send(basic_consume(1)).await;
    conn.recv().await;
    c1.drop_abruptly();

    assert_eq!(ids(&conn.recv().await), (CHANNEL, CHANNEL_CLOSE));
    // Broker 不回复 close-ok：代理放弃这个上游连接。
    conn.expect_closed().await;

    let (c2, _conn2) = tokio::join!(
        TestClient::connect(proxy, Auth::Plain, GUEST),
        broker.accept()
    );
    c2.unwrap();
}

#[tokio::test]
async fn a_channel_closed_by_the_broker_is_not_reused() {
    let (proxy, _broker, mut c1, mut conn) = two_clients().await;
    open_channel(&mut c1, &mut conn, 1).await;

    conn.send(methods::build_channel_close(1, 404, "NOT_FOUND"))
        .await;
    let f = c1.recv().await;
    assert_eq!(ids(&f), (CHANNEL, CHANNEL_CLOSE));
    c1.send(methods::build_channel_close_ok(1)).await;
    assert_eq!(ids(&conn.recv().await), (CHANNEL, CHANNEL_CLOSE_OK));
    c1.close().await;

    let mut c2 = TestClient::connect(proxy, Auth::Plain, GUEST)
        .await
        .unwrap();
    open_channel(&mut c2, &mut conn, 1).await;
}

#[tokio::test]
async fn the_proxy_acknowledges_a_broker_channel_close_that_the_client_never_answered() {
    let (_proxy, _broker, mut c1, mut conn) = two_clients().await;
    open_channel(&mut c1, &mut conn, 1).await;

    conn.send(methods::build_channel_close(1, 404, "NOT_FOUND"))
        .await;
    assert_eq!(ids(&c1.recv().await), (CHANNEL, CHANNEL_CLOSE));
    c1.drop_abruptly();

    let f = conn.recv().await;
    assert_eq!((ids(&f), f.channel), ((CHANNEL, CHANNEL_CLOSE_OK), 1));
}

#[tokio::test]
async fn transactions_and_confirms_make_a_channel_unsafe() {
    for state_changing in [method(1, 90, 10, b""), method(1, 85, 10, b"")] {
        let (_proxy, _broker, mut c1, mut conn) = two_clients().await;
        open_channel(&mut c1, &mut conn, 1).await;
        c1.send(state_changing.clone()).await;
        assert_eq!(conn.recv().await, state_changing);
        c1.send(methods::build_channel_close(1, 200, "bye")).await;
        assert_eq!(ids(&conn.recv().await), (CHANNEL, CHANNEL_CLOSE));
    }
}

#[tokio::test]
async fn basic_get_is_safe_only_without_acknowledgements() {
    // no_ack = true：安全，关闭 channel 时 Broker 收不到 channel.close
    let (_proxy, _broker, mut c1, mut conn) = two_clients().await;
    open_channel(&mut c1, &mut conn, 1).await;
    c1.send(basic_get(1, true)).await;
    assert_eq!(conn.recv().await, basic_get(1, true));
    c1.send(methods::build_channel_close(1, 200, "bye")).await;
    assert_eq!(ids(&c1.recv().await), (CHANNEL, CHANNEL_CLOSE_OK));
    conn.expect_quiet_for(Duration::from_millis(200)).await;

    // no_ack = false：不安全
    let (_proxy, _broker, mut c1, mut conn) = two_clients().await;
    open_channel(&mut c1, &mut conn, 1).await;
    c1.send(basic_get(1, false)).await;
    assert_eq!(conn.recv().await, basic_get(1, false));
    c1.send(methods::build_channel_close(1, 200, "bye")).await;
    assert_eq!(ids(&conn.recv().await), (CHANNEL, CHANNEL_CLOSE));
}

#[tokio::test]
async fn a_channel_that_the_broker_closed_while_idle_in_the_pool_is_not_reused() {
    let (proxy, _broker, mut c1, mut conn) = two_clients().await;
    open_channel(&mut c1, &mut conn, 1).await;
    c1.close().await;

    conn.send(methods::build_channel_close(1, 406, "PRECONDITION_FAILED"))
        .await;
    assert_eq!(ids(&conn.recv().await), (CHANNEL, CHANNEL_CLOSE_OK));

    let mut c2 = TestClient::connect(proxy, Auth::Plain, GUEST)
        .await
        .unwrap();
    open_channel(&mut c2, &mut conn, 1).await;
}
