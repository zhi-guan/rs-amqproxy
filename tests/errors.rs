//! 错误路径（#9）：消息传输中断开、帧错误、Broker 关闭连接、写入失败。

mod support;

use std::time::Duration;

use amqproxy::frame::{Frame, FrameType};
use amqproxy::methods::{self, CHANNEL, CHANNEL_CLOSE, CONNECTION, CONNECTION_CLOSE};
use bytes::{BufMut, Bytes, BytesMut};
use support::*;

fn header(channel: u16, body_size: u64) -> Frame {
    let mut b = BytesMut::new();
    b.put_u16(60); // class
    b.put_u16(0); // weight
    b.put_u64(body_size);
    b.put_u16(0); // property flags
    Frame::new(FrameType::Header, channel, b.freeze())
}

fn body(channel: u16, len: usize) -> Frame {
    Frame::new(FrameType::Body, channel, Bytes::from(vec![1u8; len]))
}

async fn connected(broker: &FakeBroker) -> (std::net::SocketAddr, TestClient, BrokerConn) {
    let proxy = start_proxy(broker.addr).await;
    let (client, conn) = tokio::join!(
        TestClient::connect(proxy, Auth::Plain, GUEST),
        broker.accept()
    );
    (proxy, client.unwrap(), conn)
}

#[tokio::test]
async fn a_client_that_disconnects_in_the_middle_of_a_message_poisons_the_upstream_connection() {
    let broker = FakeBroker::start().await;
    let (proxy, mut c1, mut conn) = connected(&broker).await;
    open_channel(&mut c1, &mut conn, 1).await;

    c1.send(publish(1)).await;
    c1.send(header(1, 100)).await;
    c1.send(body(1, 40)).await; // 还差 60 字节
    for _ in 0..3 {
        conn.recv().await;
    }
    c1.drop_abruptly();

    conn.expect_closed().await;
    let (c2, _conn2) = tokio::join!(
        TestClient::connect(proxy, Auth::Plain, GUEST),
        broker.accept()
    );
    c2.unwrap();
}

#[tokio::test]
async fn a_client_that_disconnects_between_messages_keeps_the_upstream_connection() {
    let broker = FakeBroker::start().await;
    let (proxy, mut c1, mut conn) = connected(&broker).await;
    open_channel(&mut c1, &mut conn, 1).await;

    // 一个由两个 body 帧组成的完整消息，加上一个没有 body 的消息。
    c1.send(publish(1)).await;
    c1.send(header(1, 100)).await;
    c1.send(body(1, 40)).await;
    c1.send(body(1, 60)).await;
    c1.send(publish(1)).await;
    c1.send(header(1, 0)).await;
    for _ in 0..6 {
        conn.recv().await;
    }
    c1.drop_abruptly();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut c2 = TestClient::connect(proxy, Auth::Plain, GUEST)
        .await
        .unwrap();
    c2.send(publish(1)).await;
    assert_eq!(conn.recv().await, publish(1));
}

#[tokio::test]
async fn an_oversized_frame_from_the_client_gets_frame_error_and_the_upstream_is_dropped() {
    let broker = FakeBroker::start().await;
    let (_proxy, mut c1, mut conn) = connected(&broker).await;
    // 声明 1 MiB 的 payload，超过给客户端的 frame_max (131072)。
    c1.send_raw(&[3, 0, 1, 0, 0x10, 0, 0]).await;

    let close = methods::parse_connection_close(&c1.recv().await.payload).unwrap();
    assert_eq!(close.code, methods::FRAME_ERROR);
    assert!(close.text.contains("FRAME_ERROR"), "{}", close.text);
    conn.expect_closed().await;
}

#[tokio::test]
async fn a_frame_with_a_bad_frame_end_gets_frame_error() {
    let broker = FakeBroker::start().await;
    let (_proxy, mut c1, _conn) = connected(&broker).await;
    c1.send_raw(&[8, 0, 0, 0, 0, 0, 0, 0xAA]).await;
    let close = methods::parse_connection_close(&c1.recv().await.payload).unwrap();
    assert_eq!(close.code, methods::FRAME_ERROR);
}

#[tokio::test]
async fn a_frame_larger_than_the_brokers_frame_max_gets_frame_error() {
    let mut broker = FakeBroker::start().await;
    broker.frame_max = 8192;
    let (_proxy, mut c1, mut conn) = connected(&broker).await;
    open_channel(&mut c1, &mut conn, 1).await;

    c1.send(body(1, 10_000)).await; // 对客户端合法，但超过 Broker 的 8192
    let close = methods::parse_connection_close(&c1.recv().await.payload).unwrap();
    assert_eq!(close.code, methods::FRAME_ERROR);
    assert!(close.text.contains("frame_max"), "{}", close.text);
    conn.expect_closed().await;
}

#[tokio::test]
async fn a_connection_close_from_the_broker_is_forwarded_and_acknowledged() {
    let broker = FakeBroker::start().await;
    let (_proxy, mut c1, mut conn) = connected(&broker).await;
    conn.send(methods::build_connection_close(
        320,
        "CONNECTION_FORCED - shutdown",
    ))
    .await;

    let f = c1.recv().await;
    assert_eq!(ids(&f), (CONNECTION, CONNECTION_CLOSE));
    let close = methods::parse_connection_close(&f.payload).unwrap();
    assert_eq!(
        (close.code, close.text.as_str()),
        (320, "CONNECTION_FORCED - shutdown")
    );
    // 代理回复 close-ok 并关闭。
    conn.expect_closed().await;
}

#[tokio::test]
async fn a_client_that_stops_reading_does_not_kill_the_upstream_connection() {
    let broker = FakeBroker::start().await;
    let (proxy, mut c1, mut conn) = connected(&broker).await;
    open_channel(&mut c1, &mut conn, 1).await;
    c1.send(basic_consume(1)).await;
    conn.recv().await;

    // Broker 持续投递，而客户端从不读取，然后突然消失。
    let flood = tokio::spawn(async move {
        for _ in 0..2000 {
            conn.send(method(1, 60, 60, &[9u8; 1000])).await;
        }
        conn
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    c1.drop_abruptly();
    let mut conn = flood.await.unwrap();

    // 代理清理 channel（关闭它，丢弃剩余的投递），然后连接可以复用。
    loop {
        let f = conn.recv().await;
        if f.kind == FrameType::Method && ids(&f) == (CHANNEL, CHANNEL_CLOSE) {
            break;
        }
    }
    conn.send(methods::build_channel_close_ok(1)).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut c2 = TestClient::connect(proxy, Auth::Plain, GUEST)
        .await
        .unwrap();
    open_channel(&mut c2, &mut conn, 1).await;
}
