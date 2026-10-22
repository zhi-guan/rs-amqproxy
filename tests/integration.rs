//! 集成测试：用真实的 RabbitMQ 验证（#2、#5）。
//!
//! 运行前先启动测试环境：
//!   test/gen-certs.sh
//!   docker compose -f test/docker-compose.yml up -d --wait
//! 然后：cargo test --features integration --test integration

#![cfg(feature = "integration")]

mod support;

use std::time::Duration;

use amqproxy::frame::{Frame, FrameType};
use amqproxy::methods::{self, CHANNEL, CHANNEL_OPEN_OK};
use bytes::{BufMut, Bytes, BytesMut};
use support::*;

const RABBIT: &str = "127.0.0.1";
const GUEST_LOGIN: Creds<'static> = Creds {
    user: "guest",
    password: "guest",
    vhost: "/",
};

fn short_str(b: &mut BytesMut, s: &str) {
    b.put_u8(s.len() as u8);
    b.put_slice(s.as_bytes());
}

fn queue_declare(ch: u16, queue: &str) -> Frame {
    let mut b = BytesMut::new();
    b.put_u16(0);
    short_str(&mut b, queue);
    b.put_u8(0); // 非 passive、非 durable、非 exclusive、非 auto-delete
    b.put_u32(0); // 参数表
    method(ch, 50, 10, &b)
}

fn queue_purge(ch: u16, queue: &str) -> Frame {
    let mut b = BytesMut::new();
    b.put_u16(0);
    short_str(&mut b, queue);
    b.put_u8(0);
    method(ch, 50, 30, &b)
}

fn queue_delete(ch: u16, queue: &str) -> Frame {
    let mut b = BytesMut::new();
    b.put_u16(0);
    short_str(&mut b, queue);
    b.put_u8(0);
    method(ch, 50, 40, &b)
}

fn basic_publish_to(ch: u16, queue: &str) -> Frame {
    let mut b = BytesMut::new();
    b.put_u16(0);
    short_str(&mut b, ""); // 默认 exchange
    short_str(&mut b, queue);
    b.put_u8(0);
    method(ch, 60, 40, &b)
}

fn get_from(ch: u16, queue: &str, no_ack: bool) -> Frame {
    let mut b = BytesMut::new();
    b.put_u16(0);
    short_str(&mut b, queue);
    b.put_u8(no_ack as u8);
    method(ch, 60, 70, &b)
}

fn consume_from(ch: u16, queue: &str) -> Frame {
    let mut b = BytesMut::new();
    b.put_u16(0);
    short_str(&mut b, queue);
    short_str(&mut b, ""); // consumer tag 由 Broker 生成
    b.put_u8(0); // 需要 ack
    b.put_u32(0);
    method(ch, 60, 20, &b)
}

fn body_frame(ch: u16, body: &'static [u8]) -> Frame {
    Frame::new(FrameType::Body, ch, Bytes::from_static(body))
}

async fn open(client: &mut TestClient, ch: u16) {
    client.send(methods::build_channel_open(ch)).await;
    let f = client.recv().await;
    assert_eq!(ids(&f), (CHANNEL, CHANNEL_OPEN_OK), "{f:?}");
}

async fn expect_method(client: &mut TestClient, class: u16, method: u16) -> Frame {
    let f = client.recv().await;
    assert_eq!(ids(&f), (class, method), "{f:?}");
    f
}

async fn declare(client: &mut TestClient, ch: u16, queue: &str) {
    client.send(queue_declare(ch, queue)).await;
    expect_method(client, 50, 11).await;
}

async fn publish(client: &mut TestClient, ch: u16, queue: &str, body: &'static [u8]) {
    client.send(basic_publish_to(ch, queue)).await;
    client.send(content_header(ch, body.len() as u64)).await;
    client.send(body_frame(ch, body)).await;
}

/// 取一条消息（no_ack）。返回 `None` 表示队列为空。
async fn get(client: &mut TestClient, ch: u16, queue: &str) -> Option<Bytes> {
    client.send(get_from(ch, queue, true)).await;
    let f = client.recv().await;
    match ids(&f) {
        (60, 72) => None,
        (60, 71) => {
            let header = client.recv().await;
            assert_eq!(header.kind, FrameType::Header);
            let body = client.recv().await;
            assert_eq!(body.kind, FrameType::Body);
            Some(body.payload)
        }
        other => panic!("unexpected reply to basic.get: {other:?}"),
    }
}

async fn clean(client: &mut TestClient, ch: u16, queue: &str) {
    client.send(queue_delete(ch, queue)).await;
    expect_method(client, 50, 41).await;
}

#[tokio::test]
async fn publish_and_get_through_the_proxy() {
    let proxy = start_proxy_to(RABBIT, 5672, false).await;
    let mut c = TestClient::connect(proxy, Auth::Amqplain, GUEST_LOGIN)
        .await
        .unwrap();
    open(&mut c, 1).await;
    declare(&mut c, 1, "amqproxy-it-basic").await;
    publish(&mut c, 1, "amqproxy-it-basic", b"hello").await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        get(&mut c, 1, "amqproxy-it-basic").await.as_deref(),
        Some(&b"hello"[..])
    );
    assert_eq!(get(&mut c, 1, "amqproxy-it-basic").await, None);
    clean(&mut c, 1, "amqproxy-it-basic").await;
    c.close().await;
}

#[tokio::test]
async fn a_reused_connection_and_channel_keep_working_against_rabbitmq() {
    let proxy = start_proxy_to(RABBIT, 5672, false).await;
    let queue = "amqproxy-it-reuse";

    let mut c1 = TestClient::connect(proxy, Auth::Plain, GUEST_LOGIN)
        .await
        .unwrap();
    open(&mut c1, 1).await;
    declare(&mut c1, 1, queue).await;
    publish(&mut c1, 1, queue, b"one").await;
    c1.send(methods::build_channel_close(1, 200, "bye")).await;
    expect_method(&mut c1, CHANNEL, methods::CHANNEL_CLOSE_OK).await;
    c1.close().await;

    // 第二个客户端复用上游连接和 channel 1（伪造的 open-ok）。
    let mut c2 = TestClient::connect(proxy, Auth::Plain, GUEST_LOGIN)
        .await
        .unwrap();
    open(&mut c2, 1).await;
    publish(&mut c2, 1, queue, b"two").await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(get(&mut c2, 1, queue).await.as_deref(), Some(&b"one"[..]));
    assert_eq!(get(&mut c2, 1, queue).await.as_deref(), Some(&b"two"[..]));
    clean(&mut c2, 1, queue).await;
    c2.close().await;
}

#[tokio::test]
async fn unacknowledged_messages_return_to_the_queue_when_the_consumer_disconnects() {
    let proxy = start_proxy_to(RABBIT, 5672, false).await;
    let queue = "amqproxy-it-requeue";

    let mut c1 = TestClient::connect(proxy, Auth::Plain, GUEST_LOGIN)
        .await
        .unwrap();
    open(&mut c1, 1).await;
    declare(&mut c1, 1, queue).await;
    c1.send(queue_purge(1, queue)).await;
    expect_method(&mut c1, 50, 31).await;
    publish(&mut c1, 1, queue, b"important").await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    c1.send(consume_from(1, queue)).await;
    expect_method(&mut c1, 60, 21).await;
    expect_method(&mut c1, 60, 60).await; // 投递，但是不 ack
    c1.drop_abruptly();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 代理关闭了 consumer 所在的 channel，Broker 把消息放回队列。
    let mut c2 = TestClient::connect(proxy, Auth::Plain, GUEST_LOGIN)
        .await
        .unwrap();
    open(&mut c2, 1).await;
    assert_eq!(
        get(&mut c2, 1, queue).await.as_deref(),
        Some(&b"important"[..])
    );
    clean(&mut c2, 1, queue).await;
    c2.close().await;
}

#[tokio::test]
async fn a_channel_error_from_rabbitmq_does_not_poison_the_next_client() {
    let proxy = start_proxy_to(RABBIT, 5672, false).await;

    let mut c1 = TestClient::connect(proxy, Auth::Plain, GUEST_LOGIN)
        .await
        .unwrap();
    open(&mut c1, 1).await;
    // passive 声明不存在的队列：Broker 以 404 关闭 channel。
    let mut b = BytesMut::new();
    b.put_u16(0);
    short_str(&mut b, "amqproxy-it-does-not-exist");
    b.put_u8(1); // passive
    b.put_u32(0);
    c1.send(method(1, 50, 10, &b)).await;
    let f = expect_method(&mut c1, CHANNEL, methods::CHANNEL_CLOSE).await;
    assert_eq!(methods::parse_channel_close(&f.payload).unwrap().code, 404);
    c1.send(methods::build_channel_close_ok(1)).await;
    c1.close().await;

    let mut c2 = TestClient::connect(proxy, Auth::Plain, GUEST_LOGIN)
        .await
        .unwrap();
    open(&mut c2, 1).await;
    declare(&mut c2, 1, "amqproxy-it-after-error").await;
    clean(&mut c2, 1, "amqproxy-it-after-error").await;
    c2.close().await;
}

#[tokio::test]
async fn a_wrong_password_is_reported_when_connecting() {
    let proxy = start_proxy_to(RABBIT, 5672, false).await;
    let wrong = Creds {
        user: "guest",
        password: "not-the-password",
        vhost: "/",
    };
    let close = TestClient::connect(proxy, Auth::Amqplain, wrong)
        .await
        .err()
        .expect("must fail");
    assert_eq!(close.code, 403, "{close:?}");
    assert!(close.text.starts_with("ACCESS_REFUSED"), "{close:?}");
}

#[tokio::test]
async fn an_unknown_vhost_is_reported_when_connecting() {
    let proxy = start_proxy_to(RABBIT, 5672, false).await;
    let wrong = Creds {
        vhost: "/no-such-vhost",
        ..GUEST_LOGIN
    };
    let close = TestClient::connect(proxy, Auth::Plain, wrong)
        .await
        .err()
        .expect("must fail");
    assert!(close.code == 403 || close.code == 530, "{close:?}");
}

#[tokio::test]
async fn amqps_upstream_works_with_a_private_ca() {
    let ca = concat!(env!("CARGO_MANIFEST_DIR"), "/test/certs/ca.crt");
    // 在第一次 TLS 连接之前设置，TLS 配置只创建一次。
    unsafe { std::env::set_var("SSL_CERT_FILE", ca) };
    let proxy = start_proxy_to("localhost", 5671, true).await;
    let mut c = TestClient::connect(proxy, Auth::Plain, GUEST_LOGIN)
        .await
        .unwrap();
    open(&mut c, 1).await;
    declare(&mut c, 1, "amqproxy-it-tls").await;
    publish(&mut c, 1, "amqproxy-it-tls", b"over tls").await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        get(&mut c, 1, "amqproxy-it-tls").await.as_deref(),
        Some(&b"over tls"[..])
    );
    clean(&mut c, 1, "amqproxy-it-tls").await;
    c.close().await;
}
