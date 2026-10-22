//! 端到端透传（#4、#5、#6）：客户端 ⇄ 代理 ⇄ 假 Broker。

mod support;

use amqproxy::frame::{Frame, FrameType};
use amqproxy::methods::{self, CONNECTION, CONNECTION_CLOSE, CONNECTION_CLOSE_OK};
use bytes::Bytes;
use support::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::test]
async fn client_credentials_reach_the_broker_with_plain_auth() {
    let broker = FakeBroker::start().await;
    let proxy = start_proxy(broker.addr).await;
    let creds = Creds {
        user: "alice",
        password: "pw1",
        vhost: "/prod",
    };
    let (client, conn) = tokio::join!(
        TestClient::connect(proxy, Auth::Plain, creds),
        broker.accept()
    );
    client.unwrap();
    assert_eq!(
        (
            conn.user.as_str(),
            conn.password.as_str(),
            conn.vhost.as_str()
        ),
        ("alice", "pw1", "/prod")
    );
}

#[tokio::test]
async fn amqplain_auth_is_translated_to_plain_for_the_broker() {
    let broker = FakeBroker::start().await;
    let proxy = start_proxy(broker.addr).await;
    let creds = Creds {
        user: "bob",
        password: "pw2",
        vhost: "v",
    };
    let (client, conn) = tokio::join!(
        TestClient::connect(proxy, Auth::Amqplain, creds),
        broker.accept()
    );
    client.unwrap();
    assert_eq!((conn.user.as_str(), conn.password.as_str()), ("bob", "pw2"));
}

#[tokio::test]
async fn frames_are_forwarded_unchanged_in_both_directions() {
    let broker = FakeBroker::start().await;
    let proxy = start_proxy(broker.addr).await;
    let (client, conn) = tokio::join!(
        TestClient::connect(proxy, Auth::Plain, GUEST),
        broker.accept()
    );
    let (mut client, mut conn) = (client.unwrap(), conn);

    let publish = method(1, 60, 40, b"\x00\x00\x00\x00\x00");
    let header = content_header(1, 100_000);
    let body = Frame::new(FrameType::Body, 1, Bytes::from(vec![7u8; 100_000]));
    for f in [&publish, &header, &body] {
        client.send(f.clone()).await;
    }
    assert_eq!(conn.recv().await, publish);
    assert_eq!(conn.recv().await, header);
    assert_eq!(conn.recv().await, body);

    let deliver = method(1, 60, 60, b"deliver");
    conn.send(deliver.clone()).await;
    assert_eq!(client.recv().await, deliver);
}

#[tokio::test]
async fn rejected_credentials_make_the_client_connect_fail() {
    let broker = FakeBroker::start().await;
    let proxy = start_proxy(broker.addr).await;
    let creds = Creds {
        user: "guest",
        password: "wrong",
        vhost: "/",
    };
    let (client, _) = tokio::join!(
        TestClient::connect(proxy, Auth::Plain, creds),
        broker.accept_rejecting(403, "no way")
    );
    let close = client.err().expect("connect must fail");
    assert_eq!(close.code, 403);
    assert!(close.text.starts_with("ACCESS_REFUSED"), "{}", close.text);
    assert!(close.text.contains("no way"), "{}", close.text);
}

#[tokio::test]
async fn unreachable_broker_makes_the_client_connect_fail() {
    let dead = FakeBroker::start().await;
    let addr = dead.addr;
    drop(dead);
    let proxy = start_proxy(addr).await;
    let close = TestClient::connect(proxy, Auth::Plain, GUEST)
        .await
        .err()
        .expect("connect must fail");
    assert_eq!(close.code, 320);
    assert!(close.text.contains("UPSTREAM_ERROR"), "{}", close.text);
}

#[tokio::test]
async fn client_close_is_answered_and_the_upstream_connection_stays_open_for_reuse() {
    let broker = FakeBroker::start().await;
    let proxy = start_proxy(broker.addr).await;
    let (client, conn) = tokio::join!(
        TestClient::connect(proxy, Auth::Plain, GUEST),
        broker.accept()
    );
    let (mut client, mut conn) = (client.unwrap(), conn);

    client
        .send(methods::build_connection_close(200, "bye"))
        .await;
    let reply = client.recv().await;
    assert_eq!(ids(&reply), (CONNECTION, CONNECTION_CLOSE_OK));
    assert!(
        client.recv_or_eof().await.is_none(),
        "proxy must close the client socket"
    );
    conn.expect_quiet_for(std::time::Duration::from_millis(300))
        .await;
}

#[tokio::test]
async fn abrupt_client_disconnect_leaves_the_clean_upstream_connection_pooled() {
    let broker = FakeBroker::start().await;
    let proxy = start_proxy(broker.addr).await;
    let (client, conn) = tokio::join!(
        TestClient::connect(proxy, Auth::Plain, GUEST),
        broker.accept()
    );
    let (client, mut conn) = (client.unwrap(), conn);
    client.drop_abruptly();
    conn.expect_quiet_for(std::time::Duration::from_millis(300))
        .await;
}

#[tokio::test]
async fn broker_disconnect_is_reported_to_the_client_as_upstream_error() {
    let broker = FakeBroker::start().await;
    let proxy = start_proxy(broker.addr).await;
    let (client, conn) = tokio::join!(
        TestClient::connect(proxy, Auth::Plain, GUEST),
        broker.accept()
    );
    let (mut client, conn) = (client.unwrap(), conn);
    drop(conn);
    let f = client.recv().await;
    assert_eq!(ids(&f), (CONNECTION, CONNECTION_CLOSE));
    let close = methods::parse_connection_close(&f.payload).unwrap();
    assert_eq!(close.code, 320);
    assert!(close.text.contains("UPSTREAM_ERROR"));
}

#[tokio::test]
async fn client_heartbeats_are_echoed() {
    let broker = FakeBroker::start().await;
    let proxy = start_proxy(broker.addr).await;
    let (client, _conn) = tokio::join!(
        TestClient::connect(proxy, Auth::Plain, GUEST),
        broker.accept()
    );
    let mut client = client.unwrap();
    client.send(Frame::heartbeat()).await;
    assert_eq!(client.recv().await, Frame::heartbeat());
}

#[tokio::test]
async fn invalid_protocol_header_gets_the_supported_header_and_a_closed_socket() {
    let broker = FakeBroker::start().await;
    let proxy = start_proxy(broker.addr).await;
    let mut s = TcpStream::connect(proxy).await.unwrap();
    s.write_all(b"GET / HT").await.unwrap();
    let mut got = Vec::new();
    within("close", s.read_to_end(&mut got)).await.unwrap();
    assert_eq!(got, methods::PROTOCOL_HEADER);
}
