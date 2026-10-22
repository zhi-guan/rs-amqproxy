//! 测试脚手架：脚本化的假 Broker 和测试用 AMQP 客户端。
//! 所有测试都只通过 TCP 与代理交互（唯一的 seam）。

#![allow(dead_code)]

use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;

use amqproxy::config::Config;
use amqproxy::frame::{Frame, FrameReader, FrameType};
use amqproxy::methods::{self, Close, Tune};
use amqproxy::server::Server;
use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};

pub const TIMEOUT: Duration = Duration::from_secs(5);

pub async fn within<T>(what: &str, fut: impl Future<Output = T>) -> T {
    tokio::time::timeout(TIMEOUT, fut)
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
}

pub fn method(channel: u16, class: u16, method: u16, extra: &[u8]) -> Frame {
    let mut b = BytesMut::new();
    b.put_u16(class);
    b.put_u16(method);
    b.put_slice(extra);
    Frame::new(FrameType::Method, channel, b.freeze())
}

pub fn ids(frame: &Frame) -> (u16, u16) {
    assert_eq!(
        frame.kind,
        FrameType::Method,
        "expected a method frame: {frame:?}"
    );
    methods::method_ids(&frame.payload).expect("method ids")
}

// ---------- 假 Broker ----------

pub struct FakeBroker {
    listener: TcpListener,
    pub addr: SocketAddr,
}

pub struct BrokerConn {
    pub user: String,
    pub password: String,
    pub vhost: String,
    reader: FrameReader<ReadHalf<TcpStream>>,
    writer: WriteHalf<TcpStream>,
}

impl FakeBroker {
    pub async fn start() -> FakeBroker {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        FakeBroker { listener, addr }
    }

    /// 等待代理连接并完成握手（成功）。
    pub async fn accept(&self) -> BrokerConn {
        self.accept_with(None)
            .await
            .expect("handshake should succeed")
    }

    /// 等待代理连接；握手到 `open` 时回复 `connection.close(code, text)`。
    pub async fn accept_rejecting(&self, code: u16, text: &str) {
        let r = self.accept_with(Some((code, text.to_string()))).await;
        assert!(r.is_none());
    }

    async fn accept_with(&self, reject: Option<(u16, String)>) -> Option<BrokerConn> {
        let (stream, _) = within("proxy to connect to the broker", self.listener.accept())
            .await
            .unwrap();
        let (r, mut writer) = tokio::io::split(stream);
        let mut r = r;
        let mut header = [0u8; 8];
        within("protocol header", r.read_exact(&mut header))
            .await
            .unwrap();
        assert_eq!(header, methods::PROTOCOL_HEADER);
        let mut reader = FrameReader::new(r, 131_072);
        send(&mut writer, &methods::build_start()).await;
        let start_ok = within("start-ok", reader.read_frame())
            .await
            .unwrap()
            .unwrap();
        let start_ok = methods::parse_start_ok(&start_ok.payload).unwrap();
        assert_eq!(start_ok.mechanism, "PLAIN");
        let (user, password) = methods::parse_plain_credentials(&start_ok.response).unwrap();
        send(
            &mut writer,
            &methods::build_tune(Tune {
                channel_max: 2047,
                frame_max: 131_072,
                heartbeat: 0,
            }),
        )
        .await;
        let tune_ok = within("tune-ok", reader.read_frame())
            .await
            .unwrap()
            .unwrap();
        methods::parse_tune_ok(&tune_ok.payload).unwrap();
        let open = within("open", reader.read_frame()).await.unwrap().unwrap();
        let open = methods::parse_open(&open.payload).unwrap();
        if let Some((code, text)) = reject {
            send(&mut writer, &methods::build_connection_close(code, &text)).await;
            let ok = within("close-ok", reader.read_frame())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                ids(&ok),
                (methods::CONNECTION, methods::CONNECTION_CLOSE_OK)
            );
            return None;
        }
        send(&mut writer, &methods::build_open_ok()).await;
        Some(BrokerConn {
            user,
            password,
            vhost: open.vhost,
            reader,
            writer,
        })
    }
}

async fn send(w: &mut (impl AsyncWriteExt + Unpin), frame: &Frame) {
    let mut b = BytesMut::new();
    frame.encode(&mut b);
    w.write_all(&b).await.unwrap();
}

impl BrokerConn {
    pub async fn recv(&mut self) -> Frame {
        within("frame from proxy", self.reader.read_frame())
            .await
            .expect("read error")
            .expect("proxy closed the upstream connection")
    }

    /// 返回 `None` 表示代理关闭了连接。
    pub async fn recv_or_eof(&mut self) -> Option<Frame> {
        within("frame or EOF", self.reader.read_frame())
            .await
            .unwrap_or(None)
    }

    pub async fn send(&mut self, frame: Frame) {
        send(&mut self.writer, &frame).await;
    }

    /// 在 `d` 时间内没有收到任何帧，也没有被关闭。
    pub async fn expect_quiet_for(&mut self, d: Duration) {
        if let Ok(f) = tokio::time::timeout(d, self.recv_or_eof()).await {
            panic!("expected silence, got {f:?}");
        }
    }

    pub async fn expect_closed(&mut self) {
        // 代理可能先发 connection.close 再关闭。
        while let Some(f) = self.recv_or_eof().await {
            if f.kind == FrameType::Method
                && ids(&f) == (methods::CONNECTION, methods::CONNECTION_CLOSE)
            {
                self.send(methods::build_connection_close_ok()).await;
            }
        }
    }
}

// ---------- 测试客户端 ----------

#[derive(Clone, Copy)]
pub enum Auth {
    Plain,
    Amqplain,
}

pub struct TestClient {
    reader: FrameReader<ReadHalf<TcpStream>>,
    writer: WriteHalf<TcpStream>,
}

pub struct Creds<'a> {
    pub user: &'a str,
    pub password: &'a str,
    pub vhost: &'a str,
}

fn start_ok(auth: Auth, user: &str, password: &str) -> Frame {
    let mut b = BytesMut::new();
    b.put_u16(methods::CONNECTION);
    b.put_u16(methods::CONNECTION_START_OK);
    b.put_u32(0); // client-properties: 空表
    let response: Vec<u8> = match auth {
        Auth::Plain => [&[0][..], user.as_bytes(), &[0], password.as_bytes()].concat(),
        Auth::Amqplain => {
            let mut t = Vec::new();
            for (k, v) in [("LOGIN", user), ("PASSWORD", password)] {
                t.push(k.len() as u8);
                t.extend_from_slice(k.as_bytes());
                t.push(b'S');
                t.extend_from_slice(&(v.len() as u32).to_be_bytes());
                t.extend_from_slice(v.as_bytes());
            }
            t
        }
    };
    let mechanism = match auth {
        Auth::Plain => "PLAIN",
        Auth::Amqplain => "AMQPLAIN",
    };
    b.put_u8(mechanism.len() as u8);
    b.put_slice(mechanism.as_bytes());
    b.put_u32(response.len() as u32);
    b.put_slice(&response);
    b.put_u8(5);
    b.put_slice(b"en_US");
    Frame::new(FrameType::Method, 0, b.freeze())
}

impl TestClient {
    /// 连接代理并完成握手。代理回复 `connection.close` 时返回 `Err`。
    pub async fn connect(proxy: SocketAddr, auth: Auth, c: Creds<'_>) -> Result<TestClient, Close> {
        let stream = TcpStream::connect(proxy).await.unwrap();
        let (r, mut writer) = tokio::io::split(stream);
        writer.write_all(&methods::PROTOCOL_HEADER).await.unwrap();
        let mut reader = FrameReader::new(r, 131_072);
        let start = within("start", reader.read_frame()).await.unwrap().unwrap();
        assert_eq!(
            ids(&start),
            (methods::CONNECTION, methods::CONNECTION_START)
        );
        send(&mut writer, &start_ok(auth, c.user, c.password)).await;
        let tune = within("tune", reader.read_frame()).await.unwrap().unwrap();
        let tune = methods::parse_tune(&tune.payload).unwrap();
        assert_eq!(
            tune,
            Tune {
                channel_max: 0,
                frame_max: 131_072,
                heartbeat: 0
            }
        );
        send(&mut writer, &methods::build_tune_ok(tune)).await;
        send(&mut writer, &methods::build_open(c.vhost)).await;
        let reply = within("open-ok", reader.read_frame())
            .await
            .unwrap()
            .unwrap();
        match ids(&reply) {
            (methods::CONNECTION, methods::CONNECTION_OPEN_OK) => Ok(TestClient { reader, writer }),
            (methods::CONNECTION, methods::CONNECTION_CLOSE) => {
                send(&mut writer, &methods::build_connection_close_ok()).await;
                Err(methods::parse_connection_close(&reply.payload).unwrap())
            }
            other => panic!("unexpected reply to connection.open: {other:?}"),
        }
    }

    pub async fn send(&mut self, frame: Frame) {
        send(&mut self.writer, &frame).await;
    }

    pub async fn recv(&mut self) -> Frame {
        within("frame from proxy", self.reader.read_frame())
            .await
            .expect("read error")
            .expect("proxy closed the client connection")
    }

    pub async fn recv_or_eof(&mut self) -> Option<Frame> {
        within("frame or EOF", self.reader.read_frame())
            .await
            .unwrap_or(None)
    }

    /// 直接断开 TCP 连接（不发 `connection.close`）。
    pub fn drop_abruptly(self) {}

    /// 正常关闭：发送 `connection.close`，等待 `close-ok` 和 EOF。
    pub async fn close(mut self) {
        self.send(methods::build_connection_close(200, "bye")).await;
        let reply = self.recv().await;
        assert_eq!(
            ids(&reply),
            (methods::CONNECTION, methods::CONNECTION_CLOSE_OK)
        );
        assert!(self.recv_or_eof().await.is_none());
    }
}

// ---------- 代理 ----------

pub async fn start_proxy(upstream: SocketAddr) -> SocketAddr {
    start_proxy_with_idle(upstream, Duration::from_secs(5)).await
}

pub async fn start_proxy_with_idle(upstream: SocketAddr, idle: Duration) -> SocketAddr {
    let config = Config {
        listen_address: "127.0.0.1".into(),
        listen_port: 0,
        debug: false,
        idle_connection_timeout: idle,
        upstream_host: upstream.ip().to_string(),
        upstream_port: upstream.port(),
        upstream_tls: false,
    };
    let server = Server::bind(config).await.unwrap();
    let addr = server.local_addr();
    tokio::spawn(server.run());
    addr
}

pub const GUEST: Creds<'static> = Creds {
    user: "guest",
    password: "secret",
    vhost: "/",
};
