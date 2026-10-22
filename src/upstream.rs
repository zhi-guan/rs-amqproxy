//! 到 Broker 的连接：TCP 或 TLS，握手（设计文档 §4.1）。

use std::collections::HashSet;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use futures_util::FutureExt;
use tokio::io::{AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

use crate::client_handshake::{CLIENT_FRAME_MAX, Credentials};
use crate::frame::{Frame, FrameReader, FrameType, ReadError, write_frame};
use crate::io::{BoxIo, configure_tcp};
use crate::methods::{self, Close, Tune};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct Target {
    pub host: String,
    pub port: u16,
    pub tls: bool,
}

#[derive(Debug)]
pub enum UpstreamError {
    Io(io::Error),
    /// Broker 在握手期间发来 `connection.close`（例如认证失败）。
    Refused(Close),
    Protocol(String),
    Timeout,
}

impl std::fmt::Display for UpstreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpstreamError::Io(e) => write!(f, "cannot establish connection to upstream: {e}"),
            UpstreamError::Refused(c) => {
                write!(f, "upstream refused the connection: {} {}", c.code, c.text)
            }
            UpstreamError::Protocol(m) => write!(f, "upstream protocol error: {m}"),
            UpstreamError::Timeout => write!(f, "timed out connecting to upstream"),
        }
    }
}

impl From<io::Error> for UpstreamError {
    fn from(e: io::Error) -> Self {
        UpstreamError::Io(e)
    }
}

impl From<ReadError> for UpstreamError {
    fn from(e: ReadError) -> Self {
        match e {
            ReadError::Io(e) => UpstreamError::Io(e),
            other => UpstreamError::Protocol(other.to_string()),
        }
    }
}

/// 握手完成、可以收发帧的上游连接。
pub struct UpstreamConn {
    pub reader: FrameReader<ReadHalf<BoxIo>>,
    pub writer: WriteHalf<BoxIo>,
    /// 与 Broker 协商出的 frame_max。
    pub frame_max: u32,
    pub channel_max: u16,
    /// Broker 上当前打开的 channel。
    pub open_channels: HashSet<u16>,
    /// 其中留下了状态的 channel（不安全 Channel）。
    pub unsafe_channels: HashSet<u16>,
    /// Broker 发来 `channel.close`、客户端还没有回复 `close-ok` 的 channel。
    pub pending_broker_close: HashSet<u16>,
    pub(crate) counter: Option<ConnCounter>,
}

/// 连接被丢弃时自动减少连接计数，所以任何退出路径都不会漏掉。
pub struct ConnCounter(Arc<AtomicUsize>);

impl ConnCounter {
    pub fn new(count: &Arc<AtomicUsize>) -> ConnCounter {
        count.fetch_add(1, Ordering::Relaxed);
        ConnCounter(count.clone())
    }
}

impl Drop for ConnCounter {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl UpstreamConn {
    /// 处理连接在池中空闲时已经到达的帧（设计文档 §5.1）。
    /// 返回 `false` 表示连接已断开或状态不可信，不能再使用。
    pub async fn poll_idle(&mut self) -> bool {
        loop {
            let frame = match self.reader.read_frame().now_or_never() {
                None => return true,
                Some(Ok(Some(frame))) => frame,
                Some(Ok(None)) | Some(Err(_)) => return false,
            };
            if frame.kind == FrameType::Heartbeat {
                continue;
            }
            match methods::method_ids(&frame.payload) {
                Some((methods::CHANNEL, methods::CHANNEL_CLOSE)) => {
                    self.forget_channel(frame.channel);
                    let ok = methods::build_channel_close_ok(frame.channel);
                    if write_frame(&mut self.writer, &ok).await.is_err() {
                        return false;
                    }
                }
                Some((methods::CHANNEL, methods::CHANNEL_CLOSE_OK)) => {
                    self.forget_channel(frame.channel);
                }
                Some((methods::CONNECTION, methods::CONNECTION_CLOSE)) => {
                    let _ =
                        write_frame(&mut self.writer, &methods::build_connection_close_ok()).await;
                    return false;
                }
                other => {
                    tracing::warn!("Unexpected frame on an idle upstream connection: {other:?}");
                    return false;
                }
            }
        }
    }

    /// Broker 上的这个 channel 已经关闭。
    pub fn forget_channel(&mut self, channel: u16) {
        self.open_channels.remove(&channel);
        self.unsafe_channels.remove(&channel);
    }

    /// 礼貌地关闭上游连接。
    pub async fn close(mut self, reason: &str) {
        let close = methods::build_connection_close(methods::REPLY_SUCCESS, reason);
        let _ = write_frame(&mut self.writer, &close).await;
        let _ = self.writer.shutdown().await;
    }
}

/// 所有 TLS 连接共用一个配置，节省内存。
fn tls_config() -> Arc<ClientConfig> {
    static CONFIG: OnceLock<Arc<ClientConfig>> = OnceLock::new();
    CONFIG
        .get_or_init(|| {
            let mut roots = RootCertStore::empty();
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            Arc::new(
                ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth(),
            )
        })
        .clone()
}

pub async fn connect(target: &Target, creds: &Credentials) -> Result<UpstreamConn, UpstreamError> {
    tokio::time::timeout(CONNECT_TIMEOUT, connect_inner(target, creds))
        .await
        .map_err(|_| UpstreamError::Timeout)?
}

async fn connect_inner(
    target: &Target,
    creds: &Credentials,
) -> Result<UpstreamConn, UpstreamError> {
    let tcp = TcpStream::connect((target.host.as_str(), target.port)).await?;
    configure_tcp(&tcp)?;
    let stream: BoxIo = if target.tls {
        let name = ServerName::try_from(target.host.clone())
            .map_err(|e| UpstreamError::Protocol(format!("invalid TLS server name: {e}")))?;
        Box::new(TlsConnector::from(tls_config()).connect(name, tcp).await?)
    } else {
        Box::new(tcp)
    };
    handshake(stream, creds).await
}

async fn handshake(stream: BoxIo, creds: &Credentials) -> Result<UpstreamConn, UpstreamError> {
    let (r, mut writer) = tokio::io::split(stream);
    let mut reader = FrameReader::new(r, 16 * 1024);
    writer.write_all(&methods::PROTOCOL_HEADER).await?;

    let start = next_method(&mut reader).await?;
    expect(&start, methods::CONNECTION_START, "connection.start")?;
    write_frame(
        &mut writer,
        &methods::build_start_ok_plain(&creds.user, &creds.password),
    )
    .await?;

    let tune = next_method(&mut reader).await?;
    let tune = match methods::method_ids(&tune.payload) {
        Some((methods::CONNECTION, methods::CONNECTION_CLOSE)) => {
            return refused(&mut writer, &tune).await;
        }
        _ => {
            methods::parse_tune(&tune.payload).map_err(|_| protocol("expected connection.tune"))?
        }
    };
    // frame_max 为 0 表示 Broker 不限制，这时用我们给客户端的值。
    let frame_max = match tune.frame_max {
        0 => CLIENT_FRAME_MAX,
        n => n.min(CLIENT_FRAME_MAX),
    };
    let tune_ok = Tune {
        channel_max: tune.channel_max,
        frame_max,
        heartbeat: 0,
    };
    write_frame(&mut writer, &methods::build_tune_ok(tune_ok)).await?;
    write_frame(&mut writer, &methods::build_open(&creds.vhost)).await?;

    let reply = next_method(&mut reader).await?;
    match methods::method_ids(&reply.payload) {
        Some((methods::CONNECTION, methods::CONNECTION_OPEN_OK)) => {}
        Some((methods::CONNECTION, methods::CONNECTION_CLOSE)) => {
            return refused(&mut writer, &reply).await;
        }
        _ => return Err(protocol("expected connection.open-ok")),
    }
    reader.set_frame_max(frame_max);
    Ok(UpstreamConn {
        reader,
        writer,
        frame_max,
        channel_max: tune.channel_max,
        open_channels: HashSet::new(),
        unsafe_channels: HashSet::new(),
        pending_broker_close: HashSet::new(),
        counter: None,
    })
}

async fn refused<W: tokio::io::AsyncWrite + Unpin>(
    w: &mut W,
    close: &Frame,
) -> Result<UpstreamConn, UpstreamError> {
    let close = methods::parse_connection_close(&close.payload)
        .map_err(|_| protocol("malformed connection.close"))?;
    let _ = write_frame(w, &methods::build_connection_close_ok()).await;
    let _ = w.shutdown().await;
    Err(UpstreamError::Refused(close))
}

fn protocol(msg: &str) -> UpstreamError {
    UpstreamError::Protocol(msg.to_string())
}

fn expect(frame: &Frame, method: u16, name: &str) -> Result<(), UpstreamError> {
    match methods::method_ids(&frame.payload) {
        Some((methods::CONNECTION, m)) if m == method => Ok(()),
        _ => Err(UpstreamError::Protocol(format!("expected {name}"))),
    }
}

async fn next_method<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut FrameReader<R>,
) -> Result<Frame, UpstreamError> {
    match reader.read_frame().await? {
        Some(f) if f.kind == FrameType::Method => Ok(f),
        Some(_) => Err(protocol("expected a method frame during the handshake")),
        None => Err(UpstreamError::Io(io::ErrorKind::UnexpectedEof.into())),
    }
}
