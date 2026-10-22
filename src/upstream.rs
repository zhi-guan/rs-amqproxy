//! 到 Broker 的连接：TCP 或 TLS，握手（设计文档 §4.1）。

use std::io;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

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
