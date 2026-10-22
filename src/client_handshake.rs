//! 与客户端握手：协议头、start / tune / open（设计文档 §4）。

use std::io;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};

use crate::frame::{Frame, FrameReader, FrameType, ReadError, write_frame};
use crate::io::BoxIo;
use crate::methods::{self, Tune};

/// 给客户端的 frame_max。
pub const CLIENT_FRAME_MAX: u32 = 131_072;
/// 握手期间的帧上限，防止没有认证的客户端占用内存。
const HANDSHAKE_FRAME_MAX: u32 = 16 * 1024;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// 池 Key：只有凭证和 vhost 都相同的客户端才能共用上游连接。
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Credentials {
    pub user: String,
    pub password: String,
    pub vhost: String,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("user", &self.user)
            .field("vhost", &self.vhost)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub enum HandshakeError {
    Io(io::Error),
    BadProtocolHeader,
    Protocol(String),
    Timeout,
}

impl std::fmt::Display for HandshakeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HandshakeError::Io(e) => write!(f, "I/O error: {e}"),
            HandshakeError::BadProtocolHeader => write!(f, "invalid protocol header"),
            HandshakeError::Protocol(m) => write!(f, "{m}"),
            HandshakeError::Timeout => write!(f, "handshake timed out"),
        }
    }
}

impl From<io::Error> for HandshakeError {
    fn from(e: io::Error) -> Self {
        HandshakeError::Io(e)
    }
}

impl From<ReadError> for HandshakeError {
    fn from(e: ReadError) -> Self {
        match e {
            ReadError::Io(e) => HandshakeError::Io(e),
            other => HandshakeError::Protocol(other.to_string()),
        }
    }
}

/// 握手完成后的客户端连接。
pub struct ClientConn {
    pub reader: FrameReader<ReadHalf<BoxIo>>,
    pub writer: WriteHalf<BoxIo>,
}

/// 握手到收到 `connection.open` 为止。**不会**回复 `open-ok`，由调用方在
/// 借出上游连接成功后再回复（ADR-0002 第 6 项）。
pub async fn negotiate(stream: BoxIo) -> Result<(ClientConn, Credentials), HandshakeError> {
    tokio::time::timeout(HANDSHAKE_TIMEOUT, negotiate_inner(stream))
        .await
        .map_err(|_| HandshakeError::Timeout)?
}

async fn negotiate_inner(stream: BoxIo) -> Result<(ClientConn, Credentials), HandshakeError> {
    let (mut r, mut writer) = tokio::io::split(stream);
    let mut header = [0u8; 8];
    r.read_exact(&mut header).await?;
    if header != methods::PROTOCOL_HEADER && header != methods::PROTOCOL_HEADER_0_9 {
        writer.write_all(&methods::PROTOCOL_HEADER).await?;
        let _ = writer.shutdown().await;
        return Err(HandshakeError::BadProtocolHeader);
    }
    let mut reader = FrameReader::new(r, HANDSHAKE_FRAME_MAX);

    write_frame(&mut writer, &methods::build_start()).await?;
    let frame = next_method(&mut reader).await?;
    let start_ok = methods::parse_start_ok(&frame.payload)
        .map_err(|_| protocol("expected connection.start-ok"))?;
    let (user, password) = match start_ok.mechanism.as_str() {
        "PLAIN" => methods::parse_plain_credentials(&start_ok.response),
        "AMQPLAIN" => methods::parse_amqplain_credentials(&start_ok.response),
        other => {
            return Err(protocol(format!(
                "unsupported authentication mechanism: {other}"
            )));
        }
    }
    .map_err(|_| protocol("malformed credentials"))?;

    let tune = Tune {
        channel_max: 0,
        frame_max: CLIENT_FRAME_MAX,
        heartbeat: 0,
    };
    write_frame(&mut writer, &methods::build_tune(tune)).await?;
    let frame = next_method(&mut reader).await?;
    methods::parse_tune_ok(&frame.payload).map_err(|_| protocol("expected connection.tune-ok"))?;

    let frame = next_method(&mut reader).await?;
    let open =
        methods::parse_open(&frame.payload).map_err(|_| protocol("expected connection.open"))?;

    reader.set_frame_max(CLIENT_FRAME_MAX);
    Ok((
        ClientConn { reader, writer },
        Credentials {
            user,
            password,
            vhost: open.vhost,
        },
    ))
}

fn protocol(msg: impl Into<String>) -> HandshakeError {
    HandshakeError::Protocol(msg.into())
}

async fn next_method<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut FrameReader<R>,
) -> Result<Frame, HandshakeError> {
    match reader.read_frame().await? {
        Some(f) if f.kind == FrameType::Method => Ok(f),
        Some(_) => Err(protocol("expected a method frame during the handshake")),
        None => Err(HandshakeError::Io(io::ErrorKind::UnexpectedEof.into())),
    }
}
