use std::io;
use std::time::Duration;

use socket2::{SockRef, TcpKeepalive};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

/// 客户端 socket 和上游 socket（可能带 TLS）使用同一个类型。
pub trait Io: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Io for T {}
pub type BoxIo = Box<dyn Io>;

/// TCP_NODELAY 和 keepalive（60s / 10s / 3 次），与 v0.4.4 相同。
pub fn configure_tcp(stream: &TcpStream) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let keepalive = TcpKeepalive::new()
        .with_time(Duration::from_secs(60))
        .with_interval(Duration::from_secs(10))
        .with_retries(3);
    SockRef::from(stream).set_tcp_keepalive(&keepalive)
}
