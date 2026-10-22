//! 监听、接受连接，并为每个客户端连接启动一个会话。

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};

use crate::client_handshake::{self, ClientConn, Credentials, HandshakeError};
use crate::config::Config;
use crate::frame::write_frame;
use crate::io::{BoxIo, configure_tcp};
use crate::methods;
use crate::session;
use crate::upstream::{self, Target, UpstreamError};

pub struct Server {
    listener: TcpListener,
    target: Arc<Target>,
}

impl Server {
    pub async fn bind(config: Config) -> io::Result<Server> {
        let listener =
            TcpListener::bind((config.listen_address.as_str(), config.listen_port)).await?;
        info!("Proxy listening on {}", listener.local_addr()?);
        let target = Target {
            host: config.upstream_host,
            port: config.upstream_port,
            tls: config.upstream_tls,
        };
        Ok(Server {
            listener,
            target: Arc::new(target),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.listener
            .local_addr()
            .expect("listener has a local address")
    }

    pub async fn run(self) {
        loop {
            match self.listener.accept().await {
                Ok((stream, peer)) => {
                    let target = self.target.clone();
                    tokio::spawn(async move {
                        debug!("Client connected: {peer}");
                        handle_connection(stream, &target).await;
                        debug!("Client disconnected: {peer}");
                    });
                }
                Err(e) => {
                    // 例如文件描述符用完。稍等一下再试，不要空转。
                    error!("Accept failed: {e}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
}

async fn handle_connection(stream: TcpStream, target: &Target) {
    if let Err(e) = configure_tcp(&stream) {
        debug!("Cannot configure client socket: {e}");
    }
    let stream: BoxIo = Box::new(stream);
    let (mut client, creds) = match client_handshake::negotiate(stream).await {
        Ok(v) => v,
        Err(HandshakeError::BadProtocolHeader) => {
            return debug!("Client sent an invalid protocol header");
        }
        Err(e) => return debug!("Client negotiation failed: {e}"),
    };
    match upstream::connect(target, &creds).await {
        Ok(up) => {
            if write_frame(&mut client.writer, &methods::build_open_ok())
                .await
                .is_err()
            {
                return;
            }
            session::run(client, up).await;
        }
        Err(e) => refuse(&mut client, &creds, e).await,
    }
}

async fn refuse(client: &mut ClientConn, creds: &Credentials, err: UpstreamError) {
    let close = match &err {
        UpstreamError::Refused(c) => {
            error!(
                "Access refused for user '{}' to vhost '{}', reason: {}",
                creds.user, creds.vhost, c.text
            );
            methods::build_connection_close(
                methods::ACCESS_REFUSED,
                &format!("ACCESS_REFUSED - {}", c.text),
            )
        }
        other => {
            error!(
                "Upstream error for user '{}' to vhost '{}': {other}",
                creds.user, creds.vhost
            );
            methods::build_connection_close(methods::CONNECTION_FORCED, "UPSTREAM_ERROR")
        }
    };
    let _ = write_frame(&mut client.writer, &close).await;
    let _ = tokio::io::AsyncWriteExt::shutdown(&mut client.writer).await;
}
