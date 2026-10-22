//! 监听、接受连接，并为每个客户端连接启动一个会话。

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tracing::{debug, error, info};

use crate::client_handshake::{self, ClientConn, Credentials, HandshakeError};
use crate::config::Config;
use crate::frame::write_frame;
use crate::io::{BoxIo, configure_tcp};
use crate::methods;
use crate::pool::Pool;
use crate::session;
use crate::upstream::{Target, UpstreamError};

const STATS_INTERVAL: Duration = Duration::from_secs(5);

struct Shared {
    pool: Pool,
    clients: Arc<AtomicUsize>,
    /// 关闭时发出 `true`，所有会话据此通知客户端并结束。
    shutdown: watch::Sender<bool>,
}

/// 等待会话结束的最长时间。
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

pub struct Server {
    listener: TcpListener,
    shared: Arc<Shared>,
}

/// 客户端连接的计数，丢弃时自动减少。
struct ClientGuard(Arc<AtomicUsize>);

impl ClientGuard {
    fn new(count: &Arc<AtomicUsize>) -> ClientGuard {
        count.fetch_add(1, Ordering::Relaxed);
        ClientGuard(count.clone())
    }
}

impl Drop for ClientGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
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
        let shared = Shared {
            pool: Pool::new(target, config.idle_connection_timeout),
            clients: Arc::new(AtomicUsize::new(0)),
            shutdown: watch::channel(false).0,
        };
        Ok(Server {
            listener,
            shared: Arc::new(shared),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.listener
            .local_addr()
            .expect("listener has a local address")
    }

    /// 一直运行。
    pub async fn run(self) {
        self.run_until(std::future::pending()).await;
    }

    /// 运行到 `shutdown` 完成，然后优雅关闭：停止接受连接，通知所有客户端，
    /// 关闭所有上游连接。
    pub async fn run_until(self, shutdown: impl Future<Output = ()>) {
        let background = [
            tokio::spawn(reap_loop(self.shared.clone())),
            tokio::spawn(stats_loop(self.shared.clone())),
        ];
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                accepted = self.listener.accept() => match accepted {
                    Ok((stream, peer)) => {
                        let shared = self.shared.clone();
                        tokio::spawn(async move {
                            let _guard = ClientGuard::new(&shared.clients);
                            debug!("Client connected: {peer}");
                            handle_connection(stream, &shared).await;
                            debug!("Client disconnected: {peer}");
                        });
                    }
                    Err(e) => {
                        // 例如文件描述符用完。稍等一下再试，不要空转。
                        error!("Accept failed: {e}");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                },
            }
        }
        info!("Proxy stopping accepting connections");
        drop(self.listener);
        for task in &background {
            task.abort();
        }
        let _ = self.shared.shutdown.send(true);
        if tokio::time::timeout(SHUTDOWN_GRACE, self.shared.pool.close_all())
            .await
            .is_err()
        {
            error!("Timed out closing pooled upstream connections");
        }
        let drained = async {
            while self.shared.clients.load(Ordering::Relaxed) > 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        if tokio::time::timeout(SHUTDOWN_GRACE, drained).await.is_err() {
            error!("Some clients did not disconnect in time");
        }
    }
}

async fn reap_loop(shared: Arc<Shared>) {
    let mut tick = tokio::time::interval(shared.pool.reap_interval());
    loop {
        tick.tick().await;
        shared.pool.reap().await;
    }
}

async fn stats_loop(shared: Arc<Shared>) {
    let mut tick = tokio::time::interval(STATS_INTERVAL);
    tick.tick().await;
    loop {
        tick.tick().await;
        info!(
            "clients={} upstreams={} rss={}MB",
            shared.clients.load(Ordering::Relaxed),
            shared.pool.upstream_count(),
            rss_mb().map_or_else(|| "?".to_string(), |v| v.to_string()),
        );
    }
}

/// 进程的常驻内存（MB），读取自 /proc/self/status。
fn rss_mb() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmRSS:"))?;
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb / 1024)
}

async fn handle_connection(stream: TcpStream, shared: &Shared) {
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
    match shared.pool.borrow(&creds).await {
        Ok(up) => {
            if write_frame(&mut client.writer, &methods::build_open_ok())
                .await
                .is_err()
            {
                // 上游连接还是干净的，直接放回池中。
                shared.pool.give_back(&creds, up);
                return;
            }
            if let Some(up) = session::run(&mut client, up, shared.shutdown.subscribe()).await {
                shared.pool.give_back(&creds, up);
            }
            let _ = client.writer.shutdown().await;
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
    let _ = client.writer.shutdown().await;
}
