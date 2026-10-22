//! 连接池：按池 Key 保存空闲的上游连接（设计文档 §4.2）。

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tracing::debug;

use crate::client_handshake::Credentials;
use crate::upstream::{self, ConnCounter, Target, UpstreamConn, UpstreamError};

struct Idle {
    conn: UpstreamConn,
    since: Instant,
}

pub struct Pool {
    target: Target,
    idle_timeout: Duration,
    idle: Mutex<HashMap<Credentials, VecDeque<Idle>>>,
    /// 所有上游连接的数量（借出的 + 空闲的）。
    upstreams: Arc<AtomicUsize>,
}

impl Pool {
    pub fn new(target: Target, idle_timeout: Duration) -> Pool {
        Pool {
            target,
            idle_timeout,
            idle: Mutex::new(HashMap::new()),
            upstreams: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn upstream_count(&self) -> usize {
        self.upstreams.load(Ordering::Relaxed)
    }

    /// 借出一个上游连接：优先用池中的空闲连接，没有就新建。
    pub async fn borrow(&self, creds: &Credentials) -> Result<UpstreamConn, UpstreamError> {
        while let Some(mut conn) = self.pop_idle(creds) {
            if conn.poll_idle().await {
                return Ok(conn);
            }
            debug!("Dropping a dead pooled upstream connection");
        }
        let mut conn = upstream::connect(&self.target, creds).await?;
        conn.counter = Some(ConnCounter::new(&self.upstreams));
        Ok(conn)
    }

    /// 归还一个状态干净的上游连接。
    pub fn give_back(&self, creds: &Credentials, conn: UpstreamConn) {
        let mut idle = self.idle.lock().unwrap();
        idle.entry(creds.clone()).or_default().push_back(Idle {
            conn,
            since: Instant::now(),
        });
    }

    fn pop_idle(&self, creds: &Credentials) -> Option<UpstreamConn> {
        let mut idle = self.idle.lock().unwrap();
        let queue = idle.get_mut(creds)?;
        // 后进先出：最近用过的连接最不容易已经失效，其余的会先过期。
        let conn = queue.pop_back().map(|i| i.conn);
        if queue.is_empty() {
            idle.remove(creds);
        }
        conn
    }

    /// 清理池：关闭已死的和空闲超时的连接。定期调用。
    pub async fn reap(&self) {
        let taken: Vec<(Credentials, Idle)> = {
            let mut idle = self.idle.lock().unwrap();
            let all = std::mem::take(&mut *idle);
            all.into_iter()
                .flat_map(|(k, q)| q.into_iter().map(move |i| (k.clone(), i)))
                .collect()
        };
        let mut keep = Vec::with_capacity(taken.len());
        for (creds, mut item) in taken {
            if !(item.conn.poll_idle().await && item.conn.heartbeat_tick().await) {
                debug!("Dropping a dead pooled upstream connection");
            } else if item.since.elapsed() >= self.idle_timeout {
                tokio::spawn(
                    item.conn
                        .close("Pooled connection closed due to inactivity"),
                );
            } else {
                keep.push((creds, item));
            }
        }
        let mut idle = self.idle.lock().unwrap();
        for (creds, item) in keep {
            idle.entry(creds).or_default().push_back(item);
        }
    }

    /// 关闭池中所有的空闲连接（关闭代理时调用）。
    pub async fn close_all(&self) {
        let all = std::mem::take(&mut *self.idle.lock().unwrap());
        for item in all.into_values().flatten() {
            item.conn.close("AMQProxy shutdown").await;
        }
    }

    /// 清理的间隔：不超过空闲超时的四分之一，最长 1 秒。
    pub fn reap_interval(&self) -> Duration {
        (self.idle_timeout / 4).clamp(Duration::from_millis(50), Duration::from_secs(1))
    }
}
