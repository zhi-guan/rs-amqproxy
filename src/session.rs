//! 一个客户端连接和它绑定的上游连接之间的双向转发。
//!
//! 整个会话在一个任务里用 `select!` 完成：写一侧时不读另一侧，所以缓冲区
//! 不会无限增长（背压），也不需要锁。

use tracing::{debug, error};

use crate::client_handshake::ClientConn;
use crate::frame::{Frame, FrameType, write_frame};
use crate::methods::{self, CONNECTION, CONNECTION_CLOSE};
use crate::upstream::UpstreamConn;

/// 运行会话，直到任一侧结束。如果上游连接仍然干净、可以复用，就返回它；
/// 否则关闭它并返回 `None`。
pub async fn run(client: &mut ClientConn, mut up: UpstreamConn) -> Option<UpstreamConn> {
    let reusable = forward(client, &mut up).await;
    if reusable && up.open_channels.is_empty() {
        return Some(up);
    }
    up.close("").await;
    None
}

/// 返回 `true` 表示会话因为客户端结束，上游连接没有问题。
async fn forward(client: &mut ClientConn, up: &mut UpstreamConn) -> bool {
    loop {
        tokio::select! {
            frame = client.reader.read_frame() => match frame {
                Ok(Some(frame)) => {
                    if let Some(end) = from_client(client, up, frame).await {
                        return end;
                    }
                }
                Ok(None) => { debug!("Client disconnected"); return true; }
                Err(e) => { debug!("Client read error: {e}"); return true; }
            },
            frame = up.reader.read_frame() => match frame {
                Ok(Some(frame)) => {
                    if !from_upstream(client, up, frame).await {
                        return false;
                    }
                }
                Ok(None) => { upstream_gone(client, "closed the connection").await; return false; }
                Err(e) => { upstream_gone(client, &e.to_string()).await; return false; }
            },
        }
    }
}

/// `None` 表示继续；`Some(reusable)` 表示会话结束。
async fn from_client(client: &mut ClientConn, up: &mut UpstreamConn, frame: Frame) -> Option<bool> {
    match frame.kind {
        FrameType::Heartbeat => write_frame(&mut client.writer, &frame)
            .await
            .is_err()
            .then_some(true),
        FrameType::Method if is_connection_close(&frame) => {
            let _ = write_frame(&mut client.writer, &methods::build_connection_close_ok()).await;
            Some(true)
        }
        _ => match write_frame(&mut up.writer, &frame).await {
            Ok(()) => None,
            Err(e) => {
                upstream_gone(client, &format!("write error: {e}")).await;
                Some(false)
            }
        },
    }
}

async fn from_upstream(client: &mut ClientConn, up: &mut UpstreamConn, frame: Frame) -> bool {
    if frame.kind == FrameType::Method {
        match methods::method_ids(&frame.payload) {
            Some((CONNECTION, CONNECTION_CLOSE)) => {
                // Broker 主动关闭连接：回复 close-ok，并把原因转给客户端。
                let _ = write_frame(&mut up.writer, &methods::build_connection_close_ok()).await;
                error!("Upstream closed connection: {}", close_text(&frame));
                let _ = write_frame(&mut client.writer, &frame).await;
                return false;
            }
            Some((methods::CHANNEL, methods::CHANNEL_OPEN_OK)) => {
                up.open_channels.insert(frame.channel);
            }
            Some((methods::CHANNEL, methods::CHANNEL_CLOSE_OK | methods::CHANNEL_CLOSE)) => {
                up.open_channels.remove(&frame.channel);
            }
            _ => {}
        }
    }
    write_frame(&mut client.writer, &frame).await.is_ok()
}

fn is_connection_close(frame: &Frame) -> bool {
    methods::method_ids(&frame.payload) == Some((CONNECTION, CONNECTION_CLOSE))
}

fn close_text(frame: &Frame) -> String {
    methods::parse_connection_close(&frame.payload)
        .map(|c| format!("{} {}", c.code, c.text))
        .unwrap_or_default()
}

async fn upstream_gone(client: &mut ClientConn, why: &str) {
    error!("Upstream {why}");
    let close = methods::build_connection_close(methods::CONNECTION_FORCED, "UPSTREAM_ERROR");
    let _ = write_frame(&mut client.writer, &close).await;
}
