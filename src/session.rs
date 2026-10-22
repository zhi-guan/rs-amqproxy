//! 一个客户端连接和它绑定的上游连接之间的双向转发。
//!
//! 整个会话在一个任务里用 `select!` 完成：写一侧时不读另一侧，所以缓冲区
//! 不会无限增长（背压），也不需要锁。

use tokio::io::AsyncWriteExt;
use tracing::{debug, error};

use crate::client_handshake::ClientConn;
use crate::frame::{Frame, FrameType, write_frame};
use crate::methods::{self, CONNECTION, CONNECTION_CLOSE};
use crate::upstream::UpstreamConn;

pub async fn run(mut client: ClientConn, mut up: UpstreamConn) {
    loop {
        tokio::select! {
            frame = client.reader.read_frame() => match frame {
                Ok(Some(frame)) => {
                    if !from_client(&mut client, &mut up, frame).await {
                        break;
                    }
                }
                Ok(None) => { debug!("Client disconnected"); break; }
                Err(e) => { debug!("Client read error: {e}"); break; }
            },
            frame = up.reader.read_frame() => match frame {
                Ok(Some(frame)) => {
                    if !from_upstream(&mut client, &mut up, frame).await {
                        break;
                    }
                }
                Ok(None) => { upstream_gone(&mut client, "closed the connection").await; break; }
                Err(e) => { upstream_gone(&mut client, &e.to_string()).await; break; }
            },
        }
    }
    close_upstream(&mut up).await;
    let _ = client.writer.shutdown().await;
}

/// 返回 `false` 表示会话结束。
async fn from_client(client: &mut ClientConn, up: &mut UpstreamConn, frame: Frame) -> bool {
    match frame.kind {
        FrameType::Heartbeat => write_frame(&mut client.writer, &frame).await.is_ok(),
        FrameType::Method if is_connection_close(&frame) => {
            let _ = write_frame(&mut client.writer, &methods::build_connection_close_ok()).await;
            false
        }
        _ => match write_frame(&mut up.writer, &frame).await {
            Ok(()) => true,
            Err(e) => {
                upstream_gone(client, &format!("write error: {e}")).await;
                false
            }
        },
    }
}

async fn from_upstream(client: &mut ClientConn, up: &mut UpstreamConn, frame: Frame) -> bool {
    if frame.kind == FrameType::Method && is_connection_close(&frame) {
        // Broker 主动关闭连接：回复 close-ok，并把原因转给客户端。
        let _ = write_frame(&mut up.writer, &methods::build_connection_close_ok()).await;
        error!("Upstream closed connection: {}", close_text(&frame));
        let _ = write_frame(&mut client.writer, &frame).await;
        return false;
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

async fn close_upstream(up: &mut UpstreamConn) {
    let close = methods::build_connection_close(methods::REPLY_SUCCESS, "");
    let _ = write_frame(&mut up.writer, &close).await;
    let _ = up.writer.shutdown().await;
}
