//! 一个客户端连接和它绑定的上游连接之间的双向转发，以及 Channel 复用
//! （设计文档 §5）。
//!
//! 整个会话在一个任务里用 `select!` 完成：写一侧时不读另一侧，所以缓冲区
//! 不会无限增长（背压），也不需要锁。

use std::collections::HashSet;
use std::time::Duration;

use tracing::{debug, error, warn};

use crate::client_handshake::ClientConn;
use crate::frame::{FRAME_OVERHEAD, Frame, FrameType, ReadError, write_frame};
use crate::methods::{
    self, CHANNEL, CHANNEL_CLOSE, CHANNEL_CLOSE_OK, CHANNEL_OPEN, CHANNEL_OPEN_OK,
};
use crate::methods::{CONNECTION, CONNECTION_CLOSE};
use crate::upstream::UpstreamConn;

/// 等待 Broker 确认关闭 channel 的最长时间。超时就丢弃这个上游连接。
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(3);

/// 运行会话，直到任一侧结束。如果上游连接仍然干净、可以复用，就返回它；
/// 否则关闭它并返回 `None`。
pub async fn run(client: &mut ClientConn, mut up: UpstreamConn) -> Option<UpstreamConn> {
    if forward(client, &mut up).await && release_channels(&mut up).await {
        return Some(up);
    }
    up.close("").await;
    None
}

/// 客户端正在发送的消息内容（`basic.publish` 之后跟着 header 帧和 body 帧）。
/// 客户端在消息中途离开时，上游连接的状态无法确定，必须丢弃。
#[derive(Clone, Copy, PartialEq, Eq)]
enum Content {
    Idle,
    ExpectHeader,
    Body { remaining: u64 },
}

/// 返回 `true` 表示会话因为客户端结束，上游连接没有问题。
async fn forward(client: &mut ClientConn, up: &mut UpstreamConn) -> bool {
    let mut content = Content::Idle;
    let reusable = loop {
        tokio::select! {
            frame = client.reader.read_frame() => match frame {
                Ok(Some(frame)) => {
                    if let Some(end) = from_client(client, up, &mut content, frame).await {
                        break end;
                    }
                }
                Ok(None) => { debug!("Client disconnected"); break true; }
                Err(ReadError::Frame(e)) => {
                    error!("Invalid frame from client: {e}");
                    frame_error(client, &e.to_string()).await;
                    break false;
                }
                Err(e) => { debug!("Client read error: {e}"); break true; }
            },
            frame = up.reader.read_frame() => match frame {
                Ok(Some(frame)) => {
                    if let Some(end) = from_upstream(client, up, frame).await {
                        break end;
                    }
                }
                Ok(None) => { upstream_gone(client, "closed the connection").await; break false; }
                Err(e) => { upstream_gone(client, &e.to_string()).await; break false; }
            },
        }
    };
    reusable && content == Content::Idle
}

/// `None` 表示继续；`Some(reusable)` 表示会话结束。
async fn from_client(
    client: &mut ClientConn,
    up: &mut UpstreamConn,
    content: &mut Content,
    frame: Frame,
) -> Option<bool> {
    match frame.kind {
        FrameType::Heartbeat => {
            return write_frame(&mut client.writer, &frame)
                .await
                .is_err()
                .then_some(true);
        }
        FrameType::Header | FrameType::Body => {
            if let Err(msg) = track_content(content, &frame) {
                frame_error(client, msg).await;
                return Some(false);
            }
            return forward_to_upstream(client, up, &frame).await;
        }
        FrameType::Method => {}
    }
    let ch = frame.channel;
    let ids = methods::method_ids(&frame.payload);
    match ids {
        Some((CONNECTION, CONNECTION_CLOSE)) => {
            let _ = write_frame(&mut client.writer, &methods::build_connection_close_ok()).await;
            return Some(true);
        }
        // 这个 channel 在上游已经打开：伪造 open-ok，不转发。
        Some((CHANNEL, CHANNEL_OPEN)) if up.open_channels.contains(&ch) => {
            return reply_to_client(client, &methods::build_channel_open_ok(ch)).await;
        }
        // 安全 Channel：伪造 close-ok，上游 channel 保持打开。
        Some((CHANNEL, CHANNEL_CLOSE)) if !up.unsafe_channels.contains(&ch) => {
            return reply_to_client(client, &methods::build_channel_close_ok(ch)).await;
        }
        Some((CHANNEL, CHANNEL_CLOSE_OK)) => {
            up.pending_broker_close.remove(&ch);
        }
        Some((class, method)) => {
            if (class, method) == (methods::BASIC, methods::BASIC_PUBLISH) {
                *content = Content::ExpectHeader;
            }
            let no_ack = class == methods::BASIC
                && method == methods::BASIC_GET
                && methods::parse_basic_get_no_ack(&frame.payload).unwrap_or(false);
            if methods::is_unsafe_client_method(class, method, no_ack) {
                up.unsafe_channels.insert(ch);
            }
        }
        None => {}
    }
    forward_to_upstream(client, up, &frame).await
}

async fn reply_to_client(client: &mut ClientConn, reply: &Frame) -> Option<bool> {
    write_frame(&mut client.writer, reply)
        .await
        .is_err()
        .then_some(true)
}

fn track_content(content: &mut Content, frame: &Frame) -> Result<(), &'static str> {
    match (*content, frame.kind) {
        (Content::ExpectHeader, FrameType::Header) => {
            let size = frame.payload.get(4..12).ok_or("malformed content header")?;
            let size = u64::from_be_bytes(size.try_into().expect("slice has 8 bytes"));
            *content = if size == 0 {
                Content::Idle
            } else {
                Content::Body { remaining: size }
            };
        }
        (Content::Body { remaining }, FrameType::Body) => {
            let remaining = remaining.saturating_sub(frame.payload.len() as u64);
            *content = if remaining == 0 {
                Content::Idle
            } else {
                Content::Body { remaining }
            };
        }
        _ => {}
    }
    Ok(())
}

async fn forward_to_upstream(
    client: &mut ClientConn,
    up: &mut UpstreamConn,
    frame: &Frame,
) -> Option<bool> {
    let size = frame.payload.len() as u64 + u64::from(FRAME_OVERHEAD);
    if size > u64::from(up.frame_max) {
        let msg = format!(
            "frame of {size} bytes exceeds the upstream frame_max of {}",
            up.frame_max
        );
        error!("{msg}");
        frame_error(client, &msg).await;
        return Some(false);
    }
    match write_frame(&mut up.writer, frame).await {
        Ok(()) => None,
        Err(e) => {
            upstream_gone(client, &format!("write error: {e}")).await;
            Some(false)
        }
    }
}

/// `None` 表示继续；`Some(reusable)` 表示会话结束。
async fn from_upstream(
    client: &mut ClientConn,
    up: &mut UpstreamConn,
    frame: Frame,
) -> Option<bool> {
    if frame.kind == FrameType::Method {
        match methods::method_ids(&frame.payload) {
            Some((CONNECTION, CONNECTION_CLOSE)) => {
                // Broker 主动关闭连接：回复 close-ok，并把原因转给客户端。
                let _ = write_frame(&mut up.writer, &methods::build_connection_close_ok()).await;
                error!("Upstream closed connection: {}", close_text(&frame));
                let _ = write_frame(&mut client.writer, &frame).await;
                return Some(false);
            }
            Some((CHANNEL, CHANNEL_OPEN_OK)) => {
                up.open_channels.insert(frame.channel);
            }
            Some((CHANNEL, CHANNEL_CLOSE_OK)) => up.forget_channel(frame.channel),
            Some((CHANNEL, CHANNEL_CLOSE)) => {
                // Broker 关闭了 channel（channel 错误）：客户端需要回复 close-ok。
                up.forget_channel(frame.channel);
                up.pending_broker_close.insert(frame.channel);
            }
            _ => {}
        }
    }
    // 写入客户端失败：客户端已经不在了，上游连接本身没有问题。
    write_frame(&mut client.writer, &frame)
        .await
        .is_err()
        .then_some(true)
}

/// 客户端结束后，把上游连接恢复到干净状态：确认 Broker 发起的关闭，关闭所有
/// 不安全 Channel 并等待 Broker 确认。返回 `false` 表示连接不能再用。
async fn release_channels(up: &mut UpstreamConn) -> bool {
    for ch in std::mem::take(&mut up.pending_broker_close) {
        if write_frame(&mut up.writer, &methods::build_channel_close_ok(ch))
            .await
            .is_err()
        {
            return false;
        }
    }
    let mut waiting: HashSet<u16> = up
        .unsafe_channels
        .intersection(&up.open_channels)
        .copied()
        .collect();
    up.unsafe_channels.clear();
    for &ch in &waiting {
        let close = methods::build_channel_close(ch, methods::REPLY_SUCCESS, "Client disconnected");
        if write_frame(&mut up.writer, &close).await.is_err() {
            return false;
        }
    }
    let drained = tokio::time::timeout(CLEANUP_TIMEOUT, async {
        while !waiting.is_empty() {
            let frame = match up.reader.read_frame().await {
                Ok(Some(f)) => f,
                _ => return false,
            };
            if frame.kind != FrameType::Method {
                continue; // 投递的 header / body：丢弃
            }
            match methods::method_ids(&frame.payload) {
                Some((CHANNEL, CHANNEL_CLOSE_OK)) => {
                    waiting.remove(&frame.channel);
                    up.forget_channel(frame.channel);
                }
                Some((CHANNEL, CHANNEL_CLOSE)) => {
                    // 双方同时关闭同一个 channel。
                    waiting.remove(&frame.channel);
                    up.forget_channel(frame.channel);
                    let ok = methods::build_channel_close_ok(frame.channel);
                    if write_frame(&mut up.writer, &ok).await.is_err() {
                        return false;
                    }
                }
                Some((CONNECTION, CONNECTION_CLOSE)) => {
                    let _ =
                        write_frame(&mut up.writer, &methods::build_connection_close_ok()).await;
                    return false;
                }
                _ => {} // 投递、basic.cancel 等：丢弃
            }
        }
        true
    })
    .await;
    match drained {
        Ok(ok) => ok,
        Err(_) => {
            warn!(
                "Timed out waiting for the broker to close channels, dropping the upstream connection"
            );
            false
        }
    }
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

async fn frame_error(client: &mut ClientConn, why: &str) {
    let close =
        methods::build_connection_close(methods::FRAME_ERROR, &format!("FRAME_ERROR - {why}"));
    let _ = write_frame(&mut client.writer, &close).await;
}
