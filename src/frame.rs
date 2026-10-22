//! AMQP 0-9-1 帧的编码和解码（ADR-0003）。
//!
//! 帧格式：`type(1) channel(2) size(4) payload(size) 0xCE`。
//! 代理只关心少数方法帧，其他帧的 payload 原样转发。

use bytes::{Buf, BufMut, Bytes, BytesMut};

pub const FRAME_END: u8 = 0xCE;
const HEADER_LEN: usize = 7;
/// 帧头 7 字节 + 帧尾 1 字节。frame_max 包含它们。
pub const FRAME_OVERHEAD: u32 = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameType {
    Method = 1,
    Header = 2,
    Body = 3,
    Heartbeat = 8,
}

impl FrameType {
    fn from_u8(v: u8) -> Option<FrameType> {
        match v {
            1 => Some(FrameType::Method),
            2 => Some(FrameType::Header),
            3 => Some(FrameType::Body),
            8 => Some(FrameType::Heartbeat),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub kind: FrameType,
    pub channel: u16,
    pub payload: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameError {
    /// payload 超过 `frame_max`。只看帧头就能发现，所以不会为它分配内存。
    TooLarge {
        size: u32,
        max: u32,
    },
    BadFrameEnd(u8),
    BadFrameType(u8),
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameError::TooLarge { size, max } => {
                write!(
                    f,
                    "frame payload of {size} bytes exceeds the limit of {max} bytes"
                )
            }
            FrameError::BadFrameEnd(b) => write!(f, "frame end byte is 0x{b:02X}, expected 0xCE"),
            FrameError::BadFrameType(t) => write!(f, "unknown frame type {t}"),
        }
    }
}

impl std::error::Error for FrameError {}

impl Frame {
    pub fn new(kind: FrameType, channel: u16, payload: Bytes) -> Frame {
        Frame {
            kind,
            channel,
            payload,
        }
    }

    pub fn heartbeat() -> Frame {
        Frame::new(FrameType::Heartbeat, 0, Bytes::new())
    }

    /// 从 `buf` 中取出一个完整的帧。数据不够时返回 `Ok(None)`，且不修改 `buf`。
    ///
    /// `frame_max` 是整个帧允许的最大字节数（含帧头和帧尾）。`buf` 的内容只要
    /// 还不够一个帧，调用方最多需要缓冲 `frame_max` 字节。
    pub fn decode(buf: &mut BytesMut, frame_max: u32) -> Result<Option<Frame>, FrameError> {
        if buf.len() < HEADER_LEN {
            return Ok(None);
        }
        let kind_byte = buf[0];
        let kind = FrameType::from_u8(kind_byte).ok_or(FrameError::BadFrameType(kind_byte))?;
        let channel = u16::from_be_bytes([buf[1], buf[2]]);
        let size = u32::from_be_bytes([buf[3], buf[4], buf[5], buf[6]]);
        let max_payload = frame_max.saturating_sub(FRAME_OVERHEAD);
        if size > max_payload {
            return Err(FrameError::TooLarge {
                size,
                max: max_payload,
            });
        }
        let total = HEADER_LEN + size as usize + 1;
        if buf.len() < total {
            return Ok(None);
        }
        let end = buf[total - 1];
        if end != FRAME_END {
            return Err(FrameError::BadFrameEnd(end));
        }
        buf.advance(HEADER_LEN);
        let payload = buf.split_to(size as usize).freeze();
        buf.advance(1);
        Ok(Some(Frame {
            kind,
            channel,
            payload,
        }))
    }

    pub fn encode(&self, out: &mut BytesMut) {
        out.reserve(HEADER_LEN + self.payload.len() + 1);
        out.put_u8(self.kind as u8);
        out.put_u16(self.channel);
        out.put_u32(self.payload.len() as u32);
        out.put_slice(&self.payload);
        out.put_u8(FRAME_END);
    }
}
