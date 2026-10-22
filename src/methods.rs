//! 代理需要理解的少数 AMQP 方法：握手、channel 和 connection 的打开/关闭，
//! 以及 `basic.get` 的 `no_ack`。其他方法只读取 class id 和 method id。

use bytes::{BufMut, BytesMut};

use crate::frame::{Frame, FrameType};

pub const CONNECTION: u16 = 10;
pub const CHANNEL: u16 = 20;
pub const EXCHANGE: u16 = 40;
pub const QUEUE: u16 = 50;
pub const BASIC: u16 = 60;
pub const CONFIRM: u16 = 85;
pub const TX: u16 = 90;

pub const CONNECTION_START: u16 = 10;
pub const CONNECTION_START_OK: u16 = 11;
pub const CONNECTION_TUNE: u16 = 30;
pub const CONNECTION_TUNE_OK: u16 = 31;
pub const CONNECTION_OPEN: u16 = 40;
pub const CONNECTION_OPEN_OK: u16 = 41;
pub const CONNECTION_CLOSE: u16 = 50;
pub const CONNECTION_CLOSE_OK: u16 = 51;
pub const CHANNEL_OPEN: u16 = 10;
pub const CHANNEL_OPEN_OK: u16 = 11;
pub const CHANNEL_CLOSE: u16 = 40;
pub const CHANNEL_CLOSE_OK: u16 = 41;
pub const BASIC_QOS: u16 = 10;
pub const BASIC_PUBLISH: u16 = 40;
pub const BASIC_GET: u16 = 70;

pub const PROTOCOL_HEADER: [u8; 8] = *b"AMQP\x00\x00\x09\x01";
pub const PROTOCOL_HEADER_0_9: [u8; 8] = *b"AMQP\x01\x01\x00\x09";

pub const REPLY_SUCCESS: u16 = 200;
pub const FRAME_ERROR: u16 = 501;
pub const ACCESS_REFUSED: u16 = 403;
pub const CONNECTION_FORCED: u16 = 320;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseError {
    Truncated,
    InvalidUtf8,
    TooDeep,
    Missing,
}

/// 读取 payload 的小工具。所有读取都检查长度，不会 panic。
struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], ParseError> {
        if self.0.len() < n {
            return Err(ParseError::Truncated);
        }
        let (head, tail) = self.0.split_at(n);
        self.0 = tail;
        Ok(head)
    }
    fn u8(&mut self) -> Result<u8, ParseError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, ParseError> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }
    fn u32(&mut self) -> Result<u32, ParseError> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn short_str(&mut self) -> Result<&'a [u8], ParseError> {
        let n = self.u8()? as usize;
        self.take(n)
    }
    fn long_str(&mut self) -> Result<&'a [u8], ParseError> {
        let n = self.u32()? as usize;
        self.take(n)
    }
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

fn utf8(b: &[u8]) -> Result<String, ParseError> {
    String::from_utf8(b.to_vec()).map_err(|_| ParseError::InvalidUtf8)
}

/// 方法帧 payload 的 (class id, method id)。
pub fn method_ids(payload: &[u8]) -> Option<(u16, u16)> {
    let mut r = Reader(payload);
    Some((r.u16().ok()?, r.u16().ok()?))
}

fn expect_method(r: &mut Reader, class: u16, method: u16) -> Result<(), ParseError> {
    if r.u16()? == class && r.u16()? == method {
        Ok(())
    } else {
        Err(ParseError::Missing)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartOk {
    pub mechanism: String,
    pub response: Vec<u8>,
}

pub fn parse_start_ok(payload: &[u8]) -> Result<StartOk, ParseError> {
    let mut r = Reader(payload);
    expect_method(&mut r, CONNECTION, CONNECTION_START_OK)?;
    r.long_str()?; // client-properties
    let mechanism = utf8(r.short_str()?)?;
    let response = r.long_str()?.to_vec();
    Ok(StartOk {
        mechanism,
        response,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tune {
    pub channel_max: u16,
    pub frame_max: u32,
    pub heartbeat: u16,
}

fn parse_tune_fields(payload: &[u8], method: u16) -> Result<Tune, ParseError> {
    let mut r = Reader(payload);
    expect_method(&mut r, CONNECTION, method)?;
    Ok(Tune {
        channel_max: r.u16()?,
        frame_max: r.u32()?,
        heartbeat: r.u16()?,
    })
}

pub fn parse_tune(payload: &[u8]) -> Result<Tune, ParseError> {
    parse_tune_fields(payload, CONNECTION_TUNE)
}

pub fn parse_tune_ok(payload: &[u8]) -> Result<Tune, ParseError> {
    parse_tune_fields(payload, CONNECTION_TUNE_OK)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Open {
    pub vhost: String,
}

pub fn parse_open(payload: &[u8]) -> Result<Open, ParseError> {
    let mut r = Reader(payload);
    expect_method(&mut r, CONNECTION, CONNECTION_OPEN)?;
    Ok(Open {
        vhost: utf8(r.short_str()?)?,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Close {
    pub code: u16,
    pub text: String,
    pub class_id: u16,
    pub method_id: u16,
}

fn parse_close(payload: &[u8], class: u16, method: u16) -> Result<Close, ParseError> {
    let mut r = Reader(payload);
    expect_method(&mut r, class, method)?;
    let code = r.u16()?;
    let text = String::from_utf8_lossy(r.short_str()?).into_owned();
    Ok(Close {
        code,
        text,
        class_id: r.u16()?,
        method_id: r.u16()?,
    })
}

pub fn parse_connection_close(payload: &[u8]) -> Result<Close, ParseError> {
    parse_close(payload, CONNECTION, CONNECTION_CLOSE)
}

pub fn parse_channel_close(payload: &[u8]) -> Result<Close, ParseError> {
    parse_close(payload, CHANNEL, CHANNEL_CLOSE)
}

/// `basic.get` 的 `no_ack` 标志。
pub fn parse_basic_get_no_ack(payload: &[u8]) -> Result<bool, ParseError> {
    let mut r = Reader(payload);
    expect_method(&mut r, BASIC, BASIC_GET)?;
    r.u16()?; // reserved
    r.short_str()?; // queue
    Ok(r.u8()? & 1 == 1)
}

/// 表的值，只保留代理需要的字符串，其他类型只跳过。
enum Value<'a> {
    Str(&'a [u8]),
    Other,
}

const MAX_TABLE_DEPTH: u8 = 8;

fn walk_table(data: &[u8], depth: u8, f: &mut dyn FnMut(&[u8], Value)) -> Result<(), ParseError> {
    if depth > MAX_TABLE_DEPTH {
        return Err(ParseError::TooDeep);
    }
    let mut r = Reader(data);
    while !r.is_empty() {
        let key = r.short_str()?;
        let v = read_value(&mut r, depth)?;
        f(key, v);
    }
    Ok(())
}

fn read_value<'a>(r: &mut Reader<'a>, depth: u8) -> Result<Value<'a>, ParseError> {
    let skip = |r: &mut Reader<'a>, n: usize| r.take(n).map(|_| Value::Other);
    match r.u8()? {
        b't' | b'b' | b'B' => skip(r, 1),
        b'U' | b'u' => skip(r, 2),
        b'I' | b'i' | b'f' => skip(r, 4),
        b'L' | b'l' | b'd' | b'T' => skip(r, 8),
        b'D' => skip(r, 5),
        b's' => r.short_str().map(|_| Value::Other),
        b'S' => r.long_str().map(Value::Str),
        b'x' => r.long_str().map(|_| Value::Other),
        b'V' => Ok(Value::Other),
        b'F' => {
            walk_table(r.long_str()?, depth + 1, &mut |_, _| {})?;
            Ok(Value::Other)
        }
        b'A' => {
            let mut items = Reader(r.long_str()?);
            if depth + 1 > MAX_TABLE_DEPTH {
                return Err(ParseError::TooDeep);
            }
            while !items.is_empty() {
                read_value(&mut items, depth + 1)?;
            }
            Ok(Value::Other)
        }
        _ => Err(ParseError::Missing),
    }
}

/// AMQPLAIN 的 response：不带长度前缀的表，包含 LOGIN 和 PASSWORD。
pub fn parse_amqplain_credentials(response: &[u8]) -> Result<(String, String), ParseError> {
    let (mut login, mut password) = (None, None);
    walk_table(response, 0, &mut |key, value| {
        if let Value::Str(s) = value {
            match key {
                b"LOGIN" => login = Some(s.to_vec()),
                b"PASSWORD" => password = Some(s.to_vec()),
                _ => {}
            }
        }
    })?;
    match (login, password) {
        (Some(l), Some(p)) => Ok((utf8(&l)?, utf8(&p)?)),
        _ => Err(ParseError::Missing),
    }
}

/// PLAIN 的 response：`\0user\0password`。
pub fn parse_plain_credentials(response: &[u8]) -> Result<(String, String), ParseError> {
    let rest = response.strip_prefix(&[0]).ok_or(ParseError::Missing)?;
    let i = rest
        .iter()
        .position(|&b| b == 0)
        .ok_or(ParseError::Missing)?;
    Ok((utf8(&rest[..i])?, utf8(&rest[i + 1..])?))
}

// ---------- 构造帧 ----------

fn method_payload(class: u16, method: u16) -> BytesMut {
    let mut b = BytesMut::new();
    b.put_u16(class);
    b.put_u16(method);
    b
}

fn put_short_str(b: &mut BytesMut, s: &str) {
    let s = &s.as_bytes()[..s.len().min(255)];
    b.put_u8(s.len() as u8);
    b.put_slice(s);
}

fn put_long_str(b: &mut BytesMut, s: &[u8]) {
    b.put_u32(s.len() as u32);
    b.put_slice(s);
}

enum Field<'a> {
    Bool(bool),
    Str(&'a str),
    Table(&'a [(&'a str, Field<'a>)]),
}

fn put_table(b: &mut BytesMut, entries: &[(&str, Field)]) {
    let mut body = BytesMut::new();
    for (key, value) in entries {
        put_short_str(&mut body, key);
        match value {
            Field::Bool(v) => {
                body.put_u8(b't');
                body.put_u8(*v as u8);
            }
            Field::Str(s) => {
                body.put_u8(b'S');
                put_long_str(&mut body, s.as_bytes());
            }
            Field::Table(t) => {
                body.put_u8(b'F');
                put_table(&mut body, t);
            }
        }
    }
    put_long_str(b, &body);
}

fn method_frame(channel: u16, payload: BytesMut) -> Frame {
    Frame::new(FrameType::Method, channel, payload.freeze())
}

/// 代理发给客户端的 `connection.start`。
pub fn build_start() -> Frame {
    let mut b = method_payload(CONNECTION, CONNECTION_START);
    b.put_u8(0);
    b.put_u8(9);
    put_table(
        &mut b,
        &[
            ("product", Field::Str("AMQProxy")),
            ("version", Field::Str(env!("CARGO_PKG_VERSION"))),
            (
                "capabilities",
                Field::Table(&[
                    ("publisher_confirms", Field::Bool(true)),
                    ("basic.nack", Field::Bool(true)),
                    ("consumer_cancel_notify", Field::Bool(false)),
                ]),
            ),
        ],
    );
    put_long_str(&mut b, b"PLAIN AMQPLAIN");
    put_long_str(&mut b, b"en_US");
    method_frame(0, b)
}

/// 代理发给 Broker 的 `connection.start-ok`（PLAIN）。
pub fn build_start_ok_plain(user: &str, password: &str) -> Frame {
    let mut b = method_payload(CONNECTION, CONNECTION_START_OK);
    put_table(
        &mut b,
        &[
            ("product", Field::Str("AMQProxy")),
            ("version", Field::Str(env!("CARGO_PKG_VERSION"))),
            (
                "capabilities",
                Field::Table(&[
                    ("authentication_failure_close", Field::Bool(true)),
                    ("consumer_cancel_notify", Field::Bool(false)),
                    ("publisher_confirms", Field::Bool(true)),
                    ("exchange_exchange_bindings", Field::Bool(true)),
                    ("basic.nack", Field::Bool(true)),
                    ("per_consumer_qos", Field::Bool(true)),
                    ("connection.blocked", Field::Bool(true)),
                ]),
            ),
        ],
    );
    put_short_str(&mut b, "PLAIN");
    let mut response = Vec::with_capacity(user.len() + password.len() + 2);
    response.push(0);
    response.extend_from_slice(user.as_bytes());
    response.push(0);
    response.extend_from_slice(password.as_bytes());
    put_long_str(&mut b, &response);
    put_short_str(&mut b, "en_US");
    method_frame(0, b)
}

fn tune_frame(method: u16, t: Tune) -> Frame {
    let mut b = method_payload(CONNECTION, method);
    b.put_u16(t.channel_max);
    b.put_u32(t.frame_max);
    b.put_u16(t.heartbeat);
    method_frame(0, b)
}

pub fn build_tune(t: Tune) -> Frame {
    tune_frame(CONNECTION_TUNE, t)
}

pub fn build_tune_ok(t: Tune) -> Frame {
    tune_frame(CONNECTION_TUNE_OK, t)
}

pub fn build_open(vhost: &str) -> Frame {
    let mut b = method_payload(CONNECTION, CONNECTION_OPEN);
    put_short_str(&mut b, vhost);
    put_short_str(&mut b, "");
    b.put_u8(0);
    method_frame(0, b)
}

pub fn build_open_ok() -> Frame {
    let mut b = method_payload(CONNECTION, CONNECTION_OPEN_OK);
    put_short_str(&mut b, "");
    method_frame(0, b)
}

fn close_frame(channel: u16, class: u16, method: u16, c: &Close) -> Frame {
    let mut b = method_payload(class, method);
    b.put_u16(c.code);
    put_short_str(&mut b, &c.text);
    b.put_u16(c.class_id);
    b.put_u16(c.method_id);
    method_frame(channel, b)
}

pub fn build_connection_close(code: u16, text: &str) -> Frame {
    let c = Close {
        code,
        text: text.to_string(),
        class_id: 0,
        method_id: 0,
    };
    close_frame(0, CONNECTION, CONNECTION_CLOSE, &c)
}

pub fn build_connection_close_ok() -> Frame {
    method_frame(0, method_payload(CONNECTION, CONNECTION_CLOSE_OK))
}

pub fn build_channel_open(channel: u16) -> Frame {
    let mut b = method_payload(CHANNEL, CHANNEL_OPEN);
    put_short_str(&mut b, "");
    method_frame(channel, b)
}

pub fn build_channel_open_ok(channel: u16) -> Frame {
    let mut b = method_payload(CHANNEL, CHANNEL_OPEN_OK);
    put_long_str(&mut b, b"");
    method_frame(channel, b)
}

pub fn build_channel_close(channel: u16, code: u16, text: &str) -> Frame {
    let c = Close {
        code,
        text: text.to_string(),
        class_id: 0,
        method_id: 0,
    };
    close_frame(channel, CHANNEL, CHANNEL_CLOSE, &c)
}

pub fn build_channel_close_ok(channel: u16) -> Frame {
    method_frame(channel, method_payload(CHANNEL, CHANNEL_CLOSE_OK))
}

/// 客户端发来的方法是否会在上游 channel 上留下状态（设计文档 §5）。
pub fn is_unsafe_client_method(class: u16, method: u16, basic_get_no_ack: bool) -> bool {
    match class {
        BASIC => match method {
            BASIC_PUBLISH | BASIC_QOS => false,
            BASIC_GET => !basic_get_no_ack,
            _ => true,
        },
        CONFIRM | TX => true,
        _ => false,
    }
}
