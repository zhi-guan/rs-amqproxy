//! 模糊测试：解码器处理的是不可信的网络输入。
//! 任意输入都不能 panic，也不能让缓冲区超过 frame_max。

use amqproxy::frame::{Frame, FrameError, FrameType};
use amqproxy::methods;
use bytes::BytesMut;

/// 固定种子的 xorshift，保证测试可重复，不引入新依赖。
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn bytes(&mut self, n: usize) -> Vec<u8> {
        (0..n).map(|_| self.next() as u8).collect()
    }
}

fn parse_everything(payload: &[u8]) {
    let _ = methods::parse_start_ok(payload);
    let _ = methods::parse_tune_ok(payload);
    let _ = methods::parse_tune(payload);
    let _ = methods::parse_open(payload);
    let _ = methods::parse_connection_close(payload);
    let _ = methods::parse_channel_close(payload);
    let _ = methods::parse_basic_get_no_ack(payload);
    let _ = methods::parse_amqplain_credentials(payload);
    let _ = methods::method_ids(payload);
}

#[test]
fn random_bytes_never_panic_or_buffer_beyond_frame_max() {
    let frame_max = 4096;
    let mut rng = Rng(0x9E3779B97F4A7C15);
    for _ in 0..20_000 {
        let n = (rng.next() % 600) as usize;
        let mut buf = BytesMut::from(&rng.bytes(n)[..]);
        while let Ok(Some(f)) = Frame::decode(&mut buf, frame_max) {
            parse_everything(&f.payload);
        }
        parse_everything(&buf);
    }
}

#[test]
fn random_payloads_never_panic_in_method_parsers() {
    let mut rng = Rng(42);
    for _ in 0..50_000 {
        let n = (rng.next() % 200) as usize;
        let mut p = rng.bytes(n);
        // 让一部分输入看起来像真实的方法帧，进入更深的解析路径。
        if p.len() >= 4 && rng.next().is_multiple_of(2) {
            p[0] = 0;
            p[1] = [10, 20, 60, 90][(rng.next() % 4) as usize];
            p[2] = 0;
            p[3] = (rng.next() % 80) as u8;
        }
        parse_everything(&p);
    }
}

#[test]
fn oversized_frame_is_rejected_from_the_header_alone() {
    // 声明 1 GiB 的 payload，但只给了 7 字节的帧头。
    let mut buf = BytesMut::from(&[1u8, 0, 1, 0x40, 0, 0, 0][..]);
    let err = Frame::decode(&mut buf, 131_072).unwrap_err();
    assert!(matches!(err, FrameError::TooLarge { .. }));
}

#[test]
fn bad_frame_end_is_rejected() {
    let mut buf = BytesMut::from(&[8u8, 0, 0, 0, 0, 0, 0, 0x00][..]);
    assert!(matches!(
        Frame::decode(&mut buf, 131_072),
        Err(FrameError::BadFrameEnd(0))
    ));
}

#[test]
fn partial_frame_waits_for_more_data_and_complete_frame_roundtrips() {
    let frame = Frame::new(FrameType::Method, 3, vec![0, 60, 0, 40, 1, 2, 3].into());
    let mut wire = BytesMut::new();
    frame.encode(&mut wire);
    let mut partial = BytesMut::from(&wire[..wire.len() - 1]);
    assert!(Frame::decode(&mut partial, 131_072).unwrap().is_none());
    let decoded = Frame::decode(&mut wire, 131_072).unwrap().unwrap();
    assert_eq!(decoded, frame);
    assert!(wire.is_empty());
}
