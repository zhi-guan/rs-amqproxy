# 自己实现最小的 AMQP 帧编解码器，不使用完整的 AMQP 库

代理只需要识别少数几种方法帧（`connection.*`、`channel.open/close`、`basic.*` 的 method id、`tx.select`、`confirm.select`），其他帧都按原始字节直接转发。因此我们自己实现一个最小的帧编解码器（7 字节帧头 + payload + `0xCE`），用 `bytes::Bytes` 转发，不引入完整的 AMQP 库（例如 `amq-protocol`）。

## 原因

- 原版的多次内存泄露都来自完整协议库和运行时（例如 amq-protocol.cr 的 StringPool）。完整解码每一帧会产生不必要的内存分配。
- 透传原始字节，可以让内存使用量只和"正在传输的帧"有关，便于证明没有泄露。
- 超过 frame_max 的帧直接拒绝，这样每个连接的内存都有明确的上限。

## 影响

- 代理不验证它不关心的帧的内容，由 Broker 负责验证。
- 握手阶段（`connection.start/tune/open`）的字段需要我们自己编码和解码，包括 PLAIN 和 AMQPLAIN 两种认证方式。
