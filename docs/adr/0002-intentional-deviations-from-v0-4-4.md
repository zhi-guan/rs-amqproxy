# 有意修改的 v0.4.4 行为

我们复刻 v0.4.4 的接口，但不照搬它的 bug。以下行为是**有意**与 v0.4.4 不同的。请不要为了"与原版一致"而改回去。

| # | v0.4.4 的行为 | 本项目的行为 | 原因 |
|---|---|---|---|
| 1 | `tx.select` 不会让 channel 变成不安全 Channel | 标为不安全 | 带事务状态的 channel 不能交给下一个客户端 |
| 2 | 每 5 秒关闭连接池中**所有**上游连接 | 只关闭空闲时间 ≥ 空闲超时的连接 | 避免 PHP-FPM 请求刚好跨过回收时间点 |
| 3 | 发出 `channel.close` 后不等回复，就归还上游连接 | 等到 `channel.close-ok` 后才归还；超时就关闭这个上游连接 | 防止下一个客户端收到上一个客户端的残留帧 |
| 4 | 客户端在消息内容（header/body 帧）传输到一半时断开，上游连接仍被归还 | 关闭这个上游连接 | 上游连接的状态已经无法确定（v0.5.4 也修复了这个问题） |
| 5 | 写入客户端失败后不关闭 socket | 关闭 socket | 防止连接泄露（v0.5.4 也修复了这个问题） |
| 6 | 先回复客户端 `connection.open-ok`，再连接 Broker | 先借出上游连接，成功后才回复 `open-ok`；失败就回复 `connection.close`（403/530 等） | 认证错误会在 `new AMQPStreamConnection()` 时直接抛出，而不是在后续操作时才出现 |
| 7 | 上游断开时，发给客户端 `connection.close(0, "UPSTREAM_ERROR")` | 关闭码改为 `320`，文本保留 `UPSTREAM_ERROR` | 0 不是合法的 AMQP 关闭码 |
| 8 | 上游连接 heartbeat=0 | 由代理维护上游心跳，见 [ADR-0005](./0005-proxy-owns-upstream-heartbeat.md) | 阿里云会关闭空闲连接（ALL_IDLE） |
| 9 | Broker 主动关闭 channel（channel 错误）后，这个 channel 仍被记录为"已打开"。下一个客户端打开它时，会收到伪造的 `open-ok`，但这个 channel 在 Broker 上实际已经关闭 | Broker 发出 `channel.close` 时，从"已打开"集合中删除这个 channel | 防止复用一个已经关闭的 channel |
| 10 | 连接池的 Map 中，池 Key 一旦创建就永远不删除 | 池 Key 下没有连接时，删除这个 Key | 不同凭证的数量很多时，防止 Map 无限增长 |
| 11 | 关闭代理时，发给客户端 `connection.close(0, "AMQProxy shutdown")` | 关闭码改为 `320` | 与第 7 项相同 |
