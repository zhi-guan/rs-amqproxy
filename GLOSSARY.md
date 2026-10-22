# rs-amqproxy

rs-amqproxy 是 AMQP 0-9-1 连接池代理，用 Rust 复刻 cloudamqp/amqproxy v0.4.4。它让短连接客户端（例如 PHP-FPM）复用到 Broker 的长连接和 channel。

## 参与方

**客户端（Client）**：
连接到代理的 AMQP 应用程序，例如使用 php-amqplib 的 PHP 进程。
_避免_：下游、consumer（consumer 在 AMQP 中另有含义）

**Broker**：
真正处理消息的 AMQP 服务器，例如阿里云 RabbitMQ 版或开源 RabbitMQ。
_避免_：服务端、MQ、上游服务器

**代理（Proxy）**：
本项目本身，位于客户端和 Broker 之间。
_避免_：网关、中间件

## 连接

**客户端连接（Client Connection）**：
一个客户端到代理的 AMQP 连接。
_避免_：下游连接、前端连接

**上游连接（Upstream Connection）**：
代理到 Broker 的一个 AMQP 连接。在任一时刻，最多只绑定一个客户端连接。
_避免_：后端连接、server connection

**连接池（Pool）**：
当前没有绑定客户端连接、等待复用的上游连接的集合。
_避免_：缓存

**池 Key（Pool Key）**：
(用户名, 密码, vhost) 三元组。只有池 Key 相同的客户端连接，才能复用同一个上游连接。
_避免_：凭证、身份

**借出（Borrow）**：
把一个上游连接绑定到一个客户端连接上。上游连接来自连接池，或者是新建的。
_避免_：分配、获取

**归还（Return）**：
客户端连接结束后，把仍然健康的上游连接放回连接池。
_避免_：释放、回收

**空闲超时（Idle Connection Timeout）**：
上游连接在连接池中最多可以停留的时间。超过这个时间，代理就关闭它。
_避免_：过期时间

## Channel

**安全 Channel（Safe Channel）**：
一个上游 channel，其中只发生过不会留下状态的操作（`basic.publish`、`basic.qos`、`basic.get` no_ack）。客户端关闭它时，代理不真正关闭它，而是留给下一个客户端复用。
_避免_：干净 channel、可复用 channel

**不安全 Channel（Unsafe Channel）**：
一个上游 channel，其中发生过会留下状态的操作（consume、ack、confirm、tx 等）。客户端关闭它或客户端断开时，代理会真正关闭它。
_避免_：脏 channel

**伪造回复（Synthesized Reply）**：
代理自己生成并发给客户端的回复帧，而不是由 Broker 发出，例如复用安全 Channel 时伪造的 `channel.open-ok`。
_避免_：假帧、mock 回复
