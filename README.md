# rs-amqproxy

用 Rust 复刻 [cloudamqp/amqproxy](https://github.com/cloudamqp/amqproxy) **v0.4.4** 的 AMQP 0-9-1 连接池代理。

PHP-FPM 这类短连接客户端，每个请求都要建立连接、打开和关闭 channel。代理让它们复用到 Broker 的长连接和 channel，减少 Broker 的压力。

本项目的目标是解决原版长期运行后内存持续增长的问题，并且可以在老系统（CentOS 7）上直接运行：发布物是静态链接的 musl 二进制。

## 安装

从 [Releases](https://github.com/zhi-guan/rs-amqproxy/releases) 下载 tar.gz：

```bash
tar xzf amqproxy-*-x86_64-linux-musl.tar.gz
sudo install -m 0755 amqproxy-*/amqproxy /usr/bin/amqproxy
sudo install -m 0644 amqproxy-*/amqproxy.service /etc/systemd/system/amqproxy.service
echo 'AMQP_URL=amqp://127.0.0.1:5672' | sudo tee /etc/default/amqproxy
sudo systemctl daemon-reload
sudo systemctl enable --now amqproxy
```

同一个 service 文件适用于 CentOS 7（systemd 219）和 Debian 12。

## 用法

```
amqproxy [options] [amqp upstream url]
```

| 参数 | 环境变量 | 默认值 | 说明 |
|---|---|---|---|
| `-l, --listen=ADDRESS` | `LISTEN_ADDRESS` | `localhost` | 监听地址 |
| `-p, --port=PORT` | `LISTEN_PORT` | `5673` | 监听端口 |
| `-t, --idle-connection-timeout=SECONDS` | — | `5` | 空闲上游连接的保留时间（相对 v0.4.4 的新增参数） |
| `-d, --debug` | — | 关闭 | DEBUG 日志 |
| `-h, --help` | — | | 帮助 |
| `-v, --version` | — | | 版本 |
| 位置参数 | `AMQP_URL` | 必填 | 上游地址，例如 `amqp://host` 或 `amqps://host:5671` |

优先级：命令行 > 环境变量 > 默认值。上游 URL 里只有 scheme、host 和端口有效；用户名、密码和 vhost 由客户端提供。`amqps://` 使用 TLS（rustls），内置常见的公共 CA。如果 Broker 使用私有 CA，把 CA 证书（PEM）的路径放进环境变量 `SSL_CERT_FILE`，它会被额外信任。

客户端只需把连接地址改成代理，例如 `127.0.0.1:5673`。

日志输出到 stdout。每 5 秒一行统计：`clients=N upstreams=M rss=XXMB`。

## 与 v0.4.4 的区别

对外接口和连接/channel 复用规则与 v0.4.4 相同。有意修改的行为见 [ADR-0002](docs/adr/0002-intentional-deviations-from-v0-4-4.md)，主要有：

- 先连接上游，成功后才回复 `open-ok`，所以密码错误会在建立连接时就报错。
- 只关闭空闲时间达到 `-t` 的连接，而不是每 5 秒全部关闭。
- 代理自己维护到 Broker 的心跳，避免 Broker 因空闲关闭连接。
- 修复了几处会让上游连接状态不干净的问题（事务 channel、消息传输到一半断开、关闭 channel 的确认等）。

不实现的功能见 [ADR-0006](docs/adr/0006-out-of-scope.md)。

## 文档

- [设计说明](docs/design.md)
- [术语表](GLOSSARY.md)
- [决策记录](docs/adr/)

## 构建

```bash
# 默认使用 jemalloc，构建需要 gcc 和 make
cargo build --release
# 没有 make 的机器：不用 jemalloc
cargo build --release --no-default-features
# 静态二进制（需要 musl-tools 和 make）
rustup target add x86_64-unknown-linux-musl
cargo build --release --target x86_64-unknown-linux-musl
```

## 测试

```bash
cargo test
```

集成测试使用真实的 RabbitMQ（含 TLS），需要 Docker：

```bash
test/gen-certs.sh
docker compose -f test/docker-compose.yml up -d --wait
cargo test --features integration --test integration
```

测试在 TCP 边界上进行：用脚本化的假 Broker 和测试客户端驱动代理（见 [设计说明](docs/design.md) 第 12 节）。

内存验收压测（PHP 7.3 + php-amqplib 3.1.0，需要 Docker）：

```bash
test/soak/build.sh                     # 构建 musl + jemalloc 二进制
test/soak/soak.py --mode smoke         # 约 15 分钟
test/soak/soak.py --mode accelerated   # 约 2 小时，发版前运行
```

报告和原始数据保存在 `target/soak/<时间>/`。

## 许可证

MIT。原项目 © CloudAMQP。
