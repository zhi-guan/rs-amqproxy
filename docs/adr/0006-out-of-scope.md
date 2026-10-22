# 不实现的功能

以下功能在 v0.4.4 中不存在，或者只存在于代码中但没有被使用，或者属于后续版本。第一版**有意不实现**：

- **配置文件（INI）**：v0.4.4 仓库中有 `config/example.ini`，但代码没有读取它。
- **客户端侧 TLS**：v0.4.4 有 `listen_tls` 函数，但没有被调用。代理部署在每台应用服务器上，只监听本机地址。只支持到 Broker 的 TLS（`amqps://`，使用 rustls）。
- **HTTP `/healthz` 和 `/metrics`**：用每 5 秒一行的统计日志代替。
- **最大连接数限制**：由 systemd 的 `LimitNOFILE` 控制。
- **v3 共享连接模型**：见 [ADR-0001](./0001-copy-v0-4-4-one-to-one-model.md)。
- **Dockerfile / 容器镜像**：交付物只有 tar.gz（二进制 + systemd 文件 + README）。Docker 只在本地测试时使用。
