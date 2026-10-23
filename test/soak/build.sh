#!/bin/bash
# 在 Docker 里构建与发布物一致的二进制（musl 静态链接 + jemalloc）。
# 输出：target/soak-build/release/amqproxy
set -euo pipefail
cd "$(dirname "$0")/../.."
docker run --rm \
  -v "$PWD":/src -w /src \
  -v amqproxy-soak-cargo:/usr/local/cargo/registry \
  rust:alpine sh -c '
    apk add --no-cache musl-dev make gcc >/dev/null &&
    cargo build --release --target-dir /src/target/soak-build &&
    chown -R '"$(id -u):$(id -g)"' /src/target/soak-build'
echo "target/soak-build/release/amqproxy"
