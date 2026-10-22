#!/bin/bash
# 把 musl 二进制、systemd 文件和 README 打包成 tar.gz。
# 用法：scripts/package.sh <版本号，例如 v0.1.0>
set -euo pipefail
version=${1:?用法: package.sh <版本号>}
name="amqproxy-${version}-x86_64-linux-musl"
bin=target/x86_64-unknown-linux-musl/release/amqproxy
rm -rf "dist/${name}"
mkdir -p "dist/${name}"
install -m 0755 "$bin" "dist/${name}/amqproxy"
install -m 0644 extras/amqproxy.service "dist/${name}/amqproxy.service"
install -m 0644 README.md LICENSE "dist/${name}/"
tar -C dist -czf "dist/${name}.tar.gz" "${name}"
echo "dist/${name}.tar.gz"
