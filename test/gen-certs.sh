#!/bin/bash
# 生成测试用的自签名 CA 和 RabbitMQ 服务器证书（仅用于本地测试）。
set -euo pipefail
cd "$(dirname "$0")/certs"
[ -f ca.crt ] && { echo "证书已存在。删除 test/certs 后可重新生成。"; exit 0; }
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=amqproxy-test-ca" -keyout ca.key -out ca.crt
openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" -keyout server.key -out server.csr
printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\n' > san.ext
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 3650 -extfile san.ext -out server.crt
chmod 644 *.crt *.key
rm -f server.csr san.ext ca.srl
