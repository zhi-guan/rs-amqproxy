<?php
// 模拟 PHP-FPM：每个“请求”都新建连接、打开 channel、发布一条消息、关闭。
require __DIR__ . '/vendor/autoload.php';

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

$host = getenv('AMQP_HOST') ?: '127.0.0.1';
$port = (int) (getenv('AMQP_PORT') ?: 5673);
$user = getenv('AMQP_USER') ?: 'guest';
$pass = getenv('AMQP_PASS') ?: 'guest';
$sleepUs = (int) (getenv('SLEEP_US') ?: 0);
$queue = 'soak.q';
$small = str_repeat('x', 1024);
$large = str_repeat('y', 300 * 1024); // 多个 body 帧，方便在消息中途被杀掉

while (true) {
    try {
        // php-amqplib 的参数：connection_timeout=30, read_write_timeout=30, keepalive=false, heartbeat=0
        $c = new AMQPStreamConnection($host, $port, $user, $pass, '/', false, 'AMQPLAIN', null, 'en_US', 30.0, 30.0, null, false, 0);
        $ch = $c->channel();
        $ch->basic_publish(new AMQPMessage(mt_rand(1, 100) <= 5 ? $large : $small), '', $queue);
        if (mt_rand(1, 100) <= 5) {
            // 异常退出：不发送 channel.close / connection.close，直接关闭 socket
            $io = $c->getIO();
            if (method_exists($io, 'getSocket')) {
                @fclose($io->getSocket());
                continue;
            }
        }
        $ch->close();
        $c->close();
    } catch (\Throwable $e) {
        usleep(200000);
    }
    if ($sleepUs > 0) {
        usleep($sleepUs);
    }
}
