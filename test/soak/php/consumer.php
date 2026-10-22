<?php
// 常驻 CLI 消费者：basic_consume + wait()，heartbeat=0，keepalive=false。
// 每隔 1~5 分钟主动重连一次，模拟部署或重启。
require __DIR__ . '/vendor/autoload.php';

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Wire\AMQPTable;

$host = getenv('AMQP_HOST') ?: '127.0.0.1';
$port = (int) (getenv('AMQP_PORT') ?: 5673);
$queue = 'soak.q';

while (true) {
    try {
        $c = new AMQPStreamConnection($host, $port, 'guest', 'guest', '/', false, 'AMQPLAIN', null, 'en_US', 30.0, 30.0, null, false, 0);
        $ch = $c->channel();
        $ch->queue_declare($queue, false, false, false, false, false, new AMQPTable(['x-max-length' => 5000, 'x-overflow' => 'drop-head']));
        $ch->basic_qos(null, 10, null);
        $ch->basic_consume($queue, '', false, false, false, false, function ($m) {
            $m->ack();
        });
        $until = time() + mt_rand(60, 300);
        while ($ch->is_consuming() && time() < $until) {
            try {
                $ch->wait(null, false, 5);
            } catch (AMQPTimeoutException $e) {
            }
        }
        $c->close();
    } catch (\Throwable $e) {
        sleep(1);
    }
}
