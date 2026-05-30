<?php
/**
 * @Author SSH
 * @Email 694711507@qq.com
 * @Date 2025/8/5 00:15
 * @Description
 */

namespace ssh\Amqp\Process;

use support\Container;
use ssh\Amqp\Client;

/**
 * Class AmqpConsumer
 * @package process
 */
class Consumer
{
    /**
     * @var string
     */
    protected $_consumerDir = '';

    /**
     * Consumer constructor.
     * @param string $consumer_dir
     */
    public function __construct($consumer_dir = '')
    {
        $this->_consumerDir = $consumer_dir;
    }

    /**
     * onWorkerStart.
     */
    public function onWorkerStart()
    {
        $dir_iterator = new \RecursiveDirectoryIterator($this->_consumerDir);
        $iterator = new \RecursiveIteratorIterator($dir_iterator);

        foreach ($iterator as $file) {
            if (is_dir($file)) {
                continue;
            }

            $fileinfo = new \SplFileInfo($file);
            $ext = $fileinfo->getExtension();

            if ($ext === 'php') {
                $class = str_replace('/', "\\", substr(substr($file, strlen(base_path())), 0, -4));

                if (!is_a($class, 'ssh\Amqp\Consumer', true)) {
                    continue;
                }

                $consumer = Container::get($class);
                $connection_name = $consumer->connection ?? 'default';
                $queue = $consumer->queue;
                $no_ack = $consumer->no_ack ?? false;
                $config = $consumer->config ?? '';

                $connection = Client::connection($connection_name, $config);

                if (isset($consumer->exchange)) {
                    $exchange = $consumer->exchange;
                    $exchange_type = $consumer->exchange_type ?? 'direct';
                    $exchange_durable = $consumer->exchange_durable ?? true;
                    $connection->declareExchange($exchange, $exchange_type, false, $exchange_durable);
                }

                if (isset($consumer->routing_key)) {
                    $routing_key = $consumer->routing_key;
                } else {
                    $routing_key = $queue;
                }

                $queue_durable = $consumer->queue_durable ?? true;
                $queue_exclusive = $consumer->queue_exclusive ?? false;
                $queue_auto_delete = $consumer->queue_auto_delete ?? false;
                $queue_arguments = $consumer->queue_arguments ?? null;

                $connection->declareQueue($queue, false, $queue_durable, $queue_exclusive, false, $queue_arguments);

                if (isset($consumer->exchange)) {
                    $connection->bindQueue($queue, $exchange, $routing_key);
                }

                $prefetch_count = $consumer->prefetch_count ?? 1;
                if ($prefetch_count > 0) {
                    $connection->qos(0, $prefetch_count);
                }

                $cb = function ($msg) use ($consumer, $connection, $no_ack) {
                    $properties = [];
                    if ($msg->has('content_type')) {
                        $properties['content_type'] = $msg->get('content_type');
                    }
                    if ($msg->has('content_encoding')) {
                        $properties['content_encoding'] = $msg->get('content_encoding');
                    }
                    if ($msg->has('delivery_mode')) {
                        $properties['delivery_mode'] = $msg->get('delivery_mode');
                    }
                    if ($msg->has('priority')) {
                        $properties['priority'] = $msg->get('priority');
                    }
                    if ($msg->has('correlation_id')) {
                        $properties['correlation_id'] = $msg->get('correlation_id');
                    }
                    if ($msg->has('reply_to')) {
                        $properties['reply_to'] = $msg->get('reply_to');
                    }
                    if ($msg->has('expiration')) {
                        $properties['expiration'] = $msg->get('expiration');
                    }
                    if ($msg->has('message_id')) {
                        $properties['message_id'] = $msg->get('message_id');
                    }
                    if ($msg->has('timestamp')) {
                        $properties['timestamp'] = $msg->get('timestamp');
                    }
                    if ($msg->has('type')) {
                        $properties['type'] = $msg->get('type');
                    }
                    if ($msg->has('user_id')) {
                        $properties['user_id'] = $msg->get('user_id');
                    }
                    if ($msg->has('app_id')) {
                        $properties['app_id'] = $msg->get('app_id');
                    }

                    \call_user_func(
                        [$consumer, 'consume'],
                        $msg->body,
                        $properties,
                        $msg->getDeliveryTag(),
                        $connection
                    );
                };

                $connection->consume($queue, '', false, $no_ack, false, false, $cb);
            }
        }
    }
}
