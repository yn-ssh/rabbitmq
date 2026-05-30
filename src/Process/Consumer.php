<?php
/**
 * @Author SSH
 * @Email 694711507@qq.com
 * @Date 2025/8/5 00:15
 * @Description
 */

namespace ssh\Amqp\Process;

use ssh\Amqp\Client;
use ssh\Amqp\Exception\AmqpException;
use ssh\Amqp\Exception\ConnectionException;
use support\Container;
use Psr\Log\LoggerInterface;

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
     * @var LoggerInterface|null
     */
    protected $_logger = null;

    /**
     * @var Client[]
     */
    protected $_connections = [];

    /**
     * Consumer constructor.
     * @param string $consumer_dir
     */
    public function __construct($consumer_dir = '')
    {
        $this->_consumerDir = $consumer_dir;
    }

    /**
     * Set logger
     *
     * @param LoggerInterface $logger
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->_logger = $logger;
    }

    /**
     * Log message
     *
     * @param string $level
     * @param string $message
     * @param array $context
     */
    protected function log($level, $message, array $context = [])
    {
        if ($this->_logger) {
            $this->_logger->$level($message, $context);
        }
    }

    /**
     * onWorkerStart.
     */
    public function onWorkerStart()
    {
        try {
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

                    try {
                        $this->setupConsumer($class);
                    } catch (AmqpException $e) {
                        $this->log('error', "Failed to setup consumer {$class}: " . $e->getMessage(), [
                            'exception' => $e,
                            'class' => $class
                        ]);
                    } catch (\Exception $e) {
                        $this->log('error', "Unexpected error setting up consumer {$class}: " . $e->getMessage(), [
                            'exception' => $e,
                            'class' => $class
                        ]);
                    }
                }
            }

            $this->log('info', "All consumers setup completed, starting message loop");

            $this->startMessageLoop();

        } catch (\Exception $e) {
            $this->log('error', "Error loading consumers: " . $e->getMessage(), [
                'exception' => $e
            ]);
        }
    }

    /**
     * Start the message consumption loop
     */
    protected function startMessageLoop()
    {
        while (true) {
            try {
                foreach ($this->_connections as $connection_name => $connection) {
                    if (!$connection->isConnected()) {
                        $this->log('warning', "Connection {$connection_name} is disconnected, attempting to reconnect");
                        try {
                            $connection->reconnect();
                            $this->log('info', "Connection {$connection_name} reconnected successfully");
                        } catch (ConnectionException $e) {
                            $this->log('error', "Failed to reconnect to {$connection_name}: " . $e->getMessage(), [
                                'exception' => $e
                            ]);
                            continue;
                        }
                    }

                    try {
                        $connection->wait(1);
                    } catch (\Exception $e) {
                        $this->log('error', "Error waiting for messages on connection {$connection_name}: " . $e->getMessage(), [
                            'exception' => $e
                        ]);
                    }
                }
            } catch (\Exception $e) {
                $this->log('error', "Error in message loop: " . $e->getMessage(), [
                    'exception' => $e
                ]);
                sleep(1);
            }
        }
    }

    /**
     * Setup a single consumer
     *
     * @param string $class
     *
     * @throws AmqpException
     */
    protected function setupConsumer($class)
    {
        $consumer = Container::get($class);
        $connection_name = $consumer->connection ?? 'default';
        $queue = $consumer->queue;
        $no_ack = $consumer->no_ack ?? false;
        $config = $consumer->config ?? '';

        $this->log('info', "Setting up consumer {$class} for queue {$queue}", [
            'class' => $class,
            'queue' => $queue,
            'connection' => $connection_name
        ]);

        $connection = Client::connection($connection_name, $config);

        if (!isset($this->_connections[$connection_name])) {
            $this->_connections[$connection_name] = $connection;
        }

        if (isset($consumer->exchange)) {
            $exchange = $consumer->exchange;
            $exchange_type = $consumer->exchange_type ?? 'direct';
            $exchange_durable = $consumer->exchange_durable ?? true;

            $this->log('debug', "Declaring exchange {$exchange} for consumer {$class}", [
                'exchange' => $exchange,
                'type' => $exchange_type
            ]);

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

        $this->log('debug', "Declaring queue {$queue} for consumer {$class}", [
            'queue' => $queue,
            'durable' => $queue_durable
        ]);

        $connection->declareQueue($queue, false, $queue_durable, $queue_exclusive, false, $queue_arguments);

        if (isset($consumer->exchange)) {
            $this->log('debug', "Binding queue {$queue} to exchange {$exchange} with routing key {$routing_key}", [
                'queue' => $queue,
                'exchange' => $exchange,
                'routing_key' => $routing_key
            ]);

            $connection->bindQueue($queue, $exchange, $routing_key);
        }

        $prefetch_count = $consumer->prefetch_count ?? 1;
        if ($prefetch_count > 0) {
            $connection->qos(0, $prefetch_count);
        }

        $cb = function ($msg) use ($consumer, $connection, $no_ack, $class, $queue) {
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

            try {
                $this->log('debug', "Processing message from queue {$queue} with consumer {$class}", [
                    'queue' => $queue,
                    'class' => $class,
                    'delivery_tag' => $msg->getDeliveryTag()
                ]);

                \call_user_func(
                    [$consumer, 'consume'],
                    $msg->body,
                    $properties,
                    $msg,
                    $connection
                );
            } catch (\Exception $e) {
                $this->log('error', "Error processing message in consumer {$class}: " . $e->getMessage(), [
                    'exception' => $e,
                    'class' => $class,
                    'queue' => $queue,
                    'delivery_tag' => $msg->getDeliveryTag()
                ]);

                if (!$no_ack) {
                    try {
                        $requeue = method_exists($consumer, 'shouldRequeue') ? $consumer->shouldRequeue($e) : true;
                        $connection->nack($msg, false, $requeue);

                        $this->log('info', "Message nacked, requeue: " . ($requeue ? 'yes' : 'no'), [
                            'delivery_tag' => $msg->getDeliveryTag(),
                            'requeue' => $requeue
                        ]);
                    } catch (AmqpException $nackException) {
                        $this->log('error', "Failed to nack message: " . $nackException->getMessage(), [
                            'exception' => $nackException,
                            'delivery_tag' => $msg->getDeliveryTag()
                        ]);
                    }
                }
            }
        };

        $connection->consume($queue, '', false, $no_ack, false, false, $cb);

        $this->log('info', "Consumer {$class} started successfully for queue {$queue}", [
            'class' => $class,
            'queue' => $queue
        ]);
    }
}
