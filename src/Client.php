<?php
/**
 * @Author SSH
 * @Email 694711507@qq.com
 * @Date 2025/8/5 00:15
 * @Description
 */
namespace ssh\Amqp;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Exchange\AMQPExchangeType;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Exception\AMQPConnectionException;
use PhpAmqpLib\Exception\AMQPChannelException;
use ssh\Amqp\Exception\ConnectionException;
use ssh\Amqp\Exception\ChannelException;
use ssh\Amqp\Exception\PublishException;
use ssh\Amqp\Exception\ConsumeException;
use ssh\Amqp\Exception\QueueException;
use ssh\Amqp\Exception\ExchangeException;

/**
 * Class Client
 * @package ssh\Amqp
 */
class Client
{
    /**
     * @var Client[]
     */
    protected static $_connections = null;

    /**
     * @var AMQPStreamConnection
     */
    protected $_connection;

    /**
     * @var AMQPChannel
     */
    protected $_channel;

    /**
     * @var array
     */
    protected $_queue = [];

    /**
     * @var array
     */
    protected $_exchange = [];

    /**
     * @var array
     */
    protected $_binding = [];

    /**
     * @var array
     */
    protected $_config = [];

    /**
     * @var int
     */
    protected $_maxReconnectAttempts = 3;

    /**
     * @var int
     */
    protected $_reconnectDelay = 1000000;

    /**
     * Client constructor.
     * @param string $host
     * @param int $port
     * @param string $user
     * @param string $password
     * @param string $vhost
     * @param array $options
     */
    public function __construct($host, $port, $user, $password, $vhost = '/', $options = [])
    {
        $this->_config = [
            'host' => $host,
            'port' => $port,
            'user' => $user,
            'password' => $password,
            'vhost' => $vhost,
            'options' => $options
        ];

        $this->_maxReconnectAttempts = $options['max_reconnect_attempts'] ?? 3;
        $this->_reconnectDelay = ($options['reconnect_delay'] ?? 1) * 1000000;

        $this->connect();
    }

    /**
     * Connect to AMQP server
     *
     * @throws ConnectionException
     */
    protected function connect()
    {
        $attempts = 0;
        $lastException = null;

        while ($attempts < $this->_maxReconnectAttempts) {
            try {
                $this->_connection = new AMQPStreamConnection(
                    $this->_config['host'],
                    $this->_config['port'],
                    $this->_config['user'],
                    $this->_config['password'],
                    $this->_config['vhost'],
                    false,
                    'AMQPLAIN',
                    null,
                    'en_US',
                    $this->_config['options']['connection_timeout'] ?? 3.0,
                    $this->_config['options']['read_write_timeout'] ?? 3.0,
                    null,
                    false,
                    $this->_config['options']['heartbeat'] ?? 0
                );

                $this->_channel = $this->_connection->channel();

                if (!empty($this->_queue)) {
                    foreach ($this->_queue as $item) {
                        call_user_func_array([$this, 'declareQueue'], $item);
                    }
                    $this->_queue = [];
                }

                if (!empty($this->_exchange)) {
                    foreach ($this->_exchange as $item) {
                        call_user_func_array([$this, 'declareExchange'], $item);
                    }
                    $this->_exchange = [];
                }

                if (!empty($this->_binding)) {
                    foreach ($this->_binding as $item) {
                        call_user_func_array([$this, 'bindQueue'], $item);
                    }
                    $this->_binding = [];
                }

                return;
            } catch (AMQPConnectionException $e) {
                $lastException = $e;
                $attempts++;
                if ($attempts < $this->_maxReconnectAttempts) {
                    usleep($this->_reconnectDelay);
                }
            } catch (\Exception $e) {
                $lastException = $e;
                $attempts++;
                if ($attempts < $this->_maxReconnectAttempts) {
                    usleep($this->_reconnectDelay);
                }
            }
        }

        $errorMessage = "Failed to connect to AMQP server after {$this->_maxReconnectAttempts} attempts. ";
        if ($lastException) {
            $errorMessage .= "Last error: " . $lastException->getMessage();
        }

        throw new ConnectionException($errorMessage, $lastException ? $lastException->getCode() : 0, $lastException);
    }

    /**
     * Reconnect to AMQP server
     *
     * @throws ConnectionException
     */
    public function reconnect()
    {
        $this->close();
        $this->connect();
    }

    /**
     * Check if connection is alive
     *
     * @return bool
     */
    public function isConnected()
    {
        try {
            return $this->_connection && $this->_connection->isConnected() && $this->_channel;
        } catch (\Exception $e) {
            return false;
        }
    }

    /**
     * @param string $queue
     * @param bool $passive
     * @param bool $durable
     * @param bool $exclusive
     * @param bool $nowait
     * @param null $arguments
     * @param int $ticket
     *
     * @throws QueueException
     */
    public function declareQueue($queue, $passive = false, $durable = false, $exclusive = false, $nowait = false, $arguments = null, $ticket = 0)
    {
        if ($this->_channel === null) {
            $this->_queue[] = func_get_args();
            return;
        }

        try {
            $this->_channel->queue_declare(
                $queue,
                $passive,
                $durable,
                $exclusive,
                $nowait,
                $arguments,
                $ticket
            );
        } catch (AMQPChannelException $e) {
            throw new QueueException(
                "Failed to declare queue '{$queue}': " . $e->getMessage(),
                $e->getCode(),
                $e
            );
        } catch (\Exception $e) {
            throw new QueueException(
                "Failed to declare queue '{$queue}': " . $e->getMessage(),
                $e->getCode(),
                $e
            );
        }
    }

    /**
     * @param string $exchange
     * @param string $type
     * @param bool $passive
     * @param bool $durable
     * @param bool $auto_delete
     * @param bool $internal
     * @param bool $nowait
     * @param null $arguments
     * @param int $ticket
     *
     * @throws ExchangeException
     */
    public function declareExchange($exchange, $type = AMQPExchangeType::DIRECT, $passive = false, $durable = false, $auto_delete = false, $internal = false, $nowait = false, $arguments = null, $ticket = 0)
    {
        if ($this->_channel === null) {
            $this->_exchange[] = func_get_args();
            return;
        }

        try {
            $this->_channel->exchange_declare(
                $exchange,
                $type,
                $passive,
                $durable,
                $auto_delete,
                $internal,
                $nowait,
                $arguments,
                $ticket
            );
        } catch (AMQPChannelException $e) {
            throw new ExchangeException(
                "Failed to declare exchange '{$exchange}': " . $e->getMessage(),
                $e->getCode(),
                $e
            );
        } catch (\Exception $e) {
            throw new ExchangeException(
                "Failed to declare exchange '{$exchange}': " . $e->getMessage(),
                $e->getCode(),
                $e
            );
        }
    }

    /**
     * @param string $queue
     * @param string $exchange
     * @param string $routing_key
     * @param bool $nowait
     * @param null $arguments
     * @param int $ticket
     *
     * @throws QueueException
     */
    public function bindQueue($queue, $exchange, $routing_key = '', $nowait = false, $arguments = null, $ticket = 0)
    {
        if ($this->_channel === null) {
            $this->_binding[] = func_get_args();
            return;
        }

        try {
            $this->_channel->queue_bind(
                $queue,
                $exchange,
                $routing_key,
                $nowait,
                $arguments,
                $ticket
            );
        } catch (AMQPChannelException $e) {
            throw new QueueException(
                "Failed to bind queue '{$queue}' to exchange '{$exchange}': " . $e->getMessage(),
                $e->getCode(),
                $e
            );
        } catch (\Exception $e) {
            throw new QueueException(
                "Failed to bind queue '{$queue}' to exchange '{$exchange}': " . $e->getMessage(),
                $e->getCode(),
                $e
            );
        }
    }

    /**
     * @param string $queue
     * @param string $exchange
     * @param string $routing_key
     * @param AMQPMessage|null $msg
     * @param bool $mandatory
     * @param bool $immediate
     * @param int $ticket
     * @param callable|null $onSuccess
     * @param callable|null $onError
     *
     * @return bool
     *
     * @throws PublishException
     * @throws ConnectionException
     */
    public function publish($queue, $exchange, $routing_key = '', $msg = null, $mandatory = false, $immediate = false, $ticket = 0, $onSuccess = null, $onError = null)
    {
        if ($msg === null) {
            $msg = new AMQPMessage('');
        } elseif (is_string($msg)) {
            $msg = new AMQPMessage($msg);
        }

        try {
            if (!$this->isConnected()) {
                $this->reconnect();
            }

            $this->_channel->basic_publish(
                $msg,
                $exchange,
                $routing_key,
                $mandatory,
                $immediate,
                $ticket
            );

            if ($onSuccess) {
                $onSuccess($msg, $queue, $exchange, $routing_key);
            }

            return true;
        } catch (ConnectionException $e) {
            if ($onError) {
                $onError($e, $msg, $queue, $exchange, $routing_key);
            }
            throw $e;
        } catch (AMQPChannelException $e) {
            $publishException = new PublishException(
                "Failed to publish message: " . $e->getMessage(),
                $e->getCode(),
                $e
            );
            if ($onError) {
                $onError($publishException, $msg, $queue, $exchange, $routing_key);
            }
            throw $publishException;
        } catch (\Exception $e) {
            if (!$this->isConnected()) {
                try {
                    $this->reconnect();
                    $this->_channel->basic_publish(
                        $msg,
                        $exchange,
                        $routing_key,
                        $mandatory,
                        $immediate,
                        $ticket
                    );

                    if ($onSuccess) {
                        $onSuccess($msg, $queue, $exchange, $routing_key);
                    }

                    return true;
                } catch (\Exception $reconnectException) {
                    $publishException = new PublishException(
                        "Failed to publish message after reconnect: " . $reconnectException->getMessage(),
                        $reconnectException->getCode(),
                        $reconnectException
                    );
                    if ($onError) {
                        $onError($publishException, $msg, $queue, $exchange, $routing_key);
                    }
                    throw $publishException;
                }
            } else {
                $publishException = new PublishException(
                    "Failed to publish message: " . $e->getMessage(),
                    $e->getCode(),
                    $e
                );
                if ($onError) {
                    $onError($publishException, $msg, $queue, $exchange, $routing_key);
                }
                throw $publishException;
            }
        }
    }

    /**
     * @param string $consumer_tag
     *
     * @throws ChannelException
     */
    public function cancel($consumer_tag)
    {
        try {
            $this->_channel->basic_cancel($consumer_tag);
        } catch (AMQPChannelException $e) {
            throw new ChannelException(
                "Failed to cancel consumer: " . $e->getMessage(),
                $e->getCode(),
                $e
            );
        } catch (\Exception $e) {
            throw new ChannelException(
                "Failed to cancel consumer: " . $e->getMessage(),
                $e->getCode(),
                $e
            );
        }
    }

    /**
     * @param string $queue
     * @param string $consumer_tag
     * @param bool $no_local
     * @param bool $no_ack
     * @param bool $exclusive
     * @param bool $nowait
     * @param callable|null $callback
     * @param int $ticket
     * @param null $arguments
     * @return mixed
     *
     * @throws ConsumeException
     */
    public function consume($queue, $consumer_tag = '', $no_local = false, $no_ack = false, $exclusive = false, $nowait = false, $callback = null, $ticket = 0, $arguments = null)
    {
        try {
            return $this->_channel->basic_consume(
                $queue,
                $consumer_tag,
                $no_local,
                $no_ack,
                $exclusive,
                $nowait,
                $callback,
                $ticket,
                $arguments
            );
        } catch (AMQPChannelException $e) {
            throw new ConsumeException(
                "Failed to start consuming from queue '{$queue}': " . $e->getMessage(),
                $e->getCode(),
                $e
            );
        } catch (\Exception $e) {
            throw new ConsumeException(
                "Failed to start consuming from queue '{$queue}': " . $e->getMessage(),
                $e->getCode(),
                $e
            );
        }
    }

    /**
     * @param AMQPMessage|null $msg
     * @param bool $multiple
     * @param bool $requeue
     *
     * @throws ChannelException
     */
    public function ack($msg, $multiple = false, $requeue = false)
    {
        try {
            if ($requeue) {
                $this->_channel->basic_nack($msg->getDeliveryTag(), $multiple, $requeue);
            } else {
                $this->_channel->basic_ack($msg->getDeliveryTag(), $multiple);
            }
        } catch (AMQPChannelException $e) {
            throw new ChannelException(
                "Failed to ack message: " . $e->getMessage(),
                $e->getCode(),
                $e
            );
        } catch (\Exception $e) {
            throw new ChannelException(
                "Failed to ack message: " . $e->getMessage(),
                $e->getCode(),
                $e
            );
        }
    }

    /**
     * @param AMQPMessage|null $msg
     * @param bool $multiple
     * @param bool $requeue
     *
     * @throws ChannelException
     */
    public function nack($msg, $multiple = false, $requeue = false)
    {
        try {
            $this->_channel->basic_nack($msg->getDeliveryTag(), $multiple, $requeue);
        } catch (AMQPChannelException $e) {
            throw new ChannelException(
                "Failed to nack message: " . $e->getMessage(),
                $e->getCode(),
                $e
            );
        } catch (\Exception $e) {
            throw new ChannelException(
                "Failed to nack message: " . $e->getMessage(),
                $e->getCode(),
                $e
            );
        }
    }

    /**
     * @param AMQPMessage $msg
     * @param bool $requeue
     *
     * @throws ChannelException
     */
    public function reject($msg, $requeue = false)
    {
        try {
            $this->_channel->basic_reject($msg->getDeliveryTag(), $requeue);
        } catch (AMQPChannelException $e) {
            throw new ChannelException(
                "Failed to reject message: " . $e->getMessage(),
                $e->getCode(),
                $e
            );
        } catch (\Exception $e) {
            throw new ChannelException(
                "Failed to reject message: " . $e->getMessage(),
                $e->getCode(),
                $e
            );
        }
    }

    /**
     * @param int|null $consumer_tag
     * @param int $prefetch_count
     * @param int $prefetch_size
     * @param bool $global
     *
     * @throws ChannelException
     */
    public function qos($consumer_tag = null, $prefetch_count = 0, $prefetch_size = 0, $global = false)
    {
        try {
            $this->_channel->basic_qos($prefetch_size, $prefetch_count, $global);
        } catch (AMQPChannelException $e) {
            throw new ChannelException(
                "Failed to set QoS: " . $e->getMessage(),
                $e->getCode(),
                $e
            );
        } catch (\Exception $e) {
            throw new ChannelException(
                "Failed to set QoS: " . $e->getMessage(),
                $e->getCode(),
                $e
            );
        }
    }

    /**
     * @return AMQPChannel
     */
    public function getChannel()
    {
        return $this->_channel;
    }

    /**
     * @return AMQPStreamConnection
     */
    public function getConnection()
    {
        return $this->_connection;
    }

    /**
     * @param float|null $timeout
     *
     * @return bool
     */
    public function wait($timeout = null)
    {
        try {
            $this->_channel->wait(null, false, $timeout);
            return true;
        } catch (AMQPTimeoutException $e) {
            return false;
        } catch (AMQPChannelException $e) {
            throw new ChannelException(
                "Error waiting for message: " . $e->getMessage(),
                $e->getCode(),
                $e
            );
        } catch (\Exception $e) {
            throw new ChannelException(
                "Error waiting for message: " . $e->getMessage(),
                $e->getCode(),
                $e
            );
        }
    }

    /**
     *
     */
    public function close()
    {
        try {
            if ($this->_channel !== null) {
                $this->_channel->close();
                $this->_channel = null;
            }
        } catch (\Exception $e) {
            $this->_channel = null;
        }

        try {
            if ($this->_connection !== null) {
                $this->_connection->close();
                $this->_connection = null;
            }
        } catch (\Exception $e) {
            $this->_connection = null;
        }
    }

    /**
     * @param string $name
     * @return Client
     */
    public static function connection($name = 'default', $config = null)
    {
        if (!isset(static::$_connections[$name])) {
            if (empty($config)) {
                $config = config('rabbitmq', config('plugin.rabbitmq.rabbitmq', []));
            } else {
                $config = config($config, []);
            }

            if (!isset($config[$name])) {
                throw new \RuntimeException("AMQP connection $name not found");
            }

            $host = $config[$name]['host'];
            $port = $config[$name]['port'] ?? 5672;
            $user = $config[$name]['user'] ?? 'guest';
            $password = $config[$name]['password'] ?? 'guest';
            $vhost = $config[$name]['vhost'] ?? '/';
            $options = $config[$name]['options'] ?? [];

            $client = new static($host, $port, $user, $password, $vhost, $options);
            static::$_connections[$name] = $client;
        }

        return static::$_connections[$name];
    }

    /**
     * @param string $queue
     * @param string $body
     * @param string $connection
     * @param string|null $config
     * @param array $properties
     * @param string $exchange
     * @param string $routing_key
     * @param callable|null $onSuccess
     * @param callable|null $onError
     *
     * @return bool
     *
     * @throws PublishException
     * @throws ConnectionException
     */
    public static function send($queue, $body, $connection = 'default', $config = null, $properties = [], $exchange = '', $routing_key = null, $onSuccess = null, $onError = null)
    {
        $msg = new AMQPMessage($body, $properties);
        
        // 如果没有指定 routing_key，使用队列名
        if ($routing_key === null) {
            $routing_key = $queue;
        }
        
        return static::connection($connection, $config)->publish($queue, $exchange, $routing_key, $msg, false, false, 0, $onSuccess, $onError);
    }

    /**
     * @param $name
     * @param $arguments
     * @return mixed
     */
    public static function __callStatic($name, $arguments)
    {
        return static::connection()->{$name}(... $arguments);
    }

    /**
     * @param string $name
     * @param mixed $arguments
     * @return mixed
     */
    public function __call($name, $arguments)
    {
        return $this->_channel->{$name}(... $arguments);
    }

    /**
     * Destructor
     */
    public function __destruct()
    {
        $this->close();
    }
}
