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
        $this->_connection = new AMQPStreamConnection(
            $host,
            $port,
            $user,
            $password,
            $vhost,
            false,
            'AMQPLAIN',
            null,
            'en_US',
            $options['connection_timeout'] ?? 3.0,
            $options['read_write_timeout'] ?? 3.0,
            null,
            false,
            $options['heartbeat'] ?? 0
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
    }

    /**
     * @param string $queue
     * @param bool $passive
     * @param bool $durable
     * @param bool $exclusive
     * @param bool $nowait
     * @param null $arguments
     * @param int $ticket
     */
    public function declareQueue($queue, $passive = false, $durable = false, $exclusive = false, $nowait = false, $arguments = null, $ticket = 0)
    {
        if ($this->_channel === null) {
            $this->_queue[] = func_get_args();
            return;
        }

        $this->_channel->queue_declare(
            $queue,
            $passive,
            $durable,
            $exclusive,
            $nowait,
            $arguments,
            $ticket
        );
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
     */
    public function declareExchange($exchange, $type = AMQPExchangeType::DIRECT, $passive = false, $durable = false, $auto_delete = false, $internal = false, $nowait = false, $arguments = null, $ticket = 0)
    {
        if ($this->_channel === null) {
            $this->_exchange[] = func_get_args();
            return;
        }

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
    }

    /**
     * @param string $queue
     * @param string $exchange
     * @param string $routing_key
     * @param bool $nowait
     * @param null $arguments
     * @param int $ticket
     */
    public function bindQueue($queue, $exchange, $routing_key = '', $nowait = false, $arguments = null, $ticket = 0)
    {
        if ($this->_channel === null) {
            $this->_binding[] = func_get_args();
            return;
        }

        $this->_channel->queue_bind(
            $queue,
            $exchange,
            $routing_key,
            $nowait,
            $arguments,
            $ticket
        );
    }

    /**
     * @param string $queue
     * @param string $exchange
     * @param string $routing_key
     * @param AMQPMessage|null $msg
     * @param bool $mandatory
     * @param bool $immediate
     * @param int $ticket
     */
    public function publish($queue, $exchange, $routing_key = '', $msg = null, $mandatory = false, $immediate = false, $ticket = 0)
    {
        if ($msg === null) {
            $msg = new AMQPMessage('');
        } elseif (is_string($msg)) {
            $msg = new AMQPMessage($msg);
        }

        $this->_channel->basic_publish(
            $msg,
            $exchange,
            $routing_key,
            $mandatory,
            $immediate,
            $ticket
        );
    }

    /**
     * @param string $consumer_tag
     */
    public function cancel($consumer_tag)
    {
        $this->_channel->basic_cancel($consumer_tag);
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
     */
    public function consume($queue, $consumer_tag = '', $no_local = false, $no_ack = false, $exclusive = false, $nowait = false, $callback = null, $ticket = 0, $arguments = null)
    {
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
    }

    /**
     * @param AMQPMessage|null $msg
     * @param bool $multiple
     * @param bool $requeue
     */
    public function ack($msg, $multiple = false, $requeue = false)
    {
        if ($requeue) {
            $this->_channel->basic_nack($msg->getDeliveryTag(), $multiple, $requeue);
        } else {
            $this->_channel->basic_ack($msg->getDeliveryTag(), $multiple);
        }
    }

    /**
     * @param AMQPMessage|null $msg
     * @param bool $multiple
     * @param bool $requeue
     */
    public function nack($msg, $multiple = false, $requeue = false)
    {
        $this->_channel->basic_nack($msg->getDeliveryTag(), $multiple, $requeue);
    }

    /**
     * @param AMQPMessage $msg
     * @param bool $requeue
     */
    public function reject($msg, $requeue = false)
    {
        $this->_channel->basic_reject($msg->getDeliveryTag(), $requeue);
    }

    /**
     * @param int|null $consumer_tag
     * @param int $prefetch_count
     * @param int $prefetch_size
     * @param bool $global
     */
    public function qos($consumer_tag = null, $prefetch_count = 0, $prefetch_size = 0, $global = false)
    {
        $this->_channel->basic_qos($prefetch_size, $prefetch_count, $global);
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
     */
    public function wait($timeout = null)
    {
        $this->_channel->wait(null, false, $timeout);
    }

    /**
     *
     */
    public function close()
    {
        if ($this->_channel !== null) {
            $this->_channel->close();
            $this->_channel = null;
        }

        if ($this->_connection !== null) {
            $this->_connection->close();
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
                $config = config('amqp', config('plugin.webman.amqp.amqp', []));
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
     * @param array $properties
     * @param string $exchange
     * @param string $routing_key
     */
    public static function send($queue, $body, $properties = [], $exchange = '', $routing_key = '')
    {
        $msg = new AMQPMessage($body, $properties);
        static::connection('default')->publish($queue, $exchange, $routing_key, $msg);
    }

    /**
     * @param $name
     * @param $arguments
     * @return mixed
     */
    public static function __callStatic($name, $arguments)
    {
        return static::connection('default')->{$name}(... $arguments);
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
}
