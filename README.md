# AMQP

AMQP library wrapper for php-amqplib，基于 php-amqplib 封装的 RabbitMQ 客户端组件。

## 安装

```bash
composer require ssh/amqp
```

或者手动安装：

```bash
cd /Users/wrkj/Workspace/Php/2025/ssh/rabbitmq
composer install
```

## 配置

在 webman 框架中创建配置文件 `config/plugin/webman/amqp/amqp.php`：

```php
<?php
return [
    'default' => [
        'host' => 'localhost',
        'port' => 5672,
        'user' => 'guest',
        'password' => 'guest',
        'vhost' => '/',
        'options' => [
            'connection_timeout' => 3.0,
            'read_write_timeout' => 3.0,
            'heartbeat' => 0,
            'max_reconnect_attempts' => 3, // 最大重连次数
            'reconnect_delay' => 1, // 重连间隔（秒）
        ],
    ],
    'cluster' => [
        'host' => '192.168.1.100',
        'port' => 5672,
        'user' => 'admin',
        'password' => 'admin123',
        'vhost' => '/',
        'options' => [
            'max_reconnect_attempts' => 5,
            'reconnect_delay' => 2,
        ],
    ],
];
```

## 发送消息

### 方式一：静态调用

```php
use ssh\Amqp\Exception\Client;

// 发送字符串消息
Client::send('my_queue', 'Hello World!');

// 发送 JSON 数据
Client::send('my_queue', json_encode(['id' => 1, 'name' => 'test']));
```

### 方式二：实例调用

```php
use ssh\Amqp\Exception\Client;

$client = Client::connection('default');

// 发送消息到默认交换机
$client->publish('my_queue', '', 'my_routing_key', 'Hello AMQP!');

// 发送消息到指定交换机
$client->publish('my_queue', 'my_exchange', 'my_routing_key', 'Hello AMQP!');
```

### 方式三：发送带属性的消息

```php
use ssh\Amqp\Exception\Client;
use PhpAmqpLib\Message\AMQPMessage;

$msg = new AMQPMessage('Hello World!', [
    'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
    'content_type' => 'application/json',
    'priority' => 5,
]);

$client = Client::connection('default');
$client->publish('my_queue', 'my_exchange', 'my_routing_key', $msg);
```

## 消费消息

### 创建消费者类

创建一个实现 `Consumer` 接口的类：

```php
<?php
namespace app\amqp;

use ssh\Amqp\Exception\Client;
use ssh\Amqp\Exception\Consumer;
use PhpAmqpLib\Message\AMQPMessage;

class MyConsumer implements Consumer
{
    // 连接名称，默认 'default'
    public $connection = 'default';

    // 队列名称
    public $queue = 'my_queue';

    // 是否自动确认，false 表示需要手动 ack
    public $no_ack = false;

    // 预取消息数量
    public $prefetch_count = 1;

    /**
     * 消费消息
     *
     * @param string $data 消息内容
     * @param array $properties 消息属性
     * @param AMQPMessage $msg AMQP 消息对象
     * @param Client $client AMQP 客户端实例
     */
    public function consume($data, $properties, $msg, $client)
    {
        // 处理消息
        echo "Received: {$data}\n";

        // 手动确认消息
        $client->ack($msg);

        // 或者拒绝消息并重新入队
        // $client->nack($msg, false, true);
    }
}
```

### 带交换机和路由键的消费者

```php
<?php
namespace app\amqp;

use ssh\Amqp\Exception\Client;
use ssh\Amqp\Exception\Consumer;
use PhpAmqpLib\Message\AMQPMessage;

class ExchangeConsumer implements Consumer
{
    public $connection = 'default';

    // 交换机名称
    public $exchange = 'my_exchange';

    // 交换机类型：direct, fanout, topic
    public $exchange_type = 'direct';
    public $exchange_durable = true;

    // 队列名称
    public $queue = 'my_queue';
    public $queue_durable = true;

    // 路由键
    public $routing_key = 'my_routing_key';

    // 消息确认模式
    public $no_ack = false;

    // 预取数量
    public $prefetch_count = 10;

    public function consume($data, $properties, $msg, $client)
    {
        echo "Received: {$data}\n";

        // 处理业务逻辑
        try {
            // 处理成功，确认消息
            $client->ack($msg);
        } catch (\Exception $e) {
            // 处理失败，拒绝消息并重新入队
            $client->nack($msg, false, true);
        }
    }
}
```

## 在 webman 中配置消费者进程

在 `config/plugin/webman/amqp/process.php` 中添加：

```php
<?php
return [
    // 消费者进程
    ssh\Amqp\Exception\Process\Consumer::class => [
        'consumer_dir' => base_path() . '/app/amqp', // 消费者类所在目录
    ],
];
```

### 消费者工作流程

当消费者进程启动后，会执行以下步骤：

1. **扫描消费者类**：扫描指定目录下的所有 PHP 文件，查找实现了 `ssh\Amqp\Consumer` 接口的类
2. **设置消费者**：为每个消费者类创建 AMQP 连接、声明交换机和队列、绑定关系
3. **启动消息循环**：进入无限循环，持续监听消息并调用相应的消费者处理
4. **自动重连**：如果连接断开，会自动尝试重连
5. **异常处理**：消息处理过程中的异常会被捕获并记录，同时根据配置决定是否重新入队

## 消费者接口说明

### 必须实现的方法

```php
interface Consumer
{
    /**
     * 返回队列名称
     *
     * @return string
     */
    public function queue();

    /**
     * 消费消息
     *
     * @param string $data 消息内容
     * @param array $properties 消息属性
     * @param AMQPMessage $msg AMQP 消息对象
     * @param Client $client AMQP 客户端
     */
    public function consume($data, $properties, $msg, $client);
}
```

### 可选属性

- `connection`: 连接名称，默认 'default'
- `exchange`: 交换机名称（可选）
- `exchange_type`: 交换机类型，默认 'direct'
- `exchange_durable`: 交换机是否持久化，默认 true
- `routing_key`: 路由键（可选）
- `queue_durable`: 队列是否持久化，默认 true
- `queue_exclusive`: 队列是否独占，默认 false
- `queue_auto_delete`: 队列是否自动删除，默认 false
- `queue_arguments`: 队列额外参数
- `no_ack`: 是否自动确认，默认 false
- `prefetch_count`: 预取消息数量，默认 1
- `config`: 配置名称（可选）

### 可选方法

- `shouldRequeue(\Exception $e)`: 判断处理失败的消息是否应该重新入队，返回 true 表示重新入队，false 表示丢弃。默认返回 true。

## 高级用法

### 手动声明交换机和队列

```php
use ssh\Amqp\Exception\Client;
use PhpAmqpLib\Exchange\AMQPExchangeType;

$client = Client::connection('default');

// 声明交换机
$client->declareExchange(
    'my_exchange',
    AMQPExchangeType::DIRECT,
    false,  // passive
    true,   // durable
    false   // auto_delete
);

// 声明队列
$client->declareQueue(
    'my_queue',
    false,  // passive
    true,   // durable
    false,  // exclusive
    false,  // nowait
    null    // arguments
);

// 绑定队列到交换机
$client->bindQueue(
    'my_queue',
    'my_exchange',
    'my_routing_key'
);
```

### 批量消费消息

```php
use ssh\Amqp\Exception\Client;

$client = Client::connection('default');

// 设置预取数量
$client->qos(0, 10);

// 开始消费
$client->consume('my_queue', '', false, false, false, false, function ($msg) {
    echo "Received: {$msg->body}\n";
});

// 保持运行
while (true) {
    $client->wait();
}
```

### 消息属性

消费者接收的 `$properties` 数组包含以下可能的字段：

- `content_type`: 内容类型
- `content_encoding`: 内容编码
- `delivery_mode`: 投递模式
- `priority`: 优先级
- `correlation_id`: 关联 ID
- `reply_to`: 回复地址
- `expiration`: 过期时间
- `message_id`: 消息 ID
- `timestamp`: 时间戳
- `type`: 消息类型
- `user_id`: 用户 ID
- `app_id`: 应用 ID

## 异常处理

### 异常类

组件提供了以下异常类：

- `ssh\Amqp\Exception\AmqpException`: 所有 AMQP 异常的基类
- `ssh\Amqp\Exception\ConnectionException`: 连接相关的异常
- `ssh\Amqp\Exception\ChannelException`: 通道相关的异常
- `ssh\Amqp\Exception\PublishException`: 发布消息相关的异常
- `ssh\Amqp\Exception\ConsumeException`: 消费消息相关的异常
- `ssh\Amqp\Exception\QueueException`: 队列相关的异常
- `ssh\Amqp\Exception\ExchangeException`: 交换机相关的异常

### 发送消息时的异常处理

```php
use ssh\Amqp\Exception\Client;
use ssh\Amqp\Exception\Exception\ConnectionException;
use ssh\Amqp\Exception\Exception\PublishException;

try {
    Client::send('my_queue', 'Hello World!');
} catch (ConnectionException $e) {
    // 处理连接异常
    echo "连接失败: " . $e->getMessage() . "\n";
    // 可以尝试重试或降级处理
} catch (PublishException $e) {
    // 处理发布异常
    echo "发布消息失败: " . $e->getMessage() . "\n";
}
```

### 消费者中的异常处理

消费者进程会自动处理消息处理过程中的异常，并记录日志。如果消费者实现了 `shouldRequeue` 方法，可以控制失败的消息是否重新入队：

```php
<?php
namespace app\amqp;

use ssh\Amqp\Exception\Client;
use ssh\Amqp\Exception\Consumer;
use PhpAmqpLib\Message\AMQPMessage;

class MyConsumer implements Consumer
{
    public $connection = 'default';
    public $queue = 'my_queue';
    public $no_ack = false;
    public $prefetch_count = 1;

    /**
     * 判断处理失败的消息是否应该重新入队
     *
     * @param \Exception $e
     * @return bool
     */
    public function shouldRequeue(\Exception $e)
    {
        // 对于数据库连接失败等临时问题，重新入队
        if (strpos($e->getMessage(), 'connection') !== false) {
            return true;
        }

        // 对于其他异常（如格式错误），不重新入队
        return false;
    }

    public function consume($data, $properties, $msg, $client)
    {
        // 处理消息
        $client->ack($msg);
    }
}
```

### 连接管理

Client 类提供了以下方法来管理连接：

```php
use ssh\Amqp\Exception\Client;

$client = Client::connection('default');

// 检查连接是否正常
if ($client->isConnected()) {
    echo "连接正常\n";
}

// 手动重连
$client->reconnect();

// 关闭连接
$client->close();
```

### 配置日志

在消费者进程中可以配置日志记录器：

```php
use ssh\Amqp\Exception\Process\Consumer;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

$logger = new Logger('amqp');
$logger->pushHandler(new StreamHandler('path/to/log/file.log', Logger::DEBUG));

$consumer = new Consumer(base_path() . '/app/amqp');
$consumer->setLogger($logger);
```

## API 参考

### Client 类方法

#### 静态方法

- `Client::connection($name, $config)`: 获取连接实例
- `Client::send($queue, $body, $properties, $exchange, $routing_key)`: 发送消息

#### 实例方法

- `publish($queue, $exchange, $routing_key, $msg, $mandatory, $immediate, $ticket)`: 发布消息
- `declareQueue(...)`: 声明队列
- `declareExchange(...)`: 声明交换机
- `bindQueue(...)`: 绑定队列
- `consume(...)`: 消费消息
- `ack($delivery_tag, $multiple, $requeue)`: 确认消息
- `nack($delivery_tag, $multiple, $requeue)`: 拒绝消息
- `reject($delivery_tag, $requeue)`: 拒绝单条消息
- `qos(...)`: 设置 QoS
- `wait($timeout)`: 等待消息
- `close()`: 关闭连接
- `isConnected()`: 检查连接是否正常
- `reconnect()`: 重新连接
- `getChannel()`: 获取通道对象
- `getConnection()`: 获取连接对象

## 依赖

- PHP >= 7.4
- php-amqplib/php-amqplib >= 3.5

## License

MIT
