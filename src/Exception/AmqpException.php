<?php
/**
 * @Author SSH
 * @Email 694711507@qq.com
 * @Date 2025/8/5 00:15
 * @Description
 */
namespace ssh\Amqp\Exception;

class AmqpException extends \Exception
{
}

class ConnectionException extends AmqpException
{
}

class ChannelException extends AmqpException
{
}

class PublishException extends AmqpException
{
}

class ConsumeException extends AmqpException
{
}

class QueueException extends AmqpException
{
}

class ExchangeException extends AmqpException
{
}
