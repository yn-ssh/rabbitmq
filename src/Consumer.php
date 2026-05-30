<?php
/**
 * @Author SSH
 * @Email 694711507@qq.com
 * @Date 2025/8/5 00:15
 * @Description
 */
namespace ssh\Amqp;

use PhpAmqpLib\Message\AMQPMessage;

/**
 * Interface Consumer
 * @package ssh\Amqp
 */
interface Consumer
{
    /**
     * @return string
     */
    public function queue();

    /**
     * @param string $data
     * @param array $properties
     * @param AMQPMessage $msg
     * @param Client $client
     */
    public function consume($data, $properties, $msg, $client);
}
