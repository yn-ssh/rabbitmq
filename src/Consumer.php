<?php
/**
 * @Author SSH
 * @Email 694711507@qq.com
 * @Date 2025/8/5 00:15
 * @Description
 */
namespace ssh\Amqp;

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
     * @param mixed $delivery_tag
     * @param Client $client
     */
    public function consume($data, $properties, $delivery_tag, $client);
}
