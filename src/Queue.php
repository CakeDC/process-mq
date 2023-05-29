<?php
namespace ProcessMQ;

use Cake\Core\Configure;
use Cake\Datasource\ConnectionManager;
use Exception;

/**
 * Queue utility class making working with RabbitMQ a lot easier
 *
 */
class Queue
{
    /**
     * Queue configuration read from the YAML file
     *
     * @var array
     */
    protected static array $_config = [];

    /**
     * List of exchanges for publication
     *
     * @var \ProcessMQ\Connection\RabbitMQConnection[]
     */
    protected static array $_publishers = [];

    /**
     * List of queues for consumption
     *
     * @var array
     */
    protected static array $_consumers = [];

    /**
     * Get the queue object for consumption
     *
     * @param string $name
     * @return \AMQPQueue
     * @throws \Exception on missing consumer configuration.
     */
    public static function consume(string $name): \AMQPQueue
    {
        $config = static::get($name);
        if (empty($config['consume'])) {
            throw new Exception('Missing consumer configuration (' . $name . ')');
        }

        $config = $config['consume'];
        $config += [
            'connection' => 'rabbit',
            'prefetchCount' => 1,
        ];

        if (!array_key_exists($name, static::$_consumers)) {
            /** @var \ProcessMQ\Connection\RabbitMQConnection $connection */
            $connection = ConnectionManager::get($config['connection']);
            static::$_consumers[$name] = $connection->queue($config['queue'], $config);
        }

        return static::$_consumers[$name];
    }

    /**
     * Publish a message to a RabbitMQ exchange
     *
     * @param string $name
     * @param mixed $data
     * @param array $options
     * @return boolean
     * @throws \AMQPChannelException
     * @throws \AMQPConnectionException
     * @throws \AMQPExchangeException
     */
    public static function publish(string $name, mixed $data, array $options = []): bool
    {
        $config = static::get($name);
        if (empty($config['publish'])) {
            throw new Exception('Missing publisher configuration (' . $name . ')');
        }

        $config = $config['publish'];
        $config += $options;
        $config += [
            'connection' => 'rabbit',
            'className' => 'RabbitMQ.RabbitQueue'
        ];

        if (!array_key_exists($name, static::$_publishers)) {
            static::$_publishers[$name] = ConnectionManager::get($config['connection']);
        }

        return static::$_publishers[$name]->send($config['exchange'], $config['routing'], $data, $config);
    }

    /**
     * Test if a queue is configured
     *
     * @param string $name
     * @return boolean
     */
    public static function configured(string $name): bool
    {
        static::load();

        return array_key_exists($name, static::$_config);
    }

    /**
     * Get the queue configuration
     *
     * @param string $name
     * @return array
     * @throws \Exception
     */
    public static function get(string $name): array
    {
        if (!static::configured($name)) {
            throw new Exception($name);
        }

        return static::$_config[$name];
    }

    /**
     * Clear all internal state in the class
     *
     * @return void
     */
    public static function clear(): void
    {
        static::$_config = [];
        static::$_publishers = [];
        static::$_consumers = [];
    }

    /**
     * Load the configuration array
     *
     * @return void
     */
    protected static function load(): void
    {
        if (!empty(static::$_config)) {
            return;
        }
        static::$_config = Configure::read('Queues') ?: [];
    }

    /**
     * Class is purely static and singleton
     */
    protected function __construct()
    {
    }
}
