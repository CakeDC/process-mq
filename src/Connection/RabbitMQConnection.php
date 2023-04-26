<?php
declare(strict_types=1);

namespace ProcessMQ\Connection;

use AMQPChannel;
use AMQPConnection;
use AMQPExchange;
use AMQPQueue;
use Cake\Database\Log\QueryLogger;
use Psr\Log\LoggerInterface;
use RuntimeException;

/**
 * RabbitMQ Connection object
 *
 */
class RabbitMQConnection
{
    /**
     * Configuration options
     *
     * @var array
     */
    public mixed $config = [
        'user' => 'guest',
        'password' => 'guest',
        'host' => 'localhost',
        'port' => '5672',
        'vhost' => '/',
        'timeout' => 0,
        'readTimeout' => 172800 // two days
    ];

    /**
     * Connection object
     *
     * @var \AMQPConnection
     */
    protected AMQPConnection $connection;

    /**
     * List of queues
     *
     * @var array
     */
    protected array $queues = [];

    /**
     * List of exchanges
     *
     * @var array
     */
    protected array $exchanges = [];

    /**
     * List of channels
     *
     * @var array
     */
    protected array $channels = [];

    /**
     * Logger instance
     *
     * @var mixed
     */
    protected mixed $logger;

    /**
     * Whether or not to log queries.
     *
     * @var bool
     */
    protected bool $logQueries;

    /**
     * Initializes connection to RabbitMQ
     */
    public function __construct($config = [])
    {
        $this->config = $config + $this->config;
        $this->connection = new AMQPConnection();
        $this->connect();
    }

    /**
     * Get the configuration data used to create the connection.
     *
     * @return array
     */
    public function config(): array
    {
        return $this->config;
    }

    /**
     * Get the configuration name for this connection.
     *
     * @return string
     */
    public function configName(): string
    {
        if (empty($this->config['name'])) {
            return '';
        }

        return $this->config['name'];
    }

    /**
     * Enables or disables query logging for this connection.
     *
     * @param bool|null $enable whether to turn logging on or disable it.
     *   Use null to read current value.
     * @return bool|null
     */
    public function logQueries(bool $enable = null): ?bool
    {
        if ($enable !== null) {
            $this->logQueries = $enable;
        }

        if ($this->logQueries) {
            return $this->logQueries;
        }

        return $this->logQueries = $enable;
    }

    /**
     * Sets the logger object instance. When called with no arguments
     * it returns the currently setup logger instance.
     *
     * @param \Psr\Log\LoggerInterface|null $logger logger object instance
     * @return \Psr\Log\LoggerInterface logger instance
     */
    public function logger(LoggerInterface $logger = null): LoggerInterface
    {
        if ($logger) {
            $this->logger = $logger;
        }

        if ($this->logger) {
            return $this->logger;
        }

        $this->logger = new QueryLogger();

        return $this->logger;
    }

    /**
     * Connects to RabbitMQ
     *
     * @return void
     * @throws \AMQPConnectionException
     */
    public function connect(): void
    {
        $connection = $this->connection;
        $connection->setLogin($this->config['user']);
        $connection->setPassword($this->config['password']);
        $connection->setHost($this->config['host']);
        $connection->setPort($this->config['port']);
        $connection->setVhost($this->config['vhost']);
        $connection->setReadTimeout($this->config['readTimeout']);

        # You shall not use persistent connections
        #
        # AMQPChannelException' with message 'Could not create channel. Connection has no open channel slots remaining
        #
        # The PECL extension is foobar, http://stackoverflow.com/questions/23864647/how-to-avoid-max-channels-per-tcp-connection-with-amqp-php-persistent-connectio
        #

        $connection->connect();
    }

    /**
     * Returns the internal connection object
     *
     * @return \AMQPConnection
     */
    public function connection(): AMQPConnection
    {
        return $this->connection;
    }

    /**
     * Creates a new channel to communicate with an exchange or queue object
     *
     * @param string $name
     * @param array $options
     * @return \AMQPChannel
     * @throws \AMQPConnectionException
     */
    public function channel(string $name, array $options = []): AMQPChannel
    {
        if (empty($this->channels[$name])) {
            $this->channels[$name] = new AMQPChannel($this->connection());
            if (!empty($options['prefetchCount'])) {
                $this->channels[$name]->setPrefetchCount((int)$options['prefetchCount']);
            }
        }

        return $this->channels[$name];
    }

    /**
     * Connects to an exchange with the given name, the object returned
     * can be used to configure and create a new one.
     *
     * @param string $name
     * @param array $options
     * @return \AMQPExchange
     * @throws \AMQPConnectionException
     * @throws \AMQPExchangeException
     */
    public function exchange(string $name, array $options = []): AMQPExchange
    {
        if (empty($this->exchanges[$name])) {
            $channel = $this->channel($name, $options);
            $exchange = new AMQPExchange($channel);
            $exchange->setName($name);

            if (!empty($options['type'])) {
                $exchange->setType($options['type']);
            }

            if (!empty($options['flags'])) {
                $exchange->setFlags($options['flags']);
            }

            $this->exchanges[$name] = $exchange;
        }

        return $this->exchanges[$name];
    }

    /**
     * Connects to a queue with the given name, the object returned
     * can be used to configure and create a new one.
     *
     * @param string $name
     * @param array $options
     * @return \AMQPQueue
     * @throws \AMQPConnectionException
     * @throws \AMQPQueueException
     */
    public function queue(string $name, array $options = []): AMQPQueue
    {
        if (empty($this->queues[$name])) {
            $channel = $this->channel($name, $options);
            $queue = new AMQPQueue($channel);
            $queue->setName($name);

            if (!empty($options['flags'])) {
                $queue->setFlags($options['flags']);
            }

            $this->queues[$name] = $queue;
        }

        return $this->queues[$name];
    }

    /**
     * Creates a new message in the exchange $topic with routing key $task, containing
     * $message
     *
     * @param string $topic
     * @param string $task
     * @param mixed $data
     * @param array $options
     * @return boolean
     * @throws \AMQPChannelException
     * @throws \AMQPConnectionException
     * @throws \AMQPExchangeException
     */
    public function send(
        string $topic,
        string $task,
        mixed $data,
        array $options = []
    ): bool {
        [$data, $attributes, $options] = $this->prepareMessage($data, $options);

        return $this->exchange($topic)->publish($data, $task, AMQP_NOPARAM, $attributes);
    }

    /**
     * Creates a list of new $messages in the exchange $topic with routing key $task
     *
     * @param string $topic
     * @param string $task
     * @param array $messages
     * @param array $options
     * @return boolean
     * @throws \AMQPChannelException
     * @throws \AMQPConnectionException
     * @throws \AMQPExchangeException
     */
    public function sendBatch(
        string $topic,
        string $task,
        array $messages,
        array $options = []
    ): bool {
        return array_walk($messages, function ($data) use ($topic, $task, $options): void {
            $this->send($topic, $task, $data, $options);
        });
    }

    /**
     * Prepare a message by serializing, optionally compressing and setting the correct content type
     * and content type for the message going to RabbitMQ
     *
     * @param  mixed $data
     * @param  array $options
     * @return array
     */
    protected function prepareMessage(mixed $data, array $options): array
    {
        $attributes = [];

        $options += [
            'silent' => false,
            'compress' => true,
            'serializer' => extension_loaded('msgpack') ? 'msgpack' : 'json',
            'delivery_mode' => 1
        ];

        switch ($options['serializer']) {
            case 'json':
                $data = json_encode($data);
                $attributes['content_type'] = 'application/json';
                break;

            case 'text':
                $attributes['content_type'] = 'application/text';
                break;

            default:
                throw new RuntimeException('Unknown serializer: ' . $options['serializer']);
        }

        if (!empty($options['compress'])) {
            $data = gzcompress($data);
            $attributes += ['content_encoding' => 'gzip'];
        }

        if (!empty($options['delivery_mode'])) {
            $attributes['delivery_mode'] = $options['delivery_mode'];
        }

        return [$data, $attributes, $options];
    }
}
