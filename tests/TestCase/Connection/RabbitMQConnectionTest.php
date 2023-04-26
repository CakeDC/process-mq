<?php
declare(strict_types=1);

namespace TestCase\Connection;

use Cake\TestSuite\TestCase;
use ProcessMQ\Connection\RabbitMQConnection;

class RabbitMQConnectionTest extends TestCase
{
    /**
     * @var \ProcessMQ\Connection\RabbitMQConnection
     */
    public RabbitMQConnection $connection;

    public function setUp(): void
    {
        $this->connection = new RabbitMQConnection();
    }

    public function tearDown(): void
    {
        unset($this->connection);
    }

    public function testConnect()
    {
        $this->markTestIncomplete('Not implemented yet.');
    }

    public function testChannel()
    {
        $this->markTestIncomplete('Not implemented yet.');
    }

    public function testExchange()
    {
        $this->markTestIncomplete('Not implemented yet.');
    }

    public function testQueue()
    {
        $this->markTestIncomplete('Not implemented yet.');
    }

    public function testSend()
    {
        $this->markTestIncomplete('Not implemented yet.');
    }

    public function testSendBatch()
    {
        $this->markTestIncomplete('Not implemented yet.');
    }

    public function testPrepareMessage()
    {
        $this->markTestIncomplete('Not implemented yet.');
    }
}
