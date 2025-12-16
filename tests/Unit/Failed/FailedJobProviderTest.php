<?php

declare(strict_types=1);

use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\Topic;
use OffloadProject\GooglePubSub\Exceptions\PubSubException;
use OffloadProject\GooglePubSub\Failed\PubSubFailedJobProvider;
use OffloadProject\GooglePubSub\PubSubManager;

beforeEach(function () {
    $this->config = [
        'project_id' => 'test-project',
        'auth_method' => 'application_default',
        'auto_create_topics' => true,
        'monitoring' => [
            'log_failed_messages' => true,
        ],
    ];

    $this->provider = new PubSubFailedJobProvider(null, $this->config);
});

describe('PubSubFailedJobProvider', function () {
    it('logs failed job to pubsub topic', function () {
        $mockClient = Mockery::mock(PubSubClient::class);
        $mockTopic = Mockery::mock(Topic::class);
        $mockManager = Mockery::mock(PubSubManager::class);

        $mockManager->shouldReceive('client')
            ->andReturn($mockClient);

        // Mock the topic creation/retrieval
        $mockClient->shouldReceive('topic')
            ->with('laravel-failed-jobs')
            ->andReturn($mockTopic);

        $mockTopic->shouldReceive('exists')
            ->andReturn(false);

        $mockTopic->shouldReceive('create')
            ->once();

        $mockTopic->shouldReceive('publish')
            ->withArgs(function ($message) {
                $decoded = json_decode($message['data'], true);

                return $decoded['connection'] === 'pubsub'
                    && $decoded['queue'] === 'default'
                    && isset($decoded['payload'])
                    && isset($decoded['exception'])
                    && isset($decoded['failed_at'])
                    && $message['attributes']['connection'] === 'pubsub'
                    && $message['attributes']['queue'] === 'default';
            })
            ->andReturn(['messageIds' => ['msg-failed-123']])
            ->once();

        $provider = new PubSubFailedJobProvider($mockManager, $this->config);

        $exception = new Exception('Job processing failed');
        $result = $provider->log('pubsub', 'default', '{"job":"TestJob"}', $exception);

        expect($result)->toBe('msg-failed-123');
    });

    it('creates failed jobs topic if it does not exist', function () {
        $mockClient = Mockery::mock(PubSubClient::class);
        $mockTopic = Mockery::mock(Topic::class);
        $mockManager = Mockery::mock(PubSubManager::class);

        $mockManager->shouldReceive('client')
            ->andReturn($mockClient);

        $mockClient->shouldReceive('topic')
            ->with('laravel-failed-jobs')
            ->andReturn($mockTopic);

        $mockTopic->shouldReceive('exists')
            ->andReturn(false);

        $mockTopic->shouldReceive('create')
            ->once();

        $mockTopic->shouldReceive('publish')
            ->andReturn(['messageIds' => ['msg-123']]);

        $provider = new PubSubFailedJobProvider($mockManager, $this->config);

        $provider->log('pubsub', 'default', 'payload', new Exception('error'));

        // If we got here without exception, topic was created
        expect(true)->toBeTrue();
    });

    it('uses existing topic if it exists', function () {
        $mockClient = Mockery::mock(PubSubClient::class);
        $mockTopic = Mockery::mock(Topic::class);
        $mockManager = Mockery::mock(PubSubManager::class);

        $mockManager->shouldReceive('client')
            ->andReturn($mockClient);

        $mockClient->shouldReceive('topic')
            ->with('laravel-failed-jobs')
            ->andReturn($mockTopic);

        $mockTopic->shouldReceive('exists')
            ->andReturn(true);

        $mockTopic->shouldNotReceive('create');

        $mockTopic->shouldReceive('publish')
            ->andReturn(['messageIds' => ['msg-456']]);

        $provider = new PubSubFailedJobProvider($mockManager, $this->config);

        $provider->log('pubsub', 'default', 'payload', new Exception('error'));

        expect(true)->toBeTrue();
    });

    it('throws exception when logging fails', function () {
        $mockClient = Mockery::mock(PubSubClient::class);
        $mockTopic = Mockery::mock(Topic::class);
        $mockManager = Mockery::mock(PubSubManager::class);

        $mockManager->shouldReceive('client')
            ->andReturn($mockClient);

        $mockClient->shouldReceive('topic')
            ->andReturn($mockTopic);

        $mockTopic->shouldReceive('exists')
            ->andReturn(true);

        $mockTopic->shouldReceive('publish')
            ->andThrow(new Exception('Network error'));

        $provider = new PubSubFailedJobProvider($mockManager, $this->config);

        expect(fn () => $provider->log('pubsub', 'default', 'payload', new Exception('error')))
            ->toThrow(PubSubException::class, 'Failed to log failed job');
    });

    it('includes exception class in attributes', function () {
        $mockClient = Mockery::mock(PubSubClient::class);
        $mockTopic = Mockery::mock(Topic::class);
        $mockManager = Mockery::mock(PubSubManager::class);

        $mockManager->shouldReceive('client')
            ->andReturn($mockClient);

        $mockClient->shouldReceive('topic')
            ->andReturn($mockTopic);

        $mockTopic->shouldReceive('exists')
            ->andReturn(true);

        $mockTopic->shouldReceive('publish')
            ->withArgs(function ($message) {
                return $message['attributes']['exception_class'] === 'RuntimeException';
            })
            ->andReturn(['messageIds' => ['msg-789']]);

        $provider = new PubSubFailedJobProvider($mockManager, $this->config);

        $exception = new RuntimeException('Runtime error');
        $provider->log('pubsub', 'default', 'payload', $exception);

        expect(true)->toBeTrue();
    });

    it('throws BadMethodCallException for all() method', function () {
        expect(fn () => $this->provider->all())
            ->toThrow(BadMethodCallException::class, 'does not support retrieving');
    });

    it('throws BadMethodCallException for find() method', function () {
        expect(fn () => $this->provider->find('some-id'))
            ->toThrow(BadMethodCallException::class, 'does not support finding');
    });

    it('throws BadMethodCallException for forget() method', function () {
        expect(fn () => $this->provider->forget('some-id'))
            ->toThrow(BadMethodCallException::class, 'does not support deleting');
    });

    it('throws BadMethodCallException for ids() method', function () {
        expect(fn () => $this->provider->ids())
            ->toThrow(BadMethodCallException::class, 'does not support listing');
    });

    it('throws BadMethodCallException for ids() with queue parameter', function () {
        expect(fn () => $this->provider->ids('default'))
            ->toThrow(BadMethodCallException::class, 'does not support listing');
    });

    it('throws BadMethodCallException for flush() method', function () {
        expect(fn () => $this->provider->flush())
            ->toThrow(BadMethodCallException::class, 'does not support flushing');
    });

    it('throws BadMethodCallException for flush() with hours parameter', function () {
        expect(fn () => $this->provider->flush(24))
            ->toThrow(BadMethodCallException::class, 'does not support flushing');
    });

    it('throws BadMethodCallException for count() method', function () {
        expect(fn () => $this->provider->count())
            ->toThrow(BadMethodCallException::class, 'does not support counting');
    });

    it('uses configurable topic name', function () {
        $mockClient = Mockery::mock(PubSubClient::class);
        $mockTopic = Mockery::mock(Topic::class);
        $mockManager = Mockery::mock(PubSubManager::class);

        $mockManager->shouldReceive('client')
            ->andReturn($mockClient);

        $config = array_merge($this->config, [
            'failed_jobs' => [
                'topic' => 'custom-failed-jobs-topic',
            ],
        ]);

        $mockClient->shouldReceive('topic')
            ->with('custom-failed-jobs-topic')
            ->andReturn($mockTopic);

        $mockTopic->shouldReceive('exists')
            ->andReturn(true);

        $mockTopic->shouldReceive('publish')
            ->andReturn(['messageIds' => ['msg-custom']]);

        $provider = new PubSubFailedJobProvider($mockManager, $config);

        $provider->log('pubsub', 'default', 'payload', new Exception('error'));

        expect(true)->toBeTrue();
    });

    it('falls back to creating client when manager is null', function () {
        // Skip if emulator is not running - this test requires actual connection
        if (! getenv('PUBSUB_EMULATOR_HOST')) {
            $this->markTestSkipped('Pub/Sub emulator not running');
        }

        $config = [
            'project_id' => 'test-project',
            'auth_method' => 'application_default',
            'emulator_host' => getenv('PUBSUB_EMULATOR_HOST'),
            'auto_create_topics' => true,
            'monitoring' => [],
        ];

        $provider = new PubSubFailedJobProvider(null, $config);

        $reflection = new ReflectionClass($provider);
        $method = $reflection->getMethod('getPubSubClient');
        $method->setAccessible(true);

        // This should create a client using the fallback path
        $client = $method->invoke($provider);

        expect($client)->toBeInstanceOf(PubSubClient::class);
    });

    it('caches topic instance', function () {
        $mockClient = Mockery::mock(PubSubClient::class);
        $mockTopic = Mockery::mock(Topic::class);
        $mockManager = Mockery::mock(PubSubManager::class);

        $mockManager->shouldReceive('client')
            ->andReturn($mockClient);

        $mockClient->shouldReceive('topic')
            ->with('laravel-failed-jobs')
            ->once()
            ->andReturn($mockTopic);

        $mockTopic->shouldReceive('exists')
            ->once()
            ->andReturn(true);

        $mockTopic->shouldReceive('publish')
            ->twice()
            ->andReturn(['messageIds' => ['msg-1']]);

        $provider = new PubSubFailedJobProvider($mockManager, $this->config);

        // Log twice - topic() should only be called once due to caching
        $provider->log('pubsub', 'default', 'payload1', new Exception('error1'));
        $provider->log('pubsub', 'default', 'payload2', new Exception('error2'));

        expect(true)->toBeTrue();
    });
});
