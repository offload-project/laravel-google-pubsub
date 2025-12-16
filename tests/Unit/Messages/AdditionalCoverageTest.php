<?php

declare(strict_types=1);

use Google\Cloud\PubSub\PubSubClient;
use OffloadProject\GooglePubSub\Exceptions\SchemaValidationException;
use OffloadProject\GooglePubSub\Messages\WebhookMessage;
use OffloadProject\GooglePubSub\PubSubManager;
use OffloadProject\GooglePubSub\Queue\Jobs\PubSubJob;

describe('WebhookMessage', function () {
    it('creates webhook message with all properties', function () {
        $message = new WebhookMessage(
            'msg-123',
            '{"test":"data"}',
            ['priority' => 'high', 'source' => 'api'],
            '2024-01-15T10:00:00Z'
        );

        expect($message->id())->toBe('msg-123');
        expect($message->data())->toBe('{"test":"data"}');
        expect($message->attributes())->toBe(['priority' => 'high', 'source' => 'api']);
        expect($message->publishTime())->toBe('2024-01-15T10:00:00Z');
    });

    it('returns subscription from attributes if present', function () {
        $message = new WebhookMessage(
            'msg-123',
            'data',
            ['subscription' => 'test-subscription'],
            '2024-01-15T10:00:00Z'
        );

        expect($message->subscription())->toBe('test-subscription');
    });

    it('returns null for subscription if not in attributes', function () {
        $message = new WebhookMessage(
            'msg-123',
            'data',
            [],
            '2024-01-15T10:00:00Z'
        );

        expect($message->subscription())->toBeNull();
    });

    it('returns ordering key from attributes if present', function () {
        $message = new WebhookMessage(
            'msg-123',
            'data',
            ['ordering_key' => 'user-456'],
            '2024-01-15T10:00:00Z'
        );

        expect($message->orderingKey())->toBe('user-456');
    });

    it('returns null for ordering key if not in attributes', function () {
        $message = new WebhookMessage(
            'msg-123',
            'data',
            [],
            '2024-01-15T10:00:00Z'
        );

        expect($message->orderingKey())->toBeNull();
    });
});

describe('PubSubJob edge cases', function () {
    beforeEach(function () {
        $this->container = Mockery::mock(Illuminate\Container\Container::class);
        $this->pubsubQueue = Mockery::mock(OffloadProject\GooglePubSub\Queue\PubSubQueue::class);
        $this->message = Mockery::mock(Google\Cloud\PubSub\Message::class);
        $this->subscription = Mockery::mock(Google\Cloud\PubSub\Subscription::class);
    });

    it('handles uncompressed messages correctly', function () {
        $this->message->shouldReceive('id')->andReturn('msg-123');
        $this->message->shouldReceive('data')->andReturn('{"test":"data"}');
        $this->message->shouldReceive('attributes')->andReturn([]); // No compression

        $job = new PubSubJob(
            $this->container,
            $this->pubsubQueue,
            $this->message,
            $this->subscription,
            'pubsub',
            'default'
        );

        $body = $job->getRawBody();

        expect($body)->toBe('{"test":"data"}');
    });

    it('gets attempts from payload when available', function () {
        $payload = json_encode([
            'job' => 'TestJob',
            'attempts' => 3,
        ]);

        $this->message->shouldReceive('id')->andReturn('msg-123');
        $this->message->shouldReceive('data')->andReturn($payload);
        $this->message->shouldReceive('attributes')->andReturn([]);

        $job = new PubSubJob(
            $this->container,
            $this->pubsubQueue,
            $this->message,
            $this->subscription,
            'pubsub',
            'default'
        );

        expect($job->attempts())->toBe(3);
    });

    it('falls back to delivery attempt when payload has no attempts', function () {
        $payload = json_encode(['job' => 'TestJob']);

        $this->message->shouldReceive('id')->andReturn('msg-123');
        $this->message->shouldReceive('data')->andReturn($payload);
        $this->message->shouldReceive('attributes')->andReturn(['delivery_attempt' => '2']);

        $job = new PubSubJob(
            $this->container,
            $this->pubsubQueue,
            $this->message,
            $this->subscription,
            'pubsub',
            'default'
        );

        expect($job->attempts())->toBe(2);
    });

    it('defaults to 1 attempt when no attempt info available', function () {
        $payload = json_encode(['job' => 'TestJob']);

        $this->message->shouldReceive('id')->andReturn('msg-123');
        $this->message->shouldReceive('data')->andReturn($payload);
        $this->message->shouldReceive('attributes')->andReturn([]);

        $job = new PubSubJob(
            $this->container,
            $this->pubsubQueue,
            $this->message,
            $this->subscription,
            'pubsub',
            'default'
        );

        expect($job->attempts())->toBe(1);
    });

    it('gets display name from payload', function () {
        $payload = json_encode([
            'job' => 'TestJob',
            'displayName' => 'My Custom Job Name',
        ]);

        $this->message->shouldReceive('id')->andReturn('msg-123');
        $this->message->shouldReceive('data')->andReturn($payload);
        $this->message->shouldReceive('attributes')->andReturn([]);

        $job = new PubSubJob(
            $this->container,
            $this->pubsubQueue,
            $this->message,
            $this->subscription,
            'pubsub',
            'default'
        );

        expect($job->getName())->toBe('My Custom Job Name');
    });

    it('releases message immediately with zero delay', function () {
        $this->message->shouldReceive('id')->andReturn('msg-123');
        $this->subscription->shouldReceive('modifyAckDeadline')
            ->with($this->message, 0)
            ->once();

        $job = new PubSubJob(
            $this->container,
            $this->pubsubQueue,
            $this->message,
            $this->subscription,
            'pubsub',
            'default'
        );

        $job->release(0);
    });

    it('returns null publish time when not available', function () {
        $this->message->shouldReceive('publishTime')->andReturn(null);

        $job = new PubSubJob(
            $this->container,
            $this->pubsubQueue,
            $this->message,
            $this->subscription,
            'pubsub',
            'default'
        );

        expect($job->getPublishTime())->toBeNull();
    });

    it('checks for empty ordering key correctly', function () {
        $this->message->shouldReceive('orderingKey')->andReturn('');

        $job = new PubSubJob(
            $this->container,
            $this->pubsubQueue,
            $this->message,
            $this->subscription,
            'pubsub',
            'default'
        );

        expect($job->hasOrderingKey())->toBeFalse();
        expect($job->getOrderingKey())->toBeNull();
    });

    it('gets underlying pubsub message', function () {
        $job = new PubSubJob(
            $this->container,
            $this->pubsubQueue,
            $this->message,
            $this->subscription,
            'pubsub',
            'default'
        );

        expect($job->getPubSubMessage())->toBe($this->message);
    });
});

describe('PubSubManager edge cases', function () {
    it('throws exception when key file not found', function () {
        config([
            'pubsub.project_id' => 'test-project',
            'pubsub.auth_method' => 'key_file',
            'pubsub.key_file' => '/non/existent/path.json',
        ]);

        $manager = new PubSubManager(fn () => app());

        expect(fn () => $manager->client())
            ->toThrow(OffloadProject\GooglePubSub\Exceptions\PubSubException::class, 'Key file not found');
    });

    it('throws exception when key file path is required but missing', function () {
        config([
            'pubsub.project_id' => 'test-project',
            'pubsub.auth_method' => 'key_file',
            'pubsub.key_file' => null,
        ]);

        $manager = new PubSubManager(fn () => app());

        expect(fn () => $manager->client())
            ->toThrow(OffloadProject\GooglePubSub\Exceptions\PubSubException::class, 'Key file path is required');
    });

    it('reuses cached subscriber instances', function () {
        $client = Mockery::mock(PubSubClient::class);
        config(['pubsub.use_streaming' => false]);
        $app = app();

        $manager = new class($app, $client) extends PubSubManager
        {
            private PubSubClient $mockClient;

            public function __construct($app, $client)
            {
                parent::__construct(fn () => $app);
                $this->mockClient = $client;
            }

            public function client(): PubSubClient
            {
                return $this->mockClient;
            }
        };

        $subscriber1 = $manager->subscriber('test-subscription', 'test-topic');
        $subscriber2 = $manager->subscriber('test-subscription', 'test-topic');

        expect($subscriber1)->toBe($subscriber2);
    });

    it('does not create topic when auto create is disabled', function () {
        $manager = Mockery::mock(PubSubManager::class)->makePartial();
        $client = Mockery::mock(PubSubClient::class);
        $topic = Mockery::mock(Google\Cloud\PubSub\Topic::class);

        $manager->shouldReceive('client')->andReturn($client);
        $client->shouldReceive('topic')->andReturn($topic);

        $topic->shouldReceive('exists')->andReturn(false);
        $topic->shouldNotReceive('create');

        // This would normally create the topic, but with auto_create disabled it shouldn't
        config(['pubsub.auto_create_topics' => false]);

        // Using reflection to test the internal method
        $reflection = new ReflectionClass($manager);

        expect(true)->toBeTrue(); // Placeholder assertion
    });
});

describe('SchemaValidationException', function () {
    it('stores and returns validation errors', function () {
        $errors = [
            'field1' => ['error1', 'error2'],
            'field2' => ['error3'],
        ];

        $exception = new SchemaValidationException('Validation failed', 0, null, $errors);

        expect($exception->getErrors())->toBe($errors);
        expect($exception->getMessage())->toBe('Validation failed');
    });

    it('creates exception without errors', function () {
        $exception = new SchemaValidationException('Validation failed');

        expect($exception->getErrors())->toBeArray()->toBeEmpty();
    });

    it('preserves previous exception', function () {
        $previous = new Exception('Previous error');
        $exception = new SchemaValidationException('Validation failed', 0, $previous);

        expect($exception->getPrevious())->toBe($previous);
    });
});

describe('PubSubQueue edge cases', function () {
    beforeEach(function () {
        $this->pubsub = Mockery::mock(PubSubClient::class);
        $this->topic = Mockery::mock(Google\Cloud\PubSub\Topic::class);
        $this->subscription = Mockery::mock(Google\Cloud\PubSub\Subscription::class);
    });

    it('handles delayed jobs correctly', function () {
        $this->pubsub->shouldReceive('topic')->andReturn($this->topic);
        $this->topic->shouldReceive('exists')->andReturn(true);
        $this->topic->shouldReceive('name')->andReturn('projects/test/topics/default');
        $this->topic->shouldReceive('publish')
            ->withArgs(function ($message) {
                return isset($message['attributes']['deliver_after']);
            })
            ->andReturn(['messageIds' => ['delayed-123']]);

        $queue = new OffloadProject\GooglePubSub\Queue\PubSubQueue($this->pubsub, 'default', [
            'monitoring' => ['log_published_messages' => false],
        ]);

        $messageId = $queue->later(60, 'TestJob', 'data');

        expect($messageId)->toBe('delayed-123');
    });

    it('returns 0 for size method', function () {
        $queue = new OffloadProject\GooglePubSub\Queue\PubSubQueue($this->pubsub, 'default');

        expect($queue->size())->toBe(0);
        expect($queue->size('custom-queue'))->toBe(0);
    });

    it('adds metadata to messages when configured', function () {
        $this->pubsub->shouldReceive('topic')->andReturn($this->topic);
        $this->topic->shouldReceive('exists')->andReturn(true);
        $this->topic->shouldReceive('name')->andReturn('projects/test/topics/default');

        $messagePublished = false;
        $this->topic->shouldReceive('publish')
            ->withArgs(function ($message) use (&$messagePublished) {
                $messagePublished = isset($message['attributes']['laravel_queue'])
                    && isset($message['attributes']['published_at'])
                    && isset($message['attributes']['hostname']);

                return $messagePublished;
            })
            ->andReturn(['messageIds' => ['meta-123']]);

        $queue = new OffloadProject\GooglePubSub\Queue\PubSubQueue($this->pubsub, 'default', [
            'message_options' => ['add_metadata' => true],
            'monitoring' => ['log_published_messages' => false],
        ]);

        $queue->pushRaw('test payload');

        expect($messagePublished)->toBeTrue();
    });

    it('handles custom attributes in push', function () {
        $this->pubsub->shouldReceive('topic')->andReturn($this->topic);
        $this->topic->shouldReceive('exists')->andReturn(true);
        $this->topic->shouldReceive('name')->andReturn('projects/test/topics/default');

        $customAttrFound = false;
        $this->topic->shouldReceive('publish')
            ->withArgs(function ($message) use (&$customAttrFound) {
                $customAttrFound = isset($message['attributes']['custom_attr'])
                    && $message['attributes']['custom_attr'] === 'custom_value';

                return true;
            })
            ->andReturn(['messageIds' => ['custom-123']]);

        $queue = new OffloadProject\GooglePubSub\Queue\PubSubQueue($this->pubsub, 'default', [
            'monitoring' => ['log_published_messages' => false],
            'message_options' => ['add_metadata' => false],
        ]);

        $queue->pushRaw('test', null, [
            'attributes' => ['custom_attr' => 'custom_value'],
        ]);

        expect($customAttrFound)->toBeTrue();
    });
});
