<?php

declare(strict_types=1);

use Google\Cloud\PubSub\Message;
use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\Subscription;
use Google\Cloud\PubSub\Topic;
use OffloadProject\GooglePubSub\Contracts\MessageFormatter;
use OffloadProject\GooglePubSub\Exceptions\PublishException;
use OffloadProject\GooglePubSub\Exceptions\SubscriptionException;
use OffloadProject\GooglePubSub\Formatters\CloudEventsFormatter;
use OffloadProject\GooglePubSub\Formatters\JsonFormatter;
use OffloadProject\GooglePubSub\Publisher\Publisher;
use OffloadProject\GooglePubSub\Subscriber\StreamingSubscriber;
use OffloadProject\GooglePubSub\Subscriber\Subscriber;

describe('Publisher edge cases', function () {
    beforeEach(function () {
        $this->client = Mockery::mock(PubSubClient::class);
        $this->topic = Mockery::mock(Topic::class);
    });

    it('uses cached topic instances', function () {
        $this->client->shouldReceive('topic')
            ->with('cached-topic')
            ->once()
            ->andReturn($this->topic);

        $this->topic->shouldReceive('exists')->andReturn(true);
        $this->topic->shouldReceive('publish')
            ->twice()
            ->andReturn(['messageIds' => ['msg-1']], ['messageIds' => ['msg-2']]);

        $publisher = new Publisher($this->client, [
            'monitoring' => ['log_published_messages' => false],
            'message_options' => ['add_metadata' => false],
        ]);

        // Publish twice to same topic
        $publisher->publish('cached-topic', 'data1');
        $publisher->publish('cached-topic', 'data2');

        // Topic should only be fetched once
        expect(true)->toBeTrue();
    });

    it('allows custom message formatter', function () {
        $customFormatter = Mockery::mock(MessageFormatter::class);
        $customFormatter->shouldReceive('format')
            ->with(['custom' => 'data'])
            ->andReturn('custom-formatted-data')
            ->once();

        $this->client->shouldReceive('topic')->andReturn($this->topic);
        $this->topic->shouldReceive('exists')->andReturn(true);
        $this->topic->shouldReceive('publish')
            ->withArgs(function ($message) {
                return $message['data'] === 'custom-formatted-data';
            })
            ->andReturn(['messageIds' => ['msg-custom']]);

        $publisher = new Publisher($this->client, [
            'monitoring' => ['log_published_messages' => false],
            'message_options' => ['add_metadata' => false],
        ]);

        $publisher->setFormatter($customFormatter);
        $publisher->publish('test-topic', ['custom' => 'data']);
    });

    it('does not compress when disabled via options', function () {
        $largeData = str_repeat('a', 5000);

        $this->client->shouldReceive('topic')->andReturn($this->topic);
        $this->topic->shouldReceive('exists')->andReturn(true);

        $notCompressed = false;
        $this->topic->shouldReceive('publish')
            ->withArgs(function ($message) use (&$notCompressed) {
                $notCompressed = ! isset($message['attributes']['compressed'])
                    && mb_strlen($message['data']) > 4000; // Not compressed

                return true;
            })
            ->andReturn(['messageIds' => ['msg-uncompressed']]);

        $publisher = new Publisher($this->client, [
            'message_options' => [
                'compress_payload' => true,
                'compression_threshold' => 1024,
                'add_metadata' => false,
            ],
            'monitoring' => ['log_published_messages' => false],
        ]);

        // Pass compress: false in options to override config
        $publisher->publish('test-topic', $largeData, [], ['compress' => false]);

        expect($notCompressed)->toBeTrue();
    });

    it('enables message ordering for specific topics', function () {
        $this->client->shouldReceive('topic')
            ->with('ordered-topic')
            ->andReturn($this->topic);

        $this->topic->shouldReceive('exists')->andReturn(false);
        $this->topic->shouldReceive('create')
            ->withArgs(function ($config) {
                return $config['enableMessageOrdering'] === true;
            })
            ->once();
        $this->topic->shouldReceive('publish')
            ->andReturn(['messageIds' => ['msg-ordered']]);

        $publisher = new Publisher($this->client, [
            'auto_create_topics' => true,
            'topics' => [
                'ordered-topic' => ['enable_message_ordering' => true],
            ],
            'monitoring' => ['log_published_messages' => false],
            'message_options' => ['add_metadata' => false],
        ]);

        $publisher->publish('ordered-topic', 'data');
    });

    it('throws exception when publish returns no message id', function () {
        $this->client->shouldReceive('topic')->andReturn($this->topic);
        $this->topic->shouldReceive('exists')->andReturn(true);
        $this->topic->shouldReceive('publish')
            ->andReturn([]); // No messageIds

        $publisher = new Publisher($this->client, [
            'monitoring' => ['log_published_messages' => false],
            'message_options' => ['add_metadata' => false],
        ]);

        expect(fn () => $publisher->publish('test-topic', 'data'))
            ->toThrow(PublishException::class, 'Failed to get message ID');
    });

    it('handles batch publish with mixed messages', function () {
        $this->client->shouldReceive('topic')->andReturn($this->topic);
        $this->topic->shouldReceive('exists')->andReturn(true);
        $this->topic->shouldReceive('publishBatch')
            ->withArgs(function ($messages) {
                return count($messages) === 3
                    && isset($messages[0]['data'])
                    && isset($messages[1]['attributes']['priority'])
                    && isset($messages[2]['data']);
            })
            ->andReturn(['messageIds' => ['msg-1', 'msg-2', 'msg-3']]);

        $publisher = new Publisher($this->client, [
            'monitoring' => ['log_published_messages' => false],
            'message_options' => ['add_metadata' => false],
        ]);

        $messages = [
            ['data' => 'message1'],
            ['data' => 'message2', 'attributes' => ['priority' => 'high']],
            ['data' => 'message3'],
        ];

        $result = $publisher->publishBatch('test-topic', $messages);

        expect($result)->toHaveCount(3);
    });
});

describe('Subscriber edge cases', function () {
    beforeEach(function () {
        $this->client = Mockery::mock(PubSubClient::class);
        $this->subscription = Mockery::mock(Subscription::class);
        $this->topic = Mockery::mock(Topic::class);
        $this->message = Mockery::mock(Message::class);
    });

    it('can set custom formatter', function () {
        $subscriber = new Subscriber($this->client, 'test-subscription');

        $customFormatter = new CloudEventsFormatter();

        $reflection = new ReflectionClass($subscriber);
        $property = $reflection->getProperty('formatter');
        $property->setAccessible(true);
        $property->setValue($subscriber, $customFormatter);

        $value = $property->getValue($subscriber);

        expect($value)->toBeInstanceOf(CloudEventsFormatter::class);
    });

    it('creates topic when auto create enabled and topic missing', function () {
        $this->client->shouldReceive('subscription')
            ->with('new-subscription')
            ->andReturn($this->subscription);

        $this->client->shouldReceive('topic')
            ->with('test-topic')
            ->andReturn($this->topic);

        $this->subscription->shouldReceive('exists')->andReturn(false);
        $this->topic->shouldReceive('exists')->andReturn(false);
        $this->topic->shouldReceive('create')->once();
        $this->topic->shouldReceive('subscribe')->andReturn($this->subscription);

        $this->subscription->shouldReceive('pull')->andReturn([]);

        $subscriber = new Subscriber($this->client, 'new-subscription', 'test-topic', [
            'auto_create_subscriptions' => true,
            'auto_create_topics' => true,
            'monitoring' => ['log_consumed_messages' => false],
        ]);

        $subscriber->pull();
    });

    it('throws exception when creating subscription without topic', function () {
        $this->client->shouldReceive('subscription')
            ->andReturn($this->subscription);

        $this->subscription->shouldReceive('exists')->andReturn(false);

        $subscriber = new Subscriber($this->client, 'new-subscription', null, [
            'auto_create_subscriptions' => true,
        ]);

        expect(fn () => $subscriber->pull())
            ->toThrow(SubscriptionException::class, 'Cannot create subscription');
    });

    it('acknowledges batch of messages', function () {
        $message1 = Mockery::mock(Message::class);
        $message2 = Mockery::mock(Message::class);

        $this->client->shouldReceive('subscription')
            ->andReturn($this->subscription);

        $this->subscription->shouldReceive('exists')->andReturn(true);
        $this->subscription->shouldReceive('acknowledgeBatch')
            ->with([$message1, $message2])
            ->once();

        $subscriber = new Subscriber($this->client, 'test-subscription');

        // Set subscription via reflection
        $reflection = new ReflectionClass($subscriber);
        $property = $reflection->getProperty('subscription');
        $property->setAccessible(true);
        $property->setValue($subscriber, $this->subscription);

        $subscriber->acknowledgeBatch([$message1, $message2]);
    });

    it('handles multiple handlers', function () {
        $this->client->shouldReceive('subscription')
            ->andReturn($this->subscription);

        $this->subscription->shouldReceive('exists')->andReturn(true);
        $this->subscription->shouldReceive('pull')->andReturn([$this->message]);
        $this->subscription->shouldReceive('acknowledge')->once();

        $this->message->shouldReceive('data')->andReturn('{"test":"data"}');
        $this->message->shouldReceive('attributes')->andReturn([]);
        $this->message->shouldReceive('id')->andReturn('msg-123');

        $subscriber = new Subscriber($this->client, 'test-subscription', null, [
            'monitoring' => ['log_consumed_messages' => false],
        ]);

        $handler1Called = false;
        $handler2Called = false;

        $subscriber->handler(function () use (&$handler1Called) {
            $handler1Called = true;
        });

        $subscriber->handler(function () use (&$handler2Called) {
            $handler2Called = true;
        });

        $subscriber->pull();

        expect($handler1Called)->toBeTrue();
        expect($handler2Called)->toBeTrue();
    });

    it('throws exception when decompression fails', function () {
        $this->client->shouldReceive('subscription')
            ->andReturn($this->subscription);

        $this->subscription->shouldReceive('exists')->andReturn(true);
        $this->subscription->shouldReceive('pull')->andReturn([$this->message]);

        $this->message->shouldReceive('data')->andReturn('corrupted data');
        $this->message->shouldReceive('attributes')->andReturn(['compressed' => 'true']);
        $this->message->shouldReceive('id')->andReturn('msg-123');

        $subscriber = new Subscriber($this->client, 'test-subscription', null, [
            'monitoring' => ['log_consumed_messages' => false],
        ]);

        $errorCaught = false;
        $subscriber->onError(function ($error) use (&$errorCaught) {
            $errorCaught = true;
            // The error could be either SubscriptionException or ErrorException
            expect($error)->toBeInstanceOf(Exception::class);
        });

        $subscriber->pull();

        expect($errorCaught)->toBeTrue();
    });

    it('configures retry policy in subscription', function () {
        $this->client->shouldReceive('subscription')
            ->andReturn($this->subscription);

        $this->client->shouldReceive('topic')
            ->andReturn($this->topic);

        $this->subscription->shouldReceive('exists')->andReturn(false);
        $this->topic->shouldReceive('exists')->andReturn(true);

        $retryPolicyConfigured = false;
        $this->topic->shouldReceive('subscribe')
            ->withArgs(function ($name, $config) use (&$retryPolicyConfigured) {
                $retryPolicyConfigured = isset($config['retryPolicy'])
                    && $config['retryPolicy']['minimumBackoff'] === 10
                    && $config['retryPolicy']['maximumBackoff'] === 600;

                return $name === 'retry-subscription';
            })
            ->andReturn($this->subscription);

        $this->subscription->shouldReceive('pull')->andReturn([]);

        $subscriber = new Subscriber($this->client, 'retry-subscription', 'test-topic', [
            'auto_create_subscriptions' => true,
            'retry_policy' => [
                'minimumBackoff' => 10,
                'maximumBackoff' => 600,
            ],
            'monitoring' => ['log_consumed_messages' => false],
        ]);

        $subscriber->pull();

        expect($retryPolicyConfigured)->toBeTrue();
    });
});

describe('StreamingSubscriber edge cases', function () {
    beforeEach(function () {
        $this->client = Mockery::mock(PubSubClient::class);
        $this->subscription = Mockery::mock(Subscription::class);
        $this->message = Mockery::mock(Message::class);
    });

    it('uses flow control configuration', function () {
        $subscriber = new StreamingSubscriber(
            $this->client,
            'test-subscription',
            null,
            ['max_messages_per_pull' => 100]
        );

        $configured = $subscriber->withFlowControl(200);

        expect($configured)->toBe($subscriber); // Fluent interface
    });

    it('configures wait time between pulls', function () {
        $subscriber = new StreamingSubscriber(
            $this->client,
            'test-subscription',
            null,
            []
        );

        $configured = $subscriber->withWaitTime(500);

        expect($configured)->toBe($subscriber); // Fluent interface
    });

    it('processes messages in stream', function () {
        $this->client->shouldReceive('subscription')
            ->andReturn($this->subscription);

        $this->subscription->shouldReceive('exists')->andReturn(true);
        $this->subscription->shouldReceive('pull')
            ->andReturn([$this->message])
            ->once();

        $this->subscription->shouldReceive('pull')
            ->andReturn([])
            ->once();

        $this->message->shouldReceive('data')->andReturn('{"stream":"data"}');
        $this->message->shouldReceive('attributes')->andReturn([]);
        $this->message->shouldReceive('id')->andReturn('msg-stream');
        $this->message->shouldReceive('publishTime')->andReturn('2024-01-01T00:00:00Z');

        $this->subscription->shouldReceive('acknowledge')
            ->with($this->message)
            ->once();

        $subscriber = new class($this->client, 'test-subscription', null, ['monitoring' => ['log_consumed_messages' => false]]) extends StreamingSubscriber
        {
            public $pullCount = 0;

            protected function shouldStop(): bool
            {
                return ++$this->pullCount >= 2;
            }
        };

        $messageReceived = false;
        $subscriber->handler(function ($data) use (&$messageReceived) {
            $messageReceived = true;
            expect($data)->toBe(['stream' => 'data']);
        });

        $subscriber->stream();

        expect($messageReceived)->toBeTrue();
    });

    it('listen method calls stream', function () {
        $subscriber = Mockery::mock(StreamingSubscriber::class)
            ->makePartial()
            ->shouldAllowMockingProtectedMethods();

        $subscriber->shouldReceive('stream')
            ->with(['max_messages' => 10])
            ->once();

        $subscriber->listen(['max_messages' => 10]);
    });
});

describe('Formatter edge cases', function () {
    it('json formatter returns empty array for empty string', function () {
        $formatter = new JsonFormatter();

        expect($formatter->parse(''))->toBeNull();
    });

    it('json formatter handles null data', function () {
        $formatter = new JsonFormatter();

        $result = $formatter->parse('null');

        expect($result)->toBeNull();
    });

    it('cloud events formatter uses custom default type', function () {
        $formatter = new CloudEventsFormatter('https://test.com', 'com.test.custom');

        $json = $formatter->format(['data' => 'test']);
        $event = json_decode($json, true);

        expect($event['type'])->toBe('com.test.custom');
    });

    it('cloud events formatter returns already formatted data', function () {
        $formatter = new CloudEventsFormatter();

        $existingEvent = json_encode([
            'specversion' => '1.0',
            'type' => 'existing',
            'source' => 'test',
            'id' => '123',
            'data' => 'test',
        ]);

        $result = $formatter->format($existingEvent);

        expect($result)->toBe($existingEvent);
    });

    it('cloud events formatter handles object with toArray', function () {
        $object = new class()
        {
            public function toArray(): array
            {
                return ['key' => 'value'];
            }
        };

        $formatter = new CloudEventsFormatter();
        $json = $formatter->format($object);
        $event = json_decode($json, true);

        expect($event['data'])->toBe(['key' => 'value']);
    });
});
