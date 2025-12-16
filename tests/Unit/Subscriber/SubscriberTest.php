<?php

declare(strict_types=1);

use Google\Cloud\PubSub\Message;
use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\Subscription;
use Google\Cloud\PubSub\Topic;
use OffloadProject\GooglePubSub\Subscriber\Subscriber;

beforeEach(function () {
    $this->client = Mockery::mock(PubSubClient::class);
    $this->subscription = Mockery::mock(Subscription::class);
    $this->topic = Mockery::mock(Topic::class);
    $this->message = Mockery::mock(Message::class);
});

it('pulls messages from subscription', function () {
    $this->client->shouldReceive('subscription')
        ->with('test-subscription')
        ->andReturn($this->subscription);

    $this->subscription->shouldReceive('exists')->andReturn(true);
    $this->subscription->shouldReceive('pull')
        ->with([
            'maxMessages' => 10,
            'returnImmediately' => true,
        ])
        ->andReturn([$this->message]);

    $this->message->shouldReceive('data')->andReturn('{"test":"data"}');
    $this->message->shouldReceive('attributes')->andReturn([]);
    $this->message->shouldReceive('id')->andReturn('msg-123');

    $this->subscription->shouldReceive('acknowledge')
        ->with($this->message)
        ->once();

    $subscriber = new Subscriber($this->client, 'test-subscription', null, [
        'auto_acknowledge' => true,
        'monitoring' => ['log_consumed_messages' => false],
    ]);

    $result = null;
    $subscriber->handler(function ($data, $message) use (&$result) {
        $result = $data;
    });

    $messages = $subscriber->pull();

    expect($messages)->toHaveCount(1);
    expect($result)->toBe(['test' => 'data']);
});

it('creates subscription if auto create is enabled', function () {
    $this->client->shouldReceive('subscription')
        ->with('new-subscription')
        ->andReturn($this->subscription);

    $this->client->shouldReceive('topic')
        ->with('test-topic')
        ->andReturn($this->topic);

    $this->subscription->shouldReceive('exists')->andReturn(false);
    $this->topic->shouldReceive('exists')->andReturn(true);
    $this->topic->shouldReceive('subscribe')
        ->with('new-subscription', Mockery::type('array'))
        ->andReturn($this->subscription);

    $this->subscription->shouldReceive('pull')->andReturn([]);

    $subscriber = new Subscriber($this->client, 'new-subscription', 'test-topic', [
        'auto_create_subscriptions' => true,
        'monitoring' => ['log_consumed_messages' => false],
    ]);

    $messages = $subscriber->pull();

    expect($messages)->toBeArray()->toBeEmpty();
});

it('handles compressed messages', function () {
    $originalData = '{"test":"compressed data"}';
    $compressedData = gzcompress($originalData);

    $this->client->shouldReceive('subscription')
        ->with('test-subscription')
        ->andReturn($this->subscription);

    $this->subscription->shouldReceive('exists')->andReturn(true);
    $this->subscription->shouldReceive('pull')->andReturn([$this->message]);
    $this->subscription->shouldReceive('acknowledge')->once();

    $this->message->shouldReceive('data')->andReturn($compressedData);
    $this->message->shouldReceive('attributes')->andReturn(['compressed' => 'true']);
    $this->message->shouldReceive('id')->andReturn('msg-123');

    $subscriber = new Subscriber($this->client, 'test-subscription', null, [
        'monitoring' => ['log_consumed_messages' => false],
    ]);

    $result = null;
    $subscriber->handler(function ($data) use (&$result) {
        $result = $data;
    });

    $subscriber->pull();

    expect($result)->toBe(['test' => 'compressed data']);
});

it('calls error handler on exception', function () {
    $this->client->shouldReceive('subscription')
        ->with('test-subscription')
        ->andReturn($this->subscription);

    $this->subscription->shouldReceive('exists')->andReturn(true);
    $this->subscription->shouldReceive('pull')->andReturn([$this->message]);

    $this->message->shouldReceive('data')->andReturn('invalid json');
    $this->message->shouldReceive('attributes')->andReturn([]);
    $this->message->shouldReceive('id')->andReturn('msg-123');

    $subscriber = new Subscriber($this->client, 'test-subscription', null, [
        'monitoring' => ['log_consumed_messages' => false],
    ]);

    $errorCaught = false;
    $subscriber->handler(function ($data) {
        // This will fail due to invalid JSON
    });

    $subscriber->onError(function ($error, $message) use (&$errorCaught) {
        $errorCaught = true;
        expect($error)->toBeInstanceOf(Exception::class);
        expect($message)->not->toBeNull();
    });

    $subscriber->pull();

    expect($errorCaught)->toBeTrue();
});

it('does not auto acknowledge when disabled', function () {
    $this->client->shouldReceive('subscription')
        ->with('test-subscription')
        ->andReturn($this->subscription);

    $this->subscription->shouldReceive('exists')->andReturn(true);
    $this->subscription->shouldReceive('pull')->andReturn([$this->message]);

    $this->message->shouldReceive('data')->andReturn('{"test":"data"}');
    $this->message->shouldReceive('attributes')->andReturn([]);
    $this->message->shouldReceive('id')->andReturn('msg-123');

    // Should NOT receive acknowledge
    $this->subscription->shouldNotReceive('acknowledge');

    $subscriber = new Subscriber($this->client, 'test-subscription', null, [
        'auto_acknowledge' => false,
        'monitoring' => ['log_consumed_messages' => false],
    ]);

    $subscriber->handler(function ($data) {
        // Process message
    });

    $subscriber->pull();
});

it('modifies ack deadline', function () {
    $this->client->shouldReceive('subscription')
        ->with('test-subscription')
        ->andReturn($this->subscription);

    $this->subscription->shouldReceive('exists')->andReturn(true);
    $this->subscription->shouldReceive('modifyAckDeadline')
        ->with($this->message, 120)
        ->once();

    $subscriber = new Subscriber($this->client, 'test-subscription');

    // Use reflection to set the subscription
    $reflection = new ReflectionClass($subscriber);
    $property = $reflection->getProperty('subscription');
    $property->setAccessible(true);
    $property->setValue($subscriber, $this->subscription);

    $subscriber->modifyAckDeadline($this->message, 120);
});

it('creates dead letter topic when configured', function () {
    $deadLetterTopic = Mockery::mock(Topic::class);

    $this->client->shouldReceive('subscription')
        ->with('test-subscription')
        ->andReturn($this->subscription);

    $this->client->shouldReceive('topic')
        ->with('test-topic')
        ->andReturn($this->topic);

    $this->client->shouldReceive('topic')
        ->with('test-topic-dead-letter')
        ->andReturn($deadLetterTopic);

    $this->subscription->shouldReceive('exists')->andReturn(false);
    $this->topic->shouldReceive('exists')->andReturn(true);

    $deadLetterTopic->shouldReceive('exists')->andReturn(false);
    $deadLetterTopic->shouldReceive('create')->once();
    $deadLetterTopic->shouldReceive('name')->andReturn('projects/test/topics/test-topic-dead-letter');

    $this->topic->shouldReceive('subscribe')
        ->withArgs(function ($name, $config) {
            return $name === 'test-subscription'
                && isset($config['deadLetterPolicy'])
                && $config['deadLetterPolicy']['maxDeliveryAttempts'] === 5;
        })
        ->andReturn($this->subscription);

    $this->subscription->shouldReceive('pull')->andReturn([]);

    $subscriber = new Subscriber($this->client, 'test-subscription', 'test-topic', [
        'auto_create_subscriptions' => true,
        'dead_letter_policy' => [
            'enabled' => true,
            'max_delivery_attempts' => 5,
            'dead_letter_topic_suffix' => '-dead-letter',
        ],
        'monitoring' => ['log_consumed_messages' => false],
    ]);

    $subscriber->pull();
});
