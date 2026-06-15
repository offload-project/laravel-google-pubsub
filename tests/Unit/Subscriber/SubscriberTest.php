<?php

declare(strict_types=1);

use Google\Cloud\PubSub\Message;
use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\Subscription;
use Google\Cloud\PubSub\Topic;
use OffloadProject\GooglePubSub\Subscriber\Subscriber;

beforeEach(function () {
    $this->pubsubClient = Mockery::mock(PubSubClient::class);
    $this->subscription = Mockery::mock(Subscription::class);
    $this->topic = Mockery::mock(Topic::class);
    $this->message = Mockery::mock(Message::class);
});

it('pulls messages from subscription', function () {
    $this->pubsubClient->shouldReceive('subscription')
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

    $subscriber = new Subscriber($this->pubsubClient, 'test-subscription', null, [
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
    $this->pubsubClient->shouldReceive('subscription')
        ->with('new-subscription')
        ->andReturn($this->subscription);

    $this->pubsubClient->shouldReceive('topic')
        ->with('test-topic')
        ->andReturn($this->topic);

    $this->subscription->shouldReceive('exists')->andReturn(false);
    $this->topic->shouldReceive('exists')->andReturn(true);
    $this->topic->shouldReceive('subscribe')
        ->with('new-subscription', Mockery::type('array'))
        ->andReturn($this->subscription);

    $this->subscription->shouldReceive('pull')->andReturn([]);

    $subscriber = new Subscriber($this->pubsubClient, 'new-subscription', 'test-topic', [
        'auto_create_subscriptions' => true,
        'monitoring' => ['log_consumed_messages' => false],
    ]);

    $messages = $subscriber->pull();

    expect($messages)->toBeArray()->toBeEmpty();
});

it('handles compressed messages', function () {
    $originalData = '{"test":"compressed data"}';
    $compressedData = gzcompress($originalData);

    $this->pubsubClient->shouldReceive('subscription')
        ->with('test-subscription')
        ->andReturn($this->subscription);

    $this->subscription->shouldReceive('exists')->andReturn(true);
    $this->subscription->shouldReceive('pull')->andReturn([$this->message]);
    $this->subscription->shouldReceive('acknowledge')->once();

    $this->message->shouldReceive('data')->andReturn($compressedData);
    $this->message->shouldReceive('attributes')->andReturn(['compressed' => 'true']);
    $this->message->shouldReceive('id')->andReturn('msg-123');

    $subscriber = new Subscriber($this->pubsubClient, 'test-subscription', null, [
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
    $this->pubsubClient->shouldReceive('subscription')
        ->with('test-subscription')
        ->andReturn($this->subscription);

    $this->subscription->shouldReceive('exists')->andReturn(true);
    $this->subscription->shouldReceive('pull')->andReturn([$this->message]);

    $this->message->shouldReceive('data')->andReturn('invalid json');
    $this->message->shouldReceive('attributes')->andReturn([]);
    $this->message->shouldReceive('id')->andReturn('msg-123');

    $subscriber = new Subscriber($this->pubsubClient, 'test-subscription', null, [
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
    $this->pubsubClient->shouldReceive('subscription')
        ->with('test-subscription')
        ->andReturn($this->subscription);

    $this->subscription->shouldReceive('exists')->andReturn(true);
    $this->subscription->shouldReceive('pull')->andReturn([$this->message]);

    $this->message->shouldReceive('data')->andReturn('{"test":"data"}');
    $this->message->shouldReceive('attributes')->andReturn([]);
    $this->message->shouldReceive('id')->andReturn('msg-123');

    // Should NOT receive acknowledge
    $this->subscription->shouldNotReceive('acknowledge');

    $subscriber = new Subscriber($this->pubsubClient, 'test-subscription', null, [
        'auto_acknowledge' => false,
        'monitoring' => ['log_consumed_messages' => false],
    ]);

    $subscriber->handler(function ($data) {
        // Process message
    });

    $subscriber->pull();
});

it('modifies ack deadline', function () {
    $this->pubsubClient->shouldReceive('subscription')
        ->with('test-subscription')
        ->andReturn($this->subscription);

    $this->subscription->shouldReceive('exists')->andReturn(true);
    $this->subscription->shouldReceive('modifyAckDeadline')
        ->with($this->message, 120)
        ->once();

    $subscriber = new Subscriber($this->pubsubClient, 'test-subscription');

    // Use reflection to set the subscription
    $reflection = new ReflectionClass($subscriber);
    $property = $reflection->getProperty('subscription');
    $property->setValue($subscriber, $this->subscription);

    $subscriber->modifyAckDeadline($this->message, 120);
});

it('creates dead letter topic when configured', function () {
    $deadLetterTopic = Mockery::mock(Topic::class);

    $this->pubsubClient->shouldReceive('subscription')
        ->with('test-subscription')
        ->andReturn($this->subscription);

    $this->pubsubClient->shouldReceive('topic')
        ->with('test-topic')
        ->andReturn($this->topic);

    $this->pubsubClient->shouldReceive('topic')
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

    $subscriber = new Subscriber($this->pubsubClient, 'test-subscription', 'test-topic', [
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

it('nacks remaining messages when shouldStop fires mid-batch', function () {
    $message1 = Mockery::mock(Message::class);
    $message2 = Mockery::mock(Message::class);
    $message3 = Mockery::mock(Message::class);

    foreach ([$message1, $message2, $message3] as $msg) {
        $msg->shouldReceive('data')->andReturn('{"x":1}');
        $msg->shouldReceive('attributes')->andReturn([]);
        $msg->shouldReceive('id')->andReturn('msg');
    }

    $this->pubsubClient->shouldReceive('subscription')
        ->with('test-subscription')
        ->andReturn($this->subscription);

    $this->subscription->shouldReceive('exists')->andReturn(true);
    $this->subscription->shouldReceive('pull')
        ->andReturn([$message1, $message2, $message3])
        ->once();

    $this->subscription->shouldReceive('acknowledge')->with($message1)->once();
    $this->subscription->shouldReceive('modifyAckDeadline')->with($message2, 0)->once();
    $this->subscription->shouldReceive('modifyAckDeadline')->with($message3, 0)->once();

    $subscriber = new class($this->pubsubClient, 'test-subscription', null, ['monitoring' => ['log_consumed_messages' => false]]) extends Subscriber
    {
        protected function shouldStop(): bool
        {
            return true;
        }
    };

    $subscriber->handler(fn () => null);
    $subscriber->pull();
});

it('does not nack remaining messages when nack_on_shutdown is disabled', function () {
    $message1 = Mockery::mock(Message::class);
    $message2 = Mockery::mock(Message::class);

    foreach ([$message1, $message2] as $msg) {
        $msg->shouldReceive('data')->andReturn('{"x":1}');
        $msg->shouldReceive('attributes')->andReturn([]);
        $msg->shouldReceive('id')->andReturn('msg');
    }

    $this->pubsubClient->shouldReceive('subscription')
        ->with('test-subscription')
        ->andReturn($this->subscription);

    $this->subscription->shouldReceive('exists')->andReturn(true);
    $this->subscription->shouldReceive('pull')
        ->andReturn([$message1, $message2])
        ->once();

    $this->subscription->shouldReceive('acknowledge')->with($message1)->once();
    $this->subscription->shouldNotReceive('modifyAckDeadline');

    $subscriber = new class($this->pubsubClient, 'test-subscription', null, ['nack_on_shutdown' => false, 'monitoring' => ['log_consumed_messages' => false]]) extends Subscriber
    {
        protected function shouldStop(): bool
        {
            return true;
        }
    };

    $subscriber->handler(fn () => null);
    $subscriber->pull();
});
