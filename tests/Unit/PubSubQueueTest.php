<?php

declare(strict_types=1);

use Google\Cloud\PubSub\Message;
use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\Subscription;
use Google\Cloud\PubSub\Topic;
use OffloadProject\GooglePubSub\Queue\Jobs\PubSubJob;
use OffloadProject\GooglePubSub\Queue\PubSubQueue;

beforeEach(function () {
    $this->pubsub = Mockery::mock(PubSubClient::class);
    $this->topic = Mockery::mock(Topic::class);
    $this->subscription = Mockery::mock(Subscription::class);
    $this->message = Mockery::mock(Message::class);
});

afterEach(function () {
    Mockery::close();
});

it('can push a job to the queue', function () {
    $this->pubsub->shouldReceive('topic')
        ->with('default')
        ->andReturn($this->topic);

    $this->topic->shouldReceive('exists')->andReturn(true);
    $this->topic->shouldReceive('publish')->andReturn(['messageIds' => ['123']]);
    $this->topic->shouldReceive('name')->andReturn('projects/test/topics/default');

    $queue = new PubSubQueue($this->pubsub, 'default', [
        'monitoring' => ['log_published_messages' => false],
    ]);

    $result = $queue->push('TestJob', ['data' => 'test']);

    expect($result)->toBe('123');
});

it('creates topic if not exists when auto create is enabled', function () {
    $this->pubsub->shouldReceive('topic')
        ->with('new-queue')
        ->andReturn($this->topic);

    $this->topic->shouldReceive('exists')->andReturn(false);
    $this->topic->shouldReceive('create')->once();
    $this->topic->shouldReceive('publish')->andReturn(['messageIds' => ['456']]);
    $this->topic->shouldReceive('name')->andReturn('projects/test/topics/new-queue');

    $queue = new PubSubQueue($this->pubsub, 'default', [
        'auto_create_topics' => true,
        'monitoring' => ['log_published_messages' => false],
    ]);

    $result = $queue->push('TestJob', ['data' => 'test'], 'new-queue');

    expect($result)->toBe('456');
});

it('can pop a job from the queue', function () {
    $this->pubsub->shouldReceive('subscription')
        ->with('default-laravel')
        ->andReturn($this->subscription);

    $this->subscription->shouldReceive('exists')->andReturn(true);
    $this->subscription->shouldReceive('pull')->andReturn([$this->message]);
    $this->subscription->shouldReceive('name')->andReturn('projects/test/subscriptions/default-laravel');

    $this->message->shouldReceive('id')->andReturn('msg-123');
    $this->message->shouldReceive('data')->andReturn(json_encode([
        'job' => 'TestJob',
        'data' => ['test' => 'data'],
    ]));
    $this->message->shouldReceive('attributes')->andReturn([]);

    $queue = new PubSubQueue($this->pubsub, 'default', [
        'subscription_suffix' => '-laravel',
        'monitoring' => ['log_consumed_messages' => false],
    ]);

    // Set the container on the queue instance
    $queue->setContainer($this->app);

    $job = $queue->pop();

    expect($job)->toBeInstanceOf(PubSubJob::class)
        ->and($job->getJobId())->toBe('msg-123');
});

it('returns null when no messages available', function () {
    $this->pubsub->shouldReceive('subscription')
        ->with('default-laravel')
        ->andReturn($this->subscription);

    $this->subscription->shouldReceive('exists')->andReturn(true);
    $this->subscription->shouldReceive('pull')->andReturn([]);

    $queue = new PubSubQueue($this->pubsub, 'default', [
        'subscription_suffix' => '-laravel',
    ]);

    $job = $queue->pop();

    expect($job)->toBeNull();
});

it('compresses large payloads when enabled', function () {
    $largePayload = str_repeat('a', 2000);

    $this->pubsub->shouldReceive('topic')
        ->with('default')
        ->andReturn($this->topic);

    $this->topic->shouldReceive('exists')->andReturn(true);
    $this->topic->shouldReceive('name')->andReturn('projects/test/topics/default');

    $this->topic->shouldReceive('publish')
        ->withArgs(function ($messageData) {
            return isset($messageData['attributes']['compressed'])
                && $messageData['attributes']['compressed'] === 'true'
                && mb_strlen($messageData['data']) < 2000; // Compressed data should be smaller
        })
        ->andReturn(['messageIds' => ['789']]);

    $queue = new PubSubQueue($this->pubsub, 'default', [
        'message_options' => [
            'compress_payload' => true,
            'compression_threshold' => 1024,
        ],
        'monitoring' => ['log_published_messages' => false],
    ]);

    $result = $queue->pushRaw($largePayload);

    expect($result)->toBe('789');
});

it('handles message ordering when enabled', function () {
    $this->pubsub->shouldReceive('topic')
        ->with('ordered-queue')
        ->andReturn($this->topic);

    $this->topic->shouldReceive('exists')->andReturn(true);
    $this->topic->shouldReceive('name')->andReturn('projects/test/topics/ordered-queue');

    $this->topic->shouldReceive('publish')
        ->withArgs(function ($messageData) {
            return isset($messageData['attributes']['ordering_key'])
                && $messageData['attributes']['ordering_key'] === 'test-key';
        })
        ->andReturn(['messageIds' => ['ordered-123']]);

    $queue = new PubSubQueue($this->pubsub, 'default', [
        'enable_message_ordering' => true,
        'monitoring' => ['log_published_messages' => false],
    ]);

    $result = $queue->pushRaw('test payload', 'ordered-queue', [
        'ordering_key' => 'test-key',
    ]);

    expect($result)->toBe('ordered-123');
});
