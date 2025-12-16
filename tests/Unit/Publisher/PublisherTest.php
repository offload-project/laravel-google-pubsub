<?php

declare(strict_types=1);

use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\Topic;
use OffloadProject\GooglePubSub\Exceptions\PublishException;
use OffloadProject\GooglePubSub\Publisher\Publisher;
use OffloadProject\GooglePubSub\Schema\SchemaValidator;

beforeEach(function () {
    $this->client = Mockery::mock(PubSubClient::class);
    $this->topic = Mockery::mock(Topic::class);
    $this->publisher = new Publisher($this->client, [
        'monitoring' => ['log_published_messages' => false],
        'message_options' => ['add_metadata' => false],
    ]);
});

it('publishes a message to a topic', function () {
    $this->client->shouldReceive('topic')
        ->with('test-topic')
        ->andReturn($this->topic);

    $this->topic->shouldReceive('exists')->andReturn(true);
    $this->topic->shouldReceive('publish')
        ->withArgs(function ($message) {
            return $message['data'] === '{"test":"data"}'
                && $message['attributes']['custom'] === 'attribute';
        })
        ->andReturn(['messageIds' => ['msg-123']]);

    $messageId = $this->publisher->publish('test-topic', ['test' => 'data'], ['custom' => 'attribute']);

    expect($messageId)->toBe('msg-123');
});

it('creates topic if auto create is enabled', function () {
    $this->client->shouldReceive('topic')
        ->with('new-topic')
        ->andReturn($this->topic);

    $this->topic->shouldReceive('exists')->andReturn(false);
    $this->topic->shouldReceive('create')->once();
    $this->topic->shouldReceive('publish')->andReturn(['messageIds' => ['msg-456']]);

    $publisher = new Publisher($this->client, [
        'auto_create_topics' => true,
        'monitoring' => ['log_published_messages' => false],
        'message_options' => ['add_metadata' => false],
    ]);

    $messageId = $publisher->publish('new-topic', 'test');

    expect($messageId)->toBe('msg-456');
});

it('throws exception when publish fails', function () {
    $this->client->shouldReceive('topic')
        ->with('test-topic')
        ->andReturn($this->topic);

    $this->topic->shouldReceive('exists')->andReturn(true);
    $this->topic->shouldReceive('publish')->andThrow(new Exception('Network error'));

    $this->publisher->publish('test-topic', 'test');
})->throws(PublishException::class, 'Failed to publish message');

it('publishes batch messages', function () {
    $this->client->shouldReceive('topic')
        ->with('test-topic')
        ->andReturn($this->topic);

    $this->topic->shouldReceive('exists')->andReturn(true);
    $this->topic->shouldReceive('publishBatch')
        ->andReturn(['messageIds' => ['msg-1', 'msg-2', 'msg-3']]);

    $messages = [
        ['data' => ['event' => 'test1']],
        ['data' => ['event' => 'test2'], 'attributes' => ['priority' => 'high']],
        ['data' => ['event' => 'test3']],
    ];

    $messageIds = $this->publisher->publishBatch('test-topic', $messages);

    expect($messageIds)->toHaveCount(3);
    expect($messageIds)->toBe(['msg-1', 'msg-2', 'msg-3']);
});

it('compresses large payloads when enabled', function () {
    $largeData = str_repeat('a', 2000);

    $this->client->shouldReceive('topic')
        ->with('test-topic')
        ->andReturn($this->topic);

    $this->topic->shouldReceive('exists')->andReturn(true);
    $this->topic->shouldReceive('publish')
        ->withArgs(function ($message) {
            return isset($message['attributes']['compressed'])
                && $message['attributes']['compressed'] === 'true'
                && mb_strlen($message['data']) < 2000;
        })
        ->andReturn(['messageIds' => ['msg-789']]);

    $publisher = new Publisher($this->client, [
        'message_options' => [
            'compress_payload' => true,
            'compression_threshold' => 1024,
            'add_metadata' => false,
        ],
        'monitoring' => ['log_published_messages' => false],
    ]);

    $messageId = $publisher->publish('test-topic', $largeData);

    expect($messageId)->toBe('msg-789');
});

it('adds metadata when enabled', function () {
    $this->client->shouldReceive('topic')
        ->with('test-topic')
        ->andReturn($this->topic);

    $this->topic->shouldReceive('exists')->andReturn(true);
    $this->topic->shouldReceive('publish')
        ->withArgs(function ($message) {
            return isset($message['attributes']['published_at'])
                && isset($message['attributes']['publisher'])
                && isset($message['attributes']['hostname'])
                && $message['attributes']['publisher'] === 'laravel';
        })
        ->andReturn(['messageIds' => ['msg-meta']]);

    $publisher = new Publisher($this->client, [
        'message_options' => ['add_metadata' => true],
        'monitoring' => ['log_published_messages' => false],
    ]);

    $messageId = $publisher->publish('test-topic', 'test');

    expect($messageId)->toBe('msg-meta');
});

it('validates message against schema when configured', function () {
    $validator = Mockery::mock(SchemaValidator::class);
    $validator->shouldReceive('validate')
        ->with(['test' => 'data'], 'test_schema')
        ->once();

    $this->client->shouldReceive('topic')
        ->with('test-topic')
        ->andReturn($this->topic);

    $this->topic->shouldReceive('exists')->andReturn(true);
    $this->topic->shouldReceive('publish')->andReturn(['messageIds' => ['msg-123']]);

    $publisher = new Publisher($this->client, [
        'topics' => [
            'test-topic' => ['schema' => 'test_schema'],
        ],
        'monitoring' => ['log_published_messages' => false],
        'message_options' => ['add_metadata' => false],
    ]);

    // Inject the mock validator
    $reflection = new ReflectionClass($publisher);
    $property = $reflection->getProperty('validator');
    $property->setAccessible(true);
    $property->setValue($publisher, $validator);

    $publisher->publish('test-topic', ['test' => 'data']);
});

it('adds ordering key when provided', function () {
    $this->client->shouldReceive('topic')
        ->with('test-topic')
        ->andReturn($this->topic);

    $this->topic->shouldReceive('exists')->andReturn(true);
    $this->topic->shouldReceive('publish')
        ->withArgs(function ($message) {
            return isset($message['orderingKey'])
                && $message['orderingKey'] === 'user-123';
        })
        ->andReturn(['messageIds' => ['msg-ordered']]);

    $messageId = $this->publisher->publish(
        'test-topic',
        'test',
        [],
        ['ordering_key' => 'user-123']
    );

    expect($messageId)->toBe('msg-ordered');
});
