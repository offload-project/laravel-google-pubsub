<?php

declare(strict_types=1);

use Google\Cloud\PubSub\Message;
use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\Subscription;
use OffloadProject\GooglePubSub\Subscriber\StreamingSubscriber;

beforeEach(function () {
    $this->pubsubClient = Mockery::mock(PubSubClient::class);
    $this->subscription = Mockery::mock(Subscription::class);
    $this->message = Mockery::mock(Message::class);
});

it('uses long polling with returnImmediately false', function () {
    $this->pubsubClient->shouldReceive('subscription')
        ->with('test-subscription')
        ->andReturn($this->subscription);

    $this->subscription->shouldReceive('exists')->andReturn(true);

    // First pull returns messages
    $this->subscription->shouldReceive('pull')
        ->with([
            'returnImmediately' => false,
            'maxMessages' => 100,
        ])
        ->andReturn([$this->message])
        ->once();

    // Second pull returns empty (to trigger stop)
    $this->subscription->shouldReceive('pull')
        ->with([
            'returnImmediately' => false,
            'maxMessages' => 100,
        ])
        ->andReturn([])
        ->once();

    $this->message->shouldReceive('data')->andReturn('{"test":"streaming"}');
    $this->message->shouldReceive('attributes')->andReturn([]);
    $this->message->shouldReceive('id')->andReturn('msg-stream-1');
    $this->message->shouldReceive('publishTime')->andReturn('2024-01-01T00:00:00Z');

    $this->subscription->shouldReceive('acknowledge')
        ->with($this->message)
        ->once();

    $subscriber = new StreamingSubscriber($this->pubsubClient, 'test-subscription', null, [
        'monitoring' => ['log_consumed_messages' => false],
    ]);

    $messageCount = 0;
    $subscriber->handler(function ($data) use (&$messageCount) {
        $messageCount++;
        expect($data)->toBe(['test' => 'streaming']);
    });

    $subscriber = new class($this->pubsubClient, 'test-subscription', null, ['monitoring' => ['log_consumed_messages' => false]]) extends StreamingSubscriber
    {
        public $pullCount = 0;

        // shouldStop() is called once per message inside the inner loop AND once at the
        // outer-loop check, so 2 pull cycles invoke it 3 times: after the 1st message,
        // then twice at the outer checks. Stop on the final call.
        protected function shouldStop(): bool
        {
            return ++$this->pullCount >= 3;
        }
    };

    $subscriber->handler(function ($data) use (&$messageCount) {
        $messageCount++;
    });

    $subscriber->stream();

    expect($messageCount)->toBe(1);
});

it('nacks message on error when configured', function () {
    $this->pubsubClient->shouldReceive('subscription')
        ->with('test-subscription')
        ->andReturn($this->subscription);

    $this->subscription->shouldReceive('exists')->andReturn(true);
    $this->subscription->shouldReceive('pull')
        ->andReturn([$this->message])
        ->once();

    $this->subscription->shouldReceive('pull')
        ->andReturn([])
        ->once();

    $this->message->shouldReceive('data')->andReturn('{"test":"data"}');
    $this->message->shouldReceive('attributes')->andReturn([]);
    $this->message->shouldReceive('id')->andReturn('msg-123');
    $this->message->shouldReceive('publishTime')->andReturn('2024-01-01T00:00:00Z');

    // Should nack by setting deadline to 0
    $this->subscription->shouldReceive('modifyAckDeadline')
        ->with($this->message, 0)
        ->once();

    $subscriber = new class($this->pubsubClient, 'test-subscription', null, ['nack_on_error' => true, 'monitoring' => ['log_consumed_messages' => false]]) extends StreamingSubscriber
    {
        public $pullCount = 0;

        // See note in 'uses long polling' test: shouldStop() is now invoked from both
        // the per-message inner loop and the outer-loop check.
        protected function shouldStop(): bool
        {
            return ++$this->pullCount >= 3;
        }
    };

    $subscriber->handler(function ($data) {
        throw new Exception('Processing error');
    });

    $subscriber->onError(function ($error) {
        expect($error->getMessage())->toBe('Processing error');
    });

    $subscriber->stream();
});

it('respects max messages per pull configuration', function () {
    $this->pubsubClient->shouldReceive('subscription')
        ->with('test-subscription')
        ->andReturn($this->subscription);

    $this->subscription->shouldReceive('exists')->andReturn(true);

    // Expect pull with custom max messages
    $this->subscription->shouldReceive('pull')
        ->with([
            'returnImmediately' => false,
            'maxMessages' => 50,  // Custom max messages from withFlowControl
        ])
        ->andReturn([])
        ->once();

    $subscriber = new class($this->pubsubClient, 'test-subscription', null, ['max_messages_per_pull' => 50]) extends StreamingSubscriber
    {
        protected function shouldStop(): bool
        {
            return true; // Stop after first pull
        }
    };

    $subscriber->stream();

    expect(true)->toBeTrue(); // Add assertion
});
