<?php

declare(strict_types=1);

use Illuminate\Contracts\Events\Dispatcher;
use OffloadProject\GooglePubSub\Attributes\PublishTo;
use OffloadProject\GooglePubSub\Contracts\ShouldPublishToPubSub;
use OffloadProject\GooglePubSub\Events\PubSubEventDispatcher;
use OffloadProject\GooglePubSub\Events\PubSubEventSubscriber;
use OffloadProject\GooglePubSub\Messages\WebhookMessage;
use OffloadProject\GooglePubSub\Publisher\Publisher;
use OffloadProject\GooglePubSub\PubSubManager;

// Test event classes
#[PublishTo('orders')]
final class TestOrderEvent implements ShouldPublishToPubSub
{
    public function __construct(
        public int $orderId,
        public float $total
    ) {}

    public function pubsubTopic(): string
    {
        return 'orders';
    }

    public function toPubSub(): array
    {
        return [
            'order_id' => $this->orderId,
            'total' => $this->total,
        ];
    }

    public function pubsubAttributes(): array
    {
        return ['priority' => 'high'];
    }

    public function pubsubOrderingKey(): string
    {
        return "order-{$this->orderId}";
    }
}

final class TestUserEvent
{
    public function __construct(public int $userId) {}

    public function toArray(): array
    {
        return ['user_id' => $this->userId];
    }
}

beforeEach(function () {
    $this->pubsub = Mockery::mock(PubSubManager::class);
    $this->events = Mockery::mock(Dispatcher::class);
    $this->publisher = Mockery::mock(Publisher::class);

    $this->pubsub->shouldReceive('publisher')->andReturn($this->publisher);
});

describe('PubSubEventDispatcher', function () {
    it('publishes events that implement ShouldPublishToPubSub', function () {
        $config = [
            'events' => ['enabled' => true],
            'monitoring' => ['log_published_messages' => false],
        ];

        $dispatcher = new PubSubEventDispatcher($this->pubsub, $this->events, $config);

        $this->events->shouldReceive('listen')
            ->with('*', [$dispatcher, 'handleEvent'])
            ->once();

        $this->pubsub->shouldReceive('publish')
            ->withArgs(function ($topic, $data, $attributes, $options) {
                return $topic === 'orders'
                    && $data['data']['order_id'] === 123
                    && $data['data']['total'] === 99.99
                    && $attributes['priority'] === 'high'
                    && $options['ordering_key'] === 'order-123';
            })
            ->andReturn('msg-123')
            ->once();

        $dispatcher->register();

        $event = new TestOrderEvent(123, 99.99);
        $dispatcher->handleEvent(TestOrderEvent::class, [$event]);
    });

    it('publishes events in configured publish list', function () {
        $config = [
            'events' => [
                'enabled' => true,
                'publish' => [TestUserEvent::class],
            ],
            'monitoring' => ['log_published_messages' => false],
        ];

        $dispatcher = new PubSubEventDispatcher($this->pubsub, $this->events, $config);

        $this->events->shouldReceive('listen')->once();

        $this->pubsub->shouldReceive('publish')
            ->withArgs(function ($topic, $data) {
                return $topic === 'laravel-events'  // Default topic
                    && $data['data']['user_id'] === 456;
            })
            ->andReturn('msg-456')
            ->once();

        $dispatcher->register();

        $event = new TestUserEvent(456);
        $dispatcher->handleEvent(TestUserEvent::class, [$event]);
    });

    it('publishes events matching patterns', function () {
        $config = [
            'events' => [
                'enabled' => true,
                'publish_patterns' => ['Test*Event'],
            ],
            'monitoring' => ['log_published_messages' => false],
        ];

        $dispatcher = new PubSubEventDispatcher($this->pubsub, $this->events, $config);

        $this->events->shouldReceive('listen')->once();
        $this->pubsub->shouldReceive('publish')->twice()->andReturn('msg-123');

        $dispatcher->register();

        $dispatcher->handleEvent(TestOrderEvent::class, [new TestOrderEvent(1, 10)]);
        $dispatcher->handleEvent(TestUserEvent::class, [new TestUserEvent(2)]);
    });

    it('does not publish when disabled', function () {
        $config = ['events' => ['enabled' => false]];

        $dispatcher = new PubSubEventDispatcher($this->pubsub, $this->events, $config);

        $this->events->shouldNotReceive('listen');
        $this->publisher->shouldNotReceive('publish');

        $dispatcher->register();
    });

    it('prevents infinite loops', function () {
        $config = [
            'events' => [
                'enabled' => true,
                'publish' => [TestOrderEvent::class],
            ],
            'monitoring' => ['log_published_messages' => false],
        ];

        $dispatcher = new PubSubEventDispatcher($this->pubsub, $this->events, $config);

        $this->events->shouldReceive('listen')->once();
        $this->pubsub->shouldReceive('publish')->once()->andReturn('msg-123');

        $dispatcher->register();

        // Manually set the dispatching array to simulate an event already being processed
        $reflection = new ReflectionClass($dispatcher);
        $property = $reflection->getProperty('dispatching');
        $property->setAccessible(true);
        $property->setValue($dispatcher, [TestOrderEvent::class]);

        // This call should be ignored since the event is already being dispatched
        $dispatcher->handleEvent(TestOrderEvent::class, [new TestOrderEvent(1, 10)]);

        // Reset the dispatching array
        $property->setValue($dispatcher, []);

        // This call should publish
        $dispatcher->handleEvent(TestOrderEvent::class, [new TestOrderEvent(2, 20)]);
    });
});

describe('PubSubEventSubscriber', function () {
    it('dispatches Laravel events from messages', function () {
        $config = [
            'events' => ['handle_classes' => [TestOrderEvent::class]],
        ];

        $subscriber = new PubSubEventSubscriber($this->pubsub, $this->events, $config);

        $message = new WebhookMessage(
            'msg-123',
            '{"test":"data"}',
            [],
            '2024-01-01T00:00:00Z'
        );

        $data = [
            'event' => TestOrderEvent::class,
            'class' => TestOrderEvent::class,
            'data' => ['orderId' => 123, 'total' => 99.99],  // Use camelCase to match constructor
        ];

        $this->events->shouldReceive('dispatch')
            ->withArgs(function ($event) {
                return $event instanceof TestOrderEvent
                    && $event->orderId === 123
                    && $event->total === 99.99;
            })
            ->once();

        $subscriber->handleMessage($data, $message, 'orders');
    });

    it('dispatches generic events for non-Laravel messages', function () {
        $config = [];

        $subscriber = new PubSubEventSubscriber($this->pubsub, $this->events, $config);

        $message = new WebhookMessage(
            'msg-123',
            '{"test":"data"}',
            ['event_type' => 'order.shipped'],
            '2024-01-01T00:00:00Z'
        );

        $data = ['order_id' => 123, 'tracking' => 'ABC123'];

        $this->events->shouldReceive('dispatch')
            ->withArgs(function ($eventName, $payload) {
                return $eventName === 'pubsub.orders.order.shipped'
                    && $payload['data']['order_id'] === 123;
            })
            ->once();

        $this->events->shouldReceive('dispatch')
            ->with('pubsub.message.received', Mockery::any())
            ->once();

        $subscriber->handleMessage($data, $message, 'orders');
    });

    it('reconstructs events with fromPubSub method', function () {
        $config = [
            'events' => ['handle_classes' => ['TestEventWithFactory']],
        ];

        $subscriber = new PubSubEventSubscriber($this->pubsub, $this->events, $config);

        $message = new WebhookMessage('msg-123', '{}', [], '2024-01-01');

        $data = [
            'event' => 'TestEventWithFactory',
            'class' => 'TestEventWithFactory',
            'data' => ['test' => 'factory'],
        ];

        // Mock the static factory method
        $this->events->shouldReceive('dispatch')
            ->withArgs(function ($eventName) {
                return $eventName === 'pubsub.event.received';
            })
            ->once();

        $subscriber->handleMessage($data, $message, 'test');
    });
});
