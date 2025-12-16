# Event Integration

Seamless bidirectional integration between Laravel's event system and Google Cloud Pub/Sub, enabling event-driven
architectures across multiple services with full cross-platform compatibility.

## Overview

- **Outbound**: Laravel events automatically publish to Pub/Sub topics
- **Inbound**: Pub/Sub messages trigger Laravel events
- **Cross-Service**: Events flow between Laravel, Go, Python, and other services
- **Bidirectional Flow**: Complete event round-trip between services
- **Automatic Event Mapping**: Smart routing based on event types and attributes

## Configuration

Enable event integration in `config/pubsub.php`:

```php
'events' => [
    'enabled' => true,
    
    // Events to publish to Pub/Sub
    'publish' => [
        \App\Events\OrderPlaced::class,
        \App\Events\UserRegistered::class,
        \App\Events\PaymentProcessed::class,
    ],
    
    // Or use patterns
    'publish_patterns' => [
        'App\Events\Order*',
        'App\Domain\*\Events\*',
    ],
    
    // Subscribe to external events
    'subscribe' => true,
],
```

## Publishing Events to Pub/Sub

### Method 1: Using Interface

```php
use OffloadProject\GooglePubSub\Contracts\ShouldPublishToPubSub;

class OrderPlaced implements ShouldPublishToPubSub
{
    public function __construct(
        public Order $order,
        public User $customer
    ) {}
    
    public function pubsubTopic(): string
    {
        return 'orders';
    }
    
    public function toPubSub(): array
    {
        return [
            'order_id' => $this->order->id,
            'customer_id' => $this->customer->id,
            'total' => $this->order->total,
            'items' => $this->order->items->toArray(),
            'created_at' => $this->order->created_at->toIso8601String(),
        ];
    }
}
```

### Method 2: Using Attributes

```php
use OffloadProject\GooglePubSub\Attributes\PublishTo;

#[PublishTo('orders')]
class OrderShipped
{
    public function __construct(
        public int $orderId,
        public string $trackingNumber,
        public string $carrier
    ) {}
    
    public function toPubSub(): array
    {
        return [
            'order_id' => $this->orderId,
            'tracking_number' => $this->trackingNumber,
            'carrier' => $this->carrier,
            'shipped_at' => now()->toIso8601String(),
        ];
    }
}
```

### Advanced Event Configuration

```php
#[PublishTo('orders')]
class OrderPlaced implements ShouldPublishToPubSub
{
    public function __construct(
        public Order $order
    ) {}
    
    public function pubsubTopic(): string
    {
        // Dynamic topic selection
        return $this->order->isInternational() ? 'orders-international' : 'orders';
    }
    
    public function toPubSub(): array
    {
        return $this->order->toArray();
    }
    
    /**
     * Add custom attributes to the Pub/Sub message
     */
    public function pubsubAttributes(): array
    {
        return [
            'region' => $this->order->shipping_region,
            'priority' => $this->order->total > 1000 ? 'high' : 'normal',
            'customer_type' => $this->order->customer->type,
        ];
    }
    
    /**
     * Set ordering key for ordered delivery
     */
    public function pubsubOrderingKey(): string
    {
        return "customer-{$this->order->customer_id}";
    }
}
```

## Receiving Events from Pub/Sub

### Configure Topics to Subscribe

```php
// config/pubsub.php
'topics' => [
    'orders' => [
        'subscribe' => true,
        'events' => [
            \App\Events\OrderPlaced::class,
            \App\Events\OrderUpdated::class,
        ],
    ],
    'payments' => [
        'subscribe' => true,
    ],
],
```

### Handling External Events

External services publish events that Laravel receives:

```go
// Go service publishes an event
message := &pubsub.Message{
    Data: []byte(`{
        "order_id": 123,
        "status": "shipped",
        "tracking_number": "ABC123"
    }`),
    Attributes: map[string]string{
        "event_type": "order.shipped",
        "source": "fulfillment-service",
    },
}
topic.Publish(ctx, message)
```

Laravel automatically dispatches events based on the message:

```php
// Listen for specific event types
Event::listen('pubsub.orders.order.shipped', function ($event) {
    $data = $event['data'];
    $message = $event['message'];
    
    Log::info('Order shipped', [
        'order_id' => $data['order_id'],
        'tracking' => $data['tracking_number'],
        'source' => $message->attributes()['source']
    ]);
    
    // Update local order status
    Order::find($data['order_id'])->markAsShipped($data['tracking_number']);
});

// Listen for all events from a topic
Event::listen('pubsub.orders.*', function ($eventName, $payload) {
    $event = $payload[0];
    Log::info("Received event: {$eventName}", $event);
});

// Listen for all Pub/Sub messages
Event::listen('pubsub.message.received', function ($event) {
    Log::info('Raw message received', [
        'topic' => $event['topic'],
        'data' => $event['data']
    ]);
});
```

### Reconstructing Laravel Events

When Laravel services communicate, events can be fully reconstructed:

```php
// Service A publishes
event(new OrderPlaced($order));

// Service B receives and reconstructs
class OrderPlaced
{
    public static function fromPubSub(array $data, $message): static
    {
        return new static(
            Order::make($data)
        );
    }
}

// The event is dispatched normally in Service B
Event::listen(OrderPlaced::class, function ($event) {
    // Handle the reconstructed event
});
```

## Topic Configuration

### Map Events to Topics

```php
'topics' => [
    'orders' => [
        'enable_message_ordering' => true,
        'schema' => 'order_events',
        'events' => [
            \App\Events\OrderPlaced::class,
            \App\Events\OrderUpdated::class,
            \App\Events\OrderCancelled::class,
        ],
    ],
    'users' => [
        'events' => [
            \App\Events\UserRegistered::class,
            \App\Events\UserUpdated::class,
        ],
    ],
    'payments' => [
        'schema' => 'payment_events',
        'subscribe' => false, // Only publish, don't subscribe
    ],
],
```

## Event Listener Examples

### Using Event Service Provider

```php
// app/Providers/EventServiceProvider.php
protected $listen = [
    // Specific event types
    'pubsub.orders.order.shipped' => [
        \App\Listeners\UpdateOrderStatus::class,
        \App\Listeners\SendShippingNotification::class,
    ],
    
    // All events from a topic
    'pubsub.payments.*' => [
        \App\Listeners\ProcessPaymentEvent::class,
    ],
    
    // All Pub/Sub messages
    'pubsub.message.received' => [
        \App\Listeners\LogPubSubMessage::class,
    ],
];
```

### Listener Implementation

```php
namespace App\Listeners;

class ProcessPaymentEvent
{
    public function handle(string $eventName, array $payload): void
    {
        $event = $payload[0];
        $data = $event['data'];
        $message = $event['message'];
        $topic = $event['topic'];
        
        match($eventName) {
            'pubsub.payments.payment.completed' => $this->handleCompleted($data),
            'pubsub.payments.payment.failed' => $this->handleFailed($data),
            'pubsub.payments.refund.issued' => $this->handleRefund($data),
            default => Log::warning("Unknown payment event: {$eventName}")
        };
    }
    
    private function handleCompleted(array $data): void
    {
        Order::find($data['order_id'])->markAsPaid([
            'transaction_id' => $data['transaction_id'],
            'amount' => $data['amount'],
        ]);
    }
}
```

## Best Practices

### 1. Use Consistent Event Formats

Define a standard structure for events across services:

```php
// Standardized event structure
[
    'event_id' => 'uuid',
    'event_type' => 'order.placed',
    'timestamp' => '2024-01-01T00:00:00Z',
    'version' => '1.0',
    'data' => [
        // Event-specific data
    ],
    'metadata' => [
        'source' => 'order-service',
        'correlation_id' => 'uuid',
    ]
]
```

### 2. Version Your Events

Handle event evolution:

```php
public function toPubSub(): array
{
    return [
        'version' => '2.0',
        'order_id' => $this->order->id,
        // v2.0 additions
        'tax_amount' => $this->order->tax_amount,
        'discount_codes' => $this->order->discount_codes,
    ];
}

// In listener
public function handle($event)
{
    $version = $event['version'] ?? '1.0';
    
    match($version) {
        '1.0' => $this->handleV1($event),
        '2.0' => $this->handleV2($event),
        default => Log::warning("Unknown event version: {$version}")
    };
}
```

### 3. Add Correlation IDs

Track events across services:

```php
public function pubsubAttributes(): array
{
    return [
        'correlation_id' => request()->header('X-Correlation-ID') ?? Str::uuid(),
        'source_service' => config('app.name'),
        'user_id' => auth()->id(),
    ];
}
```

### 4. Handle Duplicate Events

Pub/Sub may deliver messages more than once:

```php
Event::listen('pubsub.orders.*', function ($eventName, $payload) {
    $event = $payload[0];
    $messageId = $event['message']->id();
    
    // Idempotency check
    if (ProcessedEvent::where('message_id', $messageId)->exists()) {
        return; // Already processed
    }
    
    DB::transaction(function () use ($event, $messageId) {
        // Process event
        $this->processEvent($event);
        
        // Mark as processed
        ProcessedEvent::create(['message_id' => $messageId]);
    });
});
```

### 5. Monitor Event Flow

```php
Event::listen('pubsub.message.received', function ($event) {
    Metrics::increment('pubsub.messages.received', 1, [
        'topic' => $event['topic'],
        'event_type' => $event['data']['event_type'] ?? 'unknown',
    ]);
});
```
