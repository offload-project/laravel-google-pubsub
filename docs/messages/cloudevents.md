# CloudEvents Support

Full CloudEvents v1.0 support for standardized event messaging across services and platforms. CloudEvents is a CNCF
specification that provides a common way to describe event data.

## Overview

CloudEvents standardizes how events are structured, making it easier to:

- **Integrate with multiple platforms** - Azure Event Grid, AWS EventBridge, Google Cloud Eventarc
- **Build event-driven architectures** - Consistent event format across all services
- **Improve observability** - Standard metadata for tracing and monitoring
- **Enable tool interoperability** - Works with Knative, Serverless frameworks, etc.

## Configuration

Enable CloudEvents formatting in `config/pubsub.php`:

```php
'formatters' => [
    'default' => 'cloud_events', // Use CloudEvents by default
    'cloud_events_source' => env('PUBSUB_CLOUDEVENTS_SOURCE', config('app.url')),
],

'topics' => [
    'orders' => [
        'formatter' => 'cloud_events',
        'schema' => 'order_events',
    ],
    'analytics' => [
        'formatter' => 'cloud_events',
    ],
],
```

Set your CloudEvents source:

```dotenv
PUBSUB_CLOUDEVENTS_SOURCE=https://myapp.com
```

## Basic Usage

### Publishing CloudEvents

```php
use OffloadProject\GooglePubSub\Facades\PubSub;

// Automatically formatted as CloudEvents
PubSub::publish('orders', [
    'order_id' => 123,
    'customer_id' => 456,
    'total' => 99.99,
    'status' => 'created',
]);
```

This produces a CloudEvents message:

```json
{
  "specversion": "1.0",
  "type": "com.example.event",
  "source": "https://myapp.com",
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "time": "2024-01-15T10:30:00Z",
  "datacontenttype": "application/json",
  "data": {
    "order_id": 123,
    "customer_id": 456,
    "total": 99.99,
    "status": "created"
  }
}
```

### Custom Event Types

```php
// Specify event type in data
PubSub::publish('orders', [
    'type' => 'com.mycompany.orders.created',
    'order_id' => 123,
    'total' => 99.99,
]);

// Results in:
// "type": "com.mycompany.orders.created"
```

### Adding Metadata

```php
PubSub::publish('orders', [
    'type' => 'com.mycompany.orders.shipped',
    'id' => 'order-123-shipped',
    'source' => 'https://fulfillment.mycompany.com',
    'subject' => 'orders/123',
    'order_id' => 123,
    'tracking_number' => 'ABC123',
]);
```

Results in:

```json
{
  "specversion": "1.0",
  "type": "com.mycompany.orders.shipped",
  "source": "https://fulfillment.mycompany.com",
  "id": "order-123-shipped",
  "subject": "orders/123",
  "time": "2024-01-15T10:30:00Z",
  "datacontenttype": "application/json",
  "data": {
    "order_id": 123,
    "tracking_number": "ABC123"
  }
}
```

## Event Classes with CloudEvents

### Method 1: Event Object Methods

```php
namespace App\Events;

use OffloadProject\GooglePubSub\Contracts\ShouldPublishToPubSub;

class OrderShipped implements ShouldPublishToPubSub
{
    public function __construct(
        public Order $order,
        public string $trackingNumber
    ) {}
    
    public function pubsubTopic(): string
    {
        return 'orders';
    }
    
    // CloudEvents-specific methods
    public function getEventType(): string
    {
        return 'com.mycompany.orders.shipped';
    }
    
    public function getEventId(): string
    {
        return "order-{$this->order->id}-shipped";
    }
    
    public function getEventSource(): string
    {
        return 'https://fulfillment.mycompany.com';
    }
    
    public function getEventSubject(): string
    {
        return "orders/{$this->order->id}";
    }
    
    public function toCloudEventData(): array
    {
        return [
            'order_id' => $this->order->id,
            'customer_id' => $this->order->customer_id,
            'tracking_number' => $this->trackingNumber,
            'shipped_at' => now()->toIso8601String(),
        ];
    }
    
    // Fallback for non-CloudEvents formatters
    public function toPubSub(): array
    {
        return array_merge($this->toCloudEventData(), [
            'type' => $this->getEventType(),
        ]);
    }
}
```

### Method 2: Data Array Approach

```php
class OrderService
{
    public function shipOrder(Order $order, string $trackingNumber): void
    {
        $order->update(['status' => 'shipped', 'tracking_number' => $trackingNumber]);
        
        PubSub::publish('orders', [
            'type' => 'com.mycompany.orders.shipped',
            'id' => "order-{$order->id}-shipped",
            'source' => 'https://fulfillment.mycompany.com',
            'subject' => "orders/{$order->id}",
            'order_id' => $order->id,
            'customer_id' => $order->customer_id,
            'tracking_number' => $trackingNumber,
            'shipped_at' => now()->toIso8601String(),
        ]);
    }
}
```

## Receiving CloudEvents

### Automatic Parsing

CloudEvents are automatically parsed when received:

```php
Event::listen('pubsub.orders.*', function ($eventName, $payload) {
    $event = $payload[0];
    $data = $event['data']; // Just the data payload
    $message = $event['message'];
    
    // Access CloudEvents metadata from message attributes
    $cloudEventType = $message->attributes()['ce-type'] ?? null;
    $cloudEventSource = $message->attributes()['ce-source'] ?? null;
    $cloudEventId = $message->attributes()['ce-id'] ?? null;
    
    Log::info('Received CloudEvent', [
        'type' => $cloudEventType,
        'source' => $cloudEventSource,
        'id' => $cloudEventId,
        'data' => $data,
    ]);
});
```

### Event Type Mapping

Map CloudEvent types to Laravel events:

```php
// In EventServiceProvider
protected $listen = [
    'pubsub.orders.com.mycompany.orders.created' => [
        \App\Listeners\ProcessOrderCreated::class,
    ],
    'pubsub.orders.com.mycompany.orders.shipped' => [
        \App\Listeners\ProcessOrderShipped::class,
    ],
];
```

## CloudEvents with Webhooks

Push subscriptions automatically handle CloudEvents:

```bash
# Create push subscription
php artisan pubsub:subscriptions:create-push \
    orders-webhook \
    orders \
    https://myapp.com/pubsub/webhook/orders
```

Webhook messages are automatically parsed as CloudEvents:

```php
// The webhook controller automatically handles CloudEvents format
// Your event listeners receive the parsed data
Event::listen('pubsub.orders.*', function ($eventName, $payload) {
    $event = $payload[0];
    $data = $event['data']; // CloudEvents data payload
    $message = $event['message']; // WebhookMessage with CloudEvents metadata
});
```

## Schema Validation with CloudEvents

Combine CloudEvents with JSON Schema validation:

```php
// config/pubsub.php
'topics' => [
    'orders' => [
        'formatter' => 'cloud_events',
        'schema' => 'order_events',
    ],
],

'schemas' => [
    'order_events' => [
        'file' => 'schemas/order-events.json',
    ],
],
```

Schema validates the `data` portion of CloudEvents:

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Order Events CloudEvents Schema",
  "description": "Schema for order events in CloudEvents format",
  "type": "object",
  "required": [
    "order_id",
    "customer_id"
  ],
  "properties": {
    "order_id": {
      "type": "integer",
      "description": "Unique order identifier"
    },
    "customer_id": {
      "type": "integer",
      "description": "Customer who placed the order"
    },
    "tracking_number": {
      "type": "string",
      "description": "Shipping tracking number",
      "pattern": "^[A-Z0-9]+$"
    },
    "shipped_at": {
      "type": "string",
      "format": "date-time",
      "description": "ISO8601 timestamp when order was shipped"
    }
  }
}
```

## Cross-Platform Integration

### Azure Event Grid

```php
// Events published to Pub/Sub can be forwarded to Azure Event Grid
// The CloudEvents format ensures compatibility

PubSub::publish('integration', [
    'type' => 'com.mycompany.user.created',
    'source' => 'https://api.mycompany.com',
    'subject' => 'users/123',
    'user_id' => 123,
    'email' => 'user@example.com',
]);
```

### AWS EventBridge

```python
# Python service consuming CloudEvents from Pub/Sub
# and forwarding to AWS EventBridge

import json
from google.cloud import pubsub_v1
import boto3

def process_cloudevent(message):
    # Message is already in CloudEvents format
    event_data = json.loads(message.data.decode('utf-8'))
    
    # Forward to EventBridge
    eventbridge = boto3.client('events')
    eventbridge.put_events(
        Entries=[{
            'Source': event_data['source'],
            'DetailType': event_data['type'],
            'Detail': json.dumps(event_data['data']),
            'Time': event_data['time'],
        }]
    )
```

### Knative Eventing

```yaml
# Knative service receiving CloudEvents from Pub/Sub
apiVersion: serving.knative.dev/v1
kind: Service
metadata:
  name: order-processor
spec:
  template:
    metadata:
      annotations:
        autoscaling.knative.dev/minScale: "1"
    spec:
      containers:
        - image: gcr.io/my-project/order-processor
          env:
            - name: CE_SPECVERSION
              value: "1.0"
---
apiVersion: eventing.knative.dev/v1
kind: Trigger
metadata:
  name: order-events
spec:
  broker: default
  filter:
    attributes:
      type: com.mycompany.orders.created
  subscriber:
    ref:
      apiVersion: serving.knative.dev/v1
      kind: Service
      name: order-processor
```

## Testing CloudEvents

### Unit Testing

```php
// tests/Unit/CloudEventsTest.php
namespace Tests\Unit;

use Tests\TestCase;
use OffloadProject\GooglePubSub\Formatters\CloudEventsFormatter;

class CloudEventsTest extends TestCase
{
    public function test_formats_data_as_cloudevents()
    {
        $formatter = new CloudEventsFormatter('https://test.com', 'test.event');
        
        $data = [
            'user_id' => 123,
            'action' => 'login',
        ];
        
        $json = $formatter->format($data);
        $event = json_decode($json, true);
        
        $this->assertEquals('1.0', $event['specversion']);
        $this->assertEquals('test.event', $event['type']);
        $this->assertEquals('https://test.com', $event['source']);
        $this->assertArrayHasKey('id', $event);
        $this->assertArrayHasKey('time', $event);
        $this->assertEquals('application/json', $event['datacontenttype']);
        $this->assertEquals($data, $event['data']);
    }
    
    public function test_uses_custom_event_metadata()
    {
        $formatter = new CloudEventsFormatter();
        
        $data = [
            'type' => 'com.example.custom',
            'id' => 'custom-123',
            'source' => 'https://custom.com',
            'subject' => 'users/123',
            'user_id' => 123,
        ];
        
        $json = $formatter->format($data);
        $event = json_decode($json, true);
        
        $this->assertEquals('com.example.custom', $event['type']);
        $this->assertEquals('custom-123', $event['id']);
        $this->assertEquals('https://custom.com', $event['source']);
        $this->assertEquals('users/123', $event['subject']);
        $this->assertEquals(['user_id' => 123], $event['data']);
    }
}
```

### Integration Testing

```php
// tests/Integration/CloudEventsIntegrationTest.php
namespace Tests\Integration;

use Tests\TestCase;
use OffloadProject\GooglePubSub\Facades\PubSub;

class CloudEventsIntegrationTest extends TestCase
{
    public function test_publishes_and_receives_cloudevents()
    {
        // Configure topic to use CloudEvents
        config(['pubsub.topics.test-topic.formatter' => 'cloud_events']);
        
        // Publish CloudEvent
        $messageId = PubSub::publish('test-topic', [
            'type' => 'com.test.event',
            'id' => 'test-123',
            'test_data' => 'value',
        ]);
        
        $this->assertNotNull($messageId);
        
        // Subscribe and verify CloudEvents format
        $subscriber = PubSub::subscribe('test-subscription', 'test-topic');
        
        $receivedEvent = null;
        $subscriber->handler(function ($data, $message) use (&$receivedEvent) {
            $receivedEvent = [
                'data' => $data,
                'attributes' => $message->attributes(),
            ];
        });
        
        $messages = $subscriber->pull(1);
        
        $this->assertNotEmpty($messages);
        $this->assertEquals(['test_data' => 'value'], $receivedEvent['data']);
        
        // Verify CloudEvents attributes were preserved
        $this->assertEquals('com.test.event', $receivedEvent['attributes']['ce-type']);
        $this->assertEquals('test-123', $receivedEvent['attributes']['ce-id']);
    }
}
```

## Advanced Features

### Custom CloudEvents Formatter

```php
// app/Formatters/CustomCloudEventsFormatter.php
namespace App\Formatters;

use OffloadProject\GooglePubSub\Formatters\CloudEventsFormatter;

class CustomCloudEventsFormatter extends CloudEventsFormatter
{
    protected function getEventType($data): string
    {
        // Custom type generation logic
        if (is_array($data) && isset($data['entity'], $data['action'])) {
            return "com.mycompany.{$data['entity']}.{$data['action']}";
        }
        
        return parent::getEventType($data);
    }
    
    protected function getEventSubject($data): ?string
    {
        // Generate subject from data
        if (is_array($data) && isset($data['entity'], $data['id'])) {
            return "{$data['entity']}/{$data['id']}";
        }
        
        return parent::getEventSubject($data);
    }
}
```

Register your custom formatter:

```php
// In a service provider
use App\Formatters\CustomCloudEventsFormatter;

$this->app->bind('pubsub.formatter.custom_cloud_events', function () {
    return new CustomCloudEventsFormatter(
        config('pubsub.formatters.cloud_events_source')
    );
});
```

### CloudEvents Extensions

Add custom extensions to CloudEvents:

```php
class OrderShipped implements ShouldPublishToPubSub
{
    public function toCloudEventData(): array
    {
        return [
            'order_id' => $this->order->id,
            'tracking_number' => $this->trackingNumber,
            
            // CloudEvents extensions (prefixed with custom domain)
            'mycompanyorderversion' => '2.0',
            'mycompanyregion' => 'us-east-1',
            'mycompanytenant' => $this->order->tenant_id,
        ];
    }
}
```

## Best Practices

### 1. Consistent Event Types

Use reverse DNS notation for event types:

```php
// ✓ Good
'type' => 'com.mycompany.orders.created'
'type' => 'com.mycompany.users.profile.updated'
'type' => 'com.mycompany.payments.processed'

// ✗ Avoid
'type' => 'order_created'
'type' => 'UserUpdate'
'type' => 'payment-done'
```

### 2. Meaningful Sources

Use stable, resolvable URIs for sources:

```php
// ✓ Good
'source' => 'https://api.mycompany.com/orders'
'source' => 'https://fulfillment.mycompany.com'
'source' => 'urn:mycompany:service:user-management'

// ✗ Avoid
'source' => 'order-service'
'source' => 'localhost:8080'
'source' => 'my-app'
```

### 3. Structured Subjects

Use hierarchical subjects for filtering:

```php
// ✓ Good
'subject' => 'orders/123'
'subject' => 'users/456/profile'
'subject' => 'tenants/abc/orders/789'

// ✗ Avoid
'subject' => '123'
'subject' => 'order123'
'subject' => 'user-profile-update'
```

### 4. Stable Event IDs

Ensure event IDs are unique and stable:

```php
public function getEventId(): string
{
    // Include entity type and action for uniqueness
    return "order-{$this->order->id}-{$this->action}-" . now()->timestamp;
}
```

### 5. Version Your Event Types

Plan for schema evolution:

```php
// Start with version
'type' => 'com.mycompany.orders.created.v1'

// Evolve to v2 when needed
'type' => 'com.mycompany.orders.created.v2'

// Handle both versions in consumers
Event::listen('pubsub.orders.com.mycompany.orders.created.*', function ($event) {
    $type = $event['message']->attributes()['ce-type'];
    
    match(true) {
        str_ends_with($type, '.v1') => $this->handleV1($event),
        str_ends_with($type, '.v2') => $this->handleV2($event),
        default => $this->handleLatest($event),
    };
});
```

## Troubleshooting

### Common Issues

**Missing CloudEvents Fields**

```php
// Ensure required CloudEvents fields are present
$event = [
    'specversion' => '1.0',  // Required
    'type' => 'com.example.test',  // Required
    'source' => 'https://example.com',  // Required
    'id' => 'unique-id',  // Required
    'data' => ['key' => 'value'],
];
```

**Invalid Event Types**

```php
// Event types must not be empty and should follow conventions
'type' => 'com.mycompany.orders.created'  // ✓ Valid
'type' => ''  // ✗ Invalid
'type' => 'just-a-string'  // ✓ Valid but not recommended
```

**Debugging CloudEvents**

Enable verbose logging:

```php
'monitoring' => [
    'log_published_messages' => true,
    'log_consumed_messages' => true,
],
```

Inspect CloudEvents structure:

```php
Event::listen('pubsub.message.received', function ($event) {
    if (isset($event['message'])) {
        $attributes = $event['message']->attributes();
        $cloudEventAttrs = array_filter($attributes, fn($key) => str_starts_with($key, 'ce-'), ARRAY_FILTER_USE_KEY);
        
        Log::debug('CloudEvents attributes', $cloudEventAttrs);
    }
});
```
