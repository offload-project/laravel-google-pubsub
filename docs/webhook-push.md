# Webhooks (Push Subscriptions)

Push subscriptions deliver messages to your application via HTTP POST requests, ideal for serverless environments or
when you can't run persistent queue workers. Comprehensive webhook support with built-in security, flexible
authentication, and automatic request verification.

## Overview

- **Push Delivery**: Pub/Sub sends messages directly to your HTTP endpoint
- **Automatic Retries**: Built-in retry logic with exponential backoff
- **Multi-layer Security**: Token authentication, IP allowlisting, and request verification
- **Scalability**: Handle high message volumes without managing workers
- **Development Support**: Local development with ngrok and emulator integration

## Configuration

### 1. Enable Webhooks

In `config/pubsub.php`:

```php
'webhook' => [
    'enabled' => true,
    'route_prefix' => 'pubsub/webhook',
    'auth_token' => env('PUBSUB_WEBHOOK_TOKEN'),
    'skip_verification' => false,
    
    // Optional IP allowlist
    'allowed_ips' => [
        // Google's IP ranges
    ],
    
    // Custom middleware
    'middleware' => [
        \OffloadProject\GooglePubSub\Http\Middleware\VerifyPubSubWebhook::class,
        'throttle:1000,1', // Rate limiting
    ],
],
```

### 2. Set Environment Variables

```dotenv
PUBSUB_WEBHOOK_ENABLED=true
PUBSUB_WEBHOOK_PREFIX=pubsub/webhook
PUBSUB_WEBHOOK_TOKEN=your-secret-token-here
```

## Creating Push Subscriptions

### Using Artisan Command

```bash
# Basic push subscription
php artisan pubsub:subscriptions:create-push orders-webhook orders https://myapp.com/pubsub/webhook/orders

# With authentication
php artisan pubsub:subscriptions:create-push orders-webhook orders https://myapp.com/pubsub/webhook/orders \
    --token=your-secret-token

# With options
php artisan pubsub:subscriptions:create-push orders-webhook orders https://myapp.com/pubsub/webhook/orders \
    --token=your-secret-token \
    --ack-deadline=60 \
    --enable-ordering \
    --dead-letter
```

### Programmatically

```php
use OffloadProject\GooglePubSub\Facades\PubSub;

PubSub::createSubscription('orders-webhook', 'orders', [
    'pushConfig' => [
        'pushEndpoint' => 'https://myapp.com/pubsub/webhook/orders',
        'attributes' => [
            'x-goog-subscription-authorization' => 'Bearer ' . config('pubsub.webhook.auth_token'),
        ],
    ],
    'ackDeadlineSeconds' => 60,
]);
```

## Webhook URL Format

Your webhook URLs follow this pattern:

```
https://yourdomain.com/{prefix}/{topic}
```

Examples:

- `https://api.example.com/pubsub/webhook/orders`
- `https://api.example.com/pubsub/webhook/users`
- `https://api.example.com/pubsub/webhook/notifications`

## Security

### 1. Token Authentication

Pub/Sub includes the token in the `Authorization` header:

```php
// Creating subscription with token
php artisan pubsub:subscriptions:create-push my-webhook my-topic https://myapp.com/webhook \
    --token=secret-token-123

// Pub/Sub sends:
// Authorization: Bearer secret-token-123
```

### 2. Request Verification

Automatically verifies:

- Required Google Pub/Sub headers
- Authentication token (if configured)
- IP address (if allowlist configured)

### 3. HTTPS Required

Always use HTTPS endpoints in production:

```bash
# ✓ Good
https://api.example.com/pubsub/webhook/orders

# ✗ Bad (only for local development)
http://api.example.com/pubsub/webhook/orders
```

## Handling Webhook Messages

Messages are automatically processed through the event system:

### 1. Listen for Events

```php
// app/Providers/EventServiceProvider.php
protected $listen = [
    'pubsub.orders.*' => [
        \App\Listeners\ProcessOrderWebhook::class,
    ],
];
```

### 2. Process Messages

```php
namespace App\Listeners;

class ProcessOrderWebhook
{
    public function handle(string $eventName, array $payload): void
    {
        $event = $payload[0];
        $data = $event['data'];
        $message = $event['message'];
        
        Log::info('Webhook received', [
            'event' => $eventName,
            'message_id' => $message->id(),
            'attributes' => $message->attributes(),
        ]);
        
        // Process based on event type
        match($eventName) {
            'pubsub.orders.order.created' => $this->handleOrderCreated($data),
            'pubsub.orders.order.updated' => $this->handleOrderUpdated($data),
            default => Log::warning("Unknown event: {$eventName}")
        };
    }
}
```

## Local Development

### Using ngrok

For local development with real Pub/Sub:

```bash
# 1. Start your Laravel app
php artisan serve

# 2. Create ngrok tunnel
ngrok http 8000

# 3. Create subscription with ngrok URL
php artisan pubsub:subscriptions:create-push local-webhook orders \
    https://abc123.ngrok.io/pubsub/webhook/orders
```

### Using the Emulator

The Pub/Sub emulator supports push subscriptions:

```bash
# 1. Start emulator
gcloud beta emulators pubsub start

# 2. Set environment
export PUBSUB_EMULATOR_HOST=localhost:8085

# 3. Create push subscription
php artisan pubsub:subscriptions:create-push local-webhook orders \
    http://localhost:8000/pubsub/webhook/orders
```

## Testing Webhooks

### Manual Testing with cURL

```bash
curl -X POST https://myapp.com/pubsub/webhook/orders \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your-secret-token" \
  -H "X-Goog-Resource-State: exists" \
  -H "X-Goog-Message-Id: test-123" \
  -H "X-Goog-Subscription-Name: projects/my-project/subscriptions/orders-webhook" \
  -d '{
    "message": {
      "data": "eyJvcmRlcl9pZCI6IDEyMywgInN0YXR1cyI6ICJwZW5kaW5nIn0=",
      "attributes": {
        "event_type": "order.created"
      },
      "messageId": "test-123",
      "publishTime": "2024-01-01T00:00:00Z"
    }
  }'
```

Note: The `data` field must be base64 encoded.

### Unit Testing

```php
use Illuminate\Support\Facades\Event;

public function test_webhook_processes_order_event()
{
    Event::fake();
    
    $payload = [
        'message' => [
            'data' => base64_encode(json_encode([
                'order_id' => 123,
                'status' => 'pending'
            ])),
            'attributes' => ['event_type' => 'order.created'],
            'messageId' => 'test-123',
            'publishTime' => now()->toIso8601String(),
        ]
    ];
    
    $response = $this->postJson('/pubsub/webhook/orders', $payload, [
        'Authorization' => 'Bearer ' . config('pubsub.webhook.auth_token'),
        'X-Goog-Resource-State' => 'exists',
        'X-Goog-Message-Id' => 'test-123',
        'X-Goog-Subscription-Name' => 'test-sub',
    ]);
    
    $response->assertOk();
    Event::assertDispatched('pubsub.orders.order.created');
}
```

## Advanced Configuration

### Custom Middleware

Add additional security or processing:

```php
// config/pubsub.php
'webhook' => [
    'middleware' => [
        \OffloadProject\GooglePubSub\Http\Middleware\VerifyPubSubWebhook::class,
        'throttle:1000,1',
        \App\Http\Middleware\LogWebhookRequests::class,
        \App\Http\Middleware\ValidateWebhookPayload::class,
    ],
],
```

### IP Allowlisting

Restrict to Google's IP ranges:

```php
'webhook' => [
    'allowed_ips' => [
        // Google's current IP ranges
        '35.190.0.0/16',
        '35.191.0.0/16',
        // ... add more from Google's documentation
    ],
],
```

### Conditional Processing

Skip processing in certain environments:

```php
class ProcessOrderWebhook
{
    public function handle(string $eventName, array $payload): void
    {
        // Skip in testing
        if (app()->environment('testing')) {
            return;
        }
        
        // Skip if maintenance mode
        if (app()->isDownForMaintenance()) {
            throw new \Exception('Application in maintenance mode');
        }
        
        // Process normally
        $this->processOrder($payload[0]);
    }
}
```

## Best Practices

### 1. Idempotent Processing

Handle duplicate deliveries:

```php
public function handle($event)
{
    $messageId = $event['message']->id();
    
    $processed = Cache::remember("webhook:{$messageId}", 3600, function () use ($event) {
        // Process only if not in cache
        $this->processEvent($event);
        return true;
    });
    
    if (!$processed) {
        Log::info('Duplicate webhook ignored', ['message_id' => $messageId]);
    }
}
```

### 2. Quick Response

Acknowledge quickly to avoid retries:

```php
public function handle($event)
{
    // Queue for async processing
    ProcessWebhookJob::dispatch($event)->onQueue('webhooks');
    
    // Return quickly
    return response('', 200);
}
```

### 3. Error Handling

Return 200 to prevent retry storms:

```php
try {
    $this->processWebhook($event);
} catch (\Exception $e) {
    // Log error
    Log::error('Webhook processing failed', [
        'error' => $e->getMessage(),
        'message_id' => $event['message']->id()
    ]);
    
    // Still return 200 to prevent retries
    // The message will be redelivered based on subscription settings
    return response('', 200);
}
```

### 4. Monitor Webhook Health

Track webhook performance:

```php
'monitoring' => [
    'log_webhooks' => true,
],

// Custom monitoring
Event::listen('pubsub.message.received', function ($event) {
    if ($event['message'] instanceof WebhookMessage) {
        Metrics::timing('webhook.processing_time', $processingTime, [
            'topic' => $event['topic']
        ]);
    }
});
```

### 5. Use Dead Letter Topics

Configure maximum delivery attempts:

```bash
php artisan pubsub:subscriptions:create-push orders-webhook orders \
    https://myapp.com/pubsub/webhook/orders \
    --dead-letter \
    --max-delivery-attempts=5
```
