# Queue Driver

Full Laravel queue driver implementation for Google Cloud Pub/Sub, allowing use of all Laravel queue features while
leveraging Pub/Sub's scalability and multiservice capabilities.

## Basic Usage

### Dispatching Jobs

Use Laravel's standard job dispatching:

```php
use App\Jobs\ProcessPodcast;

// Dispatch to default queue
ProcessPodcast::dispatch($podcast);

// Dispatch to specific queue (Pub/Sub topic)
ProcessPodcast::dispatch($podcast)->onQueue('audio-processing');

// Delay job execution
ProcessPodcast::dispatch($podcast)->delay(now()->addMinutes(5));

// Chain jobs
ProcessPodcast::dispatch($podcast)->chain([
    new OptimizePodcast($podcast),
    new NotifySubscribers($podcast)
]);
```

### Queue Worker

Run workers as you would with any Laravel queue:

```bash
# Basic worker
php artisan queue:work pubsub

# Specific queue
php artisan queue:work pubsub --queue=audio-processing

# With options
php artisan queue:work pubsub --tries=3 --timeout=90 --memory=512

# Multiple queues with priority
php artisan queue:work pubsub --queue=high,default,low
```

## Multi-Service Architecture

One of Pub/Sub's key advantages is allowing multiple services to subscribe to the same topic:

### Laravel Subscription

Laravel automatically creates subscriptions with a suffix (default: `-laravel`):

```php
// Topic: orders
// Laravel subscription: orders-laravel
ProcessOrder::dispatch($order)->onQueue('orders');
```

### Other Service Subscriptions

Your other services can create their own subscriptions:

```go
// Go service
subscription := client.Subscription("orders-go-service")
if exists, _ := subscription.Exists(ctx); !exists {
    subscription, _ = client.CreateSubscription(ctx, "orders-go-service", pubsub.SubscriptionConfig{
        Topic: client.Topic("orders"),
    })
}
```

```python
# Python service
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, "orders-python-service")
topic_path = subscriber.topic_path(project_id, "orders")

try:
    subscription = subscriber.create_subscription(
        request={"name": subscription_path, "topic": topic_path}
    )
except AlreadyExists:
    pass
```

## Advanced Features

### Message Ordering

Enable message ordering for strict processing order:

```php
// In config/queue.php
'pubsub' => [
    'enable_message_ordering' => true,
],

// In your job
class ProcessOrder implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable;

    public function __construct(
        public Order $order
    ) {}

    public function middleware()
    {
        return [
            function ($job, $next) {
                // Set ordering key
                if ($job->job instanceof \OffloadProject\GooglePubSub\Queue\Jobs\PubSubJob) {
                    $job->pubsubOptions = [
                        'ordering_key' => 'customer-' . $this->order->customer_id
                    ];
                }
                $next($job);
            }
        ];
    }
}
```

### Custom Attributes

Add metadata to messages for cross-service communication:

```php
ProcessOrder::dispatch($order)->through(function ($job) {
    $job->pubsubOptions = [
        'attributes' => [
            'source' => 'web-api',
            'version' => '2.0',
            'priority' => 'high',
            'region' => 'us-east1'
        ]
    ];
});
```

### Accessing Pub/Sub Features in Jobs

```php
class ProcessOrder implements ShouldQueue
{
    use InteractsWithQueue;

    public function handle()
    {
        // Check if this is a Pub/Sub job
        if ($this->job instanceof \OffloadProject\GooglePubSub\Queue\Jobs\PubSubJob) {
            // Get message attributes
            $attributes = $this->job->getMessageAttributes();
            $priority = $attributes['priority'] ?? 'normal';
            
            // Get publish time
            $publishTime = $this->job->getPublishTime();
            
            // Get ordering key
            $orderingKey = $this->job->getOrderingKey();
            
            // Access the raw Pub/Sub message
            $message = $this->job->getPubSubMessage();
        }
        
        // Your job logic here
    }
}
```

## Failed Jobs

Failed jobs are handled through both Laravel's failed jobs system and Pub/Sub's dead letter topics:

### Laravel Failed Jobs

```bash
# View failed jobs
php artisan queue:failed

# Retry a failed job
php artisan queue:retry 5

# Retry all failed jobs
php artisan queue:retry all
```

### Dead Letter Topics

Configure dead letter handling in `config/pubsub.php`:

```php
'dead_letter_policy' => [
    'enabled' => true,
    'max_delivery_attempts' => 5,
    'dead_letter_topic_suffix' => '-dead-letter',
],
```

Failed messages are automatically moved to dead letter topics after max attempts:

- Original topic: `orders`
- Dead letter topic: `orders-dead-letter`

## Configuration Options

### Queue-Level Configuration

```php
// config/queue.php
'pubsub' => [
    'driver' => 'pubsub',
    'project_id' => env('GOOGLE_CLOUD_PROJECT_ID'),
    'queue' => 'default',
    
    // Subscription settings
    'subscription_suffix' => '-laravel',
    'ack_deadline' => 60,
    'max_messages' => 10,
    
    // Retry policy
    'retry_policy' => [
        'minimum_backoff' => 10,
        'maximum_backoff' => 600,
    ],
    
    // Message options
    'message_options' => [
        'add_metadata' => true,
        'compress_payload' => true,
        'compression_threshold' => 1024,
    ],
],
```

### Performance Tuning

```dotenv
# Worker settings
PUBSUB_MAX_MESSAGES=50          # Messages per pull
PUBSUB_ACK_DEADLINE=60          # Seconds before retry
PUBSUB_WAIT_TIME=3              # Wait time when queue is empty

# Compression
PUBSUB_COMPRESS_PAYLOAD=true
PUBSUB_COMPRESSION_THRESHOLD=1024
```

## Best Practices

1. **Use Specific Queues**: Create dedicated topics for different job types
2. **Set Appropriate Timeouts**: Configure `ack_deadline` based on job processing time
3. **Monitor Dead Letters**: Regularly check dead letter topics for failed messages
4. **Use Ordering Keys**: For jobs that must be processed in order
5. **Add Metadata**: Include attributes for debugging and routing
