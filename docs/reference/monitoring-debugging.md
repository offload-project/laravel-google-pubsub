# Monitoring & Debugging

Comprehensive monitoring and debugging tools to ensure your Pub/Sub integration runs smoothly in production.

## Built-in Logging

### Configuration

Control logging verbosity in `config/pubsub.php`:

```php
'monitoring' => [
    'log_published_messages' => env('PUBSUB_LOG_PUBLISHED', false),
    'log_consumed_messages' => env('PUBSUB_LOG_CONSUMED', false),
    'log_failed_messages' => env('PUBSUB_LOG_FAILED', true),
    'log_webhooks' => env('PUBSUB_LOG_WEBHOOKS', false),
],
```

### Log Examples

Published messages:

```
[2024-01-15 10:30:45] production.INFO: Published message to Pub/Sub {
    "topic": "orders",
    "message_id": "1234567890",
    "attributes": {"priority": "high"},
    "size": 1024
}
```

Consumed messages:

```
[2024-01-15 10:30:46] production.INFO: Processing Pub/Sub message {
    "subscription": "orders-laravel",
    "message_id": "1234567890",
    "publish_time": "2024-01-15T10:30:45Z",
    "attributes": {"priority": "high"}
}
```

Failed messages:

```
[2024-01-15 10:30:47] production.ERROR: Failed to process Pub/Sub message {
    "error": "Order processing failed: Invalid customer ID",
    "message_id": "1234567890",
    "attempts": 3,
    "subscription": "orders-laravel"
}
```

Webhook requests:

```
[2024-01-15 10:30:48] production.INFO: Received Pub/Sub webhook {
    "topic": "orders",
    "message_id": "1234567890",
    "subscription": "projects/my-project/subscriptions/orders-webhook",
    "endpoint": "/pubsub/webhook/orders"
}
```

## Custom Logging

### Log Channel Configuration

Create a dedicated log channel for Pub/Sub in `config/logging.php`:

```php
'channels' => [
    'pubsub' => [
        'driver' => 'daily',
        'path' => storage_path('logs/pubsub.log'),
        'level' => env('LOG_LEVEL', 'debug'),
        'days' => 14,
    ],
],
```

### Custom Log Implementation

```php
use Illuminate\Support\Facades\Log;

// In your event listener
class ProcessOrderEvents
{
    public function handle($event)
    {
        $startTime = microtime(true);
        
        try {
            // Process the event
            $this->processOrder($event['data']);
            
            // Log success with metrics
            Log::channel('pubsub')->info('Order processed successfully', [
                'order_id' => $event['data']['order_id'],
                'processing_time' => microtime(true) - $startTime,
                'message_id' => $event['message']->id(),
                'attributes' => $event['message']->attributes(),
            ]);
        } catch (\Exception $e) {
            Log::channel('pubsub')->error('Order processing failed', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
                'order_id' => $event['data']['order_id'] ?? null,
                'message_id' => $event['message']->id(),
            ]);
            
            throw $e;
        }
    }
}
```

## Performance Monitoring

### Message Processing Metrics

Track key performance indicators:

```php
use Illuminate\Support\Facades\Cache;

class MetricsCollector
{
    public function recordPublished(string $topic, string $messageId, int $size): void
    {
        // Increment counters
        Cache::increment("pubsub:metrics:published:{$topic}:count");
        Cache::increment("pubsub:metrics:published:{$topic}:bytes", $size);
        
        // Track rate
        $key = "pubsub:metrics:published:{$topic}:rate:" . date('Y-m-d-H');
        Cache::increment($key);
        Cache::expire($key, 86400); // 24 hours
    }
    
    public function recordProcessed(string $subscription, float $duration): void
    {
        // Track processing time
        $key = "pubsub:metrics:processing:{$subscription}";
        
        $stats = Cache::get($key, [
            'count' => 0,
            'total_time' => 0,
            'min_time' => PHP_FLOAT_MAX,
            'max_time' => 0,
        ]);
        
        $stats['count']++;
        $stats['total_time'] += $duration;
        $stats['min_time'] = min($stats['min_time'], $duration);
        $stats['max_time'] = max($stats['max_time'], $duration);
        
        Cache::put($key, $stats, now()->endOfDay());
    }
    
    public function getMetrics(string $topic = null): array
    {
        $pattern = $topic 
            ? "pubsub:metrics:*:{$topic}:*" 
            : "pubsub:metrics:*";
            
        $keys = Cache::getRedis()->keys($pattern);
        $metrics = [];
        
        foreach ($keys as $key) {
            $metrics[$key] = Cache::get($key);
        }
        
        return $metrics;
    }
}
```

### Integration with Monitoring Services

#### Laravel Telescope

```php
// In your subscriber
use Laravel\Telescope\Telescope;

$subscriber->handler(function ($data, $message) {
    Telescope::recordQuery(
        'pubsub',
        "Processing message from subscription",
        [
            'subscription' => $this->subscriptionName,
            'message_id' => $message->id(),
            'attributes' => $message->attributes(),
        ],
        microtime(true)
    );
    
    // Process message
});
```

#### Custom Metrics Export

```php
// app/Console/Commands/ExportPubSubMetrics.php
namespace App\Console\Commands;

use Illuminate\Console\Command;

class ExportPubSubMetrics extends Command
{
    protected $signature = 'pubsub:metrics:export {--format=json}';
    
    public function handle(MetricsCollector $collector): void
    {
        $metrics = $collector->getMetrics();
        
        $processed = [
            'timestamp' => now()->toIso8601String(),
            'topics' => [],
            'subscriptions' => [],
        ];
        
        foreach ($metrics as $key => $value) {
            if (str_contains($key, 'published')) {
                // Parse topic metrics
                preg_match('/published:(.+):(.+)/', $key, $matches);
                $topic = $matches[1] ?? 'unknown';
                $metric = $matches[2] ?? 'unknown';
                
                $processed['topics'][$topic][$metric] = $value;
            } elseif (str_contains($key, 'processing')) {
                // Parse subscription metrics
                preg_match('/processing:(.+)/', $key, $matches);
                $subscription = $matches[1] ?? 'unknown';
                
                $processed['subscriptions'][$subscription] = $value;
                
                // Calculate average
                if (isset($value['count']) && $value['count'] > 0) {
                    $processed['subscriptions'][$subscription]['avg_time'] = 
                        $value['total_time'] / $value['count'];
                }
            }
        }
        
        if ($this->option('format') === 'json') {
            $this->line(json_encode($processed, JSON_PRETTY_PRINT));
        } else {
            $this->table(['Metric', 'Value'], $this->formatTable($processed));
        }
    }
}
```

## Health Checks

### Basic Health Check

```php
// app/Http/Controllers/HealthController.php
namespace App\Http\Controllers;

use OffloadProject\GooglePubSub\Facades\PubSub;
use Illuminate\Support\Facades\Cache;

class HealthController extends Controller
{
    public function pubsub()
    {
        $health = [
            'status' => 'healthy',
            'timestamp' => now()->toIso8601String(),
            'checks' => [],
        ];
        
        // Check connection
        try {
            $topics = PubSub::topics();
            $health['checks']['connection'] = [
                'status' => 'ok',
                'topics_count' => count($topics),
            ];
        } catch (\Exception $e) {
            $health['status'] = 'unhealthy';
            $health['checks']['connection'] = [
                'status' => 'failed',
                'error' => $e->getMessage(),
            ];
        }
        
        // Check subscriptions
        try {
            $subscriptions = PubSub::subscriptions();
            $health['checks']['subscriptions'] = [
                'status' => 'ok',
                'count' => count($subscriptions),
            ];
        } catch (\Exception $e) {
            $health['status'] = 'unhealthy';
            $health['checks']['subscriptions'] = [
                'status' => 'failed',
                'error' => $e->getMessage(),
            ];
        }
        
        // Check processing metrics
        $metrics = Cache::get('pubsub:metrics:health', []);
        $health['checks']['processing'] = [
            'status' => $metrics['failed_recently'] ?? false ? 'degraded' : 'ok',
            'last_success' => $metrics['last_success'] ?? null,
            'last_failure' => $metrics['last_failure'] ?? null,
            'failure_rate' => $metrics['failure_rate'] ?? 0,
        ];
        
        return response()->json($health, $health['status'] === 'healthy' ? 200 : 503);
    }
}
```

### Subscription Health Monitor

```php
// app/Console/Commands/MonitorSubscriptions.php
namespace App\Console\Commands;

use Illuminate\Console\Command;
use OffloadProject\GooglePubSub\Facades\PubSub;

class MonitorSubscriptions extends Command
{
    protected $signature = 'pubsub:monitor:subscriptions {--alert}';
    
    public function handle(): void
    {
        $subscriptions = PubSub::subscriptions();
        $alerts = [];
        
        foreach ($subscriptions as $subscription) {
            $info = $subscription->info();
            $name = $subscription->name();
            
            // Check for issues
            $issues = [];
            
            // Check acknowledgment deadline
            if ($info['ackDeadlineSeconds'] < 10) {
                $issues[] = 'Ack deadline too short: ' . $info['ackDeadlineSeconds'] . 's';
            }
            
            // Check for dead letter configuration
            if (!isset($info['deadLetterPolicy'])) {
                $issues[] = 'No dead letter policy configured';
            }
            
            // Check message retention
            if (($info['messageRetentionDuration'] ?? 0) < 600) {
                $issues[] = 'Message retention less than 10 minutes';
            }
            
            if (!empty($issues)) {
                $alerts[$name] = $issues;
            }
        }
        
        if (empty($alerts)) {
            $this->info('All subscriptions are healthy!');
            return;
        }
        
        $this->warn('Found issues with subscriptions:');
        foreach ($alerts as $subscription => $issues) {
            $this->line("\n{$subscription}:");
            foreach ($issues as $issue) {
                $this->line("  - {$issue}");
            }
        }
        
        if ($this->option('alert')) {
            // Send alerts (email, Slack, etc.)
            event(new SubscriptionHealthAlert($alerts));
        }
    }
}
```

## Debugging Tools

### Message Inspector

```php
// app/Console/Commands/InspectMessage.php
namespace App\Console\Commands;

use Illuminate\Console\Command;
use OffloadProject\GooglePubSub\Facades\PubSub;

class InspectMessage extends Command
{
    protected $signature = 'pubsub:inspect {subscription} {--limit=1}';
    
    public function handle(): void
    {
        $subscription = $this->argument('subscription');
        $limit = (int) $this->option('limit');
        
        $this->info("Inspecting messages from subscription: {$subscription}");
        
        $subscriber = PubSub::subscribe($subscription);
        
        $subscriber->handler(function ($data, $message) {
            $this->line("\n" . str_repeat('=', 50));
            $this->info("Message ID: " . $message->id());
            $this->line("Publish Time: " . ($message->publishTime() ?? 'N/A'));
            
            // Display attributes
            $attributes = $message->attributes();
            if (!empty($attributes)) {
                $this->line("\nAttributes:");
                foreach ($attributes as $key => $value) {
                    $this->line("  {$key}: {$value}");
                }
            }
            
            // Display data
            $this->line("\nData:");
            $this->line(json_encode($data, JSON_PRETTY_PRINT));
            
            // Show ordering key if present
            if ($message->orderingKey()) {
                $this->line("\nOrdering Key: " . $message->orderingKey());
            }
            
            $this->line(str_repeat('=', 50));
        });
        
        $messages = $subscriber->pull($limit);
        
        $this->info("\nInspected {$limit} message(s)");
    }
}
```

### Dead Letter Inspector

```php
// app/Console/Commands/InspectDeadLetters.php
namespace App\Console\Commands;

use Illuminate\Console\Command;
use OffloadProject\GooglePubSub\Facades\PubSub;

class InspectDeadLetters extends Command
{
    protected $signature = 'pubsub:dead-letters {topic} {--process}';
    
    public function handle(): void
    {
        $topic = $this->argument('topic');
        $deadLetterTopic = $topic . '-dead-letter';
        $deadLetterSubscription = $deadLetterTopic . '-inspector';
        
        $this->info("Inspecting dead letters for topic: {$topic}");
        
        // Create temporary subscription for inspection
        PubSub::createSubscription($deadLetterSubscription, $deadLetterTopic);
        
        $subscriber = PubSub::subscribe($deadLetterSubscription);
        
        $deadLetters = [];
        $subscriber->handler(function ($data, $message) use (&$deadLetters) {
            $deadLetters[] = [
                'id' => $message->id(),
                'data' => $data,
                'attributes' => $message->attributes(),
                'publish_time' => $message->publishTime(),
            ];
        });
        
        // Pull all available messages
        $subscriber->pull(100);
        
        if (empty($deadLetters)) {
            $this->info('No dead letter messages found!');
            return;
        }
        
        $this->warn("Found {count($deadLetters)} dead letter message(s):");
        
        foreach ($deadLetters as $index => $letter) {
            $this->line("\n[{$index}] Message ID: {$letter['id']}");
            $this->line("Published: {$letter['publish_time']}");
            $this->line("Data: " . json_encode($letter['data']));
        }
        
        if ($this->option('process')) {
            $this->line('');
            if ($this->confirm('Do you want to reprocess these messages?')) {
                $this->reprocessDeadLetters($topic, $deadLetters);
            }
        }
        
        // Cleanup temporary subscription
        PubSub::client()->subscription($deadLetterSubscription)->delete();
    }
    
    protected function reprocessDeadLetters(string $topic, array $deadLetters): void
    {
        $succeeded = 0;
        $failed = 0;
        
        foreach ($deadLetters as $letter) {
            try {
                PubSub::publish($topic, $letter['data'], $letter['attributes']);
                $succeeded++;
                $this->info("✓ Reprocessed message: {$letter['id']}");
            } catch (\Exception $e) {
                $failed++;
                $this->error("✗ Failed to reprocess: {$letter['id']} - {$e->getMessage()}");
            }
        }
        
        $this->line("\nReprocessing complete: {$succeeded} succeeded, {$failed} failed");
    }
}
```

## Troubleshooting

### Common Issues

#### 1. Messages Not Being Received

```php
// Debug subscription configuration
$subscription = PubSub::client()->subscription('orders-laravel');
$info = $subscription->info();

dump([
    'exists' => $subscription->exists(),
    'topic' => $info['topic'] ?? 'N/A',
    'ackDeadlineSeconds' => $info['ackDeadlineSeconds'] ?? 'N/A',
    'messageRetentionDuration' => $info['messageRetentionDuration'] ?? 'N/A',
]);
```

#### 2. High Message Redelivery Rate

Monitor and adjust acknowledgment deadlines:

```php
use OffloadProject\GooglePubSub\Facades\PubSub;

// Monitor processing time
$subscriber = PubSub::subscribe('orders-laravel');

$subscriber->handler(function ($data, $message) use ($subscriber) {
    $startTime = microtime(true);
    
    try {
        // Process message
        $this->processOrder($data);
        
        $duration = microtime(true) - $startTime;
        
        // Log if processing time approaches ack deadline
        if ($duration > 45) { // 75% of 60s deadline
            Log::warning('Slow message processing', [
                'duration' => $duration,
                'message_id' => $message->id(),
            ]);
        }
    } catch (\Exception $e) {
        // Extend deadline if more time needed
        $subscriber->modifyAckDeadline($message, 120);
        
        // Reprocess
        throw $e;
    }
});
```

#### 3. Memory Issues with Large Messages

```php
// Monitor memory usage
$subscriber->handler(function ($data, $message) {
    $memoryBefore = memory_get_usage();
    
    // Process message
    $this->processLargeMessage($data);
    
    $memoryAfter = memory_get_usage();
    $memoryUsed = ($memoryAfter - $memoryBefore) / 1024 / 1024; // MB
    
    if ($memoryUsed > 50) {
        Log::warning('High memory usage for message', [
            'message_id' => $message->id(),
            'memory_used_mb' => $memoryUsed,
        ]);
    }
    
    // Force garbage collection for large messages
    if ($memoryUsed > 100) {
        gc_collect_cycles();
    }
});
```

### Debug Mode

Enable comprehensive debugging:

```php
// config/pubsub.php
'debug' => env('PUBSUB_DEBUG', false),

// In your service provider
if (config('pubsub.debug')) {
    Event::listen('pubsub.*', function ($eventName, $data) {
        Log::channel('pubsub-debug')->debug($eventName, $data);
    });
}
```

### Message Flow Tracing

```php
// Trace message through the system
class MessageTracer
{
    public static function startTrace(string $messageId): void
    {
        Cache::put("trace:{$messageId}", [
            'started_at' => now(),
            'steps' => [],
        ], 3600);
    }
    
    public static function addStep(string $messageId, string $step, array $data = []): void
    {
        $trace = Cache::get("trace:{$messageId}", [
            'started_at' => now(),
            'steps' => [],
        ]);
        
        $trace['steps'][] = [
            'step' => $step,
            'timestamp' => now()->toIso8601String(),
            'data' => $data,
        ];
        
        Cache::put("trace:{$messageId}", $trace, 3600);
    }
    
    public static function getTrace(string $messageId): ?array
    {
        return Cache::get("trace:{$messageId}");
    }
}

// Usage in your handlers
$subscriber->handler(function ($data, $message) {
    $messageId = $message->id();
    
    MessageTracer::startTrace($messageId);
    MessageTracer::addStep($messageId, 'received', [
        'subscription' => 'orders-laravel',
    ]);
    
    // Process steps
    MessageTracer::addStep($messageId, 'validation', [
        'status' => 'passed',
    ]);
    
    // ... more processing ...
    
    MessageTracer::addStep($messageId, 'completed', [
        'duration' => microtime(true) - LARAVEL_START,
    ]);
});
```

## Production Best Practices

### 1. Alerting

Set up alerts for critical metrics:

```php
// app/Monitors/PubSubMonitor.php
namespace App\Monitors;

class PubSubMonitor
{
    public function checkHealth(): array
    {
        $alerts = [];
        
        // Check failed message rate
        $failureRate = Cache::get('pubsub:metrics:failure_rate', 0);
        if ($failureRate > 0.05) { // 5% failure rate
            $alerts[] = [
                'level' => 'critical',
                'message' => "High failure rate: {$failureRate}%",
            ];
        }
        
        // Check processing lag
        $lag = $this->getProcessingLag();
        if ($lag > 300) { // 5 minutes
            $alerts[] = [
                'level' => 'warning',
                'message' => "Processing lag: {$lag} seconds",
            ];
        }
        
        // Check dead letter queue size
        $deadLetterCount = $this->getDeadLetterCount();
        if ($deadLetterCount > 100) {
            $alerts[] = [
                'level' => 'critical',
                'message' => "Dead letter queue size: {$deadLetterCount}",
            ];
        }
        
        return $alerts;
    }
}
```

### 2. Performance Optimization

```php
// Batch processing for better performance
$subscriber = PubSub::subscribe('high-volume-subscription');

$batch = [];
$subscriber->handler(function ($data, $message) use (&$batch) {
    $batch[] = ['data' => $data, 'message' => $message];
    
    // Process in batches of 100
    if (count($batch) >= 100) {
        $this->processBatch($batch);
        
        // Acknowledge all at once
        $messages = array_column($batch, 'message');
        $this->subscriber->acknowledgeBatch($messages);
        
        $batch = [];
    }
});
```

### 3. Circuit Breaker Pattern

```php
use Illuminate\Support\Facades\Cache;

class CircuitBreaker
{
    protected string $service;
    protected int $threshold;
    protected int $timeout;
    
    public function __construct(string $service, int $threshold = 5, int $timeout = 60)
    {
        $this->service = $service;
        $this->threshold = $threshold;
        $this->timeout = $timeout;
    }
    
    public function call(callable $callback)
    {
        $state = Cache::get("circuit:{$this->service}:state", 'closed');
        
        if ($state === 'open') {
            $openedAt = Cache::get("circuit:{$this->service}:opened_at", 0);
            
            if (time() - $openedAt < $this->timeout) {
                throw new \Exception("Circuit breaker is open for {$this->service}");
            }
            
            // Try half-open
            $state = 'half-open';
        }
        
        try {
            $result = $callback();
            
            if ($state === 'half-open') {
                $this->close();
            }
            
            return $result;
        } catch (\Exception $e) {
            $this->recordFailure();
            throw $e;
        }
    }
    
    protected function recordFailure(): void
    {
        $failures = Cache::increment("circuit:{$this->service}:failures");
        
        if ($failures >= $this->threshold) {
            $this->open();
        }
    }
    
    protected function open(): void
    {
        Cache::put("circuit:{$this->service}:state", 'open', $this->timeout);
        Cache::put("circuit:{$this->service}:opened_at", time(), $this->timeout);
        
        Log::warning("Circuit breaker opened for {$this->service}");
    }
    
    protected function close(): void
    {
        Cache::forget("circuit:{$this->service}:state");
        Cache::forget("circuit:{$this->service}:failures");
        
        Log::info("Circuit breaker closed for {$this->service}");
    }
}
```

### 4. Monitoring Dashboard

Create a simple monitoring dashboard:

```php
// routes/web.php
Route::get('/admin/pubsub/dashboard', function () {
    $metrics = [
        'topics' => PubSub::topics(),
        'subscriptions' => PubSub::subscriptions(),
        'published_today' => Cache::get('pubsub:metrics:published:count:' . date('Y-m-d'), 0),
        'processed_today' => Cache::get('pubsub:metrics:processed:count:' . date('Y-m-d'), 0),
        'failure_rate' => Cache::get('pubsub:metrics:failure_rate', 0),
        'avg_processing_time' => Cache::get('pubsub:metrics:avg_processing_time', 0),
        'dead_letters' => Cache::get('pubsub:metrics:dead_letters:count', 0),
    ];
    
    return view('admin.pubsub.dashboard', compact('metrics'));
});
```

## Integration with APM Tools

### New Relic Integration

```php
// In your subscriber
$subscriber->handler(function ($data, $message) {
    if (extension_loaded('newrelic')) {
        newrelic_start_transaction('pubsub-message');
        newrelic_add_custom_parameter('message_id', $message->id());
        newrelic_add_custom_parameter('topic', $this->topic);
        
        try {
            $this->process($data);
            newrelic_end_transaction();
        } catch (\Exception $e) {
            newrelic_notice_error($e);
            throw $e;
        }
    }
});
```

### Datadog Integration

```php
use DataDog\DogStatsd;

$statsd = new DogStatsd();

$subscriber->handler(function ($data, $message) use ($statsd) {
    $start = microtime(true);
    
    try {
        $this->process($data);
        
        $duration = (microtime(true) - $start) * 1000;
        $statsd->timing('pubsub.processing.duration', $duration, [
            'topic' => $this->topic,
            'subscription' => $this->subscription,
        ]);
        
        $statsd->increment('pubsub.messages.processed', 1, [
            'topic' => $this->topic,
            'status' => 'success',
        ]);
    } catch (\Exception $e) {
        $statsd->increment('pubsub.messages.processed', 1, [
            'topic' => $this->topic,
            'status' => 'failure',
        ]);
        throw $e;
    }
});
```

### Tips

- Start with basic logging and expand as needed
- Set up alerts for critical metrics
- Regularly review dead letter queues
- Monitor processing times and adjust deadlines accordingly
- Use circuit breakers for external dependencies
- Keep historical metrics for trend analysis
