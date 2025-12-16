# Configuration

Comprehensive configuration guide for Laravel Google Pub/Sub, covering all available options and best practices.

## Configuration File

The main configuration is in `config/pubsub.php`. Publish it with:

```bash
php artisan vendor:publish --provider="OffloadProject\GooglePubSub\PubSubServiceProvider" --tag="config"
```

## Basic Configuration

### Project and Authentication

```php
// config/pubsub.php
'project_id' => env('GOOGLE_CLOUD_PROJECT_ID'),

'auth_method' => env('PUBSUB_AUTH_METHOD', 'application_default'),
'key_file' => env('GOOGLE_APPLICATION_CREDENTIALS'),

'emulator_host' => env('PUBSUB_EMULATOR_HOST'),
```

**Environment Variables:**

```dotenv
GOOGLE_CLOUD_PROJECT_ID=your-project-id
PUBSUB_AUTH_METHOD=key_file
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# For local development
PUBSUB_EMULATOR_HOST=localhost:8085
```

### Queue Integration

```php
'default_queue' => env('PUBSUB_DEFAULT_QUEUE', 'default'),
'use_streaming' => env('PUBSUB_USE_STREAMING', true),
```

## Auto-Creation Settings

Control automatic creation of topics and subscriptions:

```php
'auto_create_topics' => env('PUBSUB_AUTO_CREATE_TOPICS', true),
'auto_create_subscriptions' => env('PUBSUB_AUTO_CREATE_SUBSCRIPTIONS', true),
'auto_acknowledge' => env('PUBSUB_AUTO_ACKNOWLEDGE', true),
'nack_on_error' => env('PUBSUB_NACK_ON_ERROR', true),
```

**Environment Variables:**

```dotenv
PUBSUB_AUTO_CREATE_TOPICS=true
PUBSUB_AUTO_CREATE_SUBSCRIPTIONS=true
PUBSUB_AUTO_ACKNOWLEDGE=true
PUBSUB_NACK_ON_ERROR=true
```

**Best Practices:**

- Set to `false` in production for security
- Use `true` in development for convenience
- Create topics/subscriptions manually in production using Terraform or gcloud CLI

## Subscription Settings

Configure default subscription behavior:

```php
'subscription_suffix' => env('PUBSUB_SUBSCRIPTION_SUFFIX', '-laravel'),
'ack_deadline' => env('PUBSUB_ACK_DEADLINE', 60),
'max_messages' => env('PUBSUB_MAX_MESSAGES', 10),
'wait_time' => env('PUBSUB_WAIT_TIME', 3),
```

**Environment Variables:**

```dotenv
PUBSUB_SUBSCRIPTION_SUFFIX=-laravel
PUBSUB_ACK_DEADLINE=60
PUBSUB_MAX_MESSAGES=10
PUBSUB_WAIT_TIME=3
```

### Subscription Configuration Details

- **subscription_suffix**: Appended to queue names for Laravel subscriptions
- **ack_deadline**: Seconds before message redelivery (10-600 seconds)
- **max_messages**: Messages pulled per request (1-1000)
- **wait_time**: Seconds to wait when no messages available

## Retry Policy

Configure message retry behavior:

```php
'retry_policy' => [
    'minimum_backoff' => env('PUBSUB_MIN_BACKOFF', 10),
    'maximum_backoff' => env('PUBSUB_MAX_BACKOFF', 600),
],
```

**Environment Variables:**

```dotenv
PUBSUB_MIN_BACKOFF=10
PUBSUB_MAX_BACKOFF=600
```

**Backoff Algorithm:**

- Uses exponential backoff with jitter
- Starts at `minimum_backoff` seconds
- Doubles until reaching `maximum_backoff`
- Applies random jitter to prevent thundering herd

## Dead Letter Policy

Configure dead letter topic handling:

```php
'dead_letter_policy' => [
    'enabled' => env('PUBSUB_DEAD_LETTER_ENABLED', true),
    'max_delivery_attempts' => env('PUBSUB_MAX_DELIVERY_ATTEMPTS', 5),
    'dead_letter_topic_suffix' => env('PUBSUB_DEAD_LETTER_SUFFIX', '-dead-letter'),
],
```

**Environment Variables:**

```dotenv
PUBSUB_DEAD_LETTER_ENABLED=true
PUBSUB_MAX_DELIVERY_ATTEMPTS=5
PUBSUB_DEAD_LETTER_SUFFIX=-dead-letter
```

**How it works:**

- Messages failing after `max_delivery_attempts` are moved to dead letter topic
- Dead letter topic name: `{original-topic}{suffix}`
- Example: `orders` → `orders-dead-letter`

## Failed Jobs Configuration

Configure the Pub/Sub topic for storing failed Laravel queue jobs:

```php
'failed_jobs' => [
    'topic' => env('PUBSUB_FAILED_JOBS_TOPIC', 'laravel-failed-jobs'),
],
```

**Environment Variables:**

```dotenv
PUBSUB_FAILED_JOBS_TOPIC=laravel-failed-jobs
```

**Usage Notes:**

The `PubSubFailedJobProvider` publishes failed job data to a Pub/Sub topic for downstream processing or monitoring. This is useful for:

- Alerting systems that monitor the failed jobs topic
- Reprocessing failed jobs through a separate pipeline
- Archiving failed jobs to BigQuery or Cloud Storage

**Important Limitations:**

The Pub/Sub failed job provider only supports **logging** failed jobs. The following operations are **not supported** and will throw `BadMethodCallException`:

- `all()` - Retrieving all failed jobs
- `find($id)` - Finding a specific failed job
- `forget($id)` - Deleting a failed job
- `flush()` - Clearing all failed jobs
- `count()` - Counting failed jobs

This is because Pub/Sub is a fire-and-forget message queue and does not store historical messages. If you need to retrieve, list, or manage failed jobs, use a database-backed failed job provider instead.

**Example Queue Configuration:**

```php
// config/queue.php
'failed' => [
    'driver' => 'pubsub',
],
```

## Message Options

Configure message formatting and compression:

```php
'message_options' => [
    'add_metadata' => env('PUBSUB_ADD_METADATA', true),
    'compress_payload' => env('PUBSUB_COMPRESS_PAYLOAD', true),
    'compression_threshold' => env('PUBSUB_COMPRESSION_THRESHOLD', 1024), // bytes
],
```

**Environment Variables:**

```dotenv
PUBSUB_ADD_METADATA=true
PUBSUB_COMPRESS_PAYLOAD=true
PUBSUB_COMPRESSION_THRESHOLD=1024
```

### Message Metadata

When `add_metadata` is enabled, these attributes are added:

```php
'attributes' => [
    'published_at' => (string) time(),
    'publisher' => 'laravel',
    'hostname' => gethostname(),
    'app_name' => config('app.name'),
]
```

### Message Compression

- Payloads larger than `compression_threshold` bytes are automatically compressed
- Uses gzip compression
- Adds `compressed=true` attribute
- Automatically decompressed on consumption

## Schema Configuration

Define JSON schemas for message validation:

```php
'schemas' => [
    'order_events' => [
        'file' => 'schemas/order-events.json',
    ],
    'user_events' => [
        'schema' => [
            '$schema' => 'http://json-schema.org/draft-07/schema#',
            'type' => 'object',
            'required' => ['user_id', 'event_type'],
            'properties' => [
                'user_id' => ['type' => 'integer'],
                'event_type' => ['type' => 'string'],
                'data' => ['type' => 'object'],
            ],
        ],
    ],
    'payment_events' => [
        'url' => 'https://schemas.example.com/payment-events-v1.json',
    ],
],

'schema_validation' => [
    'enabled' => env('PUBSUB_SCHEMA_VALIDATION', true),
    'strict_mode' => env('PUBSUB_SCHEMA_STRICT', true),
],
```

**Environment Variables:**

```dotenv
PUBSUB_SCHEMA_VALIDATION=true
PUBSUB_SCHEMA_STRICT=true
```

### Schema Sources

1. **File**: Load from local file
2. **Inline**: Define schema directly in config
3. **URL**: Load from remote URL

### Schema Validation Modes

- **strict_mode=true**: Fails if schema not found
- **strict_mode=false**: Skips validation if schema missing

## Message Formatters

Configure message formatting:

```php
'formatters' => [
    'default' => 'json',
    'cloud_events_source' => env('PUBSUB_CLOUDEVENTS_SOURCE', config('app.url')),
],
```

**Available Formatters:**

- `json`: Standard JSON formatting (default)
- `cloud_events`: CloudEvents v1.0 formatting

**Environment Variables:**

```dotenv
PUBSUB_CLOUDEVENTS_SOURCE=https://myapp.com
```

## Topic Configuration

Configure specific topics with custom settings:

```php
'topics' => [
    'orders' => [
        'enable_message_ordering' => true,
        'schema' => 'order_events',
        'formatter' => 'cloud_events',
        'events' => [
            \App\Events\OrderPlaced::class,
            \App\Events\OrderUpdated::class,
        ],
    ],
    'analytics' => [
        'schema' => 'analytics_events',
        'subscription_options' => [
            'ack_deadline' => 30,
            'max_messages' => 100,
        ],
    ],
    'notifications' => [
        'enable_message_ordering' => false,
        'subscribe' => false, // Only publish, don't subscribe
    ],
],
```

### Topic Options

- **enable_message_ordering**: Enable ordered delivery
- **schema**: Schema name for validation
- **formatter**: Message formatter (`json` or `cloud_events`)
- **events**: Laravel events to publish to this topic
- **subscribe**: Whether to create subscription (default: true)
- **subscription_options**: Override default subscription settings

## Event Integration

Configure Laravel event integration:

```php
'events' => [
    'enabled' => env('PUBSUB_EVENTS_ENABLED', false),

    // Events to publish to Pub/Sub
    'publish' => [
        \App\Events\OrderPlaced::class,
        \App\Events\UserRegistered::class,
    ],

    // Or use patterns
    'publish_patterns' => [
        'App\Events\Order*',
        'App\Domain\*\Events\*',
    ],
],
```

**Environment Variables:**

```dotenv
PUBSUB_EVENTS_ENABLED=true
```

### Event Publishing Options

1. **Explicit List**: List specific event classes
2. **Patterns**: Use wildcards to match event classes
3. **Interface**: Implement `ShouldPublishToPubSub` interface
4. **Attribute**: Use `#[PublishTo('topic')]` attribute

## Webhook Configuration

Configure webhook endpoints for push subscriptions:

```php
'webhook' => [
    'enabled' => env('PUBSUB_WEBHOOK_ENABLED', true),
    'route_prefix' => env('PUBSUB_WEBHOOK_PREFIX', 'pubsub/webhook'),
    'auth_token' => env('PUBSUB_WEBHOOK_TOKEN'),
    'skip_verification' => env('PUBSUB_WEBHOOK_SKIP_VERIFICATION', false),

    // IP allowlist (leave empty to allow all)
    'allowed_ips' => [
        // Google's IP ranges for Pub/Sub
        // '35.190.0.0/16',
        // '35.191.0.0/16',
    ],

    // Additional middleware
    'middleware' => [
        \OffloadProject\GooglePubSub\Http\Middleware\VerifyPubSubWebhook::class,
        'throttle:1000,1', // Rate limiting
    ],
],
```

**Environment Variables:**

```dotenv
PUBSUB_WEBHOOK_ENABLED=true
PUBSUB_WEBHOOK_PREFIX=pubsub/webhook
PUBSUB_WEBHOOK_TOKEN=your-secret-token
PUBSUB_WEBHOOK_SKIP_VERIFICATION=false
```

### Webhook Security

- **auth_token**: Bearer token for authentication
- **allowed_ips**: IP allowlist for additional security
- **skip_verification**: Disable verification (development only)
- **middleware**: Additional middleware for rate limiting, etc.

## Monitoring Configuration

Configure logging and monitoring:

```php
'monitoring' => [
    'log_published_messages' => env('PUBSUB_LOG_PUBLISHED', false),
    'log_consumed_messages' => env('PUBSUB_LOG_CONSUMED', false),
    'log_failed_messages' => env('PUBSUB_LOG_FAILED', true),
    'log_webhooks' => env('PUBSUB_LOG_WEBHOOKS', false),
],
```

**Environment Variables:**

```dotenv
PUBSUB_LOG_PUBLISHED=false
PUBSUB_LOG_CONSUMED=false
PUBSUB_LOG_FAILED=true
PUBSUB_LOG_WEBHOOKS=false
```

### Log Levels

- **Published**: Info level, includes topic, message ID, size
- **Consumed**: Info level, includes subscription, message ID
- **Failed**: Error level, includes error details, retry count
- **Webhooks**: Info level, includes endpoint, status

## Queue Driver Configuration

Add to `config/queue.php`:

```php
'connections' => [
    'pubsub' => [
        'driver' => 'pubsub',
        'project_id' => env('GOOGLE_CLOUD_PROJECT_ID'),
        'queue' => env('PUBSUB_DEFAULT_QUEUE', 'default'),
        'auth_method' => env('PUBSUB_AUTH_METHOD', 'application_default'),
        'key_file' => env('GOOGLE_APPLICATION_CREDENTIALS'),
        
        // Override global settings for queue driver
        'auto_create_topics' => true,
        'auto_create_subscriptions' => true,
        'subscription_suffix' => '-laravel',
        'ack_deadline' => 60,
        'max_messages' => 10,
        'enable_message_ordering' => false,
        
        // Message options
        'message_options' => [
            'add_metadata' => true,
            'compress_payload' => true,
            'compression_threshold' => 1024,
        ],
        
        // Dead letter policy
        'dead_letter_policy' => [
            'enabled' => true,
            'max_delivery_attempts' => 5,
            'dead_letter_topic_suffix' => '-dead-letter',
        ],
        
        // Retry policy
        'retry_policy' => [
            'minimum_backoff' => 10,
            'maximum_backoff' => 600,
        ],
        
        // Monitoring
        'monitoring' => [
            'log_published_messages' => false,
            'log_consumed_messages' => false,
            'log_failed_messages' => true,
        ],
    ],
],
```

## Laravel Octane Configuration

For Laravel Octane compatibility, add to `config/octane.php`:

```php
'warm' => [
    'pubsub', // Warm the Pub/Sub manager
],

'flush' => [
    'pubsub.subscribers', // Reset subscriber state between requests
],
```

Automatically handles:

- Connection pooling and reuse
- Memory leak prevention
- State isolation between requests
- Graceful subscriber shutdown

## Environment-Specific Configuration

### Development

```dotenv
# Use emulator for local development
PUBSUB_EMULATOR_HOST=localhost:8085
PUBSUB_AUTO_CREATE_TOPICS=true
PUBSUB_AUTO_CREATE_SUBSCRIPTIONS=true
PUBSUB_LOG_PUBLISHED=true
PUBSUB_LOG_CONSUMED=true
PUBSUB_WEBHOOK_SKIP_VERIFICATION=true
```

### Production

```dotenv
# Use real Google Cloud Pub/Sub
PUBSUB_AUTH_METHOD=application_default
PUBSUB_AUTO_CREATE_TOPICS=false
PUBSUB_AUTO_CREATE_SUBSCRIPTIONS=false
PUBSUB_LOG_PUBLISHED=false
PUBSUB_LOG_CONSUMED=false
PUBSUB_LOG_FAILED=true
PUBSUB_WEBHOOK_SKIP_VERIFICATION=false
```

### Testing

```dotenv
# Use emulator for testing
PUBSUB_EMULATOR_HOST=localhost:8085
PUBSUB_AUTO_CREATE_TOPICS=true
PUBSUB_AUTO_CREATE_SUBSCRIPTIONS=true
PUBSUB_LOG_PUBLISHED=false
PUBSUB_LOG_CONSUMED=false
PUBSUB_LOG_FAILED=false
```

## Performance Tuning

### High Throughput

```php
'max_messages' => 1000,
'ack_deadline' => 30,
'use_streaming' => true,
'message_options' => [
    'compress_payload' => true,
    'compression_threshold' => 512,
],
```

### Low Latency

```php
'max_messages' => 1,
'ack_deadline' => 10,
'wait_time' => 0,
'use_streaming' => true,
```

### Batch Processing

```php
'max_messages' => 100,
'ack_deadline' => 300,
'auto_acknowledge' => false, // Manual acknowledgment
```

## Security Best Practices

### Authentication

1. **Use Service Accounts**: Never use personal accounts
2. **Principle of Least Privilege**: Grant minimal required permissions
3. **Rotate Keys**: Regularly rotate service account keys
4. **Environment Variables**: Never commit credentials to code

### Network Security

1. **Private IPs**: Use private Google access when possible
2. **VPC Peering**: Connect via private networks
3. **Firewall Rules**: Restrict outbound connections

### Message Security

1. **Encrypt Sensitive Data**: Encrypt payloads containing PII
2. **Validate Schemas**: Always validate message structure
3. **Audit Logging**: Enable comprehensive logging
4. **Token Rotation**: Rotate webhook tokens regularly

## Troubleshooting Configuration

### Common Issues

**Invalid Project ID**

```bash
# Verify project exists
gcloud projects describe your-project-id
```

**Authentication Errors**

```bash
# Test credentials
gcloud auth application-default print-access-token
```

**Permission Denied**

```bash
# Check IAM permissions
gcloud projects get-iam-policy your-project-id
```

**Emulator Not Working**

```bash
# Ensure emulator is running
gcloud beta emulators pubsub start
export PUBSUB_EMULATOR_HOST=localhost:8085
```

### Configuration Validation

Create a simple test to validate your configuration:

```php
// tests/Unit/ConfigurationTest.php
public function test_pubsub_configuration()
{
    $this->assertNotEmpty(config('pubsub.project_id'));
    $this->assertContains(config('pubsub.auth_method'), ['application_default', 'key_file']);
    
    if (config('pubsub.auth_method') === 'key_file') {
        $this->assertFileExists(config('pubsub.key_file'));
    }
}
```

## Migration from Other Queue Drivers

### From Redis/Database

```php
// Before (Redis/Database)
ProcessJob::dispatch($data)->onQueue('high-priority');

// After (Pub/Sub) - no changes needed!
ProcessJob::dispatch($data)->onQueue('high-priority');
```

### Configuration Mapping

| Redis/Database   | Pub/Sub                      | Notes                 |
|------------------|------------------------------|-----------------------|
| `redis.database` | `pubsub.project_id`          | Google Cloud project  |
| `redis.host`     | `pubsub.auth_method`         | Authentication method |
| `database.table` | `pubsub.subscription_suffix` | Subscription naming   |
| `retry_after`    | `pubsub.ack_deadline`        | Message timeout       |
| Queue name       | Topic name                   | Direct mapping        |

