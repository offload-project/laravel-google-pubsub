<p align="center">
    <a href="https://packagist.org/packages/offload-project/laravel-google-pubsub"><img src="https://img.shields.io/packagist/v/offload-project/laravel-google-pubsub.svg?style=flat-square" alt="Latest Version on Packagist"></a>
    <a href="https://github.com/offload-project/laravel-google-pubsub/actions"><img src="https://img.shields.io/github/actions/workflow/status/offload-project/laravel-google-pubsub/tests.yml?branch=main&style=flat-square" alt="GitHub Tests Action Status"></a>
    <a href="https://packagist.org/packages/offload-project/laravel-google-pubsub"><img src="https://img.shields.io/packagist/dt/offload-project/laravel-google-pubsub.svg?style=flat-square" alt="Total Downloads"></a>
</p>

# Laravel Google Pub/Sub

A comprehensive Google Cloud Pub/Sub integration for Laravel that goes beyond basic queue functionality. This package
provides a complete toolkit for building event-driven architectures, microservice communication, and real-time data
pipelines.

## Features

- **Full Laravel Queue Driver** - Seamless integration with Laravel's queue system
- **Publisher/Subscriber Services** - Direct publishing with compression, metadata, and batch support
- **Event Integration** - Bidirectional event flow between Laravel and Pub/Sub
- **Webhook Support** - Handle push subscriptions with built-in security
- **Schema Validation** - JSON Schema validation for message contracts
- **Streaming Support** - Real-time message processing with StreamingPull
- **Multi-Service Architecture** - Built for microservice communication
- **CloudEvents Support** - Industry-standard event formatting with v1.0 compatibility
- **Enterprise Ready** - Dead letter topics, retry policies, monitoring
- **Emulator Support** - Local development with Google Cloud Pub/Sub emulator
- **Laravel Octane Compatible** - Optimized for high-performance applications
- **Comprehensive CLI** - Rich set of Artisan commands for management

## Table of Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [Basic Configuration](#basic-configuration)
- [Quick Start](#quick-start)
    - [Basic Queue Usage](#1-basic-queue-usage)
    - [Direct Publishing](#2-direct-publishing)
    - [Event Integration](#3-event-integration)
    - [Subscribing to Messages](#4-subscribing-to-messages)
- [Full Documentation](#full-documentation)
- [Performance Tips](#performance-tips)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## Requirements

* PHP 8.4+
* Laravel 11.0+
* Google Cloud Pub/Sub PHP library

## Installation

Install the package via Composer:

```bash
composer require offload-project/laravel-google-pubsub
```

Publish the configuration file:

```bash
php artisan vendor:publish --provider="OffloadProject\GooglePubSub\PubSubServiceProvider" --tag="config"
```

## Basic Configuration

### Environment Variables

Add the following to your `.env` file:

```dotenv
# Basic Configuration
QUEUE_CONNECTION=pubsub
GOOGLE_CLOUD_PROJECT_ID=your-project-id

# Authentication (choose one method)
# Method 1: Service Account Key File
PUBSUB_AUTH_METHOD=key_file
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# Method 2: Application Default Credentials
PUBSUB_AUTH_METHOD=application_default

# Optional Settings
PUBSUB_DEFAULT_QUEUE=default
PUBSUB_AUTO_CREATE_TOPICS=true
PUBSUB_AUTO_CREATE_SUBSCRIPTIONS=true
```

### Queue Configuration

Update your `config/queue.php`:

```php
'connections' => [
    'pubsub' => [
        'driver' => 'pubsub',
        'project_id' => env('GOOGLE_CLOUD_PROJECT_ID'),
        'queue' => env('PUBSUB_DEFAULT_QUEUE', 'default'),
        'auth_method' => env('PUBSUB_AUTH_METHOD', 'application_default'),
        'key_file' => env('GOOGLE_APPLICATION_CREDENTIALS'),
        
        // Optional overrides
        'auto_create_topics' => true,
        'auto_create_subscriptions' => true,
        'subscription_suffix' => '-laravel',
        'enable_message_ordering' => false,
    ],
],
```

## Quick Start

### 1. Basic Queue Usage

Use it exactly like any other Laravel queue:

```php
// Dispatch jobs as normal
ProcessPodcast::dispatch($podcast);

// Dispatch to specific queue (Pub/Sub topic)
ProcessPodcast::dispatch($podcast)->onQueue('audio-processing');

// Your Go microservices can subscribe to the same topic
// Subscription name: audio-processing-go-service
```

### 2. Direct Publishing

```php
use OffloadProject\GooglePubSub\Facades\PubSub;

// Publish directly to a topic
PubSub::publish('orders', [
    'order_id' => 123,
    'total' => 99.99,
    'customer_id' => 456
]);

// With attributes and ordering
PubSub::publish('orders', $data, [
    'priority' => 'high',
    'source' => 'api'
], [
    'ordering_key' => 'customer-456'
]);
```

### 3. Event Integration

```php
use OffloadProject\GooglePubSub\Attributes\PublishTo;
use OffloadProject\GooglePubSub\Contracts\ShouldPublishToPubSub;

#[PublishTo('orders')]
class OrderPlaced implements ShouldPublishToPubSub
{
    public function __construct(
        public Order $order
    ) {}
    
    public function pubsubTopic(): string
    {
        return 'orders';
    }
    
    public function toPubSub(): array
    {
        return [
            'order_id' => $this->order->id,
            'total' => $this->order->total,
            'customer_id' => $this->order->customer_id,
        ];
    }
}

// This event automatically publishes to the 'orders' topic
event(new OrderPlaced($order));
```

### 4. Subscribing to Messages

```php
use OffloadProject\GooglePubSub\Facades\PubSub;

// Create a subscriber
$subscriber = PubSub::subscribe('orders-processor', 'orders');

// Add message handler
$subscriber->handler(function ($data, $message) {
    // Process the order
    processOrder($data);
});

// Start listening
$subscriber->listen();
```

## Full Documentation

- [Installation](docs/implementation/installation.md)
- [Configuration](docs/implementation/configuration.md) (comprehensive)
- [Queue Driver](docs/queue-driver.md)
- [Publisher & Subscriber](docs/direct-pubsub.md)
- [Event Integration](docs/event-integration.md)
- [Webhooks (Push Subscriptions)](docs/webhook-push.md)
- [Message Schemas and Validation](docs/messages/message-schemas.md)
- [CloudEvents](docs/messages/cloudevents.md)
- [Artisan Command](docs/artisan-commands.md)
- [Monitoring & Debugging](docs/reference/monitoring-debugging.md)
- [Testing](docs/reference/testing.md)
- [Examples](docs/reference/examples.md)

## Performance Tips

### 1. Use Streaming Subscribers for Real-time Processing

Streaming subscribers provide lower latency and better throughput:

```php
// config/pubsub.php
'use_streaming' => true,

// Real-time processing
$subscriber = PubSub::subscribe('high-volume-subscription');
$subscriber->stream(['max_messages_per_pull' => 1000]);
```

### 2. Enable Message Ordering Only When Necessary

Ordering reduces throughput, use it selectively:

```php
// Only for specific topics that require order
'topics' => [
    'financial-transactions' => [
        'enable_message_ordering' => true,
    ],
    'analytics' => [
        'enable_message_ordering' => false, // Better performance
    ],
],
```

### 3. Set Appropriate Timeouts for Job Processing

Match acknowledgment deadlines to your processing time:

```php
// Quick jobs (< 30 seconds)
'subscriptions' => [
    'quick-jobs' => ['ack_deadline' => 30],
],

// Slow jobs (up to 10 minutes)  
'subscriptions' => [
    'data-processing' => ['ack_deadline' => 600],
],
```

### 4. Monitor Dead Letter Topics for Failed Messages

Set up automated monitoring:

```bash
# Check dead letter queue size
php artisan pubsub:dead-letters orders --summary

# Reprocess dead letters
php artisan pubsub:dead-letters orders --process
```

### 5. Use Compression for Large Payloads

Automatic compression for messages over 1KB:

```php
'message_options' => [
    'compress_payload' => true,
    'compression_threshold' => 1024, // bytes
],
```

### 6. Batch Publishing for High Volume

Reduce API calls with batch publishing:

```php
$messages = collect($items)->map(fn($item) => [
    'data' => $item->toArray(),
    'attributes' => ['type' => 'bulk-import'],
]);

PubSub::publishBatch('imports', $messages->toArray());
```

### 7. Connection Pooling with Octane

Laravel Octane automatically reuses connections:

```php
// config/octane.php
'warm' => [
    'pubsub', // Warm the Pub/Sub manager
],
```

## Troubleshooting

### Connection Errors

#### Verify your Google Cloud project ID

```bash
# Check current project
gcloud config get-value project

# Verify in your .env
grep GOOGLE_CLOUD_PROJECT_ID .env
```

#### Check service account permissions

```bash
# List current service account permissions
gcloud projects get-iam-policy your-project-id \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:your-service-account@*.iam.gserviceaccount.com"

# Required roles:
# - roles/pubsub.publisher (to publish)
# - roles/pubsub.subscriber (to subscribe)
# - roles/pubsub.admin (to create topics/subscriptions)
```

#### Ensure Pub/Sub API is enabled

```bash
# Check if API is enabled
gcloud services list --enabled | grep pubsub

# Enable if needed
gcloud services enable pubsub.googleapis.com
```

### Message Delivery Issues

#### Check subscription acknowledgment settings

```bash
# View subscription details
gcloud pubsub subscriptions describe orders-laravel

# Update ack deadline if needed
gcloud pubsub subscriptions update orders-laravel --ack-deadline=120
```

#### Verify topic and subscription names

```bash
# List all topics
php artisan pubsub:topics:list

# List all subscriptions
php artisan pubsub:subscriptions:list

# Test specific subscription
php artisan pubsub:listen orders-laravel --max-messages=1
```

#### Monitor dead letter topics

```bash
# Check if dead letter is configured
gcloud pubsub subscriptions describe orders-laravel \
  --format="value(deadLetterPolicy)"

# Monitor dead letter messages
php artisan pubsub:listen orders-dead-letter-inspector \
  --topic=orders-dead-letter \
  --max-messages=10
```

### Performance Issues

#### Adjust max_messages and ack_deadline settings

```php
// In config/queue.php or config/pubsub.php
'pubsub' => [
    'max_messages' => 100,        // Increase for batch processing
    'ack_deadline' => 120,        // Increase for slow jobs
    'wait_time' => 0,            // Reduce for lower latency
],
```

#### Use streaming subscribers for high throughput

```php
// For real-time, high-volume processing
$subscriber = PubSub::subscribe('high-volume');
$subscriber->stream([
    'max_messages_per_pull' => 1000,
]);
```

#### Consider message batching for publishing

```php
// Instead of individual publishes
collect($events)->each(fn($e) => PubSub::publish('events', $e));

// Use batch publishing
$messages = collect($events)->map(fn($e) => ['data' => $e])->toArray();
PubSub::publishBatch('events', $messages);
```

### Memory Issues

#### Large Message Handling

```php
// Monitor memory usage
$subscriber->handler(function ($data, $message) {
    $before = memory_get_usage();
    
    // Process large message
    $this->processLargeFile($data);
    
    // Force garbage collection if needed
    if (memory_get_usage() - $before > 50 * 1024 * 1024) { // 50MB
        gc_collect_cycles();
    }
});
```

#### Queue Worker Memory Limits

```bash
# Set appropriate memory limit
php artisan queue:work pubsub --memory=512 --timeout=300
```

### Authentication Issues

#### Application Default Credentials

```bash
# Set up ADC locally
gcloud auth application-default login

# Verify credentials
gcloud auth application-default print-access-token
```

#### Service Account Key File

```bash
# Verify key file exists and is valid
ls -la $GOOGLE_APPLICATION_CREDENTIALS

# Test authentication
php artisan pubsub:topics:list
```

### Debugging Tools

#### Enable Debug Logging

```dotenv
# In .env
PUBSUB_LOG_PUBLISHED=true
PUBSUB_LOG_CONSUMED=true
PUBSUB_LOG_FAILED=true
PUBSUB_LOG_WEBHOOKS=true
```

#### Message Inspector

```bash
# Inspect messages without processing
php artisan pubsub:inspect orders-laravel --limit=5
```

#### Test Publishing

```bash
# Test with a simple message
php artisan pubsub:publish orders '{"test":true,"timestamp":"'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"}'
```

#### Health Check

```bash
# Run health check
curl http://your-app.com/health/pubsub
```

### Common Error Messages

#### "Permission denied"

- Check service account has required Pub/Sub roles
- Verify project ID is correct
- Ensure API is enabled

#### "Resource not found"

- Topic or subscription doesn't exist
- Enable auto-creation or create manually

#### "Deadline exceeded"

- Increase ack_deadline for slow processing jobs
- Consider breaking large jobs into smaller tasks

#### "Invalid message format"

- Check schema validation if enabled
- Verify JSON encoding of messages
- Check for compression issues with large payloads

## Testing

```bash
./vendor/bin/pest
```

## License

The MIT License (MIT). Please see [License File](LICENSE) for more information.
