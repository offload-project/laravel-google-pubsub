<p align="center">
    <a href="https://packagist.org/packages/offload-project/laravel-google-pubsub"><img src="https://img.shields.io/packagist/v/offload-project/laravel-google-pubsub.svg?style=flat-square" alt="Latest Version on Packagist"></a>
    <a href="https://github.com/offload-project/laravel-google-pubsub/actions"><img src="https://img.shields.io/github/actions/workflow/status/offload-project/laravel-google-pubsub/tests.yml?branch=main&style=flat-square" alt="GitHub Tests Action Status"></a>
    <a href="https://packagist.org/packages/offload-project/laravel-google-pubsub"><img src="https://img.shields.io/packagist/dt/offload-project/laravel-google-pubsub.svg?style=flat-square" alt="Total Downloads"></a>
</p>

# Laravel Google Pub/Sub

A comprehensive Google Cloud Pub/Sub integration for Laravel that goes beyond a basic queue driver — a complete toolkit for event-driven architectures, microservice communication, and real-time data pipelines.

## Features

- **Full Laravel queue driver** — drop-in replacement for any other queue connection
- **Publisher / Subscriber services** — direct publishing with compression, metadata, and batch support
- **Event integration** — bidirectional flow between Laravel events and Pub/Sub topics
- **Webhook support** — handle push subscriptions with built-in IP allowlist and token verification
- **Schema validation** — JSON Schema validation for message contracts
- **Streaming support** — real-time, lower-latency processing with StreamingPull
- **CloudEvents support** — industry-standard event formatting with v1.0 compatibility
- **Dead-letter topics & retry policies** — auto-wired for resilient message delivery
- **Emulator support** — local development with the Google Cloud Pub/Sub emulator
- **Laravel Octane compatible** — connection pooling and warm bindings
- **Rich Artisan command set** — manage topics, subscriptions, publishing, and listening from the CLI

## Table of Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
    - [Basic Queue Usage](#1-basic-queue-usage)
    - [Direct Publishing](#2-direct-publishing)
    - [Event Integration](#3-event-integration)
    - [Subscribing to Messages](#4-subscribing-to-messages)
- [Full Documentation](#full-documentation)
- [AI Coding Assistant Skill](#ai-coding-assistant-skill)
- [Testing](#testing)
- [Contributing](#contributing)
- [Security](#security)
- [License](#license)

## Requirements

- PHP 8.3+
- Laravel 11 / 12 / 13
- A Google Cloud project with the Pub/Sub API enabled (or the [Pub/Sub emulator](https://cloud.google.com/pubsub/docs/emulator) for local development)

## Installation

```bash
composer require offload-project/laravel-google-pubsub

php artisan vendor:publish --provider="OffloadProject\GooglePubSub\PubSubServiceProvider" --tag="config"
```

Add the basics to your `.env`:

```dotenv
QUEUE_CONNECTION=pubsub
GOOGLE_CLOUD_PROJECT_ID=your-project-id

# Authentication — pick one
PUBSUB_AUTH_METHOD=application_default
# or
PUBSUB_AUTH_METHOD=key_file
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

Then add a `pubsub` connection to `config/queue.php`:

```php
'connections' => [
    'pubsub' => [
        'driver' => 'pubsub',
        'project_id' => env('GOOGLE_CLOUD_PROJECT_ID'),
        'queue' => env('PUBSUB_DEFAULT_QUEUE', 'default'),
        'auth_method' => env('PUBSUB_AUTH_METHOD', 'application_default'),
        'key_file' => env('GOOGLE_APPLICATION_CREDENTIALS'),
        'auto_create_topics' => true,
        'auto_create_subscriptions' => true,
        'subscription_suffix' => '-laravel',
        'enable_message_ordering' => false,
    ],
],
```

See the [Configuration reference](docs/implementation/configuration.md) for every option.

## Quick Start

### 1. Basic Queue Usage

Use it exactly like any other Laravel queue:

```php
ProcessPodcast::dispatch($podcast);

// Dispatch to a specific topic
ProcessPodcast::dispatch($podcast)->onQueue('audio-processing');
```

### 2. Direct Publishing

```php
use OffloadProject\GooglePubSub\Facades\PubSub;

PubSub::publish('orders', [
    'order_id' => 123,
    'total' => 99.99,
    'customer_id' => 456,
]);

// With attributes and an ordering key
PubSub::publish('orders', $data, [
    'priority' => 'high',
    'source' => 'api',
], [
    'ordering_key' => 'customer-456',
]);
```

### 3. Event Integration

```php
use OffloadProject\GooglePubSub\Attributes\PublishTo;
use OffloadProject\GooglePubSub\Contracts\ShouldPublishToPubSub;

#[PublishTo('orders')]
class OrderPlaced implements ShouldPublishToPubSub
{
    public function __construct(public Order $order) {}

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

event(new OrderPlaced($order));
```

### 4. Subscribing to Messages

```php
use OffloadProject\GooglePubSub\Facades\PubSub;

$subscriber = PubSub::subscribe('orders-processor', 'orders');

$subscriber->handler(function ($data, $message) {
    processOrder($data);
});

$subscriber->listen();
```

## Full Documentation

- **[Installation](docs/implementation/installation.md)**
- **[Configuration](docs/implementation/configuration.md)** — every config key, with defaults
- **[Queue Driver](docs/queue-driver.md)**
- **[Publisher & Subscriber](docs/direct-pubsub.md)**
- **[Event Integration](docs/event-integration.md)**
- **[Webhooks (Push Subscriptions)](docs/webhook-push.md)**
- **[Message Schemas and Validation](docs/messages/message-schemas.md)**
- **[CloudEvents](docs/messages/cloudevents.md)**
- **[Artisan Commands](docs/artisan-commands.md)**
- **[Monitoring & Debugging](docs/reference/monitoring-debugging.md)** — performance tuning and troubleshooting
- **[Testing](docs/reference/testing.md)**
- **[Examples](docs/reference/examples.md)**

## AI Coding Assistant Skill

This package ships a [Laravel Boost](https://skills.laravel.cloud/) skill so coding assistants (Claude Code, Cursor, etc.) follow the package's conventions when generating code. Install it in your app with:

```bash
php artisan boost:add-skill offload-project/laravel-google-pubsub
```

The skill source lives at [`skills/SKILL.md`](skills/SKILL.md).

## Testing

```bash
composer test
```

## Contributing

Contributions are welcome! Please see the documents below before getting started.

- [Contributing Guide](CONTRIBUTING.md) — setup, workflow, commit conventions, and PR process
- [Code of Conduct](CODE_OF_CONDUCT.md) — expectations for participation in this project

## Security

- [Security Policy](SECURITY.md) — how to report a vulnerability privately

## License

The MIT License (MIT). Please see [License File](LICENSE.md) for more information.
