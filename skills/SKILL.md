---
name: Laravel Google Pub/Sub
description: Conventions and APIs for the offload-project/laravel-google-pubsub package — Google Cloud Pub/Sub as a Laravel queue driver, direct publishing/subscribing, event-driven topic integration, push-subscription webhooks, JSON Schema validation, and CloudEvents formatting.
compatible_agents:
  - Claude Code
  - Cursor
tags:
  - laravel
  - php
  - pubsub
  - google-cloud
  - queue
  - events
  - messaging
  - cloudevents
---

## Context

`offload-project/laravel-google-pubsub` is a Laravel 11/12/13 package (PHP 8.3+) that wraps Google Cloud Pub/Sub. It ships:

- A Laravel queue driver (`'driver' => 'pubsub'`) backed by `PubSubQueue` and `PubSubConnector`.
- A `PubSub` facade and `PubSubManager` for direct publishing and subscribing.
- A `Publisher` with formatter, compression, schema validation, and batch support.
- Two subscribers: pull-based `Subscriber` and lower-latency `StreamingSubscriber` (selected by `pubsub.use_streaming`).
- Event integration via the `#[PublishTo]` attribute and the `ShouldPublishToPubSub` contract, plus `PubSubEventDispatcher` and `PubSubEventSubscriber`.
- Push-subscription webhook surface: `PubSubWebhookController` routed under `pubsub.webhook.route_prefix`, protected by the `VerifyPubSubWebhook` middleware.
- `MessageFormatter` contract with built-in `JsonFormatter` (default) and `CloudEventsFormatter` (CloudEvents v1.0).
- JSON Schema validation via `SchemaValidator` (Opis JSON Schema) configured under `pubsub.schemas`.
- Dead-letter topic auto-wiring and a write-only `PubSubFailedJobProvider`.
- Artisan commands under the `pubsub:*` namespace for topics, subscriptions, listening, publishing, and schema validation.

Apply this skill when working in a Laravel app that has `offload-project/laravel-google-pubsub` in `composer.json`, or when the user asks for help with `PubSub`, `PubSubManager`, the `pubsub` queue connection, `#[PublishTo]`, `ShouldPublishToPubSub`, Pub/Sub webhooks, CloudEvents publishing, or any `pubsub:*` Artisan command in this package.

## Rules

### Publishing

1. Use the `PubSub` facade for direct publishing — `PubSub::publish($topic, $data, $attributes, $options)` and `PubSub::publishBatch($topic, $messages, $options)`. Do **not** instantiate `PubSubManager`, `Publisher`, or `Google\Cloud\PubSub\PubSubClient` directly; the manager handles client caching, auth, formatter selection, schema validation, and compression.
2. The fourth argument to `publish()` is `$options`, not attributes — put attributes (string key/string value pairs) in the third argument and reserve `$options` for `ordering_key` and other publish-level settings.
3. For batch publishes, pass an array of `['data' => ..., 'attributes' => [...], 'ordering_key' => '...']` rows to `publishBatch()`. Prefer batch over a loop of `publish()` calls for hot paths — the package issues a single API call per batch.
4. Auto-compression kicks in when the encoded payload exceeds `pubsub.message_options.compression_threshold` (default 1024 bytes) and `compress_payload` is true. Consumers in this package decompress automatically; if you have non-PHP consumers on the same topic, either turn compression off for that topic or make sure those consumers also handle the `compressed=true` attribute.
5. Pub/Sub's max message size is 10 MB. For payloads near that ceiling, store the body in GCS and publish a pointer instead.

### Event integration

6. To auto-publish a Laravel event to Pub/Sub, do **all** of: add the `#[PublishTo('topic-name')]` attribute, implement `ShouldPublishToPubSub`, and define `pubsubTopic(): string` and `toPubSub(): array`. The attribute and the contract serve different purposes — the contract enables the dispatcher; the attribute carries topic-level metadata.
7. Enable event publishing by setting `pubsub.events.enabled = true`. Without it the `PubSubEventDispatcher` does not bind and your events will not publish.
8. Prefer `pubsub.events.publish` (explicit class list) over `pubsub.events.publish_patterns` (wildcards) for clarity — patterns are useful for `App\Events\Order*` families but make it harder to audit what's leaving the app.
9. `toPubSub()` is the wire format. Return a stable, versioned shape — downstream consumers (often non-PHP services) depend on it. Treat field renames and removals as breaking changes.

### Subscribing

10. Use `PubSub::subscribe($subscription, $topic)` to get a `Subscriber` (or `StreamingSubscriber` if `pubsub.use_streaming = true`). Register a handler with `->handler(fn ($data, $message) => ...)` and an error callback with `->onError(...)` before calling `->listen()` or `->stream()`.
11. The first arg to the handler is the **decoded** payload (already JSON-decoded, decompressed, and formatter-processed). The second is the raw `Google\Cloud\PubSub\Message`. Use the raw message only for attributes and message ID — don't re-decode the data.
12. Default behavior is auto-ACK on successful return, NACK on thrown exception (`pubsub.auto_acknowledge`, `pubsub.nack_on_error`). Throw to trigger redelivery; return normally to ACK. Don't call `$message->ack()` or `$message->nack()` manually unless you have explicitly disabled the auto-behavior.
13. Match `pubsub.ack_deadline` to your slowest expected handler. If processing exceeds the deadline, Pub/Sub will redeliver and you'll get duplicate-handler invocations.
14. For long-running listeners (containers, supervisor, Octane workers), the subscribers already register SIGTERM/SIGINT handlers via the `HandlesSignals` concern when `pcntl` is available — let them shut down gracefully, do not `exit()` from a handler.

### Queue driver

15. To use Pub/Sub as a queue, set `QUEUE_CONNECTION=pubsub` (or pass `->onConnection('pubsub')`) and add a `pubsub` block to `config/queue.php` with `'driver' => 'pubsub'`. The queue driver auto-creates the topic and subscription using `pubsub.subscription_suffix` (default `-laravel`) — e.g. dispatching to queue `orders` listens on subscription `orders-laravel`.
16. `PubSubQueue::size()`, `pendingSize()`, and `delayedSize()` always return `0` — Pub/Sub does not expose queue depth. Don't build dashboards or scaling logic on these methods; use Cloud Monitoring metrics on the subscription instead.
17. `PubSubFailedJobProvider` is **write-only**. It publishes failure records to the `pubsub.failed_jobs.topic` (default `laravel-failed-jobs`) but does not implement `all()`, `find()`, `forget()`, or `flush()` in a usable way. Don't wire `php artisan queue:failed`, `queue:retry`, or `queue:flush` flows against it — drain the failed-jobs topic with a real consumer.

### Webhooks (push subscriptions)

18. Push subscriptions hit `POST {pubsub.webhook.route_prefix}/{topic}` (default `/pubsub/webhook/{topic}`) and are protected by `VerifyPubSubWebhook`. Keep the middleware enabled. Don't set `pubsub.webhook.skip_verification = true` outside local testing.
19. Configure **either** the Bearer auth token (`pubsub.webhook.auth_token` plus the matching token on the Pub/Sub push subscription) **or** an IP allowlist (`pubsub.webhook.allowed_ips`), and preferably both in production. An empty allowlist with no token effectively allows any caller that knows the URL.
20. The webhook controller decodes the Pub/Sub envelope, decompresses if needed, and dispatches to the configured event/handler. Don't re-decode `request()->json('message.data')` yourself — use the controller, or write a custom controller that delegates to `WebhookMessage`.

### Schema validation

21. Define schemas under `pubsub.schemas` with either a `'file'` (relative to base path) or an inline `'schema'` array. Link a schema to a topic via `pubsub.topics.{topic}.schema = 'schema-name'`.
22. With `pubsub.schema_validation.strict_mode = true`, publishing or consuming a message whose topic has no matching schema raises `SchemaValidationException`. Use `strict_mode = false` only during migrations from un-validated topics.
23. Catch `SchemaValidationException` and call `->getErrors()` to surface the specific JSON-path errors back to the user — the message alone is too generic for end-user feedback.

### CloudEvents

24. Switch a topic (or globally) to CloudEvents by setting `pubsub.formatters.default = 'cloudevents'`, or wire the formatter explicitly: `PubSub::publisher()->setFormatter(new CloudEventsFormatter())`. The formatter wraps payloads in a CloudEvents v1.0 envelope (`id`, `source`, `type`, `time`, `data`).
25. Set `pubsub.formatters.cloud_events_source` to a stable URI that identifies *your* service (e.g. `https://orders.example.com`). Do not leave it as `config('app.url')` if your app URL varies by environment — consumers use `source` for routing and deduplication.

### Configuration & auth

26. Pick one auth strategy: `application_default` (ADC — preferred for GKE, Cloud Run, local `gcloud auth application-default login`) or `key_file` (path to a service-account JSON via `GOOGLE_APPLICATION_CREDENTIALS`). Don't ship service-account JSON inside the repo.
27. For local dev, set `PUBSUB_EMULATOR_HOST=localhost:8085` and run the emulator. Emulator state is non-persistent by default — re-create topics/subscriptions on each restart, or mount a volume.
28. Keep `auto_create_topics` / `auto_create_subscriptions` on in dev. In production, prefer to provision topics and subscriptions via Terraform/`gcloud` and turn auto-create off — silent creation hides config drift and may fail anyway if the service account lacks `pubsub.admin`.

### Ordering & throughput

29. Enable message ordering per-topic via `pubsub.topics.{topic}.enable_message_ordering = true`, **and** pass an `ordering_key` on every publish to that topic. Ordering substantially reduces throughput — only enable it where causal order genuinely matters (state machines, financial event streams). Don't enable it on high-volume analytics topics.

### Extension points

30. To customize message wire format, implement `MessageFormatter` (`format($data): string`, `parse(string): mixed`) and inject it via `PubSub::publisher()->setFormatter()` or by replacing the default in a service provider. Don't subclass `JsonFormatter` or `CloudEventsFormatter` — implement the contract directly.
31. To customize publishing behavior, implement `PublisherInterface`. Don't edit files under `vendor/offload-project/laravel-google-pubsub`.

## Examples

### Direct publishing

```php
use OffloadProject\GooglePubSub\Facades\PubSub;

PubSub::publish('orders', [
    'order_id' => 123,
    'total' => 99.99,
], [
    'priority' => 'high',
], [
    'ordering_key' => 'customer-456',
]);
```

### Batch publishing

```php
$messages = collect($orders)->map(fn ($order) => [
    'data' => [
        'order_id' => $order->id,
        'total' => $order->total,
    ],
    'attributes' => ['source' => 'import'],
])->all();

PubSub::publishBatch('orders', $messages);
```

### Event auto-publishing

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
            'placed_at' => $this->order->created_at->toIso8601String(),
        ];
    }
}
```

```php
// config/pubsub.php
'events' => [
    'enabled' => true,
    'publish' => [
        App\Events\OrderPlaced::class,
    ],
],
```

### Subscribing

```php
use OffloadProject\GooglePubSub\Facades\PubSub;

PubSub::subscribe('orders-processor', 'orders')
    ->handler(function (array $data, $message) {
        // $data already decoded, decompressed, and formatter-parsed.
        ProcessOrder::dispatchSync($data);
    })
    ->onError(function (\Throwable $e, $message) {
        report($e);
        // Throw to NACK (will redeliver). Return to swallow + NACK silently.
        throw $e;
    })
    ->listen();
```

### Schema validation

```php
// config/pubsub.php
'schemas' => [
    'order_events' => ['file' => 'schemas/order-events.json'],
],

'topics' => [
    'orders' => [
        'schema' => 'order_events',
    ],
],
```

```php
use OffloadProject\GooglePubSub\Exceptions\SchemaValidationException;

try {
    PubSub::publish('orders', $data);
} catch (SchemaValidationException $e) {
    foreach ($e->getErrors() as $error) {
        logger()->warning('Order event schema violation', $error);
    }
    throw $e;
}
```

### CloudEvents formatter

```php
// config/pubsub.php
'formatters' => [
    'default' => 'cloudevents',
    'cloud_events_source' => 'https://orders.example.com',
],
```

### Queue driver

```php
// config/queue.php
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
    ],
],
```

```php
ProcessPodcast::dispatch($podcast)->onQueue('audio-processing');
```

### Webhook (push subscription) setup

```php
// config/pubsub.php
'webhook' => [
    'enabled' => true,
    'route_prefix' => 'pubsub/webhook',
    'auth_token' => env('PUBSUB_WEBHOOK_TOKEN'), // matches push subscription's token
    'allowed_ips' => [
        // Google Cloud Pub/Sub source ranges
    ],
],
```

```bash
# Create the push subscription pointing at the route
php artisan pubsub:subscriptions:create-push orders-webhook orders \
  https://app.example.com/pubsub/webhook/orders \
  --token=$PUBSUB_WEBHOOK_TOKEN \
  --ack-deadline=120 \
  --dead-letter
```

### Common Artisan commands

```bash
php artisan pubsub:topics:list
php artisan pubsub:topics:create orders --enable-ordering
php artisan pubsub:subscriptions:list --topic=orders
php artisan pubsub:subscriptions:create orders-processor orders --ack-deadline=120 --dead-letter
php artisan pubsub:listen orders-processor --max-messages=10
php artisan pubsub:publish orders '{"order_id":1,"total":9.99}' --attributes=source=cli
php artisan pubsub:schema:validate order_events path/to/sample.json
```

## Anti-patterns

- ❌ Instantiating `Google\Cloud\PubSub\PubSubClient` or `PubSubManager` directly. Always go through the `PubSub` facade — it caches the client, applies auth, formatters, schema validation, and compression.
- ❌ Treating `$options` and `$attributes` interchangeably in `PubSub::publish()`. Attributes are the third argument (string=>string metadata); `$options` is the fourth (e.g. `ordering_key`).
- ❌ Calling `$message->ack()` / `$message->nack()` from a handler while `auto_acknowledge` is on. Return to ACK; throw to NACK. Mixing manual and auto behavior leads to double-ACKs and lost messages.
- ❌ Setting `pubsub.webhook.skip_verification = true` outside local testing, or running the webhook without **either** an auth token **or** an IP allowlist. The route is public; either guard makes it abusable.
- ❌ Relying on `PubSubQueue::size()` / `pendingSize()` / `delayedSize()` for autoscaling or dashboards. They always return `0` — use Cloud Monitoring on the subscription.
- ❌ Wiring `php artisan queue:failed`, `queue:retry`, or `queue:flush` against `PubSubFailedJobProvider`. It's write-only; consume the failed-jobs topic with a dedicated subscriber.
- ❌ Enabling `enable_message_ordering` on a high-volume topic to "just be safe". Ordering caps throughput — only use it where causal order genuinely matters.
- ❌ Implementing `ShouldPublishToPubSub` without the `#[PublishTo]` attribute (or vice versa) and expecting the dispatcher to auto-publish. You need both, plus `pubsub.events.enabled = true`.
- ❌ Subclassing `JsonFormatter` or `CloudEventsFormatter`. Implement the `MessageFormatter` contract directly so the wire format is explicit.
- ❌ Editing files inside `vendor/offload-project/laravel-google-pubsub`. All extension points (formatters, publishers, middleware, webhook controller) are pluggable via config + service container.
- ❌ Committing service-account JSON to the repo. Use ADC, secret managers, or `GOOGLE_APPLICATION_CREDENTIALS` pointing at a runtime-mounted path.

## References

- Repository: <https://github.com/offload-project/laravel-google-pubsub>
- Installation: <https://github.com/offload-project/laravel-google-pubsub/blob/main/docs/implementation/installation.md>
- Configuration reference: <https://github.com/offload-project/laravel-google-pubsub/blob/main/docs/implementation/configuration.md>
- Queue driver: <https://github.com/offload-project/laravel-google-pubsub/blob/main/docs/queue-driver.md>
- Direct Publisher & Subscriber: <https://github.com/offload-project/laravel-google-pubsub/blob/main/docs/direct-pubsub.md>
- Event integration: <https://github.com/offload-project/laravel-google-pubsub/blob/main/docs/event-integration.md>
- Webhooks (push subscriptions): <https://github.com/offload-project/laravel-google-pubsub/blob/main/docs/webhook-push.md>
- Message schemas: <https://github.com/offload-project/laravel-google-pubsub/blob/main/docs/messages/message-schemas.md>
- CloudEvents: <https://github.com/offload-project/laravel-google-pubsub/blob/main/docs/messages/cloudevents.md>
- Artisan commands: <https://github.com/offload-project/laravel-google-pubsub/blob/main/docs/artisan-commands.md>
- Monitoring & debugging: <https://github.com/offload-project/laravel-google-pubsub/blob/main/docs/reference/monitoring-debugging.md>
- Google Cloud Pub/Sub: <https://cloud.google.com/pubsub/docs>
- CloudEvents spec: <https://github.com/cloudevents/spec>
