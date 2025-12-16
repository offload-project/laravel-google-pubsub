<?php

declare(strict_types=1);

use OffloadProject\GooglePubSub\Http\Middleware\VerifyPubSubWebhook;

return [
    /*
    |--------------------------------------------------------------------------
    | Google Cloud Pub/Sub Configuration
    |--------------------------------------------------------------------------
    |
    | Configure your Google Cloud Pub/Sub settings for queue operations
    | and pub/sub messaging.
    |
    */

    'project_id' => env('GOOGLE_CLOUD_PROJECT_ID'),

    /*
    |--------------------------------------------------------------------------
    | Authentication
    |--------------------------------------------------------------------------
    |
    | Configure authentication method for Google Cloud Pub/Sub.
    | Supported: "application_default", "key_file"
    |
    */

    'auth_method' => env('PUBSUB_AUTH_METHOD', 'application_default'),

    'key_file' => env('GOOGLE_APPLICATION_CREDENTIALS'),

    /*
    |--------------------------------------------------------------------------
    | Emulator Support
    |--------------------------------------------------------------------------
    |
    | Set the emulator host for local development.
    |
    */

    'emulator_host' => env('PUBSUB_EMULATOR_HOST'),

    /*
    |--------------------------------------------------------------------------
    | Default Settings
    |--------------------------------------------------------------------------
    */

    'default_queue' => env('PUBSUB_DEFAULT_QUEUE', 'default'),

    'use_streaming' => env('PUBSUB_USE_STREAMING', true),

    /*
    |--------------------------------------------------------------------------
    | Queue Options
    |--------------------------------------------------------------------------
    |
    | Configure default options for all queues. These can be overridden
    | per queue in your queue.php configuration.
    |
    */

    'auto_create_topics' => env('PUBSUB_AUTO_CREATE_TOPICS', true),
    'auto_create_subscriptions' => env('PUBSUB_AUTO_CREATE_SUBSCRIPTIONS', true),
    'auto_acknowledge' => env('PUBSUB_AUTO_ACKNOWLEDGE', true),
    'nack_on_error' => env('PUBSUB_NACK_ON_ERROR', true),

    // Subscription settings
    'subscription_suffix' => env('PUBSUB_SUBSCRIPTION_SUFFIX', '-laravel'),
    'ack_deadline' => env('PUBSUB_ACK_DEADLINE', 60),
    'max_messages' => env('PUBSUB_MAX_MESSAGES', 10),
    'wait_time' => env('PUBSUB_WAIT_TIME', 3),

    // Retry policy
    'retry_policy' => [
        'minimum_backoff' => env('PUBSUB_MIN_BACKOFF', 10),
        'maximum_backoff' => env('PUBSUB_MAX_BACKOFF', 600),
    ],

    // Dead letter policy
    'dead_letter_policy' => [
        'enabled' => env('PUBSUB_DEAD_LETTER_ENABLED', true),
        'max_delivery_attempts' => env('PUBSUB_MAX_DELIVERY_ATTEMPTS', 5),
        'dead_letter_topic_suffix' => env('PUBSUB_DEAD_LETTER_SUFFIX', '-dead-letter'),
    ],

    /*
    |--------------------------------------------------------------------------
    | Failed Jobs Configuration
    |--------------------------------------------------------------------------
    |
    | Configure the Pub/Sub topic for storing failed jobs. Note that the
    | PubSubFailedJobProvider only supports logging failed jobs to a topic.
    | Retrieving, listing, or deleting failed jobs is not supported.
    |
    */

    'failed_jobs' => [
        'topic' => env('PUBSUB_FAILED_JOBS_TOPIC', 'laravel-failed-jobs'),
    ],

    /*
    |--------------------------------------------------------------------------
    | Message Options
    |--------------------------------------------------------------------------
    */

    'message_options' => [
        'add_metadata' => env('PUBSUB_ADD_METADATA', true),
        'compress_payload' => env('PUBSUB_COMPRESS_PAYLOAD', true),
        'compression_threshold' => env('PUBSUB_COMPRESSION_THRESHOLD', 1024), // bytes
    ],

    /*
    |--------------------------------------------------------------------------
    | Message Schemas
    |--------------------------------------------------------------------------
    |
    | Define JSON schemas for message validation.
    |
    */

    'schemas' => [
        // Example schema configuration
        // 'order_events' => [
        //     'file' => 'schemas/order-events.json',
        // ],
        // 'user_events' => [
        //     'schema' => [
        //         '$schema' => 'http://json-schema.org/draft-07/schema#',
        //         'type' => 'object',
        //         'required' => ['user_id', 'event_type'],
        //         'properties' => [
        //             'user_id' => ['type' => 'integer'],
        //             'event_type' => ['type' => 'string'],
        //             'data' => ['type' => 'object'],
        //         ],
        //     ],
        // ],
    ],

    'schema_validation' => [
        'enabled' => env('PUBSUB_SCHEMA_VALIDATION', true),
        'strict_mode' => env('PUBSUB_SCHEMA_STRICT', true),
    ],

    /*
    |--------------------------------------------------------------------------
    | Message Formatters
    |--------------------------------------------------------------------------
    |
    | Configure message formatters for different topics.
    |
    */

    'formatters' => [
        'default' => 'json',
        'cloud_events_source' => env('PUBSUB_CLOUDEVENTS_SOURCE', config('app.url')),
    ],

    /*
    |--------------------------------------------------------------------------
    | Topic Configuration
    |--------------------------------------------------------------------------
    |
    | Configure specific topics with their settings, schemas, and events.
    |
    */

    'topics' => [
        // 'orders' => [
        //     'enable_message_ordering' => true,
        //     'schema' => 'order_events',
        //     'events' => [
        //         \App\Events\OrderPlaced::class,
        //         \App\Events\OrderUpdated::class,
        //     ],
        // ],
    ],

    /*
    |--------------------------------------------------------------------------
    | Event Integration
    |--------------------------------------------------------------------------
    |
    | Configure which Laravel events should be published to Pub/Sub.
    |
    */

    'events' => [
        'enabled' => env('PUBSUB_EVENTS_ENABLED', false),

        // Events to publish to Pub/Sub
        'publish' => [
            // \App\Events\OrderPlaced::class,
        ],

        // Or use patterns
        'publish_patterns' => [
            // 'App\Events\Order*',
        ],
    ],

    /*
    |--------------------------------------------------------------------------
    | Webhook Configuration
    |--------------------------------------------------------------------------
    |
    | Configure webhook endpoints for push subscriptions.
    |
    */

    'webhook' => [
        'enabled' => env('PUBSUB_WEBHOOK_ENABLED', true),
        'route_prefix' => env('PUBSUB_WEBHOOK_PREFIX', 'pubsub/webhook'),
        'auth_token' => env('PUBSUB_WEBHOOK_TOKEN'),
        'skip_verification' => env('PUBSUB_WEBHOOK_SKIP_VERIFICATION', false),

        // IP allowlist (leave empty to allow all)
        'allowed_ips' => [
            // Google's IP ranges for Pub/Sub
            // See: https://cloud.google.com/pubsub/docs/push#receive_push
        ],

        // Additional middleware
        'middleware' => [
            VerifyPubSubWebhook::class,
        ],
    ],

    /*
    |--------------------------------------------------------------------------
    | Monitoring and Logging
    |--------------------------------------------------------------------------
    */

    'monitoring' => [
        'log_published_messages' => env('PUBSUB_LOG_PUBLISHED', false),
        'log_consumed_messages' => env('PUBSUB_LOG_CONSUMED', false),
        'log_failed_messages' => env('PUBSUB_LOG_FAILED', true),
        'log_webhooks' => env('PUBSUB_LOG_WEBHOOKS', false),
    ],
];
