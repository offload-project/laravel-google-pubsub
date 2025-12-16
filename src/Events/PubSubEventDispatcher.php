<?php

declare(strict_types=1);

namespace OffloadProject\GooglePubSub\Events;

use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Str;
use OffloadProject\GooglePubSub\Attributes\PublishTo;
use OffloadProject\GooglePubSub\Contracts\ShouldPublishToPubSub;
use OffloadProject\GooglePubSub\PubSubManager;
use ReflectionClass;

class PubSubEventDispatcher
{
    /**
     * The PubSub manager instance.
     */
    private PubSubManager $pubsub;

    /**
     * The event dispatcher instance.
     */
    private Dispatcher $events;

    /**
     * The configuration array.
     *
     * @var array<string, mixed>
     */
    private array $config;

    /**
     * Events currently being dispatched (to prevent loops).
     *
     * @var array<int, string>
     */
    private array $dispatching = [];

    /**
     * Cache for PublishTo attribute lookups by class name.
     *
     * @var array<class-string, string|null>
     */
    private array $attributeCache = [];

    /**
     * Create a new PubSub event dispatcher.
     *
     * @param  array<string, mixed>  $config
     */
    public function __construct(PubSubManager $pubsub, Dispatcher $events, array $config)
    {
        $this->pubsub = $pubsub;
        $this->events = $events;
        $this->config = $config;
    }

    /**
     * Register the event listener.
     */
    public function register(): void
    {
        if (! ($this->config['events']['enabled'] ?? false)) {
            return;
        }

        $this->events->listen('*', [$this, 'handleEvent']);
    }

    /**
     * Handle an event dispatch.
     *
     * @param  array<int, mixed>  $payload
     */
    public function handleEvent(string $eventName, array $payload): void
    {
        // Prevent infinite loops
        if (in_array($eventName, $this->dispatching)) {
            return;
        }

        // Get the event object
        $event = $payload[0] ?? null;
        if (! is_object($event)) {
            return;
        }

        // Check if this event should be published
        if (! $this->shouldPublish($eventName, $event)) {
            return;
        }

        try {
            $this->dispatching[] = $eventName;
            $this->publishEvent($eventName, $event);
        } finally {
            $key = array_search($eventName, $this->dispatching);
            if ($key !== false) {
                unset($this->dispatching[$key]);
            }
        }
    }

    /**
     * Check if an event should be published to Pub/Sub.
     */
    private function shouldPublish(string $eventName, object $event): bool
    {
        // Check if event implements the interface
        if ($event instanceof ShouldPublishToPubSub) {
            return true;
        }

        // Check explicit event list
        $publishEvents = $this->config['events']['publish'] ?? [];
        if (in_array(get_class($event), $publishEvents)) {
            return true;
        }

        // Check patterns
        $patterns = $this->config['events']['publish_patterns'] ?? [];
        foreach ($patterns as $pattern) {
            if (Str::is($pattern, get_class($event))) {
                return true;
            }
        }

        return false;
    }

    /**
     * Publish an event to Pub/Sub.
     */
    private function publishEvent(string $eventName, object $event): void
    {
        $topic = $this->getTopicForEvent($event);

        $data = [
            'event' => $eventName,
            'class' => get_class($event),
            'data' => $this->serializeEvent($event),
            'timestamp' => now()->toIso8601String(),
        ];

        $attributes = [
            'source' => 'laravel',
            'event_name' => $eventName,
            'event_class' => get_class($event),
        ];

        // Add custom attributes if event provides them
        if (method_exists($event, 'pubsubAttributes')) {
            $attributes = array_merge($attributes, $event->pubsubAttributes());
        }

        // Add ordering key if provided
        $options = [];
        if (method_exists($event, 'pubsubOrderingKey')) {
            $options['ordering_key'] = $event->pubsubOrderingKey();
        }

        $messageId = $this->pubsub->publish($topic, $data, $attributes, $options);

        if ($this->config['monitoring']['log_published_messages'] ?? false) {
            Log::info('Published Laravel event to Pub/Sub', [
                'event' => $eventName,
                'topic' => $topic,
                'message_id' => $messageId,
            ]);
        }
    }

    /**
     * Get the topic for an event.
     */
    private function getTopicForEvent(object $event): string
    {
        // Check if event specifies its topic via method (highest priority)
        if (method_exists($event, 'pubsubTopic')) {
            return $event->pubsubTopic();
        }

        // Check if event has PublishTo attribute (cached)
        $attributeTopic = $this->getTopicFromAttribute($event);
        if ($attributeTopic !== null) {
            return $attributeTopic;
        }

        // Check topic mappings from config
        foreach ($this->config['topics'] ?? [] as $topic => $topicConfig) {
            $events = $topicConfig['events'] ?? [];
            if (in_array(get_class($event), $events)) {
                return $topic;
            }
        }

        // Default topic
        return $this->config['events']['default_topic'] ?? 'laravel-events';
    }

    /**
     * Get the topic from the PublishTo attribute, using cache.
     */
    private function getTopicFromAttribute(object $event): ?string
    {
        $className = get_class($event);

        // Check cache first
        if (array_key_exists($className, $this->attributeCache)) {
            return $this->attributeCache[$className];
        }

        // Perform reflection lookup
        $reflection = new ReflectionClass($event);
        $attributes = $reflection->getAttributes(PublishTo::class);

        $topic = ! empty($attributes)
            ? $attributes[0]->newInstance()->topic
            : null;

        // Cache the result (including null for classes without the attribute)
        $this->attributeCache[$className] = $topic;

        return $topic;
    }

    /**
     * Serialize an event for publishing.
     *
     * @return array<string, mixed>
     */
    private function serializeEvent(object $event): array
    {
        if (method_exists($event, 'toPubSub')) {
            return $event->toPubSub();
        }

        if (method_exists($event, 'toArray')) {
            return $event->toArray();
        }

        // Default serialization - get public properties
        return get_object_vars($event);
    }
}
