<?php

declare(strict_types=1);

namespace OffloadProject\GooglePubSub\Messages;

/**
 * Mock message class for webhook compatibility.
 *
 * This class mimics the Google\Cloud\PubSub\Message interface
 * for messages received via webhooks.
 */
final class WebhookMessage
{
    /**
     * Create a new webhook message instance.
     *
     * @param  array<string, mixed>  $attributes
     */
    public function __construct(
        private string $id,
        private string $data,
        private array $attributes,
        private string $publishTime
    ) {}

    /**
     * Get the message ID.
     */
    public function id(): string
    {
        return $this->id;
    }

    /**
     * Get the message data.
     */
    public function data(): string
    {
        return $this->data;
    }

    /**
     * Get the message attributes.
     *
     * @return array<string, mixed>
     */
    public function attributes(): array
    {
        return $this->attributes;
    }

    /**
     * Get the publish time.
     */
    public function publishTime(): string
    {
        return $this->publishTime;
    }

    /**
     * Get the subscription name (not available for webhooks).
     */
    public function subscription(): ?string
    {
        return $this->attributes['subscription'] ?? null;
    }

    /**
     * Get the ordering key if present.
     */
    public function orderingKey(): ?string
    {
        return $this->attributes['ordering_key'] ?? null;
    }
}
