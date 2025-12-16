<?php

declare(strict_types=1);

namespace OffloadProject\GooglePubSub\Contracts;

interface ShouldPublishToPubSub
{
    /**
     * Get the Pub/Sub topic for this event.
     */
    public function pubsubTopic(): string;

    /**
     * Convert the event to Pub/Sub data format.
     *
     * @return array<string, mixed>
     */
    public function toPubSub(): array;
}
