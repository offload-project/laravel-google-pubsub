<?php

declare(strict_types=1);

namespace OffloadProject\GooglePubSub\Formatters;

use Illuminate\Support\Str;
use OffloadProject\GooglePubSub\Contracts\MessageFormatter;
use OffloadProject\GooglePubSub\Exceptions\MessageFormatException;

final class CloudEventsFormatter implements MessageFormatter
{
    /**
     * CloudEvents spec version.
     */
    private string $specVersion = '1.0';

    /**
     * Default source for events.
     */
    private string $source;

    /**
     * Default event type.
     */
    private string $defaultType = 'com.example.event';

    /**
     * Create a new CloudEvents formatter.
     */
    public function __construct(?string $source = null, ?string $defaultType = null)
    {
        $this->source = $source ?? config('app.url', 'https://example.com');

        if ($defaultType) {
            $this->defaultType = $defaultType;
        }
    }

    /**
     * Format data as CloudEvents JSON.
     */
    public function format($data): string
    {
        if (is_string($data)) {
            // Assume it's already CloudEvents formatted
            return $data;
        }

        $event = $this->createCloudEvent($data);

        $json = json_encode($event, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);

        if ($json === false) {
            throw new MessageFormatException(
                'Failed to encode data as CloudEvents JSON: '.json_last_error_msg()
            );
        }

        return $json;
    }

    /**
     * Parse CloudEvents JSON data.
     */
    public function parse(string $data)
    {
        if (empty($data)) {
            return null;
        }

        $decoded = json_decode($data, true);

        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new MessageFormatException(
                'Failed to decode CloudEvents JSON: '.json_last_error_msg()
            );
        }

        // Validate required CloudEvents fields
        $required = ['specversion', 'type', 'source', 'id'];
        foreach ($required as $field) {
            if (! isset($decoded[$field])) {
                throw new MessageFormatException(
                    "Invalid CloudEvents format: missing required field '{$field}'"
                );
            }
        }

        // Return the data payload
        return $decoded['data'] ?? $decoded;
    }

    /**
     * Create a CloudEvent structure from data.
     *
     * @param  mixed  $data
     * @return array<string, mixed>
     */
    private function createCloudEvent($data): array
    {
        // If data already has CloudEvents structure, use it
        if (is_array($data) && isset($data['specversion']) && isset($data['type'])) {
            return $data;
        }

        $event = [
            'specversion' => $this->specVersion,
            'type' => $this->getEventType($data),
            'source' => $this->getEventSource($data),
            'id' => $this->getEventId($data),
            'time' => $this->getEventTime($data),
            'datacontenttype' => 'application/json',
        ];

        // Add subject if available
        if ($subject = $this->getEventSubject($data)) {
            $event['subject'] = $subject;
        }

        // Add the actual data
        $event['data'] = $this->getEventData($data);

        return $event;
    }

    /**
     * Get event type from data.
     *
     * @param  mixed  $data
     */
    private function getEventType($data): string
    {
        if (is_array($data)) {
            return $data['type']
                ?? $data['event']
                ?? $data['event_type']
                ?? $this->defaultType;
        }

        if (is_object($data)) {
            if (method_exists($data, 'getEventType')) {
                return $data->getEventType();
            }

            $className = get_class($data);

            return Str::snake(class_basename($className), '.');
        }

        return $this->defaultType;
    }

    /**
     * Get event source.
     *
     * @param  mixed  $data
     */
    private function getEventSource($data): string
    {
        if (is_array($data) && isset($data['source'])) {
            return $data['source'];
        }

        if (is_object($data) && method_exists($data, 'getEventSource')) {
            return $data->getEventSource();
        }

        return $this->source;
    }

    /**
     * Get event ID.
     *
     * @param  mixed  $data
     */
    private function getEventId($data): string
    {
        if (is_array($data) && isset($data['id'])) {
            return $data['id'];
        }

        if (is_object($data) && method_exists($data, 'getEventId')) {
            return $data->getEventId();
        }

        return (string) Str::uuid();
    }

    /**
     * Get event time.
     *
     * @param  mixed  $data
     */
    private function getEventTime($data): string
    {
        if (is_array($data) && isset($data['time'])) {
            return $data['time'];
        }

        if (is_object($data) && method_exists($data, 'getEventTime')) {
            return $data->getEventTime();
        }

        return now()->toRfc3339String();
    }

    /**
     * Get event subject.
     *
     * @param  mixed  $data
     */
    private function getEventSubject($data): ?string
    {
        if (is_array($data) && isset($data['subject'])) {
            return $data['subject'];
        }

        if (is_object($data) && method_exists($data, 'getEventSubject')) {
            return $data->getEventSubject();
        }

        return null;
    }

    /**
     * Get event data payload.
     *
     * @param  mixed  $data
     * @return mixed
     */
    private function getEventData($data)
    {
        if (is_array($data)) {
            // Remove CloudEvents metadata if present
            $metadata = ['specversion', 'type', 'source', 'id', 'time', 'subject', 'datacontenttype'];

            return array_diff_key($data, array_flip($metadata));
        }

        if (is_object($data)) {
            if (method_exists($data, 'toCloudEventData')) {
                return $data->toCloudEventData();
            }

            if (method_exists($data, 'toArray')) {
                return $data->toArray();
            }

            return get_object_vars($data);
        }

        return $data;
    }
}
