<?php

declare(strict_types=1);

namespace OffloadProject\GooglePubSub\Formatters;

use OffloadProject\GooglePubSub\Contracts\MessageFormatter;
use OffloadProject\GooglePubSub\Exceptions\MessageFormatException;

final class JsonFormatter implements MessageFormatter
{
    /**
     * JSON encoding options.
     */
    private int $encodeOptions;

    /**
     * JSON decode associative.
     */
    private bool $assoc;

    /**
     * Create a new JSON formatter.
     */
    public function __construct(
        int $encodeOptions = JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE,
        bool $assoc = true
    ) {
        $this->encodeOptions = $encodeOptions;
        $this->assoc = $assoc;
    }

    /**
     * Format data as JSON.
     */
    public function format($data): string
    {
        if (is_string($data)) {
            return $data;
        }

        $json = json_encode($data, $this->encodeOptions);

        if ($json === false) {
            throw new MessageFormatException(
                'Failed to encode data as JSON: '.json_last_error_msg()
            );
        }

        return $json;
    }

    /**
     * Parse JSON data.
     */
    public function parse(string $data)
    {
        if (empty($data)) {
            return null;
        }

        $decoded = json_decode($data, $this->assoc);

        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new MessageFormatException(
                'Failed to decode JSON data: '.json_last_error_msg()
            );
        }

        return $decoded;
    }
}
