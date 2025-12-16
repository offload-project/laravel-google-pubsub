<?php

declare(strict_types=1);

namespace OffloadProject\GooglePubSub\Contracts;

use OffloadProject\GooglePubSub\Exceptions\PublishException;

interface PublisherInterface
{
    /**
     * Publish a message to a topic.
     *
     * @param  string  $topicName  The name of the topic to publish to
     * @param  mixed  $data  The message data
     * @param  array<string, mixed>  $attributes  Optional message attributes
     * @param  array<string, mixed>  $options  Optional publishing options
     * @return string The message ID
     *
     * @throws PublishException If publishing fails
     */
    public function publish(string $topicName, mixed $data, array $attributes = [], array $options = []): string;

    /**
     * Publish multiple messages to a topic in a single request.
     *
     * @param  string  $topicName  The name of the topic to publish to
     * @param  array<int, array<string, mixed>>  $messages  Array of messages with 'data' and optional 'attributes'
     * @param  array<string, mixed>  $options  Optional publishing options
     * @return array<int, string> Array of message IDs
     *
     * @throws PublishException If publishing fails
     */
    public function publishBatch(string $topicName, array $messages, array $options = []): array;

    /**
     * Set a custom message formatter.
     *
     * @param  MessageFormatter  $formatter  The formatter to use
     */
    public function setFormatter(MessageFormatter $formatter): static;
}
