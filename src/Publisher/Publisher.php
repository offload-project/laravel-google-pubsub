<?php

declare(strict_types=1);

namespace OffloadProject\GooglePubSub\Publisher;

use Exception;
use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\Topic;
use Illuminate\Support\Facades\Log;
use OffloadProject\GooglePubSub\Contracts\MessageFormatter;
use OffloadProject\GooglePubSub\Contracts\PublisherInterface;
use OffloadProject\GooglePubSub\Exceptions\PublishException;
use OffloadProject\GooglePubSub\Formatters\JsonFormatter;
use OffloadProject\GooglePubSub\Schema\SchemaValidator;

class Publisher implements PublisherInterface
{
    /**
     * The PubSub client instance.
     */
    private PubSubClient $client;

    /**
     * The configuration array.
     *
     * @var array<string, mixed>
     */
    private array $config;

    /**
     * The message formatter.
     */
    private MessageFormatter $formatter;

    /**
     * The schema validator.
     */
    private ?SchemaValidator $validator = null;

    /**
     * Cached topic instances.
     *
     * @var array<string, Topic>
     */
    private array $topics = [];

    /**
     * Create a new publisher instance.
     *
     * @param  array<string, mixed>  $config
     */
    public function __construct(PubSubClient $client, array $config = [])
    {
        $this->client = $client;
        $this->config = $config;
        $this->formatter = new JsonFormatter();
    }

    /**
     * Publish a message to a topic.
     *
     * @param  array<string, mixed>  $attributes
     * @param  array<string, mixed>  $options
     * @return string Message ID
     *
     * @throws PublishException
     */
    public function publish(string $topicName, mixed $data, array $attributes = [], array $options = []): string
    {
        try {
            $topic = $this->getTopic($topicName);

            // Format the message
            $message = $this->prepareMessage($topicName, $data, $attributes, $options);

            // Publish the message
            $result = $topic->publish($message);

            $messageId = $result['messageIds'][0] ?? null;

            if (! $messageId) {
                throw new PublishException('Failed to get message ID from publish result');
            }

            // Log if configured
            if ($this->config['monitoring']['log_published_messages'] ?? false) {
                Log::info('Published message to Pub/Sub', [
                    'topic' => $topicName,
                    'message_id' => $messageId,
                    'attributes' => $attributes,
                ]);
            }

            return $messageId;
        } catch (Exception $e) {
            throw new PublishException(
                "Failed to publish message to topic '{$topicName}': {$e->getMessage()}",
                $e->getCode(),
                $e
            );
        }
    }

    /**
     * Publish multiple messages to a topic.
     *
     * @param  array<int, array<string, mixed>>  $messages
     * @param  array<string, mixed>  $options
     * @return array<int, string> Message IDs
     *
     * @throws PublishException
     */
    public function publishBatch(string $topicName, array $messages, array $options = []): array
    {
        try {
            $topic = $this->getTopic($topicName);

            $formattedMessages = [];
            foreach ($messages as $message) {
                $formattedMessages[] = $this->prepareMessage(
                    $topicName,
                    $message['data'] ?? null,
                    $message['attributes'] ?? [],
                    $options
                );
            }

            $result = $topic->publishBatch($formattedMessages);

            return $result['messageIds'] ?? [];
        } catch (Exception $e) {
            throw new PublishException(
                "Failed to publish batch to topic '{$topicName}': {$e->getMessage()}",
                $e->getCode(),
                $e
            );
        }
    }

    /**
     * Set a custom message formatter.
     */
    public function setFormatter(MessageFormatter $formatter): static
    {
        $this->formatter = $formatter;

        return $this;
    }

    /**
     * Get or create a topic instance.
     */
    private function getTopic(string $topicName): Topic
    {
        if (! isset($this->topics[$topicName])) {
            $topic = $this->client->topic($topicName);

            // Auto-create topic if configured
            if ($this->config['auto_create_topics'] ?? true) {
                if (! $topic->exists()) {
                    $topicConfig = $this->getTopicConfig($topicName);
                    $topic->create($topicConfig);

                    Log::info("Created Pub/Sub topic: {$topicName}");
                }
            }

            $this->topics[$topicName] = $topic;
        }

        return $this->topics[$topicName];
    }

    /**
     * Prepare a message for publishing.
     *
     * @param  array<string, mixed>  $attributes
     * @param  array<string, mixed>  $options
     * @return array<string, mixed>
     */
    private function prepareMessage(string $topicName, mixed $data, array $attributes, array $options): array
    {
        // Get topic-specific configuration
        $topicConfig = $this->config['topics'][$topicName] ?? [];

        // Apply schema validation if configured
        if (isset($topicConfig['schema'])) {
            $this->validateMessage($data, $topicConfig['schema']);
        }

        // Format the data
        $formattedData = $this->formatter->format($data);

        // Apply compression if needed
        if ($this->shouldCompress($formattedData, $options)) {
            $formattedData = gzcompress($formattedData);
            $attributes['compressed'] = 'true';
            $attributes['compression_type'] = 'gzip';
        }

        // Add metadata if configured
        if ($this->config['message_options']['add_metadata'] ?? true) {
            $attributes = array_merge($attributes, [
                'published_at' => (string) time(),
                'publisher' => 'laravel',
                'hostname' => gethostname() ?: 'unknown',
                'app_name' => $this->config['app_name'] ?? 'laravel',
            ]);
        }

        // Build the message
        $message = [
            'data' => $formattedData,
            'attributes' => $attributes,
        ];

        // Add ordering key if provided
        if (isset($options['ordering_key'])) {
            $message['orderingKey'] = $options['ordering_key'];
        }

        return $message;
    }

    /**
     * Validate message against schema.
     */
    private function validateMessage(mixed $data, string $schemaName): void
    {
        if (! $this->validator) {
            $this->validator = new SchemaValidator($this->config);
        }

        $this->validator->validate($data, $schemaName);
    }

    /**
     * Determine if the message should be compressed.
     *
     * @param  array<string, mixed>  $options
     */
    private function shouldCompress(string $data, array $options): bool
    {
        if (isset($options['compress'])) {
            return (bool) $options['compress'];
        }

        if (! ($this->config['message_options']['compress_payload'] ?? true)) {
            return false;
        }

        $threshold = $this->config['message_options']['compression_threshold'] ?? 1024;

        return mb_strlen($data) > $threshold;
    }

    /**
     * Get topic configuration.
     *
     * @return array<string, mixed>
     */
    private function getTopicConfig(string $topicName): array
    {
        $config = [];

        // Check if message ordering is enabled for this topic
        $topicSettings = $this->config['topics'][$topicName] ?? [];
        if ($topicSettings['enable_message_ordering'] ?? false) {
            $config['enableMessageOrdering'] = true;
        }

        return $config;
    }
}
