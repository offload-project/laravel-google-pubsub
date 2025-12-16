<?php

declare(strict_types=1);

namespace OffloadProject\GooglePubSub\Failed;

use BadMethodCallException;
use Exception;
use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\Topic;
use Illuminate\Queue\Failed\FailedJobProviderInterface;
use Illuminate\Support\Facades\Date;
use Illuminate\Support\Facades\Log;
use OffloadProject\GooglePubSub\Exceptions\PubSubException;
use OffloadProject\GooglePubSub\PubSubManager;
use Throwable;

final class PubSubFailedJobProvider implements FailedJobProviderInterface
{
    /**
     * The default topic name for failed jobs.
     */
    public const DEFAULT_TOPIC = 'laravel-failed-jobs';

    /**
     * The PubSub manager instance.
     */
    private ?PubSubManager $manager;

    /**
     * The configuration array.
     *
     * @var array<string, mixed>
     */
    private array $config;

    /**
     * The cached topic instance.
     */
    private ?Topic $topic = null;

    /**
     * Create a new Pub/Sub failed job provider.
     *
     * @param  array<string, mixed>  $config
     */
    public function __construct(?PubSubManager $manager, array $config)
    {
        $this->manager = $manager;
        $this->config = $config;
    }

    /**
     * Log a failed job into storage.
     *
     * @param  string  $connection
     * @param  string  $queue
     * @param  string  $payload
     * @param  Throwable  $exception
     */
    public function log($connection, $queue, $payload, $exception): string|int|null
    {
        $failedAt = Date::now();

        $topic = $this->getFailedJobsTopic();

        $messageData = [
            'data' => json_encode([
                'connection' => $connection,
                'queue' => $queue,
                'payload' => $payload,
                'exception' => (string) $exception,
                'failed_at' => $failedAt->toIso8601String(),
            ]),
            'attributes' => [
                'connection' => $connection,
                'queue' => $queue,
                'failed_at' => (string) $failedAt->timestamp,
                'exception_class' => get_class($exception),
            ],
        ];

        try {
            $result = $topic->publish($messageData);

            $messageId = $result['messageIds'][0] ?? null;

            if ($this->config['monitoring']['log_failed_messages'] ?? true) {
                Log::error('Job failed and logged to Pub/Sub', [
                    'connection' => $connection,
                    'queue' => $queue,
                    'message_id' => $messageId,
                    'exception' => $exception->getMessage(),
                ]);
            }

            return $messageId;
        } catch (Exception $e) {
            throw new PubSubException(
                "Failed to log failed job: {$e->getMessage()}",
                $e->getCode(),
                $e
            );
        }
    }

    /**
     * Get a list of all of the failed jobs.
     *
     * @return array<int, object>
     *
     * @throws BadMethodCallException
     */
    public function all(): array
    {
        throw new BadMethodCallException(
            'The PubSubFailedJobProvider does not support retrieving failed jobs. '
            .'Pub/Sub is a fire-and-forget message queue and does not store historical messages. '
            .'Consider using a database-backed failed job provider if you need to list failed jobs.'
        );
    }

    /**
     * Get a single failed job.
     *
     * @param  mixed  $id
     *
     * @throws BadMethodCallException
     */
    public function find($id): ?object
    {
        throw new BadMethodCallException(
            'The PubSubFailedJobProvider does not support finding failed jobs by ID. '
            .'Pub/Sub is a fire-and-forget message queue and does not store historical messages. '
            .'Consider using a database-backed failed job provider if you need to retrieve failed jobs.'
        );
    }

    /**
     * Delete a single failed job from storage.
     *
     * @param  mixed  $id
     *
     * @throws BadMethodCallException
     */
    public function forget($id): bool
    {
        throw new BadMethodCallException(
            'The PubSubFailedJobProvider does not support deleting failed jobs. '
            .'Pub/Sub is a fire-and-forget message queue and does not store historical messages. '
            .'Consider using a database-backed failed job provider if you need to manage failed jobs.'
        );
    }

    /**
     * Flush all of the failed jobs from storage.
     *
     * @param  int|null  $hours
     *
     * @throws BadMethodCallException
     */
    public function flush($hours = null): void
    {
        throw new BadMethodCallException(
            'The PubSubFailedJobProvider does not support flushing failed jobs. '
            .'Pub/Sub is a fire-and-forget message queue and does not store historical messages. '
            .'Consider using a database-backed failed job provider if you need to manage failed jobs.'
        );
    }

    /**
     * Get the IDs of all failed jobs.
     *
     * @param  string|null  $queue
     * @return array<int, string>
     *
     * @throws BadMethodCallException
     */
    public function ids($queue = null): array
    {
        throw new BadMethodCallException(
            'The PubSubFailedJobProvider does not support listing failed job IDs. '
            .'Pub/Sub is a fire-and-forget message queue and does not store historical messages. '
            .'Consider using a database-backed failed job provider if you need to list failed jobs.'
        );
    }

    /**
     * Count the failed jobs.
     *
     * @param  string|null  $connection
     * @param  string|null  $queue
     *
     * @throws BadMethodCallException
     */
    public function count($connection = null, $queue = null): int
    {
        throw new BadMethodCallException(
            'The PubSubFailedJobProvider does not support counting failed jobs. '
            .'Pub/Sub is a fire-and-forget message queue and does not store historical messages. '
            .'Consider using a database-backed failed job provider if you need to count failed jobs.'
        );
    }

    /**
     * Get the Pub/Sub client instance.
     */
    private function getPubSubClient(): PubSubClient
    {
        if ($this->manager) {
            return $this->manager->client();
        }

        // Fallback for when manager is not available (e.g., testing)
        $pubsubConfig = [
            'projectId' => $this->config['project_id'],
        ];

        if (($this->config['auth_method'] ?? 'application_default') === 'key_file' && ! empty($this->config['key_file'])) {
            $pubsubConfig['keyFilePath'] = $this->config['key_file'];
        }

        if ($emulatorHost = $this->config['emulator_host'] ?? null) {
            $pubsubConfig['emulatorHost'] = $emulatorHost;
        }

        return new PubSubClient($pubsubConfig);
    }

    /**
     * Get the failed jobs topic.
     */
    private function getFailedJobsTopic(): Topic
    {
        if ($this->topic !== null) {
            return $this->topic;
        }

        $client = $this->getPubSubClient();
        $topicName = $this->config['failed_jobs']['topic'] ?? self::DEFAULT_TOPIC;

        $topic = $client->topic($topicName);

        if (($this->config['auto_create_topics'] ?? true) && ! $topic->exists()) {
            $topic->create();
        }

        $this->topic = $topic;

        return $this->topic;
    }
}
