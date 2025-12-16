<?php

declare(strict_types=1);

namespace OffloadProject\GooglePubSub\Concerns;

use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\Topic;

/**
 * Provides shared subscription configuration logic.
 *
 * This trait extracts common subscription configuration building logic
 * used by PubSubQueue, Subscriber, and other classes that create subscriptions.
 */
trait ConfiguresSubscriptions
{
    /**
     * Build subscription configuration array.
     *
     * @param  array<string, mixed>  $config  The configuration array
     * @param  string|null  $topicName  The topic name (used for dead letter topic)
     * @param  PubSubClient|null  $client  The Pub/Sub client (for creating dead letter topic)
     * @return array<string, mixed>
     */
    protected function buildSubscriptionConfig(array $config, ?string $topicName = null, ?PubSubClient $client = null): array
    {
        $subscriptionConfig = [
            'ackDeadlineSeconds' => $config['ack_deadline'] ?? 60,
        ];

        // Configure retry policy
        if (isset($config['retry_policy'])) {
            $subscriptionConfig['retryPolicy'] = $this->buildRetryPolicy($config['retry_policy']);
        }

        // Configure dead letter policy
        if (($config['dead_letter_policy']['enabled'] ?? false) && $topicName !== null && $client !== null) {
            $subscriptionConfig['deadLetterPolicy'] = $this->buildDeadLetterPolicy(
                $config['dead_letter_policy'],
                $topicName,
                $client,
                $config['auto_create_topics'] ?? true
            );
        }

        // Enable message ordering
        if ($config['enable_message_ordering'] ?? false) {
            $subscriptionConfig['enableMessageOrdering'] = true;
        }

        return $subscriptionConfig;
    }

    /**
     * Build retry policy configuration.
     *
     * Accepts both snake_case config format (minimum_backoff, maximum_backoff)
     * and the Google API format (minimumBackoff, maximumBackoff).
     *
     * @param  array<string, mixed>  $retryConfig
     * @return array<string, mixed>
     */
    protected function buildRetryPolicy(array $retryConfig): array
    {
        // If already in Google API format, pass through
        if (isset($retryConfig['minimumBackoff']) || isset($retryConfig['maximumBackoff'])) {
            return $retryConfig;
        }

        // Convert from snake_case config format
        $policy = [];

        if (isset($retryConfig['minimum_backoff'])) {
            $value = $retryConfig['minimum_backoff'];
            $policy['minimumBackoff'] = is_string($value) ? $value : $value.'s';
        }

        if (isset($retryConfig['maximum_backoff'])) {
            $value = $retryConfig['maximum_backoff'];
            $policy['maximumBackoff'] = is_string($value) ? $value : $value.'s';
        }

        return $policy;
    }

    /**
     * Build dead letter policy configuration.
     *
     * @param  array<string, mixed>  $deadLetterConfig
     * @return array<string, mixed>
     */
    protected function buildDeadLetterPolicy(
        array $deadLetterConfig,
        string $topicName,
        PubSubClient $client,
        bool $autoCreateTopics = true
    ): array {
        $deadLetterTopicSuffix = $deadLetterConfig['dead_letter_topic_suffix'] ?? '-dead-letter';
        $deadLetterTopicName = $topicName.$deadLetterTopicSuffix;

        $deadLetterTopic = $client->topic($deadLetterTopicName);

        // Ensure dead letter topic exists
        if ($autoCreateTopics && ! $deadLetterTopic->exists()) {
            $deadLetterTopic->create();
        }

        return [
            'deadLetterTopic' => $deadLetterTopic->name(),
            'maxDeliveryAttempts' => $deadLetterConfig['max_delivery_attempts'] ?? 5,
        ];
    }

    /**
     * Ensure a topic exists, creating it if configured to do so.
     *
     * @param  array<string, mixed>  $config
     * @param  array<string, mixed>  $topicOptions
     */
    protected function ensureTopicExists(
        PubSubClient $client,
        string $topicName,
        array $config,
        array $topicOptions = []
    ): Topic {
        $topic = $client->topic($topicName);

        if (($config['auto_create_topics'] ?? true) && ! $topic->exists()) {
            $topic->create($topicOptions);
        }

        return $topic;
    }
}
