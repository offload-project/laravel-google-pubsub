<?php

declare(strict_types=1);

use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\Topic;
use OffloadProject\GooglePubSub\Concerns\ConfiguresSubscriptions;

beforeEach(function () {
    $this->client = Mockery::mock(PubSubClient::class);
    $this->topic = Mockery::mock(Topic::class);

    // Create a test class that uses the trait
    $this->configurator = new class
    {
        use ConfiguresSubscriptions;

        public function publicBuildSubscriptionConfig(array $config, ?string $topicName = null, ?PubSubClient $client = null): array
        {
            return $this->buildSubscriptionConfig($config, $topicName, $client);
        }

        public function publicBuildRetryPolicy(array $retryConfig): array
        {
            return $this->buildRetryPolicy($retryConfig);
        }

        public function publicBuildDeadLetterPolicy(array $deadLetterConfig, string $topicName, PubSubClient $client, bool $autoCreateTopics = true): array
        {
            return $this->buildDeadLetterPolicy($deadLetterConfig, $topicName, $client, $autoCreateTopics);
        }

        public function publicEnsureTopicExists(PubSubClient $client, string $topicName, array $config, array $topicOptions = []): Topic
        {
            return $this->ensureTopicExists($client, $topicName, $config, $topicOptions);
        }
    };
});

describe('ConfiguresSubscriptions buildSubscriptionConfig', function () {
    it('builds basic subscription config with default ack deadline', function () {
        $config = [];

        $result = $this->configurator->publicBuildSubscriptionConfig($config);

        expect($result)->toHaveKey('ackDeadlineSeconds');
        expect($result['ackDeadlineSeconds'])->toBe(60);
    });

    it('uses custom ack deadline from config', function () {
        $config = ['ack_deadline' => 120];

        $result = $this->configurator->publicBuildSubscriptionConfig($config);

        expect($result['ackDeadlineSeconds'])->toBe(120);
    });

    it('includes retry policy when configured', function () {
        $config = [
            'retry_policy' => [
                'minimum_backoff' => 10,
                'maximum_backoff' => 600,
            ],
        ];

        $result = $this->configurator->publicBuildSubscriptionConfig($config);

        expect($result)->toHaveKey('retryPolicy');
        expect($result['retryPolicy']['minimumBackoff'])->toBe('10s');
        expect($result['retryPolicy']['maximumBackoff'])->toBe('600s');
    });

    it('enables message ordering when configured', function () {
        $config = ['enable_message_ordering' => true];

        $result = $this->configurator->publicBuildSubscriptionConfig($config);

        expect($result)->toHaveKey('enableMessageOrdering');
        expect($result['enableMessageOrdering'])->toBeTrue();
    });

    it('does not include message ordering when disabled', function () {
        $config = ['enable_message_ordering' => false];

        $result = $this->configurator->publicBuildSubscriptionConfig($config);

        expect($result)->not->toHaveKey('enableMessageOrdering');
    });
});

describe('ConfiguresSubscriptions buildRetryPolicy', function () {
    it('passes through Google API format directly', function () {
        $retryConfig = [
            'minimumBackoff' => '15s',
            'maximumBackoff' => '300s',
        ];

        $result = $this->configurator->publicBuildRetryPolicy($retryConfig);

        expect($result)->toBe($retryConfig);
    });

    it('converts snake_case format to Google API format', function () {
        $retryConfig = [
            'minimum_backoff' => 10,
            'maximum_backoff' => 600,
        ];

        $result = $this->configurator->publicBuildRetryPolicy($retryConfig);

        expect($result['minimumBackoff'])->toBe('10s');
        expect($result['maximumBackoff'])->toBe('600s');
    });

    it('handles string values in snake_case format', function () {
        $retryConfig = [
            'minimum_backoff' => '20s',
            'maximum_backoff' => '500s',
        ];

        $result = $this->configurator->publicBuildRetryPolicy($retryConfig);

        expect($result['minimumBackoff'])->toBe('20s');
        expect($result['maximumBackoff'])->toBe('500s');
    });

    it('handles partial config with only minimum_backoff', function () {
        $retryConfig = ['minimum_backoff' => 5];

        $result = $this->configurator->publicBuildRetryPolicy($retryConfig);

        expect($result)->toHaveKey('minimumBackoff');
        expect($result)->not->toHaveKey('maximumBackoff');
        expect($result['minimumBackoff'])->toBe('5s');
    });

    it('handles partial config with only maximum_backoff', function () {
        $retryConfig = ['maximum_backoff' => 300];

        $result = $this->configurator->publicBuildRetryPolicy($retryConfig);

        expect($result)->toHaveKey('maximumBackoff');
        expect($result)->not->toHaveKey('minimumBackoff');
        expect($result['maximumBackoff'])->toBe('300s');
    });

    it('returns empty array for empty config', function () {
        $result = $this->configurator->publicBuildRetryPolicy([]);

        expect($result)->toBe([]);
    });
});

describe('ConfiguresSubscriptions buildDeadLetterPolicy', function () {
    it('creates dead letter policy with default suffix', function () {
        $deadLetterTopic = Mockery::mock(Topic::class);
        $deadLetterTopic->shouldReceive('exists')->andReturn(true);
        $deadLetterTopic->shouldReceive('name')->andReturn('projects/test/topics/my-topic-dead-letter');

        $this->client->shouldReceive('topic')
            ->with('my-topic-dead-letter')
            ->andReturn($deadLetterTopic);

        $deadLetterConfig = ['max_delivery_attempts' => 5];

        $result = $this->configurator->publicBuildDeadLetterPolicy(
            $deadLetterConfig,
            'my-topic',
            $this->client
        );

        expect($result)->toHaveKey('deadLetterTopic');
        expect($result)->toHaveKey('maxDeliveryAttempts');
        expect($result['deadLetterTopic'])->toBe('projects/test/topics/my-topic-dead-letter');
        expect($result['maxDeliveryAttempts'])->toBe(5);
    });

    it('uses custom dead letter topic suffix', function () {
        $deadLetterTopic = Mockery::mock(Topic::class);
        $deadLetterTopic->shouldReceive('exists')->andReturn(true);
        $deadLetterTopic->shouldReceive('name')->andReturn('projects/test/topics/my-topic-dlq');

        $this->client->shouldReceive('topic')
            ->with('my-topic-dlq')
            ->andReturn($deadLetterTopic);

        $deadLetterConfig = [
            'dead_letter_topic_suffix' => '-dlq',
            'max_delivery_attempts' => 3,
        ];

        $result = $this->configurator->publicBuildDeadLetterPolicy(
            $deadLetterConfig,
            'my-topic',
            $this->client
        );

        expect($result['maxDeliveryAttempts'])->toBe(3);
    });

    it('creates dead letter topic when it does not exist', function () {
        $deadLetterTopic = Mockery::mock(Topic::class);
        $deadLetterTopic->shouldReceive('exists')->andReturn(false);
        $deadLetterTopic->shouldReceive('create')->once();
        $deadLetterTopic->shouldReceive('name')->andReturn('projects/test/topics/my-topic-dead-letter');

        $this->client->shouldReceive('topic')
            ->with('my-topic-dead-letter')
            ->andReturn($deadLetterTopic);

        $deadLetterConfig = [];

        $result = $this->configurator->publicBuildDeadLetterPolicy(
            $deadLetterConfig,
            'my-topic',
            $this->client
        );

        expect($result)->toHaveKey('deadLetterTopic');
    });

    it('does not create dead letter topic when auto create is disabled', function () {
        $deadLetterTopic = Mockery::mock(Topic::class);
        $deadLetterTopic->shouldReceive('exists')->andReturn(false);
        $deadLetterTopic->shouldNotReceive('create');
        $deadLetterTopic->shouldReceive('name')->andReturn('projects/test/topics/my-topic-dead-letter');

        $this->client->shouldReceive('topic')
            ->with('my-topic-dead-letter')
            ->andReturn($deadLetterTopic);

        $deadLetterConfig = [];

        $result = $this->configurator->publicBuildDeadLetterPolicy(
            $deadLetterConfig,
            'my-topic',
            $this->client,
            false // autoCreateTopics = false
        );

        expect($result)->toHaveKey('deadLetterTopic');
    });

    it('uses default max delivery attempts when not specified', function () {
        $deadLetterTopic = Mockery::mock(Topic::class);
        $deadLetterTopic->shouldReceive('exists')->andReturn(true);
        $deadLetterTopic->shouldReceive('name')->andReturn('projects/test/topics/my-topic-dead-letter');

        $this->client->shouldReceive('topic')->andReturn($deadLetterTopic);

        $result = $this->configurator->publicBuildDeadLetterPolicy(
            [],
            'my-topic',
            $this->client
        );

        expect($result['maxDeliveryAttempts'])->toBe(5);
    });
});

describe('ConfiguresSubscriptions ensureTopicExists', function () {
    it('returns existing topic without creating', function () {
        $this->topic->shouldReceive('exists')->andReturn(true);
        $this->topic->shouldNotReceive('create');

        $this->client->shouldReceive('topic')
            ->with('existing-topic')
            ->andReturn($this->topic);

        $config = ['auto_create_topics' => true];

        $result = $this->configurator->publicEnsureTopicExists(
            $this->client,
            'existing-topic',
            $config
        );

        expect($result)->toBe($this->topic);
    });

    it('creates topic when it does not exist and auto create enabled', function () {
        $this->topic->shouldReceive('exists')->andReturn(false);
        $this->topic->shouldReceive('create')->with([])->once();

        $this->client->shouldReceive('topic')
            ->with('new-topic')
            ->andReturn($this->topic);

        $config = ['auto_create_topics' => true];

        $result = $this->configurator->publicEnsureTopicExists(
            $this->client,
            'new-topic',
            $config
        );

        expect($result)->toBe($this->topic);
    });

    it('does not create topic when auto create disabled', function () {
        $this->topic->shouldReceive('exists')->andReturn(false);
        $this->topic->shouldNotReceive('create');

        $this->client->shouldReceive('topic')
            ->with('missing-topic')
            ->andReturn($this->topic);

        $config = ['auto_create_topics' => false];

        $result = $this->configurator->publicEnsureTopicExists(
            $this->client,
            'missing-topic',
            $config
        );

        expect($result)->toBe($this->topic);
    });

    it('passes topic options when creating', function () {
        $this->topic->shouldReceive('exists')->andReturn(false);
        $this->topic->shouldReceive('create')
            ->with(['enableMessageOrdering' => true])
            ->once();

        $this->client->shouldReceive('topic')
            ->with('ordered-topic')
            ->andReturn($this->topic);

        $config = ['auto_create_topics' => true];
        $topicOptions = ['enableMessageOrdering' => true];

        $result = $this->configurator->publicEnsureTopicExists(
            $this->client,
            'ordered-topic',
            $config,
            $topicOptions
        );

        expect($result)->toBe($this->topic);
    });
});

describe('ConfiguresSubscriptions buildSubscriptionConfig with dead letter', function () {
    it('includes dead letter policy when enabled', function () {
        $deadLetterTopic = Mockery::mock(Topic::class);
        $deadLetterTopic->shouldReceive('exists')->andReturn(true);
        $deadLetterTopic->shouldReceive('name')->andReturn('projects/test/topics/test-topic-dead-letter');

        $this->client->shouldReceive('topic')
            ->with('test-topic-dead-letter')
            ->andReturn($deadLetterTopic);

        $config = [
            'dead_letter_policy' => [
                'enabled' => true,
                'max_delivery_attempts' => 10,
            ],
        ];

        $result = $this->configurator->publicBuildSubscriptionConfig(
            $config,
            'test-topic',
            $this->client
        );

        expect($result)->toHaveKey('deadLetterPolicy');
        expect($result['deadLetterPolicy']['maxDeliveryAttempts'])->toBe(10);
    });

    it('does not include dead letter policy when disabled', function () {
        $config = [
            'dead_letter_policy' => [
                'enabled' => false,
            ],
        ];

        $result = $this->configurator->publicBuildSubscriptionConfig($config);

        expect($result)->not->toHaveKey('deadLetterPolicy');
    });

    it('does not include dead letter policy without topic name', function () {
        $config = [
            'dead_letter_policy' => [
                'enabled' => true,
            ],
        ];

        $result = $this->configurator->publicBuildSubscriptionConfig($config, null, $this->client);

        expect($result)->not->toHaveKey('deadLetterPolicy');
    });

    it('does not include dead letter policy without client', function () {
        $config = [
            'dead_letter_policy' => [
                'enabled' => true,
            ],
        ];

        $result = $this->configurator->publicBuildSubscriptionConfig($config, 'test-topic', null);

        expect($result)->not->toHaveKey('deadLetterPolicy');
    });
});
