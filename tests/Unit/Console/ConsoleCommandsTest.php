<?php

declare(strict_types=1);

use Google\Cloud\PubSub\Subscription;
use Google\Cloud\PubSub\Topic;
use Illuminate\Support\Facades\Artisan;
use OffloadProject\GooglePubSub\Facades\PubSub;

describe('CreateTopicCommand', function () {
    it('creates a new topic successfully', function () {
        PubSub::shouldReceive('createTopic')
            ->with('test-topic', [])
            ->once();

        $exitCode = Artisan::call('pubsub:topics:create', [
            'name' => 'test-topic',
        ]);

        expect($exitCode)->toBe(0);
        expect(Artisan::output())->toContain('created successfully');
    });

    it('creates a topic with message ordering enabled', function () {
        PubSub::shouldReceive('createTopic')
            ->with('ordered-topic', ['enableMessageOrdering' => true])
            ->once();

        $exitCode = Artisan::call('pubsub:topics:create', [
            'name' => 'ordered-topic',
            '--enable-ordering' => true,
        ]);

        expect($exitCode)->toBe(0);
        expect(Artisan::output())->toContain('created successfully');
    });

    it('handles creation errors gracefully', function () {
        PubSub::shouldReceive('createTopic')
            ->andThrow(new Exception('Topic already exists'));

        $exitCode = Artisan::call('pubsub:topics:create', [
            'name' => 'existing-topic',
        ]);

        expect($exitCode)->toBe(1);
        expect(Artisan::output())->toContain('Failed to create topic');
    });

    it('validates topic name is a string', function () {
        $exitCode = Artisan::call('pubsub:topics:create', [
            'name' => null,
        ]);

        expect($exitCode)->toBe(1);
    });
});

describe('CreateSubscriptionCommand', function () {
    it('creates a new subscription successfully', function () {
        PubSub::shouldReceive('createSubscription')
            ->with('test-sub', 'test-topic', [
                'ackDeadlineSeconds' => 60,
            ])
            ->once();

        $exitCode = Artisan::call('pubsub:subscriptions:create', [
            'name' => 'test-sub',
            'topic' => 'test-topic',
        ]);

        expect($exitCode)->toBe(0);
        expect(Artisan::output())->toContain('created successfully');
    });

    it('creates subscription with custom ack deadline', function () {
        PubSub::shouldReceive('createSubscription')
            ->with('test-sub', 'test-topic', [
                'ackDeadlineSeconds' => 120,
            ])
            ->once();

        $exitCode = Artisan::call('pubsub:subscriptions:create', [
            'name' => 'test-sub',
            'topic' => 'test-topic',
            '--ack-deadline' => 120,
        ]);

        expect($exitCode)->toBe(0);
    });

    it('creates subscription with message ordering', function () {
        PubSub::shouldReceive('createSubscription')
            ->with('ordered-sub', 'test-topic', [
                'ackDeadlineSeconds' => 60,
                'enableMessageOrdering' => true,
            ])
            ->once();

        $exitCode = Artisan::call('pubsub:subscriptions:create', [
            'name' => 'ordered-sub',
            'topic' => 'test-topic',
            '--enable-ordering' => true,
        ]);

        expect($exitCode)->toBe(0);
    });

    it('creates subscription with dead letter topic', function () {
        config(['pubsub.project_id' => 'test-project']);

        PubSub::shouldReceive('createTopic')
            ->with('test-topic-dead-letter')
            ->once();

        PubSub::shouldReceive('createSubscription')
            ->with('test-sub', 'test-topic', Mockery::on(function ($config) {
                return isset($config['deadLetterPolicy'])
                    && $config['deadLetterPolicy']['maxDeliveryAttempts'] === 5
                    && str_contains($config['deadLetterPolicy']['deadLetterTopic'], 'test-topic-dead-letter');
            }))
            ->once();

        $exitCode = Artisan::call('pubsub:subscriptions:create', [
            'name' => 'test-sub',
            'topic' => 'test-topic',
            '--dead-letter' => true,
        ]);

        expect($exitCode)->toBe(0);
    });

    it('handles invalid arguments', function () {
        $exitCode = Artisan::call('pubsub:subscriptions:create', [
            'name' => null,
            'topic' => 'test-topic',
        ]);

        expect($exitCode)->toBe(1);
    });

    it('handles creation errors', function () {
        PubSub::shouldReceive('createSubscription')
            ->andThrow(new Exception('Permission denied'));

        $exitCode = Artisan::call('pubsub:subscriptions:create', [
            'name' => 'test-sub',
            'topic' => 'test-topic',
        ]);

        expect($exitCode)->toBe(1);
        expect(Artisan::output())->toContain('Failed to create subscription');
    });
});

describe('CreatePushSubscriptionCommand', function () {
    it('creates a push subscription successfully', function () {
        PubSub::shouldReceive('createSubscription')
            ->with('push-sub', 'test-topic', Mockery::on(function ($options) {
                return isset($options['pushConfig']['pushEndpoint'])
                    && $options['pushConfig']['pushEndpoint'] === 'https://example.com/webhook'
                    && $options['ackDeadlineSeconds'] === 60;
            }))
            ->once();

        $exitCode = Artisan::call('pubsub:subscriptions:create-push', [
            'name' => 'push-sub',
            'topic' => 'test-topic',
            'endpoint' => 'https://example.com/webhook',
        ]);

        expect($exitCode)->toBe(0);
        expect(Artisan::output())->toContain('created successfully');
    });

    it('creates push subscription with auth token', function () {
        PubSub::shouldReceive('createSubscription')
            ->with('push-sub', 'test-topic', Mockery::on(function ($options) {
                return isset($options['pushConfig']['attributes']['x-goog-subscription-authorization'])
                    && str_contains($options['pushConfig']['attributes']['x-goog-subscription-authorization'], 'Bearer secret-token');
            }))
            ->once();

        $exitCode = Artisan::call('pubsub:subscriptions:create-push', [
            'name' => 'push-sub',
            'topic' => 'test-topic',
            'endpoint' => 'https://example.com/webhook',
            '--token' => 'secret-token',
        ]);

        expect($exitCode)->toBe(0);
        expect(Artisan::output())->toContain('Bearer token configured');
    });

    it('creates push subscription with dead letter policy', function () {
        config(['pubsub.project_id' => 'test-project']);

        PubSub::shouldReceive('createTopic')
            ->with('test-topic-dead-letter')
            ->once();

        PubSub::shouldReceive('createSubscription')
            ->with('push-sub', 'test-topic', Mockery::on(function ($options) {
                return isset($options['deadLetterPolicy']);
            }))
            ->once();

        $exitCode = Artisan::call('pubsub:subscriptions:create-push', [
            'name' => 'push-sub',
            'topic' => 'test-topic',
            'endpoint' => 'https://example.com/webhook',
            '--dead-letter' => true,
        ]);

        expect($exitCode)->toBe(0);
    });

    it('handles invalid endpoint', function () {
        $exitCode = Artisan::call('pubsub:subscriptions:create-push', [
            'name' => 'push-sub',
            'topic' => 'test-topic',
            'endpoint' => null,
        ]);

        expect($exitCode)->toBe(1);
    });
});

describe('ListTopicsCommand', function () {
    it('lists all topics', function () {
        $topic1 = Mockery::mock(Topic::class);
        $topic1->shouldReceive('name')->andReturn('topic-1');

        $topic2 = Mockery::mock(Topic::class);
        $topic2->shouldReceive('name')->andReturn('topic-2');

        PubSub::shouldReceive('topics')
            ->andReturn([$topic1, $topic2])
            ->once();

        $exitCode = Artisan::call('pubsub:topics:list');

        expect($exitCode)->toBe(0);
        expect(Artisan::output())
            ->toContain('topic-1')
            ->toContain('topic-2');
    });

    it('displays message when no topics found', function () {
        PubSub::shouldReceive('topics')
            ->andReturn([])
            ->once();

        $exitCode = Artisan::call('pubsub:topics:list');

        expect($exitCode)->toBe(0);
        expect(Artisan::output())->toContain('No topics found');
    });

    it('handles errors gracefully', function () {
        PubSub::shouldReceive('topics')
            ->andThrow(new Exception('API error'));

        $exitCode = Artisan::call('pubsub:topics:list');

        expect($exitCode)->toBe(1);
        expect(Artisan::output())->toContain('Failed to list topics');
    });
});

describe('ListSubscriptionsCommand', function () {
    it('lists all subscriptions', function () {
        $sub1 = Mockery::mock(Subscription::class);
        $sub1->shouldReceive('name')->andReturn('subscription-1');
        $sub1->shouldReceive('info')->andReturn([
            'topic' => 'projects/test/topics/topic-1',
            'ackDeadlineSeconds' => 60,
        ]);

        $sub2 = Mockery::mock(Subscription::class);
        $sub2->shouldReceive('name')->andReturn('subscription-2');
        $sub2->shouldReceive('info')->andReturn([
            'topic' => 'projects/test/topics/topic-2',
            'ackDeadlineSeconds' => 120,
        ]);

        PubSub::shouldReceive('subscriptions')
            ->andReturn([$sub1, $sub2])
            ->once();

        $exitCode = Artisan::call('pubsub:subscriptions:list');

        expect($exitCode)->toBe(0);
        expect(Artisan::output())
            ->toContain('subscription-1')
            ->toContain('subscription-2')
            ->toContain('topic-1')
            ->toContain('60');
    });

    it('filters subscriptions by topic', function () {
        $sub1 = Mockery::mock(Subscription::class);
        $sub1->shouldReceive('name')->andReturn('subscription-1');
        $sub1->shouldReceive('info')->andReturn([
            'topic' => 'projects/test/topics/filtered-topic',
            'ackDeadlineSeconds' => 60,
        ]);

        $sub2 = Mockery::mock(Subscription::class);
        $sub2->shouldReceive('name')->andReturn('subscription-2');
        $sub2->shouldReceive('info')->andReturn([
            'topic' => 'projects/test/topics/other-topic',
            'ackDeadlineSeconds' => 120,
        ]);

        PubSub::shouldReceive('subscriptions')
            ->andReturn([$sub1, $sub2])
            ->once();

        $exitCode = Artisan::call('pubsub:subscriptions:list', [
            '--topic' => 'filtered-topic',
        ]);

        expect($exitCode)->toBe(0);
        expect(Artisan::output())
            ->toContain('subscription-1')
            ->not->toContain('subscription-2');
    });

    it('displays message when no subscriptions found', function () {
        PubSub::shouldReceive('subscriptions')
            ->andReturn([])
            ->once();

        $exitCode = Artisan::call('pubsub:subscriptions:list');

        expect($exitCode)->toBe(0);
        expect(Artisan::output())->toContain('No subscriptions found');
    });
});

describe('PublishCommand', function () {
    it('publishes a message successfully', function () {
        PubSub::shouldReceive('publish')
            ->with('test-topic', ['test' => 'data'], [], [])
            ->andReturn('msg-123')
            ->once();

        $exitCode = Artisan::call('pubsub:publish', [
            'topic' => 'test-topic',
            'message' => '{"test":"data"}',
        ]);

        expect($exitCode)->toBe(0);
        expect(Artisan::output())
            ->toContain('published successfully')
            ->toContain('msg-123');
    });

    it('publishes a plain text message', function () {
        PubSub::shouldReceive('publish')
            ->with('test-topic', 'plain text message', [], [])
            ->andReturn('msg-456')
            ->once();

        $exitCode = Artisan::call('pubsub:publish', [
            'topic' => 'test-topic',
            'message' => 'plain text message',
        ]);

        expect($exitCode)->toBe(0);
    });

    it('publishes with attributes', function () {
        PubSub::shouldReceive('publish')
            ->with('test-topic', ['test' => 'data'], [
                'priority' => 'high',
                'source' => 'cli',
            ], [])
            ->andReturn('msg-789')
            ->once();

        $exitCode = Artisan::call('pubsub:publish', [
            'topic' => 'test-topic',
            'message' => '{"test":"data"}',
            '--attributes' => ['priority:high', 'source:cli'],
        ]);

        expect($exitCode)->toBe(0);
    });

    it('publishes with ordering key', function () {
        PubSub::shouldReceive('publish')
            ->with('test-topic', 'ordered message', [], [
                'ordering_key' => 'user-123',
            ])
            ->andReturn('msg-ordered')
            ->once();

        $exitCode = Artisan::call('pubsub:publish', [
            'topic' => 'test-topic',
            'message' => 'ordered message',
            '--ordering-key' => 'user-123',
        ]);

        expect($exitCode)->toBe(0);
    });

    it('handles publish errors', function () {
        PubSub::shouldReceive('publish')
            ->andThrow(new Exception('Network error'));

        $exitCode = Artisan::call('pubsub:publish', [
            'topic' => 'test-topic',
            'message' => 'test',
        ]);

        expect($exitCode)->toBe(1);
        expect(Artisan::output())->toContain('Failed to publish message');
    });

    it('validates required arguments', function () {
        $exitCode = Artisan::call('pubsub:publish', [
            'topic' => null,
            'message' => 'test',
        ]);

        expect($exitCode)->toBe(1);
    });
});

describe('ListenCommand', function () {
    it('starts listening on a subscription', function () {
        $subscriber = Mockery::mock(OffloadProject\GooglePubSub\Subscriber\Subscriber::class);

        $subscriber->shouldReceive('handler')
            ->with(Mockery::type('callable'))
            ->andReturnSelf()
            ->once();

        $subscriber->shouldReceive('onError')
            ->with(Mockery::type('callable'))
            ->andReturnSelf()
            ->once();

        $subscriber->shouldReceive('listen')
            ->with(['max_messages' => 100])
            ->once();

        PubSub::shouldReceive('subscribe')
            ->with('test-subscription', null)
            ->andReturn($subscriber)
            ->once();

        $exitCode = Artisan::call('pubsub:listen', [
            'subscription' => 'test-subscription',
        ]);

        expect($exitCode)->toBe(0);
    });

    it('creates subscription with topic if provided', function () {
        $subscriber = Mockery::mock(OffloadProject\GooglePubSub\Subscriber\Subscriber::class);

        $subscriber->shouldReceive('handler')->andReturnSelf();
        $subscriber->shouldReceive('onError')->andReturnSelf();
        $subscriber->shouldReceive('listen')->once();

        PubSub::shouldReceive('subscribe')
            ->with('new-subscription', 'test-topic')
            ->andReturn($subscriber)
            ->once();

        $exitCode = Artisan::call('pubsub:listen', [
            'subscription' => 'new-subscription',
            '--topic' => 'test-topic',
        ]);

        expect($exitCode)->toBe(0);
    });

    it('respects max-messages option', function () {
        $subscriber = Mockery::mock(OffloadProject\GooglePubSub\Subscriber\Subscriber::class);

        $subscriber->shouldReceive('handler')->andReturnSelf();
        $subscriber->shouldReceive('onError')->andReturnSelf();
        $subscriber->shouldReceive('listen')
            ->with(['max_messages' => 50])
            ->once();

        PubSub::shouldReceive('subscribe')
            ->andReturn($subscriber);

        $exitCode = Artisan::call('pubsub:listen', [
            'subscription' => 'test-subscription',
            '--max-messages' => 50,
        ]);

        expect($exitCode)->toBe(0);
    });

    it('handles listen errors', function () {
        PubSub::shouldReceive('subscribe')
            ->andThrow(new Exception('Subscription not found'));

        $exitCode = Artisan::call('pubsub:listen', [
            'subscription' => 'missing-subscription',
        ]);

        expect($exitCode)->toBe(1);
        expect(Artisan::output())->toContain('Failed to start listener');
    });

    it('validates subscription argument', function () {
        $exitCode = Artisan::call('pubsub:listen', [
            'subscription' => null,
        ]);

        expect($exitCode)->toBe(1);
    });
});

describe('ValidateSchemaCommand', function () {
    beforeEach(function () {
        config([
            'pubsub.schemas.test_schema' => [
                'schema' => [
                    'type' => 'object',
                    'required' => ['name'],
                    'properties' => [
                        'name' => ['type' => 'string'],
                    ],
                ],
            ],
        ]);
    });

    it('validates valid JSON data', function () {
        $exitCode = Artisan::call('pubsub:schema:validate', [
            'schema' => 'test_schema',
            'data' => '{"name":"John"}',
        ]);

        expect($exitCode)->toBe(0);
        expect(Artisan::output())->toContain('Data is valid');
    });

    it('rejects invalid JSON data', function () {
        $exitCode = Artisan::call('pubsub:schema:validate', [
            'schema' => 'test_schema',
            'data' => '{"age":30}', // Missing required 'name'
        ]);

        expect($exitCode)->toBe(1);
        expect(Artisan::output())->toContain('Validation failed');
    });

    it('handles invalid JSON input', function () {
        $exitCode = Artisan::call('pubsub:schema:validate', [
            'schema' => 'test_schema',
            'data' => 'invalid json',
        ]);

        expect($exitCode)->toBe(1);
        expect(Artisan::output())->toContain('Invalid JSON');
    });

    it('handles missing data argument', function () {
        $exitCode = Artisan::call('pubsub:schema:validate', [
            'schema' => 'test_schema',
        ]);

        expect($exitCode)->toBe(1);
        // In CI, stdin might return empty string which causes JSON parse error
        // In local, it might properly detect no data
        $output = Artisan::output();
        $hasExpectedError = str_contains($output, 'No data provided') ||
            str_contains($output, 'Invalid JSON');
        expect($hasExpectedError)->toBeTrue();
    });

    it('validates schema name is string', function () {
        $exitCode = Artisan::call('pubsub:schema:validate', [
            'schema' => null,
            'data' => '{"name":"test"}',
        ]);

        expect($exitCode)->toBe(1);
    });

    it('displays validation errors', function () {
        config([
            'pubsub.schemas.strict_schema' => [
                'schema' => [
                    'type' => 'object',
                    'required' => ['name', 'email'],
                    'properties' => [
                        'name' => ['type' => 'string'],
                        'email' => ['type' => 'string', 'format' => 'email'],
                    ],
                ],
            ],
        ]);

        $exitCode = Artisan::call('pubsub:schema:validate', [
            'schema' => 'strict_schema',
            'data' => '{"name":"John"}',
        ]);

        expect($exitCode)->toBe(1);
        expect(Artisan::output())
            ->toContain('Validation failed')
            ->toContain('Errors:');
    });
});
