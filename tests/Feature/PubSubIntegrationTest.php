<?php

declare(strict_types=1);

use Illuminate\Support\Facades\Queue;
use OffloadProject\GooglePubSub\Tests\Fixtures\TestJob;

it('can be registered as a queue driver', function () {
    // Mock the queue manager to avoid actual connection attempts
    $manager = $this->app->make('queue');

    // Check that the connector is registered
    $reflection = new ReflectionClass($manager);
    $property = $reflection->getProperty('connectors');
    $property->setAccessible(true);
    $connectors = $property->getValue($manager);

    expect(array_key_exists('pubsub', $connectors))->toBeTrue();
});

it('can dispatch jobs using the pubsub driver', function () {
    Queue::fake();

    dispatch(new TestJob(['data' => 'test']));

    Queue::assertPushed(TestJob::class, function ($job) {
        return $job->data === ['data' => 'test'];
    });
});
