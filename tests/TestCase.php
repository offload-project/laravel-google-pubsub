<?php

namespace OffloadProject\GooglePubSub\Tests;

use Orchestra\Testbench\TestCase as Orchestra;
use OffloadProject\GooglePubSub\PubSubServiceProvider;

abstract class TestCase extends Orchestra
{
    protected function getPackageProviders($app): array
    {
        return [
            PubSubServiceProvider::class,
        ];
    }

    protected function getEnvironmentSetUp($app): void
    {
        $app['config']->set('queue.default', 'pubsub');
        $app['config']->set('queue.connections.pubsub', [
            'driver' => 'pubsub',
            'project_id' => 'test-project',
            'queue' => 'default',
            'auth_method' => 'application_default',
        ]);

        $app['config']->set('pubsub.project_id', 'test-project');
        $app['config']->set('pubsub.default_queue', 'default');
    }
}
