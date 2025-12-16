<?php

declare(strict_types=1);

namespace OffloadProject\GooglePubSub;

use Illuminate\Container\Container;
use Illuminate\Queue\QueueManager;
use Illuminate\Support\Facades\Route;
use Illuminate\Support\ServiceProvider;
use OffloadProject\GooglePubSub\Events\PubSubEventDispatcher;
use OffloadProject\GooglePubSub\Events\PubSubEventSubscriber;
use OffloadProject\GooglePubSub\Failed\PubSubFailedJobProvider;
use OffloadProject\GooglePubSub\Http\Controllers\PubSubWebhookController;
use OffloadProject\GooglePubSub\Http\Middleware\VerifyPubSubWebhook;
use OffloadProject\GooglePubSub\Queue\PubSubConnector;
use Override;

final class PubSubServiceProvider extends ServiceProvider
{
    /**
     * The console commands.
     *
     * @var array<int, class-string>
     */
    protected array $commands = [
        Console\Commands\ListTopicsCommand::class,
        Console\Commands\CreateTopicCommand::class,
        Console\Commands\ListSubscriptionsCommand::class,
        Console\Commands\CreateSubscriptionCommand::class,
        Console\Commands\CreatePushSubscriptionCommand::class,
        Console\Commands\ListenCommand::class,
        Console\Commands\PublishCommand::class,
        Console\Commands\ValidateSchemaCommand::class,
    ];

    /**
     * Bootstrap the application services.
     */
    public function boot(): void
    {
        if ($this->app->runningInConsole()) {
            $this->publishes([
                __DIR__.'/../config/pubsub.php' => config_path('pubsub.php'),
            ], 'pubsub-config');

            $this->commands($this->commands);
        }

        $this->registerWebhookRoutes();

        // Defer event integration registration until the application is fully booted
        // This prevents issues with deferred service providers and ensures all
        // config and dependencies are available
        $this->app->booted(function () {
            $this->registerEventIntegration();
        });
    }

    /**
     * Register the application services.
     */
    #[Override]
    public function register(): void
    {
        $this->mergeConfigFrom(
            __DIR__.'/../config/pubsub.php',
            'pubsub'
        );

        $this->registerPubSubManager();
        $this->registerPubSubConnector();
        $this->registerFailedJobProvider();
        $this->registerEventServices();
    }

    /**
     * Get the services provided by the provider.
     *
     * @return array<string>
     */
    #[Override]
    public function provides(): array
    {
        return [
            'pubsub',
            'queue.failed.pubsub',
            PubSubEventDispatcher::class,
            PubSubEventSubscriber::class,
        ];
    }

    /**
     * Register the Pub/Sub manager.
     */
    protected function registerPubSubManager(): void
    {
        $this->app->singleton('pubsub', function () {
            return new PubSubManager(fn () => Container::getInstance());
        });

        $this->app->alias('pubsub', PubSubManager::class);
    }

    /**
     * Register the Pub/Sub queue connector.
     */
    protected function registerPubSubConnector(): void
    {
        $this->app->resolving('queue', function (QueueManager $manager) {
            $manager->extend('pubsub', function () {
                return new PubSubConnector();
            });
        });
    }

    /**
     * Register the failed job provider for Pub/Sub.
     */
    protected function registerFailedJobProvider(): void
    {
        $this->app->singleton('queue.failed.pubsub', function ($app) {
            return new PubSubFailedJobProvider(
                $app['pubsub'],
                $app['config']['pubsub']
            );
        });
    }

    /**
     * Register event integration services.
     */
    protected function registerEventServices(): void
    {
        $this->app->singleton(PubSubEventDispatcher::class, function ($app) {
            return new PubSubEventDispatcher(
                $app['pubsub'],
                $app['events'],
                $app['config']['pubsub']
            );
        });

        $this->app->singleton(PubSubEventSubscriber::class, function ($app) {
            return new PubSubEventSubscriber(
                $app['pubsub'],
                $app['events'],
                $app['config']['pubsub']
            );
        });
    }

    /**
     * Register event integration.
     */
    protected function registerEventIntegration(): void
    {
        if (config('pubsub.events.enabled', false)) {
            // Register event dispatcher
            $this->app->make(PubSubEventDispatcher::class)->register();

            // Start event subscriber for configured topics
            if (config('pubsub.events.subscribe', true)) {
                $this->app->make(PubSubEventSubscriber::class)->subscribeToConfiguredTopics();
            }
        }
    }

    /**
     * Register webhook routes.
     */
    protected function registerWebhookRoutes(): void
    {
        if (! config('pubsub.webhook.enabled', true)) {
            return;
        }

        $prefix = config('pubsub.webhook.route_prefix', 'pubsub/webhook');
        $middleware = config('pubsub.webhook.middleware', [VerifyPubSubWebhook::class]);

        Route::group(compact('prefix', 'middleware'), function () {
            Route::post('{topic}', [PubSubWebhookController::class, 'handle'])
                ->name('pubsub.webhook');
        });
    }
}
