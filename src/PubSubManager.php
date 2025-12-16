<?php

declare(strict_types=1);

namespace OffloadProject\GooglePubSub;

use Closure;
use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\Subscription;
use Google\Cloud\PubSub\Topic;
use Illuminate\Contracts\Foundation\Application;
use OffloadProject\GooglePubSub\Exceptions\PubSubException;
use OffloadProject\GooglePubSub\Publisher\Publisher;
use OffloadProject\GooglePubSub\Subscriber\StreamingSubscriber;
use OffloadProject\GooglePubSub\Subscriber\Subscriber;

class PubSubManager
{
    /**
     * The application instance resolver.
     */
    private Closure|Application $appResolver;

    /**
     * The PubSub client instance.
     */
    private ?PubSubClient $client = null;

    /**
     * The Publisher instance.
     */
    private ?Publisher $publisher = null;

    /**
     * The array of resolved subscribers.
     *
     * @var array<string, Subscriber>
     */
    private array $subscribers = [];

    /**
     * Create a new PubSub manager instance.
     */
    public function __construct(Closure|Application $appResolver)
    {
        $this->appResolver = $appResolver;
    }

    /**
     * Get the PubSub client instance.
     */
    public function client(): PubSubClient
    {
        if ($this->client === null) {
            $this->client = $this->createClient();
        }

        return $this->client;
    }

    /**
     * Get the publisher instance.
     */
    public function publisher(): Publisher
    {
        if (! $this->publisher) {
            $app = $this->getApplication();
            $config = $app->make('config');
            $pubsubConfig = $config->get('pubsub', []);

            // Include app name in config for metadata
            $pubsubConfig['app_name'] = $config->get('app.name', 'laravel');

            $this->publisher = new Publisher(
                $this->client(),
                $pubsubConfig
            );
        }

        return $this->publisher;
    }

    /**
     * Create a subscriber instance.
     */
    public function subscriber(string $subscriptionName, ?string $topic = null): Subscriber
    {
        if (! isset($this->subscribers[$subscriptionName])) {
            $app = $this->getApplication();
            $config = $app->make('config')->get('pubsub', []);

            // Use StreamingSubscriber if configured
            if ($config['use_streaming'] ?? true) {
                $this->subscribers[$subscriptionName] = new StreamingSubscriber(
                    $this->client(),
                    $subscriptionName,
                    $topic,
                    $config
                );
            } else {
                $this->subscribers[$subscriptionName] = new Subscriber(
                    $this->client(),
                    $subscriptionName,
                    $topic,
                    $config
                );
            }
        }

        return $this->subscribers[$subscriptionName];
    }

    /**
     * Publish a message to a topic.
     *
     * @param  array<string, mixed>  $attributes
     * @param  array<string, mixed>  $options
     * @return string Message ID
     */
    public function publish(string $topic, mixed $data, array $attributes = [], array $options = []): string
    {
        return $this->publisher()->publish($topic, $data, $attributes, $options);
    }

    /**
     * Subscribe to a topic.
     */
    public function subscribe(string $subscription, ?string $topic = null): Subscriber
    {
        return $this->subscriber($subscription, $topic);
    }

    /**
     * Create a topic if it doesn't exist.
     *
     * @param  array<string, mixed>  $options
     */
    public function createTopic(string $topicName, array $options = []): void
    {
        $topic = $this->client()->topic($topicName);

        if (! $topic->exists()) {
            $topic->create($options);
        }
    }

    /**
     * Create a subscription if it doesn't exist.
     *
     * @param  array<string, mixed>  $options
     */
    public function createSubscription(string $subscriptionName, string $topicName, array $options = []): void
    {
        $subscription = $this->client()->subscription($subscriptionName);

        if (! $subscription->exists()) {
            $topic = $this->client()->topic($topicName);
            $topic->subscribe($subscriptionName, $options);
        }
    }

    /**
     * List all topics.
     *
     * @return array<int, Topic>
     */
    public function topics(): array
    {
        return iterator_to_array($this->client()->topics());
    }

    /**
     * List all subscriptions.
     *
     * @return array<int, Subscription>
     */
    public function subscriptions(): array
    {
        return iterator_to_array($this->client()->subscriptions());
    }

    /**
     * Get the application instance.
     */
    private function getApplication(): Application
    {
        return is_callable($this->appResolver) ? call_user_func($this->appResolver) : $this->appResolver;
    }

    /**
     * Create a new Pub/Sub client.
     */
    private function createClient(): PubSubClient
    {
        $app = $this->getApplication();
        $config = $app->make('config')->get('pubsub', []);

        $pubsubConfig = [
            'projectId' => $config['project_id'] ?? null,
        ];

        // Check for emulator
        if ($emulatorHost = $config['emulator_host'] ?? $_SERVER['PUBSUB_EMULATOR_HOST'] ?? null) {
            $pubsubConfig['emulatorHost'] = $emulatorHost;
        }

        if (empty($pubsubConfig['projectId'])) {
            throw new PubSubException('Google Cloud project ID is required');
        }

        $authMethod = $config['auth_method'] ?? 'application_default';

        if ($authMethod === 'key_file' && ! isset($pubsubConfig['emulatorHost'])) {
            $keyFile = $config['key_file'] ?? null;

            if (empty($keyFile)) {
                throw new PubSubException('Key file path is required when using key_file auth method');
            }

            if (! file_exists($keyFile)) {
                throw new PubSubException("Key file not found: {$keyFile}");
            }

            $pubsubConfig['keyFilePath'] = $keyFile;
        }

        return new PubSubClient($pubsubConfig);
    }
}
