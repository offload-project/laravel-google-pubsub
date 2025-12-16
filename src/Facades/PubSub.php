<?php

declare(strict_types=1);

namespace OffloadProject\GooglePubSub\Facades;

use Google\Cloud\PubSub\PubSubClient;
use Illuminate\Support\Facades\Facade;
use OffloadProject\GooglePubSub\Publisher\Publisher;
use OffloadProject\GooglePubSub\Subscriber\Subscriber;

/**
 * @method static string publish(string $topic, mixed $data, array<string, mixed> $attributes = [], array<string, mixed> $options = [])
 * @method static array<int, string> publishBatch(string $topic, array<int, array<string, mixed> > $messages, array<string, mixed> $options = [])
 * @method static Subscriber subscribe(string $subscription, ?string $topic = null)
 * @method static Publisher publisher()
 * @method static Subscriber subscriber(string $subscriptionName, ?string $topic = null)
 * @method static void createTopic(string $topicName, array<string, mixed> $options = [])
 * @method static void createSubscription(string $subscriptionName, string $topicName, array<string, mixed> $options = [])
 * @method static array<int, \Google\Cloud\PubSub\Topic> topics()
 * @method static array<int, \Google\Cloud\PubSub\Subscription> subscriptions()
 * @method static PubSubClient client()
 *
 * @see \OffloadProject\GooglePubSub\PubSubManager
 */
final class PubSub extends Facade
{
    /**
     * Get the registered name of the component.
     */
    protected static function getFacadeAccessor(): string
    {
        return 'pubsub';
    }
}
