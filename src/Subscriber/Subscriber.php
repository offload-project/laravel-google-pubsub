<?php

declare(strict_types=1);

namespace OffloadProject\GooglePubSub\Subscriber;

use Closure;
use Exception;
use Google\Cloud\PubSub\Message;
use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\Subscription;
use Illuminate\Support\Facades\Log;
use OffloadProject\GooglePubSub\Concerns\ConfiguresSubscriptions;
use OffloadProject\GooglePubSub\Concerns\HandlesSignals;
use OffloadProject\GooglePubSub\Contracts\MessageFormatter;
use OffloadProject\GooglePubSub\Exceptions\SubscriptionException;
use OffloadProject\GooglePubSub\Formatters\JsonFormatter;

class Subscriber
{
    use ConfiguresSubscriptions;
    use HandlesSignals;

    /**
     * The PubSub client instance.
     */
    protected PubSubClient $client;

    /**
     * The subscription name.
     */
    protected string $subscriptionName;

    /**
     * The topic name.
     */
    protected ?string $topicName;

    /**
     * The configuration array.
     *
     * @var array<string, mixed>
     */
    protected array $config;

    /**
     * The message formatter.
     */
    protected MessageFormatter $formatter;

    /**
     * The subscription instance.
     */
    protected ?Subscription $subscription = null;

    /**
     * Message handler callbacks.
     *
     * @var array<int, callable>
     */
    protected array $handlers = [];

    /**
     * Error handler callback.
     *
     * @var (Closure(Exception, Message|null): void)|null
     */
    protected ?Closure $errorHandler = null;

    /**
     * Create a new subscriber instance.
     *
     * @param  array<string, mixed>  $config
     */
    public function __construct(
        PubSubClient $client,
        string $subscriptionName,
        ?string $topicName = null,
        array $config = []
    ) {
        $this->client = $client;
        $this->subscriptionName = $subscriptionName;
        $this->topicName = $topicName;
        $this->config = $config;
        $this->formatter = new JsonFormatter();
    }

    /**
     * Set the topic for this subscription.
     */
    public function topic(string $topicName): self
    {
        $this->topicName = $topicName;

        return $this;
    }

    /**
     * Add a message handler.
     */
    public function handler(callable $handler): self
    {
        $this->handlers[] = $handler;

        return $this;
    }

    /**
     * Set the error handler.
     */
    public function onError(callable $handler): self
    {
        $this->errorHandler = $handler instanceof Closure ? $handler : Closure::fromCallable($handler);

        return $this;
    }

    /**
     * Pull messages from the subscription.
     *
     * @return array<int, mixed>
     */
    public function pull(int $maxMessages = 10): array
    {
        try {
            $subscription = $this->getSubscription();

            $messages = $subscription->pull([
                'maxMessages' => $maxMessages,
                'returnImmediately' => true,
            ]);

            $results = [];

            foreach ($messages as $message) {
                try {
                    $results[] = $this->processMessage($message);
                } catch (Exception $e) {
                    $this->handleError($e, $message);
                }
            }

            return $results;
        } catch (Exception $e) {
            throw new SubscriptionException(
                "Failed to pull messages: {$e->getMessage()}",
                $e->getCode(),
                $e
            );
        }
    }

    /**
     * Continuously pull and process messages.
     *
     * @param  array<string, mixed>  $options
     */
    public function listen(array $options = []): void
    {
        $this->registerSignalHandlers();

        $maxMessages = $options['max_messages'] ?? $this->config['max_messages'] ?? 10;
        $waitTime = $options['wait_time'] ?? $this->config['wait_time'] ?? 1;

        Log::info("Started listening on subscription: {$this->subscriptionName}");

        while (! $this->shouldStop()) {
            try {
                $messages = $this->pull($maxMessages);

                if (empty($messages) && $waitTime > 0) {
                    sleep($waitTime);
                }
            } catch (Exception $e) {
                $this->handleError($e);

                // Wait before retrying
                sleep($waitTime);
            }
        }

        Log::info("Stopped listening on subscription: {$this->subscriptionName}");
    }

    /**
     * Acknowledge a message.
     */
    public function acknowledge(Message $message): void
    {
        $this->getSubscription()->acknowledge($message);

        if ($this->config['monitoring']['log_consumed_messages'] ?? false) {
            Log::info('Acknowledged Pub/Sub message', [
                'subscription' => $this->subscriptionName,
                'message_id' => $message->id(),
            ]);
        }
    }

    /**
     * Acknowledge multiple messages.
     *
     * @param  array<int, Message>  $messages
     */
    public function acknowledgeBatch(array $messages): void
    {
        $this->getSubscription()->acknowledgeBatch($messages);
    }

    /**
     * Modify the acknowledgment deadline.
     */
    public function modifyAckDeadline(Message $message, int $seconds): void
    {
        $this->getSubscription()->modifyAckDeadline($message, $seconds);
    }

    /**
     * Get or create the subscription instance.
     */
    protected function getSubscription(): Subscription
    {
        if (! $this->subscription) {
            $subscription = $this->client->subscription($this->subscriptionName);

            // Auto-create subscription if configured
            if (($this->config['auto_create_subscriptions'] ?? true) && ! $subscription->exists()) {
                if (! $this->topicName) {
                    throw new SubscriptionException(
                        "Cannot create subscription '{$this->subscriptionName}' without a topic name"
                    );
                }

                $topic = $this->client->topic($this->topicName);

                if (! $topic->exists() && ($this->config['auto_create_topics'] ?? true)) {
                    $topic->create();
                }

                $subscriptionConfig = $this->getSubscriptionConfig();
                $subscription = $topic->subscribe($this->subscriptionName, $subscriptionConfig);

                Log::info("Created Pub/Sub subscription: {$this->subscriptionName}");
            }

            $this->subscription = $subscription;
        }

        return $this->subscription;
    }

    /**
     * Process a single message.
     */
    protected function processMessage(Message $message): mixed
    {
        $data = $this->decodeMessage($message);

        // Log if configured
        if ($this->config['monitoring']['log_consumed_messages'] ?? false) {
            Log::info('Processing Pub/Sub message', [
                'subscription' => $this->subscriptionName,
                'message_id' => $message->id(),
                'attributes' => $message->attributes(),
            ]);
        }

        $result = null;

        // Call all handlers
        foreach ($this->handlers as $handler) {
            $result = $handler($data, $message);
        }

        // Auto-acknowledge if configured
        if ($this->config['auto_acknowledge'] ?? true) {
            $this->acknowledge($message);
        }

        return $result;
    }

    /**
     * Decode a message.
     */
    protected function decodeMessage(Message $message): mixed
    {
        $data = $message->data();
        $attributes = $message->attributes();

        // Decompress if needed
        if (($attributes['compressed'] ?? false) === 'true') {
            $data = gzuncompress($data);
            if ($data === false) {
                throw new SubscriptionException('Failed to decompress message data');
            }
        }

        // Parse the data
        return $this->formatter->parse($data);
    }

    /**
     * Handle an error.
     */
    protected function handleError(Exception $e, ?Message $message = null): void
    {
        if ($this->errorHandler) {
            ($this->errorHandler)($e, $message);
        } else {
            Log::error('Pub/Sub subscriber error', [
                'subscription' => $this->subscriptionName,
                'error' => $e->getMessage(),
                'message_id' => $message?->id(),
            ]);
        }
    }

    /**
     * Get subscription configuration.
     *
     * @return array<string, mixed>
     */
    protected function getSubscriptionConfig(): array
    {
        return $this->buildSubscriptionConfig($this->config, $this->topicName, $this->client);
    }
}
