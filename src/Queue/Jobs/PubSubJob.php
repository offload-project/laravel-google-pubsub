<?php

declare(strict_types=1);

namespace OffloadProject\GooglePubSub\Queue\Jobs;

use DateTimeInterface;
use Google\Cloud\PubSub\Message;
use Google\Cloud\PubSub\Subscription;
use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Job as JobContract;
use Illuminate\Queue\Jobs\Job;
use Illuminate\Support\Arr;
use OffloadProject\GooglePubSub\Exceptions\MessageConversionException;
use OffloadProject\GooglePubSub\Queue\PubSubQueue;

final class PubSubJob extends Job implements JobContract
{
    /**
     * The Pub/Sub message instance.
     */
    protected Message $message;

    /**
     * The Pub/Sub subscription instance.
     */
    protected Subscription $subscription;

    /**
     * The Pub/Sub queue instance.
     */
    protected PubSubQueue $pubsubQueue;

    /**
     * Create a new job instance.
     */
    public function __construct(
        Container $container,
        PubSubQueue $pubsubQueue,
        Message $message,
        Subscription $subscription,
        string $connectionName,
        string $queue
    ) {
        $this->container = $container;
        $this->pubsubQueue = $pubsubQueue;
        $this->message = $message;
        $this->subscription = $subscription;
        $this->connectionName = $connectionName;
        $this->queue = $queue;
    }

    /**
     * Get the job identifier.
     *
     * @return string
     */
    public function getJobId()
    {
        return $this->message->id();
    }

    /**
     * Get the raw body of the job.
     *
     * @return string
     */
    public function getRawBody()
    {
        $data = $this->message->data();
        $attributes = $this->message->attributes();

        // Decompress if needed
        if (($attributes['compressed'] ?? false) === 'true') {
            $data = gzuncompress($data);
            if ($data === false) {
                throw new MessageConversionException('Failed to decompress message data');
            }
        }

        return $data;
    }

    /**
     * Delete the job from the queue.
     */
    public function delete(): void
    {
        parent::delete();

        $this->subscription->acknowledge($this->message);
    }

    /**
     * Release the job back into the queue.
     *
     * @param  int  $delay
     * @return void
     */
    public function release($delay = 0)
    {
        parent::release($delay);

        // Modify the ack deadline to release the message back to the queue
        if ($delay > 0) {
            $this->subscription->modifyAckDeadline($this->message, $delay);
        } else {
            // Immediately make available by setting deadline to 0
            $this->subscription->modifyAckDeadline($this->message, 0);
        }
    }

    /**
     * Get the number of times the job has been attempted.
     *
     * @return int
     */
    public function attempts()
    {
        $attributes = $this->message->attributes();

        // First check if Laravel has tracked attempts in the payload
        $payload = $this->payload();
        if (isset($payload['attempts'])) {
            return $payload['attempts'];
        }

        // Fall back to Pub/Sub delivery attempt
        return (int) ($attributes['delivery_attempt'] ?? 1);
    }

    /**
     * Get the message attributes.
     *
     * @return array<string, string>
     */
    public function getMessageAttributes(): array
    {
        return $this->message->attributes();
    }

    /**
     * Get the message publish time.
     */
    public function getPublishTime(): ?DateTimeInterface
    {
        $publishTime = $this->message->publishTime();

        if ($publishTime) {
            return $publishTime;
        }

        return null;
    }

    /**
     * Check if the message has an ordering key.
     */
    public function hasOrderingKey(): bool
    {
        return ! empty($this->message->orderingKey());
    }

    /**
     * Get the message ordering key.
     */
    public function getOrderingKey(): ?string
    {
        return $this->message->orderingKey() ?: null;
    }

    /**
     * Get the underlying Pub/Sub message.
     */
    public function getPubSubMessage(): Message
    {
        return $this->message;
    }

    /**
     * Get the name of the queued job class.
     *
     * @return string
     */
    public function getName()
    {
        return Arr::get($this->payload(), 'displayName', parent::getName());
    }
}
