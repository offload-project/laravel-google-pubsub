<?php

declare(strict_types=1);

namespace OffloadProject\GooglePubSub\Subscriber;

use Exception;
use Google\Cloud\PubSub\Message;
use Google\Cloud\PubSub\Subscription;
use Illuminate\Support\Facades\Log;
use OffloadProject\GooglePubSub\Exceptions\SubscriptionException;

class StreamingSubscriber extends Subscriber
{
    /**
     * Default wait time in microseconds when no messages are available.
     */
    protected const DEFAULT_EMPTY_WAIT_TIME = 100000; // 100ms

    /**
     * Use streaming pull to continuously receive messages.
     *
     * @param  array<string, mixed>  $options
     */
    public function stream(array $options = []): void
    {
        $this->registerSignalHandlers();

        try {
            $subscription = $this->getSubscription();

            $pullOptions = [
                'returnImmediately' => false,
                'maxMessages' => $options['max_messages_per_pull'] ?? $this->config['max_messages_per_pull'] ?? 100,
            ];

            $emptyWaitTime = ($options['empty_wait_time'] ?? $this->config['empty_wait_time'] ?? self::DEFAULT_EMPTY_WAIT_TIME / 1000) * 1000;

            Log::info("Started streaming pull on subscription: {$this->subscriptionName}");

            while (true) {
                // Pull messages
                $messages = $subscription->pull($pullOptions);

                if (! empty($messages)) {
                    foreach ($messages as $message) {
                        try {
                            $this->processStreamMessage($message, $subscription);
                        } catch (Exception $e) {
                            $this->handleError($e, $message);

                            // Optionally nack the message by setting ack deadline to 0
                            if ($this->config['nack_on_error'] ?? true) {
                                $subscription->modifyAckDeadline($message, 0);
                            }
                        }
                    }
                }

                // Check for stop signal after processing
                if ($this->shouldStop()) {
                    break;
                }

                // Small delay to prevent tight loop when no messages
                if (empty($messages)) {
                    usleep($emptyWaitTime);
                }
            }

            Log::info("Stopped streaming pull on subscription: {$this->subscriptionName}");
        } catch (Exception $e) {
            throw new SubscriptionException(
                "Streaming pull failed: {$e->getMessage()}",
                $e->getCode(),
                $e
            );
        }
    }

    /**
     * Listen using streaming pull (alias for stream).
     *
     * @param  array<string, mixed>  $options
     */
    public function listen(array $options = []): void
    {
        $this->stream($options);
    }

    /**
     * Configure flow control for pulling.
     */
    public function withFlowControl(int $maxMessages): self
    {
        $this->config['max_messages_per_pull'] = $maxMessages;

        return $this;
    }

    /**
     * Set the wait time between pulls when no messages.
     */
    public function withWaitTime(int $milliseconds): self
    {
        $this->config['empty_wait_time'] = $milliseconds;

        return $this;
    }

    /**
     * Process a message from the stream.
     */
    protected function processStreamMessage(Message $message, Subscription $subscription): void
    {
        $data = $this->decodeMessage($message);

        // Log if configured
        if ($this->config['monitoring']['log_consumed_messages'] ?? false) {
            Log::debug('Processing streamed message', [
                'subscription' => $this->subscriptionName,
                'message_id' => $message->id(),
                'publish_time' => $message->publishTime(),
            ]);
        }

        // Call all handlers
        foreach ($this->handlers as $handler) {
            $handler($data, $message);
        }

        // Acknowledge the message
        if ($this->config['auto_acknowledge'] ?? true) {
            $subscription->acknowledge($message);
        }
    }
}
