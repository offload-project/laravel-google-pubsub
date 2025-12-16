<?php

declare(strict_types=1);

namespace OffloadProject\GooglePubSub\Console\Commands;

use Exception;
use Illuminate\Console\Command;
use OffloadProject\GooglePubSub\Facades\PubSub;

final class ListenCommand extends Command
{
    protected $signature = 'pubsub:listen
                            {subscription : Subscription name}
                            {--topic= : Topic name (required if subscription doesn\'t exist)}
                            {--max-messages=100 : Maximum messages per pull}';

    protected $description = 'Listen for messages on a Pub/Sub subscription';

    public function handle(): int
    {
        $subscriptionArg = $this->argument('subscription');
        if (! is_string($subscriptionArg)) {
            $this->error('Subscription name must be a string');

            return Command::FAILURE;
        }
        $subscription = $subscriptionArg;

        $topicOption = $this->option('topic');
        $topic = is_string($topicOption) ? $topicOption : null;

        $this->info("Starting listener for subscription '{$subscription}'...");

        try {
            $subscriber = PubSub::subscribe($subscription, $topic);

            $subscriber->handler(function ($data, $message) {
                $messageId = $message->id();
                $this->info('Received message: '.$messageId);

                $jsonData = json_encode($data, JSON_PRETTY_PRINT);
                if ($jsonData !== false) {
                    $this->line($jsonData);
                }

                if ($attributes = $message->attributes()) {
                    $jsonAttributes = json_encode($attributes, JSON_PRETTY_PRINT);
                    if ($jsonAttributes !== false) {
                        $this->line('Attributes: '.$jsonAttributes);
                    }
                }
            });

            $subscriber->onError(function ($error, $message) {
                $this->error('Error processing message: '.$error->getMessage());
            });

            $this->info('Listening for messages... Press Ctrl+C to stop.');

            // Use listen() which works for both Subscriber types
            $subscriber->listen([
                'max_messages' => (int) $this->option('max-messages'),
            ]);

            return Command::SUCCESS;
        } catch (Exception $e) {
            $this->error('Failed to start listener: '.$e->getMessage());

            return Command::FAILURE;
        }
    }
}
