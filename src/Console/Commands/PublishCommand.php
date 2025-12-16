<?php

declare(strict_types=1);

namespace OffloadProject\GooglePubSub\Console\Commands;

use Exception;
use Illuminate\Console\Command;
use OffloadProject\GooglePubSub\Facades\PubSub;

final class PublishCommand extends Command
{
    protected $signature = 'pubsub:publish
                            {topic : Topic name}
                            {message : Message data (JSON)}
                            {--attributes=* : Message attributes in key:value format}
                            {--ordering-key= : Message ordering key}';

    protected $description = 'Publish a message to a Pub/Sub topic';

    public function handle(): int
    {
        $topicArg = $this->argument('topic');
        $messageDataArg = $this->argument('message');

        if (! is_string($topicArg) || ! is_string($messageDataArg)) {
            $this->error('Topic and message must be strings');

            return Command::FAILURE;
        }

        $topic = $topicArg;
        $messageData = $messageDataArg;

        try {
            // Parse message data
            $data = json_decode($messageData, true);
            if (json_last_error() !== JSON_ERROR_NONE) {
                $data = $messageData; // Use as string if not JSON
            }

            // Parse attributes
            $attributes = [];
            $attributesOption = $this->option('attributes');
            if (is_array($attributesOption)) {
                foreach ($attributesOption as $attr) {
                    if (is_string($attr) && str_contains($attr, ':')) {
                        [$key, $value] = explode(':', $attr, 2);
                        $attributes[$key] = $value;
                    }
                }
            }

            // Prepare options
            $options = [];
            $orderingKeyOption = $this->option('ordering-key');
            if (is_string($orderingKeyOption) && $orderingKeyOption !== '') {
                $options['ordering_key'] = $orderingKeyOption;
            }

            $this->info("Publishing message to topic '{$topic}'...");

            $messageId = PubSub::publish($topic, $data, $attributes, $options);

            $this->info('✓ Message published successfully!');
            $this->line("Message ID: {$messageId}");

            return Command::SUCCESS;
        } catch (Exception $e) {
            $this->error('Failed to publish message: '.$e->getMessage());

            return Command::FAILURE;
        }
    }
}
