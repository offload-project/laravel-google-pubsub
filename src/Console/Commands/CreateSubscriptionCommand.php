<?php

declare(strict_types=1);

namespace OffloadProject\GooglePubSub\Console\Commands;

use Exception;
use Illuminate\Console\Command;
use OffloadProject\GooglePubSub\Facades\PubSub;

final class CreateSubscriptionCommand extends Command
{
    protected $signature = 'pubsub:subscriptions:create
                            {name : Subscription name}
                            {topic : Topic name}
                            {--ack-deadline=60 : Acknowledgment deadline in seconds}
                            {--enable-ordering : Enable message ordering}
                            {--dead-letter : Enable dead letter topic}';

    protected $description = 'Create a new Pub/Sub subscription';

    public function handle(): int
    {
        $nameArg = $this->argument('name');
        $topicArg = $this->argument('topic');

        if (! is_string($nameArg) || ! is_string($topicArg)) {
            $this->error('Invalid arguments provided');

            return Command::FAILURE;
        }

        $name = $nameArg;
        $topic = $topicArg;

        $this->info("Creating subscription '{$name}' for topic '{$topic}'...");

        try {
            $options = [
                'ackDeadlineSeconds' => (int) $this->option('ack-deadline'),
            ];

            if ($this->option('enable-ordering')) {
                $options['enableMessageOrdering'] = true;
            }

            if ($this->option('dead-letter')) {
                $deadLetterTopic = $topic.'-dead-letter';
                PubSub::createTopic($deadLetterTopic);

                $projectId = $this->getProjectId();
                $options['deadLetterPolicy'] = [
                    'deadLetterTopic' => "projects/{$projectId}/topics/{$deadLetterTopic}",
                    'maxDeliveryAttempts' => 5,
                ];
            }

            PubSub::createSubscription($name, $topic, $options);

            $this->info("✓ Subscription '{$name}' created successfully!");

            return Command::SUCCESS;
        } catch (Exception $e) {
            $this->error('Failed to create subscription: '.$e->getMessage());

            return Command::FAILURE;
        }
    }

    protected function getProjectId(): string
    {
        $projectId = config('pubsub.project_id');

        return is_string($projectId) ? $projectId : '';
    }
}
