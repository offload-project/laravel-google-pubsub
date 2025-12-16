<?php

declare(strict_types=1);

namespace OffloadProject\GooglePubSub\Console\Commands;

use Exception;
use Illuminate\Console\Command;
use OffloadProject\GooglePubSub\Facades\PubSub;

final class ListSubscriptionsCommand extends Command
{
    protected $signature = 'pubsub:subscriptions:list {--topic= : Filter by topic}';

    protected $description = 'List all Pub/Sub subscriptions';

    public function handle(): int
    {
        $this->info('Fetching Pub/Sub subscriptions...');

        try {
            $subscriptions = PubSub::subscriptions();
            $topicFilter = $this->option('topic');

            if (is_string($topicFilter) && $topicFilter !== '') {
                $subscriptions = array_filter($subscriptions, function ($sub) use ($topicFilter) {
                    $topicInfo = $sub->info()['topic'] ?? '';

                    return is_string($topicInfo) && str_contains($topicInfo, $topicFilter);
                });
            }

            if (empty($subscriptions)) {
                $this->warn('No subscriptions found.');

                return Command::SUCCESS;
            }

            $rows = [];
            foreach ($subscriptions as $subscription) {
                $info = $subscription->info();
                $rows[] = [
                    $subscription->name(),
                    basename($info['topic'] ?? 'N/A'),
                    $info['ackDeadlineSeconds'] ?? 'N/A',
                ];
            }

            $this->table(['Subscription', 'Topic', 'Ack Deadline'], $rows);

            return Command::SUCCESS;
        } catch (Exception $e) {
            $this->error('Failed to list subscriptions: '.$e->getMessage());

            return Command::FAILURE;
        }
    }
}
