<?php

declare(strict_types=1);

namespace OffloadProject\GooglePubSub\Console\Commands;

use Exception;
use Illuminate\Console\Command;
use OffloadProject\GooglePubSub\Facades\PubSub;

final class ListTopicsCommand extends Command
{
    protected $signature = 'pubsub:topics:list';

    protected $description = 'List all Pub/Sub topics';

    public function handle(): int
    {
        $this->info('Fetching Pub/Sub topics...');

        try {
            $topics = PubSub::topics();

            if (empty($topics)) {
                $this->warn('No topics found.');

                return Command::SUCCESS;
            }

            $this->table(['Topic Name'], array_map(fn ($topic) => [$topic->name()], $topics));

            return Command::SUCCESS;
        } catch (Exception $e) {
            $this->error('Failed to list topics: '.$e->getMessage());

            return Command::FAILURE;
        }
    }
}
