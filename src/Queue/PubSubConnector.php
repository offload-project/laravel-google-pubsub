<?php

declare(strict_types=1);

namespace OffloadProject\GooglePubSub\Queue;

use Exception;
use Google\Cloud\PubSub\PubSubClient;
use Illuminate\Contracts\Queue\Queue;
use Illuminate\Queue\Connectors\ConnectorInterface;
use Illuminate\Support\Arr;
use OffloadProject\GooglePubSub\Exceptions\PubSubException;

final class PubSubConnector implements ConnectorInterface
{
    /**
     * Establish a queue connection.
     *
     * @param  array<string, mixed>  $config
     *
     * @throws PubSubException
     */
    public function connect(array $config): Queue
    {
        try {
            $pubsubConfig = $this->getPubSubConfig($config);
        } catch (PubSubException $e) {
            throw $e;
        }

        try {
            $client = new PubSubClient($pubsubConfig);
        } catch (Exception $e) {
            throw new PubSubException(
                "Failed to create Pub/Sub client: {$e->getMessage()}",
                $e->getCode(),
                $e
            );
        }

        return new PubSubQueue(
            $client,
            $config['queue'] ?? config('pubsub.default_queue'),
            Arr::except($config, ['driver', 'project_id', 'key_file'])
        );
    }

    /**
     * Get the Pub/Sub client configuration.
     *
     * @param  array<string, mixed>  $config
     * @return array<string, mixed>
     *
     * @throws PubSubException
     */
    private function getPubSubConfig(array $config): array
    {
        $projectId = $config['project_id'] ?? config('pubsub.project_id');

        if (empty($projectId)) {
            throw new PubSubException('Google Cloud project ID is required');
        }

        $pubsubConfig = compact('projectId');

        $authMethod = $config['auth_method'] ?? config('pubsub.auth_method', 'application_default');

        if ($authMethod === 'key_file') {
            $keyFile = $config['key_file'] ?? config('pubsub.key_file');

            if (empty($keyFile)) {
                throw new PubSubException('Key file path is required when using key_file auth method');
            }

            if (! file_exists($keyFile)) {
                throw new PubSubException("Key file not found: {$keyFile}");
            }

            $pubsubConfig['keyFilePath'] = $keyFile;
        }

        return $pubsubConfig;
    }
}
