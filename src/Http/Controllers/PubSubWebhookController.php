<?php

declare(strict_types=1);

namespace OffloadProject\GooglePubSub\Http\Controllers;

use Exception;
use Illuminate\Http\Request;
use Illuminate\Http\Response;
use Illuminate\Routing\Controller;
use Illuminate\Support\Facades\Log;
use OffloadProject\GooglePubSub\Events\PubSubEventSubscriber;
use OffloadProject\GooglePubSub\Formatters\JsonFormatter;
use OffloadProject\GooglePubSub\Messages\WebhookMessage;

final class PubSubWebhookController extends Controller
{
    /**
     * The event subscriber instance.
     */
    protected PubSubEventSubscriber $subscriber;

    /**
     * The message formatter.
     */
    protected JsonFormatter $formatter;

    /**
     * Create a new webhook controller.
     */
    public function __construct(PubSubEventSubscriber $subscriber)
    {
        $this->subscriber = $subscriber;
        $this->formatter = new JsonFormatter();
    }

    /**
     * Handle a Pub/Sub push request.
     */
    public function handle(Request $request, string $topic): Response
    {
        // Verify the request is from Pub/Sub
        if (! $this->verifyRequest($request)) {
            return response('Unauthorized', 401);
        }

        // Get the message from the request
        $message = $this->parseMessage($request);

        if (! $message) {
            return response('Bad Request', 400);
        }

        try {
            // Log the webhook if configured
            if (config('pubsub.monitoring.log_webhooks', false)) {
                Log::info('Received Pub/Sub webhook', [
                    'topic' => $topic,
                    'message_id' => $message['messageId'] ?? 'unknown',
                    'subscription' => $message['subscription'] ?? 'unknown',
                ]);
            }

            // Process the message
            $this->processMessage($message, $topic);

            // Acknowledge receipt
            return response('', 200);
        } catch (Exception $e) {
            Log::error('Failed to process Pub/Sub webhook', [
                'topic' => $topic,
                'error' => $e->getMessage(),
                'message_id' => $message['messageId'] ?? 'unknown',
            ]);

            // Return 200 anyway to prevent redelivery storm
            // The message will be redelivered based on subscription settings
            return response('', 200);
        }
    }

    /**
     * Verify the request is from Google Pub/Sub.
     */
    protected function verifyRequest(Request $request): bool
    {
        // Check for required headers
        if (! $request->hasHeader('X-Goog-Resource-State')) {
            return false;
        }

        // Verify Bearer token if configured
        $token = config('pubsub.webhook.auth_token');
        if ($token) {
            $authHeader = $request->header('Authorization');
            if ($authHeader !== "Bearer {$token}") {
                return false;
            }
        }

        // Additional verification can be added here
        // For production, consider implementing:
        // - IP allowlist for Google's services
        // - OIDC token verification
        // - Custom authentication headers

        return true;
    }

    /**
     * Parse the Pub/Sub message from the request.
     *
     * @return array<string, mixed>|null
     */
    protected function parseMessage(Request $request): ?array
    {
        $content = $request->getContent();

        if (empty($content)) {
            return null;
        }

        $data = json_decode($content, true);

        if (json_last_error() !== JSON_ERROR_NONE || ! isset($data['message'])) {
            return null;
        }

        return $data['message'];
    }

    /**
     * Process the message.
     *
     * @param  array<string, mixed>  $messageData
     */
    protected function processMessage(array $messageData, string $topic): void
    {
        // Decode the message data
        $data = base64_decode($messageData['data'] ?? '');
        $attributes = $messageData['attributes'] ?? [];

        // Parse the data
        $parsedData = $this->formatter->parse($data);

        // Create a mock Message object for compatibility
        $message = new WebhookMessage(
            $messageData['messageId'] ?? uniqid('', true),
            $data,
            $attributes,
            $messageData['publishTime'] ?? now()->toIso8601String()
        );

        // Use the subscriber to handle the message
        $this->subscriber->handleMessage($parsedData, $message, $topic);
    }
}
