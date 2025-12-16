<?php

declare(strict_types=1);

use Illuminate\Http\Request;
use OffloadProject\GooglePubSub\Events\PubSubEventSubscriber;
use OffloadProject\GooglePubSub\Http\Controllers\PubSubWebhookController;
use OffloadProject\GooglePubSub\Http\Middleware\VerifyPubSubWebhook;

describe('PubSubWebhookController', function () {
    beforeEach(function () {
        $this->subscriber = Mockery::mock(PubSubEventSubscriber::class);
        $this->controller = new PubSubWebhookController($this->subscriber);
    });

    it('handles valid webhook request', function () {
        $request = Request::create('/webhook/orders', 'POST', [], [], [], [
            'HTTP_X-Goog-Resource-State' => 'exists',
            'HTTP_Authorization' => 'Bearer test-token',
        ], json_encode([
            'message' => [
                'messageId' => 'msg-123',
                'data' => base64_encode('{"order_id":123}'),
                'attributes' => ['source' => 'test'],
                'publishTime' => '2024-01-01T00:00:00Z',
            ],
        ]));

        config(['pubsub.webhook.auth_token' => 'test-token']);

        $this->subscriber->shouldReceive('handleMessage')
            ->withArgs(function ($data, $message, $topic) {
                return $data['order_id'] === 123
                    && $message->id() === 'msg-123'
                    && $topic === 'orders';
            })
            ->once();

        $response = $this->controller->handle($request, 'orders');

        expect($response->getStatusCode())->toBe(200);
    });

    it('returns 401 for unauthorized request', function () {
        $request = Request::create('/webhook/orders', 'POST');

        $response = $this->controller->handle($request, 'orders');

        expect($response->getStatusCode())->toBe(401);
        expect($response->getContent())->toBe('Unauthorized');
    });

    it('returns 400 for invalid message format', function () {
        $request = Request::create('/webhook/orders', 'POST', [], [], [], [
            'HTTP_X-Goog-Resource-State' => 'exists',
        ], 'invalid json');

        $response = $this->controller->handle($request, 'orders');

        expect($response->getStatusCode())->toBe(400);
    });

    it('returns 200 even on processing error to prevent redelivery storm', function () {
        $request = Request::create('/webhook/orders', 'POST', [], [], [], [
            'HTTP_X-Goog-Resource-State' => 'exists',
        ], json_encode([
            'message' => [
                'messageId' => 'msg-123',
                'data' => base64_encode('{"test":"data"}'),
            ],
        ]));

        $this->subscriber->shouldReceive('handleMessage')
            ->andThrow(new Exception('Processing error'));

        $response = $this->controller->handle($request, 'orders');

        expect($response->getStatusCode())->toBe(200);
    });
});

describe('VerifyPubSubWebhook middleware', function () {
    beforeEach(function () {
        $this->middleware = new VerifyPubSubWebhook();
        $this->next = fn ($request) => response('OK', 200);
    });

    it('passes valid webhook request', function () {
        $request = Request::create('/webhook', 'POST', [], [], [], [
            'HTTP_X-Goog-Resource-State' => 'exists',
            'HTTP_X-Goog-Message-Id' => 'msg-123',
            'HTTP_X-Goog-Subscription-Name' => 'test-sub',
        ]);

        $response = $this->middleware->handle($request, $this->next);

        expect($response->getStatusCode())->toBe(200);
        expect($response->getContent())->toBe('OK');
    });

    it('rejects request without required headers', function () {
        $request = Request::create('/webhook', 'POST');

        $response = $this->middleware->handle($request, $this->next);

        expect($response->getStatusCode())->toBe(401);
    });

    it('verifies auth token when configured', function () {
        config(['pubsub.webhook.auth_token' => 'secret-token']);

        $request = Request::create('/webhook', 'POST', [], [], [], [
            'HTTP_X-Goog-Resource-State' => 'exists',
            'HTTP_X-Goog-Message-Id' => 'msg-123',
            'HTTP_X-Goog-Subscription-Name' => 'test-sub',
            'HTTP_Authorization' => 'Bearer wrong-token',
        ]);

        $response = $this->middleware->handle($request, $this->next);

        expect($response->getStatusCode())->toBe(401);
    });

    it('checks IP allowlist when configured', function () {
        config(['pubsub.webhook.allowed_ips' => ['192.168.1.0/24']]);

        $request = Request::create('/webhook', 'POST', [], [], [], [
            'HTTP_X-Goog-Resource-State' => 'exists',
            'HTTP_X-Goog-Message-Id' => 'msg-123',
            'HTTP_X-Goog-Subscription-Name' => 'test-sub',
            'REMOTE_ADDR' => '10.0.0.1', // Not in allowlist
        ]);

        $response = $this->middleware->handle($request, $this->next);

        expect($response->getStatusCode())->toBe(403);
    });

    it('skips verification in local environment when configured', function () {
        config(['pubsub.webhook.skip_verification' => true]);
        app()->detectEnvironment(fn () => 'local');

        $request = Request::create('/webhook', 'POST'); // No headers

        $response = $this->middleware->handle($request, $this->next);

        expect($response->getStatusCode())->toBe(200);
    });

    it('validates CIDR notation correctly', function () {
        $middleware = new VerifyPubSubWebhook();
        $reflection = new ReflectionClass($middleware);
        $method = $reflection->getMethod('ipMatches');
        $method->setAccessible(true);

        expect($method->invoke($middleware, '192.168.1.100', '192.168.1.0/24'))->toBeTrue();
        expect($method->invoke($middleware, '192.168.2.100', '192.168.1.0/24'))->toBeFalse();
        expect($method->invoke($middleware, '192.168.1.100', '192.168.1.100'))->toBeTrue();
    });
});
