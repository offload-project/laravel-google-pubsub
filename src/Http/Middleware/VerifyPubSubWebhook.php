<?php

declare(strict_types=1);

namespace OffloadProject\GooglePubSub\Http\Middleware;

use Closure;
use Illuminate\Http\Request;
use Illuminate\Http\Response;
use Illuminate\Support\Facades\Log;

final class VerifyPubSubWebhook
{
    /**
     * Handle an incoming request.
     */
    public function handle(Request $request, Closure $next): Response
    {
        // Skip verification in local/testing environments if configured
        if (config('pubsub.webhook.skip_verification') && app()->environment(['local', 'testing'])) {
            return $next($request);
        }

        // Verify Google Cloud headers
        if (! $this->hasValidHeaders($request)) {
            Log::warning('Invalid Pub/Sub webhook headers', [
                'headers' => $request->headers->all(),
                'ip' => $request->ip(),
            ]);

            return response('Unauthorized', 401);
        }

        // Verify IP address if allowlist is configured
        if (! $this->isAllowedIp($request)) {
            Log::warning('Pub/Sub webhook from unauthorized IP', [
                'ip' => $request->ip(),
            ]);

            return response('Forbidden', 403);
        }

        // Verify authentication token if configured
        if (! $this->verifyAuthToken($request)) {
            Log::warning('Invalid Pub/Sub webhook auth token', [
                'ip' => $request->ip(),
            ]);

            return response('Unauthorized', 401);
        }

        return $next($request);
    }

    /**
     * Check if request has valid Pub/Sub headers.
     */
    private function hasValidHeaders(Request $request): bool
    {
        // Google Pub/Sub always sends these headers
        $requiredHeaders = [
            'X-Goog-Resource-State',
            'X-Goog-Message-Id',
            'X-Goog-Subscription-Name',
        ];

        foreach ($requiredHeaders as $header) {
            if (! $request->hasHeader($header)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Check if request is from allowed IP.
     */
    private function isAllowedIp(Request $request): bool
    {
        $allowedIps = config('pubsub.webhook.allowed_ips', []);

        if (empty($allowedIps)) {
            return true; // No IP restriction
        }

        $requestIp = $request->ip();

        if ($requestIp === null) {
            return false;
        }

        foreach ($allowedIps as $allowedIp) {
            if ($this->ipMatches($requestIp, $allowedIp)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Check if IP matches pattern (supports CIDR).
     */
    private function ipMatches(string $ip, string $pattern): bool
    {
        if ($ip === $pattern) {
            return true;
        }

        // Check CIDR notation
        if (str_contains($pattern, '/')) {
            [$subnet, $mask] = explode('/', $pattern);
            $subnet = ip2long($subnet);
            $ip = ip2long($ip);
            $mask = -1 << (32 - (int) $mask);

            return ($ip & $mask) === ($subnet & $mask);
        }

        return false;
    }

    /**
     * Verify authentication token.
     */
    private function verifyAuthToken(Request $request): bool
    {
        $configuredToken = config('pubsub.webhook.auth_token');

        if (! $configuredToken) {
            return true; // No token configured
        }

        $authHeader = $request->header('Authorization');

        if (! $authHeader) {
            return false;
        }

        // Support both "Bearer TOKEN" and just "TOKEN"
        $token = str_starts_with($authHeader, 'Bearer ')
            ? mb_substr($authHeader, 7)
            : $authHeader;

        return hash_equals($configuredToken, $token);
    }
}
