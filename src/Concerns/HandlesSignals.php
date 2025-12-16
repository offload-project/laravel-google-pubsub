<?php

declare(strict_types=1);

namespace OffloadProject\GooglePubSub\Concerns;

use Illuminate\Support\Facades\Log;

/**
 * Provides graceful shutdown handling via PCNTL signals.
 *
 * This trait enables long-running processes (like subscribers) to respond
 * to SIGTERM and SIGINT signals for graceful shutdown in containerized
 * environments (Docker, Kubernetes, etc.).
 */
trait HandlesSignals
{
    /**
     * Whether a stop signal has been received.
     */
    protected bool $shouldStopFlag = false;

    /**
     * Whether signal handlers have been registered.
     */
    protected bool $signalsRegistered = false;

    /**
     * Request a graceful stop.
     *
     * This can be called programmatically to request shutdown.
     */
    public function stop(): void
    {
        $this->shouldStopFlag = true;
    }

    /**
     * Register signal handlers for graceful shutdown.
     */
    protected function registerSignalHandlers(): void
    {
        if ($this->signalsRegistered) {
            return;
        }

        if (! extension_loaded('pcntl')) {
            Log::debug('PCNTL extension not loaded, graceful shutdown via signals not available');

            return;
        }

        // Handle SIGTERM (sent by Docker/Kubernetes for graceful shutdown)
        pcntl_signal(SIGTERM, function () {
            $this->handleShutdownSignal('SIGTERM');
        });

        // Handle SIGINT (Ctrl+C)
        pcntl_signal(SIGINT, function () {
            $this->handleShutdownSignal('SIGINT');
        });

        // Handle SIGQUIT (Ctrl+\)
        pcntl_signal(SIGQUIT, function () {
            $this->handleShutdownSignal('SIGQUIT');
        });

        $this->signalsRegistered = true;

        Log::debug('Registered signal handlers for graceful shutdown');
    }

    /**
     * Handle a shutdown signal.
     */
    protected function handleShutdownSignal(string $signal): void
    {
        Log::info("Received {$signal} signal, initiating graceful shutdown");

        $this->shouldStopFlag = true;
    }

    /**
     * Process any pending signals.
     *
     * This should be called periodically in long-running loops to check
     * for pending signals since PHP signal handling is synchronous.
     */
    protected function processSignals(): void
    {
        if (extension_loaded('pcntl')) {
            pcntl_signal_dispatch();
        }
    }

    /**
     * Check if the process should stop.
     */
    protected function shouldStop(): bool
    {
        $this->processSignals();

        return $this->shouldStopFlag;
    }

    /**
     * Reset the stop flag.
     *
     * Useful for testing or restarting the subscriber.
     */
    protected function resetStopFlag(): void
    {
        $this->shouldStopFlag = false;
    }
}
