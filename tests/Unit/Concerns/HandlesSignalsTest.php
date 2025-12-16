<?php

declare(strict_types=1);

use OffloadProject\GooglePubSub\Concerns\HandlesSignals;

beforeEach(function () {
    // Create a test class that uses the trait
    $this->handler = new class
    {
        use HandlesSignals;

        public function getSignalsRegistered(): bool
        {
            return $this->signalsRegistered;
        }

        public function getShouldStopFlag(): bool
        {
            return $this->shouldStopFlag;
        }

        public function publicShouldStop(): bool
        {
            return $this->shouldStop();
        }

        public function publicRegisterSignalHandlers(): void
        {
            $this->registerSignalHandlers();
        }

        public function publicHandleShutdownSignal(string $signal): void
        {
            $this->handleShutdownSignal($signal);
        }

        public function publicProcessSignals(): void
        {
            $this->processSignals();
        }

        public function publicResetStopFlag(): void
        {
            $this->resetStopFlag();
        }
    };
});

describe('HandlesSignals trait', function () {
    it('initializes with shouldStopFlag as false', function () {
        expect($this->handler->getShouldStopFlag())->toBeFalse();
    });

    it('initializes with signalsRegistered as false', function () {
        expect($this->handler->getSignalsRegistered())->toBeFalse();
    });

    it('shouldStop returns false initially', function () {
        expect($this->handler->publicShouldStop())->toBeFalse();
    });

    it('stop method sets shouldStopFlag to true', function () {
        $this->handler->stop();

        expect($this->handler->getShouldStopFlag())->toBeTrue();
        expect($this->handler->publicShouldStop())->toBeTrue();
    });

    it('resetStopFlag resets the flag to false', function () {
        $this->handler->stop();
        expect($this->handler->getShouldStopFlag())->toBeTrue();

        $this->handler->publicResetStopFlag();
        expect($this->handler->getShouldStopFlag())->toBeFalse();
    });

    it('handleShutdownSignal sets shouldStopFlag to true', function () {
        $this->handler->publicHandleShutdownSignal('SIGTERM');

        expect($this->handler->getShouldStopFlag())->toBeTrue();
    });

    it('handles SIGTERM signal', function () {
        $this->handler->publicHandleShutdownSignal('SIGTERM');

        expect($this->handler->publicShouldStop())->toBeTrue();
    });

    it('handles SIGINT signal', function () {
        $this->handler->publicHandleShutdownSignal('SIGINT');

        expect($this->handler->publicShouldStop())->toBeTrue();
    });

    it('handles SIGQUIT signal', function () {
        $this->handler->publicHandleShutdownSignal('SIGQUIT');

        expect($this->handler->publicShouldStop())->toBeTrue();
    });

    it('registerSignalHandlers only registers once', function () {
        // First registration
        $this->handler->publicRegisterSignalHandlers();
        expect($this->handler->getSignalsRegistered())->toBeTrue();

        // Second call should not change anything (no errors)
        $this->handler->publicRegisterSignalHandlers();
        expect($this->handler->getSignalsRegistered())->toBeTrue();
    });

    it('processSignals can be called safely', function () {
        // This should not throw even without signal handlers registered
        $this->handler->publicProcessSignals();

        expect(true)->toBeTrue();
    });

    it('shouldStop processes signals before checking flag', function () {
        // Register handlers first
        $this->handler->publicRegisterSignalHandlers();

        // Flag should still be false (no signals sent)
        expect($this->handler->publicShouldStop())->toBeFalse();
    });
});

describe('HandlesSignals PCNTL integration', function () {
    it('skips signal registration when pcntl not loaded', function () {
        // If PCNTL is not loaded, signals shouldn't be registered but no error
        if (! extension_loaded('pcntl')) {
            $this->handler->publicRegisterSignalHandlers();

            // signalsRegistered stays false when PCNTL not available
            expect($this->handler->getSignalsRegistered())->toBeFalse();
        } else {
            $this->handler->publicRegisterSignalHandlers();
            expect($this->handler->getSignalsRegistered())->toBeTrue();
        }
    });
});
