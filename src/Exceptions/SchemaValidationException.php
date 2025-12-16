<?php

declare(strict_types=1);

namespace OffloadProject\GooglePubSub\Exceptions;

use Throwable;

final class SchemaValidationException extends PubSubException
{
    /**
     * The validation errors.
     *
     * @var array<int|string, mixed>
     */
    protected array $errors = [];

    /**
     * Create a new schema validation exception.
     *
     * @param  array<int|string, mixed>  $errors
     */
    public function __construct(string $message, int $code = 0, ?Throwable $previous = null, array $errors = [])
    {
        parent::__construct($message, $code, $previous);
        $this->errors = $errors;
    }

    /**
     * Get the validation errors.
     *
     * @return array<int|string, mixed>
     */
    public function getErrors(): array
    {
        return $this->errors;
    }
}
