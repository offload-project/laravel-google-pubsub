<?php

declare(strict_types=1);

namespace OffloadProject\GooglePubSub\Schema;

use OffloadProject\GooglePubSub\Exceptions\SchemaValidationException;
use Opis\JsonSchema\Errors\ErrorFormatter;
use Opis\JsonSchema\Validator;

class SchemaValidator
{
    /**
     * The JSON Schema validator instance.
     */
    private Validator $validator;

    /**
     * Loaded schemas cache.
     *
     * @var array<string, object>
     */
    private array $schemas = [];

    /**
     * Schema configuration.
     *
     * @var array<string, mixed>
     */
    private array $config;

    /**
     * Create a new schema validator.
     *
     * @param  array<string, mixed>  $config
     */
    public function __construct(array $config = [])
    {
        $this->config = $config;
        $this->validator = new Validator();
    }

    /**
     * Validate data against a schema.
     *
     * @throws SchemaValidationException
     */
    public function validate(mixed $data, string $schemaName): void
    {
        $schema = $this->getSchema($schemaName);

        if (! $schema) {
            if ($this->config['strict_mode'] ?? true) {
                throw new SchemaValidationException("Schema '{$schemaName}' not found");
            }

            return;
        }

        $result = $this->validator->validate($data, $schema);

        if (! $result->isValid()) {
            $error = $result->error();
            if ($error === null) {
                throw new SchemaValidationException("Validation failed for schema '{$schemaName}' with unknown error");
            }

            $formatter = new ErrorFormatter();
            $errors = $formatter->format($error);

            throw new SchemaValidationException(
                "Validation failed for schema '{$schemaName}': ".json_encode($errors),
                errors: $errors
            );
        }
    }

    /**
     * Check if data is valid against a schema.
     */
    public function isValid(mixed $data, string $schemaName): bool
    {
        try {
            $this->validate($data, $schemaName);

            return true;
        } catch (SchemaValidationException $e) {
            return false;
        }
    }

    /**
     * Get validation errors without throwing exception.
     *
     * @return array<string, mixed>|null
     */
    public function getErrors(mixed $data, string $schemaName): ?array
    {
        $schema = $this->getSchema($schemaName);

        if (! $schema) {
            return null;
        }

        $result = $this->validator->validate($data, $schema);

        if (! $result->isValid()) {
            $error = $result->error();
            if ($error === null) {
                return ['unknown' => 'Validation failed with unknown error'];
            }

            $formatter = new ErrorFormatter();

            return $formatter->format($error);
        }

        return null;
    }

    /**
     * Register a schema programmatically.
     */
    public function registerSchema(string $name, mixed $schema): void
    {
        if (is_string($schema)) {
            $decoded = json_decode($schema);
            if ($decoded === null && json_last_error() !== JSON_ERROR_NONE) {
                throw new SchemaValidationException('Invalid JSON schema string: '.json_last_error_msg());
            }
            if ($decoded === null) {
                throw new SchemaValidationException('Schema string contains null value');
            }
            $schema = $decoded;
        } elseif (is_array($schema)) {
            $encoded = json_encode($schema);
            if ($encoded === false) {
                throw new SchemaValidationException('Failed to encode schema array: '.json_last_error_msg());
            }
            $decoded = json_decode($encoded);
            if ($decoded === null) {
                throw new SchemaValidationException('Failed to decode schema array');
            }
            $schema = $decoded;
        }

        if (! is_object($schema)) {
            throw new SchemaValidationException('Schema must be an object, array, or valid JSON string');
        }

        $this->schemas[$name] = $schema;
    }

    /**
     * Clear schema cache.
     */
    public function clearCache(): void
    {
        $this->schemas = [];
    }

    /**
     * Load a schema.
     */
    private function getSchema(string $schemaName): ?object
    {
        if (isset($this->schemas[$schemaName])) {
            return $this->schemas[$schemaName];
        }

        $schemaConfig = $this->config['schemas'][$schemaName] ?? null;

        if (! $schemaConfig) {
            return null;
        }

        $schema = $this->loadSchemaFromConfig($schemaConfig);
        $this->schemas[$schemaName] = $schema;

        return $schema;
    }

    /**
     * Load schema from configuration.
     *
     * @param  array<string, mixed>  $config
     */
    private function loadSchemaFromConfig(array $config): object
    {
        // Load from file
        if (isset($config['file'])) {
            $path = $config['file'];

            if (! file_exists($path)) {
                $path = base_path($path);
            }

            if (! file_exists($path)) {
                throw new SchemaValidationException("Schema file not found: {$config['file']}");
            }

            $content = file_get_contents($path);
            if ($content === false) {
                throw new SchemaValidationException("Failed to read schema file: {$config['file']}");
            }

            $decoded = json_decode($content);

            if (json_last_error() !== JSON_ERROR_NONE) {
                throw new SchemaValidationException('Invalid JSON in schema file: '.json_last_error_msg());
            }

            if ($decoded === null) {
                throw new SchemaValidationException('Schema file contains null value');
            }

            return $decoded;
        }

        // Load from array
        if (isset($config['schema'])) {
            $encoded = json_encode($config['schema']);
            if ($encoded === false) {
                throw new SchemaValidationException('Failed to encode schema array: '.json_last_error_msg());
            }
            $decoded = json_decode($encoded);
            if ($decoded === null && json_last_error() !== JSON_ERROR_NONE) {
                throw new SchemaValidationException('Failed to decode schema: '.json_last_error_msg());
            }

            return $decoded;
        }

        // Load from URL
        if (isset($config['url'])) {
            $content = file_get_contents($config['url']);
            if ($content === false) {
                throw new SchemaValidationException("Failed to fetch schema from URL: {$config['url']}");
            }

            $decoded = json_decode($content);

            if (json_last_error() !== JSON_ERROR_NONE) {
                throw new SchemaValidationException('Invalid JSON from URL: '.json_last_error_msg());
            }

            if ($decoded === null) {
                throw new SchemaValidationException('Schema URL returned null value');
            }

            return $decoded;
        }

        throw new SchemaValidationException('Schema configuration must include file, schema, or url');
    }
}
