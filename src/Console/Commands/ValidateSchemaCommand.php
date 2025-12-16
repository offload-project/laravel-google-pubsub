<?php

declare(strict_types=1);

namespace OffloadProject\GooglePubSub\Console\Commands;

use Exception;
use Illuminate\Console\Command;
use OffloadProject\GooglePubSub\Schema\SchemaValidator;

final class ValidateSchemaCommand extends Command
{
    protected $signature = 'pubsub:schema:validate
                            {schema : Schema name}
                            {data? : JSON data to validate (or pipe from stdin)}';

    protected $description = 'Validate JSON data against a configured schema';

    public function handle(): int
    {
        $schemaNameArg = $this->argument('schema');
        if (! is_string($schemaNameArg)) {
            $this->error('Schema name must be a string');

            return Command::FAILURE;
        }
        $schemaName = $schemaNameArg;

        $dataInputArg = $this->argument('data');
        $dataInput = is_string($dataInputArg) ? $dataInputArg : null;

        // Get data from argument or stdin
        if ($dataInput === null && ! posix_isatty(STDIN)) {
            $stdinContent = file_get_contents('php://stdin');
            $dataInput = $stdinContent !== false ? $stdinContent : null;
        }

        if ($dataInput === null) {
            $this->error('No data provided. Pass JSON as argument or pipe from stdin.');

            return Command::FAILURE;
        }

        // Parse JSON data
        $data = json_decode($dataInput);
        if (json_last_error() !== JSON_ERROR_NONE) {
            $this->error('Invalid JSON: '.json_last_error_msg());

            return Command::FAILURE;
        }

        // Validate
        $validator = new SchemaValidator(config('pubsub'));

        try {
            $validator->validate($data, $schemaName);
            $this->info("✓ Data is valid against schema '{$schemaName}'");

            return Command::SUCCESS;
        } catch (Exception $e) {
            $this->error("✗ Validation failed: {$e->getMessage()}");

            if ($errors = $validator->getErrors($data, $schemaName)) {
                $this->line('');
                $this->line('Errors:');
                $jsonErrors = json_encode($errors, JSON_PRETTY_PRINT);
                if ($jsonErrors !== false) {
                    $this->line($jsonErrors);
                }
            }

            return Command::FAILURE;
        }
    }
}
