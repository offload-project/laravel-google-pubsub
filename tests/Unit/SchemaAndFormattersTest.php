<?php

declare(strict_types=1);

use OffloadProject\GooglePubSub\Exceptions\MessageFormatException;
use OffloadProject\GooglePubSub\Exceptions\SchemaValidationException;
use OffloadProject\GooglePubSub\Formatters\CloudEventsFormatter;
use OffloadProject\GooglePubSub\Formatters\JsonFormatter;
use OffloadProject\GooglePubSub\Schema\SchemaValidator;

describe('SchemaValidator', function () {
    beforeEach(function () {
        $this->validator = new SchemaValidator([
            'schemas' => [
                'test_schema' => [
                    'schema' => [
                        'type' => 'object',
                        'required' => ['name', 'age'],
                        'properties' => [
                            'name' => ['type' => 'string'],
                            'age' => ['type' => 'integer', 'minimum' => 0],
                        ],
                    ],
                ],
            ],
        ]);
    });

    it('validates valid data against schema', function () {
        $data = (object) ['name' => 'John', 'age' => 30];

        // Test that validation doesn't throw
        $this->validator->validate($data, 'test_schema');

        expect($this->validator->isValid($data, 'test_schema'))->toBeTrue();
    });

    it('throws exception for invalid data', function () {
        $data = (object) ['name' => 'John']; // Missing required 'age'

        expect(fn () => $this->validator->validate($data, 'test_schema'))
            ->toThrow(SchemaValidationException::class);

        expect($this->validator->isValid($data, 'test_schema'))->toBeFalse();
    });

    it('returns errors without throwing', function () {
        $data = (object) ['name' => 123, 'age' => -5]; // Wrong types

        $errors = $this->validator->getErrors($data, 'test_schema');

        expect($errors)->not->toBeNull();
        expect($errors)->toBeArray();
    });

    it('loads schema from file', function () {
        $schemaFile = sys_get_temp_dir().'/test-schema.json';
        file_put_contents($schemaFile, json_encode([
            'type' => 'object',
            'properties' => ['test' => ['type' => 'string']],
        ]));

        $validator = new SchemaValidator([
            'schemas' => [
                'file_schema' => ['file' => $schemaFile],
            ],
        ]);

        expect($validator->isValid((object) ['test' => 'value'], 'file_schema'))->toBeTrue();

        unlink($schemaFile);
    });

    it('registers schema programmatically', function () {
        $this->validator->registerSchema('dynamic', [
            'type' => 'array',
            'items' => ['type' => 'number'],
        ]);

        expect($this->validator->isValid([1, 2, 3], 'dynamic'))->toBeTrue();
        expect($this->validator->isValid(['a', 'b'], 'dynamic'))->toBeFalse();
    });
});

describe('JsonFormatter', function () {
    beforeEach(function () {
        $this->formatter = new JsonFormatter();
    });

    it('formats data as JSON', function () {
        $data = ['test' => 'data', 'number' => 123];

        $json = $this->formatter->format($data);

        expect($json)->toBe('{"test":"data","number":123}');
    });

    it('returns string data as-is', function () {
        $data = 'already a string';

        $result = $this->formatter->format($data);

        expect($result)->toBe('already a string');
    });

    it('parses JSON data', function () {
        $json = '{"test":"data","number":123}';

        $data = $this->formatter->parse($json);

        expect($data)->toBe(['test' => 'data', 'number' => 123]);
    });

    it('throws exception for invalid JSON', function () {
        expect(fn () => $this->formatter->parse('invalid json'))
            ->toThrow(MessageFormatException::class);
    });

    it('handles empty data', function () {
        expect($this->formatter->parse(''))->toBeNull();
    });
});

describe('CloudEventsFormatter', function () {
    beforeEach(function () {
        $this->formatter = new CloudEventsFormatter('https://example.com', 'com.example.test');
    });

    it('formats data as CloudEvents', function () {
        $data = ['test' => 'data'];

        $json = $this->formatter->format($data);
        $event = json_decode($json, true);

        expect($event)->toHaveKeys(['specversion', 'type', 'source', 'id', 'time', 'datacontenttype', 'data']);
        expect($event['specversion'])->toBe('1.0');
        expect($event['type'])->toBe('com.example.test');
        expect($event['source'])->toBe('https://example.com');
        expect($event['data'])->toBe(['test' => 'data']);
    });

    it('uses event type from data', function () {
        $data = ['type' => 'custom.event', 'payload' => 'test'];

        $json = $this->formatter->format($data);
        $event = json_decode($json, true);

        expect($event['type'])->toBe('custom.event');
        expect($event['data'])->toBe(['payload' => 'test']); // 'type' removed from data
    });

    it('generates event from object', function () {
        $object = new class()
        {
            public function getEventType(): string
            {
                return 'object.event';
            }

            public function getEventSubject(): string
            {
                return 'test-subject';
            }

            public function toCloudEventData(): array
            {
                return ['object' => 'data'];
            }
        };

        $json = $this->formatter->format($object);
        $event = json_decode($json, true);

        expect($event['type'])->toBe('object.event');
        expect($event['subject'])->toBe('test-subject');
        expect($event['data'])->toBe(['object' => 'data']);
    });

    it('parses CloudEvents format', function () {
        $cloudEvent = [
            'specversion' => '1.0',
            'type' => 'test.event',
            'source' => 'https://example.com',
            'id' => 'test-123',
            'data' => ['test' => 'payload'],
        ];

        $data = $this->formatter->parse(json_encode($cloudEvent));

        expect($data)->toBe(['test' => 'payload']);
    });

    it('validates required CloudEvents fields', function () {
        $invalidEvent = ['type' => 'test']; // Missing required fields

        expect(fn () => $this->formatter->parse(json_encode($invalidEvent)))
            ->toThrow(MessageFormatException::class, 'missing required field');
    });
});
