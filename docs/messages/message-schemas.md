# Schema Validation

Ensure message contracts between services with JSON Schema validation using the Opis JSON Schema library. Helps maintain
data integrity and provides clear documentation of your message formats with comprehensive validation capabilities.

## Schema Validation Engine

This package uses **Opis JSON Schema** for robust validation:

- **JSON Schema Draft-07** support
- **Complex validation rules** with conditionals and references
- **Multiple schema sources**: files, inline definitions, remote URLs
- **Detailed error reporting** with property-level feedback
- **CLI testing tools** for development and debugging

## Configuration

### 1. Define Schemas

In `config/pubsub.php`:

```php
'schemas' => [
    'order_events' => [
        'file' => 'schemas/order-events.json',
    ],
    'user_events' => [
        'schema' => [
            '$schema' => 'http://json-schema.org/draft-07/schema#',
            'type' => 'object',
            'required' => ['user_id', 'event_type'],
            'properties' => [
                'user_id' => ['type' => 'integer'],
                'event_type' => [
                    'type' => 'string',
                    'enum' => ['created', 'updated', 'deleted']
                ],
                'timestamp' => [
                    'type' => 'string',
                    'format' => 'date-time'
                ],
            ],
        ],
    ],
    'payment_events' => [
        'url' => 'https://schemas.example.com/payment-events-v1.json',
    ],
],

'schema_validation' => [
    'enabled' => true,
    'strict_mode' => true, // Fail on missing schemas
],
```

### 2. Assign Schemas to Topics

```php
'topics' => [
    'orders' => [
        'schema' => 'order_events',
        'enable_message_ordering' => true,
    ],
    'users' => [
        'schema' => 'user_events',
    ],
    'payments' => [
        'schema' => 'payment_events',
    ],
],
```

## Creating Schema Files

### Example: Order Events Schema

Create `schemas/order-events.json`:

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Order Events",
  "description": "Schema for order-related events",
  "type": "object",
  "required": [
    "event_type",
    "order_id",
    "timestamp"
  ],
  "properties": {
    "event_type": {
      "type": "string",
      "enum": [
        "order.created",
        "order.updated",
        "order.shipped",
        "order.delivered",
        "order.cancelled"
      ]
    },
    "order_id": {
      "type": "integer",
      "minimum": 1
    },
    "customer_id": {
      "type": "integer",
      "minimum": 1
    },
    "timestamp": {
      "type": "string",
      "format": "date-time"
    },
    "data": {
      "type": "object",
      "properties": {
        "total": {
          "type": "number",
          "minimum": 0
        },
        "currency": {
          "type": "string",
          "pattern": "^[A-Z]{3}$",
          "default": "USD"
        },
        "items": {
          "type": "array",
          "items": {
            "$ref": "#/definitions/orderItem"
          }
        },
        "shipping_address": {
          "$ref": "#/definitions/address"
        }
      }
    }
  },
  "definitions": {
    "orderItem": {
      "type": "object",
      "required": [
        "product_id",
        "quantity",
        "price"
      ],
      "properties": {
        "product_id": {
          "type": "integer"
        },
        "sku": {
          "type": "string"
        },
        "name": {
          "type": "string"
        },
        "quantity": {
          "type": "integer",
          "minimum": 1
        },
        "price": {
          "type": "number",
          "minimum": 0
        }
      }
    },
    "address": {
      "type": "object",
      "required": [
        "street",
        "city",
        "country"
      ],
      "properties": {
        "street": {
          "type": "string"
        },
        "city": {
          "type": "string"
        },
        "state": {
          "type": "string"
        },
        "postal_code": {
          "type": "string"
        },
        "country": {
          "type": "string",
          "pattern": "^[A-Z]{2}$"
        }
      }
    }
  },
  "if": {
    "properties": {
      "event_type": {
        "const": "order.shipped"
      }
    }
  },
  "then": {
    "required": [
      "data"
    ],
    "properties": {
      "data": {
        "required": [
          "tracking_number",
          "carrier"
        ]
      }
    }
  }
}
```

## Validation

### Automatic Validation

Messages are automatically validated when publishing to topics with schemas:

```php
// This will be validated against the 'order_events' schema
PubSub::publish('orders', [
    'event_type' => 'order.created',
    'order_id' => 123,
    'timestamp' => now()->toIso8601String(),
    'data' => [
        'total' => 99.99,
        'currency' => 'USD'
    ]
]);

// Invalid message will throw SchemaValidationException
PubSub::publish('orders', [
    'event_type' => 'invalid.type', // Not in enum
    'order_id' => 'abc', // Should be integer
]);
```

### Manual Validation

Use the schema validator directly:

```php
use OffloadProject\GooglePubSub\Schema\SchemaValidator;

$validator = app(SchemaValidator::class);

// Validate data
try {
    $validator->validate($data, 'order_events');
    echo "Valid!";
} catch (SchemaValidationException $e) {
    echo "Invalid: " . $e->getMessage();
    print_r($e->getErrors());
}

// Check validity without exception
if ($validator->isValid($data, 'order_events')) {
    // Process valid data
}

// Get errors without throwing
$errors = $validator->getErrors($data, 'order_events');
if ($errors) {
    // Handle validation errors
}
```

### Testing Schemas

Use the artisan command to test schemas:

```bash
# Validate JSON data against a schema
php artisan pubsub:schema:validate order_events '{"event_type":"order.created","order_id":123,"timestamp":"2024-01-01T00:00:00Z"}'

# Validate from file
cat order.json | php artisan pubsub:schema:validate order_events

# Test with invalid data
php artisan pubsub:schema:validate order_events '{"event_type":"invalid"}'
```

## Schema Versioning

### Strategy 1: Versioned Schemas

```php
'schemas' => [
    'order_events_v1' => ['file' => 'schemas/order-events-v1.json'],
    'order_events_v2' => ['file' => 'schemas/order-events-v2.json'],
],

'topics' => [
    'orders' => ['schema' => 'order_events_v2'], // Current version
    'orders-v1' => ['schema' => 'order_events_v1'], // Legacy support
],
```

### Strategy 2: Backward Compatible Evolution

Design schemas to be backward compatible:

```json5
{
  "properties": {
    "order_id": {
      "type": "integer"
    },
    "customer_id": {
      "type": "integer"
    },
    // New field with default (backward compatible)
    "priority": {
      "type": "string",
      "default": "normal",
      "enum": [
        "low",
        "normal",
        "high"
      ]
    },
    // Optional new field (backward compatible)
    "metadata": {
      "type": "object",
      "required": false
    }
  }
}
```

### Strategy 3: Version in Message

Include version in the message:

```json
{
  "required": [
    "version",
    "event_type"
  ],
  "properties": {
    "version": {
      "type": "string",
      "pattern": "^\\d+\\.\\d+$"
    }
  },
  "allOf": [
    {
      "if": {
        "properties": {
          "version": {
            "const": "1.0"
          }
        }
      },
      "then": {
        "$ref": "#/definitions/v1"
      }
    },
    {
      "if": {
        "properties": {
          "version": {
            "const": "2.0"
          }
        }
      },
      "then": {
        "$ref": "#/definitions/v2"
      }
    }
  ]
}
```

## Programmatic Schema Registration

Register schemas at runtime:

```php
use OffloadProject\GooglePubSub\Schema\SchemaValidator;

$validator = app(SchemaValidator::class);

// Register from array
$validator->registerSchema('dynamic_events', [
    'type' => 'object',
    'required' => ['event_id', 'timestamp'],
    'properties' => [
        'event_id' => ['type' => 'string', 'format' => 'uuid'],
        'timestamp' => ['type' => 'string', 'format' => 'date-time'],
        'data' => ['type' => 'object'],
    ]
]);

// Register from JSON string
$validator->registerSchema('custom_events', $jsonSchemaString);

// Now use the schema
$validator->validate($data, 'dynamic_events');
```

## Sharing Schemas Across Services

### Option 1: Schema Registry

Host schemas centrally:

```php
'schemas' => [
    'order_events' => [
        'url' => 'https://schemas.mycompany.com/pubsub/order-events/v2.json',
    ],
    'user_events' => [
        'url' => 'https://schemas.mycompany.com/pubsub/user-events/v1.json',
    ],
],
```

### Option 2: Git Submodule

Share schemas via Git:

```bash
# Add schemas repository as submodule
git submodule add https://github.com/mycompany/pubsub-schemas.git schemas

# Reference in config
'schemas' => [
    'order_events' => ['file' => 'schemas/events/order-events.json'],
],
```

### Option 3: Composer Package

Distribute schemas as a package:

```json
{
  "name": "mycompany/pubsub-schemas",
  "autoload": {
    "files": [
      "src/schemas.php"
    ]
  }
}
```

## Best Practices

### 1. Start Strict, Loosen Carefully

Begin with strict validation and relax as needed:

```json5
{
  "additionalProperties": false,
  // No extra properties allowed
  "required": [
    "id",
    "type",
    "timestamp"
  ]
  // All required initially
}
```

### 2. Use References for Reusability

```json
{
  "definitions": {
    "timestamp": {
      "type": "string",
      "format": "date-time"
    },
    "money": {
      "type": "object",
      "required": [
        "amount",
        "currency"
      ],
      "properties": {
        "amount": {
          "type": "number"
        },
        "currency": {
          "type": "string",
          "pattern": "^[A-Z]{3}$"
        }
      }
    }
  },
  "properties": {
    "created_at": {
      "$ref": "#/definitions/timestamp"
    },
    "total": {
      "$ref": "#/definitions/money"
    }
  }
}
```

### 3. Document Everything

```json
{
  "title": "Order Created Event",
  "description": "Emitted when a new order is placed",
  "properties": {
    "order_id": {
      "type": "integer",
      "description": "Unique order identifier",
      "examples": [
        12345,
        67890
      ]
    }
  }
}
```

### 4. Validate Examples

Include and validate example messages:

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "examples": [
    {
      "event_type": "order.created",
      "order_id": 12345,
      "timestamp": "2024-01-01T00:00:00Z"
    }
  ]
}
```

### 5. Handle Schema Failures Gracefully

```php
try {
    PubSub::publish('orders', $data);
} catch (SchemaValidationException $e) {
    // Log validation error
    Log::error('Schema validation failed', [
        'errors' => $e->getErrors(),
        'data' => $data
    ]);
    
    // Send to dead letter topic
    PubSub::publish('orders-invalid', [
        'original_data' => $data,
        'validation_errors' => $e->getErrors(),
        'timestamp' => now()->toIso8601String()
    ]);
}
```
