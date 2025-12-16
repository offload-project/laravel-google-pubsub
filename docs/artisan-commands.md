# Artisan Commands

Laravel Google Pub/Sub Artisan commands for managing topics, subscriptions, publishing, consuming, and testing.

## Topic Management

### List Topics

List all topics in your Google Cloud project:

```bash
php artisan pubsub:topics:list
```

**Output Example:**

```
+------------------+
| Topic Name       |
+------------------+
| orders           |
| analytics        |
| notifications    |
| user-events      |
+------------------+
```

### Create Topic

Create a new topic:

```bash
php artisan pubsub:topics:create {topic-name} [options]
```

**Options:**

- `--enable-ordering` : Enable message ordering

**Examples:**

```bash
# Basic topic
php artisan pubsub:topics:create orders

# Topic with message ordering
php artisan pubsub:topics:create orders --enable-ordering

# Topic for analytics
php artisan pubsub:topics:create analytics
```

## Subscription Management

### List Subscriptions

List all subscriptions in your project:

```bash
php artisan pubsub:subscriptions:list [options]
```

**Options:**

- `--topic=TOPIC` : Filter by specific topic

**Examples:**

```bash
# List all subscriptions
php artisan pubsub:subscriptions:list

# List subscriptions for specific topic
php artisan pubsub:subscriptions:list --topic=orders
```

**Output Example:**

```
+------------------------+----------+--------------+
| Subscription           | Topic    | Ack Deadline |
+------------------------+----------+--------------+
| orders-laravel         | orders   | 60           |
| orders-go-service      | orders   | 30           |
| analytics-processor    | analytics| 120          |
+------------------------+----------+--------------+
```

### Create Pull Subscription

Create a standard pull subscription:

```bash
php artisan pubsub:subscriptions:create {subscription-name} {topic-name} [options]
```

**Options:**

- `--ack-deadline=SECONDS` : Acknowledgment deadline (default: 60)
- `--enable-ordering` : Enable message ordering
- `--dead-letter` : Enable dead letter topic

**Examples:**

```bash
# Basic subscription
php artisan pubsub:subscriptions:create orders-processor orders

# Subscription with custom ack deadline
php artisan pubsub:subscriptions:create orders-processor orders --ack-deadline=120

# Subscription with message ordering
php artisan pubsub:subscriptions:create orders-sequential orders --enable-ordering

# Subscription with dead letter topic
php artisan pubsub:subscriptions:create orders-reliable orders --dead-letter

# Subscription with all options
php artisan pubsub:subscriptions:create orders-processor orders \
    --ack-deadline=90 \
    --enable-ordering \
    --dead-letter
```

### Create Push Subscription

Create a push subscription (webhook):

```bash
php artisan pubsub:subscriptions:create-push {subscription-name} {topic-name} {endpoint-url} [options]
```

**Options:**

- `--token=TOKEN` : Authentication token
- `--ack-deadline=SECONDS` : Acknowledgment deadline (default: 60)
- `--enable-ordering` : Enable message ordering
- `--dead-letter` : Enable dead letter topic

**Examples:**

```bash
# Basic push subscription
php artisan pubsub:subscriptions:create-push orders-webhook orders \
    https://myapp.com/pubsub/webhook/orders

# Push subscription with authentication
php artisan pubsub:subscriptions:create-push orders-webhook orders \
    https://myapp.com/pubsub/webhook/orders \
    --token=secret-token-123

# Push subscription with all options
php artisan pubsub:subscriptions:create-push orders-webhook orders \
    https://myapp.com/pubsub/webhook/orders \
    --token=secret-token-123 \
    --ack-deadline=60 \
    --enable-ordering \
    --dead-letter

# Local development with ngrok
php artisan pubsub:subscriptions:create-push local-webhook orders \
    https://abc123.ngrok.io/pubsub/webhook/orders \
    --token=dev-token
```

## Publishing Messages

### Publish Message

Publish a message to a topic:

```bash
php artisan pubsub:publish {topic-name} {message-data} [options]
```

**Options:**

- `--attributes=KEY:VALUE` : Message attributes (can be used multiple times)
- `--ordering-key=KEY` : Message ordering key

**Examples:**

```bash
# Basic message
php artisan pubsub:publish orders '{"order_id":123,"total":99.99}'

# Message with attributes
php artisan pubsub:publish orders '{"order_id":123}' \
    --attributes=priority:high \
    --attributes=source:cli

# Message with ordering key
php artisan pubsub:publish orders '{"order_id":123}' \
    --ordering-key=customer-456

# Complex message with all options
php artisan pubsub:publish orders '{"order_id":123,"customer_id":456,"total":99.99}' \
    --attributes=priority:high \
    --attributes=source:admin-panel \
    --attributes=region:us-east-1 \
    --ordering-key=customer-456

# Analytics event
php artisan pubsub:publish analytics '{"event":"page_view","user_id":789,"page":"/products"}'

# User event
php artisan pubsub:publish user-events '{"event":"profile_updated","user_id":123,"changes":{"email":"new@example.com"}}'
```

**Message Data Formats:**

- JSON string: `'{"key":"value"}'`
- Simple string: `'Hello World'`
- Numbers: `'42'` or `'3.14'`

## Consuming Messages

### Listen to Subscription

Listen for messages on a subscription:

```bash
php artisan pubsub:listen {subscription-name} [options]
```

**Options:**

- `--topic=TOPIC` : Topic name (required if subscription doesn't exist)
- `--max-messages=COUNT` : Maximum messages per pull (default: 100)

**Examples:**

```bash
# Listen to existing subscription
php artisan pubsub:listen orders-processor

# Listen and auto-create subscription
php artisan pubsub:listen orders-processor --topic=orders

# Listen with custom batch size
php artisan pubsub:listen orders-processor --max-messages=50

# Listen to webhook subscription (for testing)
php artisan pubsub:listen orders-webhook --topic=orders
```

**Output Example:**

```
Starting listener for subscription 'orders-processor'...
Listening for messages... Press Ctrl+C to stop.

Received message: 1234567890
{
    "order_id": 123,
    "customer_id": 456,
    "total": 99.99,
    "status": "created"
}
Attributes: {
    "priority": "high",
    "source": "web-api"
}

Received message: 1234567891
{
    "order_id": 124,
    "customer_id": 789,
    "total": 149.99,
    "status": "created"
}
```

### Stop Listening

- Press `Ctrl+C` to gracefully stop the listener
- The command will finish processing current messages before stopping

## Schema Validation

### Validate Message

Test message data against a configured schema:

```bash
php artisan pubsub:schema:validate {schema-name} [data]
```

**Data Sources:**

- Command argument: `'{"key":"value"}'`
- Standard input (pipe): `cat file.json | php artisan pubsub:schema:validate schema-name`

**Examples:**

```bash
# Validate JSON string
php artisan pubsub:schema:validate order_events '{"order_id":123,"total":99.99}'

# Validate from file
php artisan pubsub:schema:validate order_events '{"order_id":123,"customer_id":456,"total":99.99,"items":[{"product_id":1,"quantity":2}]}'

# Pipe from file
cat order.json | php artisan pubsub:schema:validate order_events

# Pipe from curl
curl -s https://api.example.com/order/123 | php artisan pubsub:schema:validate order_events

# Test invalid data
php artisan pubsub:schema:validate order_events '{"order_id":"invalid","total":-10}'
```

**Success Output:**

```
✓ Data is valid against schema 'order_events'
```

**Failure Output:**

```
✗ Validation failed: The property order_id must be of type integer

Errors:
[
    {
        "property": "order_id",
        "message": "The property order_id must be of type integer",
        "constraint": "type"
    },
    {
        "property": "total",
        "message": "Must have a minimum value of 0",
        "constraint": "minimum"
    }
]
```

## Command Usage Patterns

### Development Workflow

```bash
# 1. Create topics and subscriptions
php artisan pubsub:topics:create orders --enable-ordering
php artisan pubsub:subscriptions:create orders-dev orders --ack-deadline=30

# 2. Test publishing
php artisan pubsub:publish orders '{"test":true}'

# 3. Test consuming
php artisan pubsub:listen orders-dev --max-messages=1

# 4. Validate schemas
php artisan pubsub:schema:validate order_events '{"order_id":123}'
```

### Production Setup

```bash
# Create production topics
php artisan pubsub:topics:create orders --enable-ordering
php artisan pubsub:topics:create analytics
php artisan pubsub:topics:create notifications

# Create production subscriptions
php artisan pubsub:subscriptions:create orders-laravel orders \
    --ack-deadline=120 \
    --enable-ordering \
    --dead-letter

php artisan pubsub:subscriptions:create analytics-processor analytics \
    --ack-deadline=60 \
    --dead-letter

# Create webhook subscriptions
php artisan pubsub:subscriptions:create-push orders-webhook orders \
    https://api.mycompany.com/pubsub/webhook/orders \
    --token=production-secret-token \
    --ack-deadline=60 \
    --dead-letter
```

### Testing and Debugging

```bash
# List all resources
php artisan pubsub:topics:list
php artisan pubsub:subscriptions:list

# Test message flow
php artisan pubsub:publish test-topic '{"debug":true,"timestamp":"'$(date -Iseconds)'"}'
php artisan pubsub:listen test-subscription --topic=test-topic --max-messages=1

# Validate message formats
echo '{"order_id":123,"total":99.99}' | php artisan pubsub:schema:validate order_events

# Test webhook locally
php artisan pubsub:subscriptions:create-push local-test orders \
    https://$(hostname).ngrok.io/pubsub/webhook/orders \
    --token=test-token
```

### Monitoring and Maintenance

```bash
# Check subscription status
php artisan pubsub:subscriptions:list --topic=orders

# Test connectivity
php artisan pubsub:topics:list

# Validate configuration
php artisan pubsub:publish health-check '{"status":"ok","timestamp":"'$(date -Iseconds)'"}'
```

## Command Options Reference

### Global Options

All commands support standard Laravel options:

- `--env=ENV` : Set environment
- `--quiet` : Suppress output
- `--verbose` : Increase verbosity
- `--help` : Show help

### Exit Codes

- `0` : Success
- `1` : General error
- `2` : Invalid arguments
- `3` : Authentication error
- `4` : Permission denied

### Environment Variables

Commands respect these environment variables:

```bash
# Override project for specific command
GOOGLE_CLOUD_PROJECT_ID=different-project php artisan pubsub:topics:list

# Use different credentials
GOOGLE_APPLICATION_CREDENTIALS=/path/to/other/creds.json php artisan pubsub:publish test '{"data":"value"}'

# Force emulator usage
PUBSUB_EMULATOR_HOST=localhost:8085 php artisan pubsub:topics:create test-topic
```

## Scripting and Automation

### Bash Scripts

```bash
#!/bin/bash
# setup-pubsub.sh

# Create topics
for topic in orders analytics notifications; do
    php artisan pubsub:topics:create "$topic"
done

# Create subscriptions
php artisan pubsub:subscriptions:create orders-laravel orders --dead-letter
php artisan pubsub:subscriptions:create analytics-processor analytics

echo "Pub/Sub setup complete!"
```

### CI/CD Integration

```yaml
# .github/workflows/deploy.yml
- name: Setup Pub/Sub Resources
  run: |
    php artisan pubsub:topics:create orders --enable-ordering
    php artisan pubsub:subscriptions:create orders-production orders \
      --ack-deadline=120 \
      --enable-ordering \
      --dead-letter
```

### Health Checks

```bash
#!/bin/bash
# health-check.sh

# Test publishing
if php artisan pubsub:publish health-check '{"status":"ok"}' > /dev/null 2>&1; then
    echo "✓ Publishing works"
else
    echo "✗ Publishing failed"
    exit 1
fi

# Test schema validation
if echo '{"order_id":123}' | php artisan pubsub:schema:validate order_events > /dev/null 2>&1; then
    echo "✓ Schema validation works"
else
    echo "✗ Schema validation failed"
    exit 1
fi
```

## Troubleshooting Commands

### Connection Issues

```bash
# Test basic connectivity
php artisan pubsub:topics:list

# Test with verbose output
php artisan pubsub:topics:list --verbose

# Test with specific credentials
GOOGLE_APPLICATION_CREDENTIALS=/path/to/creds.json php artisan pubsub:topics:list
```

### Permission Issues

```bash
# Test publishing (requires publisher role)
php artisan pubsub:publish test-topic '{"test":true}'

# Test listing (requires viewer role)
php artisan pubsub:topics:list

# Test subscription creation (requires admin role)
php artisan pubsub:subscriptions:create test-sub test-topic
```

### Emulator Issues

```bash
# Ensure emulator is running
gcloud beta emulators pubsub start &

# Test with emulator
PUBSUB_EMULATOR_HOST=localhost:8085 php artisan pubsub:topics:list

# Create test resources in emulator
PUBSUB_EMULATOR_HOST=localhost:8085 php artisan pubsub:topics:create test
PUBSUB_EMULATOR_HOST=localhost:8085 php artisan pubsub:publish test '{"data":"test"}'
```

## Best Practices

### Command Naming

Use consistent naming conventions:

```bash
# Environment-specific suffixes
php artisan pubsub:subscriptions:create orders-staging orders
php artisan pubsub:subscriptions:create orders-production orders

# Service-specific suffixes
php artisan pubsub:subscriptions:create orders-laravel orders
php artisan pubsub:subscriptions:create orders-go-service orders
php artisan pubsub:subscriptions:create orders-python-worker orders
```

### Resource Management

```bash
# Use dead letter topics for important subscriptions
php artisan pubsub:subscriptions:create critical-orders orders --dead-letter

# Use appropriate ack deadlines
php artisan pubsub:subscriptions:create fast-processor orders --ack-deadline=30
php artisan pubsub:subscriptions:create slow-processor orders --ack-deadline=300

# Enable ordering only when needed
php artisan pubsub:subscriptions:create sequential-orders orders --enable-ordering
```

### Development vs Production

```bash
# Development: Auto-create with short timeouts
php artisan pubsub:subscriptions:create dev-orders orders --ack-deadline=10

# Production: Explicit creation with longer timeouts and dead letters
php artisan pubsub:subscriptions:create prod-orders orders \
    --ack-deadline=120 \
    --dead-letter
```
