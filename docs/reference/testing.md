# Testing

Comprehensive testing strategies for Pub/Sub integration, unit tests to integration testing with the Google Cloud
Pub/Sub emulator.

## Test Environment Setup

### Using the Pub/Sub Emulator

The Google Cloud Pub/Sub emulator provides a local testing environment without requiring actual Google Cloud resources.

#### 1. Install the Emulator

```bash
# Install Google Cloud SDK if not already installed
curl https://sdk.cloud.google.com | bash

# Install the Pub/Sub emulator
gcloud components install pubsub-emulator
```

#### 2. Start the Emulator

```bash
# Start the emulator (runs on localhost:8085 by default)
gcloud beta emulators pubsub start

# In another terminal, set the environment variable
export PUBSUB_EMULATOR_HOST=localhost:8085
```

#### 3. Configure Your Tests

```php
// tests/TestCase.php
namespace Tests;

use Orchestra\Testbench\TestCase as Orchestra;
use OffloadProject\GooglePubSub\PubSubServiceProvider;

abstract class TestCase extends Orchestra
{
    protected function setUp(): void
    {
        parent::setUp();
        
        // Use emulator for testing
        $this->app['config']->set('pubsub.emulator_host', 'localhost:8085');
        $this->app['config']->set('pubsub.project_id', 'test-project');
    }
    
    protected function getPackageProviders($app): array
    {
        return [PubSubServiceProvider::class];
    }
}
```

### Docker Setup

For CI/CD environments, use Docker:

```yaml
# docker-compose.test.yml
version: '3.8'
services:
    pubsub-emulator:
        image: google/cloud-sdk:alpine
        command: gcloud beta emulators pubsub start --host-port=0.0.0.0:8085
        ports:
            - "8085:8085"
        environment:
            - PUBSUB_PROJECT_ID=test-project
```

```bash
# Start emulator in CI
docker-compose -f docker-compose.test.yml up -d pubsub-emulator
export PUBSUB_EMULATOR_HOST=localhost:8085
```

## Unit Testing

### Testing Queue Jobs

```php
// tests/Unit/Jobs/ProcessOrderJobTest.php
namespace Tests\Unit\Jobs;

use Tests\TestCase;
use App\Jobs\ProcessOrderJob;
use App\Models\Order;
use Illuminate\Support\Facades\Queue;

class ProcessOrderJobTest extends TestCase
{
    public function test_job_can_be_dispatched()
    {
        Queue::fake();
        
        $order = Order::factory()->create();
        
        ProcessOrderJob::dispatch($order);
        
        Queue::assertPushed(ProcessOrderJob::class, function ($job) use ($order) {
            return $job->order->id === $order->id;
        });
    }
    
    public function test_job_processes_order_correctly()
    {
        $order = Order::factory()->create(['status' => 'pending']);
        
        $job = new ProcessOrderJob($order);
        $job->handle();
        
        $this->assertEquals('processing', $order->fresh()->status);
    }
    
    public function test_job_handles_failure_gracefully()
    {
        $order = Order::factory()->create();
        
        // Mock external service failure
        $this->mock(ExternalService::class, function ($mock) {
            $mock->shouldReceive('processOrder')->andThrow(new Exception('Service unavailable'));
        });
        
        $job = new ProcessOrderJob($order);
        
        $this->expectException(Exception::class);
        $job->handle();
    }
}
```

### Testing Publishers

```php
// tests/Unit/PublisherTest.php
namespace Tests\Unit;

use Tests\TestCase;
use OffloadProject\GooglePubSub\Facades\PubSub;

class PublisherTest extends TestCase
{
    public function test_can_publish_message()
    {
        $messageId = PubSub::publish('test-topic', [
            'test' => 'data',
            'timestamp' => now()->timestamp,
        ]);
        
        $this->assertNotNull($messageId);
        $this->assertIsString($messageId);
    }
    
    public function test_can_publish_with_attributes()
    {
        $messageId = PubSub::publish('test-topic', ['test' => 'data'], [
            'priority' => 'high',
            'source' => 'test-suite',
        ]);
        
        $this->assertNotNull($messageId);
    }
    
    public function test_can_publish_with_ordering_key()
    {
        $messageId = PubSub::publish('test-topic', ['test' => 'data'], [], [
            'ordering_key' => 'test-key-123',
        ]);
        
        $this->assertNotNull($messageId);
    }
    
    public function test_publishes_batch_messages()
    {
        $messages = [
            ['data' => ['event' => 'test1']],
            ['data' => ['event' => 'test2'], 'attributes' => ['priority' => 'high']],
            ['data' => ['event' => 'test3']],
        ];
        
        $messageIds = PubSub::publishBatch('test-topic', $messages);
        
        $this->assertCount(3, $messageIds);
        $this->assertContainsOnly('string', $messageIds);
    }
}
```

### Testing Subscribers

```php
// tests/Unit/SubscriberTest.php
namespace Tests\Unit;

use Tests\TestCase;
use OffloadProject\GooglePubSub\Facades\PubSub;

class SubscriberTest extends TestCase
{
    public function test_can_subscribe_to_topic()
    {
        // First, publish a message
        PubSub::publish('test-topic', ['test' => 'subscriber-data']);
        
        // Create subscriber
        $subscriber = PubSub::subscribe('test-subscription', 'test-topic');
        
        $receivedData = null;
        $subscriber->handler(function ($data, $message) use (&$receivedData) {
            $receivedData = $data;
        });
        
        // Pull messages
        $messages = $subscriber->pull(1);
        
        $this->assertNotEmpty($messages);
        $this->assertEquals(['test' => 'subscriber-data'], $receivedData);
    }
    
    public function test_handles_message_attributes()
    {
        PubSub::publish('test-topic', ['test' => 'data'], ['priority' => 'high']);
        
        $subscriber = PubSub::subscribe('test-subscription', 'test-topic');
        
        $receivedAttributes = null;
        $subscriber->handler(function ($data, $message) use (&$receivedAttributes) {
            $receivedAttributes = $message->attributes();
        });
        
        $subscriber->pull(1);
        
        $this->assertEquals('high', $receivedAttributes['priority']);
    }
    
    public function test_error_handler_is_called_on_exception()
    {
        PubSub::publish('test-topic', ['test' => 'data']);
        
        $subscriber = PubSub::subscribe('test-subscription', 'test-topic');
        
        $errorHandled = false;
        $subscriber->handler(function ($data, $message) {
            throw new Exception('Processing failed');
        });
        
        $subscriber->onError(function ($error, $message) use (&$errorHandled) {
            $errorHandled = true;
            $this->assertEquals('Processing failed', $error->getMessage());
        });
        
        $subscriber->pull(1);
        
        $this->assertTrue($errorHandled);
    }
}
```

## Integration Testing

### Testing Event Integration

```php
// tests/Integration/EventIntegrationTest.php
namespace Tests\Integration;

use Tests\TestCase;
use Illuminate\Support\Facades\Event;
use OffloadProject\GooglePubSub\Facades\PubSub;
use OffloadProject\GooglePubSub\Contracts\ShouldPublishToPubSub;

class OrderCreatedEvent implements ShouldPublishToPubSub
{
    public function __construct(public array $orderData) {}
    
    public function pubsubTopic(): string
    {
        return 'orders';
    }
    
    public function toPubSub(): array
    {
        return $this->orderData;
    }
}

class EventIntegrationTest extends TestCase
{
    public function test_events_are_published_to_pubsub()
    {
        // Enable event publishing
        config(['pubsub.events.enabled' => true]);
        
        $orderData = ['order_id' => 123, 'total' => 99.99];
        
        // Dispatch event
        event(new OrderCreatedEvent($orderData));
        
        // Verify message was published
        $subscriber = PubSub::subscribe('test-subscription', 'orders');
        $messages = $subscriber->pull(1);
        
        $this->assertNotEmpty($messages);
        
        // Verify event content
        $subscriber->handler(function ($data, $message) use ($orderData) {
            $this->assertEquals('OrderCreatedEvent', $data['class']);
            $this->assertEquals($orderData, $data['data']);
        });
        
        $subscriber->pull(1);
    }
    
    public function test_pubsub_messages_trigger_laravel_events()
    {
        Event::fake();
        
        // Publish a message that should trigger an event
        PubSub::publish('orders', [
            'order_id' => 456,
            'status' => 'shipped',
        ], ['event_type' => 'order.shipped']);
        
        // Simulate webhook processing
        $subscriber = PubSub::subscribe('orders-subscription', 'orders');
        $subscriber->handler(function ($data, $message) {
            // This would normally be handled by the webhook controller
            event('pubsub.orders.order.shipped', [
                'data' => $data,
                'message' => $message,
                'topic' => 'orders',
            ]);
        });
        
        $subscriber->pull(1);
        
        Event::assertDispatched('pubsub.orders.order.shipped');
    }
}
```

### Testing Webhooks

```php
// tests/Integration/WebhookTest.php
namespace Tests\Integration;

use Tests\TestCase;
use Illuminate\Support\Facades\Event;

class WebhookTest extends TestCase
{
    public function test_webhook_endpoint_processes_pubsub_message()
    {
        Event::fake();
        
        $payload = [
            'message' => [
                'messageId' => 'test-123',
                'data' => base64_encode(json_encode([
                    'order_id' => 789,
                    'status' => 'completed',
                ])),
                'attributes' => ['event_type' => 'order.completed'],
                'publishTime' => now()->toIso8601String(),
            ],
        ];
        
        $response = $this->postJson('/pubsub/webhook/orders', $payload, [
            'X-Goog-Resource-State' => 'exists',
            'X-Goog-Message-Id' => 'test-123',
            'X-Goog-Subscription-Name' => 'test-subscription',
        ]);
        
        $response->assertOk();
        Event::assertDispatched('pubsub.orders.order.completed');
    }
    
    public function test_webhook_requires_valid_headers()
    {
        $response = $this->postJson('/pubsub/webhook/orders', []);
        
        $response->assertStatus(401);
    }
    
    public function test_webhook_verifies_auth_token()
    {
        config(['pubsub.webhook.auth_token' => 'secret-token']);
        
        $response = $this->postJson('/pubsub/webhook/orders', [], [
            'X-Goog-Resource-State' => 'exists',
            'X-Goog-Message-Id' => 'test-123',
            'X-Goog-Subscription-Name' => 'test-subscription',
            'Authorization' => 'Bearer wrong-token',
        ]);
        
        $response->assertStatus(401);
    }
}
```

## Testing with Real Google Cloud

### Service Account Setup

For integration testing with real Google Cloud:

```php
// tests/Integration/GoogleCloudTest.php
namespace Tests\Integration;

use Tests\TestCase;

class GoogleCloudTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        
        // Skip if no credentials available
        if (!env('GOOGLE_APPLICATION_CREDENTIALS')) {
            $this->markTestSkipped('Google Cloud credentials not available');
        }
        
        // Use test project
        $this->app['config']->set('pubsub.project_id', 'my-test-project');
        $this->app['config']->set('pubsub.emulator_host', null);
    }
    
    public function test_can_connect_to_real_pubsub()
    {
        $topics = PubSub::topics();
        
        $this->assertIsArray($topics);
    }
    
    public function test_end_to_end_message_flow()
    {
        $testTopic = 'test-topic-' . uniqid();
        $testSubscription = 'test-subscription-' . uniqid();
        
        try {
            // Create topic and subscription
            PubSub::createTopic($testTopic);
            PubSub::createSubscription($testSubscription, $testTopic);
            
            // Publish message
            $messageId = PubSub::publish($testTopic, ['test' => 'real-cloud']);
            $this->assertNotNull($messageId);
            
            // Subscribe and receive
            $subscriber = PubSub::subscribe($testSubscription);
            $receivedData = null;
            
            $subscriber->handler(function ($data) use (&$receivedData) {
                $receivedData = $data;
            });
            
            // Allow time for message delivery
            sleep(1);
            $messages = $subscriber->pull(1);
            
            $this->assertEquals(['test' => 'real-cloud'], $receivedData);
            
        } finally {
            // Cleanup
            try {
                $client = PubSub::client();
                $client->subscription($testSubscription)->delete();
                $client->topic($testTopic)->delete();
            } catch (Exception $e) {
                // Ignore cleanup errors
            }
        }
    }
}
```

## Testing Best Practices

### 1. Use Factories for Test Data

```php
// tests/Factories/OrderFactory.php
use Illuminate\Database\Eloquent\Factories\Factory;

class OrderFactory extends Factory
{
    public function definition(): array
    {
        return [
            'customer_id' => $this->faker->numberBetween(1, 1000),
            'total' => $this->faker->randomFloat(2, 10, 1000),
            'status' => 'pending',
            'items' => [
                ['product_id' => 1, 'quantity' => 2, 'price' => 29.99],
                ['product_id' => 2, 'quantity' => 1, 'price' => 39.99],
            ],
        ];
    }
}
```

### 2. Mock External Dependencies

```php
// tests/Unit/Services/OrderServiceTest.php
namespace Tests\Unit\Services;

use Tests\TestCase;
use App\Services\OrderService;
use App\Services\PaymentService;
use OffloadProject\GooglePubSub\Facades\PubSub;

class OrderServiceTest extends TestCase
{
    public function test_creates_order_and_publishes_event()
    {
        // Mock external payment service
        $this->mock(PaymentService::class, function ($mock) {
            $mock->shouldReceive('processPayment')->andReturn(true);
        });
        
        // Mock PubSub
        PubSub::shouldReceive('publish')
            ->once()
            ->with('orders', Mockery::type('array'))
            ->andReturn('msg-123');
        
        $orderService = new OrderService();
        $order = $orderService->createOrder([
            'customer_id' => 123,
            'total' => 99.99,
        ]);
        
        $this->assertNotNull($order);
        $this->assertEquals('pending', $order->status);
    }
}
```

### 3. Test Schema Validation

```php
// tests/Unit/SchemaValidationTest.php
namespace Tests\Unit;

use Tests\TestCase;
use OffloadProject\GooglePubSub\Schema\SchemaValidator;
use OffloadProject\GooglePubSub\Exceptions\SchemaValidationException;

class SchemaValidationTest extends TestCase
{
    public function test_validates_order_schema()
    {
        $validator = new SchemaValidator([
            'schemas' => [
                'order' => [
                    'schema' => [
                        'type' => 'object',
                        'required' => ['order_id', 'total'],
                        'properties' => [
                            'order_id' => ['type' => 'integer'],
                            'total' => ['type' => 'number', 'minimum' => 0],
                        ],
                    ],
                ],
            ],
        ]);
        
        // Valid data
        $validData = (object) ['order_id' => 123, 'total' => 99.99];
        $validator->validate($validData, 'order');
        
        // Invalid data
        $invalidData = (object) ['order_id' => 'invalid', 'total' => -10];
        
        $this->expectException(SchemaValidationException::class);
        $validator->validate($invalidData, 'order');
    }
}
```

### 4. Test Error Handling

```php
// tests/Unit/ErrorHandlingTest.php
namespace Tests\Unit;

use Tests\TestCase;
use OffloadProject\GooglePubSub\Facades\PubSub;
use OffloadProject\GooglePubSub\Exceptions\PublishException;

class ErrorHandlingTest extends TestCase
{
    public function test_handles_publish_errors_gracefully()
    {
        // Force an error by using invalid project ID
        config(['pubsub.project_id' => 'invalid-project-id']);
        config(['pubsub.emulator_host' => null]); // Disable emulator
        
        $this->expectException(PublishException::class);
        
        PubSub::publish('test-topic', ['test' => 'data']);
    }
    
    public function test_subscriber_error_handler_is_called()
    {
        PubSub::publish('test-topic', ['test' => 'data']);
        
        $subscriber = PubSub::subscribe('test-subscription', 'test-topic');
        
        $errorCaught = false;
        $subscriber->handler(function ($data, $message) {
            throw new Exception('Handler error');
        });
        
        $subscriber->onError(function ($error, $message) use (&$errorCaught) {
            $errorCaught = true;
            $this->assertEquals('Handler error', $error->getMessage());
        });
        
        $subscriber->pull(1);
        
        $this->assertTrue($errorCaught);
    }
}
```

## Continuous Integration

### GitHub Actions Example

```yaml
# .github/workflows/tests.yml
name: Tests

on: [ push, pull_request ]

jobs:
    test:
        runs-on: ubuntu-latest

        services:
            pubsub-emulator:
                image: google/cloud-sdk:alpine
                ports:
                    - 8085:8085
                options: --entrypoint sh
                env:
                    PUBSUB_PROJECT_ID: test-project
                # Start emulator
                run: |
                    gcloud beta emulators pubsub start --host-port=0.0.0.0:8085 &
                    sleep 5

        steps:
            -   uses: actions/checkout@v3

            -   name: Setup PHP
                uses: shivammathur/setup-php@v2
                with:
                    php-version: '8.4'
                    extensions: dom, curl, libxml, mbstring, zip

            -   name: Install dependencies
                run: composer install --prefer-dist --no-progress

            -   name: Run tests
                env:
                    PUBSUB_EMULATOR_HOST: localhost:8085
                    PUBSUB_PROJECT_ID: test-project
                run: vendor/bin/phpunit
```

## Performance Testing

### Load Testing

```php
// tests/Performance/LoadTest.php
namespace Tests\Performance;

use Tests\TestCase;
use OffloadProject\GooglePubSub\Facades\PubSub;

class LoadTest extends TestCase
{
    public function test_can_handle_burst_publishing()
    {
        $messageCount = 100;
        $messages = [];
        
        for ($i = 0; $i < $messageCount; $i++) {
            $messages[] = [
                'data' => ['message_id' => $i, 'timestamp' => microtime(true)],
                'attributes' => ['batch' => 'load-test'],
            ];
        }
        
        $startTime = microtime(true);
        
        // Publish in batches
        $batches = array_chunk($messages, 10);
        foreach ($batches as $batch) {
            PubSub::publishBatch('load-test-topic', $batch);
        }
        
        $endTime = microtime(true);
        $duration = $endTime - $startTime;
        
        $this->assertLessThan(5.0, $duration, 'Publishing should complete within 5 seconds');
        
        // Verify all messages were received
        $subscriber = PubSub::subscribe('load-test-subscription', 'load-test-topic');
        $receivedCount = 0;
        
        $subscriber->handler(function ($data, $message) use (&$receivedCount) {
            $receivedCount++;
        });
        
        // Pull all messages
        while ($receivedCount < $messageCount) {
            $messages = $subscriber->pull(50);
            if (empty($messages)) {
                break;
            }
        }
        
        $this->assertEquals($messageCount, $receivedCount);
    }
}
```

## Debugging Tests

### Enable Verbose Logging

```php
// tests/TestCase.php
protected function setUp(): void
{
    parent::setUp();
    
    // Enable detailed logging for debugging
    if (env('TEST_DEBUG')) {
        config([
            'pubsub.monitoring.log_published_messages' => true,
            'pubsub.monitoring.log_consumed_messages' => true,
            'pubsub.monitoring.log_failed_messages' => true,
        ]);
    }
}
```

```bash
# Run tests with debug output
TEST_DEBUG=true vendor/bin/phpunit --verbose
```

### Test Utilities

```php
// tests/Utilities/PubSubTestHelpers.php
namespace Tests\Utilities;

use OffloadProject\GooglePubSub\Facades\PubSub;

trait PubSubTestHelpers
{
    protected function publishTestMessage(string $topic, array $data = null, array $attributes = []): string
    {
        $data = $data ?? ['test' => 'data', 'timestamp' => now()->timestamp];
        
        return PubSub::publish($topic, $data, array_merge([
            'source' => 'test-suite',
            'test_id' => uniqid(),
        ], $attributes));
    }
    
    protected function waitForMessage(string $subscription, int $timeoutSeconds = 10)
    {
        $subscriber = PubSub::subscribe($subscription);
        $receivedMessage = null;
        
        $subscriber->handler(function ($data, $message) use (&$receivedMessage) {
            $receivedMessage = ['data' => $data, 'message' => $message];
        });
        
        $startTime = time();
        while (time() - $startTime < $timeoutSeconds) {
            $messages = $subscriber->pull(1);
            if ($receivedMessage) {
                break;
            }
            usleep(100000); // 100ms
        }
        
        return $receivedMessage;
    }
    
    protected function cleanupTestResources(array $topics = [], array $subscriptions = []): void
    {
        $client = PubSub::client();
        
        foreach ($subscriptions as $subscription) {
            try {
                $client->subscription($subscription)->delete();
            } catch (Exception $e) {
                // Ignore cleanup errors
            }
        }
        
        foreach ($topics as $topic) {
            try {
                $client->topic($topic)->delete();
            } catch (Exception $e) {
                // Ignore cleanup errors
            }
        }
    }
}
```

Use the helpers in your tests:

```php
class MyTest extends TestCase
{
    use PubSubTestHelpers;
    
    public function test_message_flow()
    {
        $messageId = $this->publishTestMessage('test-topic', ['order_id' => 123]);
        
        $received = $this->waitForMessage('test-subscription');
        
        $this->assertNotNull($received);
        $this->assertEquals(123, $received['data']['order_id']);
    }
}
```
