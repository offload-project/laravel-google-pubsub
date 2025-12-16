# Examples

Real-world examples demonstrating how to use Laravel Google Pub/Sub for common use cases and architectural patterns.

## E-commerce Order Processing

A complete order processing system with multiple services subscribing to order events.

### Order Service (Publisher)

```php
// app/Services/OrderService.php
namespace App\Services;

use App\Events\OrderCreated;
use App\Events\OrderUpdated;
use App\Events\OrderCancelled;
use App\Models\Order;
use Illuminate\Support\Facades\DB;

class OrderService
{
    public function createOrder(array $data): Order
    {
        return DB::transaction(function () use ($data) {
            $order = Order::create([
                'customer_id' => $data['customer_id'],
                'total' => $data['total'],
                'status' => 'pending',
                'items' => $data['items'],
            ]);
            
            // This automatically publishes to the 'orders' topic
            event(new OrderCreated($order));
            
            return $order;
        });
    }
    
    public function updateOrderStatus(Order $order, string $status): Order
    {
        $oldStatus = $order->status;
        $order->update(['status' => $status]);
        
        event(new OrderUpdated($order, $oldStatus));
        
        return $order;
    }
    
    public function cancelOrder(Order $order, string $reason = null): Order
    {
        $order->update(['status' => 'cancelled']);
        
        event(new OrderCancelled($order, $reason));
        
        return $order;
    }
}
```

### Order Events

```php
// app/Events/OrderCreated.php
namespace App\Events;

use App\Models\Order;
use OffloadProject\GooglePubSub\Attributes\PublishTo;
use OffloadProject\GooglePubSub\Contracts\ShouldPublishToPubSub;

#[PublishTo('orders')]
class OrderCreated implements ShouldPublishToPubSub
{
    public function __construct(
        public Order $order
    ) {}
    
    public function pubsubTopic(): string
    {
        return 'orders';
    }
    
    public function toPubSub(): array
    {
        return [
            'event_type' => 'order.created',
            'order_id' => $this->order->id,
            'customer_id' => $this->order->customer_id,
            'total' => $this->order->total,
            'items' => $this->order->items,
            'status' => $this->order->status,
            'created_at' => $this->order->created_at->toIso8601String(),
        ];
    }
    
    public function pubsubAttributes(): array
    {
        return [
            'event_type' => 'order.created',
            'customer_id' => (string) $this->order->customer_id,
            'priority' => $this->order->total > 1000 ? 'high' : 'normal',
            'source' => 'order-service',
        ];
    }
    
    public function pubsubOrderingKey(): string
    {
        return "customer-{$this->order->customer_id}";
    }
}
```

### Inventory Service (Subscriber)

```php
// app/Services/InventoryService.php
namespace App\Services;

use App\Models\Product;
use App\Models\StockReservation;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

class InventoryService
{
    public function reserveItems(array $items, int $orderId): bool
    {
        return DB::transaction(function () use ($items, $orderId) {
            foreach ($items as $item) {
                $product = Product::find($item['product_id']);
                
                if (!$product || $product->stock < $item['quantity']) {
                    Log::warning('Insufficient stock for reservation', [
                        'product_id' => $item['product_id'],
                        'requested' => $item['quantity'],
                        'available' => $product->stock ?? 0,
                        'order_id' => $orderId,
                    ]);
                    
                    return false;
                }
                
                // Reserve stock
                $product->decrement('stock', $item['quantity']);
                
                // Create reservation record
                StockReservation::create([
                    'order_id' => $orderId,
                    'product_id' => $item['product_id'],
                    'quantity' => $item['quantity'],
                    'expires_at' => now()->addHours(24),
                ]);
            }
            
            return true;
        });
    }
    
    public function releaseReservation(int $orderId): void
    {
        $reservations = StockReservation::where('order_id', $orderId)->get();
        
        DB::transaction(function () use ($reservations) {
            foreach ($reservations as $reservation) {
                // Return stock
                Product::where('id', $reservation->product_id)
                    ->increment('stock', $reservation->quantity);
                
                // Delete reservation
                $reservation->delete();
            }
        });
    }
}
```

### Event Listeners

```php
// app/Listeners/ProcessOrderEvents.php
namespace App\Listeners;

use App\Services\InventoryService;
use App\Services\PaymentService;
use App\Services\NotificationService;
use Illuminate\Support\Facades\Log;

class ProcessOrderEvents
{
    public function __construct(
        private InventoryService $inventoryService,
        private PaymentService $paymentService,
        private NotificationService $notificationService
    ) {}
    
    public function handleOrderCreated(string $eventName, array $payload): void
    {
        $event = $payload[0];
        $data = $event['data'];
        
        Log::info('Processing order created event', [
            'order_id' => $data['order_id'],
            'customer_id' => $data['customer_id'],
        ]);
        
        // Reserve inventory
        $reserved = $this->inventoryService->reserveItems(
            $data['items'],
            $data['order_id']
        );
        
        if (!$reserved) {
            Log::error('Failed to reserve inventory', [
                'order_id' => $data['order_id'],
            ]);
            
            // Publish inventory failed event
            PubSub::publish('inventory-events', [
                'event_type' => 'reservation.failed',
                'order_id' => $data['order_id'],
                'reason' => 'insufficient_stock',
            ]);
            
            return;
        }
        
        // Process payment
        $paymentResult = $this->paymentService->processPayment(
            $data['customer_id'],
            $data['total']
        );
        
        if ($paymentResult['success']) {
            // Send confirmation
            $this->notificationService->sendOrderConfirmation(
                $data['customer_id'],
                $data['order_id']
            );
        } else {
            // Release reservation
            $this->inventoryService->releaseReservation($data['order_id']);
        }
    }
    
    public function handleOrderCancelled(string $eventName, array $payload): void
    {
        $event = $payload[0];
        $data = $event['data'];
        
        Log::info('Processing order cancelled event', [
            'order_id' => $data['order_id'],
            'reason' => $data['reason'],
        ]);
        
        // Release inventory reservation
        $this->inventoryService->releaseReservation($data['order_id']);
        
        // Process refund if payment was taken
        if ($data['status'] === 'paid') {
            $this->paymentService->processRefund($data['order_id']);
        }
        
        // Send cancellation notification
        $this->notificationService->sendOrderCancellation(
            $data['customer_id'],
            $data['order_id'],
            $data['reason']
        );
    }
}
```

### Register Event Listeners

```php
// app/Providers/EventServiceProvider.php
namespace App\Providers;

use App\Listeners\ProcessOrderEvents;
use Illuminate\Foundation\Support\Providers\EventServiceProvider as ServiceProvider;

class EventServiceProvider extends ServiceProvider
{
    protected $listen = [
        'pubsub.orders.order.created' => [
            [ProcessOrderEvents::class, 'handleOrderCreated'],
        ],
        'pubsub.orders.order.cancelled' => [
            [ProcessOrderEvents::class, 'handleOrderCancelled'],
        ],
    ];
}
```

### Configuration

```php
// config/pubsub.php
return [
    'events' => [
        'enabled' => true,
        'publish' => [
            \App\Events\OrderCreated::class,
            \App\Events\OrderUpdated::class,
            \App\Events\OrderCancelled::class,
        ],
    ],
    
    'topics' => [
        'orders' => [
            'enable_message_ordering' => true,
            'schema' => 'order_events',
        ],
    ],
    
    'schemas' => [
        'order_events' => [
            'file' => 'schemas/order-events.json',
        ],
    ],
];
```

## Real-time Analytics Pipeline

Track user behavior and process analytics in real-time.

### Analytics Events

```php
// app/Services/AnalyticsService.php
namespace App\Services;

use OffloadProject\GooglePubSub\Facades\PubSub;
use Illuminate\Support\Facades\Auth;

class AnalyticsService
{
    public function trackPageView(string $page, array $metadata = []): void
    {
        $this->track('page_view', array_merge([
            'page' => $page,
            'url' => request()->url(),
            'referrer' => request()->header('referer'),
            'user_agent' => request()->header('user-agent'),
        ], $metadata));
    }
    
    public function trackEvent(string $event, array $data = []): void
    {
        $this->track($event, $data);
    }
    
    public function trackConversion(string $type, float $value, array $metadata = []): void
    {
        $this->track('conversion', array_merge([
            'type' => $type,
            'value' => $value,
            'currency' => 'USD',
        ], $metadata));
    }
    
    private function track(string $event, array $data): void
    {
        PubSub::publish('analytics', [
            'event' => $event,
            'data' => $data,
            'timestamp' => now()->timestamp,
            'session_id' => session()->getId(),
            'user_id' => Auth::id(),
            'ip_address' => request()->ip(),
        ], [
            'event_type' => $event,
            'user_id' => Auth::id() ? (string) Auth::id() : 'anonymous',
            'session_id' => session()->getId(),
        ]);
    }
}
```

### Analytics Middleware

```php
// app/Http/Middleware/TrackAnalytics.php
namespace App\Http\Middleware;

use App\Services\AnalyticsService;
use Closure;
use Illuminate\Http\Request;

class TrackAnalytics
{
    public function __construct(
        private AnalyticsService $analytics
    ) {}
    
    public function handle(Request $request, Closure $next)
    {
        $response = $next($request);
        
        // Track page views for successful GET requests
        if ($request->isMethod('GET') && $response->getStatusCode() === 200) {
            $this->analytics->trackPageView(
                $request->path(),
                ['status_code' => $response->getStatusCode()]
            );
        }
        
        return $response;
    }
}
```

### Analytics Processor

```php
// app/Console/Commands/ProcessAnalytics.php
namespace App\Console\Commands;

use Illuminate\Console\Command;
use OffloadProject\GooglePubSub\Facades\PubSub;
use App\Services\AnalyticsProcessor;

class ProcessAnalytics extends Command
{
    protected $signature = 'analytics:process {--batch-size=100}';
    protected $description = 'Process analytics events from Pub/Sub';
    
    public function handle(AnalyticsProcessor $processor): void
    {
        $subscriber = PubSub::subscribe('analytics-processor', 'analytics');
        
        $subscriber->handler(function ($data, $message) use ($processor) {
            $processor->process($data);
        });
        
        $subscriber->onError(function ($error, $message) {
            $this->error('Analytics processing error: ' . $error->getMessage());
        });
        
        $this->info('Starting analytics processor...');
        
        // Use streaming for real-time processing
        $subscriber->stream([
            'max_messages_per_pull' => $this->option('batch-size'),
        ]);
    }
}
```

### Analytics Processor Service

```php
// app/Services/AnalyticsProcessor.php
namespace App\Services;

use App\Models\AnalyticsEvent;
use App\Models\UserSession;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Log;

class AnalyticsProcessor
{
    public function process(array $data): void
    {
        try {
            // Store raw event
            AnalyticsEvent::create([
                'event' => $data['event'],
                'data' => $data['data'],
                'user_id' => $data['user_id'],
                'session_id' => $data['session_id'],
                'timestamp' => $data['timestamp'],
                'ip_address' => $data['ip_address'],
            ]);
            
            // Update real-time metrics
            $this->updateMetrics($data);
            
            // Process specific event types
            match($data['event']) {
                'page_view' => $this->processPageView($data),
                'conversion' => $this->processConversion($data),
                'user_signup' => $this->processUserSignup($data),
                default => null,
            };
            
        } catch (\Exception $e) {
            Log::error('Analytics processing failed', [
                'error' => $e->getMessage(),
                'data' => $data,
            ]);
            
            throw $e; // Re-throw to trigger retry
        }
    }
    
    private function updateMetrics(array $data): void
    {
        $date = date('Y-m-d', $data['timestamp']);
        $hour = date('Y-m-d H:00:00', $data['timestamp']);
        
        // Daily metrics
        Cache::increment("analytics:daily:{$date}:events");
        Cache::increment("analytics:daily:{$date}:events:{$data['event']}");
        
        // Hourly metrics
        Cache::increment("analytics:hourly:{$hour}:events");
        
        // Real-time counters
        Cache::increment('analytics:realtime:events', 1);
        Cache::expire('analytics:realtime:events', 300); // 5 minutes
    }
    
    private function processPageView(array $data): void
    {
        $page = $data['data']['page'];
        $sessionId = $data['session_id'];
        
        // Update session data
        UserSession::updateOrCreate(
            ['session_id' => $sessionId],
            [
                'user_id' => $data['user_id'],
                'last_activity' => $data['timestamp'],
                'page_views' => \DB::raw('page_views + 1'),
            ]
        );
        
        // Track popular pages
        Cache::increment("analytics:popular_pages:{$page}");
    }
    
    private function processConversion(array $data): void
    {
        $type = $data['data']['type'];
        $value = $data['data']['value'];
        
        // Update conversion metrics
        Cache::increment("analytics:conversions:{$type}:count");
        Cache::increment("analytics:conversions:{$type}:value", $value);
        
        // Track conversion funnel
        if ($data['user_id']) {
            Cache::sadd("analytics:converted_users:{$type}", $data['user_id']);
        }
    }
    
    private function processUserSignup(array $data): void
    {
        // Track signup metrics
        Cache::increment('analytics:signups:count');
        
        // Attribution tracking
        $referrer = $data['data']['referrer'] ?? 'direct';
        Cache::increment("analytics:signups:source:{$referrer}");
    }
}
```

## Microservice Communication

Example of how different microservices communicate through Pub/Sub.

### User Service (Laravel)

```php
// app/Services/UserService.php
namespace App\Services;

use App\Models\User;
use OffloadProject\GooglePubSub\Facades\PubSub;

class UserService
{
    public function createUser(array $data): User
    {
        $user = User::create($data);
        
        // Publish user created event
        PubSub::publish('user-events', [
            'event_type' => 'user.created',
            'user_id' => $user->id,
            'email' => $user->email,
            'name' => $user->name,
            'created_at' => $user->created_at->toIso8601String(),
        ], [
            'event_type' => 'user.created',
            'user_id' => (string) $user->id,
            'source' => 'user-service',
        ]);
        
        return $user;
    }
    
    public function updateProfile(User $user, array $data): User
    {
        $oldEmail = $user->email;
        $user->update($data);
        
        // Publish profile updated event
        PubSub::publish('user-events', [
            'event_type' => 'user.profile_updated',
            'user_id' => $user->id,
            'changes' => $data,
            'old_email' => $oldEmail,
            'new_email' => $user->email,
            'updated_at' => now()->toIso8601String(),
        ], [
            'event_type' => 'user.profile_updated',
            'user_id' => (string) $user->id,
            'email_changed' => $oldEmail !== $user->email ? 'true' : 'false',
        ]);
        
        return $user;
    }
}
```

### Email Service (Laravel)

```php
// app/Services/EmailService.php
namespace App\Services;

use App\Jobs\SendWelcomeEmail;
use App\Jobs\SendEmailVerification;
use App\Jobs\SendProfileUpdateNotification;
use Illuminate\Support\Facades\Log;

class EmailService
{
    public function handleUserCreated(string $eventName, array $payload): void
    {
        $event = $payload[0];
        $data = $event['data'];
        
        Log::info('Processing user created event for email', [
            'user_id' => $data['user_id'],
            'email' => $data['email'],
        ]);
        
        // Send welcome email
        SendWelcomeEmail::dispatch($data['user_id'], $data['email'], $data['name']);
        
        // Send email verification
        SendEmailVerification::dispatch($data['user_id'], $data['email']);
    }
    
    public function handleProfileUpdated(string $eventName, array $payload): void
    {
        $event = $payload[0];
        $data = $event['data'];
        $message = $event['message'];
        
        Log::info('Processing profile updated event for email', [
            'user_id' => $data['user_id'],
            'email_changed' => $message->attributes()['email_changed'] === 'true',
        ]);
        
        // Send email verification if email changed
        if ($message->attributes()['email_changed'] === 'true') {
            SendEmailVerification::dispatch($data['user_id'], $data['new_email']);
        }
        
        // Send profile update notification
        SendProfileUpdateNotification::dispatch($data['user_id'], $data['changes']);
    }
}
```

### Notification Service (Go)

```go
// notification-service/main.go
package main

import (
    "context"
    "encoding/json"
    "log"
    
    "cloud.google.com/go/pubsub"
)

type UserEvent struct {
    EventType string                 `json:"event_type"`
    UserID    int                    `json:"user_id"`
    Email     string                 `json:"email"`
    Name      string                 `json:"name"`
    Changes   map[string]interface{} `json:"changes"`
    CreatedAt string                 `json:"created_at"`
}

func main() {
    ctx := context.Background()
    
    client, err := pubsub.NewClient(ctx, "your-project-id")
    if err != nil {
        log.Fatalf("Failed to create client: %v", err)
    }
    defer client.Close()
    
    // Subscribe to user events
    sub := client.Subscription("user-events-notification-service")
    
    log.Println("Listening for user events...")
    
    err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
        var event UserEvent
        if err := json.Unmarshal(msg.Data, &event); err != nil {
            log.Printf("Failed to unmarshal message: %v", err)
            msg.Nack()
            return
        }
        
        switch event.EventType {
        case "user.created":
            handleUserCreated(event)
        case "user.profile_updated":
            handleProfileUpdated(event)
        default:
            log.Printf("Unknown event type: %s", event.EventType)
        }
        
        msg.Ack()
    })
    
    if err != nil {
        log.Fatalf("Failed to receive messages: %v", err)
    }
}

func handleUserCreated(event UserEvent) {
    log.Printf("Sending push notification for new user: %d", event.UserID)
    
    // Send push notification
    notification := map[string]interface{}{
        "user_id": event.UserID,
        "title":   "Welcome!",
        "message": "Welcome to our platform, " + event.Name,
        "type":    "welcome",
    }
    
    sendPushNotification(notification)
}

func handleProfileUpdated(event UserEvent) {
    log.Printf("Sending push notification for profile update: %d", event.UserID)
    
    // Send push notification
    notification := map[string]interface{}{
        "user_id": event.UserID,
        "title":   "Profile Updated",
        "message": "Your profile has been successfully updated",
        "type":    "profile_update",
    }
    
    sendPushNotification(notification)
}

func sendPushNotification(notification map[string]interface{}) {
    // Implementation for sending push notifications
    // This could integrate with Firebase, APNs, etc.
    log.Printf("Push notification sent: %+v", notification)
}
```

### Analytics Service (Python)

```python
# analytics-service/main.py
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from google.cloud import pubsub_v1
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AnalyticsService:
    def __init__(self):
        self.subscriber = pubsub_v1.SubscriberClient()
        self.bigquery = bigquery.Client()
        self.table_id = "your-project.analytics.user_events"
        
    def process_user_event(self, message):
        try:
            data = json.loads(message.data.decode('utf-8'))
            
            # Insert into BigQuery
            rows_to_insert = [{
                'event_type': data['event_type'],
                'user_id': data['user_id'],
                'email': data['email'],
                'timestamp': data.get('created_at', data.get('updated_at')),
                'metadata': json.dumps(data)
            }]
            
            errors = self.bigquery.insert_rows_json(
                self.bigquery.get_table(self.table_id),
                rows_to_insert
            )
            
            if errors:
                logger.error(f"BigQuery insert errors: {errors}")
                message.nack()
                return
                
            # Update real-time metrics
            self.update_metrics(data)
            
            logger.info(f"Processed {data['event_type']} for user {data['user_id']}")
            message.ack()
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            message.nack()
    
    def update_metrics(self, data):
        # Update real-time metrics in Redis, Cloud Monitoring, etc.
        pass
    
    def start(self):
        subscription_path = self.subscriber.subscription_path(
            'your-project-id', 
            'user-events-analytics-service'
        )
        
        # Configure flow control
        flow_control = pubsub_v1.types.FlowControl(max_messages=100)
        
        logger.info("Starting analytics service...")
        
        streaming_pull_future = self.subscriber.pull(
            request={"subscription": subscription_path},
            callback=self.process_user_event,
            flow_control=flow_control,
        )
        
        # Keep the main thread running
        try:
            streaming_pull_future.result()
        except KeyboardInterrupt:
            streaming_pull_future.cancel()
            logger.info("Analytics service stopped")

if __name__ == "__main__":
    service = AnalyticsService()
    service.start()
```

## Event Sourcing Pattern

Implement event sourcing using Pub/Sub for audit trails and state reconstruction.

### Event Store

```php
// app/Services/EventStore.php
namespace App\Services;

use App\Models\Event;
use OffloadProject\GooglePubSub\Facades\PubSub;
use Illuminate\Support\Facades\DB;

class EventStore
{
    public function append(string $aggregateId, string $eventType, array $data, int $expectedVersion = -1): Event
    {
        return DB::transaction(function () use ($aggregateId, $eventType, $data, $expectedVersion) {
            // Check version for optimistic concurrency control
            if ($expectedVersion >= 0) {
                $currentVersion = Event::where('aggregate_id', $aggregateId)
                    ->max('version') ?? -1;
                
                if ($currentVersion !== $expectedVersion) {
                    throw new \Exception("Concurrency conflict. Expected version {$expectedVersion}, got {$currentVersion}");
                }
            }
            
            $version = Event::where('aggregate_id', $aggregateId)->max('version') + 1;
            
            // Store event
            $event = Event::create([
                'aggregate_id' => $aggregateId,
                'event_type' => $eventType,
                'data' => $data,
                'version' => $version,
                'occurred_at' => now(),
            ]);
            
            // Publish to Pub/Sub
            PubSub::publish('events', [
                'event_id' => $event->id,
                'aggregate_id' => $aggregateId,
                'event_type' => $eventType,
                'data' => $data,
                'version' => $version,
                'occurred_at' => $event->occurred_at->toIso8601String(),
            ], [
                'event_type' => $eventType,
                'aggregate_id' => $aggregateId,
                'version' => (string) $version,
            ]);
            
            return $event;
        });
    }
    
    public function getEvents(string $aggregateId, int $fromVersion = 0): array
    {
        return Event::where('aggregate_id', $aggregateId)
            ->where('version', '>=', $fromVersion)
            ->orderBy('version')
            ->get()
            ->toArray();
    }
    
    public function getEventsByType(string $eventType, int $limit = 100): array
    {
        return Event::where('event_type', $eventType)
            ->orderBy('occurred_at', 'desc')
            ->limit($limit)
            ->get()
            ->toArray();
    }
}
```

### Account Aggregate

```php
// app/Domain/Account.php
namespace App\Domain;

use App\Services\EventStore;

class Account
{
    private string $id;
    private string $email;
    private float $balance = 0.0;
    private bool $isActive = true;
    private int $version = -1;
    private array $uncommittedEvents = [];
    
    public function __construct(string $id, private EventStore $eventStore)
    {
        $this->id = $id;
    }
    
    public static function create(string $id, string $email, EventStore $eventStore): self
    {
        $account = new self($id, $eventStore);
        $account->apply([
            'event_type' => 'account.created',
            'data' => ['email' => $email],
        ]);
        
        return $account;
    }
    
    public function deposit(float $amount): void
    {
        if ($amount <= 0) {
            throw new \InvalidArgumentException('Amount must be positive');
        }
        
        $this->apply([
            'event_type' => 'account.deposit',
            'data' => ['amount' => $amount],
        ]);
    }
    
    public function withdraw(float $amount): void
    {
        if ($amount <= 0) {
            throw new \InvalidArgumentException('Amount must be positive');
        }
        
        if ($this->balance < $amount) {
            throw new \Exception('Insufficient funds');
        }
        
        $this->apply([
            'event_type' => 'account.withdrawal',
            'data' => ['amount' => $amount],
        ]);
    }
    
    public function deactivate(): void
    {
        if (!$this->isActive) {
            throw new \Exception('Account already deactivated');
        }
        
        $this->apply([
            'event_type' => 'account.deactivated',
            'data' => [],
        ]);
    }
    
    public function save(): void
    {
        foreach ($this->uncommittedEvents as $event) {
            $this->eventStore->append(
                $this->id,
                $event['event_type'],
                $event['data'],
                $this->version
            );
            
            $this->version++;
        }
        
        $this->uncommittedEvents = [];
    }
    
    public function loadFromHistory(): void
    {
        $events = $this->eventStore->getEvents($this->id);
        
        foreach ($events as $event) {
            $this->applyEvent($event['event_type'], $event['data']);
            $this->version = $event['version'];
        }
    }
    
    private function apply(array $event): void
    {
        $this->applyEvent($event['event_type'], $event['data']);
        $this->uncommittedEvents[] = $event;
    }
    
    private function applyEvent(string $eventType, array $data): void
    {
        match($eventType) {
            'account.created' => $this->email = $data['email'],
            'account.deposit' => $this->balance += $data['amount'],
            'account.withdrawal' => $this->balance -= $data['amount'],
            'account.deactivated' => $this->isActive = false,
            default => throw new \Exception("Unknown event type: {$eventType}"),
        };
    }
    
    // Getters
    public function getId(): string { return $this->id; }
    public function getEmail(): string { return $this->email; }
    public function getBalance(): float { return $this->balance; }
    public function isActive(): bool { return $this->isActive; }
    public function getVersion(): int { return $this->version; }
}
```

### Projection Builder

```php
// app/Services/ProjectionBuilder.php
namespace App\Services;

use App\Models\AccountProjection;
use Illuminate\Support\Facades\Log;

class ProjectionBuilder
{
    public function handleAccountEvent(string $eventName, array $payload): void
    {
        $event = $payload[0];
        $data = $event['data'];
        
        Log::info('Building projection for account event', [
            'event_type' => $data['event_type'],
            'aggregate_id' => $data['aggregate_id'],
        ]);
        
        match($data['event_type']) {
            'account.created' => $this->handleAccountCreated($data),
            'account.deposit' => $this->handleAccountDeposit($data),
            'account.withdrawal' => $this->handleAccountWithdrawal($data),
            'account.deactivated' => $this->handleAccountDeactivated($data),
            default => Log::warning("Unknown event type: {$data['event_type']}"),
        };
    }
    
    private function handleAccountCreated(array $data): void
    {
        AccountProjection::create([
            'account_id' => $data['aggregate_id'],
            'email' => $data['data']['email'],
            'balance' => 0.0,
            'is_active' => true,
            'version' => $data['version'],
        ]);
    }
    
    private function handleAccountDeposit(array $data): void
    {
        AccountProjection::where('account_id', $data['aggregate_id'])
            ->increment('balance', $data['data']['amount']);
    }
    
    private function handleAccountWithdrawal(array $data): void
    {
        AccountProjection::where('account_id', $data['aggregate_id'])
            ->decrement('balance', $data['data']['amount']);
    }
    
    private function handleAccountDeactivated(array $data): void
    {
        AccountProjection::where('account_id', $data['aggregate_id'])
            ->update(['is_active' => false]);
    }
}
```
