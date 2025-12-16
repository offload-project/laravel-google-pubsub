# Installation

## Requirements

- PHP 8.4 or higher
- Laravel 12.0 or higher
- Google Cloud Project with Pub/Sub API enabled
- Service Account credentials (for production)

## Installation

Install the package via Composer:

```bash
composer require offload-project/laravel-google-pubsub
```

## Configuration

See [Configuration](configuration.md) for full config coverage.

### 1. Publish the Configuration File

```bash
php artisan vendor:publish --provider="OffloadProject\GooglePubSub\PubSubServiceProvider" --tag="config"
```

This will create `config/pubsub.php` with all available options.

### 2. Environment Configuration

Add the following to your `.env` file:

```dotenv
# Basic Configuration
QUEUE_CONNECTION=pubsub
GOOGLE_CLOUD_PROJECT_ID=your-project-id

# Authentication (choose one method)
# Method 1: Service Account Key File
PUBSUB_AUTH_METHOD=key_file
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# Method 2: Application Default Credentials
PUBSUB_AUTH_METHOD=application_default

# Optional Settings
PUBSUB_DEFAULT_QUEUE=default
PUBSUB_AUTO_CREATE_TOPICS=true
PUBSUB_AUTO_CREATE_SUBSCRIPTIONS=true
```

### 3. Queue Configuration

Add the Pub/Sub connection to `config/queue.php`:

```php
'connections' => [
    'pubsub' => [
        'driver' => 'pubsub',
        'project_id' => env('GOOGLE_CLOUD_PROJECT_ID'),
        'queue' => env('PUBSUB_DEFAULT_QUEUE', 'default'),
        'auth_method' => env('PUBSUB_AUTH_METHOD', 'application_default'),
        'key_file' => env('GOOGLE_APPLICATION_CREDENTIALS'),
        
        // Optional overrides
        'auto_create_topics' => true,
        'auto_create_subscriptions' => true,
        'subscription_suffix' => '-laravel',
        'enable_message_ordering' => false,
    ],
],
```

## Authentication Methods

### Service Account (Recommended for Production)

1. Create a service account in Google Cloud Console
2. Download the JSON key file
3. Grant the service account these roles:
    - `Pub/Sub Publisher`
    - `Pub/Sub Subscriber`
    - `Pub/Sub Admin` (if auto-creating topics/subscriptions)

4. Set the path in your environment:
   ```dotenv
   PUBSUB_AUTH_METHOD=key_file
   GOOGLE_APPLICATION_CREDENTIALS=/secure/path/to/service-account.json
   ```

### Application Default Credentials

For Google Cloud environments (GKE, Cloud Run, App Engine):

```dotenv
PUBSUB_AUTH_METHOD=application_default
```

The SDK will automatically use the environment's credentials.

### Local Development with Emulator

For local development without real Google Cloud resources, this package fully supports the Pub/Sub emulator:

1. **Install the Pub/Sub emulator**:
   ```bash
   gcloud components install pubsub-emulator
   gcloud beta emulators pubsub start
   ```

2. **Automatic Detection**: Set the emulator host for expected automatic detection:
   ```dotenv
   PUBSUB_EMULATOR_HOST=localhost:8085
   ```

3. **Full Feature Support**: All package features work with the emulator:
   ```bash
   # Create topics and subscriptions
   php artisan pubsub:topics:create test-topic
   php artisan pubsub:subscriptions:create test-sub test-topic
   
   # Test publishing and consuming
   php artisan pubsub:publish test-topic '{"test": "data"}'
   php artisan pubsub:listen test-sub
   ```

4. **Docker Integration**:
   ```yaml
   # docker-compose.yml
   services:
     pubsub-emulator:
       image: google/cloud-sdk:alpine
       command: gcloud beta emulators pubsub start --host-port=0.0.0.0:8085
       ports:
         - "8085:8085"
   ```

## Verifying Installation

Run the following artisan command to verify your setup:

```bash
php artisan pubsub:topics:list
```

If everything is configured correctly, you should see a list of topics (or an empty list if none exist).

## Next Steps

- [More comprehensive configuration](configuration.md)
- [Set up your queues](../queue-driver.md)
- [Enable event integration](../event-integration.md)
- [Explore publisher/subscriber features](../direct-pubsub.md)
