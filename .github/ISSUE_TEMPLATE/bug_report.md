---
name: Bug Report
about: Report a bug in laravel-google-pubsub
title: "[Bug]: "
labels: bug
assignees: ''
---

### Description

A clear and concise description of the bug.

### Steps to Reproduce

Provide a minimal code sample or steps that reproduce the issue.

```php
// e.g. publishing a message, dispatching a queued job, handling a webhook, etc.
```

1. Configure '...'
2. Call '...'
3. See the error.

### Expected Behavior

Explain what you expected to happen.

### Actual Behavior

What actually happened? Include stack traces, exception messages, or relevant log output.

```
// Paste stack trace / error output here
```

### Environment

- Package version: [e.g., 1.2.0]
- Laravel version: [e.g., 11.x, 12.x, 13.x]
- PHP version: [e.g., 8.3, 8.4, 8.5]
- google/cloud-pubsub version: [e.g., 2.16]
- Running against: [Google Cloud Pub/Sub | Emulator (version)]
- OS: [e.g., macOS, Linux, Windows/WSL]

### Relevant Configuration

If applicable, share your `config/pubsub.php` overrides, queue/topic configuration, schema, or webhook setup. **Redact any service account credentials or auth tokens before sharing.**

### Additional Context

Add any other context about the problem here (auth method, dead-letter setup, ordering, streaming vs pull, push/webhook delivery, Octane, etc.).
