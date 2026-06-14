# Security Policy

## Supported versions

Security fixes are applied to the latest minor release line. Older minor versions may receive fixes for critical issues at the maintainers' discretion — when in doubt, please upgrade.

| Version       | Supported              |
| ------------- | ---------------------- |
| `1.1.x`       | ✅                     |
| `1.x` (older) | ⚠️ critical fixes only |
| `< 1.0`       | ❌ (please upgrade)    |

## Reporting a vulnerability

**Please do not open a public GitHub issue for security reports.**

Use [GitHub Security Advisories](https://github.com/offload-project/laravel-google-pubsub/security/advisories/new) to report privately. This lets us discuss, fix, and coordinate disclosure before details become public.

When reporting, please include:

- A description of the issue and its potential impact.
- Steps to reproduce, or a minimal proof-of-concept.
- Affected package version(s), Laravel version, PHP version, and `google/cloud-pubsub` version.
- Whether the issue reproduces against real Google Cloud Pub/Sub, the local emulator, or both.
- Any suggested fix or mitigation (optional).

**Please do not include real service account credentials, auth tokens, or production project IDs in your report. Redact them — a sanitized example payload is enough.**

## Response expectations

- **Acknowledgement:** within 5 business days.
- **Initial assessment:** within 10 business days.
- **Fix timeline:** depends on severity. Critical issues get prioritized; lower-severity issues may be batched into the next regular release.

We'll keep you updated on progress and credit you in the advisory unless you'd prefer to stay anonymous.

## Scope

Things in scope for this project:

- Vulnerabilities in any code published under `OffloadProject\GooglePubSub\` (facade, manager, publisher, subscriber, queue driver, webhook controller/middleware, schema validator, formatters, console commands, event integration).
- Webhook verification issues — bypasses of the `VerifyPubSubWebhook` middleware (IP allowlist, Google headers, Bearer auth token).
- Push subscription authentication issues — accepting messages that did not originate from Google Cloud Pub/Sub.
- Schema validation bypass — paths where `SchemaValidator` is silently skipped despite configuration requiring it.
- Message handling vulnerabilities — deserialization, decompression bombs, or path traversal via attributes/payload data.
- Information disclosure via exceptions, logs, or attribute metadata (e.g., leaking project IDs, ordering keys, or payload contents that should be redacted).
- Insecure defaults in the published config (`config/pubsub.php`), the queue driver, or the webhook routes.
- Credential handling issues — for example, the package writing service account JSON to disk in an unsafe location.

Things **not** in scope (please report upstream or with the relevant project):

- Vulnerabilities in `google/cloud-pubsub`, `google/auth`, `google/gax`, Laravel itself, or other Composer dependencies — please file with the respective project.
- Application-level misconfiguration in a consuming app (e.g., disabling webhook verification, exposing the webhook route without a token, broadcasting sensitive event payloads to a public topic, granting `roles/pubsub.admin` to a service account that doesn't need it).
- Issues caused by user-supplied implementations of the package's extension points (custom formatters, custom subscribers, overridden controllers).
- Vulnerabilities in the host application's authentication, mail driver, cache driver, database, or the Google Cloud project's IAM configuration.
- Pub/Sub emulator behavior that does not reproduce against real Google Cloud Pub/Sub (the emulator is best-effort and is not a production target).

## Disclosure

Once a fix is published, we will:

1. Publish a GitHub Security Advisory with details and credit.
2. Tag a patch release.
3. Update the changelog with a brief mention (without exploit details prior to the disclosure window).

Thanks for helping keep the project and its users safe.
