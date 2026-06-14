# Contributing to Laravel Google Pub/Sub

Thanks for your interest in contributing! This document outlines the process and standards for contributing to `offload-project/laravel-google-pubsub`.

## Code of Conduct

By participating in this project, you agree to treat fellow contributors with respect. Be kind, assume good intent, and keep discussions focused on the work. See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

## Ways to Contribute

- Reporting bugs via the [Bug Report](.github/ISSUE_TEMPLATE/bug_report.md) template
- Proposing new features via the [Feature Request](.github/ISSUE_TEMPLATE/feature_request.md) template
- Improving documentation (`README.md`, `docs/`, `CHANGELOG.md`)
- Fixing bugs or implementing features through pull requests
- Reviewing open pull requests

Before opening a large PR, please open an issue first to discuss the approach.

## Requirements

- PHP **8.3+** (CI matrix runs 8.3, 8.4, 8.5)
- Composer 2
- Either a Google Cloud project with the Pub/Sub API enabled, **or** the [Pub/Sub emulator](https://cloud.google.com/pubsub/docs/emulator) for local development

## Getting Set Up

1. Fork the repository on GitHub and clone your fork:

   ```bash
   git clone git@github.com:<your-username>/laravel-google-pubsub.git
   cd laravel-google-pubsub
   ```

2. Install dependencies:

   ```bash
   composer install
   ```

3. Install the Git hooks (runs Pint pre-commit, validates Conventional Commits on commit-msg, runs tests and static analysis pre-push):

   ```bash
   composer install-hooks
   ```

4. Create a feature branch off `main`:

   ```bash
   git checkout -b feat/short-description
   ```

## Development Workflow

This package supports Laravel 11, 12, and 13 and PHP 8.3–8.5. Changes must work across that matrix.

### Running the Test Suite

```bash
composer test
```

Run with coverage:

```bash
composer test-coverage
```

Tests are written with [Pest](https://pestphp.com/) and live under `tests/`. New behavior should be covered by tests; bug fixes should include a regression test.

If your change touches the queue driver, publisher, subscriber, or webhook flow, please verify against the [Pub/Sub emulator](https://cloud.google.com/pubsub/docs/emulator) where practical — mocked Google Cloud clients can hide protocol-level regressions.

### Static Analysis

```bash
composer analyse
```

We use Larastan (PHPStan for Laravel). If you must suppress a finding, prefer narrow ignores via the baseline over loosening the rule set, and explain why in your PR.

### Code Style

```bash
composer pint
```

Pint runs on `pre-commit`. PRs must be Pint-clean — the `code-style.yml` workflow will fail otherwise.

## Commit Messages

We use [Conventional Commits](https://www.conventionalcommits.org/). The `commit-msg` hook validates this; CI/release tooling depends on it.

Format: `<type>(<optional scope>): <description>`

Common types used in this repo:

| Type         | Use for                                                             |
| ------------ | ------------------------------------------------------------------- |
| `feat`       | New user-facing functionality                                       |
| `fix`        | Bug fixes                                                           |
| `deprecate`  | Marking existing API as deprecated                                  |
| `refactor`   | Internal change with no behavior difference                         |
| `test`       | Adding or updating tests                                            |
| `docs`       | Documentation only                                                  |
| `chore`      | Tooling, dependency bumps, repo housekeeping                        |
| `ci`         | Changes to GitHub Actions workflows                                 |

Examples (taken from this project's history):

- `fix: add keyFile to pubsubConfig from JSON file`
- `chore: add error handling for message pull operation`
- `ci: add merge_commit_sha to release workflow`
- `test: allow advisories for Laravel 11`

Breaking changes: add `!` after the type (e.g., `feat!: rename PubSub::publishBatch signature`) and explain the migration path in the PR body.

## Pull Requests

1. Make sure your branch is up to date with `main`.
2. Run the full local check before pushing:

   ```bash
   composer pint && composer analyse && composer test
   ```

3. Push your branch and open a PR against `main` using the [PR template](.github/pull_request_template.md).
4. Fill in:
   - What changed and why
   - Type of change (bug fix, feature, breaking, deprecation, etc.)
   - How it was tested (PHP/Laravel versions, real Pub/Sub vs emulator)
   - Whether docs or `CHANGELOG.md` were updated
5. Keep PRs focused. One logical change per PR makes review faster and bisection easier.
6. CI must pass before review:
   - `tests.yml` — Pest across the PHP × Laravel × stability matrix
   - `code-style.yml` — Pint
7. Address review feedback in additional commits rather than force-pushing while review is active.

## Adding or Changing Features

When working on this package, keep these areas in mind:

- **Facade & PubSubManager** — `PubSub` is the documented public entry point. Method renames or signature changes are breaking.
- **Contracts** — `PublisherInterface`, `ShouldPublishToPubSub`, and `MessageFormatter` are public extension points. Changing them is breaking.
- **Attributes** — `#[PublishTo]` is part of the public API for event integration. Adding optional params is non-breaking; renaming or removing is.
- **Queue driver** — `PubSubQueue` and `PubSubConnector` must remain compatible with Laravel's queue contracts across the supported Laravel versions.
- **Webhook surface** — `PubSubWebhookController` and `VerifyPubSubWebhook` are security-sensitive. Don't relax verification (IP allowlist, auth token, Google headers) without a strong reason and explicit call-out.
- **Schema validation** — Schemas configured via `pubsub.schemas` are part of the user contract. Don't change validation semantics (strict vs lax) silently.
- **Formatters** — `JsonFormatter` and `CloudEventsFormatter` follow `MessageFormatter`. CloudEvents output must remain v1.0-compliant.
- **Config** — new config keys must have safe defaults and be documented in `config/pubsub.php` with a comment.
- **Exceptions** — extend `PubSubException` for new failure modes. Be specific so callers can branch on them.

## Documentation

If your change affects public API, configuration, or usage, update:

- `README.md` — quick start / feature list
- `docs/implementation/`, `docs/messages/`, `docs/reference/`, and the relevant top-level `docs/*.md` entries (queue driver, direct pub/sub, event integration, webhook push, artisan commands)
- `CHANGELOG.md` — under the `Unreleased` section (or note in your PR if you'd like a maintainer to add it)

## Reporting Security Issues

Please do **not** open a public issue for security vulnerabilities. See [SECURITY.md](SECURITY.md) for the private reporting process.

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE.md) that covers this project.
