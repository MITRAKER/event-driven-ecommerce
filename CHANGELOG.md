# Changelog

All notable changes to this project are documented in this file.

From version 0.1.0 on, this file is maintained by [release-please](https://github.com/googleapis/release-please). It writes each release's entries from the [Conventional Commits](https://www.conventionalcommits.org/) merged since the previous release and inserts the new version just below this note, above the newest version heading. Please don't edit those sections by hand: fix the commit or pull request title instead. The project follows [semantic versioning](https://semver.org/).

<!-- release-please inserts each new version above the first heading that starts with a version number, so keep "0.0.0" at the start of the heading below. -->

## 0.0.0 (before the first release)

The project was built in six steps before the first release. Each step has a lesson in [`LEARNING.md`](LEARNING.md).

- **Step 1: Local infrastructure and event contracts.** Docker Compose with Redpanda (Kafka API), Redpanda Console and PostgreSQL (one database per service), plus the event envelope JSON schema and the event catalog in `contracts/events/`.
- **Step 2: Order service and the transactional outbox.** `POST /orders` and `GET /orders/:id`. The order and its `OrderCreated` event are saved in one database transaction, and an outbox relay publishes the event to Kafka, which solves the dual-write problem. Shared event types in `packages/contracts`.
- **Step 3: Inventory, payment and the saga with compensation.** The inventory service reserves stock (locking products in sku order) and the payment service charges a simulated card. The order service settles each order as `CONFIRMED` or `CANCELLED`, and a cancellation releases the reserved stock. Idempotent consumers (an inbox keyed by `eventId`) and shared plumbing in `packages/messaging`.
- **Step 4: CQRS read model.** The order query service consumes all three topics into one `order_view` per order, with an event timeline, and serves `GET /orders/:id`, `GET /customers/:customerId/orders` and `GET /stats`. The projection gives the same result whatever order the events arrive in.
- **Step 5: Reliability, Docker, end-to-end tests and CI.** Retries with exponential backoff, dead-letter topics (`<topic>.dlq`), explicit topic creation, one Dockerfile for all services, a black-box end-to-end test, and GitHub Actions CI.
- **Step 6: Professional standards.** The NASA/JPL "Power of Ten" rules adapted to TypeScript ([`docs/CODING_STANDARD.md`](docs/CODING_STANDARD.md)): the strictest compiler settings, type-aware ESLint with zero warnings and a custom no-recursion rule, `invariant()` assertions, `Limits`, and `parseEvent()` at the Kafka boundary. Failures are now classified as permanent, infrastructure or unknown, so a database outage never dead-letters healthy events. Fixes from a code review: server-side price checks against the catalog, a lost update in the read model, a deadlock in stock release, an integer overflow in `/stats`, a crash on idle connection errors, shutdown ordering, and migrations that now run exactly once. Also: `GET /health` on every service, Jest coverage thresholds, git hooks with commitlint, CI with CodeQL, dependency review, gitleaks, hadolint and `npm audit`, release-please releases with images on GitHub Container Registry, Dependabot, and the community files (contributing guide, security policy, code of conduct, issue forms, PR template).
