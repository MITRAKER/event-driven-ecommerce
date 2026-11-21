# Changelog

All notable changes to this project are documented in this file.

From version 0.1.0 on, this file is maintained by [release-please](https://github.com/googleapis/release-please). It writes each release's entries from the [Conventional Commits](https://www.conventionalcommits.org/) merged since the previous release and inserts the new version just below this note, above the newest version heading. Please don't edit those sections by hand: fix the commit or pull request title instead. The project follows [semantic versioning](https://semver.org/).

<!-- release-please inserts each new version above the first heading that starts with a version number, so keep "0.0.0" at the start of the heading below. -->

## 0.1.0 (2026-11-21)


### Features

* bring steps 3 to 5 to main (saga, CQRS read model, reliability and CI) ([e16edfc](https://github.com/MITRAKER/event-driven-ecommerce/commit/e16edfcc60bcd7221fa85efdc88cf0529e51d740))
* **inventory-service:** check order prices against the catalog ([201bfd7](https://github.com/MITRAKER/event-driven-ecommerce/commit/201bfd71a8f8103ad57cc7ef2a023608674768ea))
* **messaging:** run each migration exactly once ([529deff](https://github.com/MITRAKER/event-driven-ecommerce/commit/529deff8f8c601008558660ee36fbc42c2ea6e06))
* **order-service:** bound orders and settle the saga under a row lock ([2a67181](https://github.com/MITRAKER/event-driven-ecommerce/commit/2a67181a524d56203b76a717e093da52657d2a6b))
* **payment-service:** make the payment decision pure and configurable ([cc0c3d1](https://github.com/MITRAKER/event-driven-ecommerce/commit/cc0c3d136a87f1c557ee9f825b2b8d35b8c8fb1c))
* professional repo standards, NASA/JPL rules and CI/CD (step 6) ([ea998ce](https://github.com/MITRAKER/event-driven-ecommerce/commit/ea998ce41d07123dfe1afee89e12b95cd4f4f021))


### Bug Fixes

* **deps:** upgrade NestJS to 11 to patch body-parser, multer, qs, file-type ([d59b851](https://github.com/MITRAKER/event-driven-ecommerce/commit/d59b85116ce57699daaa7b16beba8409294fad4e))
* **messaging:** name the dead-letter producer's partitioner ([b9b1c15](https://github.com/MITRAKER/event-driven-ecommerce/commit/b9b1c15b12d3443624158ddd9b0cb747ca1ae114))
* **messaging:** reject blank and malformed numeric settings ([6d8ac78](https://github.com/MITRAKER/event-driven-ecommerce/commit/6d8ac780d7a8a32a5da4768bf1c1b05d2e4495b9))
* **messaging:** send the Kafka heartbeat before every try ([cb2b53b](https://github.com/MITRAKER/event-driven-ecommerce/commit/cb2b53be8b77b16c2bdeb011320265901639a744))
* **messaging:** skip unknown event types and heartbeat Kafka during retries ([3a9d7a3](https://github.com/MITRAKER/event-driven-ecommerce/commit/3a9d7a329d62c305e59156a93d7368b5ad4a6e91))
* **messaging:** treat a statement timeout as a database problem, not a bug ([0144350](https://github.com/MITRAKER/event-driven-ecommerce/commit/0144350e3e87ebceb6a7c3f75423bb2b0a7618cd))
* **messaging:** wait for the running batch on shutdown even after skipped ticks ([7e71459](https://github.com/MITRAKER/event-driven-ecommerce/commit/7e7145979bf7cacb00e399b122904d67c4356a81))
* **order-query-service:** lock-safe projection and exact stats ([15ad63d](https://github.com/MITRAKER/event-driven-ecommerce/commit/15ad63dfaab0899c8a044285a57e6527eaf525b6))


### Build and Dependencies

* **deps:** Bump node from 22-alpine to 26-alpine ([8ea7a35](https://github.com/MITRAKER/event-driven-ecommerce/commit/8ea7a3514bb1ae8ef3f31e63917b38669cdb7f09))
* **deps:** move to Node.js 26 LTS (image, CI and types) ([6c850c1](https://github.com/MITRAKER/event-driven-ecommerce/commit/6c850c14398aea1be42a77499028afd99fde67d0))
* enforce real coverage, test the lint rule, add git hooks ([edf8ede](https://github.com/MITRAKER/event-driven-ecommerce/commit/edf8eded29924f5f6e5a2f145532bdbb991f780a))
* run CI, development and types on Node.js 26 like the image ([7e47b8e](https://github.com/MITRAKER/event-driven-ecommerce/commit/7e47b8e8d38e5f286d4c2124433be5158a3edb5f))

## 0.0.0 (before the first release)

The project was built in six steps before the first release. Each step has a lesson in [`LEARNING.md`](LEARNING.md).

- **Step 1: Local infrastructure and event contracts.** Docker Compose with Redpanda (Kafka API), Redpanda Console and PostgreSQL (one database per service), plus the event envelope JSON schema and the event catalog in `contracts/events/`.
- **Step 2: Order service and the transactional outbox.** `POST /orders` and `GET /orders/:id`. The order and its `OrderCreated` event are saved in one database transaction, and an outbox relay publishes the event to Kafka, which solves the dual-write problem. Shared event types in `packages/contracts`.
- **Step 3: Inventory, payment and the saga with compensation.** The inventory service reserves stock (locking products in sku order) and the payment service charges a simulated card. The order service settles each order as `CONFIRMED` or `CANCELLED`, and a cancellation releases the reserved stock. Idempotent consumers (an inbox keyed by `eventId`) and shared plumbing in `packages/messaging`.
- **Step 4: CQRS read model.** The order query service consumes all three topics into one `order_view` per order, with an event timeline, and serves `GET /orders/:id`, `GET /customers/:customerId/orders` and `GET /stats`. The projection gives the same result whatever order the events arrive in.
- **Step 5: Reliability, Docker, end-to-end tests and CI.** Retries with exponential backoff, dead-letter topics (`<topic>.dlq`), explicit topic creation, one Dockerfile for all services, a black-box end-to-end test, and GitHub Actions CI.
- **Step 6: Professional standards.** The NASA/JPL "Power of Ten" rules adapted to TypeScript ([`docs/CODING_STANDARD.md`](docs/CODING_STANDARD.md)): the strictest compiler settings, type-aware ESLint with zero warnings and a custom no-recursion rule, `invariant()` assertions, `Limits`, and `parseEvent()` at the Kafka boundary. Failures are now classified as permanent, infrastructure or unknown, so a database outage never dead-letters healthy events. Fixes from a code review: server-side price checks against the catalog, a lost update in the read model, a deadlock in stock release, an integer overflow in `/stats`, a crash on idle connection errors, shutdown ordering, and migrations that now run exactly once. Also: `GET /health` on every service, Jest coverage thresholds, git hooks with commitlint, CI with CodeQL, dependency review, gitleaks, hadolint and `npm audit`, release-please releases with images on GitHub Container Registry, Dependabot, and the community files (contributing guide, security policy, code of conduct, issue forms, PR template).
