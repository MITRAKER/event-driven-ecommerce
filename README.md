# Event-Driven E-Commerce Backend

[![CI](https://github.com/MITRAKER/event-driven-ecommerce/actions/workflows/ci.yml/badge.svg)](https://github.com/MITRAKER/event-driven-ecommerce/actions/workflows/ci.yml)
[![CodeQL](https://github.com/MITRAKER/event-driven-ecommerce/actions/workflows/codeql.yml/badge.svg)](https://github.com/MITRAKER/event-driven-ecommerce/actions/workflows/codeql.yml)
[![Coding standard: NASA/JPL Power of Ten](https://img.shields.io/badge/coding%20standard-NASA%2FJPL%20Power%20of%20Ten-0B3D91)](docs/CODING_STANDARD.md)
[![TypeScript: strict](https://img.shields.io/badge/TypeScript-strict-3178C6?logo=typescript&logoColor=white)](tsconfig.base.json)
[![Node.js >= 26](https://img.shields.io/badge/node-%3E%3D26-339933?logo=node.js&logoColor=white)](.nvmrc)
[![Conventional Commits](https://img.shields.io/badge/Conventional%20Commits-1.0.0-fe5196?logo=conventionalcommits&logoColor=white)](https://www.conventionalcommits.org)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A small online-shop backend where microservices talk **only through Kafka events**. It's built to show the patterns that make event-driven systems reliable: the **transactional outbox**, **idempotent consumers**, a **choreographed saga with compensation**, a **CQRS read model**, and **retries with a dead-letter topic**. The code follows an adapted version of NASA/JPL's **"Power of Ten"** rules for safety-critical code, enforced by the compiler, the linter and CI.

**Stack:** TypeScript · NestJS · Kafka (Redpanda) · PostgreSQL · Docker Compose · Jest · GitHub Actions

> 📘 New to these patterns? [`LEARNING.md`](LEARNING.md) explains every step: what was built, why, and exercises to try.

## Architecture

```mermaid
flowchart LR
  client([Client]) -- POST /orders --> order[order-service]
  order -- OrderCreated --> K[(Kafka)]
  K -- OrderCreated --> inv[inventory-service]
  inv -- InventoryReserved / Rejected --> K
  K -- InventoryReserved --> pay[payment-service]
  pay -- PaymentSucceeded / Failed --> K
  K -- inventory + payment results --> order
  order -- OrderConfirmed / Cancelled --> K
  K -- OrderCancelled: release stock --> inv
  K -- all events --> query[order-query-service]
  client -- GET /orders/:id --> query

  order --- odb[(orders DB)]
  inv --- idb[(inventory DB)]
  pay --- pdb[(payments DB)]
  query --- qdb[(read model DB)]
```

| Service             | Port | Owns                                   | Listens to          | Publishes                                    |
| ------------------- | ---- | -------------------------------------- | ------------------- | -------------------------------------------- |
| order-service       | 3001 | orders                                 | inventory, payments | OrderCreated, OrderConfirmed, OrderCancelled |
| inventory-service   | 3002 | products (stock, prices), reservations | orders              | InventoryReserved, InventoryRejected         |
| payment-service     | 3003 | payments                               | inventory           | PaymentSucceeded, PaymentFailed              |
| order-query-service | 3004 | order_view (read model)                | everything          | nothing                                      |

Every service also answers `GET /health`, which checks its database.

## Patterns implemented

| Pattern                             | Where                                                  | Problem it solves                                                                                                         |
| ----------------------------------- | ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------- |
| Transactional outbox                | `packages/messaging/src/outbox.*`                      | Saving to the DB and publishing to Kafka can't be atomic (the dual-write problem)                                         |
| Idempotent consumer (inbox)         | `packages/messaging/src/inbox.ts`, `event.consumer.ts` | At-least-once delivery means duplicates. Processing each `eventId` once per service gives effectively-once processing     |
| Choreographed saga and compensation | `services/*/src/**/*consumer.ts`                       | No distributed transactions across services. Failures are undone by releasing stock                                       |
| CQRS read model                     | `services/order-query-service`                         | One fast, join-free view built from three services' events, rebuildable by replay                                         |
| Order-independent projection        | `order-query-service/src/projection.ts`                | Kafka orders messages only within a partition, and the view must not depend on arrival order                              |
| Retry, backoff and DLQ              | `packages/messaging/src/retry.ts`, `errors.ts`         | Outages are retried and redelivered, never dead-lettered. Poison messages are parked in `<topic>.dlq` instead of blocking |
| Pessimistic locking                 | inventory `FOR UPDATE` in sku order                    | Two orders can't buy the last item, and reserve and release can't deadlock                                                |
| Server-side price check             | inventory catalog (`002_add_prices.sql`)               | A client can't choose its own price: orders with tampered prices are rejected                                             |
| Parse at the boundary               | `packages/contracts/src/parse-event.ts`, DTOs, config  | Untrusted input (Kafka messages, HTTP bodies, env vars) is checked once and typed from then on                            |
| Functional core / imperative shell  | `nextState`, `decideReservation`, `charge`, `project`  | Business rules are pure functions that are easy to test                                                                   |

## Run it

```bash
docker compose up -d --build --wait   # Kafka, Postgres, Redpanda Console and all 4 services, healthy
npm run test:e2e                      # end-to-end checks against the running system
```

Then try it:

```bash
# confirmed
curl -X POST localhost:3001/orders -H 'content-type: application/json' \
  -d '{"customerId":"c-1","items":[{"sku":"BOOK","quantity":1,"unitPriceCents":4500}]}'
# payment declined (over $1,000), so the order is cancelled and stock is released
curl -X POST localhost:3001/orders -H 'content-type: application/json' \
  -d '{"customerId":"c-1","items":[{"sku":"LAPTOP","quantity":1,"unitPriceCents":150000}]}'
# tampered price: the inventory service owns the catalog, so the order is cancelled
curl -X POST localhost:3001/orders -H 'content-type: application/json' \
  -d '{"customerId":"c-1","items":[{"sku":"LAPTOP","quantity":1,"unitPriceCents":1}]}'

curl localhost:3004/orders/<orderId>      # status + event timeline
curl localhost:3004/customers/c-1/orders
curl localhost:3004/stats
curl localhost:3002/products              # stock and prices
```

Watch the events live in Redpanda Console at http://localhost:8080.

**Develop locally** (infrastructure in Docker, services on your machine):

```bash
docker compose up -d redpanda redpanda-console postgres
npm ci && npm run build
npm run start:order | start:inventory | start:payment | start:query   # one terminal each
npm run check                                                          # every quality gate, like CI
```

## Quality and CI/CD

The repository is set up the way a professional team would run it:

- **Coding standard:** the NASA/JPL "Power of Ten" rules adapted to TypeScript: no recursion, bounded loops and retries, small functions, runtime assertions (`invariant()`), every input parsed at the boundary, zero warnings. [`docs/CODING_STANDARD.md`](docs/CODING_STANDARD.md) explains each rule and what enforces it.
- **Static analysis:** the strictest TypeScript settings (`noUncheckedIndexedAccess`, `exactOptionalPropertyTypes`, ...) and type-aware ESLint (`strictTypeChecked`) plus the JPL rules and a custom no-recursion rule, all with `--max-warnings 0`.
- **Tests:** Jest unit tests across every workspace with coverage thresholds that fail the build, and a black-box end-to-end test against the real system in Docker.
- **CI on every pull request:** format, build, lint, type-check, tests with coverage, a Conventional PR title, hadolint, gitleaks, `npm audit`, dependency review, CodeQL (also daily), and the end-to-end test.
- **Releases:** [release-please](https://github.com/googleapis/release-please) turns Conventional Commits into a release PR with the version bump and [`CHANGELOG.md`](CHANGELOG.md). Merging it tags the release and publishes the four service images to GitHub Container Registry.
- **Maintenance:** Dependabot opens weekly update PRs. Git hooks (husky, lint-staged, commitlint) run the fast checks before each commit.

## Project layout

```
packages/contracts     event types, parseEvent(), invariant(), Limits
packages/messaging     outbox, inbox, consumer base class, retry and DLQ, config, DB and Kafka wiring
services/*             the four services (each with its own migrations/)
contracts/events       envelope JSON schema and event catalog
tests/e2e              black-box end-to-end test (used in CI)
tools/eslint-rules     custom ESLint rule (no recursion)
docs                   coding standard
.github                CI/CD workflows, Dependabot, issue forms, PR template
```

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for setup, the quality gates and the commit convention. Please follow the [Code of Conduct](CODE_OF_CONDUCT.md), and report security issues privately as described in [`SECURITY.md`](SECURITY.md).

## License

[MIT](LICENSE)
