# Learning log: building an event-driven e-commerce backend

This file grows with every step. Each section covers what was built, why it was built that way, the key concepts, and how to try it yourself.

---

## Step 1: Local infrastructure and event contracts

### What we built

- `docker-compose.yml` starts three things:
  - **Redpanda**, a Kafka-compatible message broker. It is a single binary with no ZooKeeper, so it is much lighter on a laptop than real Kafka. Your code uses normal Kafka client libraries, so everything you learn applies to Kafka.
  - **Redpanda Console** at http://localhost:8080, a web UI where you can watch topics and messages flow. Seeing events with your own eyes is the fastest way to learn.
  - **PostgreSQL**, with one database per service (`infra/postgres-init.sql`).
- `contracts/events/` holds the **event envelope** schema and the **event catalog** (which service sends which event, and who listens).

### Why it is designed this way

1. **One database per service.** In microservices, each service owns its data. The inventory service may never read the orders table directly. If it needs order information, it learns it from events. This keeps services independent, so you can deploy, scale, or rewrite one without breaking the others. (In production these would be separate servers; one Postgres with four databases is a cheap way to simulate that.)
2. **Contracts first, code second.** Events are the public API between services. Writing them down before any code forces you to design the conversation between services, just like designing REST endpoints before implementing them.
3. **A shared envelope.** Every event carries the same metadata:
   - `eventId`: a unique id. Consumers store the ids they have processed, so if Kafka delivers the same message twice (it can!), they skip it. This is called **idempotency**.
   - `version`: lets you change an event's shape later without breaking old consumers.
   - `aggregateId`: the order id. We also use it as the **Kafka message key**. Kafka guarantees order only _within a partition_, and messages with the same key always go to the same partition. So all events of one order arrive in order.
   - `correlationId` / `causationId`: let you trace a whole chain ("this payment happened because of that reservation, which happened because of that order"). This is essential when debugging distributed systems.

### Key concepts

- **Event vs command.** A command says "do this" (`ReserveStock`) and can be refused. An event says "this happened" (`InventoryReserved`), is in the past tense, and is a fact. We mostly publish events.
- **Topic.** A named log of messages. Producers append, consumers read at their own pace and remember their position (the _offset_).
- **Consumer group.** Several instances of one service share a group id, so Kafka splits partitions between them. That is how you scale horizontally.
- **Saga (choreography style).** A business process that spans services without a distributed transaction. Each service reacts to events and publishes new ones. If a later step fails, earlier steps are undone by **compensating actions** (for example, payment fails, so inventory releases the stock). See the diagram in `contracts/events/README.md`.

### Try it yourself

```bash
docker compose up -d redpanda redpanda-console postgres   # just the infrastructure
# open http://localhost:8080, then create a topic called "orders"
docker compose exec redpanda rpk topic produce orders --key order-1
# type a JSON line and press Enter, then watch it appear in the Console
docker compose exec redpanda rpk topic consume orders
```

Exercise: produce three messages with key `order-1` and three with key `order-2`, and check in the Console which partition each landed in.

---

## Step 2: Order service and the transactional outbox

### What we built

```
packages/contracts/          shared TypeScript types for every event + createEvent() helper
services/order-service/
  migrations/001_init.sql    orders table + outbox table
  src/orders/                POST /orders, GET /orders/:id
  src/outbox/                addToOutbox() + OutboxRelay (outbox table -> Kafka)
  src/infra/                 Postgres pool with transaction() helper, Kafka producer
```

Flow: `POST /orders` -> one DB transaction inserts the order **and** an outbox row -> the relay (every 500 ms) reads unpublished outbox rows -> sends them to the `orders` topic -> marks them published.

### The problem this solves: the dual write

The naive version looks like this:

```ts
await db.query('INSERT INTO orders ...');
await kafka.send('OrderCreated'); // what if the process dies right here, or Kafka is down?
```

Two separate systems, two separate writes, and no way to make them atomic. If the app crashes between the lines, the order exists but no other service ever hears about it. Swap the order of the lines and you can announce an order that was never saved. This is the **dual-write problem**, and it is one of the most common interview questions about event-driven systems.

### The fix: the transactional outbox

Write the event into a table _in the same database transaction_ as the order (`orders.service.ts`). A database transaction is atomic, so either both rows exist or neither does. A separate background loop (`outbox.relay.ts`) then copies outbox rows to Kafka. If Kafka is down, rows simply wait in the table and get sent later. Nothing is lost.

### Design choices worth understanding

- **`FOR UPDATE SKIP LOCKED`** in the relay query. If you run three copies of the service, each relay locks a different batch of rows instead of all three sending the same events. This is a classic Postgres job-queue trick.
- **`ORDER BY id`** with a `BIGSERIAL` id sends events roughly in the order they were created. Roughly, because ids are handed out when a row is inserted, while transactions commit in their own order: id 12 can become visible before id 11. That's fine here, because order only matters _per order_ (per Kafka key), and one order's events are causally chained: each one is written only after the previous one was published and handled downstream, so they can never overtake each other.
- **At-least-once delivery.** If the relay crashes _after_ Kafka accepted a batch but _before_ the `UPDATE ... published_at`, that batch is sent again. You cannot get exactly-once for free across two systems, so the rule is: **producers may duplicate, and consumers must be idempotent.** We build idempotent consumers in Step 3.
- **Bounded producer retries** (`retry: { retries: 5 }` and `maxInFlightRequests: 1` in `kafka.ts`). The first version used Kafka's _idempotent producer_, which removes duplicates caused by the client's own network retries. In kafkajs that mode expects unlimited retries, and Step 6 makes every retry loop bounded, so we switched it off. Nothing is lost: if a send still fails, the rows stay unpublished and the next relay tick tries again, and consumers deduplicate anyway (they have to, because of the crash case above). `maxInFlightRequests: 1` keeps a retried batch from overtaking the next one.
- **HTTP 202 Accepted** instead of 201. The order is accepted, but whether it ends up confirmed or cancelled is decided later by other services. The client polls `GET /orders/:id` (or, later, the query service).
- **Plain SQL instead of an ORM.** For learning, seeing the exact `BEGIN / INSERT / INSERT / COMMIT` makes the pattern obvious. In a bigger codebase you might use Prisma or TypeORM. The pattern stays the same.
- **A shared `@shop/contracts` package.** Every service imports the same event types, so if a payload changes, the TypeScript compiler shows every place that breaks.

### Alternatives you should be able to name

- **Change Data Capture (CDC)** with Debezium. Instead of polling, Debezium reads Postgres's write-ahead log and streams outbox inserts to Kafka. It has lower latency and no polling load, but there's one more piece of infrastructure to run.
- **Listen to yourself.** Publish to Kafka first, and have the service consume its own event to update its database. It works, but the API can't immediately read back what it just wrote.

### How the tests prove it (`npm test -w @shop/order-service`)

- `orders.service.spec.ts`: the order insert and the outbox insert happen in the same transaction. The event's message key is the order id. If the order insert fails, no event is staged.
- `outbox.relay.spec.ts`: rows are grouped by topic and marked published. **If Kafka is down, rows stay unpublished and get retried.** No outbox rows means no Kafka call.

### Try it yourself

```bash
docker compose up -d redpanda redpanda-console postgres   # infrastructure only; the service runs on your machine
npm ci && npm run build
npm run start:order
curl -X POST localhost:3001/orders -H 'content-type: application/json' \
  -d '{"customerId":"c-1","items":[{"sku":"BOOK","quantity":2,"unitPriceCents":4500}]}'
```

(A plain `docker compose up -d` would also start the Dockerised services from Step 5, which already use ports 3001 to 3004.)

Watch the `orders` topic in Redpanda Console (http://localhost:8080).

Exercise: stop Kafka (`docker compose stop redpanda`), create three orders, then run `SELECT id, published_at FROM outbox;` in the `orders` database. Start Kafka again and watch the relay catch up. That's the outbox doing its job.

---

## Step 3: Inventory, payment, and the saga with compensation

### What we built

```
packages/messaging/            shared plumbing every service now reuses
  schema.ts                    outbox + processed_events tables
  inbox.ts                     claimEvent(): dedupe by eventId
  event.consumer.ts            base class: Kafka message -> BEGIN, claim, handle, COMMIT
  outbox.relay.ts, database.ts, kafka.ts   (moved here from order-service)
services/inventory-service     reserves stock on OrderCreated, releases it on OrderCancelled
services/payment-service       charges (fake) on InventoryReserved
services/order-service/src/saga  state machine + consumer that settles the order
```

### The whole flow

```
             order-service          inventory-service         payment-service
POST /orders ──► OrderCreated ──────► reserve stock
                                      ├─ ok ──► InventoryReserved ──► charge card
                                      │                               ├─ ok ──► PaymentSucceeded ─┐
                                      │                               └─ no ──► PaymentFailed ────┤
                                      └─ no ──► InventoryRejected ───────────────────────────────┤
                                                                                                  ▼
             order-service settles: PENDING ──► CONFIRMED  (OrderConfirmed)
                                     PENDING ──► CANCELLED  (OrderCancelled) ──► inventory releases stock
```

Try it: order a `LAPTOP` (price above $1,000) and payment declines. Watch `GET localhost:3002/products`: the laptop count goes down when it's reserved, then back up when the order is cancelled. That's **compensation**.

### Why a saga?

In a monolith you'd wrap "reserve stock + charge card + confirm order" in one database transaction. Here those are three services with three databases, so there's no shared transaction. (Two-phase commit exists, but it is slow, fragile, and Kafka doesn't support it.)

A **saga** replaces one big transaction with a chain of small local ones. Each step commits on its own and publishes an event. If a later step fails, earlier steps are **undone by compensating actions**, which are new business operations that reverse the effect ("release the stock"). It's not a rollback, because the stock really was held for a few moments.

### Choreography vs orchestration

- **Choreography (what we built):** no central boss. Each service reacts to events and emits new ones. It's loosely coupled and simple for 3–4 steps, but the overall flow is spread across services, so you need a diagram like the one above to see it.
- **Orchestration:** one "saga orchestrator" sends commands ("ReserveStock", "ChargeCard") and tracks state. The flow is easier to follow and change, but there's one more component, and it can become a "god service". Tools like Temporal are built for this.
- **Interview answer:** choreography for short flows, orchestration when the flow has many steps, branches, or timeouts.

### Idempotent consumers (the other half of Step 2's promise)

The outbox guarantees _at-least-once_, so consumers _will_ see duplicates sometimes. `EventConsumer.process()` does this in **one transaction**:

1. `INSERT INTO processed_events (event_id) ... ON CONFLICT DO NOTHING`. If 0 rows were inserted, it's a duplicate, so skip it.
2. Run the handler (business change + outbox rows).
3. `COMMIT`, then Kafka commits the offset.

Because the dedupe row and the business change commit together, a crash anywhere means _both_ are rolled back and the redelivery is processed cleanly. Together: **at-least-once delivery + idempotent processing = effectively exactly-once.** This sentence is worth memorising.

Extra safety nets, in layers:

- `payments.order_id` is the PRIMARY KEY, so the database itself refuses a second charge for one order.
- The inventory release only touches reservations with `status = 'RESERVED'`, so releasing twice is impossible.
- The order state machine only moves from `PENDING`, so a late `PaymentSucceeded` after a cancel does nothing.

### Concurrency details

- **`SELECT ... FOR UPDATE`** on product rows: two orders for the last keyboard can't both succeed. The second one waits for the first to commit, then sees 0 left.
- **`ORDER BY sku`** when locking: if order A locks BOOK then MUG while order B locks MUG then BOOK, they deadlock. Always locking in the same order prevents it. The rule has to hold on _every_ path that touches those rows: the release (compensation) gives stock back in sku order too. (The first version released in the order items were listed; a review caught it in Step 6.)
- **`CHECK (available >= 0)`**: the last line of defence, enforced by the database even if the code has a bug.
- **Message key = order id**: all events for one order land in one partition, so they're consumed in order.

### Design pattern: functional core, imperative shell

The decisions are **pure functions** with no database and no Kafka: `nextState()` (order), `decideReservation()` (inventory), and `charge()` (payment). The consumers are thin shells that load data, call the pure function, and write results. That's why the business rules have fast, simple unit tests (19 tests across the repo now).

### Why events carry data ("event-carried state transfer")

`InventoryReserved` includes the items and prices, so the payment service can compute the amount **without calling the order service**. If services call each other synchronously to fill gaps, you get a distributed monolith: one service down means everything is down.

### Try it yourself

```bash
docker compose up -d redpanda redpanda-console postgres && npm ci && npm run build
npm run start:inventory   # terminal 1
npm run start:payment     # terminal 2
npm run start:order       # terminal 3

# happy path -> CONFIRMED
curl -X POST localhost:3001/orders -H 'content-type: application/json' \
  -d '{"customerId":"c-1","items":[{"sku":"BOOK","quantity":1,"unitPriceCents":4500}]}'
# payment fails -> CANCELLED, and laptop stock comes back
curl -X POST localhost:3001/orders -H 'content-type: application/json' \
  -d '{"customerId":"c-1","items":[{"sku":"LAPTOP","quantity":1,"unitPriceCents":150000}]}'
# out of stock -> CANCELLED (only 2 keyboards exist)
curl -X POST localhost:3001/orders -H 'content-type: application/json' \
  -d '{"customerId":"c-1","items":[{"sku":"KEYBOARD","quantity":3,"unitPriceCents":9000}]}'

curl localhost:3001/orders/<orderId>     # status
curl localhost:3002/products             # stock
```

Exercises:

1. Stop the payment service, place an order, and check that it stays `PENDING`. Start payment again and watch it finish. Events wait in Kafka, so nothing is lost.
2. In Redpanda Console, follow one order's events by `correlationId` across all three topics.
3. Hard: add a `ShippingScheduled` step after `PaymentSucceeded`. Which compensations would a shipping failure need?

---

## Step 4: CQRS, the read side

### What we built

`services/order-query-service` (port 3004). It consumes **all three topics** and keeps one wide `order_view` row per order:

- `GET /orders/:id`: status, inventory result, payment result, cancel reason, and a full **event timeline** (an order-tracking page)
- `GET /customers/:customerId/orders`: a "my orders" page
- `GET /stats`: order count and revenue by status (a mini admin dashboard)

It has no outbox and publishes no business events. The only thing it ever writes outside its own database is a message it can't process, which goes to that topic's dead-letter topic (`orders.dlq`, for example; Step 5).

### What CQRS means

**Command Query Responsibility Segregation:** use different models (and here, different services and databases) for **writing** and for **reading**.

- **Command side** (order, inventory and payment services): normalised tables designed to enforce rules correctly: locks, constraints, state machines.
- **Query side** (this service): a denormalised table designed for what screens need, with zero joins. To show one order you'd otherwise need data from three services and three databases. Here it's one primary-key lookup.

The two sides are connected by events. The read model is a **projection**: a fold of the event stream (`events.reduce(project, emptyView)`), which is literally how the tests build it.

### Trade-offs to be able to explain

- **Eventual consistency.** Right after `POST /orders`, the query service may not have the order yet (a few milliseconds of lag). That's why `GET /orders/:id` returns a 404 that says "may not be projected yet". UIs handle this by showing "processing…", polling, or reading their own write from the command side.
- **More moving parts.** Don't use CQRS for a simple CRUD app. It pays off when reads and writes have very different shapes or loads (e.g. 100× more reads than writes, or reads that need data from several services).
- **Scaling.** Reads and writes scale separately: add read replicas or swap Postgres for Elasticsearch or Redis on the query side without touching the command side.

### The tricky part: ordering across topics

Kafka guarantees order **only within one partition**. Our events for one order are spread over three topics, so the projector may see `PaymentFailed` before `OrderCreated`. The projector (`projection.ts`) is written so **any arrival order gives the same final view**:

- each event fills in only the fields it owns
- a final status is never overwritten by an earlier-stage event
- the timeline is sorted by `occurredAt`, not by arrival (with the event type as a tie-break, so two events with the same timestamp always come out in the same order)

The test `gives the same view whatever the arrival order` replays the same events in four different orders (reversed, cancel first, reservation last, interleaved) and checks that every result is identical. It caught a real bug while I was writing it: a reservation arriving _after_ the cancel showed as `RESERVED` instead of `RELEASED`.

### Rebuilding a projection, a CQRS superpower

The view is **disposable**. Want a new column, or found a bug in the projector? Fix the code, empty the view **and the inbox** with `TRUNCATE order_view, processed_events;`, then start with a new consumer group (`PROJECTION_GROUP_ID=order-query-v2`) and it replays every topic from the beginning.

Why the inbox too? `processed_events` remembers every event id the projector has already applied (that's how it skips duplicates). If you keep it, every replayed event looks like a duplicate and the rebuilt view stays empty. And why a new group id? Kafka remembers how far each consumer group has read; a new group has no position yet, so it starts from the first message.

This only works if Kafka keeps events long enough, which is why event-sourced systems often set topic retention to "forever" or use compacted topics.

### CQRS vs event sourcing (often confused)

- **Event sourcing:** the _write side's_ source of truth is the event log itself (no `orders` table, just events, and current state = replay).
- **We are not event-sourced:** our write side stores current state in normal tables and publishes events via the outbox. CQRS and event sourcing are often used together, but each works without the other.

### Try it yourself

```bash
npm run build && npm run start:query          # alongside the other three services
curl localhost:3004/orders/<orderId>         # look at "timeline"
curl localhost:3004/customers/c-1/orders
curl localhost:3004/stats
```

Exercises:

1. Stop the query service, place 5 orders, start it again and watch it catch up.
2. Rebuild the view: stop the query service, run `docker compose exec postgres psql -U shop -d order_query -c 'TRUNCATE order_view, processed_events;'`, then start it again with `PROJECTION_GROUP_ID=order-query-v2 npm run start:query`. Every order comes back.
3. Add a `cancelledBecauseOutOfStock` count to `/stats` _without touching any other service_. That's the point of CQRS.

---

## Step 5: Reliability, Docker, end-to-end tests, CI

### What we built

- **Retries with exponential backoff** (`packages/messaging/src/retry.ts`): a failed event is tried up to 5 times, waiting 200, 400, 800 and then 1,600 ms in between (`CONSUMER_RETRY_ATTEMPTS` and `CONSUMER_RETRY_BASE_MS` change this).
- **Dead-letter topics** (`orders.dlq`, `inventory.dlq`, `payments.dlq`): after the last retry, the message is copied to `<topic>.dlq` with headers explaining what failed (`dlq-error`, original topic, partition, offset, consumer group), and the consumer moves on. (Step 6 refines which failures end up there: a database outage never does.)
- **Explicit topic creation** (`ensureTopics()`): 3 partitions per topic, created on startup by whichever service starts first.
- **One Dockerfile for all services** plus a full `docker-compose.yml` with health checks. `docker compose up -d --build` runs the whole system.
- **End-to-end test** (`tests/e2e/e2e.mjs`): a black-box test that drives the real system over HTTP. It covers the happy path, payment declined, out of stock, and checks that compensation restored the stock.
- **GitHub Actions CI** (`.github/workflows/ci.yml`): job 1 runs the build and unit tests, and job 2 starts everything in Docker and runs the end-to-end test.
- **README** with an architecture diagram and a pattern table, written for recruiters and interviewers.

### Why retries AND a dead-letter queue?

There are two very different kinds of failure:

- **Transient**, like the DB restarting or a network blip. Trying again a moment later works. → **Retry with backoff.** Backoff matters because hammering a struggling database makes it worse.
- **Poison**, like a malformed message or a bug for one specific input. It fails every time. If you retry forever, that message blocks its whole partition, so every order behind it is stuck. → **Dead-letter it** and keep going.

Step 6 splits "transient" in two, because a review showed this version had a nasty gap: during a 10-second database restart, _healthy_ events used up their retries and were dead-lettered. See "Three kinds of failure" in Step 6.

What happens to DLQ messages? In real systems you alert on DLQ size (Step 5 of the Observability project could do this!), inspect the messages, fix the bug, then **replay** them back to the main topic.

**Trade-off to mention in interviews:** dead-lettering breaks per-key ordering. If event 2 of an order is parked and event 3 is processed, the order may end up inconsistent. Our state machine and projection tolerate this (they ignore events that don't apply), but it's why some teams _stop the partition_ instead of skipping when ordering is critical.

### Why retries live inside the consumer, not in Kafka

Kafka has no built-in per-message retry or delay. Options are:

1. **In-process retry** (what we do): simple, but it holds up the partition for a few hundred ms.
2. **Retry topics** (`orders.retry.1m`, `orders.retry.10m`, ...): the failed message goes to a delay topic and a separate consumer re-publishes it later. Non-blocking, but more topics and more code. Uber's engineering blog has a well-known write-up on this approach.

### Why test end-to-end at all, when there are unit tests?

Unit tests prove each piece of logic. Only an end-to-end test proves the _wiring_: topic names match, events serialise correctly, consumer groups subscribe to the right topics, migrations run, and services find each other in Docker. Both kinds catch different bugs, which is the testing pyramid: many fast unit tests, a few slow end-to-end tests.

### Docker details worth knowing

- **Multi-stage build:** the build stage installs every dependency, compiles TypeScript, then removes the dev dependencies (`npm prune --omit=dev`) and the TypeScript sources. The runtime stage copies only what is left: compiled JavaScript, SQL migrations, `package.json` files and production `node_modules`. The image is smaller and has less attack surface (no compiler, no test tools).
- **`npm ci --ignore-scripts`:** installs exactly what `package-lock.json` says, and doesn't run any package's install scripts during the build (a common way for a malicious dependency to run code).
- **`USER node`:** don't run containers as root.
- **`CMD ["node", "dist/main.js"]`, not `npm start`:** when Docker stops a container it sends `SIGTERM` to the main process. If that process is npm, the signal isn't reliably passed on to Node, so the graceful shutdown (stop consumers, finish the relay tick, close the pool) may never run, and Docker kills the process after 10 seconds.
- **Two Kafka listeners:** containers reach the broker as `redpanda:9092` and your laptop as `localhost:19092`. Kafka tells clients which address to reconnect to (the _advertised_ address), so a single address can't work for both. This is the #1 "why can't I connect to Kafka in Docker" problem.
- **Health checks + `depends_on: condition: service_healthy`:** services wait for Kafka and Postgres to be ready, not just started. Each service's own health check calls `GET /health`, which runs `SELECT 1` against its database, so `docker compose up --wait` returns only when the whole system can actually serve requests.

### Try it yourself

```bash
docker compose up -d --build --wait   # --wait returns once every health check passes
npm run test:e2e                      # same as: node tests/e2e/e2e.mjs
```

Exercises:

1. Make a poison pill: in Redpanda Console, produce `not json` to the `orders` topic. Check the inventory logs, then find the message in `orders.dlq` with its error headers.
2. Simulate a transient failure: `docker compose stop postgres` for 1 second while placing an order, and watch the `retry 1 for ...` log lines.
3. Scale out: `docker compose up -d --scale inventory-service=3` (remove its fixed port first). With 3 partitions, Kafka gives each replica one partition. Check the consumer group in Console.
4. Write a tiny `dlq-replay` script that reads `orders.dlq` and re-publishes each message to its `dlq-original-topic`.

---

## Step 6: Professional standards: NASA/JPL rules, strict tooling, CI/CD

Steps 1 to 5 made the system _work_. This step makes the repository look and behave like one a professional team would trust: a coding standard that machines enforce, automated checks on every pull request, security scanning, automated releases, and the files every serious open-source repo has.

### What we built

```
tsconfig.base.json            the strictest TypeScript settings, shared by every workspace
eslint.config.mjs             type-aware linting + the NASA/JPL rules; zero warnings allowed
tools/eslint-rules/           our own ESLint rule (no recursion) and its tests
docs/CODING_STANDARD.md       the 10 rules, how each is adapted to TypeScript, and what enforces it
packages/contracts/src/
  invariant.ts                invariant() and assertNever(): runtime assertions
  limits.ts                   every business bound in one place (items per order, max price, ...)
  parse-event.ts              parseEvent(): untrusted JSON in, a typed event (or an error) out
packages/messaging/src/
  config.ts                   environment variables read once at startup and range-checked
  errors.ts                   classifyFailure(): permanent, infrastructure or unknown
.husky/, commitlint.config.mjs   git hooks: lint staged files, check commit messages
.github/workflows/            CI, CodeQL, dependency review, release (CD)
.github/dependabot.yml        weekly dependency-update pull requests
CONTRIBUTING.md, SECURITY.md, CODE_OF_CONDUCT.md, CHANGELOG.md, issue forms, PR template, CODEOWNERS
```

One command runs what CI's `quality` job runs: `npm run check`. It checks formatting, builds, lints, type-checks, runs the unit tests with coverage, and tests our own lint rule. The security scans, the PR title check and the end-to-end test run only in CI (`npm audit --omit=dev` and `npm run test:e2e` work locally too).

### Why use a space-flight coding standard for a web shop?

In 2006, Gerard Holzmann of NASA's Jet Propulsion Laboratory (JPL) published **"The Power of Ten: Rules for Developing Safety-Critical Code"**. The idea is simple: ten rules that are short enough to remember, and **strict enough that a tool can check them**. Code that follows them is simple enough for people to review and for analysers to reason about. You can't restart a Mars rover to fix a hung loop.

Our backend will never leave Earth, but the same properties pay off in any system that must not lose data:

- **Bounded loops and retries** mean a consumer can't hang forever on one message.
- **Assertions** turn a bug into a loud, rolled-back error at the line that broke, instead of a corrupt row that surfaces three services later.
- **Small functions** mean a reviewer can actually check them.

The rules were written for C, so some need translating for TypeScript, which runs on a garbage-collected runtime. Here's the short version. [`docs/CODING_STANDARD.md`](docs/CODING_STANDARD.md) explains each rule in detail, including what our tools _can't_ check.

| #   | Original rule (C)                                               | Our TypeScript version                                                                                                                                 | Enforced by                                                                                                                                 |
| --- | --------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Simple control flow: no `goto`, `setjmp`/`longjmp` or recursion | No labels, no `with`, no recursion                                                                                                                     | ESLint `no-restricted-syntax`, our `jpl/no-recursion`, `complexity`, `max-depth`                                                            |
| 2   | Every loop has a fixed upper bound                              | No `while`, `do…while`, `for(;;)` or `for…in`. Only bounded `for` loops and array methods. Retries, batches and polling all have limits                | ESLint `no-restricted-syntax`, `no-constant-condition`; `withRetry` checks its own bounds                                                   |
| 3   | No dynamic memory allocation after start-up                     | A garbage-collected language allocates all the time, so the spirit becomes: **every resource is bounded** (body size, pool size, batch size, timeouts) | `Limits`, range-checked config, 64 kb request bodies, pool `max` and timeouts                                                               |
| 4   | A function fits on one printed page                             | ≤ 60 lines, ≤ 25 statements, ≤ 4 parameters, files ≤ 300 lines                                                                                         | ESLint `max-lines-per-function`, `max-statements`, `@typescript-eslint/max-params`, `max-lines`                                             |
| 5   | At least two assertions per function, on average                | `invariant()` for "this can't happen" facts, such as the row count of every write                                                                      | `invariant()` and code review (no tool can count meaningful assertions)                                                                     |
| 6   | Declare data in the smallest possible scope                     | No module-level `let`, no `var`, `const` by default, no shadowing, `readonly` fields                                                                   | ESLint `no-var`, `prefer-const`, `no-shadow`, `prefer-readonly`, `no-restricted-syntax`                                                     |
| 7   | Check every return value and every parameter                    | No forgotten promises; every input is parsed at the boundary; every write checks its row count                                                         | ESLint `no-floating-promises`, `strict-boolean-expressions`, `switch-exhaustiveness-check`; `parseEvent`, DTO validation, config validation |
| 8   | Limit the preprocessor                                          | No code that writes or loads code at runtime: no `eval`, `new Function` or dynamic `import()`                                                          | ESLint `no-eval`, `no-new-func`, `no-restricted-syntax`                                                                                     |
| 9   | Restrict pointers; no function pointers                         | No `any`, no non-null `!`, and no tables of handler functions: dispatch with `switch`, so the call graph is static                                     | ESLint `no-explicit-any`, `no-unsafe-*`, `no-non-null-assertion`, `no-restricted-syntax`                                                    |
| 10  | All warnings on, zero warnings, static analysers every day      | The strictest `tsconfig`, `eslint --max-warnings 0`, CodeQL on every PR and every day                                                                  | `tsconfig.base.json`, CI, `.github/workflows/codeql.yml`                                                                                    |

Rule 1 allows one exception, and so do we: the "forever" loops of a long-running service (the outbox relay's timer and the Kafka consumer loop) are expected to run until shutdown. What has to be bounded is the work _inside_ them: one batch, one message, a few retries.

### Static analysis, part 1: the compiler

`"strict": true` is where most projects stop. `tsconfig.base.json` adds the flags most projects skip, and each one closes a real hole:

- **`noUncheckedIndexedAccess`**: `items[0]` has type `Item | undefined`, because the array might be empty. Without this flag, TypeScript pretends every index exists.
- **`exactOptionalPropertyTypes`**: `causationId?: string` means "absent, or a string". You can't set it to `undefined` explicitly, which keeps `"causationId": null`-style surprises out of the JSON.
- **`noPropertyAccessFromIndexSignature`**: you must write `env['DATABASE_URL']` rather than `env.DATABASE_URL`, which makes it visible that the key might be missing. That flag is why the environment is read only by the settings loaders, such as `loadMessagingSettings()` in `config.ts`.
- **`noImplicitReturns`**, **`noFallthroughCasesInSwitch`**, **`noImplicitOverride`**, **`noUnusedLocals`**, **`noUnusedParameters`** and **`allowUnreachableCode: false`** turn small mistakes into compile errors.

### Static analysis, part 2: type-aware linting

A plain linter only sees the text. typescript-eslint's `strictTypeChecked` preset asks the TypeScript compiler for the type of every expression, so it can find bugs a plain linter can't:

- **`no-floating-promises`**: calling an `async` function without `await` is an error. A forgotten `await` is the classic Node.js bug: the code runs out of order and the error goes nowhere.
- **`strict-boolean-expressions`** (we also turn off its `allowNumber` and `allowString` options): `if (count)` and `if (name)` are rejected, because `0` and `''` are falsy too, so the check silently treats "zero" like "missing". You write what you mean: `if (count > 0)` or `if (name !== undefined)`.
- **`switch-exhaustiveness-check`** and **`assertNever()`**: add a new event type to the union, and every `switch` that forgot it fails the build, as long as the `switch` has no `default` (the lint rule checks it) or its `default` calls `assertNever(event)` (the compiler checks it: `assertNever` only accepts `never`, the type of a value that can't exist). A `default` that throws anything else turns both checks off. The consumers' `handle()` methods do that on purpose, because each one handles only a few types.
- **`no-unsafe-*`**: an `any` can't silently flow into typed code.

`--max-warnings 0` is rule 10 in practice. A warning nobody fixes is noise that hides the next real one, so warnings fail CI like errors.

### Writing your own lint rule

ESLint has no "no recursion" rule, so [`tools/eslint-rules/no-recursion.mjs`](tools/eslint-rules/no-recursion.mjs) adds one. It's under 90 lines, and the idea behind it is worth knowing: ESLint parses each file into an **abstract syntax tree (AST)**, a tree of nodes like `FunctionDeclaration` and `CallExpression`, and calls your functions as it walks the tree. Our rule keeps a stack of the functions it is inside, with every name each one can call itself by, and when it meets a call to one of those names (`f()` inside `f`, or `this.m()` inside method `m`), it reports it.

Paste a few lines into https://astexplorer.net (choose `@typescript-eslint/parser`) to see the tree ESLint sees.

A lint rule is code, so it gets tests too. [`no-recursion.test.mjs`](tools/eslint-rules/no-recursion.test.mjs) feeds snippets to ESLint's `RuleTester` and checks the exact errors for each one, using Node's built-in test runner (`npm run test:tools`, which `npm run check` and CI also run). Writing those tests found a gap: in `const run = function loop(n) { ... loop(n - 1) ... }` the function has two names, the rule only knew it as `run`, and the recursive call slipped through. Now the rule records both.

Honest limitation: it only catches _direct_ recursion. `a()` calling `b()` calling `a()` needs a whole-program call graph, so that's left to code review.

### Assertions with `invariant()` (rule 5)

```ts
const result = await tx.query("UPDATE orders SET status = $2 WHERE id = $1 AND status = 'PENDING'", [
  id,
  next,
]);
invariant(result.rowCount === 1, `order ${id} was not PENDING`);
```

An assertion states something that must be true if the code is correct. If it's false, carrying on would write bad data, so we stop. `invariant()` throws an `InvariantError`: the transaction rolls back, and a consumer dead-letters the event with that message. The bug becomes visible, and nothing is half-written. Because `invariant` is declared as `asserts condition`, the compiler also narrows types after the call.

Where you'll find them: every database write checks its row count, `withRetry()` checks its own arguments, `project()` checks the event belongs to the view it updates, and `charge()` checks the amount is a whole number of cents within limits.

**Assertion vs validation:** _validation_ is for things that **can** happen (a client sends quantity 0, so we answer HTTP 400). An _assertion_ is for things that **can't** happen unless there's a bug. Don't use `invariant()` for user input.

### Trust nothing at the boundary: parse, don't validate (rule 7)

A Kafka message is untrusted bytes. It might come from another team's bug, an old event version, or someone typing into Redpanda Console. `parseEvent(raw: unknown): ShopEvent` checks every field (a known event type, version 1, a UUID id, an ISO date, matching order ids, the payload's shape, and each item's limits) and either returns a fully typed event or throws `InvalidEventError`. After that one call, the rest of the code can rely on the types. That's the idea behind the slogan "parse, don't validate": instead of checking data and then passing the same loose object around, convert it once into a type that can only hold valid data.

One exception keeps old services working when new events appear. `parseEvent()` only knows today's event types, so on its own it would reject an `OrderShipped` added next year, and every old consumer would dead-letter it. So the consumer first reads just the `eventType` (`peekEventType()`), skips a type it doesn't handle without parsing it, and only parses the types it does handle. That's the **tolerant reader** pattern: be strict about what you use, and ignore what you don't.

The same idea guards the other two boundaries:

- **HTTP bodies** go through class-validator DTOs with limits from `Limits` (at most 50 items, quantity 1 to 1,000, and so on), and a body over 64 kb is refused before it's even parsed.
- **Environment variables** go through a settings loader such as `loadMessagingSettings()`, which reads every setting once at startup and checks its format and range. A typo like `OUTBOX_BATCH_SIZE=10000` stops the service immediately with `config: OUTBOX_BATCH_SIZE must be in 1..1000, got 10000`, instead of misbehaving at 3 AM.

### Three kinds of failure (an upgrade to Step 5)

Step 5 retried every failure a few times, then dead-lettered it. A review spotted the gap: if Postgres restarted for 10 seconds, every _healthy_ event that arrived during the restart used up its retries and was dead-lettered, and those orders stayed stuck. `classifyFailure()` in `errors.ts` now sorts failures into three kinds:

| Kind           | Examples                                                                                             | What the consumer does                                                                                                                                                  |
| -------------- | ---------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| permanent      | malformed JSON, an event version this code can't read, a broken invariant                            | Dead-letter it at once. Retrying can't fix bad data.                                                                                                                    |
| infrastructure | database unreachable (`ECONNREFUSED`, SQLSTATE `08xxx`), deadlock (`40P01`), query timeout (`57014`) | Retry with backoff, then throw `RedeliverLaterError`. Kafka doesn't commit the offset and delivers the message again later. **Never dead-lettered**: the event is fine. |
| unknown        | probably a bug that only some inputs trigger                                                         | Retry with backoff, then dead-letter it, so one message can't block its partition forever.                                                                              |

A kafkajs detail shaped this code: if your message handler throws a `TypeError`, `RangeError`, `ReferenceError` or `SyntaxError`, kafkajs treats it as a programming error and stops that consumer for good. The service would keep running and answering health checks while processing nothing. That's why `RedeliverLaterError` extends plain `Error`, and why handler bugs are caught, retried and dead-lettered instead of escaping to kafkajs.

A second kafkajs detail: a consumer only tells Kafka "I'm alive" (a **heartbeat**) between messages. If it's silent for the 30-second session timeout, Kafka decides it died and hands its partitions to another replica. Five tries that each wait for a 10-second query timeout already take longer than that. So `withRetry()` has a `beforeTry` hook, and the consumer sends a heartbeat there before every try (kafkajs only really sends one if 3 seconds have passed since the last). The longest silence is then one try plus one wait, and `loadMessagingSettings()` refuses retry settings whose longest wait is over 10 seconds. A first version sent the heartbeat only before each _wait_, and a review showed why that wasn't enough: when the last try fails and the message is redelivered, the last try, kafkajs's own backoff and the redelivered message's first try ran back to back, with no heartbeat for over 30 seconds. A test that drove kafkajs's real consumer loop on a fake clock measured 35 seconds before and 20 after the fix. If the heartbeat itself fails (the group is rebalancing), that error goes back to kafkajs unchanged, because kafkajs knows how to rejoin the group.

### What a code review found

Before this step, every test passed. A careful review still found real bugs. Each one teaches something you can use in interviews:

1. **Price tampering (security).** The client sent `unitPriceCents`, and nothing checked it, so anyone could buy a laptop for 1 cent. **Fix:** the inventory service owns the catalog, so it now stores prices (`migrations/002_add_prices.sql`) and rejects an order whose prices don't match, with a reason such as `price changed for BOOK: expected 4500, got 1`. **Lesson:** never trust a price, role or id that the client sends. Check it against the service that owns it.
2. **A lost update in the read model.** You can't lock a row that doesn't exist yet. Two replicas projecting the first two events of a new order both found no row, both inserted one, and the second write overwrote the first. **Fix:** insert an empty placeholder row first (`ON CONFLICT DO NOTHING`), _then_ `SELECT ... FOR UPDATE`, which now always has a row to lock.
3. **A deadlock in the compensation.** Reservations locked products in sku order, but the release gave stock back in the order items were listed. **Fix:** the release sorts by sku too. A locking rule only works if every code path follows it.
4. **An integer overflow.** `/stats` cast the revenue sum to `int`, which overflows past about $21 million (2³¹ cents). **Fix:** cast to `bigint` and convert with `Number()`, which is exact up to 2⁵³.
5. **A process crash.** When Postgres restarted, an idle pooled connection emitted an `'error'` event that nobody listened for, and Node.js kills the process on an unhandled `'error'` event. **Fix:** `pool.on('error', ...)` logs it, and the pool replaces the connection.
6. **Shutdown in the wrong order.** Every component cleaned up in the same hook (`onModuleDestroy`), so the database pool could close while a consumer or a relay tick was still using it. **Fix:** Nest runs its shutdown hooks in phases, so each kind of cleanup gets the right phase. Consumers stop and the relay finishes its tick in `onModuleDestroy`, the producer disconnects in `beforeApplicationShutdown`, and the pool closes last in `onApplicationShutdown`.
7. **A broken connection back in the pool.** If `ROLLBACK` itself failed (the connection was dead), its error replaced the real one, and the dead connection was handed back to the pool for the next request. **Fix:** keep the original error, and tell the pool to destroy that connection (`client.release(err)`).
8. **A health check that lied.** On first start, the Postgres image runs its init scripts on a temporary server that only listens on a Unix socket. `pg_isready` over that socket said "healthy" before the databases existed. **Fix:** check over TCP (`-h 127.0.0.1`), which only the real server answers.
9. **A race in the end-to-end test.** The query service reads three topics, and Kafka doesn't order messages across topics, so a view can say `CANCELLED` before `PaymentFailed` has been projected. The test waited for "status is no longer PENDING" and then checked the payment status. **Fix:** wait until _every_ field the test checks has arrived.
10. **Migrations that ran on every start.** `migrate()` executed every `.sql` file each time a service started, relying on `IF NOT EXISTS`. That breaks as soon as a migration isn't safe to repeat, such as an `UPDATE` that sets prices. **Fix:** a `schema_migrations` table records which files have run, so each runs exactly once, inside one transaction, under a Postgres advisory lock so replicas starting together take turns. **Consequence:** a migration that has shipped is never edited. You add a new numbered file instead, which is why prices arrived as `002_add_prices.sql`.
11. **A coverage gate that couldn't fail.** `jest.config.js` set coverage thresholds, but its file globs were written from the repository root (`services/*/src/**/*.ts`), while Jest matches them against each workspace's own `src/` folder. So Jest measured 0 of 0 lines, printed `Unknown%`, and let every threshold pass. **Fix:** globs relative to each workspace's `src/`, thresholds raised to what the tests really reach, and a CI step that fails if coverage ever measures 0 lines. **Lesson:** make every safety net fail once on purpose, to prove it can.

#### What the second review found

A second review, after all the checks above were green, still found five bugs. None of them could fail a test that already existed, which is why reviews and tests complement each other:

1. **A timeout counted as a bug.** The pool sets `statement_timeout`, so a database that is merely slow raises SQLSTATE `57014`. `classifyFailure()` didn't list it, so a slow minute dead-lettered healthy events. **Fix:** `57014` is now an infrastructure failure.
2. **Shutdown that stopped waiting.** The relay's timer stored each tick's promise so shutdown could wait for it, but a tick that was skipped (the previous one was still running) replaced it with an already-finished promise. **Fix:** only a tick that really starts a batch records itself. The new test fails on the old code.
3. **A blank setting became 0.** `Number('')` is `0`, so `DECLINE_ABOVE_CENTS=` with no value meant "decline every payment". **Fix:** numeric settings must be written as plain numbers, and blank, `0x10` or `1e3` stop the service at startup.
4. **New event types were dead-lettered.** The docs promised that old consumers ignore new event types, but `parseEvent()` rejected them first. **Fix:** the tolerant reader above.
5. **Retries outlasted Kafka's patience.** See the heartbeat paragraph above.

### Tests and coverage

- **One test run for the whole repo.** `jest.config.js` runs every workspace's tests together and **fails if coverage drops** below 90% of statements, lines and functions, or 85% of branches (today the tests cover about 97% of lines and 95% of branches). Coverage doesn't prove tests are good (a test with no assertions still "covers" code), but a threshold stops untested code from slipping in.
- **Fakes instead of a real database.** Unit tests use a fake `Database` whose `transaction(fn)` just calls `fn(fakeClient)`, and a fake client that records every SQL statement it receives. Tests then check _which SQL would run, with which parameters_. They're fast and deterministic. The end-to-end test in CI covers the real Postgres and Kafka.
- **See what's covered:** `npm run test:cov`, then open `coverage/lcov-report/index.html` in a browser. Red lines were never run by any test.

### Git hooks and Conventional Commits

`npm ci` runs the `prepare` script, which installs [husky](https://typicode.github.io/husky/)'s git hooks:

- **pre-commit** runs `lint-staged`: ESLint (with `--fix`) and Prettier on the files you staged only, so it's fast.
- **commit-msg** runs `commitlint`: the message must follow [Conventional Commits](https://www.conventionalcommits.org/), `type(optional scope): description`. For example, `feat(inventory-service): check prices against the catalog`, `fix(messaging): run each migration exactly once`, or `docs: add lesson 6`.

Hooks are a convenience, not a guarantee (`git commit --no-verify` skips them), so CI runs the same checks again.

Why insist on a message format? Because a tool can then work out the next version number by itself, following **semantic versioning** (MAJOR.MINOR.PATCH):

| Commits since the last release                                                | Next version (from 1.4.2) |
| ----------------------------------------------------------------------------- | ------------------------- |
| `fix:`, `perf:`, `build:` (a dependency or the image) or `revert:`, no `feat` | 1.4.3 (patch)             |
| at least one `feat:`                                                          | 1.5.0 (minor)             |
| a `!` after the type (`feat!:`) or a `BREAKING CHANGE:` footer                | 2.0.0 (major)             |
| only `docs:`, `test:`, `ci:`, `refactor:`, `style:`, `chore:`                 | no release                |

The last row is a choice, made in `release-please-config.json`: those types are `hidden`, so they stay out of the changelog, and they never make a release on their own. A docs-only change shouldn't publish four new images; such commits simply go out with the next real release. `build:` is visible on purpose, because Dependabot uses it for updates to the runtime dependencies and the base image, and a patched dependency should reach users. Its updates to development tools (`chore(deps-dev): ...`) never ship in an image, so they don't make a release either.

Before 1.0.0, one rule changes, because `bump-minor-pre-major` is on: a breaking change bumps the minor version (0.3.1 → 0.4.0) instead of jumping to 1.0.0. Version 1.0.0 is where a project promises a stable API, so you decide when to reach it, with a `Release-As: 1.0.0` footer in a commit message.

### CI: what runs on every pull request

`.github/workflows/ci.yml` runs these jobs side by side (only `e2e` waits for `quality` to pass first, because it's the slowest). A pull request should only be merged when all of them are green.

| Job           | What it does                                                                                                | What it catches                                                                                                        |
| ------------- | ----------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `quality`     | format check, build, lint, type-check, unit tests with coverage (table in the run summary), lint-rule tests | everything `npm run check` catches                                                                                     |
| `docker-lint` | hadolint on the `Dockerfile`                                                                                | Dockerfile mistakes, such as unpinned packages or running as root                                                      |
| `secrets`     | gitleaks over the whole git history                                                                         | a password or API key committed by accident, even one deleted in a later commit                                        |
| `audit`       | `npm audit --omit=dev --audit-level=high`                                                                   | known vulnerabilities in the dependencies that ship                                                                    |
| `e2e`         | starts the whole system with Docker Compose and runs `tests/e2e/e2e.mjs`                                    | wiring bugs (topic names, serialisation, migrations, networking), the whole saga, price tampering and input validation |

A small workflow of its own, **`pr-title.yml`**, checks that the pull request title is a Conventional Commit: with squash merging, the title becomes the commit message on `main`, so a bad title means a bad changelog entry. It's separate from CI because it also runs when you edit the PR, so fixing a title clears the check at once without rebuilding everything.

Three more workflows add security in depth:

- **CodeQL** (`codeql.yml`): GitHub's semantic code analysis, with its security and quality queries, on every PR and **every day**. New vulnerability patterns are added all the time, so code that was clean yesterday can be flagged today. That's rule 10's "run analysers daily".
- **Dependency review** (`dependency-review.yml`): when a PR adds or upgrades a dependency, it fails if that version has a known high-severity vulnerability.
- **Dependabot** (`dependabot.yml`, not a workflow but a GitHub feature): every week it opens small PRs that update npm packages, GitHub Actions and the Docker base image. Small, frequent updates are much easier than one painful upgrade a year.

Two habits in the workflow files are worth copying. Every workflow declares its `permissions:` explicitly, so the automatic `GITHUB_TOKEN` gets only what that workflow needs: CI can only read the code, and only the release workflow may push tags and images (the principle of least privilege). And CI's `concurrency` setting cancels an outdated run when you push again, so you don't wait for checks on code that no longer exists.

#### What the first CI run caught

The first run of these checks, on the pull request that added them, went red twice. Both failures were worth having:

- **`npm audit` found real vulnerabilities.** NestJS 10's HTTP adapter, `@nestjs/platform-express`, brought in Express 4, whose `body-parser` had a known high-severity denial-of-service bug, and an old `multer` (a file-upload library) with similar bugs. Our own code was fine: the risk came in through dependencies of a dependency. The fix was upgrading to NestJS 11, whose adapter uses Express 5 and a patched `multer`. (`npm ls multer` shows where a package comes from.) The unit tests, the end-to-end test and a smoke test of the real HTTP routes (validation, the 64 kB body limit, error codes) all passed unchanged. New advisories are published every week, so a package that was safe when you added it can be flagged later. That's why the audit runs on every pull request.
- **One upgrade, two copies.** The first install after the upgrade put a separate copy of `@nestjs/common` inside every workspace, and a unit test failed: it watched one copy's `Logger` while the code logged through another. Nest compares some classes with `instanceof` and keeps settings such as the log level in static fields, so two copies can quietly disagree in production too. `npm dedupe` moved everything back to one copy at the root. After any framework upgrade, check that `npm ls @nestjs/common` lists a single version.
- **Dependency review needs a setting.** It stopped with "Dependency review is not supported on this repository. Please ensure that Dependency graph is enabled", which is a repository setting, not a code problem (see [GitHub settings that complete the setup](#github-settings-that-complete-the-setup)).

The same run warned that several actions still ran on Node.js 20, which GitHub is retiring, so the first-party ones moved to their Node.js 24 versions (`actions/checkout@v5`, `actions/setup-node@v5`, `actions/upload-artifact@v6` and `github/codeql-action@v4`). Dependabot proposes the rest as their publishers release them.

#### What the first dependency updates taught

Dependabot's first round brought eight pull requests. The five GitHub Actions updates were green and could be merged as they were. The other three failed or hid a problem, and each one is a reason updates arrive as small pull requests that must pass CI:

- **TypeScript 7 broke the tools built on the compiler.** TypeScript 7 is the compiler rewritten in Go, and its npm package no longer exposes the JavaScript API that `ts-jest` and `typescript-eslint` load. Neither accepts 7 in its peer range yet, so `npm ci` refused to install (`ERESOLVE`). We took TypeScript 6.0, the newest version both accept, and an `ignore` rule in `dependabot.yml` holds 7 back until they support it. TypeScript 6 also deprecates `"moduleResolution": "node"`, so `tsconfig.base.json` now says `"bundler"`, and the compiled JavaScript is byte for byte the same.
- **A lockfile can quietly switch off a lint rule.** `eslint-plugin-jest` borrows its `unbound-method` rule from `@typescript-eslint/eslint-plugin`, and when it can't load that package, the rule does nothing, without an error. Dependabot's lockfile had moved the package into another package's folder, where `eslint-plugin-jest` couldn't find it, so lint stayed green while it checked less. Rebuilding the lockfile fixed it. Lockfile changes deserve a look too, and `npm ls <package>` shows where a package sits.
- **CI tested a different Node.js than the image.** The Node.js 26 image passed every check, because the end-to-end test runs in the images. But the build, the lint and the unit tests take their Node.js version from `.nvmrc`, which still said 22. A runtime version lives in several places (the `Dockerfile`, `.nvmrc`, `engines` and `@types/node`), and Dependabot only updates the first. They all say 26 now, and `dependabot.yml` skips major `@types/node` updates, because those should move with the runtime, on purpose.
- **NestJS 12 ships only ES modules.** The services compile to CommonJS and load Nest with `require()`, which Node.js can do for ES modules since 22.12, so they run as before. Jest's own module loader can't, and every test suite that loaded Nest failed with "Cannot use import statement outside a module". `jest.preset.js` now has Babel compile just the `@nestjs` files to CommonJS for the tests. And as with NestJS 11, the update put a copy of Nest in every workspace until `npm dedupe` brought it back to one.

These three pull requests all change `package.json` and the lockfile, so each was built on top of the one before. They are merged in that order (development tools, then Node.js 26, then NestJS 12) with **Create a merge commit**. A squash merge writes new commits, and the next pull request would then conflict on every line both had changed.

### CD: from merge to a published release

`.github/workflows/release.yml` uses [release-please](https://github.com/googleapis/release-please):

1. You merge pull requests into `main`, each with a Conventional title.
2. release-please keeps a **release PR** open (titled like `chore(main): release 0.2.0`). It bumps the version in `package.json` and writes the new `CHANGELOG.md` entries from the commit messages, and it updates itself after every merge.
3. When you merge the release PR, release-please creates the tag `v0.2.0` and a GitHub Release.
4. The `publish` job then builds an image for each of the four services and pushes it to GitHub Container Registry (for example `ghcr.io/mitraker/event-driven-ecommerce-order-service:0.2.0` and `:latest`).

Why does publishing live in the same workflow, instead of a separate one that runs `on: release`? A release created with the default `GITHUB_TOKEN` doesn't trigger other workflows. It's GitHub's protection against infinite loops, so a release made by release-please would never start a second workflow. (Also note the lowercase `mitraker`: image names on GHCR must be lowercase, so the workflow lowercases the owner's name.)

Three things to know about the first release:

- **It is 0.1.0 because the config says so.** The manifest (`.release-please-manifest.json`) starts at `0.0.0`, which release-please reads as "never released". Its default first version is then 1.0.0, so `initial-version: "0.1.0"` in `release-please-config.json` picks 0.1.0 instead.

- **The release PR's checks wait for your approval.** release-please opens and updates it with the same `GITHUB_TOKEN`. Since June 2026, GitHub creates the workflow runs for a pull request made by `github-actions[bot]` but holds them until someone with write access approves them ([GitHub's changelog](https://github.blog/changelog/2026-06-11-bot-created-pull-requests-can-run-workflows-if-approved/)), so generated code can't start workflows that may see secrets on its own. Before that change they didn't run at all, and a release PR could be merged without CI. Once the release PR is up to date, approve its workflows on the pull request page, wait for green, then merge it.
- **New images start private.** The first push to GHCR from a personal account creates a private package. To let anyone pull the images, open the package on your GitHub profile → Package settings → Change visibility.

#### A supply-chain lesson: why there's no image scanner

The obvious next step would be to scan each image for known vulnerabilities before publishing it, and the most popular tool for that is Trivy. But in March 2026, attackers took over Trivy's GitHub Action and **force-pushed 76 of its 77 version tags to malicious commits**. Every workflow that used `aquasecurity/trivy-action@<tag>` quietly ran code that stole the secrets available to it ([Aqua's advisory](https://www.aquasec.com/blog/trivy-supply-chain-attack-what-you-need-to-know/)).

The lesson is bigger than one tool: **every `uses:` line runs someone else's code with your workflow's permissions.** A version tag like `@v4` is just a movable pointer, while a full 40-character commit SHA can't be changed after the fact. So:

1. Give each job the fewest permissions it needs (our workflows do this), so a compromised action can steal less.
2. Prefer first-party actions (`actions/*`, `github/*`) and well-known publishers.
3. For anything that handles secrets or publishes artifacts, pin to a commit SHA you have checked, and let Dependabot propose updates to the pin.

Our workflows use version tags for readability, which is common practice. Pinning them all to SHAs is a good exercise below.

### The files every professional repository has

- **`README.md`**: what it is, how to run it, and badges that show the build is green.
- **`LICENSE`** (MIT): without a license, nobody may legally reuse the code, even if the repo is public.
- **`CONTRIBUTING.md`**: setup, the quality gates, the commit convention, and how to add a migration or a new event type.
- **`SECURITY.md`**: how to report a vulnerability privately, never in a public issue.
- **`CODE_OF_CONDUCT.md`**: the Contributor Covenant, the standard most open-source projects adopt.
- **`CHANGELOG.md`**: what changed in each version, maintained by release-please.
- **Issue forms and a PR template** (`.github/`): bug reports arrive with the details needed to reproduce them, and every PR answers "what, why, and how was it tested".
- **`CODEOWNERS`**: who must review changes.
- **`.editorconfig`, `.nvmrc`, `engines` in `package.json`**: everyone uses the same Node.js version and basic formatting.

### GitHub settings that complete the setup

Some protections live in the repository's settings on GitHub, not in files, so they have to be switched on once by hand:

1. **Rules for `main`** (Settings → Rules → Rulesets, or Branches → Branch protection): require a pull request, require the CI checks to pass, and block force pushes and deletion.
2. **Settings → Actions → General → Workflow permissions:** tick "Allow GitHub Actions to create and approve pull requests". release-please needs it to open the release PR.
3. **Settings → General → Pull Requests:** allow squash merging with the PR title as the commit message, and tick "Automatically delete head branches".
4. **Settings → Security** (called "Code security" or "Advanced Security" depending on your account): turn on the dependency graph (the dependency review check needs it), private vulnerability reporting, Dependabot alerts and security updates, and secret scanning with push protection.
5. **Settings → Moderation options → Reported content:** accept reports from all users. Then "Report content → Report to repository admins" reaches you privately, which is the reporting route `CODE_OF_CONDUCT.md` gives.

### Try it yourself

```bash
npm ci             # installs dependencies and the git hooks
npm run check      # format, build, lint, typecheck, unit tests + coverage, lint-rule tests
```

Exercises:

1. Break a rule on purpose: add `while (true) {}` to any service and run `npm run lint`. Read the `JPL-2` message. Then stage the file and try to commit it: the pre-commit hook stops you. (Delete the loop again afterwards.)
2. Check commit messages the way the commit-msg hook does: `echo "fixed stuff" | npx commitlint` is rejected, and `echo "fix: handle an empty basket" | npx commitlint` passes.
3. Comment out one test in `services/inventory-service/src/reservation.spec.ts` and run `npm run test:cov`. Does coverage still pass? What does that tell you about coverage as a quality metric?
4. Send a "poison" event: in Redpanda Console, copy a real `OrderCreated` message from the `orders` topic, change `"version": 1` to `"version": 2`, and produce it to `orders` again. You'll find it twice in `orders.dlq`, once from each service that reads `orders` (inventory and query: see the `dlq-consumer-group` header), both with `dlq-error: unsupported version 2`.
5. Watch the infrastructure failure kind: with the full system running, place three orders and immediately run `docker compose stop postgres`. Wait 20 seconds, then `docker compose start postgres`. The logs show retries and redeliveries, the orders still finish, and no `.dlq` topic receives anything.
6. Read `docs/CODING_STANDARD.md` and pick one rule that our tools can't fully check. How would you check it in a code review?
7. Pin one third-party action to a commit SHA: on the action's GitHub page, open the release you want, copy the full commit SHA, and write `uses: owner/action@<sha> # v1.2.3`. The comment keeps it readable, and Dependabot updates both.
8. Break the coverage gate the way it was once broken: in `jest.config.js`, change `'**/*.ts'` to `'services/*/src/**/*.ts'` and run `npm run test:cov`. Jest prints `Unknown%` and still passes. Then put it back.
9. Look for duplicate packages: run `npm ls @nestjs/common` and `npm ls @typescript-eslint/eslint-plugin`. Each should list one version, with every other line ending in `deduped`.

### Interview talking points

- "Every loop, retry, batch, pool and request body has a bound, and the business limits live in one file."
- "I separate permanent, infrastructure and unknown failures. A database outage never sends healthy events to the dead-letter queue."
- "Assertions turn silent corruption into a loud, rolled-back error with a clear message."
- "All the tests passed, and a review still found a price-tampering bug. Now every price the client sends is checked against the service that owns the catalog."
- "Releases are automated from Conventional Commits, and every third-party tool in the pipeline is a supply-chain risk I can explain and limit."
- "Our coverage gate once measured nothing and passed anyway. Now CI proves the gate measures real code, and I make every safety net fail once on purpose."
- "A green dependency update still gets a review. One lockfile quietly turned off a lint rule, and once CI tested a different Node.js version than the image we shipped."
