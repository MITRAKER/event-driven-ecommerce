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
   - `aggregateId`: the order id. We also use it as the **Kafka message key**. Kafka guarantees order only *within a partition*, and messages with the same key always go to the same partition. So all events of one order arrive in order.
   - `correlationId` / `causationId`: let you trace a whole chain ("this payment happened because of that reservation, which happened because of that order"). This is essential when debugging distributed systems.

### Key concepts
- **Event vs command.** A command says "do this" (`ReserveStock`) and can be refused. An event says "this happened" (`InventoryReserved`), is in the past tense, and is a fact. We mostly publish events.
- **Topic.** A named log of messages. Producers append, consumers read at their own pace and remember their position (the *offset*).
- **Consumer group.** Several instances of one service share a group id, so Kafka splits partitions between them. That is how you scale horizontally.
- **Saga (choreography style).** A business process that spans services without a distributed transaction. Each service reacts to events and publishes new ones. If a later step fails, earlier steps are undone by **compensating actions** (for example, payment fails, so inventory releases the stock). See the diagram in `contracts/events/README.md`.

### Try it yourself
```bash
docker compose up -d
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
await kafka.send('OrderCreated');   // what if the process dies right here, or Kafka is down?
```
Two separate systems, two separate writes, and no way to make them atomic. If the app crashes between the lines, the order exists but no other service ever hears about it. Swap the order of the lines and you can announce an order that was never saved. This is the **dual-write problem**, and it is one of the most common interview questions about event-driven systems.

### The fix: the transactional outbox
Write the event into a table *in the same database transaction* as the order (`orders.service.ts`). A database transaction is atomic, so either both rows exist or neither does. A separate background loop (`outbox.relay.ts`) then copies outbox rows to Kafka. If Kafka is down, rows simply wait in the table and get sent later. Nothing is lost.

### Design choices worth understanding
- **`FOR UPDATE SKIP LOCKED`** in the relay query. If you run three copies of the service, each relay locks a different batch of rows instead of all three sending the same events. This is a classic Postgres job-queue trick.
- **`ORDER BY id`** with a `BIGSERIAL` id preserves the order in which events were created.
- **At-least-once delivery.** If the relay crashes *after* Kafka accepted a batch but *before* the `UPDATE ... published_at`, that batch is sent again. You cannot get exactly-once for free across two systems, so the rule is: **producers may duplicate, and consumers must be idempotent.** We build idempotent consumers in Step 3.
- **Idempotent producer** (`idempotent: true` in `kafka.ts`). This stops duplicates caused by the Kafka client's own network retries. It does *not* cover the crash case above, which is why consumer idempotency still matters.
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
docker compose up -d
npm install && npm run build
npm start -w @shop/order-service
curl -X POST localhost:3001/orders -H 'content-type: application/json' \
  -d '{"customerId":"c-1","items":[{"sku":"BOOK","quantity":2,"unitPriceCents":1500}]}'
```
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
The outbox guarantees *at-least-once*, so consumers *will* see duplicates sometimes. `EventConsumer.process()` does this in **one transaction**:
1. `INSERT INTO processed_events (event_id) ... ON CONFLICT DO NOTHING`. If 0 rows were inserted, it's a duplicate, so skip it.
2. Run the handler (business change + outbox rows).
3. `COMMIT`, then Kafka commits the offset.

Because the dedupe row and the business change commit together, a crash anywhere means *both* are rolled back and the redelivery is processed cleanly. Together: **at-least-once delivery + idempotent processing = effectively exactly-once.** This sentence is worth memorising.

Extra safety nets, in layers:
- `payments.order_id` is the PRIMARY KEY, so the database itself refuses a second charge for one order.
- The inventory release only touches reservations with `status = 'RESERVED'`, so releasing twice is impossible.
- The order state machine only moves from `PENDING`, so a late `PaymentSucceeded` after a cancel does nothing.

### Concurrency details
- **`SELECT ... FOR UPDATE`** on product rows: two orders for the last keyboard can't both succeed. The second one waits for the first to commit, then sees 0 left.
- **`ORDER BY sku`** when locking: if order A locks BOOK then MUG while order B locks MUG then BOOK, they deadlock. Always locking in the same order prevents it.
- **`CHECK (available >= 0)`**: the last line of defence, enforced by the database even if the code has a bug.
- **Message key = order id**: all events for one order land in one partition, so they're consumed in order.

### Design pattern: functional core, imperative shell
The decisions are **pure functions** with no database and no Kafka: `nextState()` (order), `decideReservation()` (inventory), and `charge()` (payment). The consumers are thin shells that load data, call the pure function, and write results. That's why the business rules have fast, simple unit tests (19 tests across the repo now).

### Why events carry data ("event-carried state transfer")
`InventoryReserved` includes the items and prices, so the payment service can compute the amount **without calling the order service**. If services call each other synchronously to fill gaps, you get a distributed monolith: one service down means everything is down.

### Try it yourself
```bash
docker compose up -d && npm install && npm run build
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
