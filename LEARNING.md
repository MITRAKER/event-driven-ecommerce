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
