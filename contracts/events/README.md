# Event catalog

| Topic       | Event                  | Producer          | Consumers                                |
| ----------- | ---------------------- | ----------------- | ---------------------------------------- |
| `orders`    | `OrderCreated` v1      | order-service     | inventory-service, order-query           |
| `orders`    | `OrderConfirmed` v1    | order-service     | order-query                              |
| `orders`    | `OrderCancelled` v1    | order-service     | inventory-service (release), order-query |
| `inventory` | `InventoryReserved` v1 | inventory-service | payment-service, order-query             |
| `inventory` | `InventoryRejected` v1 | inventory-service | order-service, order-query               |
| `payments`  | `PaymentSucceeded` v1  | payment-service   | order-service, order-query               |
| `payments`  | `PaymentFailed` v1     | payment-service   | order-service, order-query               |
| `*.dlq`     | any                    | retry handler     | manual replay                            |

## Saga (choreography)

```
OrderCreated --+--> InventoryReserved --+--> PaymentSucceeded --> OrderConfirmed
               |                        +--> PaymentFailed -----> OrderCancelled --> stock released
               +--> InventoryRejected ----------------------------> OrderCancelled
```

Message key is always `aggregateId` (the order id), so per-order ordering is preserved within a partition.

## Compatibility

- A consumer skips event types it doesn't handle without parsing them, even types newer than its code (`peekEventType()`, then `parseEvent()` only for handled types). So adding an event type never breaks an old service. Deploy the consumers that need a new event before the service that produces it, or they skip its first messages.
- A message of a type a consumer _does_ handle must parse, or it is dead-lettered. That includes a version the consumer doesn't know yet, which is why a breaking change needs a new version and a careful rollout: see "Changing an existing event" in [`CONTRIBUTING.md`](../../CONTRIBUTING.md#adding-an-event-type).
