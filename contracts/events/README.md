# Event catalog

| Topic       | Event                  | Producer          | Consumers                                   |
| ----------- | ---------------------- | ----------------- | ------------------------------------------- |
| `orders`    | `OrderCreated` v1      | order-service     | inventory-service, order-query              |
| `orders`    | `OrderConfirmed` v1    | order-service     | order-query                                 |
| `orders`    | `OrderCancelled` v1    | order-service     | inventory-service (release), order-query    |
| `inventory` | `InventoryReserved` v1 | inventory-service | payment-service, order-service, order-query |
| `inventory` | `InventoryRejected` v1 | inventory-service | order-service, order-query                  |
| `payments`  | `PaymentSucceeded` v1  | payment-service   | order-service, order-query                  |
| `payments`  | `PaymentFailed` v1     | payment-service   | order-service, order-query                  |
| `*.dlq`     | any                    | retry handler     | manual replay                               |

## Saga (choreography)

```
OrderCreated --+--> InventoryReserved --+--> PaymentSucceeded --> OrderConfirmed
               |                        +--> PaymentFailed -----> OrderCancelled --> stock released
               +--> InventoryRejected ----------------------------> OrderCancelled
```

Message key is always `aggregateId` (the order id), so per-order ordering is preserved within a partition.
