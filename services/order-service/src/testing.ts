/* Test helpers shared by this service's specs. Excluded from the build (tsconfig.build.json). */
import {
  createEvent,
  InventoryRejected,
  InventoryReserved,
  OrderCancelled,
  OrderConfirmed,
  OrderCreated,
  PaymentFailed,
  PaymentSucceeded,
  ShopEvent,
} from '@shop/contracts';
import { Database, MessagingConfig } from '@shop/messaging';
import { PoolClient } from 'pg';

export const ORDER_ID = '6f1c2d3e-4b5a-4c6d-8e7f-90a1b2c3d4e5';

export const TEST_CONFIG = new MessagingConfig({
  serviceName: 'order-service',
  port: 3001,
  databaseUrl: 'postgres://test',
  databasePoolMax: 2,
  kafkaBrokers: ['localhost:19092'],
  outboxPollMs: 500,
  outboxBatchSize: 100,
  consumerRetryAttempts: 3,
  consumerRetryBaseMs: 1,
});

export interface RecordedQuery {
  readonly sql: string;
  readonly params: readonly unknown[];
}

/** The parts of a pg result this service reads. */
export interface FakeResult {
  readonly rows?: readonly object[];
  readonly rowCount?: number;
}

export type Responder = (sql: string, params: readonly unknown[]) => FakeResult;

/** A fake pg client: records every query and answers with whatever `respond` returns (or throws). */
export function fakeClient(respond: Responder = () => ({})): {
  client: PoolClient;
  queries: RecordedQuery[];
} {
  const queries: RecordedQuery[] = [];
  const query = jest.fn((sql: string, params: readonly unknown[] = []) => {
    queries.push({ sql, params });
    // `respond` runs inside the executor, so a responder that throws looks like a failed query.
    return new Promise((resolve) => {
      resolve({ rows: [], rowCount: 0, ...respond(sql, params) });
    });
  });
  return { client: { query } as unknown as PoolClient, queries };
}

/**
 * A fake Database around one fake client: transaction(fn) runs fn with it (the specs check
 * which statements ran, so there is no real BEGIN/COMMIT), and pool.query uses it too.
 */
export function fakeDatabase(client: PoolClient): Database {
  async function transaction<T>(fn: (tx: PoolClient) => Promise<T>): Promise<T> {
    return await fn(client);
  }
  return { transaction, pool: client } as unknown as Database;
}

/** The recorded queries whose SQL contains `fragment`, in the order they ran. */
export function queriesWith(queries: readonly RecordedQuery[], fragment: string): RecordedQuery[] {
  return queries.filter((q) => q.sql.includes(fragment));
}

// ---- One factory per event type, all for ORDER_ID.

export function orderCreated(): OrderCreated {
  return createEvent<OrderCreated>('OrderCreated', ORDER_ID, {
    orderId: ORDER_ID,
    customerId: 'c-1',
    items: [{ sku: 'BOOK', quantity: 1, unitPriceCents: 4500 }],
    totalCents: 4500,
  });
}

export function orderConfirmed(): OrderConfirmed {
  return createEvent<OrderConfirmed>('OrderConfirmed', ORDER_ID, { orderId: ORDER_ID });
}

export function orderCancelled(reason: string): OrderCancelled {
  return createEvent<OrderCancelled>('OrderCancelled', ORDER_ID, { orderId: ORDER_ID, reason });
}

export function inventoryReserved(): InventoryReserved {
  return createEvent<InventoryReserved>('InventoryReserved', ORDER_ID, {
    orderId: ORDER_ID,
    items: [{ sku: 'BOOK', quantity: 1, unitPriceCents: 4500 }],
  });
}

export function inventoryRejected(reason: string): InventoryRejected {
  return createEvent<InventoryRejected>('InventoryRejected', ORDER_ID, { orderId: ORDER_ID, reason });
}

export function paymentSucceeded(): PaymentSucceeded {
  return createEvent<PaymentSucceeded>('PaymentSucceeded', ORDER_ID, {
    orderId: ORDER_ID,
    amountCents: 4500,
  });
}

export function paymentFailed(reason: string): PaymentFailed {
  return createEvent<PaymentFailed>('PaymentFailed', ORDER_ID, { orderId: ORDER_ID, reason });
}

/** One event of every type, for table-driven tests. */
export function oneOfEach(): ShopEvent[] {
  return [
    orderCreated(),
    orderConfirmed(),
    orderCancelled('customer changed their mind'),
    inventoryReserved(),
    inventoryRejected('out of stock'),
    paymentSucceeded(),
    paymentFailed('card declined'),
  ];
}
