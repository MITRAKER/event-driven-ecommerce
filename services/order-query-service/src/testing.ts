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
} from '@shop/contracts';
import { Database, MessagingConfig } from '@shop/messaging';
import { PoolClient } from 'pg';

export const ORDER_ID = '0b6f6c8e-3f1a-4c55-9d7e-2a4b8c1d9e10';

export const TEST_CONFIG = new MessagingConfig({
  serviceName: 'order-query-service',
  port: 3004,
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

/** A fake pg client: records every query and answers with whatever `respond` returns. */
export function fakeClient(respond: Responder): { client: PoolClient; queries: RecordedQuery[] } {
  const queries: RecordedQuery[] = [];
  const query = jest.fn(async (sql: string, params: readonly unknown[] = []) => {
    queries.push({ sql, params });
    return await Promise.resolve({ rows: [], rowCount: 0, ...respond(sql, params) });
  });
  return { client: { query } as unknown as PoolClient, queries };
}

/** A fake Database around one fake client: transaction(fn) runs fn with it, and pool.query uses it too. */
export function fakeDatabase(client: PoolClient): Database {
  async function transaction<T>(fn: (tx: PoolClient) => Promise<T>): Promise<T> {
    return await fn(client);
  }
  return { transaction, pool: client } as unknown as Database;
}

/** A timestamp `second` seconds after a fixed start, so tests control the order events happened in. */
export function at(second: number): string {
  return new Date(Date.UTC(2026, 0, 1, 0, 0, second)).toISOString();
}

// ---- One factory per event type, all for ORDER_ID, each stamped with a chosen time.

export function orderCreated(second: number): OrderCreated {
  const event = createEvent<OrderCreated>('OrderCreated', ORDER_ID, {
    orderId: ORDER_ID,
    customerId: 'c-1',
    items: [{ sku: 'LAPTOP', quantity: 1, unitPriceCents: 150000 }],
    totalCents: 150000,
  });
  return { ...event, occurredAt: at(second) };
}

export function inventoryReserved(second: number): InventoryReserved {
  const event = createEvent<InventoryReserved>('InventoryReserved', ORDER_ID, {
    orderId: ORDER_ID,
    items: [{ sku: 'LAPTOP', quantity: 1, unitPriceCents: 150000 }],
  });
  return { ...event, occurredAt: at(second) };
}

export function inventoryRejected(second: number, reason: string): InventoryRejected {
  const event = createEvent<InventoryRejected>('InventoryRejected', ORDER_ID, { orderId: ORDER_ID, reason });
  return { ...event, occurredAt: at(second) };
}

export function paymentSucceeded(second: number): PaymentSucceeded {
  const event = createEvent<PaymentSucceeded>('PaymentSucceeded', ORDER_ID, {
    orderId: ORDER_ID,
    amountCents: 150000,
  });
  return { ...event, occurredAt: at(second) };
}

export function paymentFailed(second: number, reason: string): PaymentFailed {
  const event = createEvent<PaymentFailed>('PaymentFailed', ORDER_ID, { orderId: ORDER_ID, reason });
  return { ...event, occurredAt: at(second) };
}

export function orderConfirmed(second: number): OrderConfirmed {
  const event = createEvent<OrderConfirmed>('OrderConfirmed', ORDER_ID, { orderId: ORDER_ID });
  return { ...event, occurredAt: at(second) };
}

export function orderCancelled(second: number, reason: string): OrderCancelled {
  const event = createEvent<OrderCancelled>('OrderCancelled', ORDER_ID, { orderId: ORDER_ID, reason });
  return { ...event, occurredAt: at(second) };
}
