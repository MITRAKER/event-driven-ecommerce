import { Logger } from '@nestjs/common';
import {
  createEvent,
  EventType,
  InventoryRejected,
  InventoryReserved,
  InvariantError,
  Limits,
  OrderCancelled,
  OrderConfirmed,
  OrderCreated,
  OrderItem,
  parseEvent,
  PaymentFailed,
  PaymentSucceeded,
  ShopEvent,
} from '@shop/contracts';
import { Database, KafkaClient, MessagingConfig } from '@shop/messaging';
import { PoolClient } from 'pg';
import { PaymentConfig, PaymentSettings } from './payment-config';
import { PaymentConsumer } from './payment.consumer';

const ORDER_ID = '6f1c2d3e-4b5a-4c6d-8e7f-90a1b2c3d4e5';

/** A payments row, as the fake database keeps it. */
interface PaymentRow {
  readonly amountCents: unknown;
  readonly status: unknown;
  readonly reason: unknown;
}

interface Answer {
  readonly rows: unknown[];
  readonly rowCount: number;
}

interface Tables {
  readonly claimed: Set<unknown>;
  readonly payments: Map<unknown, PaymentRow>;
  readonly outbox: ShopEvent[];
}

/**
 * An in-memory stand-in for the payments database. It plays both roles: the Database
 * (transaction) and the transaction's client (query). It understands exactly the statements the
 * consumer sends, and behaves like Postgres where these tests need it to:
 * - a transaction that throws leaves nothing behind (ROLLBACK);
 * - payments.order_id is the primary key, so a second payment for one order is refused.
 * Every staged event goes through parseEvent, as it will in the next consumer, so the tests also
 * prove that what payment publishes meets the contract.
 */
class FakePaymentsDb {
  /** Every statement in the order it ran, including those of rolled-back transactions. */
  readonly statements: string[] = [];
  /** Make the payments INSERT report this row count, to fake what Postgres would never do. */
  insertRowCount: number | undefined = undefined;
  private tables: Tables = { claimed: new Set(), payments: new Map(), outbox: [] };

  get payments(): ReadonlyMap<unknown, PaymentRow> {
    return this.tables.payments;
  }

  get outbox(): readonly ShopEvent[] {
    return this.tables.outbox;
  }

  /** Work on a copy of the tables, and keep it only if fn succeeds: COMMIT or ROLLBACK. */
  async transaction<T>(fn: (tx: PoolClient) => Promise<T>): Promise<T> {
    const committed = this.tables;
    this.tables = {
      claimed: new Set(committed.claimed),
      payments: new Map(committed.payments),
      outbox: [...committed.outbox],
    };
    try {
      return await fn(this as unknown as PoolClient);
    } catch (err) {
      this.tables = committed;
      throw err;
    }
  }

  query(sql: string, params: readonly unknown[] = []): Promise<Answer> {
    this.statements.push(sql);
    // Run inside the promise, so a refused statement becomes a rejected query, as with pg.
    return new Promise((resolve) => {
      resolve(this.run(sql, params));
    });
  }

  private run(sql: string, params: readonly unknown[]): Answer {
    const [first, second, third, fourth] = params;
    if (sql.startsWith('INSERT INTO processed_events')) return this.claim(first);
    if (sql.startsWith('INSERT INTO payments')) {
      return this.insertPayment(first, { amountCents: second, status: third, reason: fourth });
    }
    if (sql.startsWith('INSERT INTO outbox')) return this.stage(fourth);
    throw new Error(`the fake database does not understand: ${sql}`);
  }

  private claim(eventId: unknown): Answer {
    const fresh = !this.tables.claimed.has(eventId);
    this.tables.claimed.add(eventId);
    return { rows: [], rowCount: fresh ? 1 : 0 };
  }

  private insertPayment(orderId: unknown, row: PaymentRow): Answer {
    if (this.tables.payments.has(orderId)) {
      // What pg throws for a primary key violation (SQLSTATE 23505).
      const message = 'duplicate key value violates unique constraint "payments_pkey"';
      throw Object.assign(new Error(message), { code: '23505' });
    }
    this.tables.payments.set(orderId, row);
    return { rows: [], rowCount: this.insertRowCount ?? 1 };
  }

  private stage(eventJson: unknown): Answer {
    this.tables.outbox.push(parseEvent(JSON.parse(String(eventJson))));
    return { rows: [], rowCount: 1 };
  }
}

/** A bug on purpose: handledTypes lets in a type that handle() has no case for. */
class MisconfiguredConsumer extends PaymentConsumer {
  protected override readonly handledTypes: ReadonlySet<EventType> = new Set<EventType>(['OrderCreated']);
}

const MESSAGING_CONFIG = new MessagingConfig({
  serviceName: 'payment-service',
  port: 3003,
  databaseUrl: 'postgres://test',
  databasePoolMax: 1,
  kafkaBrokers: ['localhost:19092'],
  outboxPollMs: 500,
  outboxBatchSize: 100,
  consumerRetryAttempts: 1,
  consumerRetryBaseMs: 1,
});

/** The defaults: decline above $1,000, no random failures. */
const SETTINGS: PaymentSettings = { declineAboveCents: 100_000, failureRate: 0 };

function setup(settings: Partial<PaymentSettings> = {}): { consumer: PaymentConsumer; db: FakePaymentsDb } {
  const db = new FakePaymentsDb();
  const kafka = {} as unknown as KafkaClient; // process() never touches Kafka
  const paymentConfig = new PaymentConfig({ ...SETTINGS, ...settings });
  return {
    consumer: new PaymentConsumer(db as unknown as Database, kafka, MESSAGING_CONFIG, paymentConfig),
    db,
  };
}

function item(sku: string, quantity: number, unitPriceCents: number): OrderItem {
  return { sku, quantity, unitPriceCents };
}

function inventoryReserved(items: OrderItem[]): InventoryReserved {
  return createEvent<InventoryReserved>('InventoryReserved', ORDER_ID, { orderId: ORDER_ID, items });
}

/** Every other event type. Payment has no business with any of them. */
function otherEvents(): ShopEvent[] {
  const orderId = ORDER_ID;
  const created = { orderId, customerId: 'c-1', items: [item('BOOK', 1, 4500)], totalCents: 4500 };
  return [
    createEvent<OrderCreated>('OrderCreated', orderId, created),
    createEvent<OrderConfirmed>('OrderConfirmed', orderId, { orderId }),
    createEvent<OrderCancelled>('OrderCancelled', orderId, { orderId, reason: 'payment: card declined' }),
    createEvent<InventoryRejected>('InventoryRejected', orderId, { orderId, reason: 'out of stock' }),
    createEvent<PaymentSucceeded>('PaymentSucceeded', orderId, { orderId, amountCents: 4500 }),
    createEvent<PaymentFailed>('PaymentFailed', orderId, { orderId, reason: 'card declined' }),
  ];
}

/** "INSERT INTO payments (order_id, ...) VALUES ..." becomes "INSERT INTO payments". */
function head(sql: string): string {
  return sql.split(' (')[0] ?? sql;
}

beforeAll(() => {
  // Keep the test output readable: the consumer logs every event it handles.
  Logger.overrideLogger(false);
});

describe('PaymentConsumer: charging an order (InventoryReserved)', () => {
  it('charges the order total and announces PaymentSucceeded, all in one transaction', async () => {
    const { consumer, db } = setup();
    const event = inventoryReserved([item('BOOK', 2, 4500), item('MUG', 1, 1200)]);

    expect(await consumer.process(event)).toBe('handled');

    // claim the eventId, record the payment, stage the outcome
    expect(db.statements.map(head)).toEqual([
      'INSERT INTO processed_events',
      'INSERT INTO payments',
      'INSERT INTO outbox',
    ]);
    expect(db.payments.get(ORDER_ID)).toEqual({ amountCents: 10_200, status: 'SUCCEEDED', reason: null });
    expect(db.outbox).toEqual([
      expect.objectContaining({
        eventType: 'PaymentSucceeded',
        aggregateId: ORDER_ID,
        payload: { orderId: ORDER_ID, amountCents: 10_200 },
        causationId: event.eventId, // the trace chain: this event happened because of that one
        correlationId: event.correlationId,
      }),
    ]);
  });

  it('declines an order over the limit and announces PaymentFailed with the reason', async () => {
    const { consumer, db } = setup();
    const event = inventoryReserved([item('LAPTOP', 1, 150_000)]);

    expect(await consumer.process(event)).toBe('handled');

    const reason = 'card declined: amount over limit';
    expect(db.payments.get(ORDER_ID)).toEqual({ amountCents: 150_000, status: 'FAILED', reason });
    expect(db.outbox).toEqual([
      expect.objectContaining({
        eventType: 'PaymentFailed',
        aggregateId: ORDER_ID,
        payload: { orderId: ORDER_ID, reason },
        causationId: event.eventId,
        correlationId: event.correlationId,
      }),
    ]);
  });

  it('takes its limit from PaymentConfig: with a $40 limit, a $45 book is declined', async () => {
    const { consumer, db } = setup({ declineAboveCents: 4_000 });
    await consumer.process(inventoryReserved([item('BOOK', 1, 4500)]));
    expect(db.payments.get(ORDER_ID)?.status).toBe('FAILED');
  });

  it('takes its failure rate from PaymentConfig: at 1, every charge fails', async () => {
    const { consumer, db } = setup({ failureRate: 1 });
    await consumer.process(inventoryReserved([item('BOOK', 1, 4500)]));
    expect(db.outbox.map((e) => e.payload)).toEqual([
      { orderId: ORDER_ID, reason: 'card declined: random failure' },
    ]);
  });

  it('skips a redelivered event, so the card is charged once', async () => {
    const { consumer, db } = setup();
    const event = inventoryReserved([item('BOOK', 1, 4500)]);

    expect(await consumer.process(event)).toBe('handled');
    expect(await consumer.process(event)).toBe('duplicate');
    expect(db.payments.size).toBe(1);
    expect(db.outbox).toHaveLength(1);
  });
});

describe('PaymentConsumer: what must never happen', () => {
  it('refuses a total above the contract limit (a bug upstream) before it charges or writes anything', async () => {
    const { consumer, db } = setup();
    // Each line is within the per-line limits that parseEvent checks, but together: 10^10 cents.
    const event = inventoryReserved([item('GOLD', Limits.maxQuantityPerItem, Limits.maxUnitPriceCents)]);

    const result = consumer.process(event);
    await expect(result).rejects.toThrow(InvariantError);
    await expect(result).rejects.toThrow(
      `order ${ORDER_ID}: total 10000000000 cents is above the limit of ${Limits.maxOrderTotalCents}`,
    );
    expect(db.statements.map(head)).toEqual(['INSERT INTO processed_events']);
    expect(db.outbox).toEqual([]);
  });

  it('stops with an InvariantError if the payment insert did not write exactly one row', async () => {
    const { consumer, db } = setup();
    db.insertRowCount = 0;

    const result = consumer.process(inventoryReserved([item('BOOK', 1, 4500)]));
    await expect(result).rejects.toThrow(InvariantError);
    await expect(result).rejects.toThrow(`payment insert for order ${ORDER_ID} wrote 0 rows`);
    // The transaction rolled back: no payment, and no outcome event for the order service.
    expect(db.payments.size).toBe(0);
    expect(db.outbox).toEqual([]);
  });

  it('never charges an order twice: the primary key refuses a second payment', async () => {
    const { consumer, db } = setup();
    await consumer.process(inventoryReserved([item('BOOK', 1, 4500)]));

    // A different event (new eventId) for the same order: the eventId dedupe can't catch it.
    const again = inventoryReserved([item('BOOK', 1, 4500)]);
    await expect(consumer.process(again)).rejects.toThrow('payments_pkey');
    expect(db.payments.size).toBe(1);
    expect(db.outbox.map((e) => e.eventType)).toEqual(['PaymentSucceeded']);
  });

  it('fails loudly if handledTypes lets in a type that handle() has no case for', async () => {
    const db = new FakePaymentsDb();
    const consumer = new MisconfiguredConsumer(
      db as unknown as Database,
      {} as unknown as KafkaClient,
      MESSAGING_CONFIG,
      new PaymentConfig(SETTINGS),
    );
    const [created] = otherEvents();
    expect(created?.eventType).toBe('OrderCreated');

    // InvariantError counts as permanent: the message goes to the dead-letter topic, not into a retry loop.
    await expect(consumer.process(created ?? inventoryReserved([]))).rejects.toThrow(
      'payment-service cannot handle OrderCreated',
    );
  });
});

describe('PaymentConsumer: wiring', () => {
  it.each(otherEvents().map((event) => [event.eventType, event] as const))(
    'ignores %s without touching the database',
    async (_type, event) => {
      const { consumer, db } = setup();
      expect(await consumer.process(event)).toBe('ignored');
      expect(db.statements).toEqual([]);
    },
  );

  it('subscribes to the inventory topic as consumer group payment-service', async () => {
    const subscribe = jest.fn();
    const consumerFactory = jest.fn(() => ({ connect: jest.fn(), subscribe, run: jest.fn() }));
    const kafka = {
      ensureTopics: jest.fn(),
      kafka: { consumer: consumerFactory, producer: () => ({ connect: jest.fn() }) },
    } as unknown as KafkaClient;
    const consumer = new PaymentConsumer(
      new FakePaymentsDb() as unknown as Database,
      kafka,
      MESSAGING_CONFIG,
      new PaymentConfig(SETTINGS),
    );

    await consumer.onApplicationBootstrap();
    expect(consumerFactory).toHaveBeenCalledWith({ groupId: 'payment-service' });
    expect(subscribe).toHaveBeenCalledWith({ topics: ['inventory'], fromBeginning: true });
  });
});
