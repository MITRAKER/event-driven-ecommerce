import { Logger } from '@nestjs/common';
import {
  createEvent,
  InvariantError,
  OrderCancelled,
  OrderConfirmed,
  OrderCreated,
  OrderItem,
  ShopEvent,
  Topic,
} from '@shop/contracts';
import { Database, KafkaClient, MessagingConfig } from '@shop/messaging';
import { PoolClient } from 'pg';
import { InventoryConsumer } from './inventory.consumer';

interface Product {
  available: number;
  priceCents: number;
}

interface Reservation {
  items: OrderItem[];
  status: 'RESERVED' | 'RELEASED';
}

interface Answer {
  rows: unknown[];
  rowCount: number;
}

/**
 * An in-memory stand-in for the inventory database. It understands exactly the statements the
 * consumer sends, records each one, and keeps products, reservations and outbox rows in plain
 * maps, so a test can say "stock went from 10 to 9" instead of matching SQL text everywhere.
 * It plays both roles: the Database (transaction) and the transaction's client (query).
 */
class FakeInventoryDb {
  readonly products: Map<string, Product>;
  readonly reservations = new Map<string, Reservation>();
  readonly outbox: ShopEvent[] = [];
  readonly statements: { sql: string; params: readonly unknown[] }[] = [];
  private readonly claimed = new Set<unknown>();
  private readonly overrides = new Map<string, Answer>();

  constructor(catalog: Readonly<Record<string, Product>>) {
    this.products = new Map(Object.entries(catalog).map(([sku, product]) => [sku, { ...product }]));
  }

  async transaction<T>(fn: (tx: PoolClient) => Promise<T>): Promise<T> {
    return await fn(this as unknown as PoolClient);
  }

  query(sql: string, params: readonly unknown[] = []): Promise<Answer> {
    const statement = sql.replace(/\s+/g, ' ').trim();
    this.statements.push({ sql: statement, params });
    return Promise.resolve(this.overrideFor(statement) ?? this.simulate(statement, params));
  }

  /** Answer statements starting with `prefix` like this, to fake what Postgres would never do. */
  override(prefix: string, answer: Answer): void {
    this.overrides.set(prefix, answer);
  }

  /** Stock left per sku, e.g. { BOOK: 9, MUG: 25 }. */
  stock(): Record<string, number> {
    return Object.fromEntries([...this.products].map(([sku, product]) => [sku, product.available]));
  }

  /** Every stock UPDATE in the order it ran, as [sku, change]. */
  stockChanges(): unknown[][] {
    return this.statements.filter((s) => s.sql.startsWith('UPDATE products')).map((s) => [...s.params]);
  }

  private overrideFor(statement: string): Answer | undefined {
    return [...this.overrides].find(([prefix]) => statement.startsWith(prefix))?.[1];
  }

  private simulate(sql: string, params: readonly unknown[]): Answer {
    const [first, second] = params;
    if (sql.startsWith('INSERT INTO processed_events')) return this.claim(first);
    if (sql.startsWith('SELECT sku, available, price_cents FROM products')) {
      return this.lockRows(first as string[]);
    }
    if (sql.startsWith('UPDATE products')) return this.addStock(first as string, second as number);
    if (sql.startsWith('INSERT INTO reservations')) return this.reserve(first as string, second as string);
    if (sql.startsWith('UPDATE reservations')) return this.release(first as string);
    if (sql.startsWith('INSERT INTO outbox')) return this.stage(params[3] as string);
    throw new Error(`the fake database does not understand: ${sql}`);
  }

  private claim(eventId: unknown): Answer {
    const fresh = !this.claimed.has(eventId);
    this.claimed.add(eventId);
    return { rows: [], rowCount: fresh ? 1 : 0 };
  }

  private lockRows(skus: string[]): Answer {
    const rows = [...this.products]
      .filter(([sku]) => skus.includes(sku))
      .sort(([a], [b]) => (a < b ? -1 : 1))
      .map(([sku, p]) => ({ sku, available: p.available, price_cents: p.priceCents }));
    return { rows, rowCount: rows.length };
  }

  private addStock(sku: string, change: number): Answer {
    const product = this.products.get(sku);
    if (product === undefined) return { rows: [], rowCount: 0 };
    product.available += change;
    return { rows: [], rowCount: 1 };
  }

  private reserve(orderId: string, itemsJson: string): Answer {
    this.reservations.set(orderId, { items: JSON.parse(itemsJson) as OrderItem[], status: 'RESERVED' });
    return { rows: [], rowCount: 1 };
  }

  private release(orderId: string): Answer {
    const reservation = this.reservations.get(orderId);
    if (reservation?.status !== 'RESERVED') return { rows: [], rowCount: 0 };
    reservation.status = 'RELEASED';
    return { rows: [{ items: reservation.items }], rowCount: 1 };
  }

  private stage(eventJson: string): Answer {
    this.outbox.push(JSON.parse(eventJson) as ShopEvent);
    return { rows: [], rowCount: 1 };
  }
}

/** Opens up what only Nest and the base class normally see. */
class InspectableConsumer extends InventoryConsumer {
  subscription(): { groupId: string; topics: readonly Topic[] } {
    return { groupId: this.groupId, topics: this.topics };
  }

  async handleDirectly(event: ShopEvent, tx: PoolClient): Promise<void> {
    await this.handle(event, tx);
  }
}

const CATALOG = {
  BOOK: { available: 10, priceCents: 4500 },
  KEYBOARD: { available: 2, priceCents: 9000 },
  LAPTOP: { available: 5, priceCents: 150_000 },
  MUG: { available: 25, priceCents: 1200 },
};

const config = new MessagingConfig({
  serviceName: 'inventory-service',
  port: 3002,
  databaseUrl: 'postgres://test',
  databasePoolMax: 1,
  kafkaBrokers: ['localhost:19092'],
  outboxPollMs: 500,
  outboxBatchSize: 100,
  consumerRetryAttempts: 1,
  consumerRetryBaseMs: 1,
});

function setup(): { consumer: InspectableConsumer; db: FakeInventoryDb } {
  const db = new FakeInventoryDb(CATALOG);
  const consumer = new InspectableConsumer(db as unknown as Database, {} as unknown as KafkaClient, config);
  return { consumer, db };
}

function item(sku: string, quantity: number, unitPriceCents: number): OrderItem {
  return { sku, quantity, unitPriceCents };
}

function orderCreated(orderId: string, items: OrderItem[]): OrderCreated {
  const totalCents = items.reduce((sum, line) => sum + line.quantity * line.unitPriceCents, 0);
  return createEvent<OrderCreated>('OrderCreated', orderId, {
    orderId,
    customerId: 'c-1',
    items,
    totalCents,
  });
}

function orderCancelled(orderId: string): OrderCancelled {
  return createEvent<OrderCancelled>('OrderCancelled', orderId, { orderId, reason: 'payment declined' });
}

beforeAll(() => {
  // Keep the test output readable: the consumer logs every event it handles.
  Logger.overrideLogger(false);
});

describe('InventoryConsumer: reserve (OrderCreated)', () => {
  it('takes merged quantities in sku order, records the reservation and publishes InventoryReserved', async () => {
    const { consumer, db } = setup();
    const items = [item('MUG', 2, 1200), item('BOOK', 1, 4500), item('MUG', 1, 1200)];
    const event = orderCreated('o-1', items);

    expect(await consumer.process(event)).toBe('handled');

    expect(db.stockChanges()).toEqual([
      ['BOOK', -1],
      ['MUG', -3],
    ]);
    expect(db.stock()).toMatchObject({ BOOK: 9, MUG: 22 });
    expect(db.reservations.get('o-1')).toEqual({ items, status: 'RESERVED' });
    expect(db.outbox).toEqual([
      expect.objectContaining({
        eventType: 'InventoryReserved',
        aggregateId: 'o-1',
        causationId: event.eventId,
        correlationId: event.correlationId,
        payload: { orderId: 'o-1', items },
      }),
    ]);
  });

  it('locks the product rows in sku order before it reads or changes stock', async () => {
    const { consumer, db } = setup();
    await consumer.process(orderCreated('o-1', [item('MUG', 1, 1200), item('BOOK', 1, 4500)]));
    const sql = db.statements.map((s) => s.sql);
    const lock = sql.findIndex((s) => s.startsWith('SELECT'));
    expect(sql[lock]).toContain('ORDER BY sku FOR UPDATE');
    expect(lock).toBeLessThan(sql.findIndex((s) => s.startsWith('UPDATE products')));
  });

  it('rejects the whole order when one product is short, and leaves all stock alone', async () => {
    const { consumer, db } = setup();
    await consumer.process(orderCreated('o-2', [item('BOOK', 1, 4500), item('KEYBOARD', 3, 9000)]));

    expect(db.stockChanges()).toEqual([]);
    expect(db.stock()).toEqual({ BOOK: 10, KEYBOARD: 2, LAPTOP: 5, MUG: 25 });
    expect(db.reservations.size).toBe(0);
    expect(db.outbox).toEqual([
      expect.objectContaining({
        eventType: 'InventoryRejected',
        payload: { orderId: 'o-2', reason: 'out of stock: KEYBOARD (wanted 3, have 2)' },
      }),
    ]);
  });

  it('rejects a line priced differently from the catalog: nobody buys a laptop for 0 cents', async () => {
    const { consumer, db } = setup();
    await consumer.process(orderCreated('o-3', [item('LAPTOP', 1, 0)]));

    expect(db.stockChanges()).toEqual([]);
    expect(db.outbox).toEqual([
      expect.objectContaining({
        eventType: 'InventoryRejected',
        payload: { orderId: 'o-3', reason: 'price changed for LAPTOP: expected 150000, got 0' },
      }),
    ]);
  });

  it('rejects a product the catalog does not have', async () => {
    const { consumer, db } = setup();
    await consumer.process(orderCreated('o-4', [item('GHOST', 1, 100)]));
    expect(db.outbox.map((e) => e.payload)).toEqual([{ orderId: 'o-4', reason: 'unknown sku GHOST' }]);
  });

  it('stops with an InvariantError if the reservation insert did not write exactly one row', async () => {
    const { consumer, db } = setup();
    db.override('INSERT INTO reservations', { rows: [], rowCount: 0 });
    await expect(consumer.process(orderCreated('o-5', [item('BOOK', 1, 4500)]))).rejects.toThrow(
      InvariantError,
    );
    expect(db.outbox).toEqual([]);
  });
});

describe('InventoryConsumer: release (OrderCancelled), the compensation', () => {
  it('gives the stock back: merged quantities, locked and updated in sku order', async () => {
    const { consumer, db } = setup();
    db.reservations.set('o-1', {
      items: [item('MUG', 2, 1200), item('BOOK', 1, 4500), item('MUG', 1, 1200)],
      status: 'RESERVED',
    });

    expect(await consumer.process(orderCancelled('o-1'))).toBe('handled');

    expect(db.stockChanges()).toEqual([
      ['BOOK', 1],
      ['MUG', 3],
    ]);
    expect(db.stock()).toMatchObject({ BOOK: 11, MUG: 28 });
    expect(db.reservations.get('o-1')?.status).toBe('RELEASED');
    const sql = db.statements.map((s) => s.sql);
    expect(sql.findIndex((s) => s.endsWith('ORDER BY sku FOR UPDATE'))).toBeLessThan(
      sql.findIndex((s) => s.startsWith('UPDATE products')),
    );
    expect(db.outbox).toEqual([]); // releasing publishes nothing
  });

  it('restores exactly what a reservation took', async () => {
    const { consumer, db } = setup();
    await consumer.process(orderCreated('o-1', [item('LAPTOP', 1, 150_000), item('MUG', 2, 1200)]));
    expect(db.stock()).toMatchObject({ LAPTOP: 4, MUG: 23 });

    await consumer.process(orderCancelled('o-1'));
    expect(db.stock()).toEqual({ BOOK: 10, KEYBOARD: 2, LAPTOP: 5, MUG: 25 });
  });

  it('is a no-op for an order with nothing reserved (it was rejected)', async () => {
    const { consumer, db } = setup();
    expect(await consumer.process(orderCancelled('o-rejected'))).toBe('handled');
    expect(db.statements.some((s) => s.sql.includes('products'))).toBe(false);
  });

  it('releases only once, even when a second cancel arrives', async () => {
    const { consumer, db } = setup();
    await consumer.process(orderCreated('o-1', [item('BOOK', 2, 4500)]));
    await consumer.process(orderCancelled('o-1'));
    await consumer.process(orderCancelled('o-1'));
    expect(db.stock()).toMatchObject({ BOOK: 10 });
  });

  it('stops with an InvariantError if a stock update does not touch exactly one row', async () => {
    const { consumer, db } = setup();
    db.reservations.set('o-1', { items: [item('DISCONTINUED', 1, 100)], status: 'RESERVED' });
    await expect(consumer.process(orderCancelled('o-1'))).rejects.toThrow(
      'stock update for DISCONTINUED changed 0 rows',
    );
  });
});

describe('InventoryConsumer: wiring', () => {
  it('consumes the orders topic as the inventory-service consumer group', () => {
    expect(setup().consumer.subscription()).toEqual({ groupId: 'inventory-service', topics: ['orders'] });
  });

  it('ignores order events it has no business with', async () => {
    const { consumer, db } = setup();
    const confirmed = createEvent<OrderConfirmed>('OrderConfirmed', 'o-1', { orderId: 'o-1' });
    expect(await consumer.process(confirmed)).toBe('ignored');
    expect(db.statements).toEqual([]);
  });

  it('skips a redelivered event, so stock is taken once', async () => {
    const { consumer, db } = setup();
    const event = orderCreated('o-1', [item('BOOK', 1, 4500)]);
    expect(await consumer.process(event)).toBe('handled');
    expect(await consumer.process(event)).toBe('duplicate');
    expect(db.stock()).toMatchObject({ BOOK: 9 });
  });

  it('refuses an event type that handle() was not built for', async () => {
    const { consumer, db } = setup();
    const confirmed = createEvent<OrderConfirmed>('OrderConfirmed', 'o-1', { orderId: 'o-1' });
    await expect(consumer.handleDirectly(confirmed, db as unknown as PoolClient)).rejects.toThrow(
      'inventory-service cannot handle OrderConfirmed',
    );
  });
});
