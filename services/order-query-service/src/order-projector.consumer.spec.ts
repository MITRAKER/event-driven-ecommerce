import { Logger } from '@nestjs/common';
import { EVENT_TYPES, InvariantError, ShopEvent, Topics } from '@shop/contracts';
import { KafkaClient } from '@shop/messaging';
import { PoolClient } from 'pg';
import { OrderProjector } from './order-projector.consumer';
import { OrderViewRepository } from './order-view.repository';
import {
  at,
  fakeClient,
  fakeDatabase,
  inventoryRejected,
  inventoryReserved,
  ORDER_ID,
  orderCancelled,
  orderConfirmed,
  orderCreated,
  paymentFailed,
  paymentSucceeded,
  RecordedQuery,
  TEST_CONFIG,
} from './testing';

/** The row loadForUpdate() creates for a new order, as pg returns it (every column, mostly NULL). */
const PLACEHOLDER_ROW = {
  order_id: ORDER_ID,
  customer_id: null,
  status: 'PENDING',
  total_cents: null,
  items: null,
  inventory_status: null,
  payment_status: null,
  cancel_reason: null,
  timeline: [],
  created_at: null,
  updated_at: new Date(at(1)),
};

/** A projector over a fake database. `fresh: false` makes processed_events report a duplicate. */
function setup(fresh = true): { projector: OrderProjector; queries: RecordedQuery[] } {
  const { client, queries } = fakeClient((sql) => {
    if (sql.includes('processed_events')) return { rowCount: fresh ? 1 : 0 };
    if (sql.startsWith('SELECT')) {
      // The placeholder row loadForUpdate() just created for a new order.
      return { rows: [PLACEHOLDER_ROW] };
    }
    return { rowCount: 1 };
  });
  const db = fakeDatabase(client);
  const kafka = {} as unknown as KafkaClient; // process() never touches Kafka
  return { projector: new OrderProjector(db, kafka, TEST_CONFIG, new OrderViewRepository(db)), queries };
}

/** Exposes handle() so the impossible default branch can be tested. */
class ExposedProjector extends OrderProjector {
  async run(event: ShopEvent, tx: PoolClient): Promise<void> {
    await this.handle(event, tx);
  }
}

describe('OrderProjector', () => {
  const originalGroupId = process.env['PROJECTION_GROUP_ID'];

  beforeEach(() => {
    jest.spyOn(Logger.prototype, 'log').mockImplementation(() => undefined);
    jest.spyOn(Logger.prototype, 'warn').mockImplementation(() => undefined);
  });

  afterEach(() => {
    jest.restoreAllMocks();
    if (originalGroupId === undefined) delete process.env['PROJECTION_GROUP_ID'];
    else process.env['PROJECTION_GROUP_ID'] = originalGroupId;
  });

  it('folds an event into the view: claim, create-or-lock the row, then update it, in one transaction', async () => {
    const { projector, queries } = setup();

    expect(await projector.process(orderCreated(2))).toBe('handled');
    expect(queries.map((q) => q.sql.trim().split(/\s+/).slice(0, 3).join(' '))).toEqual([
      'INSERT INTO processed_events',
      'INSERT INTO order_view',
      'SELECT order_id, customer_id,',
      'UPDATE order_view SET',
    ]);
    const update = queries[3]?.params ?? [];
    expect(update[0]).toBe(ORDER_ID);
    expect(update[1]).toBe('c-1'); // customer_id came from OrderCreated
    expect(update[10]).toBe(at(2)); // updated_at moved forward to the event's time
  });

  it('skips a redelivered event without touching the view', async () => {
    const { projector, queries } = setup(false);

    expect(await projector.process(paymentSucceeded(3))).toBe('duplicate');
    expect(queries).toHaveLength(1);
  });

  it.each([
    orderCreated(2),
    orderConfirmed(3),
    orderCancelled(3, 'payment: declined'),
    inventoryReserved(2),
    inventoryRejected(2, 'out of stock: BOOK'),
    paymentSucceeded(3),
    paymentFailed(3, 'declined'),
  ])('projects every event type ($eventType)', async (event) => {
    const { projector, queries } = setup();
    expect(await projector.process(event)).toBe('handled');
    expect(queries.some((q) => q.sql.startsWith('UPDATE order_view'))).toBe(true);
  });

  it('reads all three topics and handles every event type there is', () => {
    const { projector } = setup();
    const internals = projector as unknown as {
      topics: readonly string[];
      handledTypes: ReadonlySet<string>;
    };
    expect(internals.topics).toEqual([Topics.Orders, Topics.Inventory, Topics.Payments]);
    expect([...internals.handledTypes].sort()).toEqual([...EVENT_TYPES].sort());
  });

  it('takes its consumer group from PROJECTION_GROUP_ID, so a rebuild can start a fresh group', () => {
    delete process.env['PROJECTION_GROUP_ID'];
    expect((setup().projector as unknown as { groupId: string }).groupId).toBe('order-query-v1');

    process.env['PROJECTION_GROUP_ID'] = 'order-query-v2';
    expect((setup().projector as unknown as { groupId: string }).groupId).toBe('order-query-v2');
  });

  it('throws if handle() ever receives an event type it does not know', async () => {
    const { client } = fakeClient(() => ({}));
    const db = fakeDatabase(client);
    const projector = new ExposedProjector(
      db,
      {} as unknown as KafkaClient,
      TEST_CONFIG,
      new OrderViewRepository(db),
    );
    const forged = { ...orderConfirmed(1), eventType: 'OrderShipped' } as unknown as ShopEvent;

    await expect(projector.run(forged, client)).rejects.toThrow(InvariantError);
  });
});
