import { InvariantError } from '@shop/contracts';
import { PoolClient } from 'pg';
import { MAX_ORDERS_PER_CUSTOMER, OrderViewRepository } from './order-view.repository';
import { emptyView, OrderView } from './projection';
import { at, fakeClient, fakeDatabase, ORDER_ID, RecordedQuery, Responder } from './testing';

/** A row exactly as pg returns it: TIMESTAMPTZ as Date, JSONB already parsed. */
const ROW = {
  order_id: ORDER_ID,
  customer_id: 'c-1',
  status: 'CONFIRMED',
  total_cents: 4500,
  items: [{ sku: 'BOOK', quantity: 1, unitPriceCents: 4500 }],
  inventory_status: 'RESERVED',
  payment_status: 'SUCCEEDED',
  cancel_reason: null,
  timeline: [{ eventType: 'OrderCreated', occurredAt: at(1) }],
  created_at: new Date(at(1)),
  updated_at: new Date(at(4)),
};

const VIEW: OrderView = {
  orderId: ORDER_ID,
  customerId: 'c-1',
  status: 'CONFIRMED',
  totalCents: 4500,
  items: [{ sku: 'BOOK', quantity: 1, unitPriceCents: 4500 }],
  inventoryStatus: 'RESERVED',
  paymentStatus: 'SUCCEEDED',
  cancelReason: null,
  timeline: [{ eventType: 'OrderCreated', occurredAt: at(1) }],
  createdAt: at(1),
  updatedAt: at(4),
};

/** A repository over a fake database; `tx` is the same fake client, standing in for a transaction. */
function setup(respond: Responder): { repo: OrderViewRepository; tx: PoolClient; queries: RecordedQuery[] } {
  const { client, queries } = fakeClient(respond);
  return { repo: new OrderViewRepository(fakeDatabase(client)), tx: client, queries };
}

describe('OrderViewRepository', () => {
  describe('loadForUpdate', () => {
    it('creates the row first, then locks it, so there is always a row to lock', async () => {
      const { repo, tx, queries } = setup((sql) =>
        sql.startsWith('SELECT') ? { rows: [ROW] } : { rowCount: 1 },
      );

      expect(await repo.loadForUpdate(tx, ORDER_ID, at(1))).toEqual(VIEW);
      expect(queries.map((q) => q.sql.trim().split(/\s+/)[0])).toEqual(['INSERT', 'SELECT']);
      expect(queries[0]?.sql).toContain('ON CONFLICT (order_id) DO NOTHING');
      expect(queries[0]?.params).toEqual([ORDER_ID, at(1)]);
      expect(queries[1]?.sql).toContain('FOR UPDATE');
    });

    it('maps a fresh placeholder row to an empty view', async () => {
      const placeholder = {
        ...ROW,
        customer_id: null,
        status: 'PENDING',
        total_cents: null,
        items: null,
        inventory_status: null,
        payment_status: null,
        timeline: [],
        created_at: null,
        updated_at: new Date(at(2)),
      };
      const { repo, tx } = setup((sql) =>
        sql.startsWith('SELECT') ? { rows: [placeholder] } : { rowCount: 1 },
      );

      expect(await repo.loadForUpdate(tx, ORDER_ID, at(2))).toEqual(emptyView(ORDER_ID, at(2)));
    });

    it('fails loudly if the row is still missing after the insert', async () => {
      const { repo, tx } = setup(() => ({ rows: [] }));
      await expect(repo.loadForUpdate(tx, ORDER_ID, at(1))).rejects.toThrow(InvariantError);
    });
  });

  describe('save', () => {
    it('updates every column of the locked row', async () => {
      const { repo, tx, queries } = setup(() => ({ rowCount: 1 }));

      await repo.save(tx, VIEW);
      expect(queries).toHaveLength(1);
      expect(queries[0]?.sql).toMatch(/^UPDATE order_view SET/);
      expect(queries[0]?.params).toEqual([
        ORDER_ID,
        'c-1',
        'CONFIRMED',
        4500,
        JSON.stringify(VIEW.items),
        'RESERVED',
        'SUCCEEDED',
        null,
        JSON.stringify(VIEW.timeline),
        at(1),
        at(4),
      ]);
    });

    it('stores missing items as SQL NULL, not the JSON text "null"', async () => {
      const { repo, tx, queries } = setup(() => ({ rowCount: 1 }));
      await repo.save(tx, emptyView(ORDER_ID, at(1)));
      expect(queries[0]?.params[4]).toBeNull();
    });

    it('fails loudly if the update did not change exactly one row', async () => {
      const { repo, tx } = setup(() => ({ rowCount: 0 }));
      await expect(repo.save(tx, VIEW)).rejects.toThrow(InvariantError);
    });
  });

  describe('queries', () => {
    it('byId maps the row, or returns null when the order is unknown', async () => {
      const found = setup(() => ({ rows: [ROW] }));
      expect(await found.repo.byId(ORDER_ID)).toEqual(VIEW);
      expect(found.queries[0]?.params).toEqual([ORDER_ID]);

      const missing = setup(() => ({ rows: [] }));
      expect(await missing.repo.byId(ORDER_ID)).toBeNull();
    });

    it('byCustomer returns at most MAX_ORDERS_PER_CUSTOMER views, newest first', async () => {
      const { repo, queries } = setup(() => ({ rows: [ROW, ROW] }));
      expect(await repo.byCustomer('c-1')).toEqual([VIEW, VIEW]);
      expect(queries[0]?.sql).toContain('ORDER BY created_at DESC NULLS LAST LIMIT $2');
      expect(queries[0]?.params).toEqual(['c-1', MAX_ORDERS_PER_CUSTOMER]);
    });

    it('stats keeps big sums exact: no int cast, bigint strings converted with Number()', async () => {
      const { repo, queries } = setup(() => ({
        rows: [{ status: 'CONFIRMED', orders: '3', total_cents: '3000000000' }],
      }));
      expect(await repo.stats()).toEqual([{ status: 'CONFIRMED', orders: 3, totalCents: 3_000_000_000 }]);
      expect(queries[0]?.sql).toContain('::bigint');
      expect(queries[0]?.sql).not.toContain('::int');
    });
  });
});
