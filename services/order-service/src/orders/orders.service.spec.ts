import { OrdersService } from './orders.service';

/** A fake Database that records every SQL statement run inside a transaction. */
function fakeDb() {
  const queries: { sql: string; params: unknown[] }[] = [];
  const client = { query: jest.fn(async (sql: string, params: unknown[]) => { queries.push({ sql, params }); return { rows: [] }; }) };
  return { queries, db: { transaction: (fn: any) => fn(client) } as any };
}

describe('OrdersService.create', () => {
  it('writes the order and the OrderCreated event in the same transaction', async () => {
    const { db, queries } = fakeDb();
    const result = await new OrdersService(db).create({
      customerId: 'c-1',
      items: [{ sku: 'BOOK', quantity: 2, unitPriceCents: 1500 }, { sku: 'PEN', quantity: 1, unitPriceCents: 200 }],
    });

    expect(result).toMatchObject({ status: 'PENDING', totalCents: 3200 });
    expect(queries).toHaveLength(2);
    expect(queries[0].sql).toContain('INSERT INTO orders');
    expect(queries[1].sql).toContain('INSERT INTO outbox');

    const [, topic, key, payload] = queries[1].params as string[];
    const event = JSON.parse(payload);
    expect(topic).toBe('orders');
    expect(key).toBe(result.orderId); // message key = order id -> per-order ordering
    expect(event).toMatchObject({ eventType: 'OrderCreated', version: 1, aggregateId: result.orderId });
    expect(event.payload.totalCents).toBe(3200);
  });

  it('stages no event when the order insert fails (the transaction rolls back)', async () => {
    const client = { query: jest.fn().mockRejectedValueOnce(new Error('db down')) };
    const db = { transaction: (fn: any) => fn(client) } as any;
    await expect(new OrdersService(db).create({ customerId: 'c', items: [{ sku: 'X', quantity: 1, unitPriceCents: 1 }] }))
      .rejects.toThrow('db down');
    expect(client.query).toHaveBeenCalledTimes(1); // outbox insert never ran
  });
});
