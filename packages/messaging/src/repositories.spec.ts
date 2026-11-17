import { createEvent, OrderConfirmed } from '@shop/contracts';
import { Database } from './database';
import { HealthController } from './health.controller';
import { claimEvent } from './inbox';
import { addToOutbox } from './outbox.repository';
import { fakeClient } from './testing';

const event = createEvent<OrderConfirmed>('OrderConfirmed', 'o-1', { orderId: 'o-1' });

describe('addToOutbox', () => {
  it('stores the whole envelope, keyed by the aggregate id', async () => {
    const { client, queries } = fakeClient(() => ({ rowCount: 1 }));
    await addToOutbox(client, 'orders', event);
    expect(queries[0]?.params).toEqual([event.eventId, 'orders', 'o-1', JSON.stringify(event)]);
  });

  it('refuses to continue if the insert did not write exactly one row', async () => {
    const { client } = fakeClient(() => ({ rowCount: 0 }));
    await expect(addToOutbox(client, 'orders', event)).rejects.toThrow('wrote 0 rows');
  });
});

describe('claimEvent', () => {
  it('returns true for a new event and false for one already processed', async () => {
    const fresh = fakeClient(() => ({ rowCount: 1 }));
    const seen = fakeClient(() => ({ rowCount: 0 }));
    expect(await claimEvent(fresh.client, event)).toBe(true);
    expect(await claimEvent(seen.client, event)).toBe(false);
  });
});

describe('HealthController', () => {
  it('answers ok when the database responds', async () => {
    const db = { pool: { query: jest.fn().mockResolvedValue({}) } } as unknown as Database;
    await expect(new HealthController(db).check()).resolves.toEqual({ status: 'ok' });
  });

  it('fails when the database does not', async () => {
    const db = { pool: { query: jest.fn().mockRejectedValue(new Error('down')) } } as unknown as Database;
    await expect(new HealthController(db).check()).rejects.toThrow('down');
  });
});
