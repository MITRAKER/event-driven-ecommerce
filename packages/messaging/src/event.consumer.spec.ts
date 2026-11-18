import { createEvent, ShopEvent } from '@shop/contracts';
import { EventConsumer } from './event.consumer';

/** Fake transaction client: processed_events behaves like a real unique table. */
function fakeDb() {
  const seen = new Set<string>();
  const client = {
    query: jest.fn(async (sql: string, params: any[]) => {
      if (sql.includes('processed_events')) {
        const fresh = !seen.has(params[0]);
        seen.add(params[0]);
        return { rowCount: fresh ? 1 : 0 };
      }
      return { rowCount: 0, rows: [] };
    }),
  };
  return { transaction: (fn: any) => fn(client) } as any;
}

class TestConsumer extends EventConsumer {
  groupId = 'test';
  topics = ['orders'];
  calls: string[] = [];
  handlers = { OrderCreated: async (e: ShopEvent) => { this.calls.push(e.eventId); } };
}

describe('EventConsumer', () => {
  const event = createEvent('OrderCreated', 'o-1', { orderId: 'o-1', customerId: 'c', items: [], totalCents: 0 });

  it('handles an event once and skips redelivered duplicates', async () => {
    const c = new TestConsumer(fakeDb());
    expect(await c.process(event)).toBe('handled');
    expect(await c.process(event)).toBe('duplicate');
    expect(c.calls).toEqual([event.eventId]);
  });

  it('ignores event types it has no handler for', async () => {
    const c = new TestConsumer(fakeDb());
    expect(await c.process(createEvent('PaymentFailed', 'o-1', { orderId: 'o-1', reason: 'x' }) as ShopEvent)).toBe('ignored');
  });

  it('propagates handler errors so Kafka redelivers the message', async () => {
    const c = new TestConsumer(fakeDb());
    c.handlers.OrderCreated = async () => { throw new Error('boom'); };
    await expect(c.process(event)).rejects.toThrow('boom');
  });
});
