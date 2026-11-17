import { createEvent, ShopEvent } from '@shop/contracts';
import { emptyView, OrderView, project } from './projection';

let clock = 0;
const ev = (type: string, payload: any = {}) => {
  const e = createEvent(type as any, 'o-1', { orderId: 'o-1', ...payload }) as ShopEvent;
  e.occurredAt = new Date(Date.UTC(2026, 0, 1, 0, 0, clock++)).toISOString();
  return e;
};
const replay = (events: ShopEvent[]): OrderView =>
  events.reduce(project, emptyView('o-1', '1970-01-01T00:00:00.000Z'));

const created = ev('OrderCreated', {
  customerId: 'c-1',
  items: [{ sku: 'LAPTOP', quantity: 1, unitPriceCents: 150000 }],
  totalCents: 150000,
});
const reserved = ev('InventoryReserved', { items: [] });
const failed = ev('PaymentFailed', { reason: 'card declined' });
const cancelled = ev('OrderCancelled', { reason: 'payment: card declined' });

describe('order view projection', () => {
  it('builds the full picture of a failed-payment order', () => {
    expect(replay([created, reserved, failed, cancelled])).toMatchObject({
      customerId: 'c-1',
      totalCents: 150000,
      status: 'CANCELLED',
      inventoryStatus: 'RELEASED',
      paymentStatus: 'FAILED',
      cancelReason: 'payment: card declined',
    });
  });

  it('gives the same result no matter what order events arrive in', () => {
    const inOrder = replay([created, reserved, failed, cancelled]);
    const shuffled = replay([cancelled, failed, reserved, created]);
    expect(shuffled).toEqual(inOrder);
  });

  it('keeps the timeline sorted by when things happened, not when they arrived', () => {
    const view = replay([failed, created, reserved]);
    expect(view.timeline.map((t) => t.eventType)).toEqual([
      'OrderCreated',
      'InventoryReserved',
      'PaymentFailed',
    ]);
  });

  it('a confirmed order', () => {
    const view = replay([
      created,
      reserved,
      ev('PaymentSucceeded', { amountCents: 150000 }),
      ev('OrderConfirmed'),
    ]);
    expect(view).toMatchObject({
      status: 'CONFIRMED',
      inventoryStatus: 'RESERVED',
      paymentStatus: 'SUCCEEDED',
    });
  });
});
