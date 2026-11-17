import { createEvent, InvariantError, Limits, OrderConfirmed, ShopEvent } from '@shop/contracts';
import { compareEntries, emptyView, OrderView, project } from './projection';
import {
  at,
  inventoryRejected,
  inventoryReserved,
  ORDER_ID,
  orderCancelled,
  orderConfirmed,
  orderCreated,
  paymentFailed,
  paymentSucceeded,
} from './testing';

/** Fold events into a fresh view, exactly like the projector does one event at a time. */
function replay(events: readonly ShopEvent[]): OrderView {
  return events.reduce(project, emptyView(ORDER_ID, at(0)));
}

const created = orderCreated(1);
const reserved = inventoryReserved(2);
const failed = paymentFailed(3, 'card declined: amount over limit');
const cancelled = orderCancelled(4, 'payment: card declined: amount over limit');

describe('order view projection', () => {
  it('builds the full picture of a failed-payment order', () => {
    expect(replay([created, reserved, failed, cancelled])).toEqual({
      orderId: ORDER_ID,
      customerId: 'c-1',
      status: 'CANCELLED',
      totalCents: 150000,
      items: [{ sku: 'LAPTOP', quantity: 1, unitPriceCents: 150000 }],
      inventoryStatus: 'RELEASED',
      paymentStatus: 'FAILED',
      cancelReason: 'payment: card declined: amount over limit',
      timeline: [
        { eventType: 'OrderCreated', occurredAt: at(1) },
        { eventType: 'InventoryReserved', occurredAt: at(2) },
        { eventType: 'PaymentFailed', occurredAt: at(3) },
        { eventType: 'OrderCancelled', occurredAt: at(4) },
      ],
      createdAt: at(1),
      updatedAt: at(4),
    });
  });

  it('builds a confirmed order', () => {
    const view = replay([created, reserved, paymentSucceeded(3), orderConfirmed(4)]);
    expect(view).toMatchObject({
      status: 'CONFIRMED',
      inventoryStatus: 'RESERVED',
      paymentStatus: 'SUCCEEDED',
    });
    expect(view.cancelReason).toBeNull();
  });

  it('builds an order that inventory rejected', () => {
    const view = replay([created, inventoryRejected(2, 'out of stock: LAPTOP'), orderCancelled(3, 'x')]);
    expect(view).toMatchObject({ status: 'CANCELLED', inventoryStatus: 'REJECTED', paymentStatus: null });
  });

  it.each([
    ['reversed', [cancelled, failed, reserved, created]],
    ['cancel first', [cancelled, created, reserved, failed]],
    ['reservation last', [created, failed, cancelled, reserved]],
    ['interleaved', [failed, created, cancelled, reserved]],
  ])('gives the same view whatever the arrival order (%s)', (_name, events) => {
    expect(replay(events)).toEqual(replay([created, reserved, failed, cancelled]));
  });

  it('keeps the timeline sorted by when things happened, not when they arrived', () => {
    const view = replay([failed, created, reserved]);
    expect(view.timeline.map((t) => t.eventType)).toEqual([
      'OrderCreated',
      'InventoryReserved',
      'PaymentFailed',
    ]);
  });

  it('breaks timestamp ties by event type, so the timeline never depends on arrival order', () => {
    const sameTime = [paymentSucceeded(5), inventoryReserved(5)];
    const forward = replay(sameTime).timeline;
    const backward = replay([...sameTime].reverse()).timeline;
    expect(forward).toEqual(backward);
    expect(forward.map((t) => t.eventType)).toEqual(['InventoryReserved', 'PaymentSucceeded']);
  });

  it('keeps updatedAt at the latest occurredAt, even when an older event arrives last', () => {
    expect(replay([cancelled, created]).updatedAt).toBe(at(4));
  });

  it('never changes the view it is given', () => {
    const before = replay([created, reserved]);
    const snapshot = structuredClone(before);
    project(before, cancelled);
    expect(before).toEqual(snapshot);
  });

  it('refuses an event that belongs to another order', () => {
    const other: OrderConfirmed = createEvent<OrderConfirmed>('OrderConfirmed', 'another-order', {
      orderId: 'another-order',
    });
    expect(() => project(emptyView(ORDER_ID, at(0)), other)).toThrow(InvariantError);
  });

  it('refuses to grow a timeline past Limits.maxEventsPerOrder', () => {
    const full: OrderView = {
      ...emptyView(ORDER_ID, at(0)),
      timeline: Array.from({ length: Limits.maxEventsPerOrder }, (_unused, i) => ({
        eventType: 'PaymentFailed' as const,
        occurredAt: at(i),
      })),
    };
    expect(() => project(full, created)).toThrow(InvariantError);
  });
});

describe('compareEntries', () => {
  it('orders by time first, then by event type', () => {
    const early = { eventType: 'PaymentFailed' as const, occurredAt: at(1) };
    const late = { eventType: 'InventoryReserved' as const, occurredAt: at(2) };
    const lateTwin = { eventType: 'OrderCreated' as const, occurredAt: at(2) };
    expect(compareEntries(early, late)).toBeLessThan(0);
    expect(compareEntries(late, lateTwin)).toBeLessThan(0);
    expect(compareEntries(lateTwin, late)).toBeGreaterThan(0);
    expect(compareEntries(late, { ...late })).toBe(0);
  });
});
