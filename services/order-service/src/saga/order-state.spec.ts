import { InvariantError, Limits, ShopEvent } from '@shop/contracts';
import {
  inventoryRejected,
  inventoryReserved,
  oneOfEach,
  orderCancelled,
  orderConfirmed,
  orderCreated,
  paymentFailed,
  paymentSucceeded,
} from '../testing';
import { fitReason, nextState, OrderStatus, Transition } from './order-state';

/** The reason of a CANCELLED transition. Fails the test for any other transition. */
function reasonOf(t: Transition): string {
  if (t.next !== 'CANCELLED') throw new Error(`expected CANCELLED, got ${String(t.next)}`);
  return t.reason;
}

/** Whatever the TEXT column holds, typed the way the consumer reads it. */
function statusFromDatabase(text: string): OrderStatus {
  return text as OrderStatus;
}

/** True if `text` is valid Unicode: encodeURIComponent throws on half a surrogate pair. */
function isWellFormed(text: string): boolean {
  try {
    encodeURIComponent(text);
    return true;
  } catch {
    return false;
  }
}

describe('order state machine', () => {
  it('confirms a pending order when payment succeeds', () => {
    expect(nextState('PENDING', paymentSucceeded())).toEqual({ next: 'CONFIRMED' });
  });

  it('cancels when inventory is rejected or payment fails, keeping the reason', () => {
    expect(nextState('PENDING', inventoryRejected('out of stock'))).toEqual({
      next: 'CANCELLED',
      reason: 'inventory: out of stock',
    });
    expect(nextState('PENDING', paymentFailed('card declined'))).toEqual({
      next: 'CANCELLED',
      reason: 'payment: card declined',
    });
  });

  it.each(
    [orderCreated(), orderConfirmed(), orderCancelled('x'), inventoryReserved()].map(
      (event) => [event.eventType, event] as const,
    ),
  )('keeps a pending order waiting on %s, which is not an outcome', (_type, event) => {
    expect(nextState('PENDING', event)).toEqual({ next: null });
  });

  it.each(
    (['CONFIRMED', 'CANCELLED'] as const).flatMap((status) =>
      oneOfEach().map((event) => [status, event.eventType, event] as const),
    ),
  )('ignores everything once the order is %s: %s (late or reordered messages)', (status, _type, event) => {
    expect(nextState(status, event)).toEqual({ next: null });
  });

  it('keeps the cancel reason within the contract limit, so OrderCancelled stays valid', () => {
    const t = nextState('PENDING', paymentFailed('x'.repeat(Limits.maxReasonLength)));
    const reason = reasonOf(t);
    expect(reason).toHaveLength(Limits.maxReasonLength);
    expect(reason.startsWith('payment: xxx')).toBe(true);
    expect(reason.endsWith('…')).toBe(true);
  });

  it('throws on a status it does not know (the column is plain TEXT)', () => {
    expect(() => nextState(statusFromDatabase('SHIPPED'), paymentSucceeded())).toThrow(InvariantError);
  });

  it('throws on an event type it does not know', () => {
    const unknown = { ...paymentSucceeded(), eventType: 'OrderTeleported' } as unknown as ShopEvent;
    expect(() => nextState('PENDING', unknown)).toThrow(InvariantError);
  });
});

describe('fitReason', () => {
  const max = Limits.maxReasonLength;

  it('leaves a reason that fits untouched', () => {
    const exact = 'x'.repeat(max);
    expect(fitReason(exact)).toBe(exact);
  });

  it('cuts a longer reason to the limit and marks the cut with an ellipsis', () => {
    expect(fitReason('x'.repeat(max + 1))).toBe(`${'x'.repeat(max - 1)}…`);
  });

  it('never cuts an emoji (a surrogate pair) in half', () => {
    // The emoji straddles the cut: a plain slice would keep its first half and drop the second.
    const fitted = fitReason(`${'x'.repeat(max - 2)}😀 and more`);
    expect(fitted).toBe(`${'x'.repeat(max - 2)}…`);
    expect(isWellFormed(fitted)).toBe(true);
  });

  it('keeps an emoji that fits completely before the cut', () => {
    const fitted = fitReason(`${'x'.repeat(max - 3)}😀 and more`);
    expect(fitted).toBe(`${'x'.repeat(max - 3)}😀…`);
    expect(fitted).toHaveLength(max);
    expect(isWellFormed(fitted)).toBe(true);
  });
});
