import { assertNever, Limits, ShopEvent } from '@shop/contracts';

export type OrderStatus = 'PENDING' | 'CONFIRMED' | 'CANCELLED';

export type Transition =
  | { readonly next: 'CONFIRMED' }
  | { readonly next: 'CANCELLED'; readonly reason: string }
  | { readonly next: null }; // no change

const NO_CHANGE: Transition = { next: null };

/** Marks a reason that was cut short. A single UTF-16 code unit. */
const ELLIPSIS = '…';

function isHighSurrogate(code: number): boolean {
  return code >= 0xd800 && code <= 0xdbff;
}

/**
 * Fit a reason into Limits.maxReasonLength, counted in UTF-16 code units exactly like
 * String.length and parseEvent(). Without this, "inventory: " plus a 500-character reason from
 * upstream would make an OrderCancelled that every consumer rejects as invalid, and the
 * reserved stock would never be released.
 */
export function fitReason(reason: string): string {
  if (reason.length <= Limits.maxReasonLength) return reason;
  const cut = Limits.maxReasonLength - ELLIPSIS.length;
  // Never keep only the first half of a surrogate pair (an emoji, say): half a pair is not
  // valid Unicode, and Postgres refuses to store it in a JSONB column such as the outbox payload.
  const end = isHighSurrogate(reason.charCodeAt(cut - 1)) ? cut - 1 : cut;
  return reason.slice(0, end) + ELLIPSIS;
}

function cancel(source: 'inventory' | 'payment', reason: string): Transition {
  return { next: 'CANCELLED', reason: fitReason(`${source}: ${reason}`) };
}

/** What an event means for an order that is still PENDING. */
function fromPending(event: ShopEvent): Transition {
  switch (event.eventType) {
    case 'PaymentSucceeded':
      return { next: 'CONFIRMED' };
    case 'InventoryRejected':
      return cancel('inventory', event.payload.reason);
    case 'PaymentFailed':
      return cancel('payment', event.payload.reason);
    case 'InventoryReserved': // stock is held; the order waits for the payment result
    case 'OrderCreated':
    case 'OrderConfirmed':
    case 'OrderCancelled':
      return NO_CHANGE;
    default:
      return assertNever(event, 'nextState: event');
  }
}

/**
 * The order's state machine, as a pure function: (current status, incoming event) -> transition.
 * No database, no Kafka: trivial to unit-test and to reason about.
 *
 * Only PENDING orders can move. A late or duplicate event for an order that is already
 * CONFIRMED or CANCELLED is a no-op, which keeps the saga safe against reordering.
 *
 * Both switches name every case, so adding a status or an event type is a compile error here
 * until someone decides what it means. assertNever also catches impossible values at runtime:
 * the status is read from a TEXT column.
 */
export function nextState(status: OrderStatus, event: ShopEvent): Transition {
  switch (status) {
    case 'PENDING':
      return fromPending(event);
    case 'CONFIRMED':
    case 'CANCELLED':
      return NO_CHANGE;
    default:
      return assertNever(status, 'nextState: order status');
  }
}
