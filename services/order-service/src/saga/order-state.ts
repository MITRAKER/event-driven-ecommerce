import { ShopEvent } from '@shop/contracts';

export type OrderStatus = 'PENDING' | 'CONFIRMED' | 'CANCELLED';

export type Transition = { next: 'CONFIRMED' } | { next: 'CANCELLED'; reason: string } | { next: null }; // no change

/**
 * The order's state machine, as a pure function: (current status, incoming event) -> transition.
 * No database, no Kafka: trivial to unit-test and to reason about.
 *
 * Only PENDING orders can move. A late or duplicate event for an order that is already
 * CONFIRMED or CANCELLED is a no-op, which keeps the saga safe against reordering.
 */
export function nextState(status: OrderStatus, event: ShopEvent): Transition {
  if (status !== 'PENDING') return { next: null };
  switch (event.eventType) {
    case 'PaymentSucceeded':
      return { next: 'CONFIRMED' };
    case 'InventoryRejected':
      return { next: 'CANCELLED', reason: `inventory: ${event.payload.reason}` };
    case 'PaymentFailed':
      return { next: 'CANCELLED', reason: `payment: ${event.payload.reason}` };
    default:
      return { next: null };
  }
}
