import { createEvent, ShopEvent } from '@shop/contracts';
import { nextState } from './order-state';

const ev = (type: ShopEvent['eventType'], payload: any = {}) => createEvent(type as any, 'o-1', payload) as ShopEvent;

describe('order state machine', () => {
  it('confirms a pending order when payment succeeds', () => {
    expect(nextState('PENDING', ev('PaymentSucceeded'))).toEqual({ next: 'CONFIRMED' });
  });
  it('cancels when inventory is rejected or payment fails, keeping the reason', () => {
    expect(nextState('PENDING', ev('InventoryRejected', { reason: 'out of stock' })))
      .toEqual({ next: 'CANCELLED', reason: 'inventory: out of stock' });
    expect(nextState('PENDING', ev('PaymentFailed', { reason: 'card declined' })))
      .toEqual({ next: 'CANCELLED', reason: 'payment: card declined' });
  });
  it('ignores events once the order is final (late or reordered messages)', () => {
    expect(nextState('CANCELLED', ev('PaymentSucceeded'))).toEqual({ next: null });
    expect(nextState('CONFIRMED', ev('PaymentFailed', { reason: 'x' }))).toEqual({ next: null });
  });
  it('ignores events it does not care about', () => {
    expect(nextState('PENDING', ev('InventoryReserved'))).toEqual({ next: null });
  });
});
