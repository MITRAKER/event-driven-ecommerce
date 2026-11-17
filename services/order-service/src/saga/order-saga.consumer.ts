import { Injectable } from '@nestjs/common';
import {
  createEvent,
  EventType,
  invariant,
  InvariantError,
  InventoryRejected,
  OrderCancelled,
  OrderConfirmed,
  PaymentFailed,
  PaymentSucceeded,
  ShopEvent,
  Topic,
  Topics,
} from '@shop/contracts';
import { addToOutbox, EventConsumer } from '@shop/messaging';
import { PoolClient } from 'pg';
import { nextState, OrderStatus, Transition } from './order-state';

/** The events that can settle an order: a rejection from inventory, or either payment result. */
type OutcomeEvent = InventoryRejected | PaymentSucceeded | PaymentFailed;

/** A transition that actually changes the order. */
type Settled = Exclude<Transition, { next: null }>;

/** The event that announces the outcome. Passing `cause` keeps the trace chain (correlationId) intact. */
function outcomeEvent(t: Settled, cause: OutcomeEvent): OrderConfirmed | OrderCancelled {
  const orderId = cause.aggregateId;
  return t.next === 'CONFIRMED'
    ? createEvent<OrderConfirmed>('OrderConfirmed', orderId, { orderId }, cause)
    : createEvent<OrderCancelled>('OrderCancelled', orderId, { orderId, reason: t.reason }, cause);
}

/**
 * The order service's side of the choreographed saga. It listens to what inventory and
 * payment report, moves the order to its final state, and announces the outcome.
 * Publishing OrderCancelled is what triggers compensation in the inventory service.
 */
@Injectable()
export class OrderSagaConsumer extends EventConsumer {
  protected readonly groupId = 'order-service';
  protected readonly topics: readonly Topic[] = [Topics.Inventory, Topics.Payments];
  protected readonly handledTypes: ReadonlySet<EventType> = new Set<EventType>([
    'InventoryRejected',
    'PaymentSucceeded',
    'PaymentFailed',
  ]);

  protected async handle(event: ShopEvent, tx: PoolClient): Promise<void> {
    switch (event.eventType) {
      case 'InventoryRejected':
      case 'PaymentSucceeded':
      case 'PaymentFailed':
        await this.settle(event, tx);
        return;
      default:
        // process() only passes in handledTypes, so this means that set and this switch disagree.
        throw new InvariantError(`${this.groupId} has no handler for ${event.eventType}`);
    }
  }

  /**
   * Move a PENDING order to its final state and stage the event that announces it, inside the
   * consumer's transaction: the eventId claim, the status change and the outbox row commit together.
   */
  private async settle(event: OutcomeEvent, tx: PoolClient): Promise<void> {
    const orderId = event.aggregateId;
    // FOR UPDATE: lock the row so two events for the same order can't race.
    const { rows } = await tx.query<{ status: OrderStatus }>(
      'SELECT status FROM orders WHERE id = $1 FOR UPDATE',
      [orderId],
    );
    const order = rows[0];
    if (order === undefined) {
      this.logger.warn(`${event.eventType} ${event.eventId} is for unknown order ${orderId}, ignored`);
      return;
    }

    const t = nextState(order.status, event);
    if (t.next === null) return; // already final: a late or reordered event changes nothing

    // "AND status = 'PENDING'" restates the state machine's rule in SQL. The row is locked and
    // was PENDING a moment ago, so anything other than exactly one changed row is a bug.
    const updated = await tx.query(
      `UPDATE orders SET status = $2, updated_at = now() WHERE id = $1 AND status = 'PENDING'`,
      [orderId, t.next],
    );
    invariant(updated.rowCount === 1, `order ${orderId}: status update changed ${updated.rowCount} rows`);
    await addToOutbox(tx, Topics.Orders, outcomeEvent(t, event));
  }
}
