import { Injectable } from '@nestjs/common';
import { PoolClient } from 'pg';
import { createEvent, OrderCancelled, OrderConfirmed, ShopEvent, Topics } from '@shop/contracts';
import { addToOutbox, Database, EventConsumer, EventHandler } from '@shop/messaging';
import { nextState, OrderStatus } from './order-state';

/**
 * The order service's side of the choreographed saga. It listens to what inventory and
 * payment report, moves the order to its final state, and announces the outcome.
 * Publishing OrderCancelled is what triggers compensation in the inventory service.
 */
@Injectable()
export class OrderSagaConsumer extends EventConsumer {
  protected readonly groupId = 'order-service';
  protected readonly topics = [Topics.Inventory, Topics.Payments];

  private readonly settle: EventHandler = async (event, tx) => {
    const orderId = event.aggregateId;
    // FOR UPDATE: lock the row so two events for the same order can't race.
    const { rows } = await tx.query(`SELECT status FROM orders WHERE id = $1 FOR UPDATE`, [orderId]);
    if (!rows[0]) return this.logger.warn(`unknown order ${orderId}`);

    const t = nextState(rows[0].status as OrderStatus, event);
    if (t.next === null) return;

    await tx.query(`UPDATE orders SET status = $2, updated_at = now() WHERE id = $1`, [orderId, t.next]);
    await this.announce(tx, event, t.next === 'CONFIRMED' ? undefined : t.reason);
  };

  protected readonly handlers = {
    InventoryRejected: this.settle,
    PaymentSucceeded: this.settle,
    PaymentFailed: this.settle,
  };

  constructor(db: Database) {
    super(db);
  }

  private async announce(tx: PoolClient, cause: ShopEvent, cancelReason?: string) {
    const orderId = cause.aggregateId;
    const outcome =
      cancelReason === undefined
        ? createEvent<OrderConfirmed>('OrderConfirmed', orderId, { orderId }, cause)
        : createEvent<OrderCancelled>('OrderCancelled', orderId, { orderId, reason: cancelReason }, cause);
    await addToOutbox(tx, Topics.Orders, outcome);
  }
}
