import { Injectable } from '@nestjs/common';
import {
  createEvent,
  EventType,
  invariant,
  InvariantError,
  InventoryReserved,
  Limits,
  OrderItem,
  PaymentFailed,
  PaymentSucceeded,
  ShopEvent,
  Topic,
  Topics,
} from '@shop/contracts';
import { addToOutbox, Database, EventConsumer, KafkaClient, MessagingConfig } from '@shop/messaging';
import { PoolClient } from 'pg';
import { PaymentConfig } from './payment-config';
import { charge, ChargeResult } from './payment-gateway';

/** payments.status: what the (fake) card processor answered. */
type PaymentStatus = 'SUCCEEDED' | 'FAILED';

/** What the order costs: the sum of its lines, in whole cents. */
function orderTotalCents(items: readonly OrderItem[]): number {
  // Exact in a JS number: at most 50 lines x 1,000 units x 10,000,000 cents = 5 x 10^11, far below 2^53.
  return items.reduce((sum, item) => sum + item.quantity * item.unitPriceCents, 0);
}

/** The event that reports the charge. Passing `cause` keeps the trace chain (correlationId) intact. */
function outcomeEvent(
  cause: InventoryReserved,
  amountCents: number,
  result: ChargeResult,
): PaymentSucceeded | PaymentFailed {
  const { orderId } = cause.payload;
  return result.ok
    ? createEvent<PaymentSucceeded>('PaymentSucceeded', orderId, { orderId, amountCents }, cause)
    : createEvent<PaymentFailed>('PaymentFailed', orderId, { orderId, reason: result.reason }, cause);
}

/**
 * The payment step of the order saga: once inventory has reserved the stock, charge the order and
 * report the result. PaymentFailed makes the order service cancel the order, and that cancellation
 * makes inventory give the stock back (the compensation).
 */
@Injectable()
export class PaymentConsumer extends EventConsumer {
  protected readonly groupId = 'payment-service';
  protected readonly topics: readonly Topic[] = [Topics.Inventory];
  // InventoryRejected arrives on the same topic, but there is nothing to charge for a rejected order.
  protected readonly handledTypes: ReadonlySet<EventType> = new Set<EventType>(['InventoryReserved']);

  // Nest reads this constructor's parameter types to know what to inject: the three the base
  // class needs, then this service's own settings.
  constructor(
    db: Database,
    kafka: KafkaClient,
    config: MessagingConfig,
    private readonly paymentConfig: PaymentConfig,
  ) {
    super(db, kafka, config);
  }

  protected async handle(event: ShopEvent, tx: PoolClient): Promise<void> {
    switch (event.eventType) {
      case 'InventoryReserved':
        await this.pay(event, tx);
        return;
      default:
        // Unreachable: process() only calls handle() for the types in handledTypes.
        throw new InvariantError(`${this.groupId} cannot handle ${event.eventType}`);
    }
  }

  /**
   * Saga step: the stock is held, so now take the money. The payment row and the outcome event are
   * written in the consumer's transaction, together with the eventId claim: all of it or none of it.
   */
  private async pay(event: InventoryReserved, tx: PoolClient): Promise<void> {
    const { orderId, items } = event.payload;
    // Event-carried state: the prices travel inside the event, so payment never calls another
    // service. They can be trusted, because inventory checked each one against its catalog
    // before it reserved the stock.
    const amountCents = orderTotalCents(items);
    // The order service refuses bigger orders, so only a bug upstream can trip this. It also keeps
    // the amount inside the INTEGER column below and inside what parseEvent accepts downstream.
    invariant(
      amountCents <= Limits.maxOrderTotalCents,
      `order ${orderId}: total ${amountCents} cents is above the limit of ${Limits.maxOrderTotalCents}`,
    );
    // A real processor call can't be rolled back with our transaction: it would get the order id
    // as an idempotency key, so a retried event never charges the card twice.
    const result = charge(amountCents, this.paymentConfig.settings);
    const status: PaymentStatus = result.ok ? 'SUCCEEDED' : 'FAILED';
    // order_id is the primary key: one charge per order, enforced by the database. A second
    // payment for the same order fails here, and the whole transaction rolls back.
    const saved = await tx.query(
      `INSERT INTO payments (order_id, amount_cents, status, reason) VALUES ($1, $2, $3, $4)`,
      [orderId, amountCents, status, result.ok ? null : result.reason],
    );
    invariant(saved.rowCount === 1, `payment insert for order ${orderId} wrote ${saved.rowCount} rows`);
    await addToOutbox(tx, Topics.Payments, outcomeEvent(event, amountCents, result));
    this.logger.log(`order ${orderId}: payment of ${amountCents} cents ${status}`);
  }
}
