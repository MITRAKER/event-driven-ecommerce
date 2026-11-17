import { Injectable } from '@nestjs/common';
import { createEvent, InventoryReserved, PaymentFailed, PaymentSucceeded, Topics } from '@shop/contracts';
import { addToOutbox, Database, EventConsumer, EventHandler } from '@shop/messaging';
import { charge } from './payment-gateway';

@Injectable()
export class PaymentConsumer extends EventConsumer {
  protected readonly groupId = 'payment-service';
  protected readonly topics = [Topics.Inventory];

  /** Saga step: stock is held, so now take the money. */
  private readonly pay: EventHandler = async (e, tx) => {
    const event = e as InventoryReserved;
    const { orderId, items } = event.payload;
    // The amount travels inside the event, so payment never has to call the order service.
    const amountCents = items.reduce((sum, i) => sum + i.quantity * i.unitPriceCents, 0);

    const result = charge(amountCents);
    await tx.query(
      `INSERT INTO payments (order_id, amount_cents, status, reason) VALUES ($1, $2, $3, $4)`,
      [orderId, amountCents, result.ok ? 'SUCCEEDED' : 'FAILED', result.ok ? null : result.reason],
    );
    const outcome = result.ok
      ? createEvent<PaymentSucceeded>('PaymentSucceeded', orderId, { orderId, amountCents }, event)
      : createEvent<PaymentFailed>('PaymentFailed', orderId, { orderId, reason: result.reason }, event);
    await addToOutbox(tx, Topics.Payments, outcome);
  };

  protected readonly handlers = { InventoryReserved: this.pay };

  constructor(db: Database) {
    super(db);
  }
}
