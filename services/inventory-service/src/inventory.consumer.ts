import { Injectable } from '@nestjs/common';
import {
  createEvent,
  InventoryRejected,
  InventoryReserved,
  OrderCancelled,
  OrderCreated,
  Topics,
} from '@shop/contracts';
import { addToOutbox, Database, EventConsumer, EventHandler } from '@shop/messaging';
import { decideReservation } from './reservation';

@Injectable()
export class InventoryConsumer extends EventConsumer {
  protected readonly groupId = 'inventory-service';
  protected readonly topics = [Topics.Orders];

  /** Saga step: try to reserve stock for a new order. */
  private readonly reserve: EventHandler = async (e, tx) => {
    const event = e as OrderCreated;
    const { orderId, items } = event.payload;
    const skus = [...new Set(items.map((i) => i.sku))];

    // Lock the product rows (in a fixed order, to avoid deadlocks between two orders).
    const { rows } = await tx.query(
      `SELECT sku, available FROM products WHERE sku = ANY($1) ORDER BY sku FOR UPDATE`,
      [skus],
    );
    const decision = decideReservation(new Map(rows.map((r) => [r.sku, r.available])), items);

    if (!decision.ok) {
      await addToOutbox(
        tx,
        Topics.Inventory,
        createEvent<InventoryRejected>(
          'InventoryRejected',
          orderId,
          { orderId, reason: decision.reason },
          event,
        ),
      );
      return;
    }
    for (const i of items) {
      await tx.query(`UPDATE products SET available = available - $2 WHERE sku = $1`, [i.sku, i.quantity]);
    }
    await tx.query(`INSERT INTO reservations (order_id, items, status) VALUES ($1, $2, 'RESERVED')`, [
      orderId,
      JSON.stringify(items),
    ]);
    await addToOutbox(
      tx,
      Topics.Inventory,
      createEvent<InventoryReserved>('InventoryReserved', orderId, { orderId, items }, event),
    );
  };

  /**
   * Compensation: the order was cancelled (e.g. payment failed), so give the stock back.
   * Only RESERVED reservations are released, so a cancel for a rejected order is a no-op
   * and releasing twice is impossible.
   */
  private readonly release: EventHandler = async (e, tx) => {
    const { orderId } = (e as OrderCancelled).payload;
    const { rows } = await tx.query(
      `UPDATE reservations SET status = 'RELEASED', updated_at = now()
        WHERE order_id = $1 AND status = 'RESERVED' RETURNING items`,
      [orderId],
    );
    if (!rows[0]) return;
    for (const i of rows[0].items as OrderCreated['payload']['items']) {
      await tx.query(`UPDATE products SET available = available + $2 WHERE sku = $1`, [i.sku, i.quantity]);
    }
    this.logger.log(`released stock for cancelled order ${orderId}`);
  };

  protected readonly handlers = { OrderCreated: this.reserve, OrderCancelled: this.release };

  constructor(db: Database) {
    super(db);
  }
}
