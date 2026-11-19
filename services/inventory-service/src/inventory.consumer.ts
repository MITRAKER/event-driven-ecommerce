import { Injectable } from '@nestjs/common';
import {
  createEvent,
  EventType,
  InventoryRejected,
  InventoryReserved,
  invariant,
  InvariantError,
  OrderCancelled,
  OrderCreated,
  OrderItem,
  ShopEvent,
  Topic,
  Topics,
} from '@shop/contracts';
import { addToOutbox, EventConsumer } from '@shop/messaging';
import { PoolClient } from 'pg';
import { decideReservation, mergeLines, StockLevel, StockLine } from './reservation';

/** A products row as pg returns it (INTEGER columns arrive as numbers). */
interface ProductRow {
  sku: string;
  available: number;
  price_cents: number;
}

/** reservations.items is JSONB, which pg has already parsed: the order lines we reserved. */
interface ReservationRow {
  items: OrderItem[];
}

/**
 * The inventory side of the order saga:
 * - OrderCreated: reserve every line at the catalog price, or reject the whole order.
 * - OrderCancelled: the compensation, give a reserved order's stock back.
 */
@Injectable()
export class InventoryConsumer extends EventConsumer {
  protected readonly groupId = 'inventory-service';
  protected readonly topics: readonly Topic[] = [Topics.Orders];
  protected readonly handledTypes: ReadonlySet<EventType> = new Set<EventType>([
    'OrderCreated',
    'OrderCancelled',
  ]);

  protected async handle(event: ShopEvent, tx: PoolClient): Promise<void> {
    switch (event.eventType) {
      case 'OrderCreated':
        await this.reserve(event, tx);
        return;
      case 'OrderCancelled':
        await this.release(event, tx);
        return;
      default:
        // Unreachable: process() only calls handle() for the types in handledTypes.
        throw new InvariantError(`${this.groupId} cannot handle ${event.eventType}`);
    }
  }

  /** Saga step: reserve the whole order at catalog prices, or reject it. Never half an order. */
  private async reserve(event: OrderCreated, tx: PoolClient): Promise<void> {
    const { orderId, items } = event.payload;
    const stock = await this.lockProducts(tx, items);
    const decision = decideReservation(stock, items);
    if (!decision.ok) {
      const payload = { orderId, reason: decision.reason };
      await addToOutbox(
        tx,
        Topics.Inventory,
        createEvent<InventoryRejected>('InventoryRejected', orderId, payload, event),
      );
      this.logger.log(`rejected order ${orderId}: ${decision.reason}`);
      return;
    }
    await this.adjustStock(tx, decision.lines, -1);
    const saved = await tx.query(
      `INSERT INTO reservations (order_id, items, status) VALUES ($1, $2, 'RESERVED')`,
      [orderId, JSON.stringify(items)],
    );
    invariant(saved.rowCount === 1, `reservation insert for order ${orderId} wrote ${saved.rowCount} rows`);
    await addToOutbox(
      tx,
      Topics.Inventory,
      createEvent<InventoryReserved>('InventoryReserved', orderId, { orderId, items }, event),
    );
  }

  /**
   * Compensation: the order was cancelled (e.g. payment failed), so give the stock back.
   * Only RESERVED reservations are released, so a cancel for a rejected order is a no-op
   * and releasing twice is impossible.
   */
  private async release(event: OrderCancelled, tx: PoolClient): Promise<void> {
    const { orderId } = event.payload;
    const { rows } = await tx.query<ReservationRow>(
      `UPDATE reservations SET status = 'RELEASED', updated_at = now()
        WHERE order_id = $1 AND status = 'RESERVED' RETURNING items`,
      [orderId],
    );
    const reservation = rows[0];
    if (reservation === undefined) return; // the order was rejected, or this is a second cancel
    const lines = mergeLines(reservation.items);
    // Lock first, with the same query as reserve(), so both lock rows in the same order. Updating
    // straight away in reservation order could deadlock against a reserve holding the same rows.
    await this.lockProducts(tx, lines);
    await this.adjustStock(tx, lines, 1);
    this.logger.log(`released stock for cancelled order ${orderId}`);
  }

  /**
   * Lock the products rows of these lines' skus and read their stock and price.
   * The database takes the row locks in ORDER BY sku order, and every transaction that changes
   * stock locks through this one query first. So two transactions never each hold a row the
   * other is waiting for: no deadlocks. Unknown skus simply have no row in the result.
   */
  private async lockProducts(
    tx: PoolClient,
    lines: readonly Pick<StockLine, 'sku'>[],
  ): Promise<Map<string, StockLevel>> {
    const { rows } = await tx.query<ProductRow>(
      `SELECT sku, available, price_cents FROM products WHERE sku = ANY($1) ORDER BY sku FOR UPDATE`,
      [lines.map((line) => line.sku)],
    );
    return new Map(rows.map((row) => [row.sku, { available: row.available, priceCents: row.price_cents }]));
  }

  /**
   * Change each line's stock by sign * quantity, one row at a time, in the lines' (sku) order:
   * -1 takes stock for a reservation, +1 gives it back. The CHECK (available >= 0) constraint
   * is the last line of defence against a bug that would oversell.
   */
  private async adjustStock(tx: PoolClient, lines: readonly StockLine[], sign: -1 | 1): Promise<void> {
    for (const line of lines) {
      const updated = await tx.query(`UPDATE products SET available = available + $2 WHERE sku = $1`, [
        line.sku,
        sign * line.quantity,
      ]);
      invariant(updated.rowCount === 1, `stock update for ${line.sku} changed ${updated.rowCount} rows`);
    }
  }
}
