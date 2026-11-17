import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { createEvent, invariant, Limits, OrderCreated, OrderItem, Topics } from '@shop/contracts';
import { addToOutbox, Database } from '@shop/messaging';
import { OrderStatus } from '../saga/order-state';
import { CreateOrderDto } from './create-order.dto';

/** The answer to POST /orders: the order exists, and the saga decides its outcome later. */
export interface OrderAccepted {
  readonly orderId: string;
  readonly status: 'PENDING';
  readonly totalCents: number;
}

/** The answer to GET /orders/:id. */
export interface OrderView {
  readonly orderId: string;
  readonly customerId: string;
  readonly status: OrderStatus;
  readonly totalCents: number;
  readonly items: readonly OrderItem[];
  readonly createdAt: string;
  readonly updatedAt: string;
}

/** One row of the orders table, as pg returns it: JSONB already parsed, TIMESTAMPTZ as a Date. */
interface OrderRow {
  readonly id: string;
  readonly customer_id: string;
  readonly status: OrderStatus;
  readonly total_cents: number;
  readonly items: OrderItem[];
  readonly created_at: Date;
  readonly updated_at: Date;
}

/**
 * The order total in cents. The largest order the DTO allows (50 items x 1,000 x $100,000 =
 * 5 x 10^11 cents) is far below 2^53, so this sum is always exact. It can still be far above
 * what the INTEGER column holds, which is why create() checks it against the limit.
 */
export function orderTotalCents(items: readonly OrderItem[]): number {
  return items.reduce((sum, item) => sum + item.quantity * item.unitPriceCents, 0);
}

function toView(row: OrderRow): OrderView {
  return {
    orderId: row.id,
    customerId: row.customer_id,
    status: row.status,
    totalCents: row.total_cents,
    items: row.items,
    createdAt: row.created_at.toISOString(),
    updatedAt: row.updated_at.toISOString(),
  };
}

@Injectable()
export class OrdersService {
  constructor(private readonly db: Database) {}

  /**
   * Command side: create the order AND stage the OrderCreated event in ONE transaction.
   * We never call Kafka here. If we did, a crash between "INSERT order" and "send to Kafka"
   * would leave an order nobody else ever hears about (the dual-write problem).
   */
  async create(dto: CreateOrderDto): Promise<OrderAccepted> {
    // CreateOrderDto already enforced this; restating it bounds every loop over items (JPL rule 2).
    invariant(
      dto.items.length >= 1 && dto.items.length <= Limits.maxItemsPerOrder,
      `an order needs 1..${Limits.maxItemsPerOrder} items, got ${dto.items.length}`,
    );
    // Copy only the fields the contract knows, so nothing extra reaches the table or the event.
    const items: OrderItem[] = dto.items.map(({ sku, quantity, unitPriceCents }) => ({
      sku,
      quantity,
      unitPriceCents,
    }));
    const totalCents = orderTotalCents(items);
    // Checked before any write: the INTEGER column (at most 2^31 - 1) must never overflow.
    if (totalCents > Limits.maxOrderTotalCents) {
      throw new BadRequestException(
        `order total ${totalCents} cents is above the limit of ${Limits.maxOrderTotalCents}`,
      );
    }

    const orderId = randomUUID();
    await this.db.transaction(async (tx) => {
      const inserted = await tx.query(
        `INSERT INTO orders (id, customer_id, status, total_cents, items)
         VALUES ($1, $2, 'PENDING', $3, $4)`,
        [orderId, dto.customerId, totalCents, JSON.stringify(items)],
      );
      invariant(inserted.rowCount === 1, `order insert for ${orderId} wrote ${inserted.rowCount} rows`);
      const event = createEvent<OrderCreated>('OrderCreated', orderId, {
        orderId,
        customerId: dto.customerId,
        items,
        totalCents,
      });
      await addToOutbox(tx, Topics.Orders, event);
    });

    return { orderId, status: 'PENDING', totalCents };
  }

  /** The command side's own view of one order. The query service has the full event timeline. */
  async get(orderId: string): Promise<OrderView> {
    const { rows } = await this.db.pool.query<OrderRow>(
      `SELECT id, customer_id, status, total_cents, items, created_at, updated_at
         FROM orders WHERE id = $1`,
      [orderId],
    );
    const row = rows[0];
    if (row === undefined) throw new NotFoundException(`order ${orderId} not found`);
    return toView(row);
  }
}
