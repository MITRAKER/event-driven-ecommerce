import { Injectable } from '@nestjs/common';
import { invariant, OrderItem } from '@shop/contracts';
import { Database } from '@shop/messaging';
import { PoolClient } from 'pg';
import { InventoryStatus, OrderStatus, OrderView, PaymentStatus, TimelineEntry } from './projection';

/** An order_view row as pg returns it: JSONB columns already parsed, TIMESTAMPTZ as a Date. */
interface OrderViewRow {
  readonly order_id: string;
  readonly customer_id: string | null;
  readonly status: OrderStatus;
  readonly total_cents: number | null;
  readonly items: OrderItem[] | null;
  readonly inventory_status: InventoryStatus | null;
  readonly payment_status: PaymentStatus | null;
  readonly cancel_reason: string | null;
  readonly timeline: TimelineEntry[];
  readonly created_at: Date | null;
  readonly updated_at: Date;
}

/** count(*) and sum() are bigint in Postgres, and pg returns bigint as a string (no precision loss). */
interface StatsRow {
  readonly status: OrderStatus;
  readonly orders: string;
  readonly total_cents: string;
}

/** One line of GET /stats. */
export interface StatusStats {
  readonly status: OrderStatus;
  readonly orders: number;
  readonly totalCents: number;
}

/** Bounded response (JPL rule 3): a "my orders" page shows the latest 20. */
export const MAX_ORDERS_PER_CUSTOMER = 20;

const COLUMNS = `order_id, customer_id, status, total_cents, items, inventory_status, payment_status,
  cancel_reason, timeline, created_at, updated_at`;

function toView(row: OrderViewRow): OrderView {
  return {
    orderId: row.order_id,
    customerId: row.customer_id,
    status: row.status,
    totalCents: row.total_cents,
    items: row.items,
    inventoryStatus: row.inventory_status,
    paymentStatus: row.payment_status,
    cancelReason: row.cancel_reason,
    timeline: row.timeline,
    createdAt: row.created_at === null ? null : row.created_at.toISOString(),
    updatedAt: row.updated_at.toISOString(),
  };
}

@Injectable()
export class OrderViewRepository {
  constructor(private readonly db: Database) {}

  /**
   * Load and lock one order's view inside the projector's transaction.
   *
   * A row that doesn't exist yet can't be locked. If two replicas projected the first two events of
   * a new order at the same time, both would find nothing, both would insert, and the second write
   * would silently replace the first one's change: a lost update. So the row is created first with
   * a placeholder (ON CONFLICT DO NOTHING: the second inserter waits for the first to commit, then
   * does nothing), and SELECT ... FOR UPDATE always has a row to lock.
   */
  async loadForUpdate(tx: PoolClient, orderId: string, at: string): Promise<OrderView> {
    await tx.query(
      `INSERT INTO order_view (order_id, status, updated_at) VALUES ($1, 'PENDING', $2)
       ON CONFLICT (order_id) DO NOTHING`,
      [orderId, at],
    );
    const { rows } = await tx.query<OrderViewRow>(
      `SELECT ${COLUMNS} FROM order_view WHERE order_id = $1 FOR UPDATE`,
      [orderId],
    );
    const row = rows[0];
    invariant(row !== undefined, `order_view row for ${orderId} is missing right after it was created`);
    return toView(row);
  }

  /** Write the projected view back. loadForUpdate() locked the row, so exactly one row changes. */
  async save(tx: PoolClient, view: OrderView): Promise<void> {
    const result = await tx.query(
      `UPDATE order_view SET customer_id = $2, status = $3, total_cents = $4, items = $5,
              inventory_status = $6, payment_status = $7, cancel_reason = $8, timeline = $9,
              created_at = $10, updated_at = $11
        WHERE order_id = $1`,
      [
        view.orderId,
        view.customerId,
        view.status,
        view.totalCents,
        view.items === null ? null : JSON.stringify(view.items),
        view.inventoryStatus,
        view.paymentStatus,
        view.cancelReason,
        JSON.stringify(view.timeline),
        view.createdAt,
        view.updatedAt,
      ],
    );
    invariant(result.rowCount === 1, `order_view update for ${view.orderId} changed ${result.rowCount} rows`);
  }

  // ---- queries: simple, index-backed reads, no joins

  async byId(orderId: string): Promise<OrderView | null> {
    const { rows } = await this.db.pool.query<OrderViewRow>(
      `SELECT ${COLUMNS} FROM order_view WHERE order_id = $1`,
      [orderId],
    );
    const row = rows[0];
    return row === undefined ? null : toView(row);
  }

  async byCustomer(customerId: string): Promise<OrderView[]> {
    const { rows } = await this.db.pool.query<OrderViewRow>(
      `SELECT ${COLUMNS} FROM order_view WHERE customer_id = $1
        ORDER BY created_at DESC NULLS LAST LIMIT $2`,
      [customerId, MAX_ORDERS_PER_CUSTOMER],
    );
    return rows.map(toView);
  }

  /**
   * Orders and revenue per status (at most one row per status). The sum stays a bigint: casting it
   * to int would overflow past 2^31 - 1 cents (about $21 million). Number() is exact up to 2^53.
   */
  async stats(): Promise<StatusStats[]> {
    const { rows } = await this.db.pool.query<StatsRow>(
      `SELECT status, count(*) AS orders, coalesce(sum(total_cents), 0)::bigint AS total_cents
         FROM order_view GROUP BY status ORDER BY status`,
    );
    return rows.map((row) => ({
      status: row.status,
      orders: Number(row.orders),
      totalCents: Number(row.total_cents),
    }));
  }
}
