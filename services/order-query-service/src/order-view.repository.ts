import { Injectable } from '@nestjs/common';
import { PoolClient } from 'pg';
import { Database } from '@shop/messaging';
import { emptyView, OrderView } from './projection';

const toView = (r: any): OrderView => ({
  orderId: r.order_id, customerId: r.customer_id, status: r.status, totalCents: r.total_cents,
  items: r.items, inventoryStatus: r.inventory_status, paymentStatus: r.payment_status,
  cancelReason: r.cancel_reason, timeline: r.timeline,
  createdAt: r.created_at?.toISOString?.() ?? r.created_at, updatedAt: r.updated_at?.toISOString?.() ?? r.updated_at,
});

@Injectable()
export class OrderViewRepository {
  constructor(private readonly db: Database) {}

  /** Load (and lock) the current view inside the projector's transaction, or start an empty one. */
  async loadForUpdate(tx: PoolClient, orderId: string, at: string): Promise<OrderView> {
    const { rows } = await tx.query(`SELECT * FROM order_view WHERE order_id = $1 FOR UPDATE`, [orderId]);
    return rows[0] ? toView(rows[0]) : emptyView(orderId, at);
  }

  async save(tx: PoolClient, v: OrderView) {
    await tx.query(
      `INSERT INTO order_view (order_id, customer_id, status, total_cents, items, inventory_status,
                               payment_status, cancel_reason, timeline, created_at, updated_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
       ON CONFLICT (order_id) DO UPDATE SET
         customer_id = EXCLUDED.customer_id, status = EXCLUDED.status, total_cents = EXCLUDED.total_cents,
         items = EXCLUDED.items, inventory_status = EXCLUDED.inventory_status,
         payment_status = EXCLUDED.payment_status, cancel_reason = EXCLUDED.cancel_reason,
         timeline = EXCLUDED.timeline, created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at`,
      [v.orderId, v.customerId, v.status, v.totalCents, JSON.stringify(v.items), v.inventoryStatus,
       v.paymentStatus, v.cancelReason, JSON.stringify(v.timeline), v.createdAt, v.updatedAt],
    );
  }

  // ---- queries: simple, index-backed reads, no joins
  async byId(orderId: string) {
    const { rows } = await this.db.pool.query(`SELECT * FROM order_view WHERE order_id = $1`, [orderId]);
    return rows[0] ? toView(rows[0]) : null;
  }

  async byCustomer(customerId: string, limit = 20) {
    const { rows } = await this.db.pool.query(
      `SELECT * FROM order_view WHERE customer_id = $1 ORDER BY created_at DESC NULLS LAST LIMIT $2`,
      [customerId, limit],
    );
    return rows.map(toView);
  }

  async stats() {
    const { rows } = await this.db.pool.query(
      `SELECT status, count(*)::int AS orders, coalesce(sum(total_cents), 0)::int AS total_cents
         FROM order_view GROUP BY status ORDER BY status`,
    );
    return rows;
  }
}
