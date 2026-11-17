import { OrderItem } from '@shop/contracts';

export type ReservationDecision = { ok: true } | { ok: false; reason: string };

/**
 * Pure decision: can every line of the order be reserved from current stock?
 * All-or-nothing: we never reserve half an order.
 */
export function decideReservation(stock: Map<string, number>, items: OrderItem[]): ReservationDecision {
  // Merge duplicate lines (e.g. two separate "MUG x1" lines) before checking.
  const wanted = new Map<string, number>();
  for (const i of items) wanted.set(i.sku, (wanted.get(i.sku) ?? 0) + i.quantity);

  for (const [sku, qty] of wanted) {
    const available = stock.get(sku);
    if (available === undefined) return { ok: false, reason: `unknown sku ${sku}` };
    if (available < qty)
      return { ok: false, reason: `out of stock: ${sku} (wanted ${qty}, have ${available})` };
  }
  return { ok: true };
}
