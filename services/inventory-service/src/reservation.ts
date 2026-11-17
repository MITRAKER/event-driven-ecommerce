import { invariant, Limits, OrderItem } from '@shop/contracts';

/** What the catalog says about one product, read while its row is locked. */
export interface StockLevel {
  readonly available: number;
  readonly priceCents: number;
}

/** One sku of an order, with all of its lines added up. */
export interface StockLine {
  readonly sku: string;
  readonly quantity: number;
}

/** On success, `lines` is exactly what to take from stock: one entry per sku, in sku order. */
export type ReservationDecision =
  | { readonly ok: true; readonly lines: readonly StockLine[] }
  | { readonly ok: false; readonly reason: string };

/**
 * Add up duplicate lines (two separate "MUG x1" lines become one "MUG x2") and sort by sku,
 * so stock is checked per product and always changed in the same, predictable order.
 */
export function mergeLines(items: readonly StockLine[]): StockLine[] {
  // Every loop has a known upper bound (JPL rule 2): orders are validated to this many lines.
  invariant(
    items.length <= Limits.maxItemsPerOrder,
    `an order has at most ${Limits.maxItemsPerOrder} lines, got ${items.length}`,
  );
  const totals = new Map<string, number>();
  for (const item of items) {
    totals.set(item.sku, (totals.get(item.sku) ?? 0) + item.quantity);
  }
  // Plain string comparison, not localeCompare: the same order on every machine. The skus are
  // unique map keys, so two entries are never equal.
  return [...totals].map(([sku, quantity]) => ({ sku, quantity })).sort((a, b) => (a.sku < b.sku ? -1 : 1));
}

/**
 * Pure decision: can the whole order be reserved, at the catalog's prices?
 * All-or-nothing: we never reserve half an order.
 *
 * The catalog owns prices. The client still sends unitPriceCents (the price it showed the
 * customer), but a line priced differently from the catalog is rejected. Otherwise anyone
 * could buy a laptop for 0 cents.
 */
export function decideReservation(
  stock: ReadonlyMap<string, StockLevel>,
  items: readonly OrderItem[],
): ReservationDecision {
  // Merge duplicate lines first: two "MUG x1" lines need 2 mugs in stock, not 1.
  const lines = mergeLines(items);
  for (const line of lines) {
    const product = stock.get(line.sku);
    if (product === undefined) return { ok: false, reason: `unknown sku ${line.sku}` };
    // Check every line of this sku, not only the first: duplicates can carry different prices.
    const mispriced = items.find(
      (item) => item.sku === line.sku && item.unitPriceCents !== product.priceCents,
    );
    if (mispriced !== undefined) {
      return {
        ok: false,
        reason: `price changed for ${line.sku}: expected ${product.priceCents}, got ${mispriced.unitPriceCents}`,
      };
    }
    if (product.available < line.quantity) {
      return {
        ok: false,
        reason: `out of stock: ${line.sku} (wanted ${line.quantity}, have ${product.available})`,
      };
    }
  }
  return { ok: true, lines };
}
