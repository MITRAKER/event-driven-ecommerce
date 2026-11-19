import { InvariantError, Limits, OrderItem } from '@shop/contracts';
import { decideReservation, mergeLines, StockLevel } from './reservation';

const stock: ReadonlyMap<string, StockLevel> = new Map([
  ['BOOK', { available: 3, priceCents: 4500 }],
  ['MUG', { available: 1, priceCents: 1200 }],
]);

function item(sku: string, quantity: number, unitPriceCents: number): OrderItem {
  return { sku, quantity, unitPriceCents };
}

describe('decideReservation', () => {
  it('reserves when every line is in stock at the catalog price, one line per sku in sku order', () => {
    expect(decideReservation(stock, [item('MUG', 1, 1200), item('BOOK', 3, 4500)])).toEqual({
      ok: true,
      lines: [
        { sku: 'BOOK', quantity: 3 },
        { sku: 'MUG', quantity: 1 },
      ],
    });
  });

  it('rejects the whole order if any line is short (all-or-nothing)', () => {
    expect(decideReservation(stock, [item('BOOK', 1, 4500), item('MUG', 2, 1200)])).toEqual({
      ok: false,
      reason: 'out of stock: MUG (wanted 2, have 1)',
    });
  });

  it('adds up duplicate lines for the same sku before checking stock', () => {
    expect(decideReservation(stock, [item('MUG', 1, 1200), item('MUG', 1, 1200)])).toEqual({
      ok: false,
      reason: 'out of stock: MUG (wanted 2, have 1)',
    });
    expect(decideReservation(stock, [item('BOOK', 1, 4500), item('BOOK', 2, 4500)])).toEqual({
      ok: true,
      lines: [{ sku: 'BOOK', quantity: 3 }],
    });
  });

  it('rejects unknown products', () => {
    expect(decideReservation(stock, [item('GHOST', 1, 100)])).toEqual({
      ok: false,
      reason: 'unknown sku GHOST',
    });
  });

  it('rejects a line whose price is not the catalog price: the client does not choose prices', () => {
    expect(decideReservation(stock, [item('BOOK', 1, 100)])).toEqual({
      ok: false,
      reason: 'price changed for BOOK: expected 4500, got 100',
    });
  });

  it('checks the price of every duplicate line, not only the first one', () => {
    expect(decideReservation(stock, [item('BOOK', 1, 4500), item('BOOK', 1, 0)])).toEqual({
      ok: false,
      reason: 'price changed for BOOK: expected 4500, got 0',
    });
  });
});

describe('mergeLines', () => {
  it('adds up quantities per sku and sorts by sku', () => {
    const lines = mergeLines([item('MUG', 1, 1200), item('KEYBOARD', 1, 9000), item('BOOK', 2, 4500)]);
    expect(mergeLines([...lines, { sku: 'MUG', quantity: 3 }])).toEqual([
      { sku: 'BOOK', quantity: 2 },
      { sku: 'KEYBOARD', quantity: 1 },
      { sku: 'MUG', quantity: 4 },
    ]);
  });

  it('refuses more lines than an order may have (every loop is bounded)', () => {
    const tooMany = Array.from({ length: Limits.maxItemsPerOrder + 1 }, (_, i) => item(`SKU-${i}`, 1, 1));
    expect(() => mergeLines(tooMany)).toThrow(InvariantError);
  });
});
