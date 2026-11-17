import { decideReservation } from './reservation';

const stock = new Map([
  ['BOOK', 3],
  ['MUG', 1],
]);
const item = (sku: string, quantity: number) => ({ sku, quantity, unitPriceCents: 100 });

describe('decideReservation', () => {
  it('reserves when every line is in stock', () => {
    expect(decideReservation(stock, [item('BOOK', 3), item('MUG', 1)])).toEqual({ ok: true });
  });
  it('rejects the whole order if any line is short (all-or-nothing)', () => {
    expect(decideReservation(stock, [item('BOOK', 1), item('MUG', 2)])).toEqual({
      ok: false,
      reason: 'out of stock: MUG (wanted 2, have 1)',
    });
  });
  it('adds up duplicate lines for the same sku', () => {
    expect(decideReservation(stock, [item('MUG', 1), item('MUG', 1)]).ok).toBe(false);
  });
  it('rejects unknown products', () => {
    expect(decideReservation(stock, [item('GHOST', 1)])).toEqual({ ok: false, reason: 'unknown sku GHOST' });
  });
});
