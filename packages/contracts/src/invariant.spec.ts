import { assertNever, invariant, InvariantError } from './invariant';

describe('invariant', () => {
  it('does nothing when the condition holds', () => {
    expect(() => {
      invariant(true, 'never shown');
    }).not.toThrow();
  });

  it('throws InvariantError with the message when it does not', () => {
    expect(() => {
      invariant(false, 'stock went negative');
    }).toThrow(new InvariantError('stock went negative'));
  });

  it('assertNever reports the impossible value', () => {
    expect(() => assertNever('surprise' as never, 'status')).toThrow('status: unexpected value "surprise"');
  });
});
