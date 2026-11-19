import { InvariantError, Limits } from '@shop/contracts';
import { PaymentSettings } from './payment-config';
import { charge } from './payment-gateway';

/** The defaults: decline above $1,000, no random failures. */
const settings: PaymentSettings = { declineAboveCents: 100_000, failureRate: 0 };

describe('charge (the fake payment gateway)', () => {
  it('approves normal amounts, up to and including the limit', () => {
    expect(charge(4_999, settings)).toEqual({ ok: true });
    expect(charge(0, settings)).toEqual({ ok: true });
    expect(charge(settings.declineAboveCents, settings)).toEqual({ ok: true });
  });

  it('declines amounts over the limit, so the compensation path can be demoed', () => {
    expect(charge(settings.declineAboveCents + 1, settings)).toEqual({
      ok: false,
      reason: 'card declined: amount over limit',
    });
  });

  it('declines over the limit every time, without rolling the dice', () => {
    const random = jest.fn(() => 0.99);
    expect(charge(150_000, { ...settings, failureRate: 0.5 }, random).ok).toBe(false);
    expect(random).not.toHaveBeenCalled();
  });

  it('can simulate random failures, repeatably, with an injected dice roll', () => {
    const flaky = { ...settings, failureRate: 0.5 };
    expect(charge(100, flaky, () => 0.1)).toEqual({ ok: false, reason: 'card declined: random failure' });
    expect(charge(100, flaky, () => 0.5)).toEqual({ ok: true }); // a roll equal to the rate passes
    expect(charge(100, flaky, () => 0.9)).toEqual({ ok: true });
  });

  it('rolls with Math.random by default: rate 0 never fails, rate 1 always does', () => {
    // Math.random() returns a number in [0, 1): never below 0, always below 1.
    expect(charge(100, { ...settings, failureRate: 0 }).ok).toBe(true);
    expect(charge(100, { ...settings, failureRate: 1 }).ok).toBe(false);
  });

  it.each([-1, 0.5, 4_999.99, Number.NaN, Number.POSITIVE_INFINITY, Limits.maxOrderTotalCents + 1])(
    'refuses %p: an amount must be whole cents within the contract limit',
    (amountCents) => {
      expect(() => charge(amountCents, settings)).toThrow(InvariantError);
    },
  );

  it.each([1, -0.1, Number.NaN])(
    'refuses a dice roll of %p: outside [0, 1) the rate means nothing',
    (roll) => {
      expect(() => charge(100, { ...settings, failureRate: 0.5 }, () => roll)).toThrow(
        `charge: random() must return a number in [0, 1), got ${roll}`,
      );
    },
  );
});
