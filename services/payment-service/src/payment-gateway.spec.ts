import { charge, DECLINE_ABOVE_CENTS } from './payment-gateway';

describe('fake payment gateway', () => {
  afterEach(() => delete process.env.PAYMENT_FAILURE_RATE);

  it('approves normal amounts', () => {
    expect(charge(4_999)).toEqual({ ok: true });
  });
  it('declines amounts over the limit, so the compensation path can be demoed', () => {
    expect(charge(DECLINE_ABOVE_CENTS + 1)).toEqual({
      ok: false,
      reason: 'card declined: amount over limit',
    });
  });
  it('can simulate random failures', () => {
    process.env.PAYMENT_FAILURE_RATE = '0.5';
    expect(charge(100, () => 0.1).ok).toBe(false);
    expect(charge(100, () => 0.9).ok).toBe(true);
  });
});
