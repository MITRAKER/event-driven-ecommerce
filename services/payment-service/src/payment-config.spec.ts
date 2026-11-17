import { Limits } from '@shop/contracts';
import { Env } from '@shop/messaging';
import { loadPaymentSettings, PaymentConfig, paymentConfigProvider } from './payment-config';

describe('loadPaymentSettings', () => {
  it('defaults to declining above $1,000, with no random failures', () => {
    expect(loadPaymentSettings({})).toEqual({ declineAboveCents: 100_000, failureRate: 0 });
  });

  it('reads both settings from the environment', () => {
    expect(loadPaymentSettings({ DECLINE_ABOVE_CENTS: '5000', PAYMENT_FAILURE_RATE: '0.25' })).toEqual({
      declineAboveCents: 5000,
      failureRate: 0.25,
    });
  });

  it('accepts both ends of each range', () => {
    expect(loadPaymentSettings({ DECLINE_ABOVE_CENTS: '0', PAYMENT_FAILURE_RATE: '1' })).toEqual({
      declineAboveCents: 0,
      failureRate: 1,
    });
    const max = String(Limits.maxOrderTotalCents);
    expect(loadPaymentSettings({ DECLINE_ABOVE_CENTS: max }).declineAboveCents).toBe(
      Limits.maxOrderTotalCents,
    );
  });

  it.each<[Env, string]>([
    [{ DECLINE_ABOVE_CENTS: '-1' }, 'config: DECLINE_ABOVE_CENTS must be in 0..1000000000, got -1'],
    [
      { DECLINE_ABOVE_CENTS: '1000000001' },
      'config: DECLINE_ABOVE_CENTS must be in 0..1000000000, got 1000000001',
    ],
    [{ DECLINE_ABOVE_CENTS: '99.5' }, 'config: DECLINE_ABOVE_CENTS must be an integer, got "99.5"'],
    [{ DECLINE_ABOVE_CENTS: 'lots' }, 'config: DECLINE_ABOVE_CENTS must be an integer, got "lots"'],
    [{ PAYMENT_FAILURE_RATE: '50' }, 'config: PAYMENT_FAILURE_RATE must be in 0..1, got 50'],
    [{ PAYMENT_FAILURE_RATE: '-0.1' }, 'config: PAYMENT_FAILURE_RATE must be in 0..1, got -0.1'],
    [{ PAYMENT_FAILURE_RATE: 'often' }, 'config: PAYMENT_FAILURE_RATE must be a number, got "often"'],
  ])('stops the service at startup on %p', (env, message) => {
    expect(() => loadPaymentSettings(env)).toThrow(message);
  });
});

describe('paymentConfigProvider', () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('is registered under the PaymentConfig class, the token Nest injects by', () => {
    expect(paymentConfigProvider.provide).toBe(PaymentConfig);
  });

  it('builds PaymentConfig from process.env', async () => {
    jest.replaceProperty(process, 'env', { DECLINE_ABOVE_CENTS: '2500', PAYMENT_FAILURE_RATE: '0.1' });
    const config = await paymentConfigProvider.useFactory();
    expect(config).toBeInstanceOf(PaymentConfig);
    expect(config.settings).toEqual({ declineAboveCents: 2500, failureRate: 0.1 });
  });
});
