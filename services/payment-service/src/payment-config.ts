import { FactoryProvider } from '@nestjs/common';
import { Limits } from '@shop/contracts';
import { Env, readInt, readNumber } from '@shop/messaging';

/** The fake card processor's two knobs, so the saga's failure path can be demoed on purpose. */
export interface PaymentSettings {
  /** A charge above this many cents is declined. The default ($1,000) declines a LAPTOP order. */
  readonly declineAboveCents: number;
  /** The chance (0..1) that any other charge fails, to simulate a flaky real-world processor. */
  readonly failureRate: number;
}

/**
 * Read and range-check the payment settings once, at startup, like loadMessagingSettings() does
 * for the shared ones. A typo such as PAYMENT_FAILURE_RATE=50 (meant as 50%) stops the service
 * with a clear message instead of quietly declining every payment.
 */
export function loadPaymentSettings(env: Env): PaymentSettings {
  return {
    // No order can cost more than maxOrderTotalCents, so a higher limit would mean nothing.
    declineAboveCents: readInt(env, 'DECLINE_ABOVE_CENTS', 100_000, [0, Limits.maxOrderTotalCents]),
    failureRate: readNumber(env, 'PAYMENT_FAILURE_RATE', 0, [0, 1]),
  };
}

/** The settings as an injectable, so the consumer gets them from Nest instead of reading process.env. */
export class PaymentConfig {
  constructor(readonly settings: PaymentSettings) {}
}

/** Nest provider: builds PaymentConfig from the environment once, at startup. The service's only env read. */
export const paymentConfigProvider: FactoryProvider<PaymentConfig> = {
  provide: PaymentConfig,
  useFactory: (): PaymentConfig => new PaymentConfig(loadPaymentSettings(process.env)),
};
