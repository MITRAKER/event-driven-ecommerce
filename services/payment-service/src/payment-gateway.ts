import { invariant, Limits } from '@shop/contracts';
import { PaymentSettings } from './payment-config';

export type ChargeResult = { readonly ok: true } | { readonly ok: false; readonly reason: string };

/**
 * A fake card processor, written as a pure function (the "functional core"): it reads no
 * environment and does no I/O. Its settings are passed in, and so is the dice roll, which makes
 * even the random failures repeatable in a test.
 *
 * Deterministic by default (so the demo and tests are repeatable): only amounts above
 * `declineAboveCents` are declined. A `failureRate` above 0 adds random declines on top,
 * to simulate flaky real-world payments.
 */
export function charge(
  amountCents: number,
  settings: PaymentSettings,
  random: () => number = Math.random,
): ChargeResult {
  // Money is whole cents, never a float (0.1 + 0.2 !== 0.3), and never more than the contract allows.
  invariant(
    Number.isInteger(amountCents) && amountCents >= 0 && amountCents <= Limits.maxOrderTotalCents,
    `charge: amount must be a whole number of cents in 0..${Limits.maxOrderTotalCents}, got ${amountCents}`,
  );
  if (amountCents > settings.declineAboveCents) {
    return { ok: false, reason: 'card declined: amount over limit' };
  }
  // Check what the injected function returns (JPL rule 7): a broken random source returning NaN
  // or 1 would approve every charge whatever the failure rate says, and a negative one would
  // decline them all.
  const roll = random();
  invariant(roll >= 0 && roll < 1, `charge: random() must return a number in [0, 1), got ${roll}`);
  if (roll < settings.failureRate) {
    return { ok: false, reason: 'card declined: random failure' };
  }
  return { ok: true };
}
