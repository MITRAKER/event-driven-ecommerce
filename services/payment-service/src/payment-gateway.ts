export type ChargeResult = { ok: true } | { ok: false; reason: string };

/** Anything over this amount is declined, so you can trigger the failure path on purpose. */
export const DECLINE_ABOVE_CENTS = Number(process.env.DECLINE_ABOVE_CENTS ?? 100_000); // $1,000

/**
 * A fake card processor. Deterministic by default (so the demo and tests are repeatable),
 * with an optional random failure rate to simulate flaky real-world payments.
 */
export function charge(amountCents: number, random: () => number = Math.random): ChargeResult {
  if (amountCents > DECLINE_ABOVE_CENTS) return { ok: false, reason: 'card declined: amount over limit' };
  const failureRate = Number(process.env.PAYMENT_FAILURE_RATE ?? 0);
  if (random() < failureRate) return { ok: false, reason: 'card declined: random failure' };
  return { ok: true };
}
