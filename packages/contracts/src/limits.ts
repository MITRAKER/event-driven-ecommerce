/**
 * JPL rule 3 adapted to a garbage-collected language: every collection and number that
 * crosses a trust boundary has a hard upper bound, so memory use and loop counts are
 * bounded by design, not by luck. Validation (HTTP DTOs, parseEvent) enforces these.
 */
export const Limits = {
  maxItemsPerOrder: 50,
  maxQuantityPerItem: 1_000,
  maxUnitPriceCents: 10_000_000, // $100,000
  /** Must stay below 2^31 - 1, the largest value a Postgres INTEGER column can hold. */
  maxOrderTotalCents: 1_000_000_000, // $10,000,000
  maxSkuLength: 64,
  maxCustomerIdLength: 64,
  maxReasonLength: 500,
  /** A saga produces at most 6 events per order; anything past this means a bug or a replay loop. */
  maxEventsPerOrder: 32,
} as const;
