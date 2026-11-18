import { InvalidEventError, InvariantError } from '@shop/contracts';

/**
 * How a failed event should be treated. There are three kinds of failure, and each
 * needs a different answer:
 *
 * - 'permanent': the message itself is bad (invalid event) or breaks an invariant.
 *   Retrying cannot help. Dead-letter it now.
 * - 'infrastructure': a dependency is down (database restarting, connection refused,
 *   deadlock). The event is fine. Never dead-letter it: stop and let Kafka redeliver it
 *   later, once the dependency is back.
 * - 'unknown': probably a bug for this specific input. Retry a few times, then
 *   dead-letter it so one message cannot block its partition forever.
 */
export type FailureKind = 'permanent' | 'infrastructure' | 'unknown';

/**
 * Postgres SQLSTATE codes that mean "the database is unavailable or busy, try again".
 * 57014 (query_canceled) is what Postgres raises when the pool's statement_timeout
 * (database.ts) cuts a query short: a slow or overloaded database, not a bad message.
 */
const TRANSIENT_SQLSTATE = /^(08|53|57P|57014|40001|40P01)/;
/** Node network error codes. */
const TRANSIENT_NETWORK = new Set([
  'ECONNREFUSED',
  'ECONNRESET',
  'ETIMEDOUT',
  'EPIPE',
  'ENOTFOUND',
  'EAI_AGAIN',
]);
/** Messages pg / pg-pool use for connection problems that carry no code. */
const TRANSIENT_MESSAGES = [/timeout exceeded when trying to connect/i, /connection terminated/i];

function errorCode(err: Error): string | undefined {
  const code: unknown = (err as Error & { code?: unknown }).code;
  return typeof code === 'string' ? code : undefined;
}

export function classifyFailure(err: unknown): FailureKind {
  if (err instanceof InvalidEventError || err instanceof InvariantError) return 'permanent';
  if (!(err instanceof Error)) return 'unknown';
  const code = errorCode(err);
  if (code !== undefined && (TRANSIENT_SQLSTATE.test(code) || TRANSIENT_NETWORK.has(code))) {
    return 'infrastructure';
  }
  return TRANSIENT_MESSAGES.some((pattern) => pattern.test(err.message)) ? 'infrastructure' : 'unknown';
}

/**
 * Thrown to kafkajs to say "don't commit this offset, give it to me again later".
 * A plain Error on purpose: kafkajs never restarts a consumer that crashed with a
 * TypeError, RangeError, ReferenceError or SyntaxError.
 */
export class RedeliverLaterError extends Error {
  override readonly name = 'RedeliverLaterError';
}
