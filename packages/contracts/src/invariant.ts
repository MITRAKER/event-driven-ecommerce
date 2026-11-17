/** A broken assumption: either a bug in this service or input that should never have got this far. */
export class InvariantError extends Error {
  override readonly name = 'InvariantError';
}

/**
 * JPL rule 5: state your assumptions as runtime checks.
 *
 * - Side-effect free: it only reads `condition`.
 * - Narrows types for the compiler (`asserts condition`), so later code can rely on it.
 * - Recovery is explicit and the same everywhere: the error propagates, the HTTP layer
 *   answers 500, and a Kafka consumer rolls back, retries, then dead-letters the message.
 *   Nothing ever continues on top of a broken assumption.
 */
export function invariant(condition: boolean, message: string): asserts condition {
  if (!condition) {
    throw new InvariantError(message);
  }
}

/** For the impossible `default:` branch of an exhaustive switch. */
export function assertNever(value: never, context: string): never {
  throw new InvariantError(`${context}: unexpected value ${JSON.stringify(value)}`);
}
