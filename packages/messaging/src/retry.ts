import { invariant } from '@shop/contracts';

export interface RetryOptions {
  /** Total tries, including the first. Bounded (JPL rule 2). */
  attempts: number;
  /** Delay before the 2nd try; doubles each time. */
  baseDelayMs: number;
  /** Errors that retrying cannot fix (bad data). They are rethrown immediately. */
  isPermanent?: (err: unknown) => boolean;
  sleep?: (ms: number) => Promise<void>;
  onRetry?: (attempt: number, err: unknown) => void;
  /**
   * Awaited before every try, the first one included (the Kafka consumer sends a heartbeat here).
   * If it throws, withRetry stops at once and throws that error.
   */
  beforeTry?: () => Promise<void>;
}

const MAX_ATTEMPTS = 10;

function realSleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Retry with exponential backoff: 200ms, 400ms, 800ms...
 * Good for transient failures (DB restart, network blip). Useless for poison messages
 * (bad data fails every time), which is why the caller sends those to a dead-letter topic.
 */
export async function withRetry<T>(fn: () => Promise<T>, opts: RetryOptions): Promise<T> {
  invariant(opts.attempts >= 1 && opts.attempts <= MAX_ATTEMPTS, `attempts must be 1..${MAX_ATTEMPTS}`);
  invariant(opts.baseDelayMs >= 0, 'baseDelayMs must not be negative');
  const sleep = opts.sleep ?? realSleep;
  let lastError: unknown = undefined;
  for (let attempt = 1; attempt <= opts.attempts; attempt++) {
    await opts.beforeTry?.(); // outside the try: its error is not a failed attempt
    try {
      return await fn();
    } catch (err) {
      lastError = err;
      if (attempt === opts.attempts || opts.isPermanent?.(err) === true) break;
      opts.onRetry?.(attempt, err);
      await sleep(opts.baseDelayMs * 2 ** (attempt - 1));
    }
  }
  throw lastError;
}
