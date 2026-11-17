export interface RetryOptions {
  attempts: number;     // total tries, including the first
  baseDelayMs: number;  // delay before the 2nd try; doubles each time
  sleep?: (ms: number) => Promise<void>;
  onRetry?: (attempt: number, err: unknown) => void;
}

const realSleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/**
 * Retry with exponential backoff: 200ms, 400ms, 800ms...
 * Good for transient failures (DB restart, network blip). Useless for poison messages
 * (bad data fails every time), which is why the caller sends those to a dead-letter topic.
 */
export async function withRetry<T>(fn: () => Promise<T>, opts: RetryOptions): Promise<T> {
  const sleep = opts.sleep ?? realSleep;
  let lastErr: unknown;
  for (let attempt = 1; attempt <= opts.attempts; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      if (attempt === opts.attempts) break;
      opts.onRetry?.(attempt, err);
      await sleep(opts.baseDelayMs * 2 ** (attempt - 1));
    }
  }
  throw lastErr;
}
