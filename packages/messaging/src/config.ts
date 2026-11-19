import { Provider } from '@nestjs/common';
import { invariant } from '@shop/contracts';

export type Env = Readonly<Record<string, string | undefined>>;

/** Read a required (or defaulted) string setting. Fails fast at startup if it is missing. */
export function readString(env: Env, name: string, fallback?: string): string {
  const value = env[name] ?? fallback;
  invariant(value !== undefined && value.length > 0, `config: ${name} is required`);
  return value;
}

/**
 * Number() alone is too forgiving for settings: it reads '' and '  ' as 0, so a blank
 * `DECLINE_ABOVE_CENTS=` would quietly decline every payment, and it also accepts 0x1F, 1e3
 * and Infinity. A numeric setting must be a plain base-10 number.
 */
const INTEGER = /^-?\d+$/;
const DECIMAL = /^-?(\d+(\.\d+)?|\.\d+)$/; // 0.25, .25, 3 or -1.5

/** The fallback if the variable is unset. Otherwise its value, or NaN if it isn't written as `format`. */
function parseSetting(raw: string | undefined, fallback: number, format: RegExp): number {
  if (raw === undefined) return fallback;
  const text = raw.trim();
  return format.test(text) ? Number(text) : Number.NaN;
}

/** Read an integer setting and check it is inside [min, max] (JPL rules 3 and 7). */
export function readInt(env: Env, name: string, fallback: number, range: readonly [number, number]): number {
  const raw = env[name];
  const value = parseSetting(raw, fallback, INTEGER);
  const [min, max] = range;
  invariant(Number.isInteger(value), `config: ${name} must be an integer, got ${JSON.stringify(raw)}`);
  invariant(value >= min && value <= max, `config: ${name} must be in ${min}..${max}, got ${value}`);
  return value;
}

/** Read a decimal setting (for example a probability) and check it is inside [min, max]. */
export function readNumber(
  env: Env,
  name: string,
  fallback: number,
  range: readonly [number, number],
): number {
  const raw = env[name];
  const value = parseSetting(raw, fallback, DECIMAL);
  const [min, max] = range;
  invariant(Number.isFinite(value), `config: ${name} must be a number, got ${JSON.stringify(raw)}`);
  invariant(value >= min && value <= max, `config: ${name} must be in ${min}..${max}, got ${value}`);
  return value;
}

/**
 * The longest allowed wait between two tries of one message. The consumer sends Kafka a heartbeat
 * before every try, and Kafka drops a consumer it hasn't heard from in 30 s (the session timeout).
 * The longest silence is one try (a query may run up to the 10 s statement_timeout) plus one wait,
 * so a wait of at most 10 s keeps it near 20 s, with room to spare.
 */
const MAX_RETRY_WAIT_MS = 10_000;

export interface MessagingSettings {
  readonly serviceName: string;
  readonly port: number;
  readonly databaseUrl: string;
  /** Upper bound on open DB connections per service instance (JPL rule 3). */
  readonly databasePoolMax: number;
  readonly kafkaBrokers: readonly string[];
  readonly outboxPollMs: number;
  /** Upper bound on rows published per relay tick (JPL rule 2). */
  readonly outboxBatchSize: number;
  /** Upper bound on in-process retries before a message is dead-lettered (JPL rule 2). */
  readonly consumerRetryAttempts: number;
  readonly consumerRetryBaseMs: number;
}

/**
 * All settings in one validated object, read once at startup. A bad value stops the service
 * immediately with a clear message, instead of failing later in some unrelated place.
 */
export function loadMessagingSettings(env: Env): MessagingSettings {
  const brokers = readString(env, 'KAFKA_BROKERS', 'localhost:19092')
    .split(',')
    .map((b) => b.trim())
    .filter((b) => b.length > 0);
  invariant(brokers.length > 0 && brokers.length <= 16, 'config: KAFKA_BROKERS needs 1..16 brokers');
  const consumerRetryAttempts = readInt(env, 'CONSUMER_RETRY_ATTEMPTS', 5, [1, 10]);
  const consumerRetryBaseMs = readInt(env, 'CONSUMER_RETRY_BASE_MS', 200, [1, 10_000]);
  // The waits double: base, 2 x base, 4 x base... The last one is base x 2^(attempts - 2).
  const longestWait = consumerRetryBaseMs * 2 ** (consumerRetryAttempts - 2);
  invariant(
    longestWait <= MAX_RETRY_WAIT_MS,
    `config: CONSUMER_RETRY_BASE_MS and CONSUMER_RETRY_ATTEMPTS give a longest wait of ${longestWait} ms between retries, the limit is ${MAX_RETRY_WAIT_MS} ms`,
  );
  return {
    serviceName: readString(env, 'SERVICE_NAME'),
    port: readInt(env, 'PORT', 3000, [1, 65_535]),
    databaseUrl: readString(env, 'DATABASE_URL'),
    databasePoolMax: readInt(env, 'DATABASE_POOL_MAX', 10, [1, 50]),
    kafkaBrokers: brokers,
    outboxPollMs: readInt(env, 'OUTBOX_POLL_MS', 500, [50, 60_000]),
    outboxBatchSize: readInt(env, 'OUTBOX_BATCH_SIZE', 100, [1, 1_000]),
    consumerRetryAttempts,
    consumerRetryBaseMs,
  };
}

/** The settings as an injectable, so every class gets them from Nest instead of reading process.env. */
export class MessagingConfig {
  constructor(readonly settings: MessagingSettings) {}
}

/** Nest provider: builds MessagingConfig from the environment once, at startup. */
export const messagingConfigProvider: Provider = {
  provide: MessagingConfig,
  useFactory: (): MessagingConfig => new MessagingConfig(loadMessagingSettings(process.env)),
};
