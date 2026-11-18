import { loadMessagingSettings, readInt, readNumber, readString } from './config';

const base = { SERVICE_NAME: 'order-service', DATABASE_URL: 'postgres://x', PORT: '3001' };

describe('config', () => {
  it('loads defaults for everything optional', () => {
    expect(loadMessagingSettings(base)).toEqual({
      serviceName: 'order-service',
      port: 3001,
      databaseUrl: 'postgres://x',
      databasePoolMax: 10,
      kafkaBrokers: ['localhost:19092'],
      outboxPollMs: 500,
      outboxBatchSize: 100,
      consumerRetryAttempts: 5,
      consumerRetryBaseMs: 200,
    });
  });

  it('splits and trims the broker list', () => {
    expect(loadMessagingSettings({ ...base, KAFKA_BROKERS: ' a:1, b:2 ,' }).kafkaBrokers).toEqual([
      'a:1',
      'b:2',
    ]);
  });

  it('fails fast on a missing required setting', () => {
    expect(() => loadMessagingSettings({ ...base, DATABASE_URL: undefined })).toThrow(
      'config: DATABASE_URL is required',
    );
  });

  it('rejects an empty broker list', () => {
    expect(() => loadMessagingSettings({ ...base, KAFKA_BROKERS: ' , ' })).toThrow('KAFKA_BROKERS');
  });

  it('keeps every wait between retries short enough for a Kafka heartbeat to follow in time', () => {
    const settings = (attempts: string, baseMs: string): unknown =>
      loadMessagingSettings({ ...base, CONSUMER_RETRY_ATTEMPTS: attempts, CONSUMER_RETRY_BASE_MS: baseMs });
    expect(settings('10', '39')).toMatchObject({ consumerRetryAttempts: 10 }); // longest wait 9,984 ms
    expect(settings('1', '10000')).toMatchObject({ consumerRetryAttempts: 1 }); // one try, no wait
    expect(() => settings('10', '200')).toThrow('longest wait of 51200 ms');
  });

  it('readString uses the fallback only when the variable is unset', () => {
    expect(readString({}, 'X', 'fallback')).toBe('fallback');
    expect(readString({ X: 'set' }, 'X', 'fallback')).toBe('set');
    expect(() => readString({ X: '' }, 'X')).toThrow('config: X is required');
  });

  it('readInt rejects non-integers and out-of-range values', () => {
    expect(readInt({ N: '7' }, 'N', 1, [1, 10])).toBe(7);
    expect(() => readInt({ N: 'seven' }, 'N', 1, [1, 10])).toThrow('must be an integer');
    expect(() => readInt({ N: '2.5' }, 'N', 1, [1, 10])).toThrow('must be an integer');
    expect(() => readInt({ N: '11' }, 'N', 1, [1, 10])).toThrow('must be in 1..10');
  });

  // Number('') is 0, so without a format check a blank variable would quietly become 0.
  it.each(['', '  ', '0x5', '5e0', 'Infinity', '7 days'])('readInt rejects %j', (raw) => {
    expect(() => readInt({ N: raw }, 'N', 1, [0, 10])).toThrow('must be an integer');
  });

  it('readInt accepts surrounding spaces and negative numbers', () => {
    expect(readInt({ N: ' 7 ' }, 'N', 1, [-10, 10])).toBe(7);
    expect(readInt({ N: '-3' }, 'N', 1, [-10, 10])).toBe(-3);
  });

  it('readNumber accepts decimals inside the range', () => {
    expect(readNumber({ P: '0.25' }, 'P', 0, [0, 1])).toBe(0.25);
    expect(readNumber({ P: '1' }, 'P', 0, [0, 1])).toBe(1);
    expect(readNumber({ P: '.5' }, 'P', 0, [0, 1])).toBe(0.5);
    expect(readNumber({}, 'P', 0.5, [0, 1])).toBe(0.5);
    expect(() => readNumber({ P: 'abc' }, 'P', 0, [0, 1])).toThrow('must be a number');
    expect(() => readNumber({ P: '1.5' }, 'P', 0, [0, 1])).toThrow('must be in 0..1');
  });

  it.each(['', '  ', '1.', '1.2.3', '5e-1', 'NaN'])('readNumber rejects %j', (raw) => {
    expect(() => readNumber({ P: raw }, 'P', 0, [0, 1])).toThrow('must be a number');
  });
});
