import { withRetry } from './retry';

const noSleep = (): Promise<void> => Promise.resolve();

describe('withRetry', () => {
  it('returns as soon as a try succeeds, with exponential backoff between tries', async () => {
    const delays: number[] = [];
    const fn = jest
      .fn<Promise<string>, []>()
      .mockRejectedValueOnce(new Error('blip'))
      .mockRejectedValueOnce(new Error('blip'))
      .mockResolvedValue('ok');
    const onRetry = jest.fn();
    const result = await withRetry(fn, {
      attempts: 5,
      baseDelayMs: 100,
      sleep: (ms) => {
        delays.push(ms);
        return Promise.resolve();
      },
      onRetry,
    });
    expect(result).toBe('ok');
    expect(delays).toEqual([100, 200]);
    expect(onRetry).toHaveBeenCalledTimes(2);
  });

  it('gives up after the last attempt and rethrows the error', async () => {
    const fn = jest.fn<Promise<never>, []>().mockRejectedValue(new Error('poison'));
    await expect(withRetry(fn, { attempts: 3, baseDelayMs: 1, sleep: noSleep })).rejects.toThrow('poison');
    expect(fn).toHaveBeenCalledTimes(3);
  });

  it('does not retry permanent errors', async () => {
    const fn = jest.fn<Promise<never>, []>().mockRejectedValue(new TypeError('bad data'));
    const isPermanent = (err: unknown): boolean => err instanceof TypeError;
    await expect(withRetry(fn, { attempts: 5, baseDelayMs: 1, sleep: noSleep, isPermanent })).rejects.toThrow(
      'bad data',
    );
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('awaits beforeTry before every try, and stops at once if it throws', async () => {
    const fn = jest.fn<Promise<never>, []>().mockRejectedValue(new Error('blip'));
    const beforeTry = jest.fn<Promise<void>, []>().mockResolvedValue(undefined);
    await expect(withRetry(fn, { attempts: 3, baseDelayMs: 1, sleep: noSleep, beforeTry })).rejects.toThrow(
      'blip',
    );
    expect(beforeTry).toHaveBeenCalledTimes(3); // before tries 1, 2 and 3

    fn.mockClear();
    beforeTry.mockResolvedValueOnce(undefined).mockRejectedValue(new Error('rebalancing'));
    await expect(withRetry(fn, { attempts: 3, baseDelayMs: 1, sleep: noSleep, beforeTry })).rejects.toThrow(
      'rebalancing',
    );
    expect(fn).toHaveBeenCalledTimes(1); // try 1 ran, then the check before try 2 failed
  });

  it('refuses an unbounded or nonsensical retry budget', async () => {
    const fn = jest.fn<Promise<string>, []>().mockResolvedValue('ok');
    await expect(withRetry(fn, { attempts: 0, baseDelayMs: 1 })).rejects.toThrow('attempts must be 1..10');
    await expect(withRetry(fn, { attempts: 11, baseDelayMs: 1 })).rejects.toThrow('attempts must be 1..10');
    await expect(withRetry(fn, { attempts: 1, baseDelayMs: -1 })).rejects.toThrow('baseDelayMs');
    expect(fn).not.toHaveBeenCalled();
  });

  it('really waits when no sleep function is injected', async () => {
    const fn = jest
      .fn<Promise<string>, []>()
      .mockRejectedValueOnce(new Error('blip'))
      .mockResolvedValue('ok');
    await expect(withRetry(fn, { attempts: 2, baseDelayMs: 1 })).resolves.toBe('ok');
  });
});
