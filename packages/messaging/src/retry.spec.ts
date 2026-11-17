import { withRetry } from './retry';

describe('withRetry', () => {
  it('returns as soon as a try succeeds, with exponential backoff between tries', async () => {
    const delays: number[] = [];
    let calls = 0;
    const result = await withRetry(async () => { if (++calls < 3) throw new Error('blip'); return 'ok'; },
      { attempts: 5, baseDelayMs: 100, sleep: async (ms) => { delays.push(ms); } });
    expect(result).toBe('ok');
    expect(delays).toEqual([100, 200]);
  });

  it('gives up after the last attempt and rethrows the error', async () => {
    const fn = jest.fn().mockRejectedValue(new Error('poison'));
    await expect(withRetry(fn, { attempts: 3, baseDelayMs: 1, sleep: async () => {} })).rejects.toThrow('poison');
    expect(fn).toHaveBeenCalledTimes(3);
  });
});
