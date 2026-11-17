import { Database } from './database';
import { KafkaProducer } from './kafka';
import { groupByTopic, OutboxRelay } from './outbox.relay';
import { fakeClient, testConfig } from './testing';

const row = (
  id: number,
  topic: string,
): { id: string; topic: string; message_key: string; payload: object } => ({
  id: String(id),
  topic,
  message_key: `order-${id}`,
  payload: { eventId: `e${id}` },
});

interface Setup {
  relay: OutboxRelay;
  queries: ReturnType<typeof fakeClient>['queries'];
  sendBatch: jest.Mock;
}

function setup(
  rows: object[],
  sendBatch = jest.fn().mockResolvedValue([]),
  updatedCount = rows.length,
): Setup {
  const { client, queries } = fakeClient((sql) =>
    sql.trimStart().startsWith('SELECT') ? { rows } : { rowCount: updatedCount },
  );
  const db = { transaction: (fn: (tx: unknown) => Promise<unknown>) => fn(client) } as unknown as Database;
  const kafka = { producer: { sendBatch } } as unknown as KafkaProducer;
  return { relay: new OutboxRelay(db, kafka, testConfig({ outboxBatchSize: 10 })), queries, sendBatch };
}

describe('groupByTopic', () => {
  it('keeps the original order inside each topic', () => {
    const grouped = groupByTopic([row(1, 'orders'), row(2, 'payments'), row(3, 'orders')] as never);
    expect(grouped.map((g) => [g.topic, g.messages.map((m) => m.key)])).toEqual([
      ['orders', ['order-1', 'order-3']],
      ['payments', ['order-2']],
    ]);
  });
});

describe('OutboxRelay.tick', () => {
  it('publishes pending rows grouped by topic, then marks them published', async () => {
    const { relay, queries, sendBatch } = setup([row(1, 'orders'), row(2, 'orders'), row(3, 'payments')]);
    expect(await relay.tick()).toBe(3);
    expect(sendBatch).toHaveBeenCalledTimes(1);
    expect(queries[0]?.params).toEqual([10]); // the batch size bound is passed to LIMIT
    const update = queries.find((q) => q.sql.startsWith('UPDATE'));
    expect(update?.params).toEqual([['1', '2', '3']]);
  });

  it('leaves rows unpublished when Kafka is down, so the next tick retries them', async () => {
    const { relay, queries } = setup(
      [row(1, 'orders')],
      jest.fn().mockRejectedValue(new Error('broker down')),
    );
    expect(await relay.tick()).toBe(0);
    expect(queries.some((q) => q.sql.startsWith('UPDATE'))).toBe(false);
  });

  it('fails the tick (and rolls back) if not every row was marked published', async () => {
    const { relay } = setup([row(1, 'orders'), row(2, 'orders')], undefined, 1);
    expect(await relay.tick()).toBe(0);
  });

  it('does nothing when the outbox is empty', async () => {
    const { relay, sendBatch } = setup([]);
    expect(await relay.tick()).toBe(0);
    expect(sendBatch).not.toHaveBeenCalled();
  });

  it('never runs two ticks at once', async () => {
    const { relay, sendBatch } = setup([row(1, 'orders')]);
    const [first, second] = await Promise.all([relay.tick(), relay.tick()]);
    expect([first, second]).toEqual([1, 0]);
    expect(sendBatch).toHaveBeenCalledTimes(1);
  });

  it('starts and stops its polling timer, waiting for the last tick', async () => {
    jest.useFakeTimers();
    const { relay } = setup([]);
    const tick = jest.spyOn(relay, 'tick');
    relay.onApplicationBootstrap();
    jest.advanceTimersByTime(1_000);
    expect(tick).toHaveBeenCalledTimes(2);
    await relay.onModuleDestroy();
    jest.advanceTimersByTime(1_000);
    expect(tick).toHaveBeenCalledTimes(2);
    jest.useRealTimers();
  });
});
