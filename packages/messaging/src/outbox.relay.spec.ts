import { OutboxRelay } from './outbox.relay';

function setup(rows: any[], sendBatch = jest.fn().mockResolvedValue(undefined)) {
  const client = { query: jest.fn(async (sql: string, _params?: unknown[]) => (sql.startsWith('SELECT') ? { rows } : { rows: [] })) };
  const db = { transaction: (fn: any) => fn(client) } as any;
  const kafka = { producer: { sendBatch } } as any;
  return { relay: new OutboxRelay(db, kafka), client, sendBatch };
}

const row = (id: number, topic: string) => ({ id, topic, message_key: `order-${id}`, payload: { eventId: `e${id}` } });

describe('OutboxRelay.tick', () => {
  it('publishes pending rows grouped by topic, then marks them published', async () => {
    const { relay, client, sendBatch } = setup([row(1, 'orders'), row(2, 'orders'), row(3, 'payments')]);
    expect(await relay.tick()).toBe(3);

    const { topicMessages } = sendBatch.mock.calls[0][0];
    expect(topicMessages).toHaveLength(2);
    expect(topicMessages[0]).toMatchObject({ topic: 'orders', messages: [{ key: 'order-1' }, { key: 'order-2' }] });

    const update = client.query.mock.calls.find(([sql]) => sql.startsWith('UPDATE'));
    expect(update![1]).toEqual([[1, 2, 3]]);
  });

  it('leaves rows unpublished when Kafka is down, so the next tick retries them', async () => {
    const { relay, client } = setup([row(1, 'orders')], jest.fn().mockRejectedValue(new Error('broker unavailable')));
    expect(await relay.tick()).toBe(0);
    expect(client.query.mock.calls.some(([sql]) => sql.startsWith('UPDATE'))).toBe(false);
  });

  it('does nothing when the outbox is empty', async () => {
    const { relay, sendBatch } = setup([]);
    expect(await relay.tick()).toBe(0);
    expect(sendBatch).not.toHaveBeenCalled();
  });
});
