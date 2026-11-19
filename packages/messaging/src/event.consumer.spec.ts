import {
  createEvent,
  EventType,
  InvalidEventError,
  InvariantError,
  OrderCreated,
  PaymentFailed,
  ShopEvent,
  Topic,
} from '@shop/contracts';
import { KafkaMessage } from 'kafkajs';
import { PoolClient } from 'pg';
import { Database } from './database';
import { decodeMessage, Delivery, EventConsumer } from './event.consumer';
import { KafkaClient } from './kafka';
import { testConfig } from './testing';

/**
 * A fake Database whose processed_events table behaves like a real unique table,
 * including rollback: a claim made in a failed transaction is forgotten.
 */
function fakeDb(): Database {
  const committed = new Set<unknown>();
  async function transaction(fn: (tx: unknown) => Promise<unknown>): Promise<unknown> {
    const pending = new Set<unknown>();
    const client = {
      query: (sql: string, params: unknown[]) => {
        if (!sql.includes('processed_events')) return Promise.resolve({ rowCount: 0, rows: [] });
        const fresh = !committed.has(params[0]) && !pending.has(params[0]);
        pending.add(params[0]);
        return Promise.resolve({ rowCount: fresh ? 1 : 0 });
      },
    };
    const result = await fn(client);
    pending.forEach((id) => committed.add(id));
    return result;
  }
  return { transaction } as unknown as Database;
}

class TestConsumer extends EventConsumer {
  protected readonly groupId = 'test-group';
  protected readonly topics: readonly Topic[] = ['orders'];
  protected readonly handledTypes: ReadonlySet<EventType> = new Set<EventType>(['OrderCreated']);
  readonly handled: string[] = [];
  failWith: Error | undefined = undefined;

  protected handle(event: ShopEvent, _tx: PoolClient): Promise<void> {
    if (this.failWith !== undefined) return Promise.reject(this.failWith);
    this.handled.push(event.eventId);
    return Promise.resolve();
  }
}

function setup(): { consumer: TestConsumer; send: jest.Mock } {
  const send = jest.fn().mockResolvedValue([]);
  const kafka = {} as unknown as KafkaClient;
  const consumer = new TestConsumer(
    fakeDb(),
    kafka,
    testConfig({ consumerRetryAttempts: 2, consumerRetryBaseMs: 1 }),
  );
  // Pretend onApplicationBootstrap connected the dead-letter producer.
  Object.assign(consumer, { dlqProducer: { send } });
  return { consumer, send };
}

const created = createEvent<OrderCreated>('OrderCreated', 'o-1', {
  orderId: 'o-1',
  customerId: 'c',
  items: [{ sku: 'BOOK', quantity: 1, unitPriceCents: 100 }],
  totalCents: 100,
});

function message(value: string | null): KafkaMessage {
  return {
    key: Buffer.from('o-1'),
    value: value === null ? null : Buffer.from(value),
    offset: '42',
    timestamp: '0',
    attributes: 0,
    headers: {},
  };
}

/** A message as kafkajs would hand it over, with a heartbeat() that succeeds. */
function delivery(value: string | null, partition = 0): Delivery & { heartbeat: jest.Mock } {
  return {
    topic: 'orders',
    partition,
    message: message(value),
    heartbeat: jest.fn().mockResolvedValue(undefined),
  };
}

/** A type a newer producer might add, which this code has never heard of. */
const fromTheFuture = JSON.stringify({ ...created, eventType: 'OrderShipped' });

describe('decodeMessage', () => {
  const handles = new Set(['OrderCreated']);

  it('turns invalid JSON into an InvalidEventError', () => {
    expect(() => decodeMessage(Buffer.from('not json'), handles)).toThrow(InvalidEventError);
  });

  it('skips a type it does not handle without parsing it, even one it has never heard of', () => {
    expect(decodeMessage(Buffer.from(fromTheFuture), handles)).toBeUndefined();
    expect(decodeMessage(Buffer.from(JSON.stringify(created)), new Set(['PaymentFailed']))).toBeUndefined();
  });

  it('still rejects a handled type that is invalid, and a message with no type at all', () => {
    const futureVersion = JSON.stringify({ ...created, version: 2 });
    expect(() => decodeMessage(Buffer.from(futureVersion), handles)).toThrow('unsupported version 2');
    expect(() => decodeMessage(Buffer.from('{"hello":1}'), handles)).toThrow('unknown eventType');
  });
});

describe('EventConsumer.process', () => {
  it('handles an event once and skips redelivered duplicates', async () => {
    const { consumer } = setup();
    expect(await consumer.process(created)).toBe('handled');
    expect(await consumer.process(created)).toBe('duplicate');
    expect(consumer.handled).toEqual([created.eventId]);
  });

  it('ignores event types it does not handle', async () => {
    const { consumer } = setup();
    const failed = createEvent<PaymentFailed>('PaymentFailed', 'o-1', { orderId: 'o-1', reason: 'x' });
    expect(await consumer.process(failed)).toBe('ignored');
  });
});

describe('EventConsumer.receive', () => {
  it('processes a valid message without touching the dead-letter topic', async () => {
    const { consumer, send } = setup();
    await consumer.receive(delivery(JSON.stringify(created)));
    expect(consumer.handled).toEqual([created.eventId]);
    expect(send).not.toHaveBeenCalled();
  });

  it('dead-letters an invalid message immediately, with the reason in the headers', async () => {
    const { consumer, send } = setup();
    await consumer.receive(delivery('not json', 2));
    expect(send).toHaveBeenCalledTimes(1);
    const [[sent]] = send.mock.calls as [
      [{ topic: string; messages: { headers: Record<string, string> }[] }],
    ];
    expect(sent.topic).toBe('orders.dlq');
    expect(sent.messages[0]?.headers).toMatchObject({
      'dlq-error': 'message is not valid JSON',
      'dlq-original-topic': 'orders',
      'dlq-original-partition': '2',
      'dlq-original-offset': '42',
      'dlq-consumer-group': 'test-group',
    });
  });

  it('retries a failing handler, then dead-letters the message', async () => {
    const { consumer, send } = setup();
    consumer.failWith = new Error('database unavailable');
    await consumer.receive(delivery(JSON.stringify(created)));
    expect(send).toHaveBeenCalledTimes(1);
  });

  it('skips tombstones (messages with no value)', async () => {
    const { consumer, send } = setup();
    await consumer.receive(delivery(null));
    expect(send).not.toHaveBeenCalled();
    expect(consumer.handled).toEqual([]);
  });

  it('never dead-letters an event because the database is down: Kafka redelivers it later', async () => {
    const { consumer, send } = setup();
    consumer.failWith = Object.assign(new Error('connect ECONNREFUSED'), { code: 'ECONNREFUSED' });
    await expect(consumer.receive(delivery(JSON.stringify(created)))).rejects.toThrow('will be redelivered');
    expect(send).not.toHaveBeenCalled();
  });

  it('dead-letters a broken invariant without retrying', async () => {
    const { consumer, send } = setup();
    consumer.failWith = new InvariantError('stock would go negative');
    await consumer.receive(delivery(JSON.stringify(created)));
    expect(send).toHaveBeenCalledTimes(1);
  });

  it('lets the error reach Kafka if even the dead-letter send fails, so the message is redelivered', async () => {
    const { consumer, send } = setup();
    send.mockRejectedValue(new Error('broker down'));
    await expect(consumer.receive(delivery('not json'))).rejects.toThrow('broker down');
  });

  it('skips an event type from a newer producer instead of dead-lettering it', async () => {
    const { consumer, send } = setup();
    await consumer.receive(delivery(fromTheFuture));
    expect(send).not.toHaveBeenCalled();
    expect(consumer.handled).toEqual([]);
  });

  it('sends Kafka a heartbeat before every try, so a long retry never goes silent', async () => {
    const { consumer } = setup(); // 2 attempts
    consumer.failWith = new Error('a bug');
    const kafkaDelivery = delivery(JSON.stringify(created));
    await consumer.receive(kafkaDelivery);
    expect(kafkaDelivery.heartbeat).toHaveBeenCalledTimes(2);
  });

  it('hands a failed heartbeat back to kafkajs unchanged, so it rejoins the group and redelivers', async () => {
    const { consumer, send } = setup();
    consumer.failWith = new Error('a bug');
    const rebalancing = new Error('The group is rebalancing, so a rejoin is needed');
    const kafkaDelivery = delivery(JSON.stringify(created));
    kafkaDelivery.heartbeat.mockRejectedValue(rebalancing);
    await expect(consumer.receive(kafkaDelivery)).rejects.toBe(rebalancing);
    expect(send).not.toHaveBeenCalled();
  });
});
