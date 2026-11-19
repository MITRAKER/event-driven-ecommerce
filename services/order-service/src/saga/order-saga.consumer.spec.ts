import { Logger } from '@nestjs/common';
import { EventType, InvariantError, Limits, parseEvent, ShopEvent } from '@shop/contracts';
import { KafkaClient } from '@shop/messaging';
import {
  fakeClient,
  fakeDatabase,
  inventoryRejected,
  inventoryReserved,
  ORDER_ID,
  orderCancelled,
  orderConfirmed,
  orderCreated,
  paymentFailed,
  paymentSucceeded,
  queriesWith,
  RecordedQuery,
  TEST_CONFIG,
} from '../testing';
import { OrderSagaConsumer } from './order-saga.consumer';
import { OrderStatus } from './order-state';

interface Scenario {
  /** The order's current status. Leave it out for an order this service has never seen. */
  readonly status?: OrderStatus;
  /** How many rows the status UPDATE reports. */
  readonly updatedRows?: number;
  /** false: processed_events already holds this eventId (a redelivery). */
  readonly fresh?: boolean;
}

/** A consumer over a fake database that answers each statement the way `scenario` says. */
function setup(scenario: Scenario = {}): { consumer: OrderSagaConsumer; queries: RecordedQuery[] } {
  const { status, updatedRows = 1, fresh = true } = scenario;
  const { client, queries } = fakeClient((sql) => {
    if (sql.includes('processed_events')) return { rowCount: fresh ? 1 : 0 };
    if (sql.startsWith('SELECT status')) return { rows: status === undefined ? [] : [{ status }] };
    if (sql.startsWith('UPDATE orders')) return { rowCount: updatedRows };
    return { rowCount: 1 }; // INSERT INTO outbox
  });
  const kafka = {} as unknown as KafkaClient; // process() never touches Kafka
  return { consumer: new OrderSagaConsumer(fakeDatabase(client), kafka, TEST_CONFIG), queries };
}

/** A bug on purpose: handledTypes lets in a type that handle() has no case for. */
class MisconfiguredConsumer extends OrderSagaConsumer {
  protected override readonly handledTypes: ReadonlySet<EventType> = new Set<EventType>(['OrderCreated']);
}

/** The first word of every statement, in order: shows which writes ran in the transaction. */
function verbs(queries: readonly RecordedQuery[]): string[] {
  return queries.map((q) => q.sql.trim().split(/\s+/)[0] ?? '');
}

/** The [orderId, status] parameters of every status UPDATE that ran. */
function statusUpdates(queries: readonly RecordedQuery[]): (readonly unknown[])[] {
  return queriesWith(queries, 'UPDATE orders').map((q) => q.params);
}

/** The events staged in the outbox, parsed exactly as a downstream consumer will parse them. */
function staged(queries: readonly RecordedQuery[]): ShopEvent[] {
  return queriesWith(queries, 'INSERT INTO outbox').map((q) => parseEvent(JSON.parse(String(q.params[3]))));
}

describe('OrderSagaConsumer.process', () => {
  beforeEach(() => {
    // Quiet test output. The unknown-order test asserts on warn.
    jest.spyOn(Logger.prototype, 'log').mockImplementation(() => undefined);
    jest.spyOn(Logger.prototype, 'warn').mockImplementation(() => undefined);
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('confirms a PENDING order when payment succeeds, and announces OrderConfirmed', async () => {
    const { consumer, queries } = setup({ status: 'PENDING' });
    const event = paymentSucceeded();

    expect(await consumer.process(event)).toBe('handled');
    // claim the eventId, lock and read the order, update it, stage the outcome: one transaction
    expect(verbs(queries)).toEqual(['INSERT', 'SELECT', 'UPDATE', 'INSERT']);
    expect(queriesWith(queries, 'FOR UPDATE')[0]?.params).toEqual([ORDER_ID]);
    expect(statusUpdates(queries)).toEqual([[ORDER_ID, 'CONFIRMED']]);
    const [confirmed, ...rest] = staged(queries);
    expect(rest).toEqual([]);
    expect(confirmed).toMatchObject({
      eventType: 'OrderConfirmed',
      aggregateId: ORDER_ID,
      payload: { orderId: ORDER_ID },
      causationId: event.eventId, // the trace chain: this event happened because of that one
      correlationId: event.correlationId,
    });
  });

  it('cancels a PENDING order when payment fails, and announces OrderCancelled with the reason', async () => {
    const { consumer, queries } = setup({ status: 'PENDING' });
    const event = paymentFailed('card declined');

    expect(await consumer.process(event)).toBe('handled');
    expect(statusUpdates(queries)).toEqual([[ORDER_ID, 'CANCELLED']]);
    expect(staged(queries)).toMatchObject([
      {
        eventType: 'OrderCancelled',
        payload: { orderId: ORDER_ID, reason: 'payment: card declined' },
        causationId: event.eventId,
      },
    ]);
  });

  it('cancels a PENDING order when inventory is rejected, which triggers compensation downstream', async () => {
    const { consumer, queries } = setup({ status: 'PENDING' });

    expect(await consumer.process(inventoryRejected('out of stock: KEYBOARD'))).toBe('handled');
    expect(statusUpdates(queries)).toEqual([[ORDER_ID, 'CANCELLED']]);
    expect(staged(queries)).toMatchObject([
      { eventType: 'OrderCancelled', payload: { reason: 'inventory: out of stock: KEYBOARD' } },
    ]);
  });

  it.each<OrderStatus>(['CONFIRMED', 'CANCELLED'])(
    'changes nothing for an order that is already %s',
    async (status) => {
      const { consumer, queries } = setup({ status });

      expect(await consumer.process(paymentFailed('late duplicate'))).toBe('handled');
      expect(statusUpdates(queries)).toEqual([]);
      expect(staged(queries)).toEqual([]);
    },
  );

  it('logs a warning and changes nothing for an order it does not know', async () => {
    const { consumer, queries } = setup(); // the SELECT finds no row
    const warn = jest.spyOn(Logger.prototype, 'warn');

    expect(await consumer.process(paymentSucceeded())).toBe('handled');
    expect(warn).toHaveBeenCalledWith(expect.stringContaining(`unknown order ${ORDER_ID}`));
    expect(statusUpdates(queries)).toEqual([]);
    expect(staged(queries)).toEqual([]);
  });

  it('fails (so the transaction rolls back) if the UPDATE does not change exactly one row', async () => {
    const { consumer, queries } = setup({ status: 'PENDING', updatedRows: 0 });

    await expect(consumer.process(paymentSucceeded())).rejects.toThrow(InvariantError);
    expect(staged(queries)).toEqual([]);
  });

  it('skips a redelivered event without touching the order', async () => {
    const { consumer, queries } = setup({ status: 'PENDING', fresh: false });

    expect(await consumer.process(paymentSucceeded())).toBe('duplicate');
    expect(verbs(queries)).toEqual(['INSERT']); // only the eventId claim
  });

  it.each(
    [orderCreated(), orderConfirmed(), orderCancelled('x'), inventoryReserved()].map(
      (event) => [event.eventType, event] as const,
    ),
  )('ignores %s without opening a transaction', async (_type, event) => {
    const { consumer, queries } = setup({ status: 'PENDING' });

    expect(await consumer.process(event)).toBe('ignored');
    expect(queries).toEqual([]);
  });

  it('keeps a long upstream reason within the contract, so downstream consumers accept OrderCancelled', async () => {
    const { consumer, queries } = setup({ status: 'PENDING' });

    await consumer.process(inventoryRejected('x'.repeat(Limits.maxReasonLength)));
    // staged() runs parseEvent, which would throw on a reason over the limit
    const [cancelled] = staged(queries);
    expect(cancelled?.eventType).toBe('OrderCancelled');
  });

  it('fails loudly if handledTypes lets in a type that handle() has no case for', async () => {
    const { client } = fakeClient(() => ({ rowCount: 1 }));
    const consumer = new MisconfiguredConsumer(
      fakeDatabase(client),
      {} as unknown as KafkaClient,
      TEST_CONFIG,
    );

    // InvariantError counts as permanent: the message goes to the dead-letter topic, not into a retry loop.
    await expect(consumer.process(orderCreated())).rejects.toThrow(InvariantError);
  });
});

describe('OrderSagaConsumer wiring', () => {
  it('subscribes to the inventory and payments topics as consumer group order-service', async () => {
    const subscribe = jest.fn();
    const consumerFactory = jest.fn(() => ({ connect: jest.fn(), subscribe, run: jest.fn() }));
    const kafka = {
      ensureTopics: jest.fn(),
      kafka: { consumer: consumerFactory, producer: () => ({ connect: jest.fn() }) },
    } as unknown as KafkaClient;
    const consumer = new OrderSagaConsumer(fakeDatabase(fakeClient().client), kafka, TEST_CONFIG);

    await consumer.onApplicationBootstrap();
    expect(consumerFactory).toHaveBeenCalledWith({ groupId: 'order-service' });
    expect(subscribe).toHaveBeenCalledWith({ topics: ['inventory', 'payments'], fromBeginning: true });
  });
});
