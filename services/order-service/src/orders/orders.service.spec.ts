import { BadRequestException, NotFoundException } from '@nestjs/common';
import { InvariantError, Limits, OrderItem, parseEvent } from '@shop/contracts';
import { plainToInstance } from 'class-transformer';
import { fakeClient, fakeDatabase, ORDER_ID, queriesWith, RecordedQuery, Responder } from '../testing';
import { CreateOrderDto } from './create-order.dto';
import { OrdersService, orderTotalCents } from './orders.service';

const BOOKS: OrderItem = { sku: 'BOOK', quantity: 2, unitPriceCents: 1500 };
const PEN: OrderItem = { sku: 'PEN', quantity: 1, unitPriceCents: 200 };
/** The most expensive item the DTO allows: 1,000 x $100,000 = 10^10 cents. */
const GOLD: OrderItem = {
  sku: 'GOLD',
  quantity: Limits.maxQuantityPerItem,
  unitPriceCents: Limits.maxUnitPriceCents,
};

/** A request body as the ValidationPipe hands it over: turned into a CreateOrderDto. */
function order(items: OrderItem[]): CreateOrderDto {
  return plainToInstance(CreateOrderDto, { customerId: 'c-1', items });
}

/** Every write succeeds and reports one row, like a healthy database. */
const healthy: Responder = () => ({ rowCount: 1 });

function setup(respond: Responder = healthy): { service: OrdersService; queries: RecordedQuery[] } {
  const { client, queries } = fakeClient(respond);
  return { service: new OrdersService(fakeDatabase(client)), queries };
}

describe('orderTotalCents', () => {
  it('adds up quantity x unit price', () => {
    expect(orderTotalCents([BOOKS, PEN])).toBe(3200);
  });

  it('is exact for the largest order the DTO allows (far above the INTEGER column)', () => {
    const biggest = Array.from({ length: Limits.maxItemsPerOrder }, () => GOLD);
    expect(orderTotalCents(biggest)).toBe(500_000_000_000);
  });
});

describe('OrdersService.create', () => {
  it('writes the order and the OrderCreated event in the same transaction', async () => {
    const { service, queries } = setup();
    const result = await service.create(order([BOOKS, PEN]));

    expect(result).toMatchObject({ status: 'PENDING', totalCents: 3200 });
    expect(queries).toHaveLength(2);
    expect(queries[0]?.sql).toContain('INSERT INTO orders');
    expect(queries[0]?.params).toEqual([result.orderId, 'c-1', 3200, JSON.stringify([BOOKS, PEN])]);
    expect(queries[1]?.sql).toContain('INSERT INTO outbox');

    const [, topic, key, payload] = queries[1]?.params ?? [];
    expect(topic).toBe('orders');
    expect(key).toBe(result.orderId); // message key = order id -> per-order ordering
    // parseEvent: the very check inventory-service will run on this message
    expect(parseEvent(JSON.parse(String(payload)))).toMatchObject({
      eventType: 'OrderCreated',
      version: 1,
      aggregateId: result.orderId,
      payload: { orderId: result.orderId, customerId: 'c-1', items: [BOOKS, PEN], totalCents: 3200 },
    });
  });

  it('refuses a total above the limit with 400, before writing anything', async () => {
    const { service, queries } = setup();

    await expect(service.create(order([GOLD]))).rejects.toThrow(BadRequestException);
    expect(queries).toEqual([]);
  });

  it('accepts a total exactly at the limit', async () => {
    const { service } = setup();
    const atLimit: OrderItem = { sku: 'GOLD', quantity: 100, unitPriceCents: Limits.maxUnitPriceCents };

    await expect(service.create(order([atLimit]))).resolves.toMatchObject({
      totalCents: Limits.maxOrderTotalCents,
    });
  });

  it('stores and publishes only the item fields the contract knows', async () => {
    const { service, queries } = setup();
    const withExtra = { ...BOOKS, giftWrap: true };

    await service.create(order([withExtra]));
    expect(queriesWith(queries, 'INSERT INTO orders')[0]?.params[3]).toBe(JSON.stringify([BOOKS]));
  });

  it('stages no event when the order insert fails (the transaction rolls back)', async () => {
    const { service, queries } = setup(() => {
      throw new Error('db down');
    });

    await expect(service.create(order([BOOKS]))).rejects.toThrow('db down');
    expect(queries).toHaveLength(1); // the outbox insert never ran
  });

  it('stops if the order insert did not write exactly one row', async () => {
    const { service, queries } = setup(() => ({ rowCount: 0 }));

    await expect(service.create(order([BOOKS]))).rejects.toThrow(InvariantError);
    expect(queriesWith(queries, 'INSERT INTO outbox')).toEqual([]);
  });

  it.each<[string, OrderItem[]]>([
    ['no items', []],
    ['more items than the limit', Array.from({ length: Limits.maxItemsPerOrder + 1 }, () => PEN)],
  ])('refuses %s, which CreateOrderDto should have rejected', async (_label, items) => {
    const { service, queries } = setup();

    await expect(service.create(order(items))).rejects.toThrow(InvariantError);
    expect(queries).toEqual([]);
  });
});

describe('OrdersService.get', () => {
  it('maps the row to the response shape', async () => {
    const row = {
      id: ORDER_ID,
      customer_id: 'c-1',
      status: 'CONFIRMED',
      total_cents: 3000,
      items: [BOOKS],
      created_at: new Date('2026-01-02T03:04:05.000Z'),
      updated_at: new Date('2026-01-02T03:04:06.000Z'),
    };
    const { service, queries } = setup(() => ({ rows: [row] }));

    await expect(service.get(ORDER_ID)).resolves.toEqual({
      orderId: ORDER_ID,
      customerId: 'c-1',
      status: 'CONFIRMED',
      totalCents: 3000,
      items: [BOOKS],
      createdAt: '2026-01-02T03:04:05.000Z',
      updatedAt: '2026-01-02T03:04:06.000Z',
    });
    expect(queries[0]?.params).toEqual([ORDER_ID]);
  });

  it('answers 404 for an order it does not have', async () => {
    const { service } = setup(() => ({ rows: [] }));

    await expect(service.get(ORDER_ID)).rejects.toThrow(NotFoundException);
  });
});
