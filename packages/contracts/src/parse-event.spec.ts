import { createEvent, OrderCreated, PaymentFailed } from './events';
import { InvalidEventError, parseEvent } from './parse-event';

const created = createEvent<OrderCreated>('OrderCreated', 'order-1', {
  orderId: 'order-1',
  customerId: 'c-1',
  items: [{ sku: 'BOOK', quantity: 2, unitPriceCents: 1500 }],
  totalCents: 3000,
});

/** Round-trip through JSON, as Kafka does, then tweak one field. */
function wire(overrides: Record<string, unknown> = {}, payload: Record<string, unknown> = {}): unknown {
  const json = JSON.parse(JSON.stringify(created)) as Record<string, unknown>;
  const mergedPayload = { ...(json['payload'] as object), ...payload };
  return { ...json, payload: mergedPayload, ...overrides };
}

describe('parseEvent', () => {
  it('accepts a valid event and returns it typed', () => {
    expect(parseEvent(wire())).toEqual(created);
  });

  it('keeps causationId when present', () => {
    const failed = createEvent<PaymentFailed>(
      'PaymentFailed',
      'order-1',
      { orderId: 'order-1', reason: 'x' },
      created,
    );
    expect(parseEvent(JSON.parse(JSON.stringify(failed)))).toEqual(failed);
  });

  it('drops unknown payload fields', () => {
    const parsed = parseEvent(wire({}, { surprise: true }));
    expect(parsed.payload).not.toHaveProperty('surprise');
  });

  it.each([
    ['not an object', 'hello', 'event must be a JSON object'],
    ['unknown type', wire({ eventType: 'OrderTeleported' }), 'unknown eventType'],
    ['future version', wire({ version: 2 }), 'unsupported version'],
    ['bad eventId', wire({ eventId: 'nope' }), 'eventId must be a UUID'],
    ['bad timestamp', wire({ occurredAt: 'yesterday' }), 'occurredAt'],
    ['payload not an object', wire({ payload: [] }), 'payload must be an object'],
    ['orderId mismatch', wire({}, { orderId: 'someone-else' }), 'payload.orderId must equal aggregateId'],
    ['empty items', wire({}, { items: [] }), 'items must be an array'],
    ['zero quantity', wire({}, { items: [{ sku: 'A', quantity: 0, unitPriceCents: 1 }] }), 'quantity'],
    [
      'fractional price',
      wire({}, { items: [{ sku: 'A', quantity: 1, unitPriceCents: 1.5 }] }),
      'unitPriceCents',
    ],
    ['item not an object', wire({}, { items: ['A'] }), 'entries must be objects'],
    ['missing customer', wire({}, { customerId: '' }), 'customerId'],
  ])('rejects %s', (_label, raw, message) => {
    expect(() => parseEvent(raw)).toThrow(InvalidEventError);
    expect(() => parseEvent(raw)).toThrow(message);
  });
});
