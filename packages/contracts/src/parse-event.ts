import { EVENT_TYPES, EVENT_VERSION, EventType, OrderItem, ShopEvent } from './events';
import { Limits } from './limits';

/**
 * A message that is not a valid event. Retrying will never fix it, so consumers send it
 * straight to the dead-letter topic.
 */
export class InvalidEventError extends Error {
  override readonly name = 'InvalidEventError';
}

type Fields = Readonly<Record<string, unknown>>;

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function fail(message: string): never {
  throw new InvalidEventError(message);
}

function isFields(value: unknown): value is Fields {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function text(fields: Fields, key: string, maxLength: number): string {
  const value = fields[key];
  if (typeof value !== 'string' || value.length === 0 || value.length > maxLength) {
    fail(`${key} must be a string of 1..${maxLength} characters`);
  }
  return value;
}

function integer(fields: Fields, key: string, min: number, max: number): number {
  const value = fields[key];
  if (typeof value !== 'number' || !Number.isInteger(value) || value < min || value > max) {
    fail(`${key} must be an integer in ${min}..${max}`);
  }
  return value;
}

function orderItem(value: unknown): OrderItem {
  if (!isFields(value)) fail('items[] entries must be objects');
  return {
    sku: text(value, 'sku', Limits.maxSkuLength),
    quantity: integer(value, 'quantity', 1, Limits.maxQuantityPerItem),
    unitPriceCents: integer(value, 'unitPriceCents', 0, Limits.maxUnitPriceCents),
  };
}

function orderItems(fields: Fields): OrderItem[] {
  const value = fields['items'];
  if (!Array.isArray(value) || value.length === 0 || value.length > Limits.maxItemsPerOrder) {
    fail(`items must be an array of 1..${Limits.maxItemsPerOrder} entries`);
  }
  // Bounded loop: the length was checked just above (JPL rule 2).
  return value.map(orderItem);
}

function reason(fields: Fields): string {
  return text(fields, 'reason', Limits.maxReasonLength);
}

/** Check that the payload has every field this event type promises. Unknown extra fields are dropped. */
function parsePayload(eventType: EventType, payload: Fields, orderId: string): ShopEvent['payload'] {
  switch (eventType) {
    case 'OrderCreated':
      return {
        orderId,
        customerId: text(payload, 'customerId', Limits.maxCustomerIdLength),
        items: orderItems(payload),
        totalCents: integer(payload, 'totalCents', 0, Limits.maxOrderTotalCents),
      };
    case 'InventoryReserved':
      return { orderId, items: orderItems(payload) };
    case 'PaymentSucceeded':
      return { orderId, amountCents: integer(payload, 'amountCents', 0, Limits.maxOrderTotalCents) };
    case 'OrderCancelled':
    case 'InventoryRejected':
    case 'PaymentFailed':
      return { orderId, reason: reason(payload) };
    case 'OrderConfirmed':
      return { orderId };
  }
}

function readEventType(fields: Fields): EventType {
  const value = fields['eventType'];
  const known = EVENT_TYPES.find((type) => type === value);
  return known ?? fail(`unknown eventType ${JSON.stringify(value)}`);
}

/**
 * Validate an untrusted message from Kafka and turn it into a typed event (JPL rule 7:
 * every function checks the validity of its inputs, and the consumer is the boundary).
 * Throws InvalidEventError with the first problem found.
 */
export function parseEvent(raw: unknown): ShopEvent {
  if (!isFields(raw)) fail('event must be a JSON object');
  const type = readEventType(raw);
  if (raw['version'] !== EVENT_VERSION) fail(`unsupported version ${JSON.stringify(raw['version'])}`);
  const eventId = text(raw, 'eventId', 36);
  if (!UUID.test(eventId)) fail('eventId must be a UUID');
  const occurredAt = text(raw, 'occurredAt', 40);
  if (Number.isNaN(Date.parse(occurredAt))) fail('occurredAt must be an ISO-8601 timestamp');
  const aggregateId = text(raw, 'aggregateId', 64);
  const payload = raw['payload'];
  if (!isFields(payload)) fail('payload must be an object');
  if (payload['orderId'] !== aggregateId) fail('payload.orderId must equal aggregateId');

  const event = {
    eventId,
    eventType: type,
    version: EVENT_VERSION,
    occurredAt,
    aggregateId,
    correlationId: text(raw, 'correlationId', 64),
    payload: parsePayload(type, payload, aggregateId),
  };
  const causationId = raw['causationId'];
  if (causationId !== undefined) {
    return { ...event, causationId: text(raw, 'causationId', 64) } as ShopEvent;
  }
  return event as ShopEvent;
}
