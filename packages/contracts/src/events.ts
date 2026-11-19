import { randomUUID } from 'node:crypto';
import { invariant } from './invariant';

/** Kafka topics. One topic per producing service ("who owns the facts"). */
export const Topics = {
  Orders: 'orders',
  Inventory: 'inventory',
  Payments: 'payments',
} as const;
export type Topic = (typeof Topics)[keyof typeof Topics];

/** Every message on every topic has this shape. See contracts/events/envelope.schema.json. */
export interface EventEnvelope<TType extends string = string, TPayload = unknown> {
  eventId: string;
  eventType: TType;
  version: number;
  occurredAt: string;
  aggregateId: string;
  correlationId: string;
  causationId?: string;
  payload: TPayload;
}

export interface OrderItem {
  sku: string;
  quantity: number;
  unitPriceCents: number;
}

// ---- Order events (producer: order-service)
export type OrderCreated = EventEnvelope<
  'OrderCreated',
  { orderId: string; customerId: string; items: OrderItem[]; totalCents: number }
>;
export type OrderConfirmed = EventEnvelope<'OrderConfirmed', { orderId: string }>;
export type OrderCancelled = EventEnvelope<'OrderCancelled', { orderId: string; reason: string }>;

// ---- Inventory events (producer: inventory-service)
export type InventoryReserved = EventEnvelope<'InventoryReserved', { orderId: string; items: OrderItem[] }>;
export type InventoryRejected = EventEnvelope<'InventoryRejected', { orderId: string; reason: string }>;

// ---- Payment events (producer: payment-service)
export type PaymentSucceeded = EventEnvelope<'PaymentSucceeded', { orderId: string; amountCents: number }>;
export type PaymentFailed = EventEnvelope<'PaymentFailed', { orderId: string; reason: string }>;

export type ShopEvent =
  | OrderCreated
  | OrderConfirmed
  | OrderCancelled
  | InventoryReserved
  | InventoryRejected
  | PaymentSucceeded
  | PaymentFailed;

export type EventType = ShopEvent['eventType'];

export const EVENT_TYPES: readonly EventType[] = [
  'OrderCreated',
  'OrderConfirmed',
  'OrderCancelled',
  'InventoryReserved',
  'InventoryRejected',
  'PaymentSucceeded',
  'PaymentFailed',
];

/** The current schema version of every event. Bump it (and keep reading the old one) when a payload changes. */
export const EVENT_VERSION = 1;

/** Build an envelope. Pass the event that caused this one to keep the trace chain intact. */
export function createEvent<E extends ShopEvent>(
  eventType: E['eventType'],
  aggregateId: string,
  payload: E['payload'],
  cause?: Pick<EventEnvelope, 'eventId' | 'correlationId'>,
): E {
  invariant(aggregateId.length > 0, 'createEvent: aggregateId must not be empty');
  const envelope: EventEnvelope<E['eventType'], E['payload']> = {
    eventId: randomUUID(),
    eventType,
    version: EVENT_VERSION,
    occurredAt: new Date().toISOString(),
    aggregateId,
    correlationId: cause?.correlationId ?? aggregateId,
    payload,
  };
  if (cause !== undefined) {
    envelope.causationId = cause.eventId;
  }
  return envelope as E;
}
