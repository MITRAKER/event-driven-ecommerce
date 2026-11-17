import { randomUUID } from 'node:crypto';

/** Kafka topics. One topic per producing service ("who owns the facts"). */
export const Topics = {
  Orders: 'orders',
  Inventory: 'inventory',
  Payments: 'payments',
} as const;

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
export type OrderCreated = EventEnvelope<'OrderCreated', {
  orderId: string;
  customerId: string;
  items: OrderItem[];
  totalCents: number;
}>;
export type OrderConfirmed = EventEnvelope<'OrderConfirmed', { orderId: string }>;
export type OrderCancelled = EventEnvelope<'OrderCancelled', { orderId: string; reason: string }>;

// ---- Inventory events (producer: inventory-service)
export type InventoryReserved = EventEnvelope<'InventoryReserved', { orderId: string; items: OrderItem[] }>;
export type InventoryRejected = EventEnvelope<'InventoryRejected', { orderId: string; reason: string }>;

// ---- Payment events (producer: payment-service)
export type PaymentSucceeded = EventEnvelope<'PaymentSucceeded', { orderId: string; amountCents: number }>;
export type PaymentFailed = EventEnvelope<'PaymentFailed', { orderId: string; reason: string }>;

export type ShopEvent =
  | OrderCreated | OrderConfirmed | OrderCancelled
  | InventoryReserved | InventoryRejected
  | PaymentSucceeded | PaymentFailed;

/** Build an envelope. Pass the event that caused this one to keep the trace chain intact. */
export function createEvent<E extends ShopEvent>(
  eventType: E['eventType'],
  aggregateId: string,
  payload: E['payload'],
  cause?: Pick<EventEnvelope, 'eventId' | 'correlationId'>,
): E {
  return {
    eventId: randomUUID(),
    eventType,
    version: 1,
    occurredAt: new Date().toISOString(),
    aggregateId,
    correlationId: cause?.correlationId ?? aggregateId,
    causationId: cause?.eventId,
    payload,
  } as E;
}
