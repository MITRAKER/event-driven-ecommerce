import { OrderItem, ShopEvent } from '@shop/contracts';

export interface OrderView {
  orderId: string;
  customerId: string | null;
  status: 'PENDING' | 'CONFIRMED' | 'CANCELLED';
  totalCents: number | null;
  items: OrderItem[] | null;
  inventoryStatus: 'RESERVED' | 'REJECTED' | 'RELEASED' | null;
  paymentStatus: 'SUCCEEDED' | 'FAILED' | null;
  cancelReason: string | null;
  timeline: { eventType: string; occurredAt: string }[];
  createdAt: string | null;
  updatedAt: string;
}

export function emptyView(orderId: string, at: string): OrderView {
  return {
    orderId,
    customerId: null,
    status: 'PENDING',
    totalCents: null,
    items: null,
    inventoryStatus: null,
    paymentStatus: null,
    cancelReason: null,
    timeline: [],
    createdAt: null,
    updatedAt: at,
  };
}

/**
 * The projector: (current view, event) -> new view. Pure, so it's easy to test and to replay.
 *
 * Events come from three topics, and Kafka only orders messages WITHIN a partition. So this
 * service may see InventoryReserved before OrderCreated. The projector is written so that
 * any arrival order gives the same final view:
 *   - each event only fills in the fields it owns (OrderCreated never touches paymentStatus)
 *   - a final status (CONFIRMED/CANCELLED) is never overwritten by PENDING
 *   - the timeline is kept sorted by occurredAt, not by arrival
 */
export function project(view: OrderView, event: ShopEvent): OrderView {
  const v: OrderView = { ...view, timeline: [...view.timeline] };

  switch (event.eventType) {
    case 'OrderCreated':
      v.customerId = event.payload.customerId;
      v.items = event.payload.items;
      v.totalCents = event.payload.totalCents;
      v.createdAt = event.occurredAt;
      break;
    case 'InventoryReserved':
      // If the cancel already arrived, this reservation has been (or will be) released.
      v.inventoryStatus = v.status === 'CANCELLED' ? 'RELEASED' : 'RESERVED';
      break;
    case 'InventoryRejected':
      v.inventoryStatus = 'REJECTED';
      break;
    case 'PaymentSucceeded':
      v.paymentStatus = 'SUCCEEDED';
      break;
    case 'PaymentFailed':
      v.paymentStatus = 'FAILED';
      break;
    case 'OrderConfirmed':
      v.status = 'CONFIRMED';
      break;
    case 'OrderCancelled':
      v.status = 'CANCELLED';
      v.cancelReason = event.payload.reason;
      // The inventory service releases a reservation when an order is cancelled.
      if (v.inventoryStatus === 'RESERVED') v.inventoryStatus = 'RELEASED';
      break;
  }

  v.timeline.push({ eventType: event.eventType, occurredAt: event.occurredAt });
  v.timeline.sort((a, b) => a.occurredAt.localeCompare(b.occurredAt));
  if (event.occurredAt > v.updatedAt) v.updatedAt = event.occurredAt;
  return v;
}
