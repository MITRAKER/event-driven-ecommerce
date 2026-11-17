import { assertNever, EventType, invariant, Limits, OrderItem, ShopEvent } from '@shop/contracts';

export type OrderStatus = 'PENDING' | 'CONFIRMED' | 'CANCELLED';
export type InventoryStatus = 'RESERVED' | 'REJECTED' | 'RELEASED';
export type PaymentStatus = 'SUCCEEDED' | 'FAILED';

/** One line of the order-tracking timeline. */
export interface TimelineEntry {
  readonly eventType: EventType;
  readonly occurredAt: string;
}

/** One order, shaped for the screens that read it: everything in one row, no joins. */
export interface OrderView {
  readonly orderId: string;
  readonly customerId: string | null;
  readonly status: OrderStatus;
  readonly totalCents: number | null;
  readonly items: readonly OrderItem[] | null;
  readonly inventoryStatus: InventoryStatus | null;
  readonly paymentStatus: PaymentStatus | null;
  readonly cancelReason: string | null;
  readonly timeline: readonly TimelineEntry[];
  readonly createdAt: string | null;
  readonly updatedAt: string;
}

/** The view of an order we have not heard about yet. `at` is when its first event happened. */
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

/** Plain code-unit comparison, not localeCompare: the same answer on every machine. */
function compareText(a: string, b: string): number {
  if (a < b) return -1;
  return a > b ? 1 : 0;
}

/**
 * Timeline order: by when the event happened, then by event type. The tie-break makes the order
 * total, so two events stamped in the same millisecond still come out the same way whatever
 * order they arrived in.
 */
export function compareEntries(a: TimelineEntry, b: TimelineEntry): number {
  const byTime = Date.parse(a.occurredAt) - Date.parse(b.occurredAt);
  return byTime !== 0 ? byTime : compareText(a.eventType, b.eventType);
}

/** InventoryReserved after the cancel already arrived: that stock has been (or is being) given back. */
function reservedStatus(view: OrderView): InventoryStatus {
  return view.status === 'CANCELLED' ? 'RELEASED' : 'RESERVED';
}

/** The inventory service releases a reservation when its order is cancelled. */
function statusAfterCancel(current: InventoryStatus | null): InventoryStatus | null {
  return current === 'RESERVED' ? 'RELEASED' : current;
}

/** The fields one event owns. Every other field is left exactly as it was. */
function applyEvent(view: OrderView, event: ShopEvent): OrderView {
  switch (event.eventType) {
    case 'OrderCreated': {
      const { customerId, items, totalCents } = event.payload;
      return { ...view, customerId, items, totalCents, createdAt: event.occurredAt };
    }
    case 'InventoryReserved':
      return { ...view, inventoryStatus: reservedStatus(view) };
    case 'InventoryRejected':
      return { ...view, inventoryStatus: 'REJECTED' };
    case 'PaymentSucceeded':
      return { ...view, paymentStatus: 'SUCCEEDED' };
    case 'PaymentFailed':
      return { ...view, paymentStatus: 'FAILED' };
    case 'OrderConfirmed':
      return { ...view, status: 'CONFIRMED' };
    case 'OrderCancelled':
      return {
        ...view,
        status: 'CANCELLED',
        cancelReason: event.payload.reason,
        inventoryStatus: statusAfterCancel(view.inventoryStatus),
      };
    default:
      // Every event type is listed above, so a new one is a compile error here until it is handled.
      return assertNever(event, 'project');
  }
}

/**
 * The projector: (current view, event) -> new view. Pure (no database, no Kafka) and it never
 * changes its input, so it is easy to test and to replay: the read model is literally
 * `events.reduce(project, emptyView(...))`.
 *
 * Events come from three topics, and Kafka only orders messages WITHIN a partition, so this
 * service may see InventoryReserved before OrderCreated. Any arrival order gives the same view:
 *   - each event only fills in the fields it owns (OrderCreated never touches paymentStatus)
 *   - a final status (CONFIRMED/CANCELLED) is never set back to PENDING
 *   - the timeline is sorted by occurredAt (then event type), not by arrival
 *   - updatedAt is the latest occurredAt seen, whichever event brought it
 */
export function project(view: OrderView, event: ShopEvent): OrderView {
  invariant(
    event.aggregateId === view.orderId,
    `project: event ${event.eventId} is for order ${event.aggregateId}, not ${view.orderId}`,
  );
  const entry: TimelineEntry = { eventType: event.eventType, occurredAt: event.occurredAt };
  const timeline = [...view.timeline, entry].sort(compareEntries);
  // A normal order has 4 events. Far more means a bug or a flood of forged events: stop, don't grow.
  invariant(
    timeline.length <= Limits.maxEventsPerOrder,
    `project: order ${view.orderId} would have more than ${Limits.maxEventsPerOrder} events`,
  );
  const isLatest = Date.parse(event.occurredAt) > Date.parse(view.updatedAt);
  return { ...applyEvent(view, event), timeline, updatedAt: isLatest ? event.occurredAt : view.updatedAt };
}
