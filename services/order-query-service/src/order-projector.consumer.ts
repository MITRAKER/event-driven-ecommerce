import { Injectable } from '@nestjs/common';
import { assertNever, EVENT_TYPES, EventType, ShopEvent, Topic, Topics } from '@shop/contracts';
import { Database, EventConsumer, KafkaClient, MessagingConfig, readString } from '@shop/messaging';
import { PoolClient } from 'pg';
import { OrderViewRepository } from './order-view.repository';
import { project } from './projection';

/**
 * Listens to EVERY topic and folds each event into order_view, the read model. This service never
 * publishes anything and never changes another service's data: it only reads.
 *
 * Rebuilding the view (after fixing a projector bug, or to add a column):
 *   1. Stop the service and run: TRUNCATE order_view, processed_events;
 *      processed_events too: it remembers every event id already applied, so if it stayed, every
 *      replayed event would be skipped as a duplicate and the view would stay empty.
 *   2. Start it with a new PROJECTION_GROUP_ID, e.g. order-query-v2. A new consumer group has no
 *      saved offsets, so it reads every topic from the beginning and rebuilds every order.
 */
@Injectable()
export class OrderProjector extends EventConsumer {
  protected readonly groupId: string;
  protected readonly topics: readonly Topic[] = [Topics.Orders, Topics.Inventory, Topics.Payments];
  protected readonly handledTypes: ReadonlySet<EventType> = new Set<EventType>(EVENT_TYPES);

  constructor(
    db: Database,
    kafka: KafkaClient,
    config: MessagingConfig,
    private readonly views: OrderViewRepository,
  ) {
    super(db, kafka, config);
    // The read side's only extra setting. Read once, here, because the constructor already has the
    // 4 parameters JPL rule 4 allows.
    this.groupId = readString(process.env, 'PROJECTION_GROUP_ID', 'order-query-v1');
  }

  protected async handle(event: ShopEvent, tx: PoolClient): Promise<void> {
    switch (event.eventType) {
      case 'OrderCreated':
      case 'OrderConfirmed':
      case 'OrderCancelled':
      case 'InventoryReserved':
      case 'InventoryRejected':
      case 'PaymentSucceeded':
      case 'PaymentFailed':
        await this.apply(event, tx);
        return;
      default:
        // A new event type is a compile error here until someone decides how the view shows it.
        assertNever(event, 'OrderProjector.handle');
    }
  }

  /** Load and lock the order's view, fold the event in, write it back: all in the consumer's transaction. */
  private async apply(event: ShopEvent, tx: PoolClient): Promise<void> {
    const current = await this.views.loadForUpdate(tx, event.aggregateId, event.occurredAt);
    await this.views.save(tx, project(current, event));
  }
}
