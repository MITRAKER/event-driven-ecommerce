import { Injectable } from '@nestjs/common';
import { Topics } from '@shop/contracts';
import { Database, EventConsumer, EventHandler } from '@shop/messaging';
import { OrderViewRepository } from './order-view.repository';
import { project } from './projection';

/**
 * Listens to EVERY topic and folds the events into order_view.
 * This service never publishes anything and never changes another service's data: it only reads.
 *
 * Rebuild trick: change groupId (e.g. 'order-query-v2'), truncate order_view and restart.
 * The consumer starts from the beginning of every topic and rebuilds the whole view.
 */
@Injectable()
export class OrderProjector extends EventConsumer {
  protected readonly groupId = process.env.PROJECTION_GROUP_ID ?? 'order-query-v1';
  protected readonly topics = [Topics.Orders, Topics.Inventory, Topics.Payments];

  private readonly apply: EventHandler = async (event, tx) => {
    const current = await this.views.loadForUpdate(tx, event.aggregateId, event.occurredAt);
    await this.views.save(tx, project(current, event));
  };

  protected readonly handlers = {
    OrderCreated: this.apply,
    OrderConfirmed: this.apply,
    OrderCancelled: this.apply,
    InventoryReserved: this.apply,
    InventoryRejected: this.apply,
    PaymentSucceeded: this.apply,
    PaymentFailed: this.apply,
  };

  constructor(
    db: Database,
    private readonly views: OrderViewRepository,
  ) {
    super(db);
  }
}
