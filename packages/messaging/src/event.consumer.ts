import { Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy } from '@nestjs/common';
import { Consumer } from 'kafkajs';
import { PoolClient } from 'pg';
import { ShopEvent } from '@shop/contracts';
import { Database } from './database';
import { claimEvent } from './inbox';
import { kafka } from './kafka';

export type EventHandler = (event: ShopEvent, tx: PoolClient) => Promise<void>;

/**
 * Subscribe to topics and route each event to a handler, exactly once per service:
 *   BEGIN -> claim eventId -> handler (business change + outbox rows) -> COMMIT -> commit offset
 * Duplicates are skipped by claimEvent(). Unknown event types are ignored, which lets
 * producers add new events without breaking old consumers.
 */
@Injectable()
export abstract class EventConsumer implements OnApplicationBootstrap, OnModuleDestroy {
  protected readonly logger = new Logger(this.constructor.name);
  private consumer?: Consumer;

  /** Consumer group id: all replicas of one service share it. */
  protected abstract readonly groupId: string;
  protected abstract readonly topics: string[];
  /** eventType -> handler. */
  protected abstract readonly handlers: Partial<Record<ShopEvent['eventType'], EventHandler>>;

  constructor(protected readonly db: Database) {}

  async onApplicationBootstrap() {
    this.consumer = kafka.consumer({ groupId: this.groupId });
    await this.consumer.connect();
    await this.consumer.subscribe({ topics: this.topics, fromBeginning: true });
    await this.consumer.run({
      eachMessage: async ({ message }) => {
        if (!message.value) return;
        await this.process(JSON.parse(message.value.toString()) as ShopEvent);
      },
    });
  }

  async onModuleDestroy() {
    await this.consumer?.disconnect();
  }

  /** Public so tests can feed events straight in. Returns 'handled' | 'duplicate' | 'ignored'. */
  async process(event: ShopEvent): Promise<'handled' | 'duplicate' | 'ignored'> {
    const handler = this.handlers[event.eventType];
    if (!handler) return 'ignored';
    return this.db.transaction(async (tx) => {
      if (!(await claimEvent(tx, event))) {
        this.logger.warn(`duplicate ${event.eventType} ${event.eventId} skipped`);
        return 'duplicate';
      }
      await handler(event, tx);
      this.logger.log(`handled ${event.eventType} for ${event.aggregateId}`);
      return 'handled';
    });
    // If the handler throws, the transaction rolls back (claim included) and the error
    // propagates to kafkajs, which does not commit the offset and retries the message.
  }
}
