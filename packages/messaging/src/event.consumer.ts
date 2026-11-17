import { Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy } from '@nestjs/common';
import { Consumer, KafkaMessage, Producer } from 'kafkajs';
import { PoolClient } from 'pg';
import { ShopEvent } from '@shop/contracts';
import { Database } from './database';
import { claimEvent } from './inbox';
import { dlqTopic, ensureTopics, kafka } from './kafka';
import { withRetry } from './retry';

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
  private dlqProducer?: Producer;
  /** In-process retries before a message is parked in the dead-letter topic. */
  protected retryAttempts = Number(process.env.CONSUMER_RETRY_ATTEMPTS ?? 3);
  protected retryBaseDelayMs = Number(process.env.CONSUMER_RETRY_BASE_MS ?? 200);

  /** Consumer group id: all replicas of one service share it. */
  protected abstract readonly groupId: string;
  protected abstract readonly topics: string[];
  /** eventType -> handler. */
  protected abstract readonly handlers: Partial<Record<ShopEvent['eventType'], EventHandler>>;

  constructor(protected readonly db: Database) {}

  async onApplicationBootstrap() {
    await ensureTopics();
    this.dlqProducer = kafka.producer();
    await this.dlqProducer.connect();
    this.consumer = kafka.consumer({ groupId: this.groupId });
    await this.consumer.connect();
    await this.consumer.subscribe({ topics: this.topics, fromBeginning: true });
    await this.consumer.run({
      eachMessage: ({ topic, partition, message }) => this.receive(topic, partition, message),
    });
  }

  async onModuleDestroy() {
    await this.consumer?.disconnect();
    await this.dlqProducer?.disconnect();
  }

  /**
   * Parse -> process with retries -> on final failure, park the message in <topic>.dlq.
   * Without the DLQ, one bad message would be retried forever and block its whole
   * partition (every order behind it would be stuck). That's called a "poison pill".
   */
  private async receive(topic: string, partition: number, message: KafkaMessage) {
    if (!message.value) return;
    let event: ShopEvent;
    try {
      event = JSON.parse(message.value.toString());
    } catch (err) {
      return this.deadLetter(topic, partition, message, err);
    }
    try {
      await withRetry(() => this.process(event), {
        attempts: this.retryAttempts,
        baseDelayMs: this.retryBaseDelayMs,
        onRetry: (n, err) => this.logger.warn(`retry ${n} for ${event.eventType} ${event.eventId}: ${(err as Error).message}`),
      });
    } catch (err) {
      await this.deadLetter(topic, partition, message, err);
    }
  }

  private async deadLetter(topic: string, partition: number, message: KafkaMessage, err: unknown) {
    this.logger.error(`dead-lettering message from ${topic}[${partition}]@${message.offset}: ${(err as Error).message}`);
    await this.dlqProducer!.send({
      topic: dlqTopic(topic),
      messages: [{
        key: message.key,
        value: message.value,
        headers: {
          ...message.headers,
          'dlq-error': String((err as Error)?.message ?? err),
          'dlq-original-topic': topic,
          'dlq-original-partition': String(partition),
          'dlq-original-offset': message.offset,
          'dlq-consumer-group': this.groupId,
          'dlq-failed-at': new Date().toISOString(),
        },
      }],
    });
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
    // propagates to receive(), which retries with backoff and finally dead-letters it.
  }
}
