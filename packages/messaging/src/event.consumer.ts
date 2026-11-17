import { Logger, OnApplicationBootstrap, OnModuleDestroy } from '@nestjs/common';
import { EventType, InvalidEventError, parseEvent, ShopEvent, Topic } from '@shop/contracts';
import { Consumer, KafkaMessage, Producer } from 'kafkajs';
import { PoolClient } from 'pg';
import { MessagingConfig } from './config';
import { Database } from './database';
import { classifyFailure, RedeliverLaterError } from './errors';
import { claimEvent } from './inbox';
import { dlqTopic, KafkaClient } from './kafka';
import { withRetry } from './retry';

export type ProcessResult = 'handled' | 'duplicate' | 'ignored';

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/** Decode a raw Kafka message. Anything that is not a valid event becomes an InvalidEventError. */
export function decodeMessage(value: Buffer): ShopEvent {
  let raw: unknown;
  try {
    raw = JSON.parse(value.toString('utf8'));
  } catch {
    throw new InvalidEventError('message is not valid JSON');
  }
  return parseEvent(raw);
}

/**
 * Subscribe to topics and process each event exactly once per service:
 *   BEGIN -> claim eventId -> handle() (business change + outbox rows) -> COMMIT -> commit offset
 *
 * Subclasses declare which event types they handle and implement handle() with a plain
 * switch (JPL rule 9: no tables of function pointers, so the call graph stays static).
 * Duplicates are skipped by claimEvent(); event types not in handledTypes are ignored,
 * which lets producers add new events without breaking old consumers.
 */
export abstract class EventConsumer implements OnApplicationBootstrap, OnModuleDestroy {
  protected readonly logger = new Logger(this.constructor.name);
  private consumer: Consumer | undefined;
  private dlqProducer: Producer | undefined;

  /** Consumer group id: all replicas of one service share it. */
  protected abstract readonly groupId: string;
  protected abstract readonly topics: readonly Topic[];
  protected abstract readonly handledTypes: ReadonlySet<EventType>;

  /** Apply one event inside the given transaction. Throw to roll back and retry. */
  protected abstract handle(event: ShopEvent, tx: PoolClient): Promise<void>;

  constructor(
    protected readonly db: Database,
    protected readonly kafka: KafkaClient,
    protected readonly config: MessagingConfig,
  ) {}

  async onApplicationBootstrap(): Promise<void> {
    await this.kafka.ensureTopics();
    this.dlqProducer = this.kafka.kafka.producer({ retry: { retries: 5 } });
    await this.dlqProducer.connect();
    this.consumer = this.kafka.kafka.consumer({ groupId: this.groupId });
    await this.consumer.connect();
    await this.consumer.subscribe({ topics: [...this.topics], fromBeginning: true });
    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        await this.receive(topic, partition, message);
      },
    });
  }

  async onModuleDestroy(): Promise<void> {
    // Stop consuming first, so nothing new starts while the DB pool is being closed.
    await this.consumer?.disconnect();
    await this.dlqProducer?.disconnect();
  }

  /** Process one already-decoded event. Public so tests can feed events straight in. */
  async process(event: ShopEvent): Promise<ProcessResult> {
    if (!this.handledTypes.has(event.eventType)) return 'ignored';
    return await this.db.transaction(async (tx) => {
      if (!(await claimEvent(tx, event))) {
        this.logger.warn(`duplicate ${event.eventType} ${event.eventId} skipped`);
        return 'duplicate';
      }
      await this.handle(event, tx);
      this.logger.log(`handled ${event.eventType} for ${event.aggregateId}`);
      return 'handled';
    });
  }

  /**
   * Decode -> process with bounded retries -> decide what to do with a failure:
   * - permanent (bad message) or unknown (a bug, after retries): park it in <topic>.dlq.
   *   Without the DLQ, one bad message would be retried forever and block its whole
   *   partition (every order behind it would be stuck): a "poison pill".
   * - infrastructure (database down): throw, so kafkajs does not commit the offset and
   *   redelivers the message later. Healthy events are never dead-lettered just because
   *   a dependency had a bad minute.
   * If even the DLQ send fails, that error also reaches kafkajs. A message is never dropped silently.
   */
  async receive(topic: string, partition: number, message: KafkaMessage): Promise<void> {
    if (message.value === null) return; // tombstone: nothing to process
    const source = { topic, partition, message };
    let event: ShopEvent;
    try {
      event = decodeMessage(message.value);
    } catch (err) {
      await this.deadLetter(source, err);
      return;
    }
    try {
      await withRetry(async () => await this.process(event), {
        attempts: this.config.settings.consumerRetryAttempts,
        baseDelayMs: this.config.settings.consumerRetryBaseMs,
        isPermanent: (err) => classifyFailure(err) === 'permanent',
        onRetry: (n, err) => {
          this.logger.warn(`retry ${n} for ${event.eventType} ${event.eventId}: ${errorMessage(err)}`);
        },
      });
    } catch (err) {
      if (classifyFailure(err) === 'infrastructure') {
        throw new RedeliverLaterError(
          `${event.eventType} ${event.eventId} will be redelivered: ${errorMessage(err)}`,
        );
      }
      await this.deadLetter(source, err);
    }
  }

  private async deadLetter(
    source: { topic: string; partition: number; message: KafkaMessage },
    err: unknown,
  ): Promise<void> {
    const { topic, partition, message } = source;
    this.logger.error(`dead-lettering ${topic}[${partition}]@${message.offset}: ${errorMessage(err)}`);
    if (this.dlqProducer === undefined) throw new Error('dead-letter producer is not connected');
    await this.dlqProducer.send({
      topic: dlqTopic(topic),
      messages: [
        {
          key: message.key,
          value: message.value,
          headers: {
            ...message.headers,
            'dlq-error': errorMessage(err),
            'dlq-original-topic': topic,
            'dlq-original-partition': String(partition),
            'dlq-original-offset': message.offset,
            'dlq-consumer-group': this.groupId,
            'dlq-failed-at': new Date().toISOString(),
          },
        },
      ],
    });
  }
}
