import { Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy } from '@nestjs/common';
import { EventType, InvalidEventError, parseEvent, peekEventType, ShopEvent, Topic } from '@shop/contracts';
import { Consumer, EachMessagePayload, KafkaMessage, Partitioners, Producer } from 'kafkajs';
import { PoolClient } from 'pg';
import { MessagingConfig } from './config';
import { Database } from './database';
import { classifyFailure, RedeliverLaterError } from './errors';
import { claimEvent } from './inbox';
import { dlqTopic, KafkaClient } from './kafka';
import { withRetry } from './retry';

export type ProcessResult = 'handled' | 'duplicate' | 'ignored';

/** One message as kafkajs hands it to eachMessage (pause() left out: we don't use it). */
export type Delivery = Pick<EachMessagePayload, 'topic' | 'partition' | 'message' | 'heartbeat'>;

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/**
 * Carries an error from Kafka's heartbeat() past the failure handling in receive(), which hands
 * it back to kafkajs unchanged. It usually means the group is rebalancing: kafkajs rejoins, and
 * since this message's offset was never committed, the message is delivered again.
 */
class HeartbeatFailed extends Error {
  override readonly name = 'HeartbeatFailed';

  constructor(readonly original: unknown) {
    super(`Kafka heartbeat failed: ${errorMessage(original)}`);
  }
}

/**
 * Decode a raw Kafka message into an event this consumer handles. Returns undefined for a
 * message of another type, including a type newer than this code: skipping it before
 * parseEvent() (which only knows today's types) is what lets producers add events without
 * breaking old consumers. Anything else that is not a valid event becomes an InvalidEventError.
 */
export function decodeMessage(value: Buffer, handledTypes: ReadonlySet<string>): ShopEvent | undefined {
  let raw: unknown;
  try {
    raw = JSON.parse(value.toString('utf8'));
  } catch {
    throw new InvalidEventError('message is not valid JSON');
  }
  const type = peekEventType(raw);
  if (type !== undefined && !handledTypes.has(type)) return undefined;
  return parseEvent(raw);
}

/**
 * Subscribe to topics and process each event exactly once per service:
 *   BEGIN -> claim eventId -> handle() (business change + outbox rows) -> COMMIT -> commit offset
 *
 * Subclasses declare which event types they handle and implement handle() with a plain
 * switch (JPL rule 9: no tables of function pointers, so the call graph stays static).
 * Duplicates are skipped by claimEvent(). Event types not in handledTypes are skipped
 * without being parsed, even types this code has never heard of, which lets producers add
 * new events without breaking old consumers (see decodeMessage()).
 *
 * @Injectable() here makes TypeScript record this constructor's parameter types. Nest reads
 * them through the prototype chain, so a subclass that needs nothing extra can skip writing
 * a constructor at all.
 */
@Injectable()
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
    // The same partitioner as KafkaProducer. Naming it also stops kafkajs from logging a warning
    // about the default it switched to in v2.
    this.dlqProducer = this.kafka.kafka.producer({
      retry: { retries: 5 },
      createPartitioner: Partitioners.DefaultPartitioner,
    });
    await this.dlqProducer.connect();
    this.consumer = this.kafka.kafka.consumer({ groupId: this.groupId });
    await this.consumer.connect();
    await this.consumer.subscribe({ topics: [...this.topics], fromBeginning: true });
    await this.consumer.run({
      eachMessage: async (delivery) => {
        await this.receive(delivery);
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
   * - Kafka refused a heartbeat between two tries (usually a rebalance): that error goes back
   *   to kafkajs unchanged, so it can rejoin the group. The message comes back later too.
   * If even the DLQ send fails, that error also reaches kafkajs. A message is never dropped silently.
   */
  async receive(delivery: Delivery): Promise<void> {
    const { message } = delivery;
    if (message.value === null) return; // tombstone: nothing to process
    let event: ShopEvent | undefined;
    try {
      event = decodeMessage(message.value, this.handledTypes);
    } catch (err) {
      await this.deadLetter(delivery, err);
      return;
    }
    if (event === undefined) return; // a type this service doesn't handle
    await this.processWithRetries(event, delivery);
  }

  private async processWithRetries(event: ShopEvent, delivery: Delivery): Promise<void> {
    try {
      await withRetry(async () => await this.process(event), {
        attempts: this.config.settings.consumerRetryAttempts,
        baseDelayMs: this.config.settings.consumerRetryBaseMs,
        isPermanent: (err) => classifyFailure(err) === 'permanent',
        onRetry: (n, err) => {
          this.logger.warn(`retry ${n} for ${event.eventType} ${event.eventId}: ${errorMessage(err)}`);
        },
        // kafkajs only heartbeats between messages. Retrying one message can outlast the 30 s
        // session timeout, and Kafka would then drop this consumer from the group mid-retry.
        beforeWait: async () => {
          try {
            await delivery.heartbeat();
          } catch (err) {
            throw new HeartbeatFailed(err);
          }
        },
      });
    } catch (err) {
      if (err instanceof HeartbeatFailed) throw err.original;
      if (classifyFailure(err) === 'infrastructure') {
        throw new RedeliverLaterError(
          `${event.eventType} ${event.eventId} will be redelivered: ${errorMessage(err)}`,
        );
      }
      await this.deadLetter(delivery, err);
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
