import { Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy } from '@nestjs/common';
import { invariant } from '@shop/contracts';
import { PoolClient } from 'pg';
import { MessagingConfig } from './config';
import { Database } from './database';
import { KafkaProducer } from './kafka';

interface OutboxRow {
  id: string; // BIGSERIAL comes back from pg as a string
  topic: string;
  message_key: string;
  payload: unknown; // JSONB, already parsed by pg
}

/** Group rows by topic, keeping their order, so each topic gets one send call. */
export function groupByTopic(
  rows: readonly OutboxRow[],
): { topic: string; messages: { key: string; value: string }[] }[] {
  const byTopic = new Map<string, { key: string; value: string }[]>();
  for (const row of rows) {
    const list = byTopic.get(row.topic) ?? [];
    list.push({ key: row.message_key, value: JSON.stringify(row.payload) });
    byTopic.set(row.topic, list);
  }
  return [...byTopic].map(([topic, messages]) => ({ topic, messages }));
}

/**
 * The outbox relay: polls the outbox table and publishes rows to Kafka.
 *
 * Guarantee: at-least-once. If we crash after Kafka accepted the message but before
 * we mark the row published, the row is sent again on restart. That is why every
 * consumer must be idempotent (dedupe on eventId).
 *
 * The polling timer is the one loop that is meant to run forever (the JPL rules allow
 * that for a scheduler). Each tick is bounded: at most `outboxBatchSize` rows.
 */
@Injectable()
export class OutboxRelay implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly logger = new Logger(OutboxRelay.name);
  private timer: NodeJS.Timeout | undefined;
  private running = false;
  private inFlight: Promise<number> = Promise.resolve(0);

  constructor(
    private readonly db: Database,
    private readonly kafka: KafkaProducer,
    private readonly config: MessagingConfig,
  ) {}

  onApplicationBootstrap(): void {
    this.timer = setInterval(() => {
      this.inFlight = this.tick();
    }, this.config.settings.outboxPollMs);
  }

  /** Stop polling and let a tick that is already running finish its batch. */
  async onModuleDestroy(): Promise<void> {
    clearInterval(this.timer);
    await this.inFlight;
  }

  /** Publish one batch. Public so tests can drive it directly. Returns how many rows were sent. */
  async tick(): Promise<number> {
    if (this.running) return 0; // don't overlap with a slow previous tick
    this.running = true;
    try {
      return await this.db.transaction(async (tx) => await this.publishBatch(tx));
    } catch (err) {
      this.logger.error(
        `publish failed, will retry next tick: ${err instanceof Error ? err.message : String(err)}`,
      );
      return 0;
    } finally {
      this.running = false;
    }
  }

  private async publishBatch(tx: PoolClient): Promise<number> {
    const batchSize = this.config.settings.outboxBatchSize;
    // FOR UPDATE SKIP LOCKED: if you run 3 replicas of this service, each relay grabs a
    // different batch instead of all of them sending the same rows.
    const { rows } = await tx.query<OutboxRow>(
      `SELECT id, topic, message_key, payload FROM outbox
        WHERE published_at IS NULL
        ORDER BY id
        LIMIT $1
        FOR UPDATE SKIP LOCKED`,
      [batchSize],
    );
    invariant(rows.length <= batchSize, `outbox query returned ${rows.length} rows, limit was ${batchSize}`);
    if (rows.length === 0) return 0;

    await this.kafka.producer.sendBatch({ topicMessages: groupByTopic(rows) });

    const updated = await tx.query(`UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, [
      rows.map((r) => r.id),
    ]);
    invariant(
      updated.rowCount === rows.length,
      `marked ${updated.rowCount} of ${rows.length} rows published`,
    );
    this.logger.log(`published ${rows.length} event(s)`);
    return rows.length;
  }
}
