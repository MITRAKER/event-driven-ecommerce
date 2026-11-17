import { Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy } from '@nestjs/common';
import { Database } from './database';
import { KafkaProducer } from './kafka';

/**
 * The outbox relay: polls the outbox table and publishes rows to Kafka.
 *
 * Guarantee: at-least-once. If we crash after Kafka accepted the message but before
 * we mark the row published, the row is sent again on restart. That is why every
 * consumer must be idempotent (dedupe on eventId).
 */
@Injectable()
export class OutboxRelay implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly logger = new Logger(OutboxRelay.name);
  private timer?: NodeJS.Timeout;
  private running = false;

  constructor(private readonly db: Database, private readonly kafka: KafkaProducer) {}

  onApplicationBootstrap() {
    const interval = Number(process.env.OUTBOX_POLL_MS ?? 500);
    this.timer = setInterval(() => void this.tick(), interval);
  }

  onModuleDestroy() {
    clearInterval(this.timer);
  }

  /** Publish one batch. Public so tests can drive it directly. Returns how many rows were sent. */
  async tick(batchSize = 100): Promise<number> {
    if (this.running) return 0; // don't overlap with a slow previous tick
    this.running = true;
    try {
      return await this.db.transaction(async (client) => {
        // FOR UPDATE SKIP LOCKED: if you run 3 replicas of this service, each relay
        // grabs a different batch instead of all sending the same rows.
        const { rows } = await client.query(
          `SELECT id, topic, message_key, payload FROM outbox
            WHERE published_at IS NULL
            ORDER BY id
            LIMIT $1
            FOR UPDATE SKIP LOCKED`,
          [batchSize],
        );
        if (rows.length === 0) return 0;

        // Group by topic so each topic gets one send call.
        const byTopic = new Map<string, { key: string; value: string }[]>();
        for (const r of rows) {
          const list = byTopic.get(r.topic) ?? [];
          list.push({ key: r.message_key, value: JSON.stringify(r.payload) });
          byTopic.set(r.topic, list);
        }
        await this.kafka.producer.sendBatch({
          topicMessages: [...byTopic].map(([topic, messages]) => ({ topic, messages })),
        });

        await client.query(`UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, [
          rows.map((r) => r.id),
        ]);
        this.logger.log(`published ${rows.length} event(s)`);
        return rows.length;
      });
    } catch (err) {
      this.logger.error(`publish failed, will retry: ${(err as Error).message}`);
      return 0;
    } finally {
      this.running = false;
    }
  }
}
