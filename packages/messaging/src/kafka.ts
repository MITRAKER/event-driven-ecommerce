import { BeforeApplicationShutdown, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Topics } from '@shop/contracts';
import { Kafka, logLevel, Partitioners, Producer } from 'kafkajs';
import { MessagingConfig } from './config';

/** The dead-letter twin of a topic. */
export function dlqTopic(topic: string): string {
  return `${topic}.dlq`;
}

/** Every main topic, and its dead-letter twin. */
export const ALL_TOPICS: readonly string[] = Object.values(Topics).flatMap((t) => [t, dlqTopic(t)]);

/** 3 partitions per topic: up to 3 replicas of each service can consume in parallel. */
const PARTITIONS = 3;

/** One Kafka client per service, built from validated settings. */
@Injectable()
export class KafkaClient {
  private readonly logger = new Logger(KafkaClient.name);
  readonly kafka: Kafka;

  constructor(config: MessagingConfig) {
    const { serviceName, kafkaBrokers } = config.settings;
    this.kafka = new Kafka({
      clientId: serviceName,
      brokers: [...kafkaBrokers],
      logLevel: logLevel.WARN,
      // Bounded: the broker may still be starting, but we never retry forever (JPL rule 2).
      retry: { retries: 8, initialRetryTime: 300, maxRetryTime: 10_000 },
    });
  }

  /**
   * Create any topic that does not exist yet. Safe to call from every service on every start.
   * Creating explicitly (instead of relying on broker auto-create) means we choose the
   * partition count on purpose.
   */
  async ensureTopics(): Promise<void> {
    const admin = this.kafka.admin();
    await admin.connect();
    try {
      const existing = new Set(await admin.listTopics());
      const missing = ALL_TOPICS.filter((topic) => !existing.has(topic));
      if (missing.length === 0) return;
      // Returns false (no error) if another service created them a moment ago.
      const created = await admin.createTopics({
        waitForLeaders: true,
        topics: missing.map((topic) => ({ topic, numPartitions: PARTITIONS, replicationFactor: 1 })),
      });
      this.logger.log(created ? `created topics: ${missing.join(', ')}` : 'topics already created by a peer');
    } finally {
      await admin.disconnect();
    }
  }
}

/**
 * The producer used by the outbox relay.
 *
 * Not the "idempotent producer": in kafkajs that mode expects unlimited retries, and we
 * want every retry loop to be bounded (JPL rule 2). Duplicates are fine anyway, because
 * every consumer deduplicates by eventId. maxInFlightRequests: 1 keeps per-partition order
 * even when a send is retried.
 */
@Injectable()
export class KafkaProducer implements OnModuleInit, BeforeApplicationShutdown {
  readonly producer: Producer;

  constructor(private readonly client: KafkaClient) {
    this.producer = client.kafka.producer({
      maxInFlightRequests: 1,
      retry: { retries: 5 },
      createPartitioner: Partitioners.DefaultPartitioner,
    });
  }

  async onModuleInit(): Promise<void> {
    await this.client.ensureTopics();
    await this.producer.connect();
  }

  /** Runs after the outbox relay stopped (onModuleDestroy), so no send is cut off. */
  async beforeApplicationShutdown(): Promise<void> {
    await this.producer.disconnect();
  }
}
