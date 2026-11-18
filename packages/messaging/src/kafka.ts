import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Kafka, logLevel, Producer } from 'kafkajs';
import { Topics } from '@shop/contracts';

export const kafka = new Kafka({
  clientId: process.env.SERVICE_NAME ?? 'shop-service',
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:19092').split(','),
  logLevel: logLevel.WARN,
  retry: { retries: 10 }, // the broker may still be starting when a service boots
});

/** Every main topic, and its dead-letter twin. */
export const ALL_TOPICS = Object.values(Topics).flatMap((t) => [t, dlqTopic(t)]);
export function dlqTopic(topic: string) {
  return `${topic}.dlq`;
}

/**
 * Create topics if they don't exist yet (safe to call from every service on every start).
 * 3 partitions = up to 3 consumer replicas per service can work in parallel.
 * Explicit creation beats broker auto-create: you choose partitions and settings on purpose.
 */
export async function ensureTopics() {
  const admin = kafka.admin();
  await admin.connect();
  try {
    await admin.createTopics({
      waitForLeaders: true,
      topics: ALL_TOPICS.map((topic) => ({ topic, numPartitions: 3, replicationFactor: 1 })),
    });
  } catch (err) {
    // Several services start at once; losing the creation race is fine.
    if (!/already exists/i.test((err as Error).message)) throw err;
  } finally {
    await admin.disconnect();
  }
}

@Injectable()
export class KafkaProducer implements OnModuleInit, OnModuleDestroy {
  // idempotent: true makes the broker drop duplicate sends caused by producer retries.
  readonly producer: Producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 });

  async onModuleInit() {
    await ensureTopics();
    await this.producer.connect();
  }

  async onModuleDestroy() {
    await this.producer.disconnect();
  }
}
