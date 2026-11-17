import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Kafka, logLevel, Producer } from 'kafkajs';

export const kafka = new Kafka({
  clientId: process.env.SERVICE_NAME ?? 'shop-service',
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
  logLevel: logLevel.WARN,
});

@Injectable()
export class KafkaProducer implements OnModuleInit, OnModuleDestroy {
  // idempotent: true makes the broker drop duplicate sends caused by producer retries.
  readonly producer: Producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 });

  async onModuleInit() {
    await this.producer.connect();
  }

  async onModuleDestroy() {
    await this.producer.disconnect();
  }
}
