import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Kafka, Producer, logLevel } from 'kafkajs';

@Injectable()
export class KafkaProducer implements OnModuleInit, OnModuleDestroy {
  private readonly kafka = new Kafka({
    clientId: 'order-service',
    brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
    logLevel: logLevel.WARN,
  });
  // idempotent: true makes the broker drop duplicate sends caused by producer retries.
  readonly producer: Producer = this.kafka.producer({ idempotent: true, maxInFlightRequests: 1 });

  async onModuleInit() {
    await this.producer.connect();
  }

  async onModuleDestroy() {
    await this.producer.disconnect();
  }
}
