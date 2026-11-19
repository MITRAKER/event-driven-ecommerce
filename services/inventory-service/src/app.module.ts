import { Module } from '@nestjs/common';
import {
  Database,
  HealthController,
  KafkaClient,
  KafkaProducer,
  messagingConfigProvider,
  OutboxRelay,
} from '@shop/messaging';
import { InventoryConsumer } from './inventory.consumer';
import { ProductsController } from './products.controller';

/**
 * inventory-service owns the catalog (products, prices, stock) and the reservations. It consumes
 * the orders topic and publishes InventoryReserved / InventoryRejected through the outbox, so it
 * needs the producer and the outbox relay as well as the consumer.
 */
@Module({
  controllers: [HealthController, ProductsController],
  providers: [messagingConfigProvider, Database, KafkaClient, KafkaProducer, OutboxRelay, InventoryConsumer],
})
export class AppModule {}
