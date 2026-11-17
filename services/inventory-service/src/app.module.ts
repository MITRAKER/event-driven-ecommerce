import { Module } from '@nestjs/common';
import { Database, KafkaProducer, OutboxRelay } from '@shop/messaging';
import { InventoryConsumer } from './inventory.consumer';
import { ProductsController } from './products.controller';

@Module({
  controllers: [ProductsController],
  providers: [Database, KafkaProducer, OutboxRelay, InventoryConsumer],
})
export class AppModule {}
