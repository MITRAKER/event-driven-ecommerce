import { Module } from '@nestjs/common';
import { Database, KafkaProducer, OutboxRelay } from '@shop/messaging';
import { OrdersController } from './orders/orders.controller';
import { OrdersService } from './orders/orders.service';
import { OrderSagaConsumer } from './saga/order-saga.consumer';

@Module({
  controllers: [OrdersController],
  providers: [Database, KafkaProducer, OutboxRelay, OrdersService, OrderSagaConsumer],
})
export class AppModule {}
