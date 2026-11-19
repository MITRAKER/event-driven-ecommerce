import { Module } from '@nestjs/common';
import {
  Database,
  HealthController,
  KafkaClient,
  KafkaProducer,
  messagingConfigProvider,
  OutboxRelay,
} from '@shop/messaging';
import { OrdersController } from './orders/orders.controller';
import { OrdersService } from './orders/orders.service';
import { OrderSagaConsumer } from './saga/order-saga.consumer';

@Module({
  controllers: [OrdersController, HealthController],
  providers: [
    messagingConfigProvider, // validated settings, read once from the environment
    Database,
    KafkaClient,
    KafkaProducer, // this service publishes: OrderCreated, OrderConfirmed, OrderCancelled
    OutboxRelay,
    OrdersService,
    OrderSagaConsumer,
  ],
})
export class AppModule {}
