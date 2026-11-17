import { Module } from '@nestjs/common';
import { Database } from './infra/database';
import { KafkaProducer } from './infra/kafka';
import { OrdersController } from './orders/orders.controller';
import { OrdersService } from './orders/orders.service';
import { OutboxRelay } from './outbox/outbox.relay';

@Module({
  controllers: [OrdersController],
  providers: [Database, KafkaProducer, OrdersService, OutboxRelay],
})
export class AppModule {}
