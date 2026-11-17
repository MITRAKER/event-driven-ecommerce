import { Module } from '@nestjs/common';
import { Database } from '@shop/messaging';
import { OrderProjector } from './order-projector.consumer';
import { OrderViewRepository } from './order-view.repository';
import { QueriesController } from './queries.controller';

// No KafkaProducer and no OutboxRelay: the read side only consumes.
@Module({
  controllers: [QueriesController],
  providers: [Database, OrderViewRepository, OrderProjector],
})
export class AppModule {}
