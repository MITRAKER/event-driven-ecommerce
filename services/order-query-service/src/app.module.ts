import { Module } from '@nestjs/common';
import { Database, HealthController, KafkaClient, messagingConfigProvider } from '@shop/messaging';
import { OrderProjector } from './order-projector.consumer';
import { OrderViewRepository } from './order-view.repository';
import { QueriesController } from './queries.controller';

/**
 * order-query-service owns the read model (order_view). It only consumes: no KafkaProducer and no
 * OutboxRelay, because the read side never publishes anything.
 */
@Module({
  controllers: [HealthController, QueriesController],
  providers: [messagingConfigProvider, Database, KafkaClient, OrderViewRepository, OrderProjector],
})
export class AppModule {}
