import { Module } from '@nestjs/common';
import {
  Database,
  HealthController,
  KafkaClient,
  KafkaProducer,
  messagingConfigProvider,
  OutboxRelay,
} from '@shop/messaging';
import { paymentConfigProvider } from './payment-config';
import { PaymentConsumer } from './payment.consumer';

/**
 * payment-service owns the payments table. It consumes the inventory topic and publishes
 * PaymentSucceeded / PaymentFailed through the outbox, so it needs the producer and the outbox
 * relay as well as the consumer.
 */
@Module({
  controllers: [HealthController],
  providers: [
    messagingConfigProvider, // shared settings (database, Kafka), validated once at startup
    paymentConfigProvider, // DECLINE_ABOVE_CENTS and PAYMENT_FAILURE_RATE, validated the same way
    Database,
    KafkaClient,
    KafkaProducer,
    OutboxRelay,
    PaymentConsumer,
  ],
})
export class AppModule {}
