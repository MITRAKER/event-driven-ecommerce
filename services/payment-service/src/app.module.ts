import { Module } from '@nestjs/common';
import { Database, KafkaProducer, OutboxRelay } from '@shop/messaging';
import { PaymentConsumer } from './payment.consumer';

@Module({ providers: [Database, KafkaProducer, OutboxRelay, PaymentConsumer] })
export class AppModule {}
