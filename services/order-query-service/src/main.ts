import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { NestExpressApplication } from '@nestjs/platform-express';
import { MessagingConfig } from '@shop/messaging';
import { AppModule } from './app.module';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create<NestExpressApplication>(AppModule);
  // Bounded input (JPL rule 3): a JSON body over 64 kB is refused before it is parsed. This service
  // only has GET routes, but the limit costs nothing and keeps every service the same.
  app.useBodyParser('json', { limit: '64kb' });
  // On SIGTERM: stop consuming, then close the database pool.
  app.enableShutdownHooks();
  await app.listen(app.get(MessagingConfig).settings.port);
}

// No floating promise: a failed start (bad config, database down) is logged and the process exits
// at once with code 1, so Docker or Kubernetes can see it and restart it.
bootstrap().catch((err: unknown) => {
  new Logger('Bootstrap').error(err instanceof Error ? (err.stack ?? err.message) : String(err));
  process.exit(1);
});
