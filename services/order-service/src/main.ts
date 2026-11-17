import 'reflect-metadata';
import { Logger, ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { NestExpressApplication } from '@nestjs/platform-express';
import { MessagingConfig } from '@shop/messaging';
import { AppModule } from './app.module';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create<NestExpressApplication>(AppModule);
  // Bounded input (JPL rule 3): a bigger JSON body is refused with 413 before it is parsed.
  // The largest valid order (50 items) is a few kilobytes.
  app.useBodyParser('json', { limit: '64kb' });
  // whitelist + forbidNonWhitelisted: an unknown field is a 400, not silently dropped.
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, forbidNonWhitelisted: true }));
  app.enableShutdownHooks(); // stop consuming, flush the relay, then close the pool on Ctrl+C
  await app.listen(app.get(MessagingConfig).settings.port);
}

// No floating promise: a failed start (bad config, database down) is logged and the process exits
// at once with code 1, so Docker or Kubernetes can see it and restart it.
bootstrap().catch((err: unknown) => {
  new Logger('Bootstrap').error(err instanceof Error ? (err.stack ?? err.message) : String(err));
  process.exit(1);
});
