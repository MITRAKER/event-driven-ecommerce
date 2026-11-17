import { Controller, Get } from '@nestjs/common';
import { Database } from './database';

/** Liveness + readiness: answers only when the service can reach its database. Used by Docker and the e2e test. */
@Controller('health')
export class HealthController {
  constructor(private readonly db: Database) {}

  @Get()
  async check(): Promise<{ status: 'ok' }> {
    await this.db.pool.query('SELECT 1');
    return { status: 'ok' };
  }
}
