import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { Pool, PoolClient } from 'pg';
import { MESSAGING_TABLES_SQL } from './schema';

/** Thin wrapper around a pg pool. Plain SQL on purpose: you see exactly what runs. */
@Injectable()
export class Database implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(Database.name);
  readonly pool = new Pool({ connectionString: process.env.DATABASE_URL });

  async onModuleInit() {
    // Every service gets the outbox + processed_events tables, then its own migrations.
    await this.pool.query(MESSAGING_TABLES_SQL);
    const dir = join(process.cwd(), 'migrations');
    if (!existsSync(dir)) return;
    for (const file of readdirSync(dir).filter((f) => f.endsWith('.sql')).sort()) {
      await this.pool.query(readFileSync(join(dir, file), 'utf8'));
      this.logger.log(`migration applied: ${file}`);
    }
  }

  async onModuleDestroy() {
    await this.pool.end();
  }

  /** Run fn inside BEGIN/COMMIT; ROLLBACK if it throws. */
  async transaction<T>(fn: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const result = await fn(client);
      await client.query('COMMIT');
      return result;
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }
}
