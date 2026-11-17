import { Injectable, OnModuleDestroy, OnModuleInit, Logger } from '@nestjs/common';
import { readFileSync, readdirSync } from 'node:fs';
import { join } from 'node:path';
import { Pool, PoolClient } from 'pg';

/** Thin wrapper around a pg connection pool. Plain SQL on purpose: you see exactly what runs. */
@Injectable()
export class Database implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(Database.name);
  readonly pool = new Pool({
    connectionString: process.env.DATABASE_URL ?? 'postgres://shop:shop@localhost:5432/orders',
  });

  async onModuleInit() {
    // Tiny migration runner: applies every .sql file in /migrations in name order.
    // (Every statement is idempotent: CREATE ... IF NOT EXISTS.)
    const dir = join(__dirname, '..', '..', 'migrations');
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
