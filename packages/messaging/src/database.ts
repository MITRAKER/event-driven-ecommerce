import { Injectable, Logger, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { Pool, PoolClient } from 'pg';
import { MessagingConfig } from './config';
import { MESSAGING_TABLES_SQL } from './schema';

/** Any fixed number works; it just has to be the same for every replica of every service. */
const MIGRATION_LOCK_ID = 7_340_001;

/** Thin wrapper around a pg pool. Plain SQL on purpose: you see exactly what runs. */
@Injectable()
export class Database implements OnModuleInit, OnApplicationShutdown {
  private readonly logger = new Logger(Database.name);
  readonly pool: Pool;

  constructor(config: MessagingConfig) {
    const { databaseUrl, databasePoolMax } = config.settings;
    this.pool = new Pool({
      connectionString: databaseUrl,
      max: databasePoolMax, // bounded resources (JPL rule 3)
      connectionTimeoutMillis: 5_000, // bounded waits: never hang forever on a dead database
      statement_timeout: 10_000,
    });
    // Without a listener, an idle connection dropped by the server (e.g. Postgres restarting)
    // emits an unhandled 'error' event and crashes the whole process. The pool replaces it.
    this.pool.on('error', (err) => {
      this.logger.error(`idle database connection failed: ${err.message}`);
    });
  }

  async onModuleInit(): Promise<void> {
    await this.migrate(join(process.cwd(), 'migrations'));
  }

  /**
   * Runs after every onModuleDestroy hook (consumers stopped, relay finished), so nothing
   * is still using the pool when it closes.
   */
  async onApplicationShutdown(): Promise<void> {
    await this.pool.end();
  }

  /**
   * Apply the shared messaging tables, then every .sql file in `dir` in name order.
   * The advisory lock makes replicas that start at the same time take turns, because
   * concurrent CREATE TABLE IF NOT EXISTS can still collide inside Postgres.
   */
  async migrate(dir: string): Promise<void> {
    const files = existsSync(dir)
      ? readdirSync(dir)
          .filter((f) => f.endsWith('.sql'))
          .sort()
      : [];
    await this.transaction(async (tx) => {
      await tx.query('SELECT pg_advisory_xact_lock($1)', [MIGRATION_LOCK_ID]);
      await tx.query(MESSAGING_TABLES_SQL);
      for (const file of files) {
        await tx.query(readFileSync(join(dir, file), 'utf8'));
        this.logger.log(`migration applied: ${file}`);
      }
    });
  }

  /** Run fn inside BEGIN/COMMIT; ROLLBACK if it throws. */
  async transaction<T>(fn: (tx: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    const state: { broken?: Error } = {};
    try {
      await client.query('BEGIN');
      const result = await fn(client);
      await client.query('COMMIT');
      return result;
    } catch (err) {
      await this.rollback(client, state);
      throw err;
    } finally {
      // Passing an error tells the pool to destroy this connection instead of reusing it.
      client.release(state.broken);
    }
  }

  private async rollback(client: PoolClient, state: { broken?: Error }): Promise<void> {
    try {
      await client.query('ROLLBACK');
    } catch (rollbackErr) {
      // The connection itself is broken (e.g. the database restarted). Don't reuse it.
      state.broken = rollbackErr instanceof Error ? rollbackErr : new Error(String(rollbackErr));
    }
  }
}
