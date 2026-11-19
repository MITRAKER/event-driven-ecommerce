import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Pool, PoolClient } from 'pg';
import { Database } from './database';
import { fakeClient, testConfig } from './testing';

function databaseWith(client: PoolClient): { db: Database; release: jest.Mock } {
  const release = jest.fn();
  const db = new Database(testConfig());
  const pool = { connect: jest.fn().mockResolvedValue(Object.assign(client, { release })), end: jest.fn() };
  Object.assign(db, { pool: pool as unknown as Pool });
  return { db, release };
}

describe('Database.transaction', () => {
  it('commits when the function succeeds', async () => {
    const { client, queries } = fakeClient();
    const { db, release } = databaseWith(client);
    await expect(db.transaction(() => Promise.resolve('done'))).resolves.toBe('done');
    expect(queries.map((q) => q.sql)).toEqual(['BEGIN', 'COMMIT']);
    expect(release).toHaveBeenCalledWith(undefined);
  });

  it('rolls back and rethrows when the function fails', async () => {
    const { client, queries } = fakeClient();
    const { db, release } = databaseWith(client);
    await expect(db.transaction(() => Promise.reject(new Error('boom')))).rejects.toThrow('boom');
    expect(queries.map((q) => q.sql)).toEqual(['BEGIN', 'ROLLBACK']);
    expect(release).toHaveBeenCalledWith(undefined);
  });

  it('destroys the connection if ROLLBACK itself fails', async () => {
    const { client } = fakeClient((sql) => {
      if (sql === 'ROLLBACK') throw new Error('connection lost');
      return {};
    });
    const { db, release } = databaseWith(client);
    await expect(db.transaction(() => Promise.reject(new Error('boom')))).rejects.toThrow('boom');
    expect(release).toHaveBeenCalledWith(new Error('connection lost'));
  });
});

describe('Database.migrate', () => {
  function migrationsDir(): string {
    const dir = mkdtempSync(join(tmpdir(), 'migrations-'));
    writeFileSync(join(dir, '002_more.sql'), 'SELECT 2;');
    writeFileSync(join(dir, '001_init.sql'), 'SELECT 1;');
    writeFileSync(join(dir, 'README.md'), 'not a migration');
    return dir;
  }

  it('takes the advisory lock, creates the messaging tables, then applies new .sql files in name order', async () => {
    const { client, queries } = fakeClient();
    const { db } = databaseWith(client);
    await db.migrate(migrationsDir());
    const sql = queries.map((q) => q.sql);
    expect(sql[0]).toBe('BEGIN');
    expect(sql[1]).toContain('pg_advisory_xact_lock');
    expect(sql[2]).toContain('CREATE TABLE IF NOT EXISTS schema_migrations');
    expect(sql[3]).toBe('SELECT name FROM schema_migrations');
    expect(sql.slice(4)).toEqual([
      'SELECT 1;',
      'INSERT INTO schema_migrations (name) VALUES ($1)',
      'SELECT 2;',
      'INSERT INTO schema_migrations (name) VALUES ($1)',
      'COMMIT',
    ]);
  });

  it('skips migrations that already ran', async () => {
    const { client, queries } = fakeClient((sql) =>
      sql.startsWith('SELECT name') ? { rows: [{ name: '001_init.sql' }] } : {},
    );
    const { db } = databaseWith(client);
    await db.migrate(migrationsDir());
    const sql = queries.map((q) => q.sql);
    expect(sql).not.toContain('SELECT 1;');
    expect(sql).toContain('SELECT 2;');
  });

  it('works for a service without its own migrations folder', async () => {
    const { client, queries } = fakeClient();
    const { db } = databaseWith(client);
    await db.migrate('/definitely/not/here');
    expect(queries).toHaveLength(5);
  });
});
