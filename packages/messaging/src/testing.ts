/* Test helpers shared by this package's specs. Not exported from index.ts. */
import { PoolClient } from 'pg';
import { MessagingConfig, MessagingSettings } from './config';

export const TEST_SETTINGS: MessagingSettings = {
  serviceName: 'test-service',
  port: 3999,
  databaseUrl: 'postgres://test',
  databasePoolMax: 2,
  kafkaBrokers: ['localhost:19092'],
  outboxPollMs: 500,
  outboxBatchSize: 100,
  consumerRetryAttempts: 3,
  consumerRetryBaseMs: 1,
};

export function testConfig(overrides: Partial<MessagingSettings> = {}): MessagingConfig {
  return new MessagingConfig({ ...TEST_SETTINGS, ...overrides });
}

export interface FakeQuery {
  sql: string;
  params: readonly unknown[] | undefined;
}

/** A fake pg client: records every query and answers with whatever `respond` returns. */
export function fakeClient(
  respond: (sql: string, params: readonly unknown[] | undefined) => unknown = () => ({}),
): {
  client: PoolClient;
  queries: FakeQuery[];
} {
  const queries: FakeQuery[] = [];
  const query = jest.fn((sql: string, params?: readonly unknown[]) => {
    queries.push({ sql, params });
    return Promise.resolve({ rows: [], rowCount: 0, ...(respond(sql, params) as object) });
  });
  return { client: { query } as unknown as PoolClient, queries };
}
