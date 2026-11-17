/** Tables every service needs for reliable messaging. All statements are idempotent. */
export const MESSAGING_TABLES_SQL = `
-- Transactional outbox: events waiting to be published to Kafka.
-- Written in the SAME transaction as the business change, so both happen or neither does.
CREATE TABLE IF NOT EXISTS outbox (
  id            BIGSERIAL   PRIMARY KEY,   -- insertion order = publish order
  event_id      UUID        NOT NULL UNIQUE,
  topic         TEXT        NOT NULL,
  message_key   TEXT        NOT NULL,
  payload       JSONB       NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

-- Inbox / dedupe table: one row per event this service has already handled.
CREATE TABLE IF NOT EXISTS processed_events (
  event_id      UUID        PRIMARY KEY,
  event_type    TEXT        NOT NULL,
  processed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
`;
