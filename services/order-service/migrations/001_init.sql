-- Orders: the service's own state.
CREATE TABLE IF NOT EXISTS orders (
  id           UUID PRIMARY KEY,
  customer_id  TEXT        NOT NULL,
  status       TEXT        NOT NULL,       -- PENDING | CONFIRMED | CANCELLED
  total_cents  INTEGER     NOT NULL,
  items        JSONB       NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Transactional outbox: events waiting to be published to Kafka.
-- Written in the SAME transaction as the order row, so both happen or neither does.
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
