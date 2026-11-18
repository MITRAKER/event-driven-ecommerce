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
