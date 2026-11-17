-- The read model: one wide, denormalised row per order, shaped for the screens that read it.
-- No joins at query time. It can be deleted and rebuilt from the event log at any moment.
CREATE TABLE IF NOT EXISTS order_view (
  order_id          UUID        PRIMARY KEY,
  customer_id       TEXT,
  status            TEXT        NOT NULL,   -- PENDING | CONFIRMED | CANCELLED
  total_cents       INTEGER,
  items             JSONB,
  inventory_status  TEXT,                   -- RESERVED | REJECTED | RELEASED
  payment_status    TEXT,                   -- SUCCEEDED | FAILED
  cancel_reason     TEXT,
  timeline          JSONB       NOT NULL DEFAULT '[]',  -- every event, for an "order tracking" page
  created_at        TIMESTAMPTZ,
  updated_at        TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS order_view_customer ON order_view (customer_id, created_at DESC);
CREATE INDEX IF NOT EXISTS order_view_status   ON order_view (status);
