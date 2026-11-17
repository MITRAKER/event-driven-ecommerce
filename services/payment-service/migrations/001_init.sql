-- order_id is the primary key: one payment attempt per order, ever.
-- Even if every other safeguard failed, the database itself would refuse a second charge.
CREATE TABLE IF NOT EXISTS payments (
  order_id      UUID        PRIMARY KEY,
  amount_cents  INTEGER     NOT NULL,
  status        TEXT        NOT NULL,   -- SUCCEEDED | FAILED
  reason        TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
