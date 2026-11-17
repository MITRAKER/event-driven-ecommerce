CREATE TABLE IF NOT EXISTS products (
  sku        TEXT    PRIMARY KEY,
  name       TEXT    NOT NULL,
  available  INTEGER NOT NULL CHECK (available >= 0)   -- the DB itself refuses negative stock
);

-- One reservation per order, so releasing knows exactly what to give back.
CREATE TABLE IF NOT EXISTS reservations (
  order_id    UUID        PRIMARY KEY,
  items       JSONB       NOT NULL,
  status      TEXT        NOT NULL,     -- RESERVED | RELEASED
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Demo catalog
INSERT INTO products (sku, name, available) VALUES
  ('BOOK',     'Designing Data-Intensive Applications', 10),
  ('MUG',      'Kafka mug',                             25),
  ('KEYBOARD', 'Mechanical keyboard',                    2),
  ('LAPTOP',   'Laptop (expensive: payment will fail)',  5)
ON CONFLICT (sku) DO NOTHING;
