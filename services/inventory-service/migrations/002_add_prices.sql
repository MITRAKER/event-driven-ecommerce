-- Security fix: the catalog owns prices. Until now the client chose unitPriceCents, so anyone
-- could order a laptop for 0 cents. The inventory service now rejects any order line whose price
-- is not the catalog price.
--
-- 001_init.sql has already run on existing databases, and each migration runs exactly once, so a
-- shipped migration is never edited: this new file adds the column instead. The DEFAULT only exists
-- to fill the rows that are already there.
ALTER TABLE products ADD COLUMN IF NOT EXISTS price_cents INTEGER NOT NULL DEFAULT 0 CHECK (price_cents >= 0);

-- Prices for the demo catalog seeded by 001_init.sql (the end-to-end test orders at these prices).
UPDATE products SET price_cents = 4500   WHERE sku = 'BOOK';
UPDATE products SET price_cents = 1200   WHERE sku = 'MUG';
UPDATE products SET price_cents = 9000   WHERE sku = 'KEYBOARD';
UPDATE products SET price_cents = 150000 WHERE sku = 'LAPTOP';

-- Now that every row has a real price, drop the default: a product added later without a price
-- is refused by the database instead of quietly becoming free.
ALTER TABLE products ALTER COLUMN price_cents DROP DEFAULT;
