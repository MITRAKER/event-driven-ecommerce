import { Controller, Get } from '@nestjs/common';
import { Database } from '@shop/messaging';

/** A products row as pg returns it. */
interface ProductRow {
  sku: string;
  name: string;
  available: number;
  price_cents: number;
}

/** One catalog entry as the API shows it. */
export interface ProductView {
  sku: string;
  name: string;
  available: number;
  priceCents: number;
}

/** Bounded response (JPL rule 3): the demo catalog is tiny, but no endpoint returns an unbounded list. */
export const MAX_PRODUCTS = 100;

@Controller('products')
export class ProductsController {
  constructor(private readonly db: Database) {}

  /** The catalog with live stock: watch it go down on reserve and back up on compensation. */
  @Get()
  async list(): Promise<ProductView[]> {
    const { rows } = await this.db.pool.query<ProductRow>(
      `SELECT sku, name, available, price_cents FROM products ORDER BY sku LIMIT $1`,
      [MAX_PRODUCTS],
    );
    return rows.map((row) => ({
      sku: row.sku,
      name: row.name,
      available: row.available,
      priceCents: row.price_cents,
    }));
  }
}
