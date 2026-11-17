import { Controller, Get } from '@nestjs/common';
import { Database } from '@shop/messaging';

@Controller('products')
export class ProductsController {
  constructor(private readonly db: Database) {}

  /** Handy for watching stock go down on reserve and back up on compensation. */
  @Get()
  async list() {
    const { rows } = await this.db.pool.query(`SELECT sku, name, available FROM products ORDER BY sku`);
    return rows;
  }
}
