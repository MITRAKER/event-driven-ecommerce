import { Database } from '@shop/messaging';
import { MAX_PRODUCTS, ProductsController } from './products.controller';

function controllerWith(rows: object[]): { controller: ProductsController; query: jest.Mock } {
  const query = jest.fn().mockResolvedValue({ rows, rowCount: rows.length });
  const db = { pool: { query } } as unknown as Database;
  return { controller: new ProductsController(db), query };
}

describe('ProductsController.list', () => {
  it('returns the catalog with stock and prices, in camelCase for the API', async () => {
    const { controller } = controllerWith([
      { sku: 'BOOK', name: 'Designing Data-Intensive Applications', available: 10, price_cents: 4500 },
      { sku: 'MUG', name: 'Kafka mug', available: 25, price_cents: 1200 },
    ]);
    await expect(controller.list()).resolves.toEqual([
      { sku: 'BOOK', name: 'Designing Data-Intensive Applications', available: 10, priceCents: 4500 },
      { sku: 'MUG', name: 'Kafka mug', available: 25, priceCents: 1200 },
    ]);
  });

  it('asks for at most 100 rows, ordered by sku', async () => {
    const { controller, query } = controllerWith([]);
    await expect(controller.list()).resolves.toEqual([]);
    expect(MAX_PRODUCTS).toBe(100);
    expect(query).toHaveBeenCalledWith(expect.stringMatching(/ORDER BY sku LIMIT \$1$/), [MAX_PRODUCTS]);
  });

  it('fails when the database does', async () => {
    const controller = new ProductsController({
      pool: { query: jest.fn().mockRejectedValue(new Error('database down')) },
    } as unknown as Database);
    await expect(controller.list()).rejects.toThrow('database down');
  });
});
