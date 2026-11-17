import { ArgumentMetadata, BadRequestException, ValidationPipe } from '@nestjs/common';
import { Limits } from '@shop/contracts';
import { plainToInstance } from 'class-transformer';
import { validate } from 'class-validator';
import { CreateOrderDto } from './create-order.dto';

/** The options main.ts gives its global ValidationPipe. */
const PIPE_OPTIONS = { whitelist: true, forbidNonWhitelisted: true };

/** One line of class-validator's report: " - property items[0].quantity has failed ... : min " */
const FAILURE = / - property (\S+) has failed the following constraints: (.+) $/;

/**
 * Validate a body the way the ValidationPipe does (class-transformer, then class-validator)
 * and list every failure as "path: constraint", for example "items[0].quantity: min".
 */
async function failures(body: object): Promise<string[]> {
  const errors = await validate(plainToInstance(CreateOrderDto, body), PIPE_OPTIONS);
  return errors.flatMap((error) =>
    error
      .toString()
      .split('\n')
      .flatMap((line) => {
        const match = FAILURE.exec(line);
        return match === null ? [] : [`${match[1] ?? '?'}: ${match[2] ?? '?'}`];
      }),
  );
}

const item = { sku: 'BOOK', quantity: 2, unitPriceCents: 1500 };
const valid = { customerId: 'c-1', items: [item] };

/** A valid body whose first item has `change` applied. */
function withItem(change: Record<string, unknown>): object {
  return { ...valid, items: [{ ...item, ...change }] };
}

describe('CreateOrderDto', () => {
  it('accepts a valid order', async () => {
    expect(await failures(valid)).toEqual([]);
  });

  it('accepts the largest order the limits allow (the total limit is the service’s check)', async () => {
    const biggest = {
      customerId: 'c'.repeat(Limits.maxCustomerIdLength),
      items: Array.from({ length: Limits.maxItemsPerOrder }, () => ({
        sku: 'S'.repeat(Limits.maxSkuLength),
        quantity: Limits.maxQuantityPerItem,
        unitPriceCents: Limits.maxUnitPriceCents,
      })),
    };
    expect(await failures(biggest)).toEqual([]);
  });

  it.each([
    ['no items', { ...valid, items: [] }, 'items: arrayNotEmpty'],
    [
      'too many items',
      { ...valid, items: Array.from({ length: Limits.maxItemsPerOrder + 1 }, () => item) },
      'items: arrayMaxSize',
    ],
    ['an item that is not an object', { ...valid, items: ['BOOK'] }, 'items[0]: nestedValidation'],
    ['quantity 0', withItem({ quantity: 0 }), 'items[0].quantity: min'],
    ['quantity above the limit', withItem({ quantity: 1001 }), 'items[0].quantity: max'],
    ['a fractional quantity', withItem({ quantity: 1.5 }), 'items[0].quantity: isInt'],
    ['a fractional price', withItem({ unitPriceCents: 9.99 }), 'items[0].unitPriceCents: isInt'],
    ['a negative price', withItem({ unitPriceCents: -1 }), 'items[0].unitPriceCents: min'],
    ['a price above the limit', withItem({ unitPriceCents: 10_000_001 }), 'items[0].unitPriceCents: max'],
    ['an empty sku', withItem({ sku: '' }), 'items[0].sku: isLength'],
    ['a sku that is too long', withItem({ sku: 'S'.repeat(65) }), 'items[0].sku: isLength'],
    ['an empty customerId', { ...valid, customerId: '' }, 'customerId: isLength'],
    ['a customerId that is too long', { ...valid, customerId: 'c'.repeat(65) }, 'customerId: isLength'],
    ['an unknown field', { ...valid, isAdmin: true }, 'isAdmin: whitelistValidation'],
    ['an unknown item field', withItem({ discount: 100 }), 'items[0].discount: whitelistValidation'],
  ])('rejects %s', async (_label, body, expected) => {
    expect(await failures(body)).toEqual([expected]);
  });

  it('rejects values of the wrong type, with no implicit conversion ("2" is not 2)', async () => {
    const problems = await failures({ customerId: 42, items: [{ ...item, quantity: '2' }] });
    expect(problems).toHaveLength(2);
    expect(problems[0]).toMatch(/^customerId: .*\bisString\b/);
    expect(problems[1]).toMatch(/^items\[0\]\.quantity: .*\bisInt\b/);
  });

  it('makes the ValidationPipe from main.ts answer 400 Bad Request', async () => {
    const pipe = new ValidationPipe(PIPE_OPTIONS);
    const metadata: ArgumentMetadata = { type: 'body', metatype: CreateOrderDto };

    await expect(pipe.transform({ ...valid, isAdmin: true }, metadata)).rejects.toThrow(BadRequestException);
    await expect(pipe.transform(valid, metadata)).resolves.toEqual(valid);
  });
});
