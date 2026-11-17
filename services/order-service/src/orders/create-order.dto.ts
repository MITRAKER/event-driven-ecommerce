import { Limits } from '@shop/contracts';
import { Type } from 'class-transformer';
import {
  ArrayMaxSize,
  ArrayNotEmpty,
  IsArray,
  IsInt,
  IsString,
  Length,
  Max,
  Min,
  ValidateNested,
} from 'class-validator';

/*
 * The HTTP boundary. Every field has a type AND an upper bound (JPL rule 3), taken from
 * @shop/contracts, so the API accepts exactly what parseEvent() accepts further downstream.
 * main.ts runs these checks with whitelist + forbidNonWhitelisted: an unknown field is a 400.
 */

export class OrderItemDto {
  @IsString()
  @Length(1, Limits.maxSkuLength)
  readonly sku!: string;

  @IsInt()
  @Min(1)
  @Max(Limits.maxQuantityPerItem)
  readonly quantity!: number;

  /** Integer cents, never a float: 0.1 + 0.2 !== 0.3 is no way to handle money. */
  @IsInt()
  @Min(0)
  @Max(Limits.maxUnitPriceCents)
  readonly unitPriceCents!: number;
}

export class CreateOrderDto {
  @IsString()
  @Length(1, Limits.maxCustomerIdLength)
  readonly customerId!: string;

  @IsArray()
  @ArrayNotEmpty()
  @ArrayMaxSize(Limits.maxItemsPerOrder)
  @ValidateNested({ each: true })
  @Type(() => OrderItemDto)
  readonly items!: OrderItemDto[];
}
