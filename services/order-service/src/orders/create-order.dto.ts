import { Type } from 'class-transformer';
import { ArrayNotEmpty, IsInt, IsString, Min, ValidateNested } from 'class-validator';

export class OrderItemDto {
  @IsString() sku!: string;
  @IsInt() @Min(1) quantity!: number;
  @IsInt() @Min(0) unitPriceCents!: number;
}

export class CreateOrderDto {
  @IsString() customerId!: string;
  @ArrayNotEmpty() @ValidateNested({ each: true }) @Type(() => OrderItemDto) items!: OrderItemDto[];
}
