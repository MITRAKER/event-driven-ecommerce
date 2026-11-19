import { Body, Controller, Get, HttpCode, HttpStatus, Param, ParseUUIDPipe, Post } from '@nestjs/common';
import { CreateOrderDto } from './create-order.dto';
import { OrderAccepted, OrdersService, OrderView } from './orders.service';

@Controller('orders')
export class OrdersController {
  constructor(private readonly orders: OrdersService) {}

  /** 202 Accepted, not 201: the order exists, but the outcome (confirmed or cancelled) arrives later via events. */
  @Post()
  @HttpCode(HttpStatus.ACCEPTED)
  async create(@Body() dto: CreateOrderDto): Promise<OrderAccepted> {
    return await this.orders.create(dto);
  }

  /** ParseUUIDPipe answers 400 for a malformed id, before any SQL runs. */
  @Get(':id')
  async get(@Param('id', ParseUUIDPipe) id: string): Promise<OrderView> {
    return await this.orders.get(id);
  }
}
