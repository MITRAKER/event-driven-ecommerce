import {
  BadRequestException,
  Controller,
  Get,
  NotFoundException,
  Param,
  ParseUUIDPipe,
} from '@nestjs/common';
import { Limits } from '@shop/contracts';
import { OrderViewRepository, StatusStats } from './order-view.repository';
import { OrderView } from './projection';

/** The query side of CQRS: read-only endpoints, served from the projection. */
@Controller()
export class QueriesController {
  constructor(private readonly views: OrderViewRepository) {}

  /** Order tracking page: status, what inventory and payment said, and the full event timeline. */
  @Get('orders/:id')
  async order(@Param('id', ParseUUIDPipe) id: string): Promise<OrderView> {
    const view = await this.views.byId(id);
    if (view === null) throw new NotFoundException(`order ${id} not found (it may not be projected yet)`);
    return view;
  }

  /** "My orders" page: the customer's latest orders. */
  @Get('customers/:customerId/orders')
  async customerOrders(@Param('customerId') customerId: string): Promise<OrderView[]> {
    // Validate at the boundary (JPL rule 7), with the same bound POST /orders uses.
    if (customerId.length > Limits.maxCustomerIdLength) {
      throw new BadRequestException(`customerId must be 1..${Limits.maxCustomerIdLength} characters`);
    }
    return await this.views.byCustomer(customerId);
  }

  /** A tiny admin dashboard: orders and revenue by status. */
  @Get('stats')
  async stats(): Promise<StatusStats[]> {
    return await this.views.stats();
  }
}
