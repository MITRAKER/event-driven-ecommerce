import { Controller, Get, NotFoundException, Param, ParseUUIDPipe } from '@nestjs/common';
import { OrderViewRepository } from './order-view.repository';

/** The query side of CQRS: read-only endpoints, served from the projection. */
@Controller()
export class QueriesController {
  constructor(private readonly views: OrderViewRepository) {}

  /** Order tracking page: status, what inventory and payment said, and the full event timeline. */
  @Get('orders/:id')
  async order(@Param('id', ParseUUIDPipe) id: string) {
    const view = await this.views.byId(id);
    if (!view) throw new NotFoundException(`order ${id} not found (it may not be projected yet)`);
    return view;
  }

  /** "My orders" page. */
  @Get('customers/:customerId/orders')
  customerOrders(@Param('customerId') customerId: string) {
    return this.views.byCustomer(customerId);
  }

  /** A tiny admin dashboard: orders and revenue by status. */
  @Get('stats')
  stats() {
    return this.views.stats();
  }
}
