import { BadRequestException, NotFoundException } from '@nestjs/common';
import { Limits } from '@shop/contracts';
import { OrderViewRepository } from './order-view.repository';
import { emptyView } from './projection';
import { QueriesController } from './queries.controller';
import { at, ORDER_ID } from './testing';

const VIEW = emptyView(ORDER_ID, at(1));

function setup(): { controller: QueriesController; views: jest.Mocked<OrderViewRepository> } {
  const views = {
    byId: jest.fn(),
    byCustomer: jest.fn(),
    stats: jest.fn(),
  } as unknown as jest.Mocked<OrderViewRepository>;
  return { controller: new QueriesController(views), views };
}

describe('QueriesController', () => {
  it('returns the view of a projected order', async () => {
    const { controller, views } = setup();
    views.byId.mockResolvedValue(VIEW);

    expect(await controller.order(ORDER_ID)).toBe(VIEW);
    expect(views.byId).toHaveBeenCalledWith(ORDER_ID);
  });

  it('answers 404 for an order that is unknown, or not projected yet', async () => {
    const { controller, views } = setup();
    views.byId.mockResolvedValue(null);

    await expect(controller.order(ORDER_ID)).rejects.toThrow(NotFoundException);
  });

  it("lists a customer's orders", async () => {
    const { controller, views } = setup();
    views.byCustomer.mockResolvedValue([VIEW]);

    expect(await controller.customerOrders('c-1')).toEqual([VIEW]);
    expect(views.byCustomer).toHaveBeenCalledWith('c-1');
  });

  it('refuses a customerId longer than POST /orders allows, before touching the database', async () => {
    const { controller, views } = setup();

    await expect(controller.customerOrders('c'.repeat(Limits.maxCustomerIdLength + 1))).rejects.toThrow(
      BadRequestException,
    );
    expect(views.byCustomer).not.toHaveBeenCalled();
  });

  it('returns the stats', async () => {
    const { controller, views } = setup();
    const stats = [{ status: 'CONFIRMED' as const, orders: 2, totalCents: 9000 }];
    views.stats.mockResolvedValue(stats);

    expect(await controller.stats()).toBe(stats);
  });
});
