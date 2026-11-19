import { HttpStatus, NotFoundException } from '@nestjs/common';
import { HTTP_CODE_METADATA } from '@nestjs/common/constants';
import { plainToInstance } from 'class-transformer';
import { ORDER_ID } from '../testing';
import { CreateOrderDto } from './create-order.dto';
import { OrdersController } from './orders.controller';
import { OrderAccepted, OrdersService, OrderView } from './orders.service';

const body = plainToInstance(CreateOrderDto, {
  customerId: 'c-1',
  items: [{ sku: 'BOOK', quantity: 1, unitPriceCents: 4500 }],
});
const accepted: OrderAccepted = { orderId: ORDER_ID, status: 'PENDING', totalCents: 4500 };
const view: OrderView = {
  orderId: ORDER_ID,
  customerId: 'c-1',
  status: 'PENDING',
  totalCents: 4500,
  items: body.items,
  createdAt: '2026-01-02T03:04:05.000Z',
  updatedAt: '2026-01-02T03:04:05.000Z',
};

function setup(): { controller: OrdersController; service: { create: jest.Mock; get: jest.Mock } } {
  const service = { create: jest.fn().mockResolvedValue(accepted), get: jest.fn().mockResolvedValue(view) };
  return { controller: new OrdersController(service as unknown as OrdersService), service };
}

/** Metadata a Nest decorator stored on one of the controller's route handlers. */
function routeMetadata(key: string, handler: keyof OrdersController): unknown {
  const method: unknown = Object.getOwnPropertyDescriptor(OrdersController.prototype, handler)?.value;
  return typeof method === 'function' ? Reflect.getMetadata(key, method) : undefined;
}

describe('OrdersController', () => {
  it('create() hands the validated body to the service and returns its answer', async () => {
    const { controller, service } = setup();

    await expect(controller.create(body)).resolves.toBe(accepted);
    expect(service.create).toHaveBeenCalledWith(body);
  });

  it('answers POST /orders with 202 Accepted: the saga decides the outcome later', () => {
    expect(routeMetadata(HTTP_CODE_METADATA, 'create')).toBe(HttpStatus.ACCEPTED);
  });

  it('get() looks the order up by id', async () => {
    const { controller, service } = setup();

    await expect(controller.get(ORDER_ID)).resolves.toBe(view);
    expect(service.get).toHaveBeenCalledWith(ORDER_ID);
  });

  it('get() lets the service 404 through, for Nest to turn into the HTTP answer', async () => {
    const { controller, service } = setup();
    service.get.mockRejectedValue(new NotFoundException(`order ${ORDER_ID} not found`));

    await expect(controller.get(ORDER_ID)).rejects.toThrow(NotFoundException);
  });
});
