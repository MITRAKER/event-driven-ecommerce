import { Injectable, NotFoundException } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { createEvent, OrderCreated, Topics } from '@shop/contracts';
import { addToOutbox, Database } from '@shop/messaging';
import { CreateOrderDto } from './create-order.dto';

@Injectable()
export class OrdersService {
  constructor(private readonly db: Database) {}

  /**
   * Command side: create the order AND stage the OrderCreated event in ONE transaction.
   * We never call Kafka here. If we did, a crash between "INSERT order" and "send to Kafka"
   * would leave an order nobody else ever hears about (the dual-write problem).
   */
  async create(dto: CreateOrderDto) {
    const orderId = randomUUID();
    const totalCents = dto.items.reduce((sum, i) => sum + i.quantity * i.unitPriceCents, 0);

    await this.db.transaction(async (client) => {
      await client.query(
        `INSERT INTO orders (id, customer_id, status, total_cents, items)
         VALUES ($1, $2, 'PENDING', $3, $4)`,
        [orderId, dto.customerId, totalCents, JSON.stringify(dto.items)],
      );
      const event = createEvent<OrderCreated>('OrderCreated', orderId, {
        orderId,
        customerId: dto.customerId,
        items: dto.items,
        totalCents,
      });
      await addToOutbox(client, Topics.Orders, event);
    });

    return { orderId, status: 'PENDING', totalCents };
  }

  async get(orderId: string) {
    const { rows } = await this.db.pool.query(
      `SELECT id, customer_id, status, total_cents, items, created_at, updated_at
         FROM orders WHERE id = $1`,
      [orderId],
    );
    if (!rows[0]) throw new NotFoundException(`order ${orderId} not found`);
    return rows[0];
  }
}
