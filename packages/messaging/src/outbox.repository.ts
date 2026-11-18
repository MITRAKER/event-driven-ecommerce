import { PoolClient } from 'pg';
import { EventEnvelope } from '@shop/contracts';

/** Stage an event for publishing. MUST be called with the same client as the business write. */
export async function addToOutbox(client: PoolClient, topic: string, event: EventEnvelope) {
  await client.query(
    `INSERT INTO outbox (event_id, topic, message_key, payload) VALUES ($1, $2, $3, $4)`,
    [event.eventId, topic, event.aggregateId, JSON.stringify(event)],
  );
}
