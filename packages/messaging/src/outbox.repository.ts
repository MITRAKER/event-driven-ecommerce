import { EventEnvelope, invariant, Topic } from '@shop/contracts';
import { PoolClient } from 'pg';

/** Stage an event for publishing. MUST be called with the same client as the business write. */
export async function addToOutbox(tx: PoolClient, topic: Topic, event: EventEnvelope): Promise<void> {
  const result = await tx.query(
    `INSERT INTO outbox (event_id, topic, message_key, payload) VALUES ($1, $2, $3, $4)`,
    [event.eventId, topic, event.aggregateId, JSON.stringify(event)],
  );
  invariant(result.rowCount === 1, `outbox insert for ${event.eventId} wrote ${result.rowCount} rows`);
}
