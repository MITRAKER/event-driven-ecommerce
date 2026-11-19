import { EventEnvelope } from '@shop/contracts';
import { PoolClient } from 'pg';

/**
 * Claim an event for processing inside the current transaction.
 * Returns false if this service already handled it (a duplicate delivery).
 *
 * Because the claim and the business change share one transaction, either both
 * are saved or neither is: a crash mid-handler means the event is retried cleanly.
 */
export async function claimEvent(tx: PoolClient, event: EventEnvelope): Promise<boolean> {
  const result = await tx.query(
    `INSERT INTO processed_events (event_id, event_type) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
    [event.eventId, event.eventType],
  );
  return result.rowCount === 1;
}
