import pino from 'pino';
import { getDbPool } from './db';
import { getRabbitChannel, QUEUES } from './rabbitmq';
import { publishWsFanout, acquireLock, releaseLock } from './redis';
import { randomUUID } from 'crypto';

const logger = pino({ name: 'worker', level: 'info' });

async function startWorker() {
  const ch = await getRabbitChannel();
  const pool = await getDbPool();

  logger.info('Worker service online. Consuming RabbitMQ events with prefetch 10...');
  await ch.prefetch(10);

  // -------------------------------------------------------------
  // 1. Bed Events Consumer (Admissions, Discharges, Cleaning)
  // -------------------------------------------------------------
  await ch.consume(QUEUES.BED_EVENTS, async (msg: any) => {
    if (!msg) return;

    try {
      const payload = JSON.parse(msg.content.toString());
      const eventId = msg.properties.messageId || `${payload.bedId}_${payload.version || Date.now()}`;

      // Idempotency check: exactly-once effects
      const [existing]: any = await pool.query(
        'SELECT event_id FROM processed_events WHERE event_id = ?',
        [eventId]
      );

      if (existing.length > 0) {
        logger.warn({ eventId }, 'Duplicate bed event detected - discarded');
        ch.ack(msg);
        return;
      }

      const { bedId, status, actorId } = payload;

      // Update bed state in MySQL
      await pool.query(
        'UPDATE beds SET status = ?, assigned_actor = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
        [status, actorId || null, bedId]
      );

      // Record idempotent event execution
      await pool.query(
        'INSERT INTO processed_events (event_id, event_type, aggregate_id) VALUES (?, ?, ?)',
        [eventId, 'BED_STATUS_CHANGE', bedId]
      );

      // Broadcast city-wide via Redis pub/sub
      await publishWsFanout('ws:fanout', {
        type: 'bedUpdateBroadcast',
        payload: { bedId, status, actorId, timestamp: Date.now() },
      });

      ch.ack(msg);
    } catch (err) {
      logger.error({ err }, 'Error processing bed event');
      handleFailure(ch, msg);
    }
  });

  // -------------------------------------------------------------
  // 2. Saga Transfer Consumer (Two-Hospital Patient Transfer Workflow)
  // -------------------------------------------------------------
  await ch.consume(QUEUES.TRANSFERS, async (msg: any) => {
    if (!msg) return;

    try {
      const payload = JSON.parse(msg.content.toString());
      const { transferId, action, targetBedId, fromHospitalId, toHospitalId, sourceBedId, bedType } = payload;
      const priority = msg.properties.priority || 0;

      logger.info({ transferId, action, priority }, 'Processing transfer saga step');

      if (action === 'REQUEST_TRANSFER') {
        // Find suitable free bed at target receiving hospital
        const [freeBeds]: any = await pool.query(
          'SELECT id, version FROM beds WHERE hospital_id = ? AND status = "FREE" AND bed_type = ? LIMIT 1',
          [toHospitalId, bedType || 'ICU']
        );

        if (freeBeds.length > 0) {
          const candidateBed = freeBeds[0];
          const lockToken = randomUUID();
          const locked = await acquireLock(`bed:${candidateBed.id}`, lockToken, 15);

          if (locked) {
            try {
              // Optimistic lock check
              await pool.query(
                'UPDATE beds SET status = "RESERVED", version = version + 1 WHERE id = ? AND version = ?',
                [candidateBed.id, candidateBed.version]
              );

              // Update transfer record in MySQL
              await pool.query(
                'UPDATE patient_transfers SET target_bed_id = ?, step = "ACCEPTED" WHERE id = ?',
                [candidateBed.id, transferId]
              );

              // Notify receiving and sending hospitals
              await publishWsFanout('ws:fanout', {
                type: 'transferIncomingAlert',
                payload: {
                  transferId,
                  fromHospitalId,
                  toHospitalId,
                  targetBedId: candidateBed.id,
                  countdownSec: 900,
                  priority: priority >= 10 ? 'CRITICAL' : 'ROUTINE',
                },
              });
            } finally {
              await releaseLock(`bed:${candidateBed.id}`, lockToken);
            }
          }
        } else {
          // No bed available at receiving hospital: Reject transfer
          await pool.query('UPDATE patient_transfers SET step = "REJECTED" WHERE id = ?', [transferId]);
          await publishWsFanout('ws:fanout', {
            type: 'transferRejected',
            payload: { transferId, reason: 'Target hospital has no available beds in department' },
          });
        }
      } else if (action === 'ACCEPT') {
        // Receiving hospital accepted the transfer
        await pool.query('UPDATE patient_transfers SET step = "EN_ROUTE" WHERE id = ?', [transferId]);
        await publishWsFanout('ws:fanout', {
          type: 'transferEnRoute',
          payload: { transferId },
        });
      } else if (action === 'COMPLETE') {
        // Patient arrived: source bed is cleaned/freed, target bed occupied
        if (sourceBedId) {
          await pool.query('UPDATE beds SET status = "CLEANING", assigned_actor = NULL WHERE id = ?', [sourceBedId]);
        }
        if (targetBedId) {
          await pool.query('UPDATE beds SET status = "OCCUPIED" WHERE id = ?', [targetBedId]);
        }
        await pool.query('UPDATE patient_transfers SET step = "COMPLETED" WHERE id = ?', [transferId]);
        await publishWsFanout('ws:fanout', {
          type: 'transferCompleted',
          payload: { transferId },
        });
      } else if (action === 'REJECT' || action === 'TIMEOUT') {
        // SAGA COMPENSATING TRANSACTION:
        // Receiving hospital rejected or timed out: target bed released, transfer marked rolled back
        if (targetBedId) {
          await pool.query('UPDATE beds SET status = "FREE", assigned_actor = NULL WHERE id = ?', [targetBedId]);
        }
        await pool.query('UPDATE patient_transfers SET step = "ROLLED_BACK" WHERE id = ?', [transferId]);
        logger.info({ transferId }, 'Saga compensation: transfer cancelled & beds rolled back');

        await publishWsFanout('ws:fanout', {
          type: 'transferRolledBack',
          payload: { transferId, reason: payload.reason || 'Hospital declined or timed out' },
        });
      }

      ch.ack(msg);
    } catch (err) {
      logger.error({ err }, 'Error executing transfer saga workflow');
      handleFailure(ch, msg);
    }
  });

  // -------------------------------------------------------------
  // 3. Dead Letter Queue Consumer (Dead Letter Visibility)
  // -------------------------------------------------------------
  await ch.consume(QUEUES.DEAD_LETTER, async (msg: any) => {
    if (!msg) return;

    try {
      const content = msg.content.toString();
      const routingKey = msg.fields.routingKey;
      const deathHeader = msg.properties.headers?.['x-death'];
      const reason = deathHeader ? deathHeader[0]?.reason : 'rejected';

      await pool.query(
        'INSERT INTO dead_letter_log (routing_key, reason, payload) VALUES (?, ?, ?)',
        [routingKey, reason, content]
      );

      logger.warn({ routingKey, reason }, 'Logged message to dead_letter_log in MySQL');
      ch.ack(msg);
    } catch (err) {
      logger.error({ err }, 'Error in DLQ consumer');
      ch.ack(msg);
    }
  });
}

function handleFailure(ch: any, msg: any) {
  const retryCount = (msg.properties.headers?.['x-retry-count'] || 0) + 1;
  if (retryCount > 3) {
    logger.warn({ retryCount }, 'Max retries exceeded. Rejecting to Dead Letter Exchange (DLX).');
    ch.reject(msg, false); // Route to DLX
  } else {
    ch.reject(msg, false);
  }
}

startWorker().catch((err) => {
  logger.error({ err }, 'Worker fatal error');
  process.exit(1);
});
