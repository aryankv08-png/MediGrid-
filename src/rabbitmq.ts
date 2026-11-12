import amqp from 'amqplib';
import pino from 'pino';

const logger = pino({ name: 'rabbitmq' });

let conn: any = null;
let channel: any = null;

export const MAIN_EXCHANGE = 'medigrid.events';
export const DLX_EXCHANGE = 'medigrid.dlx';

export const QUEUES = {
  BED_EVENTS: 'medigrid.queue.bed_events',
  TRANSFERS: 'medigrid.queue.transfers',
  DISPATCH: 'medigrid.queue.dispatch',
  DEAD_LETTER: 'medigrid.queue.dead_letter',
};

export async function getRabbitChannel(): Promise<any> {
  if (channel) return channel;

  const host = process.env.RABBITMQ_HOST || 'localhost';
  conn = await amqp.connect(`amqp://guest:guest@${host}:5672`);
  channel = await conn.createConfirmChannel(); // Publisher confirms enabled

  // 1. Assert Dead Letter Exchange (DLX) & Queue
  await channel.assertExchange(DLX_EXCHANGE, 'topic', { durable: true });
  await channel.assertQueue(QUEUES.DEAD_LETTER, { durable: true });
  await channel.bindQueue(QUEUES.DEAD_LETTER, DLX_EXCHANGE, '#');

  // 2. Assert Main Topic Exchange
  await channel.assertExchange(MAIN_EXCHANGE, 'topic', { durable: true });

  // 3. Assert Queues with DLX and Priority Support (maxPriority: 10)
  const queueOpts = {
    durable: true,
    maxPriority: 10,
    deadLetterExchange: DLX_EXCHANGE,
    deadLetterRoutingKey: 'dead_letter',
  };

  await channel.assertQueue(QUEUES.BED_EVENTS, queueOpts);
  await channel.bindQueue(QUEUES.BED_EVENTS, MAIN_EXCHANGE, 'bed.*');

  await channel.assertQueue(QUEUES.TRANSFERS, queueOpts);
  await channel.bindQueue(QUEUES.TRANSFERS, MAIN_EXCHANGE, 'transfer.*');

  await channel.assertQueue(QUEUES.DISPATCH, queueOpts);
  await channel.bindQueue(QUEUES.DISPATCH, MAIN_EXCHANGE, 'dispatch.*');

  logger.info('RabbitMQ initialized with Publisher Confirms, DLX, and Priority queues.');
  return channel;
}

/**
 * Publish event with publisher confirms, persistence, and priority
 */
export async function publishReliableEvent(
  routingKey: string,
  payload: any,
  priority: number = 0
): Promise<boolean> {
  try {
    const ch = await getRabbitChannel();
    const buffer = Buffer.from(JSON.stringify(payload));

    return new Promise((resolve) => {
      ch.publish(
        MAIN_EXCHANGE,
        routingKey,
        buffer,
        {
          persistent: true,
          priority: Math.min(10, Math.max(0, priority)),
          contentType: 'application/json',
          timestamp: Date.now(),
        },
        (err: any) => {
          if (err) {
            logger.error({ err, routingKey }, 'Failed to confirm message publication');
            resolve(false);
          } else {
            resolve(true);
          }
        }
      );
    });
  } catch (err) {
    logger.error({ err, routingKey }, 'Error publishing reliable event');
    return false;
  }
}
