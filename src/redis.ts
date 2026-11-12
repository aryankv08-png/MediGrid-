import Redis from 'ioredis';
import pino from 'pino';

const logger = pino({ name: 'redis' });

let client: Redis | null = null;
let subClient: Redis | null = null;

export function getRedisClient(): Redis {
  if (client) return client;

  client = new Redis({
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: Number(process.env.REDIS_PORT) || 6379,
    maxRetriesPerRequest: 3,
  });

  client.on('connect', () => logger.info('Redis connected'));
  client.on('error', (err) => logger.error({ err }, 'Redis connection error'));

  return client;
}

export function getRedisSubClient(): Redis {
  if (subClient) return subClient;

  subClient = new Redis({
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: Number(process.env.REDIS_PORT) || 6379,
  });

  return subClient;
}

/**
 * Distributed Lock without Redlock/frameworks:
 * Uses atomic SET resource_key token NX EX ttlSec
 */
export async function acquireLock(resource: string, token: string, ttlSeconds: number = 10): Promise<boolean> {
  const r = getRedisClient();
  const res = await r.set(`lock:${resource}`, token, 'EX', ttlSeconds, 'NX');
  return res === 'OK';
}

/**
 * Release lock safely using Lua script to verify token ownership
 */
export async function releaseLock(resource: string, token: string): Promise<boolean> {
  const r = getRedisClient();
  const lua = `
    if redis.call("get", KEYS[1]) == ARGV[1] then
      return redis.call("del", KEYS[1])
    else
      return 0
    end
  `;
  const res = await r.eval(lua, 1, `lock:${resource}`, token);
  return res === 1;
}

/**
 * Rate Limiter per Actor ID (Sliding fixed window)
 */
export async function checkRateLimit(
  actorId: string,
  limit: number = 120,
  windowSec: number = 60
): Promise<{ allowed: boolean; remaining: number }> {
  try {
    const r = getRedisClient();
    const key = `ratelimit:${actorId}`;
    const count = await r.incr(key);
    if (count === 1) {
      await r.expire(key, windowSec);
    }
    return {
      allowed: count <= limit,
      remaining: Math.max(0, limit - count),
    };
  } catch (err) {
    logger.warn({ err }, 'Rate limiter failed open');
    return { allowed: true, remaining: limit };
  }
}

/**
 * Redis Pub/Sub: Fan-out events across multiple application instances
 */
export async function publishWsFanout(channel: string, message: any): Promise<void> {
  const r = getRedisClient();
  await r.publish(channel, JSON.stringify(message));
}
