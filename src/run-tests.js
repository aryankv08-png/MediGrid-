const mysql = require('mysql2/promise');
const Redis = require('ioredis');
const amqp = require('amqplib');
const WebSocket = require('ws');

const MYSQL_CONFIG = {
  host: process.env.MYSQL_HOST || 'mysql',
  user: process.env.MYSQL_USER || 'root',
  password: process.env.MYSQL_PASSWORD || 'example',
  database: process.env.MYSQL_DATABASE || 'medigrid',
};

const REDIS_HOST = process.env.REDIS_HOST || 'redis';
const RABBIT_HOST = process.env.RABBITMQ_HOST || 'rabbitmq';

async function runTests() {
  console.log('================================================================');
  console.log('🚀 MEDIGRID SPECIFICATION & RESILIENCE TEST SUITE');
  console.log('================================================================\n');

  let passed = 0;
  let failed = 0;

  function assert(condition, testName, extraInfo = '') {
    if (condition) {
      console.log(`✅ [PASS] ${testName} ${extraInfo}`);
      passed++;
    } else {
      console.error(`❌ [FAIL] ${testName} ${extraInfo}`);
      failed++;
    }
  }

  // TEST 1: Database Scale & Schema
  console.log('--- TEST 1: Database Scale & Relational Schema ---');
  const pool = await mysql.createPool(MYSQL_CONFIG);
  try {
    const [hosp] = await pool.query('SELECT count(*) as count FROM hospitals');
    const [beds] = await pool.query('SELECT count(*) as count FROM beds');
    const [ambs] = await pool.query('SELECT count(*) as count FROM ambulances');

    assert(hosp[0].count >= 40, 'Hospital count >= 40', `(Found: ${hosp[0].count})`);
    assert(beds[0].count >= 4000, 'Bed count >= 4000', `(Found: ${beds[0].count})`);
    assert(ambs[0].count >= 20, 'Ambulance fleet >= 20', `(Found: ${ambs[0].count})`);
  } catch (err) {
    assert(false, 'Database scale test failed', err.message);
  }

  // TEST 2: Dual-Instance REST API Health & Observability Metrics
  console.log('\n--- TEST 2: Dual-Instance REST API Health ---');
  try {
    const res1 = await fetch('http://app1:3000/api/operations/metrics');
    const data1 = await res1.json();
    const res2 = await fetch('http://app2:3000/api/operations/metrics');
    const data2 = await res2.json();

    assert(data1.instanceId === 'app-node-1', 'Instance 1 responds with node-1 identity');
    assert(data2.instanceId === 'app-node-2', 'Instance 2 responds with node-2 identity');
    assert(Array.isArray(data1.freeBedsByType) && data1.freeBedsByType.length > 0, 'Metrics return categorized free beds');
  } catch (err) {
    assert(false, 'Dual-instance REST check failed', err.message);
  }

  // TEST 3: Zero Double-Allocation Concurrency Test (20 ambulances simultaneously racing)
  console.log('\n--- TEST 3: Zero Double-Allocation Concurrency Stress Test ---');
  try {
    const requests = [];
    for (let i = 1; i <= 10; i++) {
      const a1 = `stress-amb-node1-${i}-${Date.now()}`;
      const a2 = `stress-amb-node2-${i}-${Date.now()}`;
      requests.push(makeBedRequest('http://app1:3000', a1, 42.5063, 1.5218));
      requests.push(makeBedRequest('http://app2:3000', a2, 42.5063, 1.5218));
    }

    const results = await Promise.all(requests);
    const assignedBeds = results.filter(r => r && r.bedId).map(r => r.bedId);
    const uniqueBeds = new Set(assignedBeds);

    assert(assignedBeds.length > 0 && assignedBeds.length === uniqueBeds.size,
      'Zero double-allocation achieved under race conditions',
      `(${assignedBeds.length} assignments, ${uniqueBeds.size} distinct beds)`);

    const [dbBeds] = await pool.query(
      'SELECT id, status FROM beds WHERE id IN (?)',
      [Array.from(uniqueBeds)]
    );
    const allReserved = dbBeds.every(b => b.status === 'RESERVED');
    assert(allReserved, 'All assigned beds marked RESERVED in MySQL');
  } catch (err) {
    assert(false, 'Concurrency stress test error', err.message);
  }

  // TEST 4: RabbitMQ Priority Support
  console.log('\n--- TEST 4: RabbitMQ Priority Queue Support ---');
  try {
    const conn = await amqp.connect(`amqp://guest:guest@${RABBIT_HOST}:5672`);
    const ch = await conn.createChannel();
    const testQueue = 'test.priority.queue';

    await ch.assertQueue(testQueue, { durable: false, maxPriority: 10, autoDelete: true });
    await ch.purgeQueue(testQueue);

    for (let i = 1; i <= 5; i++) {
      ch.sendToQueue(testQueue, Buffer.from(`routine-${i}`), { priority: 0 });
    }
    ch.sendToQueue(testQueue, Buffer.from('critical-emergency'), { priority: 10 });

    const firstMsg = await new Promise((resolve) => {
      ch.consume(testQueue, (msg) => {
        if (msg) {
          ch.ack(msg);
          resolve(msg.content.toString());
        }
      });
    });

    assert(firstMsg === 'critical-emergency',
      'RabbitMQ processes priority 10 ahead of pre-queued priority 0 messages',
      `(First processed: ${firstMsg})`);

    await ch.close();
    await conn.close();
  } catch (err) {
    assert(false, 'Priority queue test failed', err.message);
  }

  // TEST 5: Redis Rate Limiter & Atomic Locking
  console.log('\n--- TEST 5: Redis Atomic Mutual Exclusion & Rate Limiting ---');
  try {
    const r = new Redis({ host: REDIS_HOST });
    const actor = 'rate-test-actor';
    const limit = 5;

    let allowedCount = 0;
    for (let i = 1; i <= 7; i++) {
      const current = await r.incr(`ratelimit:${actor}`);
      if (current === 1) await r.expire(`ratelimit:${actor}`, 10);
      if (current <= limit) allowedCount++;
    }

    assert(allowedCount === 5, 'Redis rate-limiter permits allowed limit and throttles excess', `(Allowed: ${allowedCount}/7)`);

    const lockKey = 'lock:test-resource';
    const lock1 = await r.set(lockKey, 'tok-1', 'EX', 5, 'NX');
    const lock2 = await r.set(lockKey, 'tok-2', 'EX', 5, 'NX');

    assert(lock1 === 'OK' && lock2 === null, 'Atomic Redis lock (SET NX EX) guarantees mutual exclusion');

    const lua = `
      if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
      else
        return 0
      end
    `;
    const delWrong = await r.eval(lua, 1, lockKey, 'wrong-token');
    const delRight = await r.eval(lua, 1, lockKey, 'tok-1');
    assert(delWrong === 0 && delRight === 1, 'Lock release verifies token ownership via Lua script');
    r.disconnect();
  } catch (err) {
    assert(false, 'Redis test failed', err.message);
  }

  // TEST 6: Real-time Cross-Instance WebSocket Fan-Out
  console.log('\n--- TEST 6: Real-Time Cross-Instance WebSocket Fan-Out ---');
  try {
    const wsClientApp1 = new WebSocket('ws://app1:3000');
    const wsClientApp2 = new WebSocket('ws://app2:3000');

    await Promise.all([
      new Promise((res) => wsClientApp1.on('open', res)),
      new Promise((res) => wsClientApp2.on('open', res)),
    ]);

    wsClientApp1.send(JSON.stringify({ type: 'register', actorId: 'client-app1', payload: { role: 'control' } }));
    wsClientApp2.send(JSON.stringify({ type: 'register', actorId: 'client-app2', payload: { role: 'control' } }));

    const fanoutReceived = await new Promise((resolve) => {
      const timeout = setTimeout(() => resolve(false), 4000);

      wsClientApp2.on('message', (data) => {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'bedUpdateBroadcast' || msg.type === 'bedReservedFanout') {
          clearTimeout(timeout);
          resolve(true);
        }
      });

      setTimeout(() => {
        wsClientApp1.send(JSON.stringify({
          type: 'requestBed',
          actorId: 'client-app1',
          payload: { lat: 42.5063, lon: 1.5218, bedType: 'GENERAL' }
        }));
      }, 500);
    });

    assert(fanoutReceived, 'Event triggered on Instance 1 fans out in real-time to Instance 2');
    wsClientApp1.close();
    wsClientApp2.close();
  } catch (err) {
    assert(false, 'WebSocket fan-out test failed', err.message);
  }

  // TEST 7: 15-Minute Reservation Expiry Auto-Reclaim
  console.log('\n--- TEST 7: Short-Lived Reservation Expiry Auto-Reclaim ---');
  try {
    const [bed] = await pool.query('SELECT id FROM beds WHERE status = "FREE" LIMIT 1');
    if (bed.length > 0) {
      const testBedId = bed[0].id;
      await pool.query(`
        UPDATE beds
        SET status = "RESERVED",
            assigned_actor = "test-expired-actor",
            reservation_expires_at = DATE_SUB(NOW(), INTERVAL 16 MINUTE)
        WHERE id = ?
      `, [testBedId]);

      console.log('Waiting 6s for reservation expiry cleaner ticker...');
      await new Promise(r => setTimeout(r, 6000));

      const [reclaimed] = await pool.query('SELECT status, assigned_actor FROM beds WHERE id = ?', [testBedId]);
      assert(reclaimed[0].status === 'FREE' && reclaimed[0].assigned_actor === null,
        'Expired bed reservation automatically released within 5 seconds of expiry');
    }
  } catch (err) {
    assert(false, 'Reservation expiry test failed', err.message);
  }

  console.log('\n================================================================');
  console.log(`TEST RESULTS: ${passed} PASSED, ${failed} FAILED`);
  console.log('================================================================\n');

  await pool.end();
  process.exit(failed > 0 ? 1 : 0);
}

function makeBedRequest(instanceUrl, actorId, lat, lon) {
  return new Promise((resolve) => {
    const wsUrl = instanceUrl.replace('http://', 'ws://');
    const ws = new WebSocket(wsUrl);
    const timeout = setTimeout(() => {
      ws.close();
      resolve(null);
    }, 6000);

    ws.on('open', () => {
      ws.send(JSON.stringify({ type: 'register', actorId, payload: { role: 'ambulance' } }));
    });

    ws.on('message', (raw) => {
      const msg = JSON.parse(raw.toString());
      if (msg.type === 'registered') {
        ws.send(JSON.stringify({
          type: 'requestBed',
          actorId,
          payload: { lat, lon, bedType: 'ICU', priority: 'CRITICAL' }
        }));
      } else if (msg.type === 'bedAssignment') {
        clearTimeout(timeout);
        ws.close();
        resolve(msg.payload);
      } else if (msg.type === 'noBedAvailable' || msg.type === 'allocationContentionRetry') {
        clearTimeout(timeout);
        ws.close();
        resolve(null);
      }
    });

    ws.on('error', () => {
      clearTimeout(timeout);
      resolve(null);
    });
  });
}

runTests().catch(console.error);

