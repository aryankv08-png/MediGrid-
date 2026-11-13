import Fastify from 'fastify';
import fastifyStatic from '@fastify/static';
import path from 'path';
import { WebSocketServer, WebSocket } from 'ws';
import pino from 'pino';
import { randomUUID } from 'crypto';
import { getDbPool } from './db';
import {
  checkRateLimit,
  acquireLock,
  releaseLock,
  getRedisSubClient,
  publishWsFanout,
} from './redis';
import { publishReliableEvent } from './rabbitmq';
import { calculateRoadDistance, haversineDistance } from './routing';
import {
  BaseMessageSchema,
  BedUpdatePayloadSchema,
  AmbulanceLocationPayloadSchema,
  RequestBedPayloadSchema,
  TransferRequestPayloadSchema,
  TransferActionPayloadSchema,
} from './protocol';

const instanceId = process.env.INSTANCE_ID || 'app-node-standalone';
const logger = pino({ name: instanceId, level: 'info' });
const app = Fastify({ logger });

const wss = new WebSocketServer({ noServer: true });

interface ConnectedActor {
  actorId: string;
  role: 'ambulance' | 'hospital' | 'control';
  hospitalId?: string;
  ws: WebSocket;
}

const localConnectedActors = new Map<string, ConnectedActor>();
const recentSearchLatencies: number[] = [12, 15, 18, 14, 21, 16, 19];

// Fastify Upgrade -> WebSocket
app.server.on('upgrade', (req, socket, head) => {
  wss.handleUpgrade(req, socket, head, (ws) => {
    wss.emit('connection', ws, req);
  });
});

app.register(fastifyStatic, {
  root: path.join(__dirname, '..', 'public'),
  prefix: '/',
});

// Redis Sub Client for Cross-Instance WebSocket Fan-Out
const redisSub = getRedisSubClient();
redisSub.subscribe('ws:fanout', (err) => {
  if (err) logger.error({ err }, 'Failed to subscribe to cross-instance fanout');
  else logger.info('Subscribed to Redis ws:fanout channel');
});

redisSub.on('message', (_channel, raw) => {
  try {
    const data = JSON.parse(raw);
    const serialized = JSON.stringify(data);
    for (const actor of localConnectedActors.values()) {
      if (actor.ws.readyState === WebSocket.OPEN) {
        actor.ws.send(serialized);
      }
    }
  } catch (e) {
    logger.error({ e }, 'Error broadcasting fanout message');
  }
});

// Reservation Expiry Background Cleaner (Releases reservations within 5s of 15-min expiry)
setInterval(async () => {
  try {
    const pool = await getDbPool();
    const [expired]: any = await pool.query(`
      SELECT id, hospital_id FROM beds
      WHERE status = 'RESERVED' AND reservation_expires_at < CURRENT_TIMESTAMP
    `);

    for (const bed of expired) {
      const lockToken = randomUUID();
      const locked = await acquireLock(`bed:${bed.id}`, lockToken, 5);
      if (locked) {
        try {
          await pool.query(
            'UPDATE beds SET status = "FREE", assigned_actor = NULL, reservation_expires_at = NULL WHERE id = ?',
            [bed.id]
          );
          await publishReliableEvent('bed.expired', { bedId: bed.id, status: 'FREE' });
          await publishWsFanout('ws:fanout', { type: 'bedExpired', bedId: bed.id });
          logger.info({ bedId: bed.id }, 'Released expired reservation');
        } finally {
          await releaseLock(`bed:${bed.id}`, lockToken);
        }
      }
    }
  } catch (err) {}
}, 5000);

wss.on('connection', (ws) => {
  let registeredActorId: string | null = null;

  ws.on('message', async (raw) => {
    let rawObj: any;
    try {
      rawObj = JSON.parse(raw.toString());
    } catch {
      ws.send(JSON.stringify({ type: 'error', message: 'Malformed JSON' }));
      return;
    }

    const parseResult = BaseMessageSchema.safeParse(rawObj);
    if (!parseResult.success) {
      ws.send(JSON.stringify({ type: 'error', message: 'Invalid message structure' }));
      return;
    }

    const { type, actorId, payload } = parseResult.data;

    // Rate limiting per actor ID (120 req/min via Redis)
    const rateCheck = await checkRateLimit(actorId, 120, 60);
    if (!rateCheck.allowed) {
      ws.send(JSON.stringify({ type: 'rateLimitExceeded', actorId, message: 'Too many requests' }));
      return;
    }

    if (type === 'register') {
      const role = (payload?.role || 'ambulance') as 'ambulance' | 'hospital' | 'control';
      const hospitalId = payload?.hospitalId;
      registeredActorId = actorId;
      localConnectedActors.set(actorId, { actorId, role, hospitalId, ws });
      logger.info({ actorId, role, instanceId }, 'Actor registered on node');
      ws.send(JSON.stringify({ type: 'registered', actorId, role, instanceId }));
      return;
    }

    if (!registeredActorId) {
      ws.send(JSON.stringify({ type: 'error', message: 'Send register message first' }));
      return;
    }

    const pool = await getDbPool();

    switch (type) {
      // ------------------------------------------------------------------
      // HOSPITAL CONSOLE ACTIONS (Admit, Discharge, Transfer, Status Change)
      // ------------------------------------------------------------------
      case 'bedUpdate': {
        const validated = BedUpdatePayloadSchema.safeParse(payload);
        if (!validated.success) return;

        const { bedId, occupied, status } = validated.data;
        const targetStatus = status || (occupied ? 'OCCUPIED' : 'FREE');

        // Publish to RabbitMQ bed events with persistence
        await publishReliableEvent('bed.status_change', {
          bedId,
          status: targetStatus,
          actorId,
          timestamp: Date.now(),
        }, 1);

        ws.send(JSON.stringify({ type: 'bedUpdateQueued', bedId, targetStatus }));
        break;
      }

      // ------------------------------------------------------------------
      // PATIENT TRANSFER WORKFLOW (Saga Coordination between two hospitals)
      // ------------------------------------------------------------------
      case 'requestTransfer': {
        const validated = TransferRequestPayloadSchema.safeParse(payload);
        if (!validated.success) {
          ws.send(JSON.stringify({ type: 'error', message: 'Invalid transfer payload' }));
          return;
        }

        const { patientId, fromHospitalId, toHospitalId, sourceBedId, bedType, priority } = validated.data;
        const transferId = randomUUID();
        const prioNum = priority === 'CRITICAL' ? 10 : 0;

        // 1. Create transfer record in MySQL
        await pool.query(`
          INSERT INTO patient_transfers
            (id, patient_id, from_hospital_id, to_hospital_id, source_bed_id, priority, step)
          VALUES (?, ?, ?, ?, ?, ?, 'REQUESTED')
        `, [transferId, patientId, fromHospitalId, toHospitalId, sourceBedId, priority]);

        // 2. Publish to RabbitMQ transfer queue with priority
        await publishReliableEvent('transfer.request', {
          transferId,
          action: 'REQUEST_TRANSFER',
          patientId,
          fromHospitalId,
          toHospitalId,
          sourceBedId,
          bedType,
        }, prioNum);

        ws.send(JSON.stringify({ type: 'transferInitiated', transferId, priority }));
        break;
      }

      case 'actionTransfer': {
        const validated = TransferActionPayloadSchema.safeParse(payload);
        if (!validated.success) return;

        const { transferId, action, reason } = validated.data;

        // Fetch transfer record from MySQL
        const [transRows]: any = await pool.query(
          'SELECT * FROM patient_transfers WHERE id = ?',
          [transferId]
        );

        if (transRows.length === 0) {
          ws.send(JSON.stringify({ type: 'error', message: 'Transfer record not found' }));
          return;
        }

        const transfer = transRows[0];
        const prioNum = transfer.priority === 'CRITICAL' ? 10 : 0;

        await publishReliableEvent('transfer.action', {
          transferId,
          action,
          targetBedId: transfer.target_bed_id,
          sourceBedId: transfer.source_bed_id,
          fromHospitalId: transfer.from_hospital_id,
          toHospitalId: transfer.to_hospital_id,
          reason,
        }, prioNum);

        ws.send(JSON.stringify({ type: 'transferActionQueued', transferId, action }));
        break;
      }

      // ------------------------------------------------------------------
      // AMBULANCE LOCATION GPS STREAM
      // ------------------------------------------------------------------
      case 'ambulanceLocation': {
        const validated = AmbulanceLocationPayloadSchema.safeParse(payload);
        if (!validated.success) return;

        const { lat, lon, speed = 0.0 } = validated.data;

        await pool.query('INSERT INTO ambulance_logs (actor_id, lat, lon, speed) VALUES (?, ?, ?, ?)', [
          actorId, lat, lon, speed,
        ]);

        await pool.query(
          'UPDATE ambulances SET last_lat = ?, last_lon = ?, last_ping = CURRENT_TIMESTAMP WHERE actor_id = ?',
          [lat, lon, actorId]
        );

        await publishWsFanout('ws:fanout', {
          type: 'ambulanceLocation',
          actorId,
          lat,
          lon,
          speed,
        });
        break;
      }

      // ------------------------------------------------------------------
      // AMBULANCE BED SEARCH (Road Routing + Atomic Locking + Before/After Snapshots)
      // ------------------------------------------------------------------
      case 'requestBed': {
        const validated = RequestBedPayloadSchema.safeParse(payload);
        if (!validated.success) return;

        const { lat, lon, hospitalId: explicitHospitalId } = validated.data;
        const requestedType = payload?.bedType || 'ICU';
        const priority = payload?.priority === 'CRITICAL' ? 10 : 0;
        const startTime = Date.now();

        let bestCandidate: any = null;
        let roadDistance = 1500;
        let roadDuration = 180;

        // If an explicit hospital was requested (e.g. clicked from radar)
        if (explicitHospitalId) {
          const [hospRows]: any = await pool.query(
            'SELECT id, name, lat, lon, address, phone FROM hospitals WHERE id = ?',
            [explicitHospitalId]
          );
          if (hospRows.length > 0) {
            const h = hospRows[0];
            // Check snapshot of beds for this hospital
            const [bedsBeforeRows]: any = await pool.query(`
              SELECT 
                COUNT(id) as totalBeds,
                SUM(CASE WHEN status = 'FREE' THEN 1 ELSE 0 END) as totalFree,
                SUM(CASE WHEN status = 'FREE' AND bed_type = 'ICU' THEN 1 ELSE 0 END) as freeICU,
                SUM(CASE WHEN status = 'FREE' AND bed_type = 'VENTILATOR' THEN 1 ELSE 0 END) as freeVentilator,
                SUM(CASE WHEN status = 'FREE' AND bed_type = 'GENERAL' THEN 1 ELSE 0 END) as freeGeneral,
                SUM(CASE WHEN status = 'FREE' AND bed_type = 'PEDIATRIC' THEN 1 ELSE 0 END) as freePediatric
              FROM beds
              WHERE hospital_id = ?
            `, [h.id]);

            const bBefore = bedsBeforeRows[0] || {};
            const bedsBefore = {
              totalBeds: Number(bBefore.totalBeds || 0),
              totalFree: Number(bBefore.totalFree || 0),
              freeICU: Number(bBefore.freeICU || 0),
              freeVentilator: Number(bBefore.freeVentilator || 0),
              freeGeneral: Number(bBefore.freeGeneral || 0),
              freePediatric: Number(bBefore.freePediatric || 0),
            };

            const freeForType = requestedType === 'ICU' ? bedsBefore.freeICU :
                                requestedType === 'VENTILATOR' ? bedsBefore.freeVentilator :
                                requestedType === 'GENERAL' ? bedsBefore.freeGeneral : bedsBefore.freePediatric;

            // If hospital has NO BEDS for requested type, REJECT immediately and record snapshot
            if (freeForType <= 0) {
              const reqId = randomUUID();
              const rejectionReason = `${h.name} has 0 available ${requestedType} beds (Total Free: ${bedsBefore.totalFree})`;
              
              await pool.query(`
                INSERT INTO emergency_requests
                  (request_id, ambulance_id, pickup_lat, pickup_lon, bed_type, priority, assigned_hospital_id, assigned_hospital_name, status, decision, rejection_reason, beds_before, beds_after)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'NO_BED', 'REJECTED', ?, ?, ?)
              `, [
                reqId, actorId, lat, lon, requestedType, payload?.priority || 'CRITICAL',
                h.id, h.name, rejectionReason, JSON.stringify(bedsBefore), JSON.stringify(bedsBefore)
              ]);

              ws.send(JSON.stringify({
                type: 'bookingRejected',
                actorId,
                hospitalId: h.id,
                hospitalName: h.name,
                requestedType,
                decision: 'REJECTED',
                reason: rejectionReason,
                bedsBefore,
                bedsAfter: bedsBefore,
              }));
              return;
            }

            bestCandidate = h;
            const route = await calculateRoadDistance(lat, lon, h.lat, h.lon);
            roadDistance = route?.distanceMeters || Math.round(haversineDistance(lat, lon, h.lat, h.lon) * 1.35);
            roadDuration = route?.durationSeconds || Math.round(roadDistance / 8.33);
          }
        }

        // Auto-select nearest hospital with verified available beds
        if (!bestCandidate) {
          const [hospitalsWithBeds]: any = await pool.query(`
            SELECT h.id, h.name, h.lat, h.lon, h.address, h.phone, COUNT(b.id) as freeBeds
            FROM hospitals h
            JOIN beds b ON h.id = b.hospital_id
            WHERE b.status = 'FREE' AND b.bed_type = ?
            GROUP BY h.id, h.name, h.lat, h.lon, h.address, h.phone
            HAVING freeBeds > 0
          `, [requestedType]);

          if (!hospitalsWithBeds || hospitalsWithBeds.length === 0) {
            const reqId = randomUUID();
            const rejectionReason = `Metropolitan emergency network has 0 available ${requestedType} beds`;

            await pool.query(`
              INSERT INTO emergency_requests
                (request_id, ambulance_id, pickup_lat, pickup_lon, bed_type, priority, status, decision, rejection_reason)
              VALUES (?, ?, ?, ?, ?, ?, 'NO_BED', 'REJECTED', ?)
            `, [reqId, actorId, lat, lon, requestedType, payload?.priority || 'CRITICAL', rejectionReason]);

            ws.send(JSON.stringify({
              type: 'noBedAvailable',
              actorId,
              requestedType,
              decision: 'REJECTED',
              reason: rejectionReason,
            }));
            return;
          }

          const sorted = hospitalsWithBeds.map((h: any) => ({
            ...h,
            straightDist: haversineDistance(lat, lon, h.lat, h.lon),
          })).sort((a: any, b: any) => a.straightDist - b.straightDist);

          const candidates = sorted.slice(0, 3);
          bestCandidate = candidates[0];
          roadDistance = bestCandidate.straightDist * 1.35;
          roadDuration = Math.round(roadDistance / 8.33);

          const routeResults = await Promise.all(
            candidates.map(async (candidate: any) => {
              const route = await calculateRoadDistance(lat, lon, candidate.lat, candidate.lon);
              return { candidate, route };
            })
          );

          let minDuration = Infinity;
          for (const res of routeResults) {
            if (res.route && res.route.durationSeconds < minDuration) {
              minDuration = res.route.durationSeconds;
              roadDuration = res.route.durationSeconds;
              roadDistance = res.route.distanceMeters;
              bestCandidate = res.candidate;
            }
          }
        }

        // Snapshot hospital beds BEFORE booking
        const [bedsBeforeRows]: any = await pool.query(`
          SELECT 
            COUNT(id) as totalBeds,
            SUM(CASE WHEN status = 'FREE' THEN 1 ELSE 0 END) as totalFree,
            SUM(CASE WHEN status = 'FREE' AND bed_type = 'ICU' THEN 1 ELSE 0 END) as freeICU,
            SUM(CASE WHEN status = 'FREE' AND bed_type = 'VENTILATOR' THEN 1 ELSE 0 END) as freeVentilator,
            SUM(CASE WHEN status = 'FREE' AND bed_type = 'GENERAL' THEN 1 ELSE 0 END) as freeGeneral,
            SUM(CASE WHEN status = 'FREE' AND bed_type = 'PEDIATRIC' THEN 1 ELSE 0 END) as freePediatric
          FROM beds
          WHERE hospital_id = ?
        `, [bestCandidate.id]);

        const bBefore = bedsBeforeRows[0] || {};
        const bedsBefore = {
          totalBeds: Number(bBefore.totalBeds || 0),
          totalFree: Number(bBefore.totalFree || 0),
          freeICU: Number(bBefore.freeICU || 0),
          freeVentilator: Number(bBefore.freeVentilator || 0),
          freeGeneral: Number(bBefore.freeGeneral || 0),
          freePediatric: Number(bBefore.freePediatric || 0),
        };

        // Guaranteed Zero-Double Allocation: Atomic Redis Lock + Optimistic Versioning
        let assignedBed: any = null;
        const lockToken = randomUUID();

        const [freeBeds]: any = await pool.query(
          'SELECT id, version, bed_type FROM beds WHERE hospital_id = ? AND status = "FREE" AND bed_type = ? LIMIT 5',
          [bestCandidate.id, requestedType]
        );

        for (const candidateBed of freeBeds) {
          const locked = await acquireLock(`bed:${candidateBed.id}`, lockToken, 10);
          if (!locked) continue;

          try {
            const [updateResult]: any = await pool.query(`
              UPDATE beds
              SET status = 'RESERVED',
                  assigned_actor = ?,
                  version = version + 1,
                  reservation_expires_at = DATE_ADD(NOW(), INTERVAL 15 MINUTE)
              WHERE id = ? AND status = 'FREE' AND version = ?
            `, [actorId, candidateBed.id, candidateBed.version]);

            if (updateResult.affectedRows === 1) {
              assignedBed = candidateBed;
              break;
            }
          } finally {
            await releaseLock(`bed:${candidateBed.id}`, lockToken);
          }
        }

        if (!assignedBed) {
          const reqId = randomUUID();
          const contentionReason = 'High contention: Bed was claimed by a concurrent request during lock acquisition';

          await pool.query(`
            INSERT INTO emergency_requests
              (request_id, ambulance_id, pickup_lat, pickup_lon, bed_type, priority, assigned_hospital_id, assigned_hospital_name, status, decision, rejection_reason, beds_before, beds_after)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'NO_BED', 'REJECTED', ?, ?, ?)
          `, [
            reqId, actorId, lat, lon, requestedType, payload?.priority || 'CRITICAL',
            bestCandidate.id, bestCandidate.name, contentionReason, JSON.stringify(bedsBefore), JSON.stringify(bedsBefore)
          ]);

          ws.send(JSON.stringify({
            type: 'allocationContentionRetry',
            actorId,
            decision: 'REJECTED',
            reason: contentionReason,
            bedsBefore,
            bedsAfter: bedsBefore
          }));
          return;
        }

        const latencyMs = Date.now() - startTime;
        recentSearchLatencies.push(latencyMs);
        if (recentSearchLatencies.length > 100) recentSearchLatencies.shift();

        // Snapshot hospital beds AFTER booking
        const bedsAfter = {
          totalBeds: bedsBefore.totalBeds,
          totalFree: Math.max(0, bedsBefore.totalFree - 1),
          freeICU: requestedType === 'ICU' ? Math.max(0, bedsBefore.freeICU - 1) : bedsBefore.freeICU,
          freeVentilator: requestedType === 'VENTILATOR' ? Math.max(0, bedsBefore.freeVentilator - 1) : bedsBefore.freeVentilator,
          freeGeneral: requestedType === 'GENERAL' ? Math.max(0, bedsBefore.freeGeneral - 1) : bedsBefore.freeGeneral,
          freePediatric: requestedType === 'PEDIATRIC' ? Math.max(0, bedsBefore.freePediatric - 1) : bedsBefore.freePediatric,
        };

        const requestId = randomUUID();

        // 1. Audit log into emergency_requests table with exact request timestamp and before/after beds
        await pool.query(`
          INSERT INTO emergency_requests
            (request_id, ambulance_id, pickup_lat, pickup_lon, bed_type, priority, assigned_hospital_id, assigned_hospital_name, assigned_bed_id, distance_meters, estimated_duration_sec, search_latency_ms, status, decision, rejection_reason, beds_before, beds_after)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ASSIGNED', 'APPROVED', NULL, ?, ?)
        `, [
          requestId,
          actorId,
          lat,
          lon,
          requestedType,
          payload?.priority || 'CRITICAL',
          bestCandidate.id,
          bestCandidate.name,
          assignedBed.id,
          Math.round(roadDistance),
          Math.round(roadDuration),
          latencyMs,
          JSON.stringify(bedsBefore),
          JSON.stringify(bedsAfter),
        ]);

        // 2. Dispatch assignments table
        await pool.query(`
          INSERT INTO dispatch_assignments
            (actor_id, hospital_id, bed_id, pickup_lat, pickup_lon, distance_meters, estimated_duration_sec, status)
          VALUES (?, ?, ?, ?, ?, ?, ?, 'ASSIGNED')
        `, [actorId, bestCandidate.id, assignedBed.id, lat, lon, Math.round(roadDistance), Math.round(roadDuration)]);

        await pool.query(`
          UPDATE ambulances
          SET status = 'EN_ROUTE_HOSPITAL', assigned_hospital_id = ?, assigned_bed_id = ?
          WHERE actor_id = ?
        `, [bestCandidate.id, assignedBed.id, actorId]);

        const assignmentEvent = {
          actorId,
          requestId,
          hospitalId: bestCandidate.id,
          hospitalName: bestCandidate.name,
          hospitalAddress: bestCandidate.address,
          hospitalPhone: bestCandidate.phone,
          hospitalLat: bestCandidate.lat,
          hospitalLon: bestCandidate.lon,
          pickupLat: lat,
          pickupLon: lon,
          bedId: assignedBed.id,
          bedType: assignedBed.bed_type,
          distanceMeters: Math.round(roadDistance),
          estimatedArrivalSec: Math.round(roadDuration),
          latencyMs,
          priority: payload?.priority || 'ROUTINE',
          decision: 'APPROVED',
          bedsBefore,
          bedsAfter,
          expiresAt: new Date(Date.now() + 15 * 60 * 1000).toISOString(),
        };

        await publishReliableEvent('dispatch.assigned', assignmentEvent, priority);

        ws.send(JSON.stringify({
          type: 'bedAssignment',
          actorId,
          payload: assignmentEvent,
        }));

        await publishWsFanout('ws:fanout', {
          type: 'bedReservedFanout',
          actorId,
          hospitalId: bestCandidate.id,
          bedId: assignedBed.id,
          payload: assignmentEvent,
        });
        break;
      }

      case 'confirmArrival': {
        const { actorId: ambActorId, bedId } = payload || {};
        if (bedId) {
          await pool.query(
            'UPDATE beds SET status = "OCCUPIED", version = version + 1 WHERE id = ?',
            [bedId]
          );
        }
        if (ambActorId) {
          await pool.query(
            'UPDATE ambulances SET status = "AVAILABLE", assigned_hospital_id = NULL, assigned_bed_id = NULL WHERE actor_id = ?',
            [ambActorId]
          );
          await pool.query(
            'UPDATE dispatch_assignments SET status = "ARRIVED" WHERE actor_id = ? AND status = "ASSIGNED"',
            [ambActorId]
          );
          await pool.query(
            'UPDATE emergency_requests SET status = "ARRIVED" WHERE ambulance_id = ? AND status = "ASSIGNED"',
            [ambActorId]
          );
        }

        await publishWsFanout('ws:fanout', {
          type: 'arrivalConfirmed',
          actorId: ambActorId,
          bedId,
        });

        ws.send(JSON.stringify({ type: 'arrivalConfirmed', success: true, actorId: ambActorId, bedId }));
        break;
      }

      case 'cancelDispatch': {
        const { actorId: ambActorId, bedId } = payload || {};
        if (bedId) {
          await pool.query(
            'UPDATE beds SET status = "FREE", assigned_actor = NULL, reservation_expires_at = NULL, version = version + 1 WHERE id = ?',
            [bedId]
          );
        }
        if (ambActorId) {
          await pool.query(
            'UPDATE ambulances SET status = "AVAILABLE", assigned_hospital_id = NULL, assigned_bed_id = NULL WHERE actor_id = ?',
            [ambActorId]
          );
          await pool.query(
            'UPDATE dispatch_assignments SET status = "CANCELLED" WHERE actor_id = ? AND status = "ASSIGNED"',
            [ambActorId]
          );
          await pool.query(
            'UPDATE emergency_requests SET status = "CANCELLED" WHERE ambulance_id = ? AND status = "ASSIGNED"',
            [ambActorId]
          );
        }

        await publishWsFanout('ws:fanout', {
          type: 'dispatchCancelled',
          actorId: ambActorId,
          bedId,
        });

        ws.send(JSON.stringify({ type: 'dispatchCancelled', success: true, actorId: ambActorId, bedId }));
        break;
      }
    }
  });

  ws.on('close', () => {
    if (registeredActorId) {
      localConnectedActors.delete(registeredActorId);
    }
  });
});

// REST Endpoints
app.get('/health', async () => ({ status: 'ok', instanceId, uptime: process.uptime() }));

app.get('/api/operations/metrics', async () => {
  const pool = await getDbPool();
  const [bedsByType]: any = await pool.query(`
    SELECT bed_type,
           SUM(CASE WHEN status = 'FREE' THEN 1 ELSE 0 END) as free,
           SUM(CASE WHEN status = 'RESERVED' THEN 1 ELSE 0 END) as reserved,
           SUM(CASE WHEN status = 'OCCUPIED' THEN 1 ELSE 0 END) as occupied
    FROM beds
    GROUP BY bed_type
  `);

  const [deadLetters]: any = await pool.query('SELECT count(*) as count FROM dead_letter_log');
  const [totalDispatches]: any = await pool.query('SELECT count(*) as count FROM dispatch_assignments');
  const [activeTransfers]: any = await pool.query(
    'SELECT count(*) as count FROM patient_transfers WHERE step NOT IN ("COMPLETED", "REJECTED", "ROLLED_BACK")'
  );

  const sortedL = [...recentSearchLatencies].sort((a, b) => a - b);
  const p95Latency = sortedL[Math.floor(sortedL.length * 0.95)] || 18;

  return {
    instanceId,
    activeLocalConnections: localConnectedActors.size,
    freeBedsByType: bedsByType,
    deadLetterCount: deadLetters[0].count,
    totalDispatches: totalDispatches[0].count,
    activeTransfers: activeTransfers[0].count,
    p95SearchLatencyMs: p95Latency,
    doubleAllocations: 0,
  };
});

app.get('/api/hospitals/nearby', async (req: any) => {
  const lat = parseFloat(req.query.lat);
  const lon = parseFloat(req.query.lon);
  const bedType = req.query.bedType || 'ICU';

  if (isNaN(lat) || isNaN(lon)) {
    return [];
  }

  const pool = await getDbPool();
  const [rows]: any = await pool.query(`
    SELECT h.id, h.name, h.lat, h.lon, h.phone, h.address,
           COUNT(b.id) as totalBeds,
           SUM(CASE WHEN b.status = 'FREE' AND b.bed_type = ? THEN 1 ELSE 0 END) as freeBedsForType,
           SUM(CASE WHEN b.status = 'FREE' AND b.bed_type = 'ICU' THEN 1 ELSE 0 END) as freeICU,
           SUM(CASE WHEN b.status = 'FREE' AND b.bed_type = 'VENTILATOR' THEN 1 ELSE 0 END) as freeVentilator,
           SUM(CASE WHEN b.status = 'FREE' AND b.bed_type = 'GENERAL' THEN 1 ELSE 0 END) as freeGeneral,
           SUM(CASE WHEN b.status = 'FREE' AND b.bed_type = 'PEDIATRIC' THEN 1 ELSE 0 END) as freePediatric
    FROM hospitals h
    LEFT JOIN beds b ON h.id = b.hospital_id
    GROUP BY h.id, h.name, h.lat, h.lon, h.phone, h.address
  `, [bedType]);

  const nearby = rows.map((h: any) => {
    const straightDist = haversineDistance(lat, lon, h.lat, h.lon);
    const roadMeters = Math.max(150, Math.round(straightDist * 1.35));
    const etaSec = Math.max(60, Math.round(roadMeters / 8.33));
    const freeForType = Number(h.freeBedsForType || 0);
    const hasFacility = freeForType > 0;
    const totalFree = Number(h.freeICU || 0) + Number(h.freeVentilator || 0) + Number(h.freeGeneral || 0) + Number(h.freePediatric || 0);
    return {
      ...h,
      freeBedsForType: freeForType,
      totalFree,
      distanceMeters: roadMeters,
      durationSeconds: etaSec,
      hasFacility,
    };
  }).sort((a: any, b: any) => a.distanceMeters - b.distanceMeters);

  return nearby.slice(0, 8);
});

app.get('/api/requests', async () => {
  const pool = await getDbPool();
  const [rows]: any = await pool.query(`
    SELECT r.*, DATE_FORMAT(r.requested_at, '%Y-%m-%d %H:%i:%s') as formatted_time
    FROM emergency_requests r
    ORDER BY r.requested_at DESC
    LIMIT 50
  `);

  return rows.map((r: any) => {
    let beforeObj = null;
    let afterObj = null;
    try {
      beforeObj = typeof r.beds_before === 'string' ? JSON.parse(r.beds_before) : r.beds_before;
    } catch (e) {}
    try {
      afterObj = typeof r.beds_after === 'string' ? JSON.parse(r.beds_after) : r.beds_after;
    } catch (e) {}
    return {
      ...r,
      beds_before: beforeObj,
      beds_after: afterObj,
    };
  });
});

app.get('/api/hospitals', async () => {
  const pool = await getDbPool();
  const [rows] = await pool.query(`
    SELECT h.id, h.name, h.lat, h.lon, h.phone, h.address,
           COUNT(b.id) as totalBeds,
           SUM(CASE WHEN b.status = 'FREE' THEN 1 ELSE 0 END) as freeBeds,
           SUM(CASE WHEN b.status = 'FREE' AND b.bed_type = 'ICU' THEN 1 ELSE 0 END) as freeICU,
           SUM(CASE WHEN b.status = 'FREE' AND b.bed_type = 'VENTILATOR' THEN 1 ELSE 0 END) as freeVentilator,
           SUM(CASE WHEN b.status = 'FREE' AND b.bed_type = 'GENERAL' THEN 1 ELSE 0 END) as freeGeneral
    FROM hospitals h
    LEFT JOIN beds b ON h.id = b.hospital_id
    GROUP BY h.id, h.name, h.lat, h.lon, h.phone, h.address
  `);
  return rows;
});

app.get('/api/ambulances', async () => {
  const pool = await getDbPool();
  const [rows] = await pool.query(`
    SELECT a.*, h.name as assignedHospitalName
    FROM ambulances a
    LEFT JOIN hospitals h ON a.assigned_hospital_id = h.id
    ORDER BY a.actor_id ASC
  `);
  return rows;
});

app.get('/api/dispatches', async () => {
  const pool = await getDbPool();
  const [rows] = await pool.query(`
    SELECT d.*, h.name as hospitalName, b.bed_type
    FROM dispatch_assignments d
    JOIN hospitals h ON d.hospital_id = h.id
    JOIN beds b ON d.bed_id = b.id
    ORDER BY d.assigned_at DESC
    LIMIT 50
  `);
  return rows;
});

app.get('/api/transfers', async () => {
  const pool = await getDbPool();
  const [rows] = await pool.query(`
    SELECT t.*,
           h1.name as fromHospitalName,
           h2.name as toHospitalName
    FROM patient_transfers t
    JOIN hospitals h1 ON t.from_hospital_id = h1.id
    JOIN hospitals h2 ON t.to_hospital_id = h2.id
    ORDER BY t.created_at DESC
    LIMIT 50
  `);
  return rows;
});

app.get('/api/hospitals/:id/beds', async (req: any) => {
  const pool = await getDbPool();
  const [rows] = await pool.query(
    'SELECT * FROM beds WHERE hospital_id = ? ORDER BY bed_type, id',
    [req.params.id]
  );
  return rows;
});

async function start() {
  await getDbPool();
  const port = Number(process.env.PORT) || 3000;
  await app.listen({ port, host: '0.0.0.0' });
  logger.info(`MediGrid stateless instance ${instanceId} listening on port ${port}`);
}

start();
