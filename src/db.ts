import mysql, { Pool } from 'mysql2/promise';
import pino from 'pino';

const logger = pino({ name: 'db' });

let pool: Pool | null = null;

export async function getDbPool(): Promise<Pool> {
  if (pool) return pool;

  pool = mysql.createPool({
    host: process.env.MYSQL_HOST || 'localhost',
    user: process.env.MYSQL_USER || 'root',
    password: process.env.MYSQL_PASSWORD || 'example',
    database: process.env.MYSQL_DATABASE || 'medigrid',
    waitForConnections: true,
    connectionLimit: 25,
    enableKeepAlive: true,
    keepAliveInitialDelay: 10000,
  });

  await pool.query('SELECT 1');
  await initSchema(pool);
  logger.info('MySQL pool connected and schema fully migrated');
  return pool;
}

async function initSchema(p: Pool) {
  // 1. Hospitals (40+ hospitals)
  await p.query(`
    CREATE TABLE IF NOT EXISTS hospitals (
      id VARCHAR(36) PRIMARY KEY,
      name VARCHAR(120) NOT NULL,
      lat DOUBLE NOT NULL,
      lon DOUBLE NOT NULL,
      phone VARCHAR(30) DEFAULT '',
      address VARCHAR(255) DEFAULT '',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_lat_lon (lat, lon)
    );
  `);

  // 2. Beds (4,000+ beds) with version for optimistic locking & status tracking
  await p.query(`
    CREATE TABLE IF NOT EXISTS beds (
      id VARCHAR(36) PRIMARY KEY,
      hospital_id VARCHAR(36) NOT NULL,
      bed_type ENUM('ICU', 'VENTILATOR', 'GENERAL', 'PEDIATRIC') NOT NULL DEFAULT 'GENERAL',
      status ENUM('FREE', 'RESERVED', 'OCCUPIED', 'CLEANING') NOT NULL DEFAULT 'FREE',
      version INT NOT NULL DEFAULT 1,
      assigned_actor VARCHAR(64) NULL,
      reservation_expires_at TIMESTAMP NULL,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      INDEX idx_hosp_status_type (hospital_id, status, bed_type),
      CONSTRAINT fk_hosp_bed FOREIGN KEY (hospital_id) REFERENCES hospitals(id) ON DELETE CASCADE
    );
  `);

  // 3. Ambulances fleet
  await p.query(`
    CREATE TABLE IF NOT EXISTS ambulances (
      actor_id VARCHAR(64) PRIMARY KEY,
      plate_number VARCHAR(32) NOT NULL,
      status ENUM('AVAILABLE', 'EN_ROUTE_PICKUP', 'EN_ROUTE_HOSPITAL', 'OUT_OF_SERVICE') DEFAULT 'AVAILABLE',
      assigned_hospital_id VARCHAR(36) NULL,
      assigned_bed_id VARCHAR(36) NULL,
      last_lat DOUBLE NULL,
      last_lon DOUBLE NULL,
      last_ping TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      CONSTRAINT fk_amb_hosp FOREIGN KEY (assigned_hospital_id) REFERENCES hospitals(id) ON DELETE SET NULL
    );
  `);

  // 4. Transfers (Saga Workflow tracking between hospitals)
  await p.query(`
    CREATE TABLE IF NOT EXISTS patient_transfers (
      id VARCHAR(36) PRIMARY KEY,
      patient_id VARCHAR(64) NOT NULL,
      from_hospital_id VARCHAR(36) NOT NULL,
      to_hospital_id VARCHAR(36) NOT NULL,
      source_bed_id VARCHAR(36) NOT NULL,
      target_bed_id VARCHAR(36) NULL,
      priority ENUM('CRITICAL', 'ROUTINE') DEFAULT 'ROUTINE',
      step ENUM('REQUESTED', 'ACCEPTED', 'EN_ROUTE', 'COMPLETED', 'REJECTED', 'ROLLED_BACK') NOT NULL DEFAULT 'REQUESTED',
      transfer_timeout_sec INT DEFAULT 900,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      INDEX idx_patient (patient_id),
      INDEX idx_step (step)
    );
  `);

  // 4.b Dispatch assignments audit log
  await p.query(`
    CREATE TABLE IF NOT EXISTS dispatch_assignments (
      id INT AUTO_INCREMENT PRIMARY KEY,
      actor_id VARCHAR(64) NOT NULL,
      hospital_id VARCHAR(36) NOT NULL,
      bed_id VARCHAR(36) NOT NULL,
      pickup_lat DOUBLE NOT NULL,
      pickup_lon DOUBLE NOT NULL,
      distance_meters INT NOT NULL,
      estimated_duration_sec INT NOT NULL,
      status ENUM('ASSIGNED', 'ARRIVED', 'CANCELLED') DEFAULT 'ASSIGNED',
      assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_actor_status (actor_id, status)
    );
  `);

  // 5. Processed events (Idempotency table to prevent double-processing)
  await p.query(`
    CREATE TABLE IF NOT EXISTS processed_events (
      event_id VARCHAR(64) PRIMARY KEY,
      event_type VARCHAR(64) NOT NULL,
      aggregate_id VARCHAR(64) NOT NULL,
      processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  // 6. Dead letter log (audit visibility for DLX)
  await p.query(`
    CREATE TABLE IF NOT EXISTS dead_letter_log (
      id INT AUTO_INCREMENT PRIMARY KEY,
      routing_key VARCHAR(100),
      reason VARCHAR(255),
      payload JSON,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  // 7. High-frequency GPS history
  await p.query(`
    CREATE TABLE IF NOT EXISTS ambulance_logs (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      actor_id VARCHAR(64) NOT NULL,
      lat DOUBLE NOT NULL,
      lon DOUBLE NOT NULL,
      speed DOUBLE DEFAULT 0.0,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_actor_time (actor_id, created_at)
    );
  `);
}
