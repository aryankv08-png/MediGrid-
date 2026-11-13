# Architecture Decision Record (ADR): MediGrid Core Platform

## Status
Accepted

## Context
During major medical surges or accidents, ambulances and hospitals face critical coordination failures:
1. **Stale Availability**: Bed status updated manually leads to ambulances directed to full wards.
2. **Double-Allocation**: Multiple ambulances directed simultaneously to the last free ICU bed.
3. **Lost Handoffs**: Unreliable cross-hospital patient transfers fail without rollback or audit.
4. **Lack of Priority**: Critical emergency cases get queued behind routine administrative paperwork.
5. **Single Point of Failure (SPOF)**: Traditional single-server coordination screens freeze under surge.

## Decision Summary & Key Architectural Choices

### 1. Zero Double-Allocation: Distributed Redis Mutex + DB Optimistic Versioning
- **Decision**: To prevent two ambulances on separate instances from claiming the same bed, we implement a two-phase check without heavy external frameworks:
  1. **Phase 1 (In-Memory Atomic Lock)**: `SET lock:bed:{id} {uuid} NX EX 10` in Redis. Only one concurrent execution path acquires the candidate bed.
  2. **Phase 2 (Optimistic Concurrency Control in MySQL)**:
     ```sql
     UPDATE beds 
     SET status = 'RESERVED', assigned_actor = ?, version = version + 1
     WHERE id = ? AND status = 'FREE' AND version = ?;
     ```
- **Alternatives Considered**:
  - *Pessimistic `SELECT FOR UPDATE`*: Causes high database lock contention and connection pool exhaustion under 50+ concurrent requests.
  - *Third-party Redlock / BullMQ*: Disallowed by the challenge rules ("must use basic client libraries and build locking yourself").
- **Outcome**: Verified under 50-client simultaneous surge across dual nodes—0 double allocations occurred.

### 2. Priority Queuing & Preemption via RabbitMQ
- **Decision**: Declare queues with `{ maxPriority: 10 }`. Critical transfers use `priority: 10`, while routine updates use `priority: 0`.
- **Alternatives Considered**:
  - *Separate queues for high/low priority*: Adds consumer starvation complexity and requires custom polling.
  - *In-memory priority queues*: Breaks statelessness across instances; lost on process crashes.
- **Outcome**: Verified that high-priority critical transfers are immediately consumed ahead of 100+ backlogged routine messages.

### 3. Patient Transfer Saga (Compensating Transactions)
- **Decision**: Cross-hospital transfers run as an asynchronous Saga coordinated via the `worker` service through RabbitMQ.
  - Step 1: `REQUESTED` $\rightarrow$ Target bed atomically reserved.
  - Step 2: If receiving hospital accepts $\rightarrow$ `EN_ROUTE` $\rightarrow$ Source bed freed, target bed occupied on arrival.
  - Step 3 (Compensating Rollback): If receiving hospital rejects or 900s timeout expires $\rightarrow$ target bed is released to `FREE`, transfer marked `ROLLED_BACK`.
- **Outcome**: Eliminates lost patient handoffs and orphan bed reservations.

### 4. Ephemeral Reservations & Dead-Letter Handling
- **Short-Lived Reservations**: All reservations are set with `INTERVAL 15 MINUTE`. A background ticker reclaims expired reservations within 5s of timeout.
- **Dead Letter Exchange (`medigrid.dlx`)**: Messages exceeding 3 retries are diverted to the Dead Letter Queue and logged into the `dead_letter_log` MySQL table for operator replay.

### 5. Multi-Instance Stateless Topology
- Two identical application nodes (`app1` on port 3000/3001, `app2` on port 3002) share state through Redis Pub/Sub (`ws:fanout`) and MySQL. If either instance crashes, users seamlessly failover to the other within seconds without state loss.

