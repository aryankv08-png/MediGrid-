# WebSocket Protocol for MediGrid

All messages are JSON objects that must conform to the **MessageSchema** defined in `src/index.ts` (Zod).  The base shape is:

```json
{
  "type": "<string>",
  "payload": { ... },
  "actorId": "<actor-id>"
}
```

- `type` – a short identifier for the kind of event.
- `payload` – event‑specific data (any JSON).  Each type has its own schema (documented below).
- `actorId` – the unique identifier of the sender (e.g., `ambulance-A-17`, `hospital-H-04`, `control-room`).  The first message **must** be a `register` message; after that the server stores the mapping of `actorId` → WebSocket.

## Mandatory first message – Register
```json
{
  "type": "register",
  "actorId": "ambulance-A-17",
  "payload": {
    "role": "ambulance"   // optional, can be "hospital" or "control"
  }
}
```
The server replies with:
```json
{ "type": "registered", "actorId": "ambulance-A-17" }
```
If the first message is not a `register`, the connection is closed with code 1002.

## Event messages
### `bedUpdate`
Sent by a hospital when a bed becomes occupied / free.
```json
{
  "type": "bedUpdate",
  "actorId": "hospital-H-04",
  "payload": {
    "bedId": "uuid",
    "occupied": true,
    "timestamp": 1731504000   // Unix seconds
  }
}
```
The server publishes the payload to RabbitMQ exchange `medigrid` using routing key `bed.update.<actorId>` and forwards the message to all **other** connected actors (control‑room, ambulances) that are interested.

### `ambulanceLocation`
Periodic GPS update from an ambulance.
```json
{
  "type": "ambulanceLocation",
  "actorId": "ambulance-A-17",
  "payload": {
    "lat": 12.9716,
    "lon": 77.5946,
    "timestamp": 1731504010
  }
}
```
The server forwards the update to any subscribed control‑room actors.  Optionally the payload can be routed through RabbitMQ for downstream analytics.

### `requestBed`
An ambulance asks for the nearest free bed.
```json
{
  "type": "requestBed",
  "actorId": "ambulance-A-17",
  "payload": {
    "lat": 12.9720,
    "lon": 77.5940
  }
}
```
The server (or a separate worker) will:
1. Call the OSRM service (`http://osrm:5000/route/v1/driving/...`) to compute road distances to each hospital.
2. Query the MySQL `beds` table for a free bed at the closest hospital.
3. Respond with a `bedAssignment` message (see below).

### `bedAssignment`
Sent by the server (or a worker) back to the requesting ambulance.
```json
{
  "type": "bedAssignment",
  "actorId": "ambulance-A-17",
  "payload": {
    "hospitalId": "uuid",
    "bedId": "uuid",
    "estimatedArrivalSec": 420
  }
}
```

## Error handling
If a payload fails validation, the server logs the error and **does not** close the connection; the offending message is ignored.  Critical protocol violations (e.g., missing `register`) close the socket with code 1002.

---
*All messages are expected to be processed within the 2 second latency budget (see design spec).*

