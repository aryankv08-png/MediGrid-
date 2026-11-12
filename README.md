# MediGrid Prototype

## Overview
This repository contains a minimal prototype of a **hospital‑ambulance coordination system** built with:
- **Fastify** for HTTP API
- **ws** for WebSocket communication
- **MySQL** for persistent data (hospitals, beds)
- **Redis** for caching / rate‑limiting
- **RabbitMQ** for pub/sub of events
- **OSRM** (OpenStreetMap routing) for real‑road distance calculations

All services are orchestrated via a single `docker‑compose.yml` so the whole stack runs on a single developer laptop (or any machine with Docker).

## Quick start (Windows)
```powershell
# 1️⃣ Build and start the stack
docker compose up -d

# 2️⃣ Wait for services to become healthy (especially MySQL). You can check with:
# docker compose ps

# 3️⃣ **OSRM data** – download and prepare routing files (requires network). Run the helper script **outside the sandbox**:
#   .\\scripts\\setup-osrm.ps1
#   This will fetch a small OSM extract (Andorra) and generate the OSRM files in ./osrm-data.
#   If you already have an OSM extract, place `region.osm.pbf` and the generated `.osrm*` files in the `osrm-data` folder.

# 4️⃣ Run the seed script to create sample hospitals & beds (inside the app container):
# docker compose exec app npm run seed

# 5️⃣ Verify the API and WS are reachable
curl http://localhost:3000/health
# you should get {"status":"ok"}

# 6️⃣ Open the driver UI (future work) – for now you can connect with any WS client:
# ws://localhost:3000 (first message must be a register payload, see protocol.md)
```

```powershell
# 1️⃣ Build and start the stack
docker compose up -d

# 2️⃣ Wait for MySQL to become healthy (you can check with `docker compose ps`)

# 3️⃣ Run the seed script to create sample hospitals & beds
npm run seed   # inside the `app` container – we’ll exec into it
# or from host:
# docker compose exec app npm run seed

# 4️⃣ Verify the API and WS are reachable
curl http://localhost:3000/health
# you should get {"status":"ok"}

# 5️⃣ Open the driver UI (future work) – for now you can connect with any WS client:
# ws://localhost:3000 (first message must be a register payload, see protocol.md)
```

## Project structure
```
MediGrid-
│   docker-compose.yml
│   Dockerfile
│   package.json
│   tsconfig.json
│   README.md
│   protocol.md   ← WebSocket message schema
│
├─src
│   │   index.ts   ← Fastify + WS server
│   │   seed.ts    ← Simple DB seeder (static hospitals)
│   └─ … (future modules)
```

## WebSocket protocol (see `protocol.md`)
- **register** – first message after connection, supplies `actorId` (ambulance, hospital staff, control‑room).
- **bedUpdate** – broadcast when a bed occupancy changes.
- **ambulanceLocation** – periodic GPS updates.
- … add more types as needed.

## Development
```powershell
# Install dependencies locally (optional, for IDE autocomplete)
npm install
# Run in watch mode (requires ts-node)
npx ts-node-dev src/index.ts
```

## Stopping the stack
```powershell
docker compose down
```

---
*All components respect the 4‑hour hard deadline and the 2 s latency budget described in the design docs.*
