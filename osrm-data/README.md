# OSRM Data Directory

This folder holds the OpenStreetMap data file (`region.osm.pbf`) and the pre‑processed OSRM files (`region.osrm.*`).

## How to populate
1. **Run the helper script** (requires internet access):
   ```powershell
   .\scripts\setup-osrm.ps1
   ```
   The script will download a small OSM extract, place it in this folder, and run the OSRM Docker image to generate the routing files.
2. Once the files are present, the `osrm` service in `docker-compose.yml` will start automatically.

*If you already have an OSM extract for a specific region, copy the `*.osm.pbf` and the generated `*.osrm*` files into this folder and skip the script.*
