#!/usr/bin/env pwsh
# setup-osrm.ps1 – download a small OSM extract and prepare OSRM routing files
# This script needs network access, so run it with bypass sandbox.

$ErrorActionPreference = 'Stop'

# Ensure the osrm-data directory exists (relative to this script)
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$osrmDataPath = Join-Path $scriptDir '..\osrm-data'
if (-not (Test-Path $osrmDataPath)) {
  New-Item -ItemType Directory -Path $osrmDataPath | Out-Null
  Write-Host "Created osrm-data directory at $osrmDataPath"
} else {
  Write-Host "Using existing osrm-data directory at $osrmDataPath"
}

# 1️⃣ Choose a small OSM region (Andorra ~2 MB). Change URL if you prefer another area.
$osmUrl = 'https://download.geofabrik.de/europe/andorra-latest.osm.pbf'
$targetFile = Join-Path $osrmDataPath 'region.osm.pbf'

Write-Host "Downloading OSM extract..."
Invoke-WebRequest -Uri $osmUrl -OutFile $targetFile
Write-Host "Saved to $targetFile"

# 2️⃣ Run OSRM extraction (requires Docker). Use the OSRM Docker image.
Write-Host "Running OSRM extraction..."
# Mount the host folder into the container
docker run --rm -v "$(Resolve-Path $osrmDataPath):/data" osrm/osrm-backend osrm-extract -p /data/profiles/car.lua /data/region.osm.pbf

# 3️⃣ Run OSRM contraction (pre‑process for fast routing)
Write-Host "Running OSRM contraction..."
docker run --rm -v "$(Resolve-Path $osrmDataPath):/data" osrm/osrm-backend osrm-contract /data/region.osm.pbf

Write-Host "OSRM data ready in ./osrm-data"
