import pino from 'pino';

const logger = pino({ name: 'osrm' });

export interface RouteResult {
  distanceMeters: number;
  durationSeconds: number;
}

export async function calculateRoadDistance(
  fromLat: number,
  fromLon: number,
  toLat: number,
  toLon: number
): Promise<RouteResult | null> {
  const osrmHost = process.env.OSRM_HOST || 'localhost';
  const url = `http://${osrmHost}:5000/route/v1/driving/${fromLon},${fromLat};${toLon},${toLat}?overview=false`;
  const straightDist = haversineDistance(fromLat, fromLon, toLat, toLon);

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 800);
    const res = await fetch(url, { signal: controller.signal });
    clearTimeout(timeout);

    if (res.ok) {
      const data: any = await res.json();
      const isOutOfBounds = data.waypoints && data.waypoints[0] && data.waypoints[0].distance > 10000;
      const isZeroBug = data.routes && data.routes[0] && data.routes[0].distance === 0 && straightDist > 30;

      if (data.code === 'Ok' && data.routes && data.routes.length > 0 && !isOutOfBounds && !isZeroBug && data.routes[0].distance > 0) {
        return {
          distanceMeters: Math.round(data.routes[0].distance),
          durationSeconds: Math.round(data.routes[0].duration),
        };
      }
    }
  } catch (err) {
    // Fall back to city road proxy
  }

  // Realistic Hyderabad urban road network proxy:
  // 1.35 road curvature multiplier accounting for Hyderabad traffic corridors, intersections, and flyovers
  const roadMeters = Math.max(150, Math.round(straightDist * 1.35));
  // 30 km/h emergency response driving speed (~8.33 m/s) with 60s minimum response time
  const durationSeconds = Math.max(60, Math.round(roadMeters / 8.33));

  return {
    distanceMeters: roadMeters,
    durationSeconds: durationSeconds,
  };
}

export function haversineDistance(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const R = 6371e3; // Earth radius in meters
  const toRad = (x: number) => (x * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);

  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) * Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

  return R * c;
}
