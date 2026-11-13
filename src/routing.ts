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

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 1200);
    const res = await fetch(url, { signal: controller.signal });
    clearTimeout(timeout);

    if (res.ok) {
      const data: any = await res.json();
      if (data.code === 'Ok' && data.routes && data.routes.length > 0) {
        return {
          distanceMeters: Math.round(data.routes[0].distance),
          durationSeconds: Math.round(data.routes[0].duration),
        };
      }
    }
  } catch (err) {
    // Silent fallback to city-scale Haversine
  }

  // Realistic urban road network proxy (Haversine distance multiplied by 1.28 city curvature)
  const d = haversineDistance(fromLat, fromLon, toLat, toLon);
  const roadMeters = Math.round(d * 1.28);
  return {
    distanceMeters: roadMeters,
    durationSeconds: Math.round(roadMeters / 11.11), // ~40 km/h emergency transit
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
