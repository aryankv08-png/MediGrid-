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
    const res = await fetch(url);
    if (!res.ok) {
      throw new Error(`OSRM responded with status ${res.status}`);
    }
    const data: any = await res.json();
    if (data.code === 'Ok' && data.routes && data.routes.length > 0) {
      return {
        distanceMeters: data.routes[0].distance,
        durationSeconds: data.routes[0].duration,
      };
    }
  } catch (err) {
    logger.warn({ err }, 'OSRM routing failed, falling back to straight-line proxy');
  }

  // Fallback straight-line approximation (Haversine)
  const d = haversineDistance(fromLat, fromLon, toLat, toLon);
  return {
    distanceMeters: d,
    durationSeconds: d / 11.11, // ~40 km/h approx
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

