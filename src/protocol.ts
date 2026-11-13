import { z } from 'zod';

export const BaseMessageSchema = z.object({
  type: z.string(),
  actorId: z.string().min(1),
  payload: z.record(z.any()).optional().default({}),
});

export const RegisterPayloadSchema = z.object({
  role: z.enum(['ambulance', 'hospital', 'control']).default('ambulance'),
  hospitalId: z.string().optional(),
});

export const BedUpdatePayloadSchema = z.object({
  bedId: z.string(),
  occupied: z.boolean(),
  status: z.enum(['FREE', 'OCCUPIED', 'CLEANING', 'RESERVED']).optional(),
  timestamp: z.number().optional(),
});

export const AmbulanceLocationPayloadSchema = z.object({
  lat: z.number(),
  lon: z.number(),
  speed: z.number().optional(),
  timestamp: z.number().optional(),
});

export const RequestBedPayloadSchema = z.object({
  lat: z.number(),
  lon: z.number(),
  bedType: z.enum(['ICU', 'VENTILATOR', 'GENERAL', 'PEDIATRIC']).default('ICU'),
  priority: z.enum(['CRITICAL', 'ROUTINE']).default('ROUTINE'),
  hospitalId: z.string().optional(),
});

// Transfer Workflow Payloads (Section 3.A & 3.B)
export const TransferRequestPayloadSchema = z.object({
  patientId: z.string().min(1),
  fromHospitalId: z.string().min(1),
  toHospitalId: z.string().min(1),
  sourceBedId: z.string().min(1),
  bedType: z.enum(['ICU', 'VENTILATOR', 'GENERAL', 'PEDIATRIC']).default('ICU'),
  priority: z.enum(['CRITICAL', 'ROUTINE']).default('ROUTINE'),
});

export const TransferActionPayloadSchema = z.object({
  transferId: z.string().min(1),
  action: z.enum(['ACCEPT', 'REJECT', 'TIMEOUT']),
  reason: z.string().optional(),
});
