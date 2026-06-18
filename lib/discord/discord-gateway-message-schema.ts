import { z } from "zod/v4"

export const FlumeGatewayMessageSchema = z.object({
  op: z.number(),
  d: z.record(z.string(), z.unknown()).nullable(),
  s: z.number().nullable(),
  t: z.string().nullable(),
})
