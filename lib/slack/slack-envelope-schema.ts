import { z } from "zod/v4"

export const FlumeSlackEnvelopeSchema = z.object({
  envelope_id: z.string(),
  type: z.string(),
  payload: z.record(z.string(), z.unknown()),
  accepts_response_payload: z.boolean().optional(),
  retry_attempt: z.number().optional(),
  retry_reason: z.string().optional(),
})
