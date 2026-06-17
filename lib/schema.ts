import { z } from "zod/v4"

export const FlumeGatewayMessageSchema = z.object({
  op: z.number(),
  d: z.record(z.string(), z.unknown()).nullable(),
  s: z.number().nullable(),
  t: z.string().nullable(),
})

export const FlumeSlackEnvelopeSchema = z.object({
  envelope_id: z.string(),
  type: z.string(),
  payload: z.record(z.string(), z.unknown()),
  accepts_response_payload: z.boolean().optional(),
  retry_attempt: z.number().optional(),
  retry_reason: z.string().optional(),
})

export const FlumeSlackConnectionResponseSchema = z.object({
  ok: z.boolean(),
  url: z.string().optional(),
  error: z.string().optional(),
})

export const FlumeGitHubNotificationSchema = z.object({
  id: z.string(),
  reason: z.string(),
  unread: z.boolean(),
  updated_at: z.string(),
  subject: z.object({
    title: z.string(),
    url: z.string().nullable(),
    type: z.string(),
  }),
  repository: z.object({
    full_name: z.string(),
  }),
})
