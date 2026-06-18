import { z } from "zod/v4"

export const FlumeSlackConnectionResponseSchema = z.object({
  ok: z.boolean(),
  url: z.string().optional(),
  error: z.string().optional(),
})
