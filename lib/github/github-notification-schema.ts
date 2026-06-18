import { z } from "zod/v4"

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
