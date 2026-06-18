import type { FlumeSlackEnvelope } from "@/types"
import { isRecord } from "@/utils/is-record"

export function extractSlackMeta(envelope: FlumeSlackEnvelope): Record<string, string> {
  const meta: Record<string, string> = { event_type: envelope.type }
  const eventPayload = isRecord(envelope.payload.event) ? envelope.payload.event : null

  if (!eventPayload) return meta

  if (typeof eventPayload.channel === "string") meta.channel_id = eventPayload.channel
  if (typeof eventPayload.user === "string") meta.user_id = eventPayload.user
  if (typeof eventPayload.thread_ts === "string") meta.thread_ts = eventPayload.thread_ts
  if (typeof eventPayload.type === "string") meta.slack_event_type = eventPayload.type

  return meta
}
