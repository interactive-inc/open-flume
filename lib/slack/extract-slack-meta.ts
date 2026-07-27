import type { FlumeSlackEnvelope } from "@/types"
import { isRecord } from "@/utils/is-record"

export function flumeExtractSlackMeta(envelope: FlumeSlackEnvelope): Record<string, string> {
  const meta: Record<string, string> = { event_type: envelope.type }
  const eventPayload = isRecord(envelope.payload.event) ? envelope.payload.event : null

  if (typeof envelope.payload.channel_id === "string") meta.channel_id = envelope.payload.channel_id
  else if (eventPayload && typeof eventPayload.channel === "string")
    meta.channel_id = eventPayload.channel

  if (typeof envelope.payload.user_id === "string") meta.user_id = envelope.payload.user_id
  else if (eventPayload && typeof eventPayload.user === "string") meta.user_id = eventPayload.user

  if (eventPayload && typeof eventPayload.thread_ts === "string")
    meta.thread_ts = eventPayload.thread_ts
  if (eventPayload && typeof eventPayload.type === "string")
    meta.slack_event_type = eventPayload.type

  return meta
}
