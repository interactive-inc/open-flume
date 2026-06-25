import { isRecord } from "@/utils/is-record"

export function flumeExtractDiscordMeta(
  eventName: string,
  eventData: Record<string, unknown>,
): Record<string, string> {
  const meta: Record<string, string> = { event_type: eventName }

  if (typeof eventData.channel_id === "string") meta.channel_id = eventData.channel_id
  if (typeof eventData.guild_id === "string") meta.guild_id = eventData.guild_id
  if (isRecord(eventData.author) && typeof eventData.author.id === "string")
    meta.user_id = eventData.author.id

  return meta
}
