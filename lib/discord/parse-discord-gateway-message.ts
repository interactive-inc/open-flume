import type { FlumeGatewayMessage } from "@/types"
import { FlumeGatewayMessageSchema } from "@/discord/discord-gateway-message-schema"
import { FlumeParseError } from "@/errors/parse-error"
import { safeJsonParse } from "@/utils/safe-json-parse"

export function parseFlumeDiscordGatewayMessage(
  raw: string,
): FlumeGatewayMessage | FlumeParseError {
  const json = safeJsonParse(raw)

  if (json instanceof FlumeParseError) return json

  const parsed = FlumeGatewayMessageSchema.safeParse(json)

  if (!parsed.success) {
    return new FlumeParseError(`invalid gateway message frame (${raw.length} bytes)`, {
      cause: parsed.error,
    })
  }

  return parsed.data
}
