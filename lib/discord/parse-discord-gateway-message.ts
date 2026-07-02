import type { FlumeGatewayMessage } from "@/types"
import { FlumeGatewayMessageSchema } from "@/discord/discord-gateway-message-schema"
import { FlumeParseError } from "@/errors/parse-error"
import { safeJsonParse } from "@/utils/safe-json-parse"

/**
 * 省略された d / s / t は null に正規化して返す (schema は nullish を許すが、
 * 消費側が undefined を意識しなくて済むようここで揃える)
 */
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

  return {
    op: parsed.data.op,
    d: parsed.data.d ?? null,
    s: parsed.data.s ?? null,
    t: parsed.data.t ?? null,
  }
}
