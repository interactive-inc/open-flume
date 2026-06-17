import type { FlumeGatewayMessage } from "@/types"
import { FlumeGatewayMessageSchema } from "@/schema"
import { FlumeParseError } from "@/errors/parse-error"
import { safeJsonParse } from "@/utils/safe-json-parse"

export function parseDiscordGatewayMessage(raw: string): FlumeGatewayMessage | FlumeParseError {
  const json = safeJsonParse(raw)

  const parsed = FlumeGatewayMessageSchema.safeParse(json)

  if (!parsed.success) {
    return new FlumeParseError(`invalid gateway message: ${raw.slice(0, 200)}`)
  }

  return parsed.data
}
