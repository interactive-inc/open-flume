import { FlumeParseError } from "@/errors/parse-error"

export function safeJsonParse(raw: string): unknown | FlumeParseError {
  try {
    return JSON.parse(raw)
  } catch (error) {
    return new FlumeParseError(
      error instanceof Error ? `invalid JSON: ${error.message}` : "invalid JSON",
      { cause: error },
    )
  }
}
