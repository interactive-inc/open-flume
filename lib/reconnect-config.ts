import type { FlumeReconnectConfig, FlumeReconnectOptions } from "@/types"

const DEFAULTS: FlumeReconnectConfig = {
  maxAttempts: Infinity,
  baseDelay: 1000,
  maxDelay: 30_000,
}

export function resolveFlumeReconnectConfig(
  input: boolean | FlumeReconnectOptions | undefined,
): FlumeReconnectConfig | null {
  if (input === false || input === undefined) return null

  if (input === true) return { ...DEFAULTS }

  return { ...DEFAULTS, ...input }
}
