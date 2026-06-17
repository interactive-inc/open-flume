import { describe, it, expect } from "vitest"
import { resolveFlumeReconnectConfig } from "@/reconnect-config"

describe("resolveFlumeReconnectConfig", () => {
  it("undefined returns null", () => {
    expect(resolveFlumeReconnectConfig(undefined)).toBe(null)
  })

  it("false returns null", () => {
    expect(resolveFlumeReconnectConfig(false)).toBe(null)
  })

  it("true returns defaults", () => {
    const config = resolveFlumeReconnectConfig(true)

    expect(config).toEqual({
      maxAttempts: Infinity,
      baseDelay: 1000,
      maxDelay: 30_000,
    })
  })

  it("partial object merges with defaults", () => {
    const config = resolveFlumeReconnectConfig({ maxAttempts: 5 })

    expect(config).toEqual({
      maxAttempts: 5,
      baseDelay: 1000,
      maxDelay: 30_000,
    })
  })
})
