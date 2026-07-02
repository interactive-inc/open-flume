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

  it("rejects baseDelay 0 / negative / NaN to prevent 0ms hot loops", () => {
    expect(resolveFlumeReconnectConfig({ baseDelay: 0 })?.baseDelay).toBe(1000)
    expect(resolveFlumeReconnectConfig({ baseDelay: -5 })?.baseDelay).toBe(1000)
    expect(resolveFlumeReconnectConfig({ baseDelay: Number.NaN })?.baseDelay).toBe(1000)
    expect(resolveFlumeReconnectConfig({ baseDelay: Infinity })?.baseDelay).toBe(1000)
  })

  it("explicit undefined fields do not override defaults", () => {
    const config = resolveFlumeReconnectConfig({
      baseDelay: undefined,
      maxDelay: undefined,
      maxAttempts: undefined,
    })

    expect(config).toEqual({
      maxAttempts: Infinity,
      baseDelay: 1000,
      maxDelay: 30_000,
    })
  })

  it("clamps maxDelay below baseDelay up to baseDelay and rejects non-finite", () => {
    expect(resolveFlumeReconnectConfig({ baseDelay: 5000, maxDelay: 100 })?.maxDelay).toBe(5000)
    expect(resolveFlumeReconnectConfig({ maxDelay: Infinity })?.maxDelay).toBe(30_000)
    expect(resolveFlumeReconnectConfig({ maxDelay: Number.NaN })?.maxDelay).toBe(30_000)
  })

  it("rejects invalid maxAttempts (NaN / 0 / negative / fractional), keeps Infinity", () => {
    expect(resolveFlumeReconnectConfig({ maxAttempts: Number.NaN })?.maxAttempts).toBe(Infinity)
    expect(resolveFlumeReconnectConfig({ maxAttempts: 0 })?.maxAttempts).toBe(Infinity)
    expect(resolveFlumeReconnectConfig({ maxAttempts: -1 })?.maxAttempts).toBe(Infinity)
    expect(resolveFlumeReconnectConfig({ maxAttempts: 2.5 })?.maxAttempts).toBe(Infinity)
    expect(resolveFlumeReconnectConfig({ maxAttempts: Infinity })?.maxAttempts).toBe(Infinity)
  })
})
