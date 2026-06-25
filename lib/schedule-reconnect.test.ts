import { describe, it, expect, vi } from "vitest"
import { FlumeReconnector } from "@/reconnector"
import { FlumeLogger } from "@/logger"
import { scheduleFlumeReconnect } from "@/schedule-reconnect"

const createLogger = () => {
  const calls: Array<{ level: string; action: string }> = []
  const logger = new FlumeLogger({
    source: "test",
    deps: { now: () => 0 },
    handler: (log) => {
      calls.push({ level: log.level, action: log.action })
    },
  })
  return { logger, calls }
}

const createReconnector = (overrides?: { maxAttempts?: number }) =>
  new FlumeReconnector({
    maxAttempts: overrides?.maxAttempts ?? 5,
    baseDelay: 100,
    maxDelay: 1000,
    log: createLogger().logger,
    deps: {
      setTimeout: vi.fn(() => 1),
      clearTimeout: vi.fn(),
      random: () => 0.5,
    },
  })

describe("scheduleFlumeReconnect", () => {
  it("logs reconnect.disabled and disconnects when reconnector is null", () => {
    const { logger, calls } = createLogger()
    const setStatus = vi.fn()

    scheduleFlumeReconnect({ reconnector: null, log: logger, setStatus, retry: vi.fn() })

    expect(setStatus).toHaveBeenCalledWith("disconnected")
    expect(calls.some((c) => c.action === "reconnect.disabled")).toBe(true)
  })

  it("logs reconnect.aborted and disconnects when reconnector is cancelled", () => {
    const { logger, calls } = createLogger()
    const reconnector = createReconnector()
    reconnector.cancel()
    const setStatus = vi.fn()

    scheduleFlumeReconnect({ reconnector, log: logger, setStatus, retry: vi.fn() })

    expect(setStatus).toHaveBeenCalledWith("disconnected")
    expect(calls.some((c) => c.action === "reconnect.aborted")).toBe(true)
  })

  it("logs reconnect.exhausted (level=error) and disconnects when attempts ran out", () => {
    const { logger, calls } = createLogger()
    const reconnector = createReconnector({ maxAttempts: 0 })
    const setStatus = vi.fn()

    scheduleFlumeReconnect({ reconnector, log: logger, setStatus, retry: vi.fn() })

    expect(setStatus).toHaveBeenCalledWith("disconnected")
    expect(calls.find((c) => c.action === "reconnect.exhausted")?.level).toBe("error")
  })

  it("flips to reconnecting and logs reconnect.scheduled on success", () => {
    const { logger, calls } = createLogger()
    const reconnector = createReconnector()
    const setStatus = vi.fn()
    const retry = vi.fn()

    scheduleFlumeReconnect({ reconnector, log: logger, setStatus, retry })

    expect(setStatus).toHaveBeenCalledWith("reconnecting")
    expect(calls.some((c) => c.action === "reconnect.scheduled")).toBe(true)
  })

  it("does NOT flip to reconnecting when attempts are exhausted", () => {
    const { logger } = createLogger()
    const reconnector = createReconnector({ maxAttempts: 0 })
    const setStatus = vi.fn()

    scheduleFlumeReconnect({ reconnector, log: logger, setStatus, retry: vi.fn() })

    expect(setStatus).not.toHaveBeenCalledWith("reconnecting")
  })
})
