import { describe, it, expect, vi } from "vitest"
import { FlumeLogger } from "@/logger"

const createDeps = () => ({
  now: () => 1000,
})

describe("FlumeLogger", () => {
  it("debug calls handler with correct fields", () => {
    const deps = createDeps()
    const handler = vi.fn()
    const logger = new FlumeLogger({ source: "test", handler, deps })

    logger.debug({ action: "connect", message: "ok" })

    expect(handler).toHaveBeenCalledWith({
      level: "debug",
      source: "test",
      action: "connect",
      message: "ok",
      timestamp: 1000,
      error: undefined,
      detail: undefined,
    })
  })

  it("info calls handler with correct level", () => {
    const deps = createDeps()
    const handler = vi.fn()
    const logger = new FlumeLogger({ source: "src", handler, deps })

    logger.info({ action: "open", message: "ready" })

    expect(handler).toHaveBeenCalledWith(
      expect.objectContaining({ level: "info", source: "src", action: "open", message: "ready" }),
    )
  })

  it("warn calls handler with correct level", () => {
    const deps = createDeps()
    const handler = vi.fn()
    const logger = new FlumeLogger({ source: "src", handler, deps })

    logger.warn({ action: "retry", message: "slow" })

    expect(handler).toHaveBeenCalledWith(expect.objectContaining({ level: "warn" }))
  })

  it("error calls handler with the error field when provided", () => {
    const deps = createDeps()
    const handler = vi.fn()
    const logger = new FlumeLogger({ source: "src", handler, deps })
    const err = new Error("boom")

    logger.error({ action: "fail", message: "broken", error: err })

    expect(handler).toHaveBeenCalledWith(expect.objectContaining({ level: "error", error: err }))
  })

  it("error accepts entries without an error field", () => {
    const deps = createDeps()
    const handler = vi.fn()
    const logger = new FlumeLogger({ source: "src", handler, deps })

    logger.error({ action: "fail", message: "no-err" })

    expect(handler).toHaveBeenCalledWith(
      expect.objectContaining({ level: "error", error: undefined }),
    )
  })

  it("all levels invoke the handler once", () => {
    const deps = createDeps()
    const handler = vi.fn()
    const logger = new FlumeLogger({ source: "src", handler, deps })

    logger.debug({ action: "a", message: "m" })
    logger.info({ action: "b", message: "n" })
    logger.warn({ action: "c", message: "o" })
    logger.error({ action: "d", message: "p" })

    expect(handler).toHaveBeenCalledTimes(4)
    expect(handler).toHaveBeenCalledWith(
      expect.objectContaining({ level: "debug", source: "src", action: "a", message: "m" }),
    )
  })

  it("no handler does not crash", () => {
    const deps = createDeps()
    const logger = new FlumeLogger({ source: "src", deps })

    expect(() => logger.info({ action: "x", message: "y" })).not.toThrow()
  })

  it("does not propagate a synchronous throw from the handler", () => {
    const deps = createDeps()
    const logger = new FlumeLogger({
      source: "src",
      deps,
      handler: () => {
        throw new Error("boom")
      },
    })

    expect(() => logger.info({ action: "a", message: "m" })).not.toThrow()
  })

  it("subsequent emits still fire after the handler throws", () => {
    const deps = createDeps()
    let calls = 0
    const logger = new FlumeLogger({
      source: "src",
      deps,
      handler: () => {
        calls++
        if (calls === 1) throw new Error("first")
      },
    })

    logger.info({ action: "a", message: "m" })
    logger.info({ action: "b", message: "n" })

    expect(calls).toBe(2)
  })

  it("does not propagate a rejected promise from an async handler", async () => {
    const deps = createDeps()
    const logger = new FlumeLogger({
      source: "src",
      deps,
      handler: async () => {
        throw new Error("async-boom")
      },
    })

    logger.info({ action: "a", message: "m" })

    await new Promise((resolve) => globalThis.setTimeout(resolve, 0))
  })
})
