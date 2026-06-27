import { describe, it, expect, vi } from "vitest"
import { FlumeLogger } from "@/logger"
import { FlumeReconnector } from "@/reconnector"

const createDeps = () => ({
  setTimeout: vi.fn(() => 42),
  clearTimeout: vi.fn(),
  random: () => 0.5,
})

const createLog = () =>
  new FlumeLogger({ source: "test", deps: { now: () => 0 }, handler: () => {} })

const createReconnector = (
  overrides?: Partial<{ maxAttempts: number; baseDelay: number; maxDelay: number }>,
) => {
  const deps = createDeps()

  const reconnector = new FlumeReconnector({
    maxAttempts: overrides?.maxAttempts ?? 10,
    baseDelay: overrides?.baseDelay ?? 1000,
    maxDelay: overrides?.maxDelay ?? 30_000,
    log: createLog(),
    deps,
  })

  return { reconnector, deps }
}

describe("FlumeReconnector", () => {
  it("schedule calls setTimeout with jittered delay", () => {
    const { reconnector, deps } = createReconnector()
    const fn = vi.fn()

    const delay = reconnector.schedule(fn)

    expect(deps.setTimeout).toHaveBeenCalledWith(expect.any(Function), delay)
    expect(delay).toBe(1000 * (0.5 + 0.5 * 0.5))
  })

  it("schedule increments attempt", () => {
    const { reconnector } = createReconnector()

    reconnector.schedule(vi.fn())

    expect(reconnector.attempt).toBe(1)
  })

  it("reset sets attempt to 0", () => {
    const { reconnector } = createReconnector()

    reconnector.schedule(vi.fn())
    reconnector.reset()

    expect(reconnector.attempt).toBe(0)
  })

  it("cancel sets aborted to true and clears timer", () => {
    const { reconnector, deps } = createReconnector()

    reconnector.schedule(vi.fn())
    reconnector.cancel()

    expect(reconnector.aborted).toBe(true)
    expect(deps.clearTimeout).toHaveBeenCalled()
  })

  it("schedule after cancel returns 0", () => {
    const { reconnector } = createReconnector()

    reconnector.cancel()
    const delay = reconnector.schedule(vi.fn())

    expect(delay).toBe(0)
  })

  it("schedule when attempt >= maxAttempts returns -1", () => {
    const { reconnector } = createReconnector({ maxAttempts: 0 })

    const delay = reconnector.schedule(vi.fn())

    expect(delay).toBe(-1)
  })

  it("schedule clears the previous pending timer before scheduling another", () => {
    const { reconnector, deps } = createReconnector()

    reconnector.schedule(vi.fn())
    reconnector.schedule(vi.fn())

    expect(deps.clearTimeout).toHaveBeenCalledTimes(1)
  })

  it("returns -1 once attempts hit the limit even after successful schedules", () => {
    const { reconnector } = createReconnector({ maxAttempts: 2 })

    expect(reconnector.schedule(vi.fn())).toBeGreaterThan(0)
    expect(reconnector.schedule(vi.fn())).toBeGreaterThan(0)
    expect(reconnector.schedule(vi.fn())).toBe(-1)
  })

  it("cancel after exhaustion is a no-op (aborted true, no extra clear)", () => {
    const { reconnector, deps } = createReconnector({ maxAttempts: 1 })

    reconnector.schedule(vi.fn())
    reconnector.schedule(vi.fn())
    const clearsBefore = deps.clearTimeout.mock.calls.length
    reconnector.cancel()

    expect(reconnector.aborted).toBe(true)
    expect(deps.clearTimeout.mock.calls.length - clearsBefore).toBeLessThanOrEqual(1)
  })

  it("when the scheduled callback fires, internal timer reference is cleared", () => {
    let captured = (): void => {}
    const setTimeoutMock = vi.fn((fn: () => void, _ms: number) => {
      captured = fn
      return 99
    })
    const clearTimeoutMock = vi.fn()
    const reconnector = new FlumeReconnector({
      maxAttempts: 5,
      baseDelay: 100,
      maxDelay: 1000,
      log: createLog(),
      deps: { setTimeout: setTimeoutMock, clearTimeout: clearTimeoutMock, random: () => 0.5 },
    })

    const fn = vi.fn()
    reconnector.schedule(fn)
    captured()

    reconnector.schedule(vi.fn())

    expect(clearTimeoutMock).not.toHaveBeenCalled()
    expect(fn).toHaveBeenCalled()
  })

  it("does not advance attempt when setTimeout throws", () => {
    const throwingSetTimeout = vi.fn(() => {
      throw new Error("timer rejected")
    })
    const reconnector = new FlumeReconnector({
      maxAttempts: 5,
      baseDelay: 1000,
      maxDelay: 30_000,
      log: createLog(),
      deps: { setTimeout: throwingSetTimeout, clearTimeout: vi.fn(), random: () => 0.5 },
    })

    const result = reconnector.schedule(vi.fn())

    expect(result).toBe(0)
    expect(reconnector.attempt).toBe(0)
  })

  it("keeps the same backoff delay after a failed schedule retries", () => {
    let shouldThrow = true
    const setTimeoutMock = vi.fn((_fn: () => void, _ms: number) => {
      if (shouldThrow) throw new Error("timer rejected")
      return 7
    })
    const reconnector = new FlumeReconnector({
      maxAttempts: 5,
      baseDelay: 1000,
      maxDelay: 30_000,
      log: createLog(),
      deps: { setTimeout: setTimeoutMock, clearTimeout: vi.fn(), random: () => 0.5 },
    })

    reconnector.schedule(vi.fn())
    shouldThrow = false
    const delay = reconnector.schedule(vi.fn())

    // 失敗で attempt が進んでいないので、再試行も attempt 0 の delay (baseDelay 基準) になる
    expect(delay).toBe(1000 * (0.5 + 0.5 * 0.5))
    expect(reconnector.attempt).toBe(1)
  })
})
