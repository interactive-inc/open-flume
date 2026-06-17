import { describe, it, expect, vi } from "vitest"
import { FlumeReconnector } from "@/reconnector"

const createDeps = () => ({
  setTimeout: vi.fn(() => 42 as unknown as ReturnType<typeof globalThis.setTimeout>),
  clearTimeout: vi.fn(),
  random: () => 0.5,
})

const createReconnector = (overrides?: Partial<{ maxAttempts: number; baseDelay: number; maxDelay: number }>) => {
  const deps = createDeps()

  const reconnector = new FlumeReconnector({
    maxAttempts: overrides?.maxAttempts ?? 10,
    baseDelay: overrides?.baseDelay ?? 1000,
    maxDelay: overrides?.maxDelay ?? 30_000,
    deps,
  })

  return { reconnector, deps }
}

describe("FlumeReconnector", () => {
  it("schedule calls setTimeout with jittered delay", () => {
    const { reconnector, deps } = createReconnector()
    const fn = vi.fn()

    const delay = reconnector.schedule(fn)

    expect(deps.setTimeout).toHaveBeenCalledWith(fn, delay)
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
})
