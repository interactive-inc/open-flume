import { describe, expect, it, vi } from "vitest"
import { FlumeLogger } from "@/logger"
import { FlumeSignalRegistry } from "@/source-helpers/flume-signal-registry"

const createLogger = () =>
  new FlumeLogger({
    source: "test",
    deps: { now: () => 0 },
    handler: () => {},
  })

describe("FlumeSignalRegistry", () => {
  it("invokes onAbort when a registered signal aborts", () => {
    const onAbort = vi.fn()
    const reg = new FlumeSignalRegistry({ log: createLogger(), onAbort })
    const c = new AbortController()
    reg.register(c.signal)
    c.abort()
    expect(onAbort).toHaveBeenCalledOnce()
  })

  it("does nothing when a signal is undefined", () => {
    const reg = new FlumeSignalRegistry({ log: createLogger(), onAbort: vi.fn() })
    reg.register(undefined)
    expect(reg.size).toBe(0)
  })

  it("isAnyAborted reflects extra and registered signals", () => {
    const reg = new FlumeSignalRegistry({ log: createLogger(), onAbort: vi.fn() })
    const a = new AbortController()
    reg.register(a.signal)
    expect(reg.isAnyAborted()).toBe(false)

    const b = new AbortController()
    b.abort()
    expect(reg.isAnyAborted(b.signal)).toBe(true)

    a.abort()
    expect(reg.isAnyAborted()).toBe(true)
  })

  it("unregisterAll removes the listener and clears the registry", () => {
    const onAbort = vi.fn()
    const reg = new FlumeSignalRegistry({ log: createLogger(), onAbort })
    const c = new AbortController()
    reg.register(c.signal)

    reg.unregisterAll()
    c.abort()

    expect(onAbort).not.toHaveBeenCalled()
    expect(reg.size).toBe(0)
  })

  it("survives a signal with a throwing addEventListener (does not register)", () => {
    const poisoned = {
      addEventListener: () => {
        throw new Error("frozen")
      },
      removeEventListener: () => {},
    } as unknown as AbortSignal
    const reg = new FlumeSignalRegistry({ log: createLogger(), onAbort: vi.fn() })

    reg.register(poisoned)
    expect(reg.size).toBe(0)
  })
})
