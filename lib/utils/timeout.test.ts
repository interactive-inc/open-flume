import { describe, it, expect, vi } from "vitest"
import { FlumeTimeout } from "@/utils/timeout"
import { FlumeLogger } from "@/logger"
import type { FlumeLog } from "@/types"

function setup() {
  const callbacks: Array<() => void> = []
  const logs: FlumeLog[] = []
  const clearTimeout = vi.fn(() => {
    throw new Error("clear denied")
  })
  const deps = {
    setTimeout: (fn: () => void) => {
      callbacks.push(fn)
      return callbacks.length
    },
    clearTimeout,
  }
  const log = new FlumeLogger({
    source: "test",
    handler: (entry) => logs.push(entry),
    deps: { now: () => 0 },
  })
  return { timer: new FlumeTimeout({ deps, log, action: "test.timer" }), callbacks, logs }
}

describe("FlumeTimeout ownership", () => {
  it("invalidates a cancelled callback before reporting a failed clear", () => {
    const test = setup()
    const fired = vi.fn()
    test.timer.schedule(fired, 10)
    test.timer.cancel()
    test.callbacks[0]?.()
    expect(fired).not.toHaveBeenCalled()
    expect(test.logs[0]).toMatchObject({
      action: "test.timer.clear.error",
      errorCode: "FLUME_TIMER_FAILED",
    })
  })

  it("fires only the replacement even when the runtime retains both timers", () => {
    const test = setup()
    const old = vi.fn()
    const next = vi.fn()
    test.timer.schedule(old, 10)
    test.timer.schedule(next, 20)
    for (const callback of test.callbacks) {
      callback()
      callback()
    }
    expect(old).not.toHaveBeenCalled()
    expect(next).toHaveBeenCalledTimes(1)
    expect(test.timer.isArmed).toBe(false)
  })
})
