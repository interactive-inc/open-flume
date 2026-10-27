import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { FlumeOperationError, FlumeStateError } from "@/index"
import { createFlumeDefaultDeps } from "@/deps"
import { FlumeLogger } from "@/logger"
import { withDeadline } from "@/utils/with-deadline"

function setup() {
  const deps = createFlumeDefaultDeps()
  const context = { operation: "test.operation", phase: "waiting" }
  return {
    deps,
    context: () => context,
    log: new FlumeLogger({ source: "test", deps }),
    timeoutMs: 100,
  }
}
beforeEach(() => vi.useFakeTimers())
afterEach(() => vi.useRealTimers())

describe("operation deadlines", () => {
  it("returns an immutable operation error and aborts a stalled operation", async () => {
    const props = setup()
    const gate = Promise.withResolvers<void>()
    const run = vi.fn((_signal: AbortSignal) => gate.promise)
    const pending = withDeadline({ ...props, run })
    await vi.advanceTimersByTimeAsync(100)
    const error = await pending
    expect(error).toBeInstanceOf(FlumeOperationError)
    expect(error).not.toBeInstanceOf(FlumeStateError)
    expect(error).toMatchObject({
      errorCode: "FLUME_TIMEOUT",
      context: { operation: "test.operation", phase: "waiting" },
    })
    expect(Object.isFrozen(error)).toBe(true)
    props.context().phase = "changed"
    expect(error).toMatchObject({ context: { phase: "waiting" } })
    expect(run.mock.calls[0]?.[0].aborted).toBe(true)
    expect(vi.getTimerCount()).toBe(0)
    gate.resolve()
  })

  it("does not enter an operation when already aborted", async () => {
    const controller = new AbortController()
    controller.abort()
    const run = vi.fn(async () => 42)
    const error = await withDeadline({ ...setup(), signal: controller.signal, run })
    expect(error).toMatchObject({ name: "FlumeOperationError", errorCode: "FLUME_ABORTED" })
    expect(run).not.toHaveBeenCalled()
    expect(vi.getTimerCount()).toBe(0)
  })

  it("isolates a partially registered abort listener and retains the original cause", async () => {
    const controller = new AbortController()
    const register = controller.signal.addEventListener.bind(controller.signal)
    const failure = new Error("listener failure")
    vi.spyOn(controller.signal, "addEventListener").mockImplementation(
      (type, listener, options) => {
        register(type, listener, options)
        throw failure
      },
    )
    const remove = vi.spyOn(controller.signal, "removeEventListener")
    const run = vi.fn(async () => 42)
    const error = await withDeadline({ ...setup(), signal: controller.signal, run })
    expect(error).toMatchObject({
      name: "FlumeOperationError",
      errorCode: "FLUME_RUNTIME_FAILED",
      cause: failure,
    })
    expect(remove).toHaveBeenCalledOnce()
    expect(run).not.toHaveBeenCalled()
    expect(vi.getTimerCount()).toBe(0)
  })

  it("clears a successful deadline and preserves a host rejection without relabeling it", async () => {
    const props = setup()
    const run = vi.fn(async (_signal: AbortSignal) => 42)
    expect(await withDeadline({ ...props, run })).toBe(42)
    expect(run.mock.calls[0]?.[0].aborted).toBe(true)
    const failure = new Error("host failure")
    expect(
      await withDeadline({
        ...props,
        run: async () => {
          throw failure
        },
      }),
    ).toBe(failure)
    expect(vi.getTimerCount()).toBe(0)
  })
})
